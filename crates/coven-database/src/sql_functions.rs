//! The functions an app gives SQLite, registered on every connection to the
//! store's database (§4, api.md E1): collations, which order text its SQL
//! names with `COLLATE`, and scalar functions, which compute a value its SQL
//! calls by name.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::{FromSql, Value, ValueRef};

use crate::DbError;

/// SQLite's own collations. Coven's tables use only these, so an app name is
/// refused only when it is one of them.
const SQLITE_COLLATIONS: [&str; 3] = ["BINARY", "NOCASE", "RTRIM"];

type Compare = dyn Fn(&str, &str) -> Ordering + Send + Sync;
type Compute = dyn Fn(&FunctionArguments<'_>) -> rusqlite::Result<Value> + Send + Sync;

/// The arguments SQLite passes one call of an app function. Only the
/// arguments: the connection running the statement stays coven's.
pub struct FunctionArguments<'a> {
    context: &'a Context<'a>,
}

impl FunctionArguments<'_> {
    /// Argument `index`, counted from zero, converted as a row's column is.
    /// An index past the function's arguments panics.
    pub fn get<T: FromSql>(&self, index: usize) -> rusqlite::Result<T> {
        self.context.get(index)
    }

    /// Argument `index`, counted from zero, as SQLite holds it. An index past
    /// the function's arguments panics.
    pub fn get_raw(&self, index: usize) -> ValueRef<'_> {
        self.context.get_raw(index)
    }
}

/// Whether a scalar function's result depends on its arguments alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Determinism {
    /// The same arguments always give the same result, on every device and
    /// in every app version that opens the store. SQLite allows the function
    /// where a stored value depends on it: an index on an expression, a
    /// partial index's condition, a CHECK and a generated column.
    Deterministic,
    /// The result can change between calls with the same arguments, as one
    /// that reads the clock does. SQLite refuses it wherever a stored value
    /// would depend on it.
    Nondeterministic,
}

#[derive(Clone)]
struct Scalar {
    name: String,
    arguments: u8,
    determinism: Determinism,
    compute: Arc<Compute>,
}

/// The app's collations and scalar functions. Coven registers every one on
/// each connection it opens to the store's database, before any app SQL
/// runs there: the writer, which also migrates, applies other devices'
/// writes, merges and checks integrity, the readers, and every connection
/// of a read-only handle.
///
/// A collation, or a deterministic function, is part of the store's schema
/// once the schema uses it. It must give the same answer for the same
/// arguments on every device and in every app version that opens the store,
/// and a collation must be a total order: consistent, antisymmetric and
/// transitive, though distinct strings may compare equal. An index keeps
/// its entries as the function gave them when they were written, and a
/// CHECK decides by it which rows every device keeps (§9.3). So a changed
/// answer makes a store fail its integrity check on the next open
/// (`DamagedDatabase`), and two devices with different answers can keep
/// different rows.
#[derive(Clone, Default)]
pub struct SqlFunctions {
    collations: Vec<(String, Arc<Compare>)>,
    scalars: Vec<Scalar>,
}

impl SqlFunctions {
    /// Adds the collation `name`, ordering text by `compare`. SQLite
    /// compares collation names without regard to ASCII case. A name that is
    /// SQLite's own, `BINARY`, `NOCASE` or `RTRIM`, fails the open with
    /// `DbError::ReservedCollation`, and one added twice with
    /// `DbError::DuplicateCollation`. A panic in `compare` ends the process:
    /// SQLite has no way for a comparison to fail, and any answer coven made
    /// up could put an index out of order.
    pub fn collation<F>(mut self, name: impl Into<String>, compare: F) -> Self
    where
        F: Fn(&str, &str) -> Ordering + Send + Sync + 'static,
    {
        self.collations.push((name.into(), Arc::new(compare)));
        self
    }

    /// Adds the scalar function `name`, taking exactly `arguments`
    /// arguments, which SQL calls as `name(...)`. `compute` reads them and
    /// returns the result, or an error that fails the statement calling it.
    /// SQLite compares function names without regard to ASCII case. A name
    /// SQLite or coven already has, or one with coven's reserved prefix
    /// `coven_` or `_coven_`, fails the open with `DbError::ReservedFunction`,
    /// and one added twice with `DbError::DuplicateFunction`. Every app
    /// function is marked safe for triggers and views, which only the app's
    /// own migrations declare. A panic in `compute` ends the process.
    pub fn scalar<F, T>(
        mut self,
        name: impl Into<String>,
        arguments: u8,
        determinism: Determinism,
        compute: F,
    ) -> Self
    where
        F: Fn(&FunctionArguments<'_>) -> rusqlite::Result<T> + Send + Sync + 'static,
        T: Into<Value>,
    {
        self.scalars.push(Scalar {
            name: name.into(),
            arguments,
            determinism,
            compute: Arc::new(move |arguments| compute(arguments).map(Into::into)),
        });
        self
    }

    /// Register every collation and scalar function on a connection that
    /// has run no app statement, after coven's own functions.
    pub(crate) fn register(&self, connection: &rusqlite::Connection) -> Result<(), DbError> {
        for (position, (name, compare)) in self.collations.iter().enumerate() {
            if SQLITE_COLLATIONS
                .iter()
                .any(|own| own.eq_ignore_ascii_case(name))
            {
                return Err(DbError::ReservedCollation { name: name.clone() });
            }
            if self.collations[..position]
                .iter()
                .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
            {
                return Err(DbError::DuplicateCollation { name: name.clone() });
            }
            let compare = compare.clone();
            let collation = name.clone();
            connection.create_collation(name.as_str(), move |left, right| {
                unless_panicked(
                    &collation,
                    "the app's collation panicked; no order is safe to return",
                    || compare(left, right),
                )
            })?;
        }
        if self.scalars.is_empty() {
            return Ok(());
        }
        // SQLite's built-in functions and coven's, read before any of the
        // app's is registered.
        let existing = connection
            .prepare("SELECT DISTINCT lower(name) FROM pragma_function_list")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<BTreeSet<String>>>()?;
        for (position, scalar) in self.scalars.iter().enumerate() {
            let name = &scalar.name;
            if existing.contains(&name.to_ascii_lowercase())
                || crate::internal_schema::reserved_name(name)
            {
                return Err(DbError::ReservedFunction { name: name.clone() });
            }
            if self.scalars[..position]
                .iter()
                .any(|earlier| earlier.name.eq_ignore_ascii_case(name))
            {
                return Err(DbError::DuplicateFunction { name: name.clone() });
            }
            let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS;
            let flags = match scalar.determinism {
                Determinism::Deterministic => flags | FunctionFlags::SQLITE_DETERMINISTIC,
                Determinism::Nondeterministic => flags,
            };
            let compute = scalar.compute.clone();
            let function = name.clone();
            connection.create_scalar_function(
                name.as_str(),
                i32::from(scalar.arguments),
                flags,
                move |context| {
                    unless_panicked(&function, "the app's SQL function panicked", || {
                        compute(&FunctionArguments { context })
                    })
                },
            )?;
        }
        Ok(())
    }
}

/// Run one of the app's functions for SQLite, ending the process if it
/// panics. A panic is a bug in the app's function, not an answer: no order a
/// collation could return is safe, and after a scalar function panics its
/// answers can't be trusted to match the ones its indexes and CHECKs hold.
fn unless_panicked<T>(name: &str, message: &'static str, call: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(result) => result,
        Err(_panic) => {
            tracing::error!(function = %name, "{message}");
            std::process::abort()
        }
    }
}

#[cfg(test)]
#[path = "sql_functions_tests.rs"]
mod tests;
