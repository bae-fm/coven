//! The text orders an app's SQL names with `COLLATE`, registered on every
//! connection to the store's database (§4, api.md E1).

use std::cmp::Ordering;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use crate::DbError;

/// SQLite's own collations. Coven's tables use only these, so an app name is
/// refused only when it is one of them.
const SQLITE_COLLATIONS: [&str; 3] = ["BINARY", "NOCASE", "RTRIM"];

type Compare = dyn Fn(&str, &str) -> Ordering + Send + Sync;

/// The app's collations, each a name its SQL uses in `COLLATE` and the
/// function that orders two strings by it. Coven registers every one on
/// each connection it opens to the store's database, before any app SQL
/// runs there: the writer, which also migrates, applies other devices'
/// writes, merges and checks integrity, the readers, and every connection
/// of a read-only handle.
///
/// An order is part of the store's schema. Its function must be
/// deterministic, the same two strings always comparing the same way on
/// every device and in every app version that opens the store, and a total
/// order: consistent, antisymmetric and transitive, though distinct strings
/// may compare equal. An index keeps its entries in the order the function
/// gave as they were written, and a CHECK decides by it which rows every
/// device keeps (§9.3). So a changed order makes a store fail its integrity
/// check on the next open (`DamagedDatabase`), and two devices with
/// different orders can keep different rows.
#[derive(Clone, Default)]
pub struct Collations {
    orders: Vec<(String, Arc<Compare>)>,
}

impl Collations {
    /// Adds the collation `name`, ordering text by `compare`. SQLite
    /// compares collation names without regard to ASCII case. A name that is
    /// SQLite's own, `BINARY`, `NOCASE` or `RTRIM`, fails the open with
    /// `DbError::ReservedCollation`, and one added twice with
    /// `DbError::DuplicateCollation`. A panic in `compare` ends the process:
    /// SQLite has no way for a comparison to fail, and any answer coven made
    /// up could put an index out of order.
    pub fn with<F>(mut self, name: impl Into<String>, compare: F) -> Self
    where
        F: Fn(&str, &str) -> Ordering + Send + Sync + 'static,
    {
        self.orders.push((name.into(), Arc::new(compare)));
        self
    }

    /// Register every collation on a connection that has run no statement.
    pub(crate) fn register(&self, connection: &rusqlite::Connection) -> Result<(), DbError> {
        for (position, (name, compare)) in self.orders.iter().enumerate() {
            if SQLITE_COLLATIONS
                .iter()
                .any(|own| own.eq_ignore_ascii_case(name))
            {
                return Err(DbError::ReservedCollation { name: name.clone() });
            }
            if self.orders[..position]
                .iter()
                .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
            {
                return Err(DbError::DuplicateCollation { name: name.clone() });
            }
            let compare = compare.clone();
            let collation = name.clone();
            connection.create_collation(name.as_str(), move |left, right| {
                match catch_unwind(AssertUnwindSafe(|| compare(left, right))) {
                    Ok(order) => order,
                    Err(_panic) => {
                        tracing::error!(
                            collation = %collation,
                            "the app's collation panicked; no order is safe to return"
                        );
                        std::process::abort()
                    }
                }
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "collations_tests.rs"]
mod tests;
