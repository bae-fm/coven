//! Composes an app's reads from SQL fragments while the app compiles
//! (`SPEC.md`). `fragment!`, `read!` and `write!` are the macros an app writes;
//! the `compiler` feature adds the compiler its build script runs.

#[cfg(feature = "compiler")]
pub mod compiler;

pub use sqlive_compose_macros::{fragment, read, write};

use sqlive_reads::{CompiledStatement, Row, SqliteResult, ToSql};

/// A statement the compiler generated: its SQL and the values it binds. The
/// compiler implements it for the parameters struct of each `read!` and
/// `write!`, so that a store takes a generated statement by its type, and no
/// other string (`SPEC.md`, section 9).
pub trait Statement {
    /// The composed statement.
    const SQL: &'static str;

    /// The parameters by their names in the statement, each with its first
    /// character (`:name`).
    fn params(&self) -> Vec<(&'static str, &dyn ToSql)>;
}

/// A generated statement that returns rows, and the row it reads them as.
pub trait Returning: Statement {
    /// The row of the statement.
    type Row;

    /// Reads a row of the statement.
    ///
    /// # Errors
    ///
    /// Returns the error of a column that doesn't read as its field's type.
    fn read(row: &Row<'_>) -> SqliteResult<Self::Row>;
}

/// A generated read, with the scope plan the compiler built of it.
pub trait Read: Returning {
    /// The kind of handle that runs the read: `sqlive_reads::Writer` for one
    /// the call marks `writer`, and `sqlive_reads::ReadOnly` for any other.
    type Handle;

    /// The read's scope plan, built against the app's schema and encoded
    /// (`sqlive_reads::CompiledPlan`).
    const PLAN: &'static str;

    /// The read's statement with its scope plan, which sqlive runs with no
    /// plan of its own to build.
    #[must_use]
    fn compiled() -> CompiledStatement<Self::Handle> {
        CompiledStatement::new(Self::SQL, Self::PLAN)
    }
}

/// A generated write that returns no rows.
pub trait Write: Statement {}

/// A generated write that returns rows through RETURNING.
pub trait WriteReturning: Returning {}

/// What the code the compiler writes names: the crates it reads rows with and
/// decodes JSON with, so an app lists none of them.
#[doc(hidden)]
pub mod generated {
    pub use serde;
    pub use sqlive_reads;

    /// Reads bytes that a JSON document holds as hexadecimal text.
    pub fn hex_bytes<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        hex::decode(text).map_err(serde::de::Error::custom)
    }

    /// Whether the column at `index` is NULL.
    pub fn is_null(row: &sqlive_reads::Row<'_>, index: usize) -> sqlive_reads::SqliteResult<bool> {
        Ok(row.get::<_, sqlive_reads::SqlValue>(index)? == sqlive_reads::SqlValue::Null)
    }

    /// Reads a JSON document that a column holds as text.
    pub fn json_column<T: serde::de::DeserializeOwned>(
        row: &sqlive_reads::Row<'_>,
        index: usize,
    ) -> sqlive_reads::SqliteResult<T> {
        decode_json(index, &row.get::<_, String>(index)?)
    }

    /// Reads a JSON document that a column holds as text, or `None` when the
    /// column is NULL or the document is JSON's `null`.
    pub fn optional_json_column<T: serde::de::DeserializeOwned>(
        row: &sqlive_reads::Row<'_>,
        index: usize,
    ) -> sqlive_reads::SqliteResult<Option<T>> {
        match row.get::<_, Option<String>>(index)? {
            Some(text) => decode_json(index, &text),
            None => Ok(None),
        }
    }

    /// The value of the JSON document `text`, which the column at `index`
    /// holds.
    fn decode_json<T: serde::de::DeserializeOwned>(
        index: usize,
        text: &str,
    ) -> sqlive_reads::SqliteResult<T> {
        serde_json::from_str(text).map_err(|error| {
            sqlive_reads::SqliteError::FromSqlConversionFailure(
                index,
                sqlive_reads::rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
