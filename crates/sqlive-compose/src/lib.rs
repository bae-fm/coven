//! Composes an app's reads from SQL fragments while the app compiles
//! (`SPEC.md`). `fragment!` and `read!` are the macros an app writes; the `compiler` feature adds
//! the compiler its build script runs.

#[cfg(feature = "compiler")]
pub mod compiler;

pub use sqlive_compose_macros::{fragment, read};

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
        let text: String = row.get(index)?;
        serde_json::from_str(&text).map_err(|error| {
            sqlive_reads::SqliteError::FromSqlConversionFailure(
                index,
                sqlive_reads::rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    }
}
