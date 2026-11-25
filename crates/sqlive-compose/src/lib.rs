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
    pub use hex;
    pub use serde;
    pub use serde_json;
    pub use sqlive;

    /// Reads a JSON document that a column holds as text.
    pub fn json_column<T: serde::de::DeserializeOwned>(
        row: &sqlive::Row<'_>,
        index: usize,
    ) -> sqlive::SqliteResult<T> {
        let text: String = row.get(index)?;
        serde_json::from_str(&text).map_err(|error| {
            sqlive::SqliteError::FromSqlConversionFailure(
                index,
                sqlive::rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    }
}
