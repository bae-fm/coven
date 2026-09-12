use super::coven_schema::DatabaseTestTable;
use super::{Connection, DbError};

mod circle_fixture;
mod database;
mod database_capabilities;
mod image;
pub mod synthetic_store;

pub use image::DatabaseImageTest;

/// Every change a Circle bootstrap payload states: `(table, operation,
/// primary key)`, in payload order. Operations read as `insert`, `update` or
/// `delete`.
pub fn circle_bootstrap_changes_for_test(
    rows: &[u8],
) -> Result<Vec<(String, String, String)>, DbError> {
    Ok(crate::store::changeset_rows(rows)
        .map_err(DbError::from)?
        .into_iter()
        .map(|change| {
            let operation = match change.op {
                rusqlite::hooks::Action::SQLITE_INSERT => "insert",
                rusqlite::hooks::Action::SQLITE_UPDATE => "update",
                rusqlite::hooks::Action::SQLITE_DELETE => "delete",
                _ => "unknown",
            };
            (change.table, operation.to_string(), change.row_id)
        })
        .collect())
}

#[derive(Clone, Copy)]
pub enum RetainedRegistrationTamper {
    CanonicalRegistration,
    ActivationAuthority,
}

pub type OutboxAttempt = (i64, Option<String>, Option<String>);

#[derive(Debug, PartialEq, Eq)]
pub struct ScopedRoutingStateForTest {
    pub row: Option<(Option<String>, String, String)>,
    pub mirror: Option<(Option<String>, String)>,
}

pub(crate) fn table_row_count(
    connection: &Connection,
    table: DatabaseTestTable,
) -> Result<i64, DbError> {
    connection
        .query_row(&format!("SELECT COUNT(*) FROM {}", table.0), [], |row| {
            row.get(0)
        })
        .map_err(DbError::from)
}

pub(crate) fn clear_table(
    connection: &Connection,
    table: DatabaseTestTable,
) -> Result<(), DbError> {
    connection
        .execute(&format!("DELETE FROM {}", table.0), [])
        .map(|_| ())
        .map_err(DbError::from)
}
