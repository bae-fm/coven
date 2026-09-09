use super::coven_schema::DatabaseTestTable;
use super::{Connection, DbError};

mod circle_fixture;
mod database;
mod database_capabilities;
mod image;
pub mod synthetic_store;

pub use image::DatabaseImageTest;

#[derive(Clone, Copy)]
pub enum RetainedRegistrationTamper {
    CanonicalRegistration,
    ActivationAuthority,
}

pub type OutboxAttempt = (i64, Option<String>, Option<String>);

#[derive(Debug, PartialEq, Eq)]
pub struct ScopedRoutingStateForTest {
    pub row: Option<(Option<String>, String, String)>,
    pub route: Option<(String, String)>,
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
