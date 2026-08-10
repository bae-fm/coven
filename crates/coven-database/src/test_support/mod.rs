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

pub fn table_row_count(connection: &Connection, table: DatabaseTestTable) -> Result<i64, DbError> {
    connection
        .query_row(&format!("SELECT COUNT(*) FROM {}", table.0), [], |row| {
            row.get(0)
        })
        .map_err(DbError::from)
}

pub fn clear_table(connection: &Connection, table: DatabaseTestTable) -> Result<(), DbError> {
    connection
        .execute(&format!("DELETE FROM {}", table.0), [])
        .map(|_| ())
        .map_err(DbError::from)
}

pub fn author_exclusion_activation_evidence(
    connection: &Connection,
) -> Result<(String, String, String, String), DbError> {
    connection
        .query_row(
            "SELECT exclusion_ref, accepted_cut, activation_commit, activation_head
             FROM store_author_exclusion_activations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(DbError::from)
}
