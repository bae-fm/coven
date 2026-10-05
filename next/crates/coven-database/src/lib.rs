//! The store's SQLite database, protected internal tables and app migrations.
//!
//! [`DatabaseBuilder`] opens a store with one locked writer and read-only
//! connections, or with read-only connections alone. App SQL runs inside the
//! borrowed [`MigrationContext`]; no raw connection leaves its owner.

mod authorization;
mod database;
mod declaration;
mod error;
mod internal_schema;
mod migration;
mod schema;
mod sql;
mod sqlite;

pub use database::{Database, DatabaseBuilder};
pub use declaration::{CacheFill, FileDecl, Provenance, RowIdentity, SyncedTable, Uploads};
pub use error::{
    CovenError, CovenMigrationError, CovenResult, DbError, MigrationError, SchemaError,
};
pub use migration::{
    CovenMigrationPolicy, Migration, MigrationChange, MigrationContext, MigrationOutcome,
};

// SQL values and parameters are part of the API. Connections remain private.
pub use rusqlite::{named_params, params, types, Params, Row, ToSql};

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
