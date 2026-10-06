//! The store's SQLite database, protected internal tables and app migrations.
//!
//! [`DatabaseBuilder`] opens a store with one locked writer and read-only
//! connections, or with read-only connections alone. App SQL runs inside the
//! borrowed [`MigrationContext`] or [`SqlContext`]; no raw connection leaves its
//! owner. A local write commits its app rows, unsigned record and merge metadata
//! together on the writer connection.

mod authorization;
mod database;
mod declaration;
mod error;
mod internal_schema;
mod merge_store;
mod migration;
mod removal;
mod removal_schema;
mod removal_sql;
mod removal_view;
mod row_key;
mod row_queries;
mod schema;
mod schema_source;
mod sql;
mod sqlite;
mod write;
mod write_capture;
mod write_commit;
mod write_encoding;
mod write_record;
mod write_rows;
mod write_schema;

pub use database::{CovenReadHandle, Database, DatabaseBuilder};
pub use declaration::{CacheFill, FileDecl, Provenance, RowIdentity, SyncedTable, Uploads};
pub use error::{
    CovenError, CovenMigrationError, CovenResult, DbError, MigrationError, SchemaError,
};
pub use migration::{
    CovenMigrationPolicy, Migration, MigrationChange, MigrationContext, MigrationOutcome,
};
pub use row_key::RowKey;
pub use write::SqlContext;

// SQL values and parameters are part of the API. Connections remain private.
pub use rusqlite::{named_params, params, types, Params, Row, ToSql};

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
