use std::path::Path;

use crate::database::Gates;
use crate::database::{CloudOutboxRecords, Database, DbError, ExternalBlobRecords};
use crate::{Provenance, SyncedTable};
use coven_protocol::hlc::UpdatedAtStamper;

/// Host SQL against Coven's retained database connection.
///
/// The connection remains private. This context exposes query operations while
/// preventing callers from changing connection configuration or starting an
/// independent transaction.
///
/// ```compile_fail
/// fn cannot_write(sql: coven::SqlReadContext<'_>) {
///     sql.execute("DELETE FROM notes", []);
/// }
/// ```
pub struct SqlReadContext<'connection> {
    connection: &'connection rusqlite::Connection,
}

impl<'connection> SqlReadContext<'connection> {
    pub(super) fn new(connection: &'connection rusqlite::Connection) -> Self {
        Self { connection }
    }

    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.connection.query_row(sql, params, map)
    }

    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.connection.prepare(sql)?;
        let values = statement.query_map(params, map)?.collect();
        values
    }
}

/// Host SQL inside one journaled write transaction.
///
/// The underlying transaction remains private so host SQL cannot remove
/// Coven's authorizer or address Coven-owned attached schemas.
pub struct SqlContext<'context, 'connection> {
    transaction: &'context rusqlite::Transaction<'connection>,
    stamper: UpdatedAtStamper,
    tables: &'context [SyncedTable],
    gates: &'context Gates,
}

impl<'context, 'connection> SqlContext<'context, 'connection> {
    pub(super) fn new(
        transaction: &'context rusqlite::Transaction<'connection>,
        stamper: UpdatedAtStamper,
        tables: &'context [SyncedTable],
        gates: &'context Gates,
    ) -> Self {
        Self {
            transaction,
            stamper,
            tables,
            gates,
        }
    }

    fn blob_table(&self, table: &str) -> Result<&SyncedTable, DbError> {
        let declared = self
            .tables
            .iter()
            .find(|candidate| candidate.name() == table)
            .ok_or_else(|| DbError::Message(format!("undeclared synced table {table:?}")))?;
        if declared.blob().is_none() {
            return Err(DbError::Message(format!(
                "synced table {table:?} has no blob declaration"
            )));
        }
        Ok(declared)
    }

    pub fn execute<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.transaction.execute(sql, params)
    }

    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.transaction.execute_batch(sql)
    }

    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.transaction.query_row(sql, params, map)
    }

    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.transaction.prepare(sql)?;
        let values = statement.query_map(params, map)?.collect();
        values
    }

    pub fn stamp(&self) -> String {
        self.stamper.stamp()
    }

    pub fn register_external_blob(
        &self,
        table: &str,
        row_id: &str,
        path: &Path,
    ) -> Result<(), DbError> {
        crate::database::with_coven_sql_authority(|| {
            let declared = self.blob_table(table)?;
            let reference =
                Database::row_blob_ref_on(self.transaction, self.gates, declared, row_id)?;
            if reference.blob().provenance != Provenance::UserProvided {
                return Err(DbError::Message(format!(
                    "table {table:?} declares host-provided blobs, which Coven copies; \
                     an external file registration on it would never be read"
                )));
            }
            ExternalBlobRecords::new(self.transaction).register(&reference, path)
        })
    }

    pub fn enqueue_blob_delete(&self, blob: &crate::RowBlobRef) -> Result<(), DbError> {
        let stored = blob.stored().ok_or_else(|| {
            DbError::Message(format!(
                "blob {:?} in {:?} has no cloud object to remove",
                blob.blob().id,
                blob.blob().namespace
            ))
        })?;
        crate::database::with_coven_sql_authority(|| {
            CloudOutboxRecords::new(self.transaction).enqueue_delete(stored, &self.stamp())
        })
    }

    pub fn clear_external_blob(&self, table: &str, row_id: &str) -> Result<(), DbError> {
        crate::database::with_coven_sql_authority(|| {
            let declared = self.blob_table(table)?;
            let reference =
                Database::row_blob_ref_on(self.transaction, self.gates, declared, row_id)?;
            ExternalBlobRecords::new(self.transaction).clear(&reference)
        })
    }
}
