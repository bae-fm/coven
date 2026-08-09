use super::*;
use crate::payload_spool::{StoreRecordTransaction, StoreRecords};

/// A replay-owned SQLite image. Callers can apply or inspect the projection,
/// but cannot obtain the connection that implements it.
pub(super) struct ReplayProjection {
    pub(super) connection: rusqlite::Connection,
    pub(super) store_dir: coven_foundation::store_dir::StoreDir,
}

impl ReplayProjection {
    pub(super) fn replace_tables_on(
        &self,
        target: StoreRecordTransaction<'_, '_>,
        tables: &[String],
    ) -> Result<(), DbError> {
        for table in tables {
            crate::copy_table_with_conflicts(&self.connection, target.transaction, table, false)?;
        }
        Ok(())
    }

    pub(super) fn materialized_frontier(
        &self,
    ) -> Result<coven_protocol::store_commit::CommitFrontier, DbError> {
        coven_protocol::store_commit::CommitFrontier::from_refs(
            StoreDatabase::materialized_frontier_on(&self.connection, None)?,
        )
        .map_err(|error| DbError::Message(error.to_string()))
    }

    pub(super) fn capture_snapshot(
        &self,
        image: crate::SnapshotDatabaseImage,
        root: &coven_protocol::store_commit::StoreRootRef,
        tables: &[coven_protocol::synced_schema::SyncedTable],
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        audience: &coven_protocol::circle::Audience,
    ) -> Result<crate::CreatedSnapshot, crate::SnapshotImageError> {
        image.capture(
            StoreRecords::new(&self.connection, &self.store_dir),
            root,
            tables,
            routing_encryption,
            audience,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn document_count(&self, id: &str) -> Result<i64, DbError> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM documents WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }
}
