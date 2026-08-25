use super::DbError;

/// Test-only access to SQL inside a transaction owned by the database layer.
///
/// The transaction cannot escape, and commit or rollback remains the enclosing
/// database operation's responsibility.
pub(crate) struct DatabaseTestTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
}

impl DatabaseTestTransaction<'_, '_> {
    pub(crate) fn new<'transaction, 'connection>(
        transaction: &'transaction rusqlite::Transaction<'connection>,
    ) -> DatabaseTestTransaction<'transaction, 'connection> {
        DatabaseTestTransaction { transaction }
    }

    pub(crate) fn execute<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.transaction.execute(sql, params)
    }

    pub(crate) fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.transaction.query_row(sql, params, map)
    }

    pub(crate) fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: rusqlite::Params,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.transaction.prepare(sql)?;
        let values = statement.query_map(params, map)?.collect();
        values
    }

    pub(crate) fn defer_foreign_keys(&self) -> rusqlite::Result<()> {
        self.transaction
            .pragma_update(None, "defer_foreign_keys", true)
    }

    pub(crate) fn set_payload_owner_claims(
        &self,
        owner_key: &str,
        payloads: &std::collections::BTreeSet<coven_protocol::store_commit::ObjectHash>,
    ) -> Result<(), DbError> {
        crate::payload_store::set_payload_owner_claims_on(self.transaction, owner_key, payloads)
    }

    pub(crate) fn insert_store_reclaim_operation(
        &self,
        operation: &crate::DurableStoreReclaimOperation,
    ) -> Result<(), DbError> {
        crate::insert_store_reclaim_operation_on(self.transaction, operation)
    }

    pub(crate) fn record_reclaimed_store_package(
        &self,
        package: &crate::ReclaimedStorePackage,
    ) -> Result<(), DbError> {
        crate::record_reclaimed_store_package_on(self.transaction, None, package)
    }

    pub(crate) fn persist_prepared_audience_objects(
        &self,
        store_dir: &coven_foundation::store_dir::StoreDir,
        write_id: &coven_protocol::write::WriteId,
        packages: &[crate::PreparedAudiencePackage],
        blobs: &[crate::PreparedAudienceBlob],
    ) -> Result<(), DbError> {
        crate::store::persist_prepared_audience_objects_on(
            self.transaction,
            store_dir,
            write_id,
            packages,
            blobs,
        )
    }

    #[cfg(test)]
    pub(crate) fn retire_circle_bootstrap_coverage(
        &self,
        store_dir: &coven_foundation::store_dir::StoreDir,
        activation: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<usize, DbError> {
        crate::store::test_retire_circle_bootstrap_coverage(self.transaction, store_dir, activation)
    }

    pub(crate) fn enqueue_blob_upload(
        &self,
        root_table: &str,
        root_id: &str,
        root_label: &str,
        row: &coven_protocol::blob::RowBlobRef,
        source_path: &std::path::Path,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        crate::CloudOutboxRecords::new(self.transaction).enqueue_upload(
            root_table,
            root_id,
            root_label,
            row,
            source_path,
            retain_pinned,
            created_at,
        )
    }

    pub(crate) fn remove_retained_replay_ownership_from_snapshot(&self) -> Result<(), DbError> {
        crate::store::remove_retained_replay_ownership_from_snapshot_on(self.transaction)
    }

    pub(crate) fn delete_materialized_commit(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<(), DbError> {
        let sequence =
            i64::try_from(sequence).map_err(|error| DbError::context("invalid sequence", error))?;
        self.transaction
            .execute(
                "DELETE FROM materialized_commits WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![stream_id, sequence],
            )
            .map(|_| ())
            .map_err(DbError::from)
    }
}
