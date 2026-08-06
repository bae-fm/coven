use super::DbError;
use rusqlite::OptionalExtension;

/// Test-only access to SQL inside a transaction owned by the database layer.
///
/// The transaction cannot escape, and commit or rollback remains the enclosing
/// database operation's responsibility.
pub(crate) struct DatabaseTestTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
}

impl DatabaseTestTransaction<'_, '_> {
    pub(super) fn new<'transaction, 'connection>(
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

    pub(crate) fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.transaction.execute_batch(sql)
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

    pub(crate) fn enqueue_blob_upload(
        &self,
        root_table: &str,
        root_id: &str,
        row: &crate::protocol::blob::RowBlobRef,
        source_path: &std::path::Path,
        retain_pinned: bool,
        created_at: &str,
    ) -> Result<(), DbError> {
        crate::database::CloudOutboxRecords::new(self.transaction).enqueue_upload(
            root_table,
            root_id,
            row,
            source_path,
            retain_pinned,
            created_at,
        )
    }

    pub(crate) fn remove_retained_replay_ownership_from_snapshot(&self) -> Result<(), DbError> {
        crate::database::StoreDatabase::remove_retained_replay_ownership_from_snapshot_on(
            self.transaction,
        )
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

    pub(crate) fn materialized_sequence(&self, stream_id: &str) -> Result<Option<u64>, DbError> {
        self.transaction
            .query_row(
                "SELECT seq FROM materialized_commits WHERE device_id = ?1",
                [stream_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|sequence| {
                u64::try_from(sequence).map_err(|error| DbError::context("invalid sequence", error))
            })
            .transpose()
    }
}
