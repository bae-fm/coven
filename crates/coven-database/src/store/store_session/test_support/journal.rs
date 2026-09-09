use super::{DbError, StoreDatabase, StoreSession, WriteId};
use coven_protocol::write::WriteStatus;

impl StoreDatabase {
    pub async fn store_write_journal_for_test(&self) -> Result<String, DbError> {
        self.call_store(|session| session.store_write_journal_for_test())
            .await
    }

    /// Read the original capture bytes independently of replacement preparation.
    pub async fn store_write_capture_for_test(
        &self,
        write_id: WriteId,
    ) -> Result<(String, String, String), DbError> {
        self.call_store(move |session| session.store_write_capture_for_test(&write_id))
            .await
    }

    pub async fn store_write_status_count_for_test(
        &self,
        status: WriteStatus,
    ) -> Result<i64, DbError> {
        self.call_store(move |session| session.store_write_status_count_for_test(&status))
            .await
    }

    pub async fn has_rebased_store_writes_for_test(&self) -> Result<bool, DbError> {
        self.call_store(|session| session.has_rebased_store_writes_for_test())
            .await
    }
}

impl StoreSession<'_> {
    fn store_write_journal_for_test(&self) -> Result<String, DbError> {
        self.conn
            .query_row(
                "SELECT json_group_array(json_array(ordinal, write_id, status, affected_rows,
                    changeset_hash, base, blob_facts, rebased, prepared))
                 FROM (SELECT * FROM store_writes ORDER BY ordinal)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    fn store_write_capture_for_test(
        &self,
        write_id: &WriteId,
    ) -> Result<(String, String, String), DbError> {
        self.conn
            .query_row(
                "SELECT base, changeset_hash, blob_facts FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    fn store_write_status_count_for_test(&self, status: &WriteStatus) -> Result<i64, DbError> {
        let status = serde_json::to_string(status)?;
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM store_writes WHERE status = ?1",
                [status],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    fn has_rebased_store_writes_for_test(&self) -> Result<bool, DbError> {
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM store_writes WHERE rebased IS NOT NULL)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }
}
