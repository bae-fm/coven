use super::*;

/// One operation scoped to the connection thread's database-wide state.
/// Callers receive only the domain operations below; the connection and its
/// retained schema services remain private to this module.
pub(crate) struct DatabaseSession<'session> {
    conn: &'session Connection,
    connection_durability: crate::connection_io::ConnectionDurability,
    #[cfg(any(test, feature = "test-utils"))]
    store_dir: &'session coven_foundation::store_dir::StoreDir,
}

impl<'session> DatabaseSession<'session> {
    pub(crate) fn new(
        conn: &'session Connection,
        connection_durability: crate::connection_io::ConnectionDurability,
        #[cfg(any(test, feature = "test-utils"))]
        store_dir: &'session coven_foundation::store_dir::StoreDir,
    ) -> Self {
        Self {
            conn,
            connection_durability,
            #[cfg(any(test, feature = "test-utils"))]
            store_dir,
        }
    }

    pub(crate) fn complete_device_join_from_pending(
        &mut self,
        pending_path: &str,
        pending_attempt: &str,
        expected_pending: &str,
        store_key: &str,
        store_payload: &str,
    ) -> Result<(), DbError> {
        crate::store::complete_device_join_from_pending_on(
            self.conn,
            self.connection_durability,
            pending_path,
            pending_attempt,
            expected_pending,
            store_key,
            store_payload,
        )
    }

    pub(crate) fn begin_device_join(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<coven_protocol::store_commit::device_join_journal::DeviceJoinJournalRecord, DbError>
    {
        crate::store::begin_device_join_on(self.conn, key, value)
    }

    pub(crate) fn advance_device_join(
        &mut self,
        key: &str,
        previous: &str,
        next: &str,
    ) -> Result<usize, DbError> {
        crate::store::advance_device_join_on(self.conn, key, previous, next)
    }

    pub(crate) fn begin_device_join_replacement_terminal(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<String, DbError> {
        crate::store::begin_device_join_replacement_terminal_on(self.conn, key, value)
    }

    pub(crate) fn device_join_records(&mut self) -> Result<Vec<(String, String)>, DbError> {
        crate::store::device_join_records_on(self.conn)
    }

    pub(crate) fn forget_device_join(&mut self, key: &str) -> Result<(), DbError> {
        crate::store::forget_device_join_on(self.conn, key)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn forget_provider_administrator_device_joins(&mut self) -> Result<(), DbError> {
        crate::store::forget_provider_administrator_device_joins_on(self.conn)
    }

    pub(crate) fn local_blob_cleanup_intents(
        &self,
    ) -> Result<
        Vec<(
            crate::local_blob_cleanup_intents::LocalBlobCleanupIntent,
            bool,
        )>,
        DbError,
    > {
        crate::store::local_blob_cleanup_intents_on(self.conn)
    }

    pub(crate) fn complete_local_blob_cleanup(
        &self,
        namespace: &str,
        blob_id: &str,
        persisted_identity: &str,
    ) -> Result<(), DbError> {
        crate::store::complete_local_blob_cleanup_on(
            self.conn,
            namespace,
            blob_id,
            persisted_identity,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        get_protocol_state_on(self.conn, key)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        set_protocol_state_on(self.conn, key, value)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn delete_protocol_state(&self, key: &str) -> Result<(), DbError> {
        delete_protocol_state_on(self.conn, key).map(|_| ())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn run_test_sql<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: for<'connection> FnOnce(DatabaseTestSql<'connection>) -> Result<R, DbError>,
    {
        operation(DatabaseTestSql::for_store(self.conn, self.store_dir))
    }

    #[cfg(test)]
    pub(crate) fn select_one_after_delay(
        &self,
        delay: std::time::Duration,
    ) -> Result<i64, DbError> {
        std::thread::sleep(delay);
        self.conn
            .query_row("SELECT 1", [], |row| row.get(0))
            .map_err(DbError::from)
    }
}
