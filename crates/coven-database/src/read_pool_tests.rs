use super::*;

impl ReadPool {
    pub(crate) fn observe_next_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        assert!(self.waiting.lock().unwrap().replace(sender).is_none());
        receiver
    }

    pub(crate) fn assert_read_only(&self) {
        for reader in &self.readers {
            let error = reader
                .lock()
                .unwrap()
                .internal_execute("INSERT INTO notes VALUES ('x', 'y', '')", [])
                .unwrap_err();
            assert!(
                matches!(error, DbError::Sqlite(rusqlite::Error::SqliteFailure(code, _))
                if code.code == rusqlite::ErrorCode::ReadOnly)
            );
        }
    }

    pub(crate) fn integrity_checks(&self) -> usize {
        self.readers
            .iter()
            .map(|r| r.lock().unwrap().integrity_checks())
            .sum()
    }
}
