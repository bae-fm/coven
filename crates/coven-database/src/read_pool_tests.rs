use super::*;

impl ReadPool {
    /// Visit each reader in turn, under its own lock.
    pub(crate) fn each(
        &self,
        mut visit: impl FnMut(&DatabaseConnection) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        self.readers
            .iter()
            .try_for_each(|reader| crate::database::with_locked(reader, |reader| visit(reader)))
    }

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

    pub(crate) fn inspect_each(&self, inspect: impl Fn(&DatabaseConnection)) {
        self.each(|reader| {
            inspect(reader);
            Ok(())
        })
        .unwrap();
    }
}
