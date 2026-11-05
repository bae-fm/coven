//! What a test reaches into the database through: hooks that stall or
//! observe its work, and the connections it holds.

use super::*;
use crate::sqlite::DatabaseConnection;

/// The removal of a snapshot staging its caller abandoned.
pub(crate) type StagingRemoval = coven_foundation::task::CompletingTask<Result<(), DbError>>;

impl Database {
    /// Run `gate` where released files' bytes are about to go, after the
    /// writer is free, so a test can stall that disk work.
    pub(crate) async fn test_gate_file_removal(&self, gate: impl Fn() + Send + Sync + 'static) {
        let gate: Arc<dyn Fn() + Send + Sync> = Arc::new(gate);
        self.call(move |inner| {
            *lock_unpoisoned(&inner.removal_gate) = Some(gate);
            Ok::<_, DbError>(())
        })
        .await
        .unwrap();
    }

    /// Run `hook` where each write's staged files have been flushed, before
    /// its commit; an error from it fails the flush.
    pub(crate) async fn test_on_file_flush(&self, hook: coven_foundation::files::FlushHook) {
        self.call(move |inner| {
            *lock_unpoisoned(&inner.flush_hook) = Some(hook);
            Ok::<_, DbError>(())
        })
        .await
        .unwrap();
    }
}

impl Database {
    /// Tell a test waiting for it that this close finds a call holding the
    /// open store, and waits for it to end.
    pub(crate) fn tell_close_wait(&self) {
        if matches!(self.inner.try_write(), Err(std::sync::TryLockError::WouldBlock)) {
            if let Some(waits) = lock_unpoisoned(&self.writer.close_waits).take() {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "a test that stopped listening has no use for it"
                )]
                let _ = waits.send(());
            }
        }
    }
}

impl Database {
    /// Wait until the removals of every snapshot staging abandoned so far
    /// have ended.
    pub(crate) async fn abandoned_stagings_removed(&self) {
        let removing = std::mem::take(&mut *self.writer.abandoned_stagings.lock().unwrap());
        for removal in removing {
            removal.await.unwrap().unwrap();
        }
    }

    /// Told when a close finds a call holding the open store and waits for
    /// it to end.
    pub(crate) fn observe_close_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        assert!(self.writer.close_waits.lock().unwrap().replace(sender).is_none());
        receiver
    }

    pub(crate) fn observe_next_reader_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().access.readers.observe_next_wait()
    }

    pub(crate) fn commit_writer<T>(&self, run: impl FnOnce(&DatabaseConnection) -> T) -> T {
        self.inspect_writer(|writer| writer.transaction(|writer| Ok(run(writer))).unwrap())
    }

    pub(crate) fn inspect_writer_schema<T>(
        &self,
        inspect: impl FnOnce(&mut DatabaseConnection, &crate::write_schema::WriteSchema) -> T,
    ) -> T {
        let slot = self.inner.read().unwrap();
        let inner = slot.as_ref().unwrap();
        let mut writer = inner.access.writer.lock().unwrap();
        inspect(&mut writer, &inner.access.write_schema)
    }

    pub(crate) fn inspect_writer<T>(
        &self,
        inspect: impl FnOnce(&mut DatabaseConnection) -> T,
    ) -> T {
        let inner = self.inner.read().unwrap();
        let mut writer = inner.as_ref().unwrap().access.writer.lock().unwrap();
        inspect(&mut writer)
    }

    pub(crate) fn inspect_readers(&self, inspect: impl Fn(&mut DatabaseConnection)) {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().access.readers.inspect_each(inspect);
    }

    /// Every connection the store holds open, one at a time, each under its
    /// own lock: the readers, the input database's, the checkpointers' and
    /// the writer.
    pub(crate) fn inspect_connections(&self, mut inspect: impl FnMut(&DatabaseConnection)) {
        let slot = self.inner.read().unwrap();
        let inner = slot.as_ref().unwrap();
        let mut visit = |connection: &DatabaseConnection| {
            inspect(connection);
            Ok(())
        };
        inner.access.readers.each(|reader| visit(reader)).unwrap();
        inner.inputs.each_connection(&mut visit).unwrap();
        for checkpoints in &inner.checkpoints {
            checkpoints.with_connection(&mut visit).unwrap();
        }
        with_locked(&inner.access.writer, |writer| visit(writer)).unwrap();
    }

    /// Run `fault` as each checkpoint of the store's database, not its input
    /// database's, takes the writer (§4).
    pub(crate) fn fault_checkpoints(
        &self,
        fault: impl FnMut() -> Result<(), DbError> + Send + 'static,
    ) {
        let slot = self.inner.read().unwrap();
        let checkpointers = &slot.as_ref().unwrap().checkpoints;
        assert_eq!(checkpointers.len(), 2, "the store's database has a checkpointer");
        checkpointers[1].fault(fault);
    }

    /// What each checkpoint of the store's database does at its last step
    /// from now on, told while the checkpoint still holds the writer (§4).
    pub(crate) fn report_checkpoints(
        &self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<crate::WalCheckpoint> {
        let slot = self.inner.read().unwrap();
        let checkpointers = &slot.as_ref().unwrap().checkpoints;
        assert_eq!(checkpointers.len(), 2, "the store's database has a checkpointer");
        checkpointers[1].report_restarts()
    }

    /// Whether coven checkpoints the store's database, not only its input
    /// database (§4).
    pub(crate) fn checkpoints_its_database(&self) -> bool {
        let slot = self.inner.read().unwrap();
        slot.as_ref().unwrap().checkpoints.len() == 2
    }

    /// Run one checkpoint of the store's database now, on its checkpointer's
    /// connection, as a commit's wake does (§4).
    pub(crate) fn checkpoint_now(&self) -> Result<(), DbError> {
        let slot = self.inner.read().unwrap();
        let checkpointers = &slot.as_ref().unwrap().checkpoints;
        assert_eq!(checkpointers.len(), 2, "the store's database has a checkpointer");
        checkpointers[1].run_now()
    }
}

impl Database<ReadOnly> {
    pub(crate) fn inspect_readers(&self, inspect: impl Fn(&mut DatabaseConnection)) {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().readers.inspect_each(inspect);
    }

    pub(crate) fn inspect_writer<T>(
        &self,
        inspect: impl FnOnce(&mut DatabaseConnection) -> T,
    ) -> T {
        let inner = self.inner.read().unwrap();
        let mut writer = inner.as_ref().unwrap().writer.lock().unwrap();
        inspect(&mut writer)
    }
}
