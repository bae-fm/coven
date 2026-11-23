//! What a test reaches into the database through: hooks that stall or
//! observe its work, and the connections it holds.

use super::*;
use crate::sqlite::StoreSql;
use crate::store_call::StoreCall;

/// The removal of a snapshot staging its caller abandoned.
pub(crate) type StagingRemoval = coven_foundation::task::CompletingTask<Result<(), DbError>>;

impl StoreDatabase {
    /// Keep each notice the store sends from now on for a test to inspect.
    pub(crate) fn collect_commit_notices(&self) -> sqlive::CommitSubscription {
        let notices = self.observer.subscribe();
        notices.begin();
        notices
    }

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

    /// Run `gate` once, where the next read of a file on this device only
    /// has found where its bytes are and is about to open them.
    pub(crate) async fn test_gate_local_open(&self, gate: impl FnOnce() + Send + 'static) {
        self.call(move |inner| {
            *lock_unpoisoned(&inner.access.local_open_gate) = Some(Box::new(gate));
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

impl StoreDatabase {
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

impl StoreDatabase {
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
        inner.as_ref().unwrap().access.database.observe_next_reader_wait()
    }

    pub(crate) fn commit_writer<T>(&self, run: impl FnOnce(&StoreSql, &StoreCall<'_>) -> T) -> T {
        self.inspect_writer(|writer, call| {
            writer.transaction(|writer| Ok::<_, DbError>(run(writer, call))).unwrap()
        })
    }

    pub(crate) fn inspect_writer_schema<T>(
        &self,
        inspect: impl FnOnce(&mut StoreSql, &StoreCall<'_>, &crate::write_schema::WriteSchema) -> T,
    ) -> T {
        let slot = self.inner.read().unwrap();
        let access = &slot.as_ref().unwrap().access;
        access.inspect_writer(|writer, call| inspect(writer, call, &access.write_schema))
    }

    pub(crate) fn inspect_writer<T>(
        &self,
        inspect: impl FnOnce(&mut StoreSql, &StoreCall<'_>) -> T,
    ) -> T {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().access.inspect_writer(inspect)
    }

    pub(crate) fn inspect_readers(&self, inspect: impl Fn(&mut StoreSql, &StoreCall<'_>)) {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().access.inspect_readers(inspect);
    }

    /// Every connection the store holds open, one at a time, each under its
    /// own lock: the input database's, then the store database's readers,
    /// checkpointer, planner statistics' and writer.
    pub(crate) fn inspect_connections(&self, mut inspect: impl FnMut(&StoreSql)) {
        let slot = self.inner.read().unwrap();
        let inner = slot.as_ref().unwrap();
        inner.inputs.each_connection(&mut inspect);
        inner.access.database.each_connection(&mut inspect);
    }

    /// The store's database, under the open store's lock.
    fn with_store_database<T>(&self, run: impl FnOnce(&sqlive::Database<CovenPolicy>) -> T) -> T {
        let slot = self.inner.read().unwrap();
        run(&slot.as_ref().unwrap().access.database)
    }

    /// What each checkpoint of the store's database does at its last step
    /// from now on, told while the checkpoint still holds the writer (§4).
    pub(crate) fn report_checkpoints(
        &self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<crate::WalCheckpoint> {
        self.with_store_database(|database| database.report_checkpoints())
    }

    /// Each run of the planner's statistics from now on, told once it
    /// committed (§4).
    pub(crate) fn report_statistics_runs(&self) -> tokio::sync::mpsc::UnboundedReceiver<()> {
        self.with_store_database(|database| database.report_statistics_runs())
    }
}

impl StoreDatabase<ReadOnly> {
    pub(crate) fn inspect_readers(&self, inspect: impl Fn(&mut StoreSql, &StoreCall<'_>)) {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().inspect_readers(inspect);
    }

    pub(crate) fn inspect_writer<T>(
        &self,
        inspect: impl FnOnce(&mut StoreSql, &StoreCall<'_>) -> T,
    ) -> T {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().inspect_writer(inspect)
    }
}

impl<L> DatabaseAccess<L> {
    /// Run `inspect` on the writer, as a call on it would see it.
    fn inspect_writer<T>(&self, inspect: impl FnOnce(&mut StoreSql, &StoreCall<'_>) -> T) -> T {
        self.database.host(|writer| {
            let device = writer.observes_commits().then_some(self.device);
            inspect(writer, &crate::store_call::StoreCall::new(device, &self.tests))
        })
    }

    /// Run `inspect` on each reader in turn, as a call on it would see it.
    fn inspect_readers(&self, inspect: impl Fn(&mut StoreSql, &StoreCall<'_>)) {
        self.database.each_reader(|reader| {
            inspect(reader, &crate::store_call::StoreCall::new(None, &self.tests));
        });
    }
}
