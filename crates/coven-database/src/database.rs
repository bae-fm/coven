//! Opening is the composition root; a shared handle owns all connections and
//! the writer lock. Blocking calls retain these resources after its last owner
//! drops, while another open waits for their release.

use coven_foundation::locks::{lock_unpoisoned, read_unpoisoned, write_unpoisoned};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

#[path = "file_database.rs"]
pub(crate) mod file_database;

#[path = "file_staging.rs"]
mod file_staging;
pub use file_staging::StagedFiles;
#[path = "read_pool.rs"]
mod read_pool;
use read_pool::ReadPool;

#[path = "input_database.rs"]
pub(crate) mod input_database;

#[path = "database_operations.rs"]
mod operations;
#[path = "database_provider_access.rs"]
mod provider_access;

#[path = "database_file_retention.rs"]
pub(crate) mod file_retention;
#[path = "database_snapshots.rs"]
mod snapshots;

#[cfg(any(test, feature = "test-utils"))]
#[path = "database_operations_tests.rs"]
pub(crate) mod operation_tests;

use coven_foundation::clock::{ClockRef, SystemClock};
use coven_foundation::files::{StoreDir, StoreLock, StoreReadLock};
use coven_foundation::id_source::FileId;
use coven_foundation::id_source::{DeviceId, IdSourceRef, UuidIds};

use crate::authorization::SqlAuthorization;
use crate::observation::CommitObserver;
use crate::read::RunningRead;
use crate::sqlite::DatabaseConnection;
use crate::{CovenError, CovenResult, DbError, Migration, MigrationOutcome, SyncedTable};
use crate::{LiveQuery, LostValue, Read, ReconfigurableLiveQuery, SqlReadContext};

#[path = "database_builder.rs"]
mod builder;
#[path = "database_store.rs"]
mod store;

#[path = "database_retry.rs"]
mod retry;
#[path = "database_sync.rs"]
mod sync;
pub use builder::{discarded_operations, discarded_store_name, read_store_name, DatabaseBuilder};

/// Shared ownership of the database. Closing any clone closes them all.
#[derive(Clone)]
pub struct Database {
    /// Where every connection reports its commits (§4); it outlives them, and
    /// once they close makes only closed subscriptions.
    observer: CommitObserver,
    /// The files this process is sending now, which coven's views read (§4).
    transfers: crate::transfers_in_flight::TransfersInFlight,
    /// The device's network as this process last heard of it, which
    /// coven's views read (§4).
    network: crate::network_facts::NetworkFacts,
    /// Where a bug in the store's work that nobody waits for goes (E15).
    bugs: coven_foundation::task::BugSinkRef,
    closing: Arc<coven_foundation::files::StoreClosingGuard>,
    /// The open store, `None` once closed. Closing takes it before anything
    /// that can panic, so a panic while it is written leaves it `None`, a
    /// whole state: the lock is taken as it is (E15).
    inner: Arc<RwLock<Option<DatabaseInner>>>,
    file_tasks: Arc<tokio::sync::RwLock<()>>,
}

struct DatabaseInner {
    /// Coven's checkpointers of the input database and, unless the app
    /// checkpoints it, the store's (§4); stopped before their writers close.
    checkpoints: Vec<crate::wal_checkpoints::WalCheckpointer>,
    inputs: input_database::InputDatabase,
    access: DatabaseAccess<StoreLock>,
    migrations: Vec<MigrationOutcome>,
    clock: ClockRef,
    ids: IdSourceRef,
    /// The names of copies being written, or written and waiting for the
    /// write that names them, which no write's removal takes (Appendix C.3):
    /// staged app bytes and cache files. Shared with what holds them, so
    /// letting one go never takes the store's own lock. Each change under
    /// its lock, and `copying`'s, is whole before it can panic, so both are
    /// taken as they are (E15).
    staging: Arc<Mutex<BTreeSet<coven_foundation::files::FileName>>>,
    /// The paths of the copies the calls making files device-only are
    /// writing now, which no other write's removal takes (§17.3).
    copying: Arc<Mutex<BTreeSet<String>>>,
    /// Runs where released files' bytes are about to go, after the writer is
    /// free, so a test can stall that disk work.
    #[cfg(test)]
    removal_gate: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Runs where a write's staged files have been flushed, before its
    /// commit, so a test can see the order or fail the flush.
    #[cfg(test)]
    flush_hook: Mutex<Option<coven_foundation::files::FlushHook>>,
    /// Fail this many coming `end_retry_waits` transactions with a disk
    /// error, so a test can show that a failed end leaves every wait.
    #[cfg(any(test, feature = "test-utils"))]
    retry_end_failures: std::sync::atomic::AtomicUsize,
}

// The lock outlives every connection, including when the last handle is dropped.
struct DatabaseAccess<L> {
    /// Shared with the store's checkpointer, which holds it while the WAL
    /// starts over (§4).
    writer: Arc<Mutex<DatabaseConnection>>,
    readers: ReadPool,
    // Every connection reports its commits and handle failures here (§4).
    observer: CommitObserver,
    write_schema: crate::write_schema::WriteSchema,
    directory: StoreDir,
    device: DeviceId,
    lock: L,
}

impl<L> DatabaseAccess<L> {
    /// Refuse calls once a connection failed the handle. A failing call keeps
    /// its own error; one that succeeds while the handle fails reports it.
    /// So does one that fails as a bug fails the handle: its own error, such
    /// as a statement the broken authorizer refused, says less (E15).
    fn call<R, E: From<DbError>>(&self, run: impl FnOnce() -> Result<R, E>) -> Result<R, E> {
        if let Some(error) = self.observer.error() {
            return Err(error.into());
        }
        let result = run();
        match (result, self.observer.error()) {
            (Ok(result), None) => Ok(result),
            (Ok(_), Some(error)) | (Err(_), Some(error @ DbError::Bug(_))) => Err(error.into()),
            (Err(error), _) => Err(error),
        }
    }

    /// Read one committed state on a reader: read-only decisions never take
    /// the writer (§21.2).
    fn with_committed<R, E: From<DbError>>(
        &self,
        run: impl FnOnce(&DatabaseConnection) -> Result<R, E>,
    ) -> Result<R, E> {
        self.call(|| {
            self.readers
                .acquire_reader()
                .with_reader(|reader| reader.read_transaction(|| run(reader)))
        })
    }

    fn with_writer<R, E: From<DbError>>(
        &self,
        run: impl FnOnce(&mut DatabaseConnection) -> Result<R, E>,
    ) -> Result<R, E> {
        with_locked(&self.writer, |writer| {
            #[cfg(any(test, feature = "test-utils"))]
            writer.record_work(|work| work.writer_holds += 1);
            if let Some(error) = self.observer.error() {
                return Err(error.into());
            }
            #[cfg(any(test, feature = "test-utils"))]
            let run = |writer| coven_foundation::test_work::writing(|| run(writer));
            run(writer)
        })
    }

    /// Remove released files' bytes now that the writer is free, then forget
    /// their records under it (§21.2, Appendix C.3). `chosen` is what the writer
    /// that recorded them chose.
    fn remove_released(&self, chosen: crate::file_removals::Chosen) -> Vec<DbError> {
        let released = match chosen {
            Ok(released) => released,
            Err(error) => return vec![error],
        };
        let (removed, mut failures) = crate::file_removals::remove_bytes(&self.directory, released);
        if !removed.is_empty() {
            let forgotten = self.with_writer(|writer| {
                writer.transaction(|db| crate::file_removals::forget(db, &removed))?;
                #[cfg(any(test, feature = "test-utils"))]
                writer.checkpoint(crate::test_utils::WriteCheckpoint::Finished);
                Ok::<_, DbError>(())
            });
            if let Err(error) = forgotten {
                failures.push(error);
            }
        }
        failures
    }

    fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        // The bytes open inside the read that found them.
        self.readers.acquire_reader().read_snapshot(|sql| {
            sql.device_only_source(self.device, reference)?
                .map(|source| {
                    crate::file_ref::open_source(
                        &self.directory,
                        &reference.id().to_string(),
                        reference.size(),
                        source,
                    )
                })
                .transpose()
        })
    }

    fn file_ref(&self, file: FileId) -> Result<crate::FileRef, DbError> {
        self.readers.acquire_reader().read_snapshot(|sql| sql.file_ref(file))
    }

    fn user_file(&self, file: FileId) -> Result<Option<crate::UserFile>, DbError> {
        self.readers.acquire_reader().read_snapshot(|sql| sql.user_file(file))
    }

    fn read<R>(&self, read: impl FnOnce(SqlReadContext<'_>) -> CovenResult<R>) -> CovenResult<R> {
        self.readers.acquire_reader().read_snapshot(read)
    }

    fn schema_version(&self) -> Result<u32, DbError> {
        self.readers.acquire_reader().schema_version()
    }

    /// Every connection has the same settings, so one reader reports them.
    fn sqlite_settings(&self) -> Result<crate::SqliteSettingsInEffect, DbError> {
        self.readers.acquire_reader().with_reader(DatabaseConnection::sqlite_settings)
    }

    fn close(self) -> Result<(), DbError> {
        self.observer.close();
        let mut failures = self.readers.close();
        // Each checkpointer stopped before, and let go of the writer.
        match Arc::try_unwrap(self.writer) {
            Ok(writer) => {
                if let Err(error) = coven_foundation::locks::into_unpoisoned(writer).close() {
                    failures.push(error);
                }
            }
            Err(_held) => failures.push(DbError::Invariant {
                what: "a writer closes after its checkpointer stopped",
            }),
        }
        drop(self.lock);
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Closing { failures })
        }
    }
}

/// Run `run` on `connection`, one of the store's SQLite connections. A
/// panic while one is held leaves it whole: its open transaction rolls back
/// as the panic unwinds (`write_transaction.rs`), and a failed rollback fails
/// the handle. So the lock is taken as it is.
pub(crate) fn with_locked<T, R>(connection: &Mutex<T>, run: impl FnOnce(&mut T) -> R) -> R {
    run(&mut lock_unpoisoned(connection))
}

impl DatabaseInner {
    fn call<R, E: From<DbError>>(&self, run: impl FnOnce(&Self) -> Result<R, E>) -> Result<R, E> {
        self.access.call(|| run(self))
    }

    // Their durable removal records remain for the next write or open.
    fn release_file_reservations<'a>(
        &self,
        names: impl IntoIterator<Item = &'a coven_foundation::files::FileName>,
    ) {
        release_reserved(&self.staging, names);
    }

    /// Run `operation`, a write, under the writer, then remove the files it
    /// released once the writer is free (Appendix C.3), and run what its
    /// steps asked to follow its commit. A panic in it fails it with
    /// `DbError::Bug` once its transaction has rolled back, and its released
    /// files go as after any failed write.
    fn with_files<R, E: crate::WriteFailure>(
        &self,
        operation: &'static str,
        run: impl FnOnce(&mut DatabaseConnection, &crate::file_write::WriteFiles<'_>) -> Result<R, E>,
    ) -> Result<R, E> {
        let (result, chosen, committed) = self.access.with_writer(|writer| {
            let files = crate::file_write::WriteFiles::new(
                &self.access.write_schema,
                self.access.device,
                self.ids.as_ref(),
                &self.staging,
                &self.copying,
            );
            let result = coven_foundation::task::caught_sync(operation, || run(writer, &files))
                .unwrap_or_else(|bug| Err(E::from(DbError::Bug(bug))));
            // After a failed rollback the writer may still hold this write;
            // removal records wait for the next open (Appendix C.3).
            let usable = !matches!(self.access.observer.error(), Some(DbError::RollbackFailed));
            let chosen = usable.then(|| released(writer, self));
            Ok::<_, E>((result, chosen, files.take_committed()))
        })?;
        let result = match chosen {
            Some(chosen) => self.finish_removals(result, chosen),
            None => result,
        };
        // A write that returns success has committed.
        if result.is_ok() {
            committed.into_iter().for_each(|effect| effect());
        }
        result
    }

    /// Remove what `chosen` names, after the writer that chose it is
    /// released (§21.2, Appendix C.3).
    fn remove_chosen(&self, chosen: crate::file_removals::Chosen) -> Vec<DbError> {
        #[cfg(test)]
        {
            let gate = lock_unpoisoned(&self.removal_gate).clone();
            if let Some(gate) = gate {
                gate();
            }
        }
        self.access.remove_released(chosen)
    }

    /// `result`, after removing the files it released, as `chosen` names
    /// them; a failed removal keeps its record for the next write or open.
    fn finish_removals<R, E: crate::WriteFailure>(
        &self,
        result: Result<R, E>,
        chosen: crate::file_removals::Chosen,
    ) -> Result<R, E> {
        crate::write_failure::after_cleanup(result, self.remove_chosen(chosen))
    }
}

/// Let `names` go from the names a store reserves; their durable removal
/// records remain for the next write or open (Appendix C.3).
fn release_reserved<'a>(
    reserved: &Mutex<BTreeSet<coven_foundation::files::FileName>>,
    names: impl IntoIterator<Item = &'a coven_foundation::files::FileName>,
) {
    // Every name goes before the check can panic, so no panic leaves the
    // set half-changed and the lock is taken as it is.
    let unregistered = {
        let mut active = lock_unpoisoned(reserved);
        names.into_iter().filter(|name| !active.remove(*name)).count()
    };
    assert_eq!(unregistered, 0, "every reserved name is registered");
}

/// Let `paths` go from the copies a store keeps out of removals, once the
/// write that brought them has decided (§17.3).
pub(crate) fn release_copies(copying: &Mutex<BTreeSet<String>>, paths: &[String]) {
    let mut held = lock_unpoisoned(copying);
    for path in paths {
        held.remove(path);
    }
}

/// The removals the write that just ran under `writer` released, chosen
/// with the staging and copying sets held (Appendix C.3).
fn released(writer: &DatabaseConnection, inner: &DatabaseInner) -> crate::file_removals::Chosen {
    let active = lock_unpoisoned(&inner.staging);
    let copying = lock_unpoisoned(&inner.copying);
    crate::file_removals::choose(writer, &active, &copying)
}

#[cfg(test)]
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
    /// Where a bug in the store's work that nobody waits for goes (E15).
    pub fn bugs(&self) -> &coven_foundation::task::BugSinkRef {
        &self.bugs
    }

    /// Mark this instance as closing for a reopen to wait on, though workers
    /// still hold clones of it: the facade's owning scope ended (F.4).
    pub fn begin_close(&self) {
        self.closing.begin_close();
    }

    async fn call<R, E>(
        &self,
        run: impl FnOnce(&DatabaseInner) -> Result<R, E> + Send + 'static,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DbError> + Send + 'static,
    {
        finish_blocking(
            "database call",
            start_call(self.inner.clone(), move |inner: &DatabaseInner| inner.call(run)).await,
        )
    }
    /// Observe new local writes: rows inserted into the outgoing write queue.
    /// Upload confirmation removing a row is this device's own bookkeeping
    /// and wakes nothing (§6.7). Register before the first sync to
    /// include writes committed while a pass is running.
    pub fn sync_changes(&self) -> crate::DatabaseChanges {
        self.table_changes(&["_coven_uploads"], true, true)
    }

    /// Observe operations as they start: rows inserted into the journal,
    /// such as an invite, whose expiry the handle wakes to write (§13.2).
    pub fn started_operations(&self) -> crate::DatabaseChanges {
        self.table_changes(&["_coven_operations"], true, true)
    }

    /// The handle's clock could not wait for an invite's expiry: every
    /// later call and live query fails with `DbError::WallTimer` until the
    /// store is reopened (§13.2).
    pub fn fail_expiry_wait(&self, error: coven_foundation::clock::WallTimerError) {
        self.observer.fail(crate::observation::HandleFailure::WallTimer(error));
    }

    /// `first` reports the initial state as a change; `inserted_only`
    /// ignores updates and deletions.
    fn table_changes(
        &self,
        tables: &[&str],
        first: bool,
        inserted_only: bool,
    ) -> crate::DatabaseChanges {
        let reads: Vec<_> = tables
            .iter()
            .map(|table| crate::observation::TableRead {
                table: (*table).into(),
                columns: BTreeSet::new(),
                scope: crate::row_scope::RowScope::All,
                inserted_only,
            })
            .collect();
        self.observer.changes(reads, first)
    }

    /// Observe, from now, every commit made outside `attempt`: an operation
    /// attempt's own commits are no evidence against the wait it ends in,
    /// and which rows matter is known only when it ends (§6.7).
    pub fn attempt_commits(&self, attempt: crate::CommitScope) -> crate::AttemptCommits {
        self.observer.attempt(attempt)
    }

    /// Observe `rows` from here on, after `attempt`. The result first reports
    /// whether a commit since the attempt began changed one of them: such a
    /// commit is evidence against the wait the attempt ends in (IO
    /// `racing_commit_wakes`).
    pub fn watch_attempt(
        &self,
        attempt: crate::AttemptCommits,
        rows: &[crate::PrerequisiteRows],
    ) -> crate::DatabaseChanges {
        self.observer.watch_attempt(attempt, rows)
    }

    /// Open the local source of a file still on this device only. No storage
    /// operation occurs; `None` means the bytes come from the cache or
    /// storage.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        let reference = reference.clone();
        self.call(move |inner| inner.access.open_local_file(&reference)).await
    }

    /// The file this device has a record of under this id, from one
    /// committed state.
    pub async fn file_ref(&self, file: FileId) -> Result<crate::FileRef, DbError> {
        self.call(move |inner| inner.access.file_ref(file)).await
    }

    /// The recorded original's path, size and modification time; never reread its bytes.
    pub async fn user_file(&self, file: FileId) -> Result<Option<crate::UserFile>, DbError> {
        self.call(move |inner| inner.access.user_file(file)).await
    }

    /// Run ordinary SQL against one consistent snapshot when awaited.
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        Read::new(crate::read::ReadOwner::Writer(self), read)
    }

    /// The files this process is sending now: the upload worker marks each
    /// attempt, and coven's views read them (§4, §17.9).
    pub fn uploads_in_flight(&self) -> crate::InFlight {
        self.transfers.uploads()
    }

    /// The files this process is downloading to keep whole now: each pin's
    /// download marks its attempt, and `coven_downloading` reads them (§4,
    /// §17.8).
    pub fn downloads_in_flight(&self) -> crate::InFlight {
        self.transfers.downloads()
    }

    /// The store's watch says the device's network is now `network`:
    /// coven's views show it, and a live query that read them runs again
    /// (§4, §6.7).
    pub fn record_network(&self, network: coven_foundation::network_watch::NetworkState) {
        self.network.set(network);
    }

    /// Observe the tables and columns a query reads, and the rows its
    /// conditions allow.
    pub fn subscribe<F, R>(&self, query: F) -> LiveQuery<R>
    where
        F: Fn(SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        LiveQuery::new(self.subscribe_reconfigurable((), move |(), sql| query(sql)))
    }

    /// Observe a query whose request can be replaced through a shared handle.
    pub fn subscribe_reconfigurable<Q, F, R>(
        &self,
        initial_request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static,
    {
        let commits = self.observer.subscribe();
        // A subscription retains a database only for its active blocking read.
        // Idle queries and undelivered results cannot keep a closing lock held.
        let database = Arc::downgrade(&self.inner);
        let query = Arc::new(query);
        let completion = commits.clone();
        ReconfigurableLiveQuery::new(
            initial_request,
            Arc::new(move |request| {
                let database = database.clone();
                let query = query.clone();
                let completion = completion.clone();
                Box::pin(async move {
                    let database = database.upgrade().ok_or(DbError::StoreClosed)?;
                    finish_blocking(
                        "live query",
                        start_call(database, move |inner| {
                            inner.call(|inner| {
                                inner.access.readers.acquire_reader().live_read(
                                    &mut |reads, lookups| completion.finish_judging(reads, lookups),
                                    move |sql| query(&request, sql),
                                )
                            })
                        })
                        .await,
                    )
                })
            }),
            commits,
        )
    }

    /// Decode every lost cell and removed row in one snapshot.
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.read(|sql| sql.lost_values()).await
    }

    /// Observe the decoded lost cells and removed rows.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>> {
        self.subscribe(|sql| sql.lost_values())
    }

    pub(crate) fn start_read<F, R>(&self, read: F) -> RunningRead<R>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        RunningRead::new(start_call(self.inner.clone(), move |inner| {
            inner.call(|inner| inner.access.read(read))
        }))
    }

    /// Run app SQL and coven's steps, and commit its unsigned write record
    /// and merge metadata in one IMMEDIATE transaction (§4). The closure's
    /// result is returned after commit.
    pub async fn write<F, R>(&self, sql: F) -> Result<R, DbError>
    where
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.write_as(sql).await
    }

    /// `write`, for a closure whose failures are `E`: callback errors retain
    /// their type alongside rollback or byte-cleanup failures.
    pub async fn write_as<F, R, E>(&self, sql: F) -> Result<R, E>
    where
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        self.call(move |inner| {
            inner.with_files("write", |writer, files| {
                let result = coven_foundation::task::caught_sync("write", || {
                    writer.local_write(
                        &inner.access.write_schema,
                        inner.access.device,
                        inner.clock.now(),
                        files,
                        |files| sql(crate::SqlContext::new(files)),
                    )
                });
                // Staged files the write brought in go free once it has
                // decided: a write that rolled back leaves their bytes to the
                // removals its cleanup chooses.
                inner.release_file_reservations(&files.take_brought());
                release_copies(&inner.copying, &files.take_held_copies());
                #[cfg(any(test, feature = "test-utils"))]
                writer.checkpoint(crate::test_utils::WriteCheckpoint::StagingReleased);
                result.unwrap_or_else(|bug| Err(E::from(DbError::Bug(bug))))
            })
        })
        .await
    }

    /// Stage app bytes for a later write (§17.2): `stage` names each file's
    /// namespace and place and returns what that write's SQL needs, such as
    /// the ids staging gave them. Their bytes are streamed to coven's own
    /// copies, each synced, and flushed, before this returns; close waits for
    /// that, and for its cleanup if it fails or is cancelled. The result
    /// holds nothing close waits for. A write takes it with `put_staged`.
    pub async fn stage_files<F, T, E>(&self, stage: F) -> Result<StagedFiles<T>, E>
    where
        F: FnOnce(&mut crate::WriteBatch<'_>) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        let lease = self.file_tasks.clone().read_owned().await;
        let database = self.inner.clone();
        let (staging, value) = finish_blocking(
            "stage files",
            tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                file_staging::FileStaging::new(database, lease, stage)
            }))
            .await,
        )?;
        let (staging, flushed) =
            staging.write().await.map_err(|failed| E::from(DbError::from(failed)))?;
        match flushed {
            Ok(_flushed) => Ok(staging.into_staged(value)),
            // A failed copy or flush removes what the staging wrote.
            Err(error) => Err(finish_blocking(
                "discard staged files",
                tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                    Ok::<_, E>(staging.discard::<E>(error))
                }))
                .await,
            )?),
        }
    }

    /// Stage app bytes, then run a write that puts them in its rows, as an
    /// app does with `stage_files` and `put_staged`, for tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn staged_write<F, S, T, R, E>(&self, stage: F, write: S) -> Result<R, E>
    where
        F: FnOnce(&mut crate::WriteBatch<'_>) -> Result<T, E> + Send + 'static,
        S: FnOnce(crate::SqlContext<'_, '_>, T) -> Result<R, E> + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        let staged = self.stage_files(stage).await?;
        self.write_as(move |sql| {
            let value = sql.put_staged(staged)?;
            write(sql, value)
        })
        .await
    }

    /// Run app SQL that writes device-local tables alone in one IMMEDIATE
    /// transaction, as plain SQLite: no pre-update hook records its row
    /// changes, so SQLite empties a table in one step and fills one at its
    /// own speed (§4). A statement that would write a synced table, itself or
    /// through a trigger or foreign-key action, fails the write with
    /// `SyncedTableInBulkWrite`, and everything rolls back. After commit,
    /// every live query that read a table the write's statements may change
    /// runs again, whatever rows it read. The closure's result is returned
    /// after commit.
    pub async fn write_bulk<F, R, E>(&self, sql: F) -> Result<R, E>
    where
        F: FnOnce(crate::BulkSqlContext<'_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        self.call(move |inner| inner.access.with_writer(|writer| writer.bulk_write(sql))).await
    }

    /// The committed app schema version, read on a read-only connection.
    pub async fn schema_version(&self) -> Result<u32, DbError> {
        self.call(|inner| inner.access.schema_version()).await
    }

    /// The SQLite settings the store's connections have in effect (§4).
    pub async fn sqlite_settings(&self) -> Result<crate::SqliteSettingsInEffect, DbError> {
        self.call(|inner| inner.access.sqlite_settings()).await
    }

    /// Checkpoint the store's WAL on the writer, between writes (§4).
    pub async fn checkpoint(
        &self,
        mode: crate::CheckpointMode,
    ) -> Result<crate::WalCheckpoint, DbError> {
        self.call(move |inner| inner.access.with_writer(|writer| writer.wal_checkpoint(mode))).await
    }

    /// Give back free pages of a store created with incremental auto-vacuum,
    /// on the writer, between writes (§4). On any other store it does nothing.
    pub async fn incremental_vacuum(&self, pages: crate::IncrementalVacuum) -> Result<(), DbError> {
        self.call(move |inner| inner.access.with_writer(|writer| writer.incremental_vacuum(pages)))
            .await
    }

    /// Give back every connection's cached pages that no statement is using
    /// (§4). SQLite as coven builds it (`SQLITE_ENABLE_MEMORY_MANAGEMENT`)
    /// keeps the unused pages of every connection in the process in one
    /// pool, and one connection's release empties the whole pool: so one
    /// release on an idle reader frees the writer's, the input database's
    /// and the checkpointers' too, and never waits for a write.
    pub async fn release_memory(&self) -> Result<(), DbError> {
        self.call(|inner| {
            inner.access.readers.acquire_reader().with_reader(|reader| Ok(reader.release_memory()?))
        })
        .await
    }

    /// The migrations this open committed, classified individually.
    pub fn applied_migrations(&self) -> Result<Vec<MigrationOutcome>, DbError> {
        let inner = read_unpoisoned(&self.inner);
        let inner = inner.as_ref().ok_or(DbError::StoreClosed)?;
        if let Some(error) = inner.access.observer.error() {
            return Err(error);
        }
        Ok(inner.migrations.clone())
    }

    /// Wait for database calls, close all connections, then release the writer lock.
    /// Every subsequent database call, including close, reports `StoreClosed`.
    pub async fn close(&self) -> Result<(), DbError> {
        self.closing.begin_close();
        let database = self.clone();
        // Closing runs as its own task so that a caller dropping this future
        // doesn't stop it partway; the runtime dropping that task can.
        coven_foundation::task::spawn_completing("close the store", self.bugs.clone(), async move {
            let file_tasks = database.file_tasks.clone().write_owned().await;
            finish_blocking(
                "close the store",
                tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                    let _file_tasks = file_tasks;
                    let mut slot = write_unpoisoned(&database.inner);
                    let Some(inner) = slot.take() else {
                        return Err(DbError::StoreClosed);
                    };
                    let mut failures: Vec<DbError> = inner
                        .checkpoints
                        .into_iter()
                        .filter_map(|checkpoints| checkpoints.stop().err())
                        .collect();
                    failures.extend(inner.inputs.close());
                    // The statistics follow this session's writes, outside
                    // any captured transaction. A failed handle's writer may
                    // still hold a transaction, which closing rolls back.
                    if inner.access.observer.error().is_none() {
                        if let Err(error) = lock_unpoisoned(&inner.access.writer).optimize() {
                            failures.push(error);
                        }
                    }
                    match inner.access.close() {
                        Err(DbError::Closing { failures: application }) => {
                            failures.extend(application)
                        }
                        Err(error) => failures.push(error),
                        Ok(()) => (),
                    }
                    if failures.is_empty() {
                        Ok(())
                    } else {
                        Err(DbError::Closing { failures })
                    }
                }))
                .await,
            )
        })
        .await
        .map_err(DbError::from)?
    }
}

/// An application read handle that cannot write synced rows or migrate.
/// It owns a connection restricted to local cache metadata and a shared store
/// lock that prevents deletion until all its connections close.
///
/// ```compile_fail
/// async fn cannot_write(handle: &coven_database::DatabaseReadHandle) {
///     handle.write(|_| Ok(())).await;
/// }
/// ```
/// ```compile_fail
/// fn cannot_subscribe(handle: &coven_database::DatabaseReadHandle) {
///     handle.subscribe(|_| Ok(()));
/// }
/// ```
/// ```compile_fail
/// fn cannot_reconfigure(handle: &coven_database::DatabaseReadHandle) {
///     handle.subscribe_reconfigurable((), |_, _| Ok(()));
/// }
/// ```
/// ```compile_fail
/// fn cannot_subscribe_losses(handle: &coven_database::DatabaseReadHandle) {
///     handle.subscribe_lost_values();
/// }
/// ```
#[derive(Clone)]
pub struct DatabaseReadHandle {
    /// Where its connections report their commits (§4).
    observer: CommitObserver,
    /// Never started: this handle transfers nothing (§4).
    transfers: crate::transfers_in_flight::TransfersInFlight,
    /// Where a bug in the handle's work that nobody waits for goes (E15).
    bugs: coven_foundation::task::BugSinkRef,
    inner: Arc<RwLock<Option<DatabaseAccess<StoreReadLock>>>>,
}

impl DatabaseReadHandle {
    /// Where a bug in the handle's work that nobody waits for goes, such as
    /// its file owner's worker's (E15).
    pub fn bugs(&self) -> &coven_foundation::task::BugSinkRef {
        &self.bugs
    }

    /// The files this handle is sending: none, since it sends nothing.
    pub fn uploads_in_flight(&self) -> crate::InFlight {
        self.transfers.uploads()
    }

    /// The files this handle is downloading to keep whole: none, since it
    /// keeps no file whole.
    pub fn downloads_in_flight(&self) -> crate::InFlight {
        self.transfers.downloads()
    }

    async fn call<R, E>(
        &self,
        run: impl FnOnce(&DatabaseAccess<StoreReadLock>) -> Result<R, E> + Send + 'static,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DbError> + Send + 'static,
    {
        finish_blocking(
            "database call",
            start_call(self.inner.clone(), move |access| access.call(|| run(access))).await,
        )
    }
    /// Open the local bytes of a file still on this device only; `None`
    /// means the bytes come from the cache or storage.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        let reference = reference.clone();
        self.call(move |inner| inner.open_local_file(&reference)).await
    }

    /// The file this device has a record of under this id, from one
    /// committed state.
    pub async fn file_ref(&self, file: FileId) -> Result<crate::FileRef, DbError> {
        self.call(move |inner| inner.file_ref(file)).await
    }

    /// The recorded original's path, size and modification time; never reread its bytes.
    pub async fn user_file(&self, file: FileId) -> Result<Option<crate::UserFile>, DbError> {
        self.call(move |inner| inner.user_file(file)).await
    }

    /// Read one consistent snapshot when awaited.
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        Read::new(crate::read::ReadOwner::Reader(self), read)
    }

    /// Decode every lost cell and removed row in one snapshot.
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.read(|sql| sql.lost_values()).await
    }

    pub(crate) fn start_read<F, R>(&self, read: F) -> RunningRead<R>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        RunningRead::new(start_call(self.inner.clone(), move |inner| {
            inner.call(|| inner.read(read))
        }))
    }

    /// The committed app schema version.
    pub async fn schema_version(&self) -> Result<u32, DbError> {
        self.call(|inner| inner.schema_version()).await
    }

    /// The SQLite settings this handle's connections have in effect (§4).
    pub async fn sqlite_settings(&self) -> Result<crate::SqliteSettingsInEffect, DbError> {
        self.call(|inner| inner.sqlite_settings()).await
    }

    /// Wait for active calls and close every clone of this handle.
    pub async fn close(&self) -> Result<(), DbError> {
        let handle = self.clone();
        finish_blocking(
            "close the store",
            tokio::task::spawn_blocking(move || {
                let mut slot = write_unpoisoned(&handle.inner);
                slot.take().ok_or(DbError::StoreClosed)?.close()
            })
            .await,
        )
    }
}

fn start_call<I, R, E>(
    inner: Arc<RwLock<Option<I>>>,
    run: impl FnOnce(&I) -> Result<R, E> + Send + 'static,
) -> tokio::task::JoinHandle<Result<R, E>>
where
    I: Send + Sync + 'static,
    R: Send + 'static,
    E: From<DbError> + Send + 'static,
{
    tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
        let slot = read_unpoisoned(&inner);
        run(slot.as_ref().ok_or(DbError::StoreClosed)?)
    }))
}

/// The result of a database call run on the blocking pool as `operation`. A
/// panic in it is `DbError::Bug`; the pool dropping it unstarted, as it does
/// once its runtime begins shutting down, is `DbError::Shutdown`, and the
/// call changed nothing (E15).
pub(crate) fn finish_blocking<R, E: From<DbError>>(
    operation: &'static str,
    joined: Result<Result<R, E>, tokio::task::JoinError>,
) -> Result<R, E> {
    joined.map_err(|error| {
        E::from(DbError::from(coven_foundation::task::TaskFailed::of_blocking_join(
            operation, error,
        )))
    })?
}

pub(crate) async fn process<F, T>(process: F) -> CovenResult<T>
where
    F: FnOnce() -> CovenResult<T> + Send + 'static,
    T: Send + 'static,
{
    finish_blocking("process a read", tokio::task::spawn_blocking(process).await)
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;
