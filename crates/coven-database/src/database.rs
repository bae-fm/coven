//! Opening is the composition root; a shared handle owns all connections and
//! the writer lock. Blocking calls retain these resources after its last owner
//! drops, while another open waits for their release.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

#[path = "file_database.rs"]
pub(crate) mod file_database;

#[path = "file_staging.rs"]
mod file_staging;
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
mod operation_tests;

use coven_foundation::clock::{ClockRef, SystemClock};
use coven_foundation::files::{StoreDir, StoreLock, StoreReadLock};
use coven_foundation::id_source::{DeviceId, IdSourceRef, UuidIds};

use crate::authorization::SqlAuthorization;
use crate::observation::{CommitObserver, CommitSubscription};
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
    /// The files this process is sending now, which coven's views read (§4).
    uploads: crate::UploadsInFlight,
    closing: Arc<coven_foundation::files::StoreClosingGuard>,
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
    staging: Mutex<BTreeSet<coven_foundation::files::FileName>>,
    /// Runs where released files' bytes are about to go, after the writer is
    /// free, so a test can stall that disk work.
    #[cfg(test)]
    removal_gate: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
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
    fn call<R, E: From<DbError>>(&self, run: impl FnOnce() -> Result<R, E>) -> Result<R, E> {
        if let Some(error) = self.observer.error() {
            return Err(error.into());
        }
        let result = run()?;
        match self.observer.error() {
            Some(error) => Err(error.into()),
            None => Ok(result),
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
        run: impl FnOnce(&DatabaseConnection) -> Result<R, E>,
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
    /// that recorded them chose, with the failures found choosing it.
    fn remove_released(&self, chosen: crate::file_removals::Chosen) -> Vec<DbError> {
        let (released, mut failures) = match chosen {
            Ok(chosen) => chosen,
            Err(error) => return vec![error],
        };
        let (removed, removal_failures) =
            crate::file_removals::remove_bytes(&self.directory, released);
        failures.extend(removal_failures);
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
        self.readers.acquire_reader().local_file(
            &self.write_schema,
            &self.directory,
            self.device,
            reference,
        )
    }

    fn file_ref(&self, table: &str, key: &crate::RowKey) -> Result<crate::FileRef, DbError> {
        self.readers
            .acquire_reader()
            .read_snapshot(|sql| sql.file_ref(&self.write_schema, table, key))
            .0
    }

    fn user_file(
        &self,
        table: &str,
        key: &crate::RowKey,
    ) -> Result<Option<crate::UserFile>, DbError> {
        self.readers
            .acquire_reader()
            .read_snapshot(|sql| sql.user_file(&self.write_schema, table, key))
            .0
    }

    fn read<R>(&self, read: impl FnOnce(SqlReadContext<'_>) -> CovenResult<R>) -> CovenResult<R> {
        self.readers.acquire_reader().read_snapshot(read).0
    }

    fn schema_version(&self) -> Result<u32, DbError> {
        self.readers.acquire_reader().schema_version()
    }

    /// Every connection has the same settings, so one reader reports them.
    fn sqlite_settings(&self) -> Result<crate::SqliteSettingsInEffect, DbError> {
        self.readers
            .acquire_reader()
            .with_reader(DatabaseConnection::sqlite_settings)
    }

    fn close(self) -> Result<(), DbError> {
        self.observer.close();
        let mut failures = self.readers.close();
        // Each checkpointer stopped before, and let go of the writer.
        match Arc::try_unwrap(self.writer) {
            Ok(writer) => {
                if let Err(error) = writer
                    .into_inner()
                    .expect("database connection lock poisoned")
                    .close()
                {
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

// SQLite rolls back before this catches an unwind; decoded state changes only
// after commit. Release each lock before propagating a callback panic so the
// next caller can still use the committed state.
fn with_locked<T, R>(value: &Mutex<T>, run: impl FnOnce(&mut T) -> R) -> R {
    let mut guard = value.lock().expect("database state lock poisoned");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&mut guard)));
    drop(guard);
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

impl DatabaseInner {
    fn call<R, E: From<DbError>>(&self, run: impl FnOnce(&Self) -> Result<R, E>) -> Result<R, E> {
        self.access.call(|| run(self))
    }

    // The caller retains its file-task lease until these names are released.
    // Their durable removal records remain for the next write or open.
    fn release_file_reservations<'a>(
        &self,
        names: impl IntoIterator<Item = &'a coven_foundation::files::FileName>,
    ) {
        let mut active = self.staging.lock().expect("file staging lock poisoned");
        for name in names {
            assert!(active.remove(name), "reserved name is registered");
        }
    }

    /// Run a write under the writer, then remove the files it released once
    /// the writer is free (Appendix C.3).
    fn with_files<R, E: crate::WriteFailure>(
        &self,
        staged: Vec<crate::file_write::StagedFile>,
        run: impl FnOnce(&DatabaseConnection, &crate::file_write::FileWrite<'_>) -> Result<R, E>,
    ) -> Result<R, E> {
        let (outcome, chosen) = self.access.with_writer(|writer| {
            let files = crate::file_write::FileWrite::new(
                writer,
                &self.access.write_schema,
                self.access.device,
                self.ids.as_ref(),
                staged,
            );
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(writer, &files)));
            // After a failed rollback the writer may still hold this write;
            // removal records wait for the next open (Appendix C.3).
            let usable = !matches!(self.access.observer.error(), Some(DbError::RollbackFailed));
            let chosen = (outcome.is_ok() || usable).then(|| released(writer, &self.staging));
            Ok::<_, E>((outcome, chosen))
        })?;
        match outcome {
            Ok(result) => self.finish_removals(result, chosen.expect("chosen after a write")),
            Err(panic) => {
                if let Some(chosen) = chosen {
                    let failures = self.remove_chosen(chosen);
                    if !failures.is_empty() {
                        panic!("file cleanup after callback panic failed: {failures:?}");
                    }
                }
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Remove what `chosen` names, after the writer that chose it is
    /// released (§21.2, Appendix C.3).
    fn remove_chosen(&self, chosen: crate::file_removals::Chosen) -> Vec<DbError> {
        #[cfg(test)]
        {
            let gate = self
                .removal_gate
                .lock()
                .expect("removal gate lock poisoned")
                .clone();
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

/// The removals the write that just ran under `writer` released, chosen
/// with the staging set held (Appendix C.3).
fn released(
    writer: &DatabaseConnection,
    staging: &Mutex<BTreeSet<coven_foundation::files::FileName>>,
) -> crate::file_removals::Chosen {
    let active = staging.lock().expect("file staging lock poisoned");
    crate::file_removals::choose(writer, &active)
}

#[cfg(test)]
impl Database {
    /// Run `gate` where released files' bytes are about to go, after the
    /// writer is free, so a test can stall that disk work.
    pub(crate) async fn test_gate_file_removal(&self, gate: impl Fn() + Send + Sync + 'static) {
        let gate: Arc<dyn Fn() + Send + Sync> = Arc::new(gate);
        self.call(move |inner| {
            *inner
                .removal_gate
                .lock()
                .expect("removal gate lock poisoned") = Some(gate);
            Ok::<_, DbError>(())
        })
        .await
        .unwrap();
    }
}

impl Database {
    /// Track a facade's owning scope independently of database clones held by
    /// workers. Ending that scope marks this instance as closing for reopen.
    pub fn closing_guard(&self) -> coven_foundation::files::StoreClosingGuard {
        self.closing.closing_guard()
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
            start_call(self.inner.clone(), move |inner: &DatabaseInner| {
                inner.call(run)
            })
            .await,
        )
    }
    /// Observe new local writes: rows inserted into the outgoing write queue.
    /// Upload confirmation removing a row is this device's own bookkeeping
    /// and wakes nothing (§6.7). Register before the first sync to
    /// include writes committed while a pass is running.
    pub fn sync_changes(&self) -> crate::DatabaseChanges {
        self.table_changes(&["_coven_uploads"], true, true)
    }

    /// `first` reports the initial state as a change; `inserted_only`
    /// ignores updates and deletions.
    fn table_changes(
        &self,
        tables: &[&str],
        first: bool,
        inserted_only: bool,
    ) -> crate::DatabaseChanges {
        let slot = self.inner.read().expect("database lock poisoned");
        let reads: Vec<_> = tables
            .iter()
            .map(|table| crate::observation::TableRead {
                table: (*table).into(),
                columns: BTreeSet::new(),
                keys: crate::key_scope::KeyScope::All,
                inserted_only,
            })
            .collect();
        let commits = match slot.as_ref() {
            Some(inner) => inner.access.observer.subscribe(),
            None => CommitSubscription::closed(),
        };
        commits.begin();
        commits.finish(reads.clone());
        crate::DatabaseChanges {
            commits,
            reads,
            first,
        }
    }

    /// Observe, from now, every commit made outside `attempt`: an operation
    /// attempt's own commits are no evidence against the wait it ends in,
    /// and which rows matter is known only when it ends (§6.7).
    pub fn attempt_commits(&self, attempt: crate::CommitScope) -> crate::AttemptCommits {
        let slot = self.inner.read().expect("database lock poisoned");
        let commits = match slot.as_ref() {
            Some(inner) => inner.access.observer.subscribe_outside(Some(attempt)),
            None => CommitSubscription::closed(),
        };
        commits.begin();
        crate::AttemptCommits { commits }
    }

    /// Open the local source of a file this device still has queued for upload,
    /// after checking its row. No storage operation occurs; `None` means the
    /// bytes come from the cache or storage.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        let reference = reference.clone();
        self.call(move |inner| inner.access.open_local_file(&reference))
            .await
    }

    /// The row's file, audience and version from one committed state.
    pub async fn file_ref(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<crate::FileRef, DbError> {
        let table = table.to_owned();
        let key = key.into();
        self.call(move |inner| inner.access.file_ref(&table, &key))
            .await
    }

    /// The recorded original's path, size and modification time; never reread its bytes.
    pub async fn user_file(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<Option<crate::UserFile>, DbError> {
        let table = table.to_owned();
        let key = key.into();
        self.call(move |inner| inner.access.user_file(&table, &key))
            .await
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
    /// attempt, and coven's views read them (§4, §17.7).
    pub fn uploads_in_flight(&self) -> crate::UploadsInFlight {
        self.uploads.clone()
    }

    /// Observe the tables, columns and key ranges read by a query.
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
        self.subscribe_schema(initial_request, move |request, _, sql| query(request, sql))
    }

    /// One consistent snapshot with the declared file schema.
    pub(crate) async fn read_schema<R: Send + 'static>(
        &self,
        read: impl FnOnce(&crate::write_schema::WriteSchema, SqlReadContext<'_>) -> CovenResult<R>
            + Send
            + 'static,
    ) -> CovenResult<R> {
        self.call(move |inner| {
            let schema = &inner.access.write_schema;
            inner
                .access
                .readers
                .acquire_reader()
                .read_snapshot(move |sql| read(schema, sql))
                .0
        })
        .await
    }

    /// Observe a query that reads with the declared file schema.
    pub(crate) fn subscribe_schema<Q, F, R>(
        &self,
        initial_request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, &crate::write_schema::WriteSchema, SqlReadContext<'_>) -> CovenResult<R>
            + Send
            + Sync
            + 'static,
        R: Send + 'static,
    {
        let inner = self.inner.read().expect("database lock poisoned");
        let commits = match inner.as_ref() {
            Some(inner) => inner.access.observer.subscribe(),
            None => CommitSubscription::closed(),
        };
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
                        start_call(database, move |inner| {
                            inner.call(|inner| {
                                let schema = &inner.access.write_schema;
                                let (result, reads) = inner
                                    .access
                                    .readers
                                    .acquire_reader()
                                    .read_snapshot(move |sql| query(&request, schema, sql));
                                completion.finish(reads);
                                result
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

    /// Dismisses lost values in a write so every device drops them from
    /// `_coven_lost`; a removed row is deleted for good (§8.4, E4).
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()> {
        let values = values.to_vec();
        self.call(move |inner| {
            inner.with_files(Vec::new(), |writer, files| {
                crate::dismissal::dismiss(
                    writer,
                    &inner.access.write_schema,
                    inner.access.device,
                    inner.clock.now(),
                    &values,
                    files,
                )
                .map_err(Into::into)
            })
        })
        .await
    }

    /// Observe the decoded lost cells and removed rows.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>> {
        self.subscribe(|sql| sql.lost_values())
    }

    pub(crate) fn start_read<F, R>(&self, read: F) -> tokio::task::JoinHandle<CovenResult<R>>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        start_call(self.inner.clone(), move |inner| {
            inner.call(|inner| inner.access.read(read))
        })
    }

    /// Run app SQL and commit its unsigned write record and merge metadata in
    /// one IMMEDIATE transaction (§4). The closure's result is returned after commit.
    pub async fn write<F, R>(&self, sql: F) -> Result<R, DbError>
    where
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.write_with_files(|_| Ok(()), sql).await
    }

    /// Stream app-provided files to durable storage, then commit their rows and
    /// metadata atomically. A failed write discards all newly supplied bytes.
    /// Callback errors retain their type alongside rollback or byte-cleanup failures.
    pub async fn write_with_files<F, S, R, E>(&self, build: F, sql: S) -> Result<R, E>
    where
        F: FnOnce(&mut crate::WriteBatch) -> Result<(), E> + Send + 'static,
        S: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: crate::WriteFailure + Send + 'static,
    {
        let lease = self.file_tasks.clone().read_owned().await;
        let database = self.inner.clone();
        let staging = finish_blocking(
            tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                file_staging::FileStaging::new(database, lease, build)
            }))
            .await,
        )?;
        let (staging, result) = staging.write().await;
        finish_blocking(
            tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                staging.finish(result, sql)
            }))
            .await,
        )
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
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| writer.wal_checkpoint(mode))
        })
        .await
    }

    /// Give back free pages of a store created with incremental auto-vacuum,
    /// on the writer, between writes (§4). On any other store it does nothing.
    pub async fn incremental_vacuum(&self, pages: crate::IncrementalVacuum) -> Result<(), DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| writer.incremental_vacuum(pages))
        })
        .await
    }

    /// The migrations this open committed, classified individually.
    pub fn applied_migrations(&self) -> Result<Vec<MigrationOutcome>, DbError> {
        let inner = self.inner.read().expect("database lock poisoned");
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
        finish_blocking(
            tokio::spawn(async move {
                let file_tasks = database.file_tasks.clone().write_owned().await;
                finish_blocking(
                    tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                        let _file_tasks = file_tasks;
                        let mut slot = database.inner.write().expect("database lock poisoned");
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
                            if let Err(error) = inner
                                .access
                                .writer
                                .lock()
                                .expect("database connection lock poisoned")
                                .optimize()
                            {
                                failures.push(error);
                            }
                        }
                        match inner.access.close() {
                            Err(DbError::Closing {
                                failures: application,
                            }) => failures.extend(application),
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
            .await,
        )
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
    /// Never started: this handle sends nothing (§4).
    uploads: crate::UploadsInFlight,
    inner: Arc<RwLock<Option<DatabaseAccess<StoreReadLock>>>>,
}

impl DatabaseReadHandle {
    /// The files this handle is sending: none, since it sends nothing.
    pub fn uploads_in_flight(&self) -> crate::UploadsInFlight {
        self.uploads.clone()
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
            start_call(self.inner.clone(), move |access| {
                access.call(|| run(access))
            })
            .await,
        )
    }
    /// Open a queued file's local bytes while checking its current row; `None`
    /// means the bytes come from the cache or storage.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<Option<crate::LocalFileStream>, crate::LocalFileError> {
        let reference = reference.clone();
        self.call(move |inner| inner.open_local_file(&reference))
            .await
    }

    /// The row's file, audience and version from one committed state.
    pub async fn file_ref(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<crate::FileRef, DbError> {
        let table = table.to_owned();
        let key = key.into();
        self.call(move |inner| inner.file_ref(&table, &key)).await
    }

    /// The recorded original's path, size and modification time; never reread its bytes.
    pub async fn user_file(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<Option<crate::UserFile>, DbError> {
        let table = table.to_owned();
        let key = key.into();
        self.call(move |inner| inner.user_file(&table, &key)).await
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

    pub(crate) fn start_read<F, R>(&self, read: F) -> tokio::task::JoinHandle<CovenResult<R>>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        start_call(self.inner.clone(), move |inner| {
            inner.call(|| inner.read(read))
        })
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
            tokio::task::spawn_blocking(move || {
                let mut slot = handle.inner.write().expect("database lock poisoned");
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
        let slot = inner.read().expect("database lock poisoned");
        run(slot.as_ref().ok_or(DbError::StoreClosed)?)
    }))
}

pub(crate) fn finish_blocking<T>(result: Result<T, tokio::task::JoinError>) -> T {
    match result {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("blocking database task was cancelled: {error}"),
    }
}

pub(crate) async fn process<F, T>(process: F) -> CovenResult<T>
where
    F: FnOnce() -> CovenResult<T> + Send + 'static,
    T: Send + 'static,
{
    finish_blocking(tokio::task::spawn_blocking(process).await)
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;
