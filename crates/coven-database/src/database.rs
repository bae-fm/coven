//! Opening is the composition root; a shared handle owns all connections and
//! the writer lock. Blocking calls keep that owner alive until they finish.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

#[path = "file_database.rs"]
pub(crate) mod file_database;

#[path = "file_staging.rs"]
mod file_staging;
#[path = "read_pool.rs"]
mod read_pool;
use read_pool::ReadPool;

#[path = "database_operations.rs"]
mod operations;

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
#[path = "database_recovery.rs"]
mod recovery;
#[path = "database_sync.rs"]
mod sync;
pub use builder::DatabaseBuilder;

/// Shared ownership of the database. Closing any clone closes them all.
#[derive(Clone)]
pub struct Database {
    inner: Arc<RwLock<Option<DatabaseInner>>>,
    file_tasks: Arc<tokio::sync::RwLock<()>>,
}

struct DatabaseInner {
    observer: CommitObserver,
    access: DatabaseAccess<StoreLock>,
    migrations: Vec<MigrationOutcome>,
    clock: ClockRef,
    ids: IdSourceRef,
    staging: Mutex<BTreeSet<coven_foundation::files::FileName>>,
    recovery: Option<coven_foundation::files::DatabaseRecovery>,
}

// The lock outlives every connection, including when the last handle is dropped.
struct DatabaseAccess<L> {
    writer: Mutex<DatabaseConnection>,
    readers: ReadPool,
    write_schema: crate::write_schema::WriteSchema,
    directory: StoreDir,
    device: DeviceId,
    lock: L,
}

impl<L> DatabaseAccess<L> {
    fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
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

    fn close(self) -> Result<(), DbError> {
        let mut failures = self.readers.close();
        if let Err(error) = self
            .writer
            .into_inner()
            .expect("database connection lock poisoned")
            .close()
        {
            failures.push(error);
        }
        drop(self.lock);
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Closing { failures })
        }
    }
}

// SQLite rolls back before this boundary catches an unwind. Releasing the
// connection outside unwinding preserves the mutex for the next caller.
fn with_connection<R>(
    connection: &Mutex<DatabaseConnection>,
    run: impl FnOnce(&DatabaseConnection) -> R,
) -> R {
    let guard = connection
        .lock()
        .expect("database connection lock poisoned");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&guard)));
    drop(guard);
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

impl DatabaseInner {
    fn with_writer<R>(&self, run: impl FnOnce(&DatabaseConnection) -> R) -> R {
        with_connection(&self.access.writer, run)
    }

    fn with_files<R, E: crate::WriteFailure>(
        &self,
        staged: Vec<crate::file_write::StagedFile>,
        run: impl FnOnce(&DatabaseConnection, &crate::file_write::FileWrite<'_>) -> Result<R, E>,
    ) -> Result<R, E> {
        self.with_writer(|writer| {
            let files = crate::file_write::FileWrite::new(
                writer,
                &self.access.directory,
                &self.access.write_schema,
                self.access.device,
                &self.staging,
                staged,
            );
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(writer, &files)));
            match result {
                Ok(result) => files.finish(result),
                Err(panic) => {
                    if let Err(error) = files.rollback() {
                        panic!("file cleanup after callback panic failed: {error:?}");
                    }
                    std::panic::resume_unwind(panic)
                }
            }
        })
    }
}

impl Database {
    // The caller retains its file-task lease until these names are released.
    // Their durable removal records remain for the next write or open.
    fn release_file_reservations<'a>(
        &self,
        names: impl IntoIterator<Item = &'a coven_foundation::files::FileName>,
    ) {
        let slot = self.inner.read().expect("database lock poisoned");
        let inner = slot.as_ref().expect("reservation holds close guard");
        let mut active = inner.staging.lock().expect("file staging lock poisoned");
        for name in names {
            assert!(active.remove(name), "reserved name is registered");
        }
    }

    async fn call<R, E>(
        &self,
        run: impl FnOnce(&DatabaseInner) -> Result<R, E> + Send + 'static,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DbError> + Send + 'static,
    {
        finish_blocking(start_call(self.inner.clone(), run).await)
    }
    /// Observe committed changes to the outgoing write queue. Register before
    /// the first sync to include writes committed while a pass is running.
    pub fn sync_changes(&self) -> crate::DatabaseChanges {
        let slot = self.inner.read().expect("database lock poisoned");
        let reads = vec![crate::observation::TableRead {
            table: "_coven_uploads".into(),
            columns: BTreeSet::new(),
            keys: crate::key_scope::KeyScope::All,
        }];
        let commits = match slot.as_ref() {
            Some(inner) => inner.observer.subscribe(),
            None => CommitSubscription::closed(),
        };
        commits.begin();
        commits.finish(reads.clone());
        crate::DatabaseChanges {
            commits,
            reads,
            first: true,
        }
    }

    /// Publish an explicitly recovered database after sync commits its snapshot
    /// reload. Until then, ordinary writer and reader opens refuse the store.
    pub async fn finish_recovery(&self) -> Result<(), DbError> {
        let owner = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let mut slot = owner.inner.write().expect("database lock poisoned");
                let inner = slot.as_mut().ok_or(DbError::StoreClosed)?;
                if let Some(recovery) = &inner.recovery {
                    inner.with_writer(|writer| {
                        crate::file_removals::FileRemovals::new(
                            writer,
                            &inner.access.directory,
                            &BTreeSet::new(),
                        )
                        .finish(Ok::<_, DbError>(()))?;
                        recovery.finish().map_err(DbError::from)
                    })?;
                }
                inner.recovery = None;
                Ok(())
            })
            .await,
        )
    }

    /// Open the source selected by a file reference, after checking its row.
    /// No storage operation occurs; nonlocal sources return their location.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
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
        let inner = self.inner.read().expect("database lock poisoned");
        let commits = match inner.as_ref() {
            Some(inner) => inner.observer.subscribe(),
            None => CommitSubscription::closed(),
        };
        ReconfigurableLiveQuery::new(self.clone(), initial_request, query, commits)
    }

    /// Decode every lost cell and removed row in one snapshot.
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>> {
        self.read(|sql| sql.lost_values()).await
    }

    /// Dismisses lost values in a write so every device drops them from
    /// `_coven_lost`; a removed row is deleted for good (§8, E4).
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
        start_call(self.inner.clone(), move |inner| inner.access.read(read))
    }

    pub(crate) async fn observed_read<F, R>(
        &self,
        read: F,
        commits: CommitSubscription,
    ) -> CovenResult<R>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static,
    {
        self.call(move |inner| {
            let (result, reads) = inner.access.readers.acquire_reader().read_snapshot(read);
            commits.finish(reads);
            result
        })
        .await
    }

    /// Run app SQL and commit its unsigned write record and merge metadata in
    /// one IMMEDIATE transaction (§5). The closure's result is returned after commit.
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
        let database = self.clone();
        let staging = finish_blocking(
            tokio::task::spawn_blocking(move || {
                file_staging::FileStaging::new(database, lease, build)
            })
            .await,
        )?;
        let (staging, result) = staging.write().await;
        finish_blocking(tokio::task::spawn_blocking(move || staging.finish(result, sql)).await)
    }

    /// The committed app schema version, read on a read-only connection.
    pub async fn schema_version(&self) -> Result<u32, DbError> {
        self.call(|inner| inner.access.schema_version()).await
    }

    /// The migrations this open committed, classified individually.
    pub fn applied_migrations(&self) -> Result<Vec<MigrationOutcome>, DbError> {
        let inner = self.inner.read().expect("database lock poisoned");
        Ok(inner
            .as_ref()
            .ok_or(DbError::StoreClosed)?
            .migrations
            .clone())
    }

    /// Wait for database calls, close all connections, then release the writer lock.
    /// Every subsequent database call, including close, reports `StoreClosed`.
    pub async fn close(&self) -> Result<(), DbError> {
        let file_tasks = self.file_tasks.clone().write_owned().await;
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let _file_tasks = file_tasks;
                let mut slot = database.inner.write().expect("database lock poisoned");
                let Some(inner) = slot.take() else {
                    return Err(DbError::StoreClosed);
                };
                inner.observer.close();
                inner.access.close()
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
    inner: Arc<RwLock<Option<DatabaseAccess<StoreReadLock>>>>,
}

impl DatabaseReadHandle {
    async fn call<R, E>(
        &self,
        run: impl FnOnce(&DatabaseAccess<StoreReadLock>) -> Result<R, E> + Send + 'static,
    ) -> Result<R, E>
    where
        R: Send + 'static,
        E: From<DbError> + Send + 'static,
    {
        finish_blocking(start_call(self.inner.clone(), run).await)
    }
    /// Open local bytes selected by the reference while checking its current row.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
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
        start_call(self.inner.clone(), move |inner| inner.read(read))
    }

    /// The committed app schema version.
    pub async fn schema_version(&self) -> Result<u32, DbError> {
        self.call(|inner| inner.schema_version()).await
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
    tokio::task::spawn_blocking(move || {
        let slot = inner.read().expect("database lock poisoned");
        run(slot.as_ref().ok_or(DbError::StoreClosed)?)
    })
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
