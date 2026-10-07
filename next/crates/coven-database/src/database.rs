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

#[cfg(any(test, feature = "test-utils"))]
#[path = "database_operations_tests.rs"]
mod operation_tests;

use coven_foundation::clock::{ClockRef, SystemClock};
use coven_foundation::files::{StoreDir, StoreLock, StoreReadLock};
use coven_foundation::id_source::{DeviceId, IdSourceRef, UuidIds};

use crate::authorization::SqlAuthorization;
use crate::observation::{CommitObserver, CommitSubscription, ReadSet};
use crate::sqlite::DatabaseConnection;
use crate::{
    CovenError, CovenMigrationPolicy, CovenResult, DbError, Migration, MigrationOutcome,
    SyncedTable,
};
use crate::{LiveQuery, LostValue, Read, ReconfigurableLiveQuery, SqlReadContext};

#[path = "database_builder.rs"]
mod builder;
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
    // Drop every SQLite connection before releasing the writer lock.
    writer: Mutex<DatabaseConnection>,
    readers: ReadPool,
    migrations: Vec<MigrationOutcome>,
    lock: StoreLock,
    write_schema: crate::write_schema::WriteSchema,
    directory: StoreDir,
    device: DeviceId,
    clock: ClockRef,
    ids: IdSourceRef,
    staging: Mutex<BTreeSet<coven_foundation::files::FileName>>,
}

impl Database {
    /// Open the source selected by a file reference, after checking its row.
    /// No storage operation occurs; nonlocal sources return their location.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
        let owner = self.clone();
        let reference = reference.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader.local_file(
                    &inner.write_schema,
                    &inner.directory,
                    inner.device,
                    &reference,
                )
            })
            .await,
        )
    }

    /// The row's file, audience and version from one committed state.
    pub async fn file_ref(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<crate::FileRef, DbError> {
        let owner = self.clone();
        let table = table.to_owned();
        let key = key.into();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader
                    .read_snapshot(|sql| sql.file_ref(&inner.write_schema, &table, &key))
                    .0
            })
            .await,
        )
    }

    /// The recorded original's path, size and modification time; never reread its bytes.
    pub async fn user_file(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<Option<crate::UserFile>, DbError> {
        let owner = self.clone();
        let table = table.to_owned();
        let key = key.into();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader
                    .read_snapshot(|sql| sql.user_file(&inner.write_schema, &table, &key))
                    .0
            })
            .await,
        )
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
    /// `coven_lost`; a removed row is deleted for good (§8, §20.4).
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()> {
        let database = self.clone();
        let values = values.to_vec();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                let files = crate::file_write::FileWrite::new(
                    &writer,
                    &inner.directory,
                    &inner.write_schema,
                    inner.device,
                    &inner.staging,
                    Vec::new(),
                );
                files
                    .finish(crate::dismissal::dismiss(
                        &writer,
                        &inner.write_schema,
                        inner.device,
                        inner.clock.now(),
                        &values,
                        &files,
                    ))
                    .map_err(Into::into)
            })
            .await,
        )
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
        let database = self.clone();
        tokio::task::spawn_blocking(move || database.run_read(read).0)
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
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let (result, reads) = database.run_read(read);
                commits.finish(reads);
                result
            })
            .await,
        )
    }

    fn run_read<F, R>(&self, read: F) -> (CovenResult<R>, ReadSet)
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R>,
    {
        let inner = self.inner.read().expect("database lock poisoned");
        let Some(inner) = inner.as_ref() else {
            return (Err(DbError::StoreClosed.into()), ReadSet::new());
        };
        let reader = inner.readers.acquire_reader();
        reader.read_snapshot(read)
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
    pub async fn write_with_files<F, S, R>(&self, build: F, sql: S) -> Result<R, DbError>
    where
        F: FnOnce(&mut crate::WriteBatch) -> Result<(), DbError> + Send + 'static,
        S: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.write_with_files_result(build, sql).await
    }

    /// Run a write whose callbacks use the facade's error type, preserving that
    /// type alongside any rollback or byte-cleanup failure.
    pub async fn write_with_files_result<F, S, R, E>(&self, build: F, sql: S) -> Result<R, E>
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
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let inner = database.inner.read().expect("database lock poisoned");
                let inner = inner.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader.schema_version()
            })
            .await,
        )
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
                let DatabaseInner {
                    writer,
                    readers,
                    lock,
                    ..
                } = inner;
                let mut failures = readers.close();
                if let Err(error) = writer
                    .into_inner()
                    .expect("writer connection lock poisoned")
                    .close()
                {
                    failures.push(error);
                }
                drop(lock);
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(DbError::Closing { failures })
                }
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
    inner: Arc<RwLock<Option<ReadOnlyInner>>>,
}

struct ReadOnlyInner {
    cache_writer: Mutex<DatabaseConnection>,
    directory: StoreDir,
    device: DeviceId,
    readers: ReadPool,
    // Connections must drop before deletion is allowed, including implicit drop.
    lock: StoreReadLock,
    schema: crate::write_schema::WriteSchema,
}

impl DatabaseReadHandle {
    /// Open local bytes selected by the reference while checking its current row.
    pub async fn open_local_file(
        &self,
        reference: &crate::FileRef,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
        let owner = self.clone();
        let reference = reference.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader.local_file(&inner.schema, &inner.directory, inner.device, &reference)
            })
            .await,
        )
    }

    /// The row's file, audience and version from one committed state.
    pub async fn file_ref(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<crate::FileRef, DbError> {
        let owner = self.clone();
        let table = table.to_owned();
        let key = key.into();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader
                    .read_snapshot(|sql| sql.file_ref(&inner.schema, &table, &key))
                    .0
            })
            .await,
        )
    }

    /// The recorded original's path, size and modification time; never reread its bytes.
    pub async fn user_file(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<Option<crate::UserFile>, DbError> {
        let owner = self.clone();
        let table = table.to_owned();
        let key = key.into();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = owner.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let reader = inner.readers.acquire_reader();
                reader
                    .read_snapshot(|sql| sql.user_file(&inner.schema, &table, &key))
                    .0
            })
            .await,
        )
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
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            let inner = handle.inner.read().expect("database lock poisoned");
            let reader = inner
                .as_ref()
                .ok_or(DbError::StoreClosed)?
                .readers
                .acquire_reader();
            reader.read_snapshot(read).0
        })
    }

    /// The committed app schema version.
    pub async fn schema_version(&self) -> Result<u32, DbError> {
        let handle = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let inner = handle.inner.read().expect("database lock poisoned");
                let reader = inner
                    .as_ref()
                    .ok_or(DbError::StoreClosed)?
                    .readers
                    .acquire_reader();
                reader.schema_version()
            })
            .await,
        )
    }

    /// Wait for active calls and close every clone of this handle.
    pub async fn close(&self) -> Result<(), DbError> {
        let handle = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let mut slot = handle.inner.write().expect("database lock poisoned");
                let readers = slot.take().ok_or(DbError::StoreClosed)?;
                let mut failures = readers.readers.close();
                if let Err(error) = readers
                    .cache_writer
                    .into_inner()
                    .expect("cache connection lock poisoned")
                    .close()
                {
                    failures.push(error);
                }
                drop(readers.lock);
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(DbError::Closing { failures })
                }
            })
            .await,
        )
    }
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
