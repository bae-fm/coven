//! Opening is the composition root; a shared handle owns all connections and
//! the writer lock. Blocking calls keep that owner alive until they finish.

use std::sync::{Arc, Condvar, Mutex, RwLock};

use coven_foundation::clock::{ClockRef, SystemClock};
use coven_foundation::files::{StoreDir, StoreLock};
use coven_foundation::id_source::{CircleId, DeviceId, IdSourceRef, UuidIds};

use crate::authorization::SqlAuthorization;
use crate::observation::{CommitObserver, CommitSubscription, ReadSet};
use crate::sqlite::DatabaseConnection;
use crate::{
    CovenError, CovenMigrationPolicy, CovenResult, DbError, Migration, MigrationOutcome,
    SyncedTable,
};
use crate::{LiveQuery, LostValue, Read, ReconfigurableLiveQuery, SqlReadContext};

/// Choices needed to open the database part of a store.
pub struct DatabaseBuilder {
    directory: StoreDir,
    tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    policy: Option<CovenMigrationPolicy>,
    clock: Option<ClockRef>,
    ids: Option<IdSourceRef>,
}

impl DatabaseBuilder {
    /// Begin an open using a directory supplied by the store layout.
    pub fn new(directory: StoreDir) -> Self {
        Self {
            directory,
            tables: None,
            migrations: None,
            policy: None,
            clock: None,
            ids: None,
        }
    }

    /// The tables that sync, described at the newest app schema version.
    pub fn synced_tables(mut self, tables: Vec<SyncedTable>) -> Self {
        self.tables = Some(tables);
        self
    }

    /// The complete app migration sequence, numbered from one.
    pub fn migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.migrations = Some(migrations);
        self
    }

    /// Required for a writable open; read-only opens always refuse migration.
    pub fn coven_migration_policy(mut self, policy: CovenMigrationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// The clock used when stamping a local write (§7.2).
    pub fn clock(mut self, clock: ClockRef) -> Self {
        self.clock = Some(clock);
        self
    }

    /// The source of fresh names for this store's owned file bytes (§16.6).
    pub fn id_source(mut self, ids: IdSourceRef) -> Self {
        self.ids = Some(ids);
        self
    }

    /// Open one writer under the store lock and four read-only connections.
    pub async fn open(self) -> CovenResult<Database> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_graph()).await)
    }

    /// Open read-only connections without locking or migrating, including from
    /// a second process while the store's writer is open.
    pub async fn open_read_only(self) -> CovenResult<CovenReadHandle> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_read_graph()).await)
    }

    fn open_graph(self) -> CovenResult<Database> {
        let tables = self.tables.ok_or(CovenError::MissingConfiguration {
            field: "synced_tables",
        })?;
        let migrations = self.migrations.ok_or(CovenError::MissingConfiguration {
            field: "migrations",
        })?;
        let policy = self.policy.ok_or(CovenError::MissingConfiguration {
            field: "coven_migration_policy",
        })?;
        // Refuse a directory that is not a store before creating its database.
        let settings = self.directory.settings()?;
        let clock = match self.clock {
            Some(clock) => clock,
            None => Arc::new(SystemClock),
        };
        let ids = match self.ids {
            Some(ids) => ids,
            None => Arc::new(UuidIds),
        };
        let lock = self.directory.lock_exclusive()?;
        let path = self.directory.database_path();
        let mut writer = DatabaseConnection::open(&path, false, SqlAuthorization::new(&tables))?;
        #[cfg(test)]
        let profile = writer.profile_statements();
        writer.check_integrity()?;
        writer.enable_wal()?;
        let migrations = writer.prepare_schema(
            &tables,
            &migrations,
            policy,
            Some((settings.device_id, clock.now())),
        )?;
        crate::file_removals::FileRemovals::new(&writer, &self.directory).finish(Ok(()))?;
        let write_schema = crate::write_schema::WriteSchema::read(&writer, tables.clone())?;
        writer.prepare_file_triggers()?;
        let observer = CommitObserver::new();
        #[cfg(test)]
        drop(profile);
        writer.observe_commits(observer.clone())?;
        let mut readers = Vec::new();
        for _ in 0..4 {
            readers.push(Mutex::new(DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::new(&tables),
            )?));
        }
        Ok(Database {
            inner: Arc::new(RwLock::new(Some(DatabaseInner {
                observer,
                writer: Mutex::new(writer),
                readers: ReadPool::new(readers),
                migrations,
                lock,
                write_schema,
                directory: self.directory,
                device: settings.device_id,
                clock,
                ids,
            }))),
        })
    }
    fn open_read_graph(self) -> CovenResult<CovenReadHandle> {
        let tables = self.tables.ok_or(CovenError::MissingConfiguration {
            field: "synced_tables",
        })?;
        let migrations = self.migrations.ok_or(CovenError::MissingConfiguration {
            field: "migrations",
        })?;
        self.directory.settings()?;
        let path = self.directory.database_path();
        let first = DatabaseConnection::open(&path, true, SqlAuthorization::new(&tables))?;
        first.check_integrity()?;
        first.prepare_schema(
            &tables,
            &migrations,
            CovenMigrationPolicy::RefusePending,
            None,
        )?;
        let schema = crate::write_schema::WriteSchema::read(&first, tables.clone())?;
        let mut readers = vec![Mutex::new(first)];
        for _ in 1..4 {
            readers.push(Mutex::new(DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::new(&tables),
            )?));
        }
        Ok(CovenReadHandle {
            inner: Arc::new(RwLock::new(Some(ReadOnlyInner {
                readers: ReadPool::new(readers),
                schema,
            }))),
        })
    }
}

/// Shared ownership of the database. Closing any clone closes them all.
#[derive(Clone)]
pub struct Database {
    inner: Arc<RwLock<Option<DatabaseInner>>>,
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
}

impl Database {
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
        let database = self.clone();
        let runtime = tokio::runtime::Handle::current();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                let mut files = crate::file_write::FileWrite::new(
                    &writer,
                    &inner.directory,
                    &inner.write_schema,
                    inner.device,
                    inner.ids.as_ref(),
                );
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut batch = crate::WriteBatch::new();
                    build(&mut batch)?;
                    files.stage(batch, &runtime)?;
                    writer.local_write(
                        &inner.write_schema,
                        inner.device,
                        inner.clock.now(),
                        &files,
                        sql,
                    )
                }));
                let result = match result {
                    Ok(result) => files.finish(result),
                    Err(panic) => {
                        let cleanup = files.rollback();
                        drop(writer);
                        if let Err(error) = cleanup {
                            panic!("file cleanup after app panic failed: {error:?}");
                        }
                        std::panic::resume_unwind(panic)
                    }
                };
                drop(writer);
                result
            })
            .await,
        )
    }

    /// Apply one authenticated download, or report the prerequisite it awaits.
    /// Row changes, merge state, fingerprints and positions commit together.
    pub async fn apply_downloaded(
        &self,
        write: crate::DownloadedWrite,
    ) -> Result<crate::ApplyOutcome, DbError> {
        let database = self.clone();
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
                    inner.ids.as_ref(),
                );
                files.finish(crate::download::apply(
                    &writer,
                    &inner.write_schema,
                    inner.clock.now(),
                    write,
                    &files,
                ))
            })
            .await,
        )
    }

    /// Record an applied breaking change's snapshot coverage. Returns false on repetition.
    /// Loading the snapshot's rows is a separate operation.
    pub async fn apply_breaking_change(
        &self,
        version: u32,
        included: coven_format::value::WritePositions,
    ) -> Result<bool, DbError> {
        self.apply_boundary(crate::write_boundary::WriteBoundary::SchemaChange {
            version,
            included,
        })
        .await
    }

    /// Record an applied reset's snapshot coverage. Returns false on repetition.
    /// Loading the snapshot's rows is a separate operation.
    pub async fn apply_reset(
        &self,
        entry: crate::EntryId,
        audience: coven_merge::Audience,
        included: coven_format::value::WritePositions,
    ) -> Result<bool, DbError> {
        self.apply_boundary(crate::write_boundary::WriteBoundary::Reset {
            entry,
            audience,
            included,
        })
        .await
    }

    async fn apply_boundary(
        &self,
        boundary: crate::write_boundary::WriteBoundary,
    ) -> Result<bool, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                boundary.record(&writer)
            })
            .await,
        )
    }

    /// Apply a store-log circle deletion atomically. Returns false on repetition.
    pub async fn delete_circle(&self, circle: CircleId) -> Result<bool, DbError> {
        let database = self.clone();
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
                    inner.ids.as_ref(),
                );
                files.finish(crate::download::delete_circle(
                    &writer,
                    &inner.write_schema,
                    circle,
                    &files,
                ))
            })
            .await,
        )
    }

    /// Read the schema version, positions and fingerprints from one committed state.
    /// Supply fresh hashers derived from the store or circle keys. Audiences whose
    /// keys are unavailable can be omitted without interrupting sum maintenance.
    pub async fn sync_state(
        &self,
        keys: Vec<(coven_merge::Audience, coven_crypto::FingerprintHasher)>,
    ) -> Result<crate::SyncState, DbError> {
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
                let slot = database.inner.read().expect("database lock poisoned");
                let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
                let writer = inner
                    .writer
                    .lock()
                    .expect("writer connection lock poisoned");
                let schema_version = writer.schema_version()?;
                let positions = crate::download::positions(&writer)?;
                let fingerprints = keys
                    .into_iter()
                    .map(|(audience, key)| {
                        crate::fingerprint::read(&writer, &audience, key)
                            .map(|value| (audience, value))
                    })
                    .collect::<Result<_, _>>()?;
                Ok(crate::SyncState {
                    schema_version,
                    positions,
                    fingerprints,
                })
            })
            .await,
        )
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
        let database = self.clone();
        finish_blocking(
            tokio::task::spawn_blocking(move || {
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

/// A database open that cannot write or migrate. It owns no writer or store lock.
///
/// ```compile_fail
/// async fn cannot_write(handle: &coven_database::CovenReadHandle) {
///     handle.write(|_| Ok(())).await;
/// }
/// ```
/// ```compile_fail
/// fn cannot_subscribe(handle: &coven_database::CovenReadHandle) {
///     handle.subscribe(|_| Ok(()));
/// }
/// ```
/// ```compile_fail
/// fn cannot_reconfigure(handle: &coven_database::CovenReadHandle) {
///     handle.subscribe_reconfigurable((), |_, _| Ok(()));
/// }
/// ```
/// ```compile_fail
/// fn cannot_subscribe_losses(handle: &coven_database::CovenReadHandle) {
///     handle.subscribe_lost_values();
/// }
/// ```
#[derive(Clone)]
pub struct CovenReadHandle {
    inner: Arc<RwLock<Option<ReadOnlyInner>>>,
}

struct ReadOnlyInner {
    readers: ReadPool,
    schema: crate::write_schema::WriteSchema,
}

impl CovenReadHandle {
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
                let failures = readers.readers.close();
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

struct ReadPool {
    readers: Vec<Mutex<DatabaseConnection>>,
    idle_readers: Mutex<Vec<usize>>,
    reader_ready: Condvar,
}

impl ReadPool {
    fn new(readers: Vec<Mutex<DatabaseConnection>>) -> Self {
        Self {
            idle_readers: Mutex::new((0..readers.len()).collect()),
            readers,
            reader_ready: Condvar::new(),
        }
    }

    fn close(self) -> Vec<DbError> {
        let mut failures = Vec::new();
        for reader in self.readers {
            if let Err(error) = reader
                .into_inner()
                .expect("reader connection lock poisoned")
                .close()
            {
                failures.push(error);
            }
        }
        failures
    }

    fn acquire_reader(&self) -> ReaderLease<'_> {
        let mut idle = self.idle_readers.lock().expect("reader pool poisoned");
        loop {
            if let Some(index) = idle.pop() {
                return ReaderLease {
                    database: self,
                    index,
                };
            }
            idle = self.reader_ready.wait(idle).expect("reader pool poisoned");
        }
    }
}

// A call reserves one available reader while borrowing the database owner.
// Dropping the reservation wakes a caller even when the call panics.
struct ReaderLease<'a> {
    database: &'a ReadPool,
    index: usize,
}

impl ReaderLease<'_> {
    fn read_snapshot<F, R, E>(&self, read: F) -> (Result<R, E>, ReadSet)
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E>,
        E: From<DbError>,
    {
        let reader = self.database.readers[self.index]
            .lock()
            .expect("read connection lock poisoned");
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.read_snapshot(read)));
        // The read transaction rolls back while unwinding. Release the mutex
        // before propagating the app panic so the pool can reuse this reader.
        drop(reader);
        match result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn schema_version(&self) -> Result<u32, DbError> {
        self.database.readers[self.index]
            .lock()
            .expect("read connection lock poisoned")
            .schema_version()
    }
}

impl Drop for ReaderLease<'_> {
    fn drop(&mut self) {
        self.database
            .idle_readers
            .lock()
            .expect("reader pool poisoned")
            .push(self.index);
        self.database.reader_ready.notify_one();
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
