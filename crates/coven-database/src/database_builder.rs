//! Compose locked writers and read-only connections at the database boundary.

use super::*;
use coven_foundation::files::StoreDirAccess;

use crate::{migration::MigrationOperation, NewOperation};

/// Choices needed to open the database part of a store.
pub struct DatabaseBuilder {
    directory: StoreDir,
    tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    migration_operation: Option<Box<MigrationOperation>>,
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
            migration_operation: None,
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

    /// Let sync describe the operation that publishes a breaking migration batch.
    /// The database commits its step 1 with the schema, converted waiting writes
    /// and migration write. A factory error rolls all of them back. Standalone
    /// database users may omit this; the application composition root supplies it.
    pub fn migration_operation<F>(mut self, operation: F) -> Self
    where
        F: Fn(u32) -> Result<NewOperation, DbError> + Send + Sync + 'static,
    {
        self.migration_operation = Some(Box::new(operation));
        self
    }

    /// The clock used when stamping a local write (§7.2).
    pub fn clock(mut self, clock: ClockRef) -> Self {
        self.clock = Some(clock);
        self
    }

    /// The source of fresh names for this store's owned file bytes (Appendix C.3).
    pub fn id_source(mut self, ids: IdSourceRef) -> Self {
        self.ids = Some(ids);
        self
    }

    /// Open one writer under the store lock and four read-only connections.
    /// Always migrate coven's internal tables before the app's schema.
    pub async fn open(self) -> CovenResult<Database> {
        let lock = self.directory.lock_for_open().await?;
        self.open_locked(lock).await
    }

    /// Open under a lock already held by the facade while checking restored
    /// device identity. The lock must protect this builder's directory.
    pub async fn open_locked(self, lock: StoreLock) -> CovenResult<Database> {
        let closing = lock.closing_guard();
        let result =
            finish_blocking(tokio::task::spawn_blocking(move || self.open_graph(lock)).await);
        // Cancellation drops the guard while SQLite can still be opening.
        // A returned database carries its own guard across its owning clones.
        closing.disarm();
        result
    }

    /// Open read-only connections under a shared deletion guard, without
    /// migrating, including from a second process while the writer is open.
    pub async fn open_read_only(self) -> CovenResult<DatabaseReadHandle> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_read_graph()).await)
    }

    fn open_graph(self, lock: StoreLock) -> CovenResult<Database> {
        let tables = self.tables.ok_or(CovenError::MissingConfiguration {
            field: "synced_tables",
        })?;
        let migrations = self.migrations.ok_or(CovenError::MissingConfiguration {
            field: "migrations",
        })?;
        // Refuse a directory that is not a store before creating its database.
        self.directory.verify_lock(&lock)?;
        let settings = lock.settings()?;
        let clock = match self.clock {
            Some(clock) => clock,
            None => Arc::new(SystemClock),
        };
        let ids = match self.ids {
            Some(ids) => ids,
            None => Arc::new(UuidIds),
        };
        let path = self.directory.database_path();
        let mut writer = DatabaseConnection::open(&path, false, SqlAuthorization::new(&tables))?;
        #[cfg(test)]
        let profile = writer.profile_statements();
        writer.check_integrity()?;
        writer.enable_wal()?;
        let origin = crate::migration_run::MigrationOrigin(settings.device_id, clock.now());
        let outcomes = writer.prepare_schema(
            &tables,
            &migrations,
            Some(origin),
            self.migration_operation.as_deref(),
        )?;
        crate::database::file_database::declare_budgets(&writer, &tables)?;
        crate::file_removals::finish_at_open(&writer, &self.directory)?;
        let mut write_schema = crate::write_schema::WriteSchema::read(&writer, tables.clone())?;
        write_schema.conversions = crate::migration_receive::read(&writer, &migrations)?;
        write_schema.prepare(&writer)?;
        writer.prepare_file_triggers()?;
        let observer = CommitObserver::new();
        let input_path = self.directory.input_database_path();
        let mut input_writer =
            DatabaseConnection::open(&input_path, false, SqlAuthorization::new(&[]))?;
        input_writer.check_integrity()?;
        input_writer.enable_wal()?;
        let version: u32 = input_writer.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        match version {
            0 => input_writer
                .transaction(|db| db.batch(&crate::internal_schema::initial_input_schema()))?,
            1 => (),
            _ => return Err(DbError::DamagedDatabase.into()),
        }
        let mut input_reader =
            DatabaseConnection::open(&input_path, true, SqlAuthorization::new(&[]))?;
        input_writer.serve(observer.clone());
        input_reader.serve(observer.clone());
        let inputs = input_database::InputDatabase::new(input_reader, input_writer);
        #[cfg(test)]
        drop(profile);
        writer.observe_commits(observer.clone(), settings.device_id)?;
        let mut readers = Vec::new();
        for _ in 0..4 {
            let mut reader = DatabaseConnection::open(&path, true, SqlAuthorization::new(&tables))?;
            reader.serve(observer.clone());
            readers.push(Mutex::new(reader));
        }
        Ok(Database {
            closing: Arc::new(lock.closing_guard()),
            file_tasks: Arc::new(tokio::sync::RwLock::new(())),
            inner: Arc::new(RwLock::new(Some(DatabaseInner {
                inputs,
                access: DatabaseAccess {
                    writer: Mutex::new(writer),
                    readers: ReadPool::new(readers),
                    observer,
                    write_schema,
                    directory: self.directory,
                    device: settings.device_id,
                    lock,
                },
                migrations: outcomes,
                clock,
                ids,
                staging: Mutex::new(BTreeSet::new()),
                #[cfg(test)]
                removal_gate: Mutex::new(None),
                #[cfg(any(test, feature = "test-utils"))]
                retry_end_failures: std::sync::atomic::AtomicUsize::new(0),
            }))),
        })
    }
    fn open_read_graph(self) -> CovenResult<DatabaseReadHandle> {
        let tables = self.tables.ok_or(CovenError::MissingConfiguration {
            field: "synced_tables",
        })?;
        let migrations = self.migrations.ok_or(CovenError::MissingConfiguration {
            field: "migrations",
        })?;
        let lock = self.directory.lock_read_only()?;
        let settings = self.directory.settings()?;
        let path = self.directory.database_path();
        let mut first = DatabaseConnection::open(&path, true, SqlAuthorization::new(&tables))?;
        first.check_integrity()?;
        first.prepare_schema(&tables, &migrations, None, None)?;
        let schema = crate::write_schema::WriteSchema::read(&first, tables.clone())?;
        // Only the cache connection commits, and nothing subscribes to it.
        let observer = CommitObserver::new();
        first.serve(observer.clone());
        let mut readers = vec![Mutex::new(first)];
        for _ in 1..4 {
            let mut reader = DatabaseConnection::open(&path, true, SqlAuthorization::new(&tables))?;
            reader.serve(observer.clone());
            readers.push(Mutex::new(reader));
        }
        let mut writer = DatabaseConnection::open(&path, false, SqlAuthorization::new(&tables))?;
        writer.serve(observer.clone());
        Ok(DatabaseReadHandle {
            inner: Arc::new(RwLock::new(Some(DatabaseAccess {
                writer: Mutex::new(writer),
                readers: ReadPool::new(readers),
                observer,
                lock,
                write_schema: schema,
                directory: self.directory,
                device: settings.device_id,
            }))),
        })
    }
}

/// The operation journal of a database a device reset discarded (§19.3), read
/// in place and read-only on a blocking thread, never opened as a store. A
/// copy that fails SQLite's integrity check is `DamagedDatabase`.
pub async fn discarded_operations(
    path: std::path::PathBuf,
) -> Result<Vec<crate::OperationRecord>, DbError> {
    finish_blocking(
        tokio::task::spawn_blocking(move || {
            let db = DatabaseConnection::open(&path, true, SqlAuthorization::new(&[]))?;
            db.check_integrity()?;
            crate::operation::read(&db)
        })
        .await,
    )
}

/// Read the name for a local store listing without migrations or a writer.
pub async fn read_store_name(directory: StoreDir) -> CovenResult<String> {
    finish_blocking(
        tokio::task::spawn_blocking(move || {
            let _lock = directory.lock_read_only()?;
            let database = DatabaseConnection::open(
                &directory.database_path(),
                true,
                SqlAuthorization::new(&[]),
            )?;
            Ok(crate::store_state_tables::name(&database)?)
        })
        .await,
    )
}

/// The local creation name in a reset's discarded database. The reset owner
/// holds its directory lease while reading this setting.
pub async fn discarded_store_name(path: std::path::PathBuf) -> Result<String, DbError> {
    finish_blocking(
        tokio::task::spawn_blocking(move || {
            let database = DatabaseConnection::open(&path, true, SqlAuthorization::new(&[]))?;
            crate::store_state_tables::name(&database)
        })
        .await,
    )
}
