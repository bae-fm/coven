//! Compose locked writers and read-only connections at the database boundary.

use super::*;

use crate::{migration::MigrationOperation, NewOperation};

/// Choices needed to open the database part of a store.
pub struct DatabaseBuilder {
    directory: StoreDir,
    tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    migration_operation: Option<Box<MigrationOperation>>,
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
            migration_operation: None,
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
        finish_blocking(tokio::task::spawn_blocking(move || self.open_graph(None)).await)
    }

    /// Open under a lock already held by the facade while checking restored
    /// device identity. The lock must protect this builder's directory.
    pub async fn open_locked(self, lock: StoreLock) -> CovenResult<Database> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_graph(Some(lock))).await)
    }

    /// Open read-only connections under a shared deletion guard, without
    /// migrating, including from a second process while the writer is open.
    pub async fn open_read_only(self) -> CovenResult<DatabaseReadHandle> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_read_graph()).await)
    }

    fn open_graph(self, lock: Option<StoreLock>) -> CovenResult<Database> {
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
        let lock = match lock {
            Some(lock) => {
                self.directory.verify_lock(&lock)?;
                lock
            }
            None => self.directory.lock_exclusive()?,
        };
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
        let migrations = writer.prepare_schema(
            &tables,
            &migrations,
            policy,
            Some((settings.device_id, clock.now())),
            self.migration_operation.as_deref(),
        )?;
        crate::file_removals::FileRemovals::new(&writer, &self.directory, &BTreeSet::new())
            .finish(Ok::<_, DbError>(()))?;
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
            file_tasks: Arc::new(tokio::sync::RwLock::new(())),
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
                staging: Mutex::new(BTreeSet::new()),
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
        let first = DatabaseConnection::open(&path, true, SqlAuthorization::new(&tables))?;
        first.check_integrity()?;
        first.prepare_schema(
            &tables,
            &migrations,
            CovenMigrationPolicy::RefusePending,
            None,
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
        Ok(DatabaseReadHandle {
            inner: Arc::new(RwLock::new(Some(ReadOnlyInner {
                cache_writer: Mutex::new(DatabaseConnection::open(
                    &path,
                    false,
                    SqlAuthorization::new(&tables),
                )?),
                readers: ReadPool::new(readers),
                lock,
                schema,
                directory: self.directory,
                device: settings.device_id,
            }))),
        })
    }
}
