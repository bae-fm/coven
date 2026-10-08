//! Compose locked writers and read-only connections at the database boundary.

use super::*;

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

    /// The source of fresh names for this store's owned file bytes (§16.6).
    pub fn id_source(mut self, ids: IdSourceRef) -> Self {
        self.ids = Some(ids);
        self
    }

    /// Open one writer under the store lock and four read-only connections.
    /// Always migrate coven's internal tables before the app's schema.
    pub async fn open(self) -> CovenResult<Database> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_graph(None, None)).await)
    }

    /// Open under a lock already held by the facade while checking restored
    /// device identity. The lock must protect this builder's directory.
    pub async fn open_locked(self, lock: StoreLock) -> CovenResult<Database> {
        finish_blocking(
            tokio::task::spawn_blocking(move || self.open_graph(Some(lock), None)).await,
        )
    }

    /// Start an explicitly requested replacement, archiving the damaged SQLite
    /// files and salvaging readable local work. The caller must have checked
    /// storage and unlocked keys, then reload and call `finish_recovery`.
    pub async fn open_reloading_locked(
        self,
        lock: StoreLock,
        archive: coven_foundation::files::FileName,
    ) -> CovenResult<Database> {
        finish_blocking(
            tokio::task::spawn_blocking(move || self.open_graph(Some(lock), Some(archive))).await,
        )
    }

    /// Open read-only connections under a shared deletion guard, without
    /// migrating, including from a second process while the writer is open.
    pub async fn open_read_only(self) -> CovenResult<DatabaseReadHandle> {
        finish_blocking(tokio::task::spawn_blocking(move || self.open_read_graph()).await)
    }

    fn open_graph(
        self,
        lock: Option<StoreLock>,
        archive: Option<coven_foundation::files::FileName>,
    ) -> CovenResult<Database> {
        let tables = self.tables.ok_or(CovenError::MissingConfiguration {
            field: "synced_tables",
        })?;
        let migrations = self.migrations.ok_or(CovenError::MissingConfiguration {
            field: "migrations",
        })?;
        // Refuse a directory that is not a store before creating its database.
        let lock = match lock {
            Some(lock) => {
                self.directory.verify_lock(&lock)?;
                lock
            }
            None => self.directory.lock_exclusive()?,
        };
        let mut recovery = match archive {
            Some(name) => Some(self.directory.recover_database(&lock, &name)?),
            None => {
                self.directory.check_database_recovery()?;
                None
            }
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
        let origin = if let Some(recovery) = recovery.as_ref().filter(|r| r.needs_salvage()) {
            writer.prepare_internal_schema()?;
            let supported = crate::migration::validate_versions(&migrations)?;
            let waiting_version = match DatabaseConnection::open(
                recovery.source_database_path(),
                true,
                SqlAuthorization::new(&tables),
            )
            .and_then(|source| {
                let version = source.schema_version()?;
                Ok((source, version))
            }) {
                Ok((source, version)) => {
                    if version > supported {
                        return Err(crate::MigrationError::SchemaTooNew {
                            current: version,
                            supported,
                        }
                        .into());
                    }
                    super::recovery::salvage(&source, &writer)?;
                    source.close()?;
                    Some(version)
                }
                Err(error) if super::recovery::damaged(&error) => {
                    tracing::warn!(%error, "damaged database cannot supply waiting work; archive retained");
                    None
                }
                Err(error) => return Err(error.into()),
            };
            crate::migration_run::MigrationOrigin::Snapshot { waiting_version }
        } else {
            crate::migration_run::MigrationOrigin::Device(settings.device_id, clock.now())
        };
        let migrations = writer.prepare_schema(
            &tables,
            &migrations,
            Some(origin),
            self.migration_operation.as_deref(),
        )?;
        if recovery.is_none() {
            crate::file_removals::FileRemovals::new(&writer, &self.directory, &BTreeSet::new())
                .finish(Ok::<_, DbError>(()))?;
        }
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
        if let Some(recovery) = &mut recovery {
            recovery.prepared().map_err(DbError::from)?;
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
                recovery,
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
        self.directory.check_database_recovery()?;
        let settings = self.directory.settings()?;
        let path = self.directory.database_path();
        let first = DatabaseConnection::open(&path, true, SqlAuthorization::new(&tables))?;
        first.check_integrity()?;
        first.prepare_schema(&tables, &migrations, None, None)?;
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
