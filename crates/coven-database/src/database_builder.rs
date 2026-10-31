//! Compose locked writers and read-only connections at the database boundary.

use super::*;
use coven_foundation::files::StoreDirAccess;

use crate::{migration::MigrationOperation, NewOperation, OpenStep};
use coven_foundation::progress::{Progress, ProgressReport};
use tokio::sync::watch;

/// Choices needed to open the database part of a store.
pub struct DatabaseBuilder {
    directory: StoreDir,
    tables: Option<Vec<SyncedTable>>,
    namespaces: Vec<crate::Namespace>,
    migrations: Option<Vec<Migration>>,
    migration_operation: Option<Box<MigrationOperation>>,
    clock: Option<ClockRef>,
    ids: Option<IdSourceRef>,
    sqlite: crate::SqliteSettings,
    functions: crate::SqlFunctions,
    progress: watch::Sender<Option<Progress<OpenStep>>>,
}

impl DatabaseBuilder {
    /// Begin an open using a directory supplied by the store layout.
    pub fn new(directory: StoreDir) -> Self {
        Self {
            directory,
            tables: None,
            namespaces: Vec::new(),
            migrations: None,
            migration_operation: None,
            clock: None,
            ids: None,
            sqlite: crate::SqliteSettings::default(),
            functions: crate::SqlFunctions::default(),
            progress: watch::channel(None).0,
        }
    }

    /// The steps a writable open reports as it runs them, each with how
    /// much of it is done (§4). A read-only open runs none.
    pub fn subscribe_progress(&self) -> watch::Receiver<Option<Progress<OpenStep>>> {
        self.progress.subscribe()
    }

    /// The tables that sync, described at the newest app schema version.
    pub fn synced_tables(mut self, tables: Vec<SyncedTable>) -> Self {
        self.tables = Some(tables);
        self
    }

    /// The file namespaces, each with its kind and cache budget (§17.1).
    pub fn namespaces(mut self, namespaces: Vec<crate::Namespace>) -> Self {
        self.namespaces = namespaces;
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

    /// SQLite settings for every connection to the store's database; unset
    /// ones keep SQLite's defaults, except that `checkpoint_fullfsync` is on
    /// for macOS and iOS (§4).
    pub fn sqlite_settings(mut self, settings: crate::SqliteSettings) -> Self {
        self.sqlite = settings;
        self
    }

    /// The app's collations and scalar functions, registered on every
    /// connection to the store's database before any app SQL runs on it (§4).
    pub fn sql_functions(mut self, functions: crate::SqlFunctions) -> Self {
        self.functions = functions;
        self
    }

    /// Open one writer under the store lock and four read-only connections.
    /// Always migrate coven's internal tables, then rebuild the indexes a new
    /// version of SQLite or of the app's collations and functions changes,
    /// then migrate the app's schema.
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
    /// migrating or rebuilding, including from a second process while the
    /// writer is open.
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
        let started = clock.elapsed();
        let path = self.directory.database_path();
        // Commits are reported here once the writer observes them, after
        // migrating; the transfers in flight report their starts and ends.
        let observer = CommitObserver::new();
        let transfers = crate::transfers_in_flight::TransfersInFlight::new(observer.clone());
        let mut writer = DatabaseConnection::open(
            &path,
            false,
            SqlAuthorization::new(&tables),
            &self.sqlite,
            &self.functions,
        )?;
        writer.identify_device(settings.device_id)?;
        // Before the app's migrations, which may read coven's views.
        writer.serve_transfers(&transfers)?;
        #[cfg(test)]
        let profile = writer.profile_statements();
        writer.enable_wal()?;
        let mut progress = ProgressReport::new(self.progress, clock.clone());
        let migrating = clock.elapsed();
        let outcomes = writer.prepare_schema(
            &tables,
            &migrations,
            &self.functions.versions(),
            Some(crate::migration_run::MigrationOrigin(
                settings.device_id,
                clock.now(),
            )),
            self.migration_operation.as_deref(),
            &mut progress,
        )?;
        crate::database::file_database::declare_budgets(&writer, &self.namespaces)?;
        crate::file_removals::finish_at_open(&writer, &self.directory)?;
        let mut write_schema = crate::write_schema::WriteSchema::read(&writer, tables.clone())?;
        write_schema.conversions = crate::migration_receive::read(&writer, &migrations)?;
        write_schema.namespaces = self.namespaces;
        write_schema.prepare(&writer)?;
        let migrated = clock.elapsed();
        // After migrating, and before the writer captures commits, on a
        // connection of its own: one that analyzes a small table loads its
        // count, and forgetting the statistics afterwards doesn't unload it.
        // The writer then reads what that connection kept.
        DatabaseConnection::open(
            &path,
            false,
            SqlAuthorization::new(&tables),
            &self.sqlite,
            &self.functions,
        )?
        .gather_statistics()?;
        // SQLite reads its statistics again only when told to.
        writer.batch("ANALYZE sqlite_schema")?;
        let optimized = clock.elapsed();
        // The input database holds only coven's tables, which no app setting
        // or function reaches.
        let input_path = self.directory.input_database_path();
        let mut input_writer = DatabaseConnection::open(
            &input_path,
            false,
            SqlAuthorization::new(&[]),
            &crate::SqliteSettings::default(),
            &crate::SqlFunctions::default(),
        )?;
        input_writer.enable_wal()?;
        let version: u32 = input_writer.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        match version {
            0 => input_writer
                .transaction(|db| db.batch(&crate::internal_schema::initial_input_schema()))?,
            1 => (),
            _ => return Err(DbError::DamagedDatabase.into()),
        }
        let mut input_reader = DatabaseConnection::open(
            &input_path,
            true,
            SqlAuthorization::new(&[]),
            &crate::SqliteSettings::default(),
            &crate::SqlFunctions::default(),
        )?;
        input_writer.serve(observer.clone());
        input_reader.serve(observer.clone());
        let input_writer = Arc::new(Mutex::new(input_writer));
        // Coven checkpoints both databases off the commit path from here on,
        // each on a connection of its own (§4); opening, with no reader yet,
        // checkpointed as it committed.
        let mut checkpoints = vec![crate::wal_checkpoints::WalCheckpointer::start(
            DatabaseConnection::open(
                &input_path,
                false,
                SqlAuthorization::new(&[]),
                &crate::SqliteSettings::default(),
                &crate::SqlFunctions::default(),
            )?,
            &input_writer,
            crate::sqlite_settings::CHECKPOINT_PAGES,
        )?];
        let inputs = input_database::InputDatabase::new(input_reader, input_writer)?;
        #[cfg(test)]
        drop(profile);
        writer.observe_commits(observer.clone(), settings.device_id, &tables)?;
        let writer = Arc::new(Mutex::new(writer));
        if let Some(pages) = self.sqlite.checkpoint_pages() {
            let checkpointing = DatabaseConnection::open(
                &path,
                false,
                SqlAuthorization::new(&[]),
                &self.sqlite,
                &crate::SqlFunctions::default(),
            )?;
            checkpoints.push(crate::wal_checkpoints::WalCheckpointer::start(
                checkpointing,
                &writer,
                pages,
            )?);
        }
        tracing::info!(
            migrations = migrations.len(),
            migrating = ?(migrated - migrating),
            optimizing = ?(optimized - migrated),
            total = ?(clock.elapsed() - started),
            "opened the database"
        );
        let mut readers = Vec::new();
        for _ in 0..4 {
            let mut reader = DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::new(&tables),
                &self.sqlite,
                &self.functions,
            )?;
            reader.identify_device(settings.device_id)?;
            reader.serve_transfers(&transfers)?;
            reader.serve(observer.clone());
            readers.push(Mutex::new(reader));
        }
        Ok(Database {
            transfers,
            closing: Arc::new(lock.closing_guard()),
            file_tasks: Arc::new(tokio::sync::RwLock::new(())),
            inner: Arc::new(RwLock::new(Some(DatabaseInner {
                inputs,
                access: DatabaseAccess {
                    writer,
                    readers: ReadPool::new(readers),
                    observer,
                    write_schema,
                    directory: self.directory,
                    device: settings.device_id,
                    lock,
                },
                checkpoints,
                migrations: outcomes,
                clock,
                ids,
                staging: Mutex::new(BTreeSet::new()),
                copying: Mutex::new(BTreeSet::new()),
                #[cfg(test)]
                removal_gate: Mutex::new(None),
                #[cfg(test)]
                flush_hook: Mutex::new(None),
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
        // Only the cache connection commits, and nothing subscribes to it.
        let observer = CommitObserver::new();
        // A read-only handle transfers nothing: every file reads as waiting.
        let transfers = crate::transfers_in_flight::TransfersInFlight::new(observer.clone());
        let mut first = DatabaseConnection::open(
            &path,
            true,
            SqlAuthorization::new(&tables),
            &self.sqlite,
            &self.functions,
        )?;
        first.identify_device(settings.device_id)?;
        first.serve_transfers(&transfers)?;
        // A read-only open runs no step, so nothing reaches `progress`.
        let clock = match self.clock {
            Some(clock) => clock,
            None => Arc::new(SystemClock),
        };
        first.prepare_schema(
            &tables,
            &migrations,
            &self.functions.versions(),
            None,
            None,
            &mut ProgressReport::new(self.progress, clock),
        )?;
        let mut schema = crate::write_schema::WriteSchema::read(&first, tables.clone())?;
        schema.namespaces = self.namespaces;
        first.serve(observer.clone());
        let mut readers = vec![Mutex::new(first)];
        for _ in 1..4 {
            let mut reader = DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::new(&tables),
                &self.sqlite,
                &self.functions,
            )?;
            reader.identify_device(settings.device_id)?;
            reader.serve_transfers(&transfers)?;
            reader.serve(observer.clone());
            readers.push(Mutex::new(reader));
        }
        let mut writer = DatabaseConnection::open(
            &path,
            false,
            SqlAuthorization::new(&tables),
            &self.sqlite,
            &self.functions,
        )?;
        writer.identify_device(settings.device_id)?;
        writer.serve_transfers(&transfers)?;
        writer.serve(observer.clone());
        Ok(DatabaseReadHandle {
            transfers,
            inner: Arc::new(RwLock::new(Some(DatabaseAccess {
                writer: Arc::new(Mutex::new(writer)),
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
/// journal SQLite reports damaged, or whose records don't decode, is
/// `DamagedDatabase`. Like `read_store_name`, it reads only coven's table and
/// needs none of the app's collations or functions.
pub async fn discarded_operations(
    path: std::path::PathBuf,
) -> Result<Vec<crate::OperationRecord>, DbError> {
    finish_blocking(
        tokio::task::spawn_blocking(move || {
            let db = DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::new(&[]),
                &crate::SqliteSettings::default(),
                &crate::SqlFunctions::default(),
            )?;
            crate::operation::read(&db)
        })
        .await,
    )
}

/// Read the name for a local store listing without migrations or a writer.
/// It reads only coven's `_coven_store_state`, and SQLite looks a collation
/// or function up only for a statement that uses it, so the app's aren't
/// needed.
pub async fn read_store_name(directory: StoreDir) -> CovenResult<String> {
    finish_blocking(
        tokio::task::spawn_blocking(move || {
            let _lock = directory.lock_read_only()?;
            let database = DatabaseConnection::open(
                &directory.database_path(),
                true,
                SqlAuthorization::new(&[]),
                &crate::SqliteSettings::default(),
                &crate::SqlFunctions::default(),
            )?;
            Ok(crate::store_state_tables::name(&database)?)
        })
        .await,
    )
}

/// The local creation name in a reset's discarded database. The reset owner
/// holds its directory lease while reading this setting. Like
/// `read_store_name`, it reads only `_coven_store_state` and needs none of
/// the app's collations or functions.
pub async fn discarded_store_name(path: std::path::PathBuf) -> Result<String, DbError> {
    finish_blocking(
        tokio::task::spawn_blocking(move || {
            let database = DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::new(&[]),
                &crate::SqliteSettings::default(),
                &crate::SqlFunctions::default(),
            )?;
            crate::store_state_tables::name(&database)
        })
        .await,
    )
}

#[cfg(test)]
#[path = "database_builder_tests.rs"]
mod tests;
