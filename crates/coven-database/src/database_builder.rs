//! Compose locked writers and read-only connections at the database boundary.

use super::*;
use coven_foundation::files::StoreDirAccess;

use crate::{migration::MigrationOperation, NewOperation, OpenError, OpenStep};
use coven_foundation::progress::{ProgressChannel, ProgressReport, ProgressWatch};

/// Choices needed to open the database part of a store.
pub struct DatabaseBuilder {
    directory: StoreDir,
    tables: Option<Vec<SyncedTable>>,
    namespaces: Vec<crate::Namespace>,
    migrations: Option<Vec<Migration>>,
    migration_operation: Option<Box<MigrationOperation>>,
    installation: crate::migration_run::Installation,
    clock: Option<ClockRef>,
    ids: Option<IdSourceRef>,
    sqlite: crate::SqliteSettings,
    functions: crate::SqlFunctions,
    progress: ProgressChannel<OpenStep>,
    #[cfg(test)]
    upkeep_threads: Option<usize>,
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
            installation: crate::migration_run::Installation::Own,
            clock: None,
            ids: None,
            sqlite: crate::SqliteSettings::default(),
            functions: crate::SqlFunctions::default(),
            progress: ProgressChannel::new(),
            #[cfg(test)]
            upkeep_threads: None,
        }
    }

    /// Let `started` upkeep threads start, and have the system refuse the next.
    #[cfg(test)]
    pub(crate) fn refuse_upkeep_thread_after(mut self, started: usize) -> Self {
        self.upkeep_threads = Some(started);
        self
    }

    /// The steps a writable open reports as it runs them, each with how
    /// much of it is done (§4). A read-only open runs none.
    pub fn subscribe_progress(&self) -> ProgressWatch<OpenStep> {
        self.progress.watch()
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

    /// Open an installation that loads its store from storage next, for a
    /// join, restore or reset: its breaking migrations make no migration
    /// write, since the device isn't registered yet and the load replaces
    /// what they changed (§18.4).
    pub fn loading_store(mut self) -> Self {
        self.installation = crate::migration_run::Installation::Loading;
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
    pub async fn open(self) -> Result<Database, OpenError> {
        let lock = self.directory.lock_for_open().await?;
        self.open_locked(lock).await
    }

    /// Open under a lock already held by the facade while checking restored
    /// device identity. The lock must protect this builder's directory.
    pub async fn open_locked(self, lock: StoreLock) -> Result<Database, OpenError> {
        let closing = lock.closing_guard();
        let bugs = self.directory.bugs().clone();
        let result = finish_blocking(
            blocking_completing("open the store", bugs, move || self.open_graph(lock)).await,
        );
        // Cancellation drops the guard while SQLite can still be opening.
        // A returned database carries its own guard across its owning clones.
        closing.disarm();
        result
    }

    /// Open read-only connections under a shared deletion guard, without
    /// migrating or rebuilding, including from a second process while the
    /// writer is open.
    pub async fn open_read_only(self) -> Result<Database<ReadOnly>, OpenError> {
        let bugs = self.directory.bugs().clone();
        finish_blocking(
            blocking_completing("open the store read-only", bugs, move || self.open_read_graph())
                .await,
        )
    }

    fn open_graph(self, lock: StoreLock) -> Result<Database, OpenError> {
        let tables =
            self.tables.ok_or(OpenError::MissingConfiguration { field: "synced_tables" })?;
        let migrations =
            self.migrations.ok_or(OpenError::MissingConfiguration { field: "migrations" })?;
        // Refuse a directory that is not a store before creating its database.
        coven_foundation::files::verify_lock(&self.directory, &lock)?;
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
        let observer = CommitObserver::new(self.directory.bugs().clone());
        #[cfg(test)]
        observer.refuse_upkeep_thread_after(self.upkeep_threads);
        let transfers = crate::transfers_in_flight::TransfersInFlight::new(observer.clone());
        let network = crate::network_facts::NetworkFacts::new(observer.clone());
        let mut writer = DatabaseConnection::open(
            &path,
            false,
            SqlAuthorization::new(&tables),
            &self.sqlite,
            &self.functions,
        )?;
        // Before the app's migrations, which may read coven's views.
        writer.serve_views(settings.device_id, &transfers, &network)?;
        let (outcomes, write_schema, inputs, mut checkpoints, migrating, migrated, optimized) =
            writer.with_profile(|writer| -> Result<_, OpenError> {
                writer.enable_wal()?;
                let mut progress = ProgressReport::of(&self.progress, clock.clone());
                let migrating = clock.elapsed();
                let outcomes = writer.prepare_schema(
                    &tables,
                    &migrations,
                    &self.functions.versions(),
                    Some(crate::migration_run::MigrationOrigin(
                        settings.device_id,
                        clock.now(),
                        self.installation,
                    )),
                    self.migration_operation.as_deref(),
                    &mut progress,
                )?;
                crate::database::file_database::declare_budgets(writer, &self.namespaces)?;
                crate::file_removals::finish_at_open(writer, &self.directory)?;
                crate::pending::forget_causes(writer)?;
                // No connection plans until sync starts in this process.
                crate::internal_schema::retry_effect::save(
                    writer,
                    crate::internal_schema::retry_effect::RetryState::OPENED,
                )?;
                let mut write_schema =
                    crate::write_schema::WriteSchema::read(writer, tables.clone())?;
                write_schema.conversions = crate::migration_receive::read(writer, &migrations)?;
                write_schema.namespaces = self.namespaces;
                crate::write_schema::prepare_evaluators(writer, &write_schema)?;
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
                let mut input_writer = coven_only(&input_path, false)?;
                input_writer.enable_wal()?;
                let version: u32 =
                    input_writer.query_row("PRAGMA application_id", [], |r| r.get(0))?;
                match version {
                    0 => input_writer.transaction(|db| {
                        db.batch(&crate::internal_schema::initial_input_schema())
                    })?,
                    1 => (),
                    _ => return Err(DbError::DamagedDatabase.into()),
                }
                let mut input_reader = coven_only(&input_path, true)?;
                input_writer.serve(observer.clone());
                input_reader.serve(observer.clone());
                let input_writer = Arc::new(Mutex::new(input_writer));
                // Coven checkpoints both databases off the commit path from here on,
                // each on a connection of its own (§4); opening, with no reader yet,
                // checkpointed as it committed.
                let checkpoints = vec![crate::wal_checkpoints::WalCheckpointer::start(
                    coven_only(&input_path, false)?,
                    &input_writer,
                    crate::sqlite_settings::CHECKPOINT_PAGES,
                    observer.clone(),
                )?];
                let inputs = input_database::InputDatabase::new(input_reader, input_writer)?;
                Ok((outcomes, write_schema, inputs, checkpoints, migrating, migrated, optimized))
            })?;
        writer.observe_commits(observer.clone(), settings.device_id, &tables);
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
                observer.clone(),
            )?);
        }
        let statistics =
            crate::upkeep::UpkeepThread::<crate::planner_statistics::PlannerStatistics>::start(
                DatabaseConnection::open(
                    &path,
                    false,
                    SqlAuthorization::new(&tables),
                    &self.sqlite,
                    &self.functions,
                )?,
                &writer,
                observer.clone(),
            )?;
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
            reader.serve_views(settings.device_id, &transfers, &network)?;
            reader.serve(observer.clone());
            readers.push(Mutex::new(reader));
        }
        Ok(Database {
            observer: observer.clone(),
            closing: Arc::new(lock.closing_guard()),
            writer: WriterParts {
                transfers,
                network,
                file_tasks: Arc::new(tokio::sync::RwLock::new(())),
                #[cfg(test)]
                close_waits: Arc::new(Mutex::new(None)),
                #[cfg(test)]
                abandoned_stagings: Arc::new(Mutex::new(Vec::new())),
            },
            bugs: self.directory.bugs().clone(),
            inner: Arc::new(RwLock::new(Some(DatabaseInner {
                inputs,
                staging: Arc::new(crate::reserved_names::ReservedNames::new(observer.clone())),
                access: DatabaseAccess {
                    writer,
                    readers: ReadPool::new(readers),
                    observer,
                    write_schema,
                    directory: self.directory,
                    device: settings.device_id,
                    lock,
                    #[cfg(test)]
                    local_open_gate: Mutex::new(None),
                },
                checkpoints,
                statistics,
                migrations: outcomes,
                raise_operation: self.migration_operation,
                clock,
                ids,
                copying: Arc::new(Mutex::new(BTreeSet::new())),
                #[cfg(test)]
                removal_gate: Mutex::new(None),
                #[cfg(test)]
                flush_hook: Mutex::new(None),
                #[cfg(any(test, feature = "test-utils"))]
                retry_end_failures: std::sync::atomic::AtomicUsize::new(0),
                #[cfg(any(test, feature = "test-utils"))]
                applied_slot_failures: std::sync::atomic::AtomicUsize::new(0),
            }))),
        })
    }
    fn open_read_graph(self) -> Result<Database<ReadOnly>, OpenError> {
        let tables =
            self.tables.ok_or(OpenError::MissingConfiguration { field: "synced_tables" })?;
        let migrations =
            self.migrations.ok_or(OpenError::MissingConfiguration { field: "migrations" })?;
        let lock = self.directory.lock_read_only()?;
        let settings = self.directory.settings()?;
        let path = self.directory.database_path();
        // Only the cache connection commits, and nothing subscribes to it.
        let observer = CommitObserver::new(self.directory.bugs().clone());
        // A read-only handle transfers nothing: every file reads as waiting.
        let transfers = crate::transfers_in_flight::TransfersInFlight::new(observer.clone());
        // Nor does it watch the network: none is known (api.md E4).
        let network = crate::network_facts::NetworkFacts::new(observer.clone());
        let mut first = DatabaseConnection::open(
            &path,
            true,
            SqlAuthorization::read_only(&tables),
            &self.sqlite,
            &self.functions,
        )?;
        first.serve_views(settings.device_id, &transfers, &network)?;
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
            &mut ProgressReport::of(&self.progress, clock),
        )?;
        let mut schema = crate::write_schema::WriteSchema::read(&first, tables.clone())?;
        schema.namespaces = self.namespaces;
        first.serve(observer.clone());
        let mut readers = vec![Mutex::new(first)];
        for _ in 1..4 {
            let mut reader = DatabaseConnection::open(
                &path,
                true,
                SqlAuthorization::read_only(&tables),
                &self.sqlite,
                &self.functions,
            )?;
            reader.serve_views(settings.device_id, &transfers, &network)?;
            reader.serve(observer.clone());
            readers.push(Mutex::new(reader));
        }
        let mut writer = DatabaseConnection::open(
            &path,
            false,
            SqlAuthorization::read_only(&tables),
            &self.sqlite,
            &self.functions,
        )?;
        writer.serve_views(settings.device_id, &transfers, &network)?;
        writer.serve(observer.clone());
        Ok(Database {
            observer: observer.clone(),
            closing: Arc::new(lock.closing_guard()),
            writer: (),
            bugs: self.directory.bugs().clone(),
            inner: Arc::new(RwLock::new(Some(DatabaseAccess {
                writer: Arc::new(Mutex::new(writer)),
                readers: ReadPool::new(readers),
                observer,
                lock,
                write_schema: schema,
                directory: self.directory,
                device: settings.device_id,
                #[cfg(test)]
                local_open_gate: Mutex::new(None),
            }))),
        })
    }
}

/// The operation journal of a database a device reset discarded (§19.3), read
/// in place and read-only on a blocking thread, never opened as a store. A
/// journal SQLite reports damaged, or whose records don't decode, is
/// `DamagedDatabase`. Like `read_store_name`, it reads only coven's table and
/// needs none of the app's collations or functions. A caller that stops
/// waiting leaves the read to finish, and its panic goes to `bugs` (E15).
pub async fn discarded_operations(
    path: std::path::PathBuf,
    bugs: &coven_foundation::task::BugSinkRef,
) -> Result<Vec<crate::OperationRecord>, DbError> {
    finish_blocking(
        blocking_completing("read the discarded operations", bugs.clone(), move || {
            let db = coven_only(&path, true)?;
            crate::operation::read(&db)
        })
        .await,
    )
}

/// Read the name for a local store listing without migrations or a writer.
/// It reads only coven's `_coven_store_state`, and SQLite looks a collation
/// or function up only for a statement that uses it, so the app's aren't
/// needed. A caller that stops waiting leaves the read to finish, and its
/// panic goes to the store's bug sink (E15).
pub async fn read_store_name(directory: StoreDir) -> Result<String, OpenError> {
    let bugs = directory.bugs().clone();
    finish_blocking(
        blocking_completing("read the store name", bugs, move || {
            let _lock = directory.lock_read_only()?;
            let database = coven_only(&directory.database_path(), true)?;
            Ok(crate::store_state_tables::name(&database)?)
        })
        .await,
    )
}

/// The local creation name in a reset's discarded database. The reset owner
/// holds its directory lease while reading this setting. Like
/// `read_store_name`, it reads only `_coven_store_state` and needs none of
/// the app's collations or functions. A caller that stops waiting leaves
/// the read to finish, and its panic goes to `bugs` (E15).
pub async fn discarded_store_name(
    path: std::path::PathBuf,
    bugs: &coven_foundation::task::BugSinkRef,
) -> Result<String, DbError> {
    finish_blocking(
        blocking_completing("read the discarded store name", bugs.clone(), move || {
            let database = coven_only(&path, true)?;
            crate::store_state_tables::name(&database)
        })
        .await,
    )
}

/// A connection to a database of coven's own tables only, such as the input
/// database or a discarded one: no app table, SQLite setting or function
/// reaches it.
fn coven_only(path: &std::path::Path, read_only: bool) -> Result<DatabaseConnection, DbError> {
    DatabaseConnection::open(
        path,
        read_only,
        SqlAuthorization::new(&[]),
        &crate::SqliteSettings::default(),
        &crate::SqlFunctions::default(),
    )
}

#[cfg(test)]
#[path = "database_builder_tests.rs"]
mod tests;
