//! Compose locked writers and read-only connections at the database boundary.

use super::*;
use crate::sqlite::StoreSql;
use coven_foundation::files::StoreDirAccess;
use sqlive::{Database, Options};
use std::num::NonZeroUsize;

/// The read-only connections of a store's database beside its writer.
const READERS: NonZeroUsize = NonZeroUsize::new(4).expect("four is not zero");

use crate::{migration::MigrationOperation, AutoVacuum, NewOperation, OpenError, OpenStep};
use coven_foundation::progress::{ProgressChannel, ProgressReport, ProgressWatch};

/// What opening the store took that a rebuild needs again: the app's
/// newest table declarations, migrations, namespaces and SQLite choices (§18.3).
#[derive(Clone)]
pub(crate) struct OpenInputs {
    pub(crate) tables: Vec<SyncedTable>,
    pub(crate) migrations: Vec<Migration>,
    pub(crate) namespaces: Vec<crate::Namespace>,
    pub(crate) sqlite: crate::SqliteSettings,
    pub(crate) functions: crate::SqlFunctions,
    pub(crate) sync_functions: Option<crate::SyncFunctions>,
}

/// Choices needed to open the database part of a store.
pub struct DatabaseBuilder {
    directory: StoreDir,
    tables: Option<Vec<SyncedTable>>,
    namespaces: Vec<crate::Namespace>,
    migrations: Option<Vec<Migration>>,
    migration_operation: Option<Box<MigrationOperation>>,
    joining_write: Option<Box<crate::joining_write::JoiningHook>>,
    before_commit: Option<Box<crate::before_commit::BeforeCommit>>,
    installation: crate::migration_run::Installation,
    clock: Option<ClockRef>,
    ids: Option<IdSourceRef>,
    sqlite: crate::SqliteSettings,
    functions: crate::SqlFunctions,
    sync_functions: Option<crate::SyncFunctions>,
    progress: ProgressChannel<OpenStep>,
    #[cfg(test)]
    upkeep_threads: Option<usize>,
    #[cfg(any(test, feature = "test-utils"))]
    refuse_close: bool,
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
            joining_write: None,
            before_commit: None,
            installation: crate::migration_run::Installation::Own,
            clock: None,
            ids: None,
            sqlite: crate::SqliteSettings::default(),
            functions: crate::SqlFunctions::default(),
            sync_functions: None,
            progress: ProgressChannel::new(),
            #[cfg(test)]
            upkeep_threads: None,
            #[cfg(any(test, feature = "test-utils"))]
            refuse_close: false,
        }
    }

    /// Have SQLite refuse to close the opened store's writer.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn refuse_close(mut self) -> Self {
        self.refuse_close = true;
        self
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

    /// Open a rebuild's store for the migrations it hasn't run: their
    /// breaking migration writes are made under `header`, the migration write
    /// the device's real database queued for the rebuild (§18.3).
    pub(crate) fn rebuilding(mut self, header: coven_format::write::WriteHeader) -> Self {
        self.installation = crate::migration_run::Installation::Rebuilding(header);
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

    /// The app's write for a member's device as its registration applies
    /// (§13.2): it runs in the transaction that first applies this device's
    /// registration, as a local write, with the facts of its member's latest
    /// admission. Without one, nothing runs.
    pub fn joining_write<F>(mut self, write: F) -> Self
    where
        F: Fn(&crate::JoiningWrite<'_>, &crate::Joining) -> Result<(), DbError>
            + Send
            + Sync
            + 'static,
    {
        self.joining_write = Some(Box::new(write));
        self
    }

    /// The app's before-commit function (§9.4): coven runs it last in each
    /// transaction that changes the app's rows, a local write, a bulk write,
    /// an applied slot, a snapshot load or a migration as the store opens, so
    /// the app stores what its own rules compute from them in the same
    /// transaction. It writes device-local tables only, and its error fails
    /// the transaction. Without one, nothing runs.
    pub fn before_commit<F>(mut self, function: F) -> Self
    where
        F: Fn(&crate::CommitSql<'_>) -> Result<(), DbError> + Send + Sync + 'static,
    {
        self.before_commit = Some(Box::new(function));
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

    /// Sync's computations of the sync state and of each upload's phase,
    /// which `coven_sync_state` and `coven_upload_phases` call as
    /// `_coven_sync_state` and `_coven_upload_phase`, registered on every
    /// connection with the functions coven's views call (§17.9, §19.1).
    /// coven's own builder always gives them; a store opened without them
    /// fails each read of either view.
    pub fn sync_functions(mut self, functions: crate::SyncFunctions) -> Self {
        self.sync_functions = Some(functions);
        self
    }

    /// The sync functions that `open` received, or none, which a rebuild
    /// passes again when it opens its copy of the store.
    pub(crate) fn sync_functions_as_opened(
        mut self,
        functions: Option<crate::SyncFunctions>,
    ) -> Self {
        self.sync_functions = functions;
        self
    }

    /// Open one writer under the store lock and four read-only connections.
    /// Create coven's internal tables when the database holds none, then rebuild the indexes a new
    /// version of SQLite or of the app's collations and functions changes,
    /// then migrate the app's schema.
    pub async fn open(self) -> Result<StoreDatabase, OpenError> {
        let lock = self.directory.lock_for_open().await?;
        self.open_locked(lock).await
    }

    /// Open under a lock already held by the facade while checking restored
    /// device identity. The lock must protect this builder's directory.
    pub async fn open_locked(self, lock: StoreLock) -> Result<StoreDatabase, OpenError> {
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
    pub async fn open_read_only(self) -> Result<StoreDatabase<ReadOnly>, OpenError> {
        let bugs = self.directory.bugs().clone();
        finish_blocking(
            blocking_completing("open the store read-only", bugs, move || self.open_read_graph())
                .await,
        )
    }

    fn open_graph(self, lock: StoreLock) -> Result<StoreDatabase, OpenError> {
        let tables =
            self.tables.ok_or(OpenError::MissingConfiguration { field: "synced_tables" })?;
        let migrations =
            self.migrations.ok_or(OpenError::MissingConfiguration { field: "migrations" })?;
        coven_foundation::task::operation_step("checking the store's lock");
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
        let open_inputs = OpenInputs {
            tables: tables.clone(),
            migrations: migrations.clone(),
            namespaces: self.namespaces.clone(),
            sqlite: self.sqlite.clone(),
            functions: self.functions.clone(),
            sync_functions: self.sync_functions.clone(),
        };
        let path = self.directory.database_path();
        let whole = crate::internal_schema::whole_views();
        // Commits are reported here once the writer observes them, after
        // migrating; the transfers in flight report their starts and ends.
        let observer = store_observer(self.directory.bugs());
        #[cfg(test)]
        observer.refuse_upkeep_thread_after(self.upkeep_threads);
        let facts = crate::memory_facts::ProcessFacts::new(observer.clone());
        let views = |setup: &Setup<'_, crate::app_policy::CovenPolicy>| {
            crate::coven_functions::register_views(
                setup,
                settings.device_id,
                &facts,
                self.sync_functions.as_ref(),
            )
        };
        let recorded = crate::file_table::synced(&tables)
            .iter()
            .map(|table| table.name.to_ascii_lowercase())
            .collect();
        // The writer observes no commits while it opens, so the StoreCall
        // its opening runs with carries no device.
        let opening = crate::store_call::TestRecords::new();
        coven_foundation::task::operation_step("opening the store's database");
        // Every connection registers the functions coven's views call,
        // because the app's migrations may read the views.
        let setup = crate::app_policy::setup(&self.functions, Some(&views));
        let opened = Database::open_with(
            &path,
            observer.clone(),
            Options {
                settings: self.sqlite.clone(),
                readers: READERS,
                prepare_on_reader: false,
                whole_views: whole.clone(),
                policy: &|| CovenPolicy::new(&tables),
                setup: &setup,
                capture: Some(recorded),
                checkpoint_pages: self.sqlite.checkpoint_pages(),
                statistics: true,
            },
            |writer| -> Result<_, OpenError> {
                let call = &crate::store_call::StoreCall::new(None, &opening);
                let mut progress = ProgressReport::of(&self.progress, clock.clone());
                let migrating = clock.elapsed();
                coven_foundation::task::operation_step(
                    "preparing the schema and running migrations",
                );
                let outcomes = crate::schema_preparation::prepare_schema(
                    writer,
                    call,
                    &tables,
                    &migrations,
                    &self.functions.versions(),
                    crate::schema_preparation::Preparation::Writable {
                        author: crate::migration_run::MigrationOrigin(
                            settings.device_id,
                            clock.now(),
                            self.installation.clone(),
                        ),
                        operation: self.migration_operation.as_deref(),
                        audiences: &|live, call| {
                            crate::audience_database::AudienceDatabase::stage(
                                &self.directory,
                                &self.sqlite,
                                &self.functions,
                                self.sync_functions.as_ref(),
                                &observer,
                                settings.device_id,
                                &tables,
                                &whole,
                                live,
                                call,
                            )
                        },
                        before_commit: self.before_commit.as_deref(),
                    },
                    &mut progress,
                )?;
                coven_foundation::task::operation_step("reading the store's state after migrating");
                crate::database::file_database::declare_budgets(writer, &self.namespaces)?;
                crate::file_removals::finish_at_open(writer, call, &self.directory)?;
                let db = &*writer;
                crate::pending::forget_causes(db)?;
                // No connection plans until sync starts in this process.
                crate::internal_schema::retry_effect::save(
                    db,
                    crate::internal_schema::retry_effect::RetryState::OPENED,
                )?;
                let mut write_schema =
                    crate::write_schema::WriteSchema::read(db, call, tables.clone())?;
                write_schema.conversions = crate::migration_receive::read(db, &migrations)?;
                write_schema.namespaces = self.namespaces;
                crate::write_schema::prepare_evaluators(db, &write_schema)?;
                let migrated = clock.elapsed();
                coven_foundation::task::operation_step(
                    "gathering statistics, then starting commit capture and upkeep threads",
                );
                Ok((outcomes, write_schema, migrating, migrated))
            },
        );
        drop(setup);
        let (database, (outcomes, write_schema, migrating, migrated)) = opened?;
        let opened = clock.elapsed();
        #[cfg(any(test, feature = "test-utils"))]
        if self.refuse_close {
            database.host(|writer| writer.leave_statement_open());
        }
        // The input database holds only coven's tables, which no app setting
        // or function reaches. Its connections serve the store's handle, so a
        // failure in it fails the store.
        coven_foundation::task::operation_step("opening the input database");
        let functions = crate::SqlFunctions::default();
        let input_setup = crate::app_policy::setup(&functions, None);
        let (input, ()) = Database::open_with(
            &self.directory.input_database_path(),
            observer.clone(),
            Options {
                settings: coven_only_settings(),
                readers: NonZeroUsize::MIN,
                prepare_on_reader: false,
                whole_views: crate::internal_schema::whole_views(),
                policy: &|| CovenPolicy::new(&[]),
                setup: &input_setup,
                capture: None,
                checkpoint_pages: Some(sqlive::CHECKPOINT_PAGES),
                statistics: false,
            },
            |input| -> Result<(), DbError> {
                let version: u32 = input.query_row("PRAGMA application_id", [], |r| r.get(0))?;
                match version {
                    0 => input.transaction(|db| {
                        Ok::<_, DbError>(db.batch(&crate::internal_schema::initial_input_schema())?)
                    }),
                    1 => Ok(()),
                    _ => Err(DbError::DamagedDatabase),
                }
            },
        )?;
        // The database files' sizes as they opened, which a checkpoint
        // changes from now on (§17.8).
        let mut sizes = super::library_bytes::DatabaseSizes::new(
            database.checkpointed_size(),
            input.checkpointed_size(),
        );
        database.host(|writer| super::library_bytes::record_opened(writer, &mut sizes))?;
        // Record each archive that resets kept and that no row names yet
        // (§19.3).
        database.host(|writer| super::archive_bytes::record_opened(writer, &self.directory))?;
        let inputs = input_database::InputDatabase::new(input)?;
        tracing::info!(
            migrations = migrations.len(),
            migrating = ?(migrated - migrating),
            opening = ?(opened - migrated),
            total = ?(clock.elapsed() - started),
            "opened the database"
        );
        let store = StoreDatabase {
            observer: observer.clone(),
            closing: Arc::new(lock.closing_guard()),
            writer: WriterParts {
                facts: facts.clone(),
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
                    database,
                    observer,
                    write_schema,
                    directory: self.directory,
                    device: settings.device_id,
                    lock,
                    tests: crate::store_call::TestRecords::new(),
                    #[cfg(test)]
                    local_open_gate: Mutex::new(None),
                },
                migrations: outcomes,
                raise_operation: self.migration_operation,
                joining_write: self.joining_write,
                before_commit: self.before_commit,
                open_inputs,
                clock,
                ids,
                facts,
                #[cfg(test)]
                removal_gate: Mutex::new(None),
                #[cfg(test)]
                flush_hook: Mutex::new(None),
                #[cfg(any(test, feature = "test-utils"))]
                retry_end_failures: std::sync::atomic::AtomicUsize::new(0),
                #[cfg(any(test, feature = "test-utils"))]
                applied_slot_failures: std::sync::atomic::AtomicUsize::new(0),
            }))),
        };
        store.follow_database_sizes(sizes);
        Ok(store)
    }
    fn open_read_graph(self) -> Result<StoreDatabase<ReadOnly>, OpenError> {
        let tables =
            self.tables.ok_or(OpenError::MissingConfiguration { field: "synced_tables" })?;
        let migrations =
            self.migrations.ok_or(OpenError::MissingConfiguration { field: "migrations" })?;
        let lock = self.directory.lock_read_only()?;
        let settings = self.directory.settings()?;
        let path = self.directory.database_path();
        let whole = crate::internal_schema::whole_views();
        // Only the cache connection commits, and nothing subscribes to it.
        let observer = store_observer(self.directory.bugs());
        // A read-only handle transfers nothing: every file reads as waiting.
        // Nor does it watch the network: none is known (api.md E4).
        let facts = crate::memory_facts::ProcessFacts::new(observer.clone());
        let views = |setup: &Setup<'_, crate::app_policy::CovenPolicy>| {
            crate::coven_functions::register_views(
                setup,
                settings.device_id,
                &facts,
                self.sync_functions.as_ref(),
            )
        };
        // A read-only open runs no step, so nothing reaches `progress`.
        let clock = match self.clock {
            Some(clock) => clock,
            None => Arc::new(SystemClock),
        };
        let setup = crate::app_policy::setup(&self.functions, Some(&views));
        // The checks run on a reader. The cache connection, writable, opens
        // only once they pass.
        let (database, schema) = Database::open_with(
            &path,
            observer.clone(),
            Options {
                settings: self.sqlite.clone(),
                readers: READERS,
                prepare_on_reader: true,
                whole_views: whole,
                policy: &|| CovenPolicy::read_only(&tables),
                setup: &setup,
                capture: None,
                checkpoint_pages: None,
                statistics: false,
            },
            |first| {
                crate::store_call::outside_store(|call| {
                    crate::schema_preparation::prepare_schema(
                        first,
                        call,
                        &tables,
                        &migrations,
                        &self.functions.versions(),
                        crate::schema_preparation::Preparation::ReadOnly,
                        &mut ProgressReport::of(&self.progress, clock),
                    )
                })?;
                let mut schema = crate::store_call::outside_store(|call| {
                    crate::write_schema::WriteSchema::read(first, call, tables.clone())
                })?;
                schema.namespaces = self.namespaces;
                Ok::<_, OpenError>(schema)
            },
        )?;
        Ok(StoreDatabase {
            observer: observer.clone(),
            closing: Arc::new(lock.closing_guard()),
            writer: (),
            bugs: self.directory.bugs().clone(),
            inner: Arc::new(RwLock::new(Some(DatabaseAccess {
                database,
                observer,
                lock,
                write_schema: schema,
                directory: self.directory,
                device: settings.device_id,
                tests: crate::store_call::TestRecords::new(),
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
            let db = coven_only(&path, true, None)?;
            crate::operation::read(&db)
        })
        .await,
    )
}

/// The slot a store's database recorded when it stopped because another
/// installation uses its device id (§19.3), read in place and read-only on a
/// blocking thread, never opened as a store. A reset reads it before it
/// records its replacement. A database SQLite reports damaged is
/// `DamagedDatabase`. Like `discarded_operations`, it reads only coven's
/// table. A caller that stops waiting leaves the read to finish, and its
/// panic goes to `bugs` (E15).
pub async fn read_device_id_in_use(
    path: std::path::PathBuf,
    bugs: &coven_foundation::task::BugSinkRef,
) -> Result<Option<coven_merge::Slot>, DbError> {
    finish_blocking(
        blocking_completing("read the device id in use record", bugs.clone(), move || {
            let db = coven_only(&path, true, None)?;
            crate::device_reset::read_in_use(&db)
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
            let database = coven_only(&directory.database_path(), true, None)?;
            Ok(crate::store_state_tables::name(&database)?)
        })
        .await,
    )
}

/// What a closed store's sync state is computed from, which counts its unsent
/// work, in one committed snapshot of its database, read through a connection of
/// its own while `lock`, the store's writer lock, shows that no handle has it
/// open (api.md E1, E5). It reads only coven's tables, so the app's
/// declarations aren't needed; coven's tables must be at this coven's
/// version, as a read-only open requires. Nothing is sent or downloaded and
/// no network is watched, as for a read-only handle. A caller that stops
/// waiting leaves the read to finish, and its panic goes to the store's bug
/// sink (E15).
pub async fn closed_sync_facts<P: crate::PendingValue + Clone + Send + 'static>(
    directory: StoreDir,
    lock: coven_foundation::files::ClosedStoreLock,
) -> Result<crate::SyncFacts<P>, OpenError> {
    let bugs = directory.bugs().clone();
    finish_blocking(
        blocking_completing("read the closed store's sync status", bugs, move || {
            let _lock = lock;
            let settings = directory.settings()?;
            let observer = store_observer(directory.bugs());
            let facts = crate::memory_facts::ProcessFacts::new(observer);
            let views = |setup: &Setup<'_, crate::app_policy::CovenPolicy>| {
                // Sync computes a closed store's state from the facts in
                // Rust, and no app SQL reads the view here.
                crate::coven_functions::register_views(setup, settings.device_id, &facts, None)
            };
            let mut database = coven_only(&directory.database_path(), true, Some(&views))?;
            crate::store_call::outside_store(|call| {
                let database = &mut database;
                crate::schema_preparation::prepare_internal_schema(database)?;
                let db = &*database;
                let _constants = crate::transaction_constants::keep_constants(db, call);
                db.read_snapshot(|db| Ok::<_, OpenError>(crate::sync_facts::read(db)?))
            })
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
            let database = coven_only(&path, true, None)?;
            crate::store_state_tables::name(&database)
        })
        .await,
    )
}

/// The observer of a store's commits: no live query reads the tables coven
/// keeps beside its rows (`internal_schema::UNRECORDED`).
fn store_observer(bugs: &coven_foundation::task::BugSinkRef) -> StoreHandle {
    StoreHandle::new(
        bugs.clone(),
        crate::internal_schema::UNRECORDED.iter().map(|table| (*table).to_owned()).collect(),
    )
}

/// The settings of a database of coven's own tables only, such as the
/// input database or a discarded one: no app table, SQLite setting or
/// function reaches it.
fn coven_only_settings() -> crate::SqliteSettings {
    // The input database is a cache held to its byte budget (§21.1), and the
    // next received range reuses its freed pages; giving them back would
    // shrink and regrow the file.
    crate::SqliteSettings { auto_vacuum: Some(AutoVacuum::Off), ..Default::default() }
}

/// A lone connection to a database of coven's own tables only, such as a
/// discarded one, which registers coven's `views` when given.
pub(crate) fn coven_only(
    path: &std::path::Path,
    read_only: bool,
    views: Option<&crate::coven_functions::Views<'_>>,
) -> Result<StoreSql, DbError> {
    let functions = crate::SqlFunctions::default();
    let setup = crate::app_policy::setup(&functions, views);
    let connection = StoreSql::open(
        path,
        read_only,
        CovenPolicy::new(&[]),
        crate::internal_schema::whole_views(),
        &coven_only_settings(),
        &setup,
    )?;
    Ok(connection)
}

#[cfg(test)]
#[path = "database_builder_tests.rs"]
mod tests;
