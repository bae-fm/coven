//! The leaf SQLite capability. Its connection is never returned or borrowed out.

#[path = "app_statements.rs"]
mod app_statements;
#[path = "app_triggers.rs"]
mod app_triggers;
#[path = "bulk_write.rs"]
mod bulk_write;
#[path = "database_file_sync.rs"]
mod database_file_sync;
#[path = "event_capture.rs"]
mod event_capture;
#[path = "live_judging.rs"]
mod live_judging;
#[path = "local_write.rs"]
mod local_write;
#[path = "page_cache.rs"]
mod page_cache;
#[path = "pre_update_changes.rs"]
mod pre_update_changes;
#[path = "rolled_back.rs"]
mod rolled_back;
pub(crate) use pre_update_changes::ROWS_PER_TABLE;
pub(crate) use rowid_policy::rowid_guard;
#[path = "rowid_policy.rs"]
mod rowid_policy;
#[path = "stream_capture.rs"]
mod stream_capture;
#[path = "table_shapes.rs"]
pub(crate) mod table_shapes;
#[path = "transaction_capture.rs"]
mod transaction_capture;
#[path = "transaction_constants.rs"]
mod transaction_constants;
#[path = "write_transaction.rs"]
mod write_transaction;
use app_statements::StepsOf;
pub(crate) use event_capture::Event;
use transaction_capture::ChangeCapture;
use write_transaction::WriteTransaction;

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use coven_foundation::id_source::DeviceId;

use rusqlite::{fallible_iterator::FallibleIterator, Batch, Connection, OpenFlags, Params, Row};

use crate::authorization::SqlAuthorization;
use crate::internal_schema;
use crate::migration::validate_versions;
use crate::observation::CommitObserver;
use crate::schema::Schema;
use crate::{
    CovenMigrationError, DbError, Migration, MigrationError, MigrationOutcome, OpenError,
    SyncedTable,
};

/// Statements kept prepared per connection, coven's and the app's: a local
/// write runs about a hundred distinct ones of coven's, which a smaller cache
/// would evict before reuse, beside the app's.
const STATEMENT_CACHE: usize = 512;

pub(crate) struct DatabaseConnection {
    #[cfg(any(test, feature = "test-utils"))]
    work: std::cell::RefCell<Option<crate::test_utils::WorkCounter>>,
    connection: Connection,
    authorization: SqlAuthorization,
    /// Stops a lookup that runs too long (`live_judging.rs`).
    lookups: live_judging::LookupLimit,
    app_statements: app_statements::AppStatements,
    constants: transaction_constants::TransactionConstants,
    /// The open handle this connection serves: it receives the commits and
    /// failures after which the handle refuses calls. None while opening, when
    /// a failure fails the open and drops the connection instead.
    handle: Option<CommitObserver>,
    observation: Option<WriterObservation>,
    /// The main schema's tables as last read, with the schema they were
    /// read in (table_shapes.rs).
    shapes: std::cell::RefCell<Option<(i64, Arc<table_shapes::TableShapes>)>>,
    /// The writer's checkpointer, woken after each commit (§4).
    checkpoints: Option<crate::upkeep::Wake>,
    /// The writer's keeper of the planner's statistics, woken as its
    /// commits change rows (§4).
    statistics: Option<crate::planner_statistics::ChangeWake>,
    streaming: std::cell::Cell<bool>,
    #[cfg(any(test, feature = "test-utils"))]
    write_checkpoint: std::cell::RefCell<Option<crate::test_utils::WriteCheckpointObserver>>,
    #[cfg(test)]
    scans: Arc<std::sync::Mutex<Vec<tests::ProfiledStatement>>>,
    /// How many profiles are open: a profile inside another, as a
    /// migration's inside the open's, leaves the outer one tracing.
    #[cfg(test)]
    profiles: std::cell::Cell<usize>,
    #[cfg(test)]
    merge_loads: std::sync::Mutex<BTreeMap<String, usize>>,
    /// Each queued write's record and judgement, also of a refused one, for
    /// tests of its classification.
    #[cfg(test)]
    judged: std::cell::RefCell<Vec<(coven_format::write::WriteRecord, crate::judge::Judgement)>>,
    /// Panic in the next call of a write transaction's hook, as a bug in
    /// what it captures would.
    #[cfg(test)]
    hook_panics: Arc<std::sync::atomic::AtomicBool>,
}

struct WriterObservation {
    device: DeviceId,
    /// The synced tables, lowercase: a local write keeps their operations
    /// whole.
    synced: Arc<BTreeSet<String>>,
}

impl DatabaseConnection {
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn test_track_work(&self, counter: crate::test_utils::WorkCounter) {
        *self.work.borrow_mut() = Some(counter);
    }

    /// The last queued write's record and judgement, also of a refused one.
    #[cfg(test)]
    pub(crate) fn take_judged(
        &self,
    ) -> Option<(coven_format::write::WriteRecord, crate::judge::Judgement)> {
        self.judged.borrow_mut().pop()
    }

    #[cfg(test)]
    pub(crate) fn judged(
        &self,
        judged: (coven_format::write::WriteRecord, crate::judge::Judgement),
    ) {
        self.judged.borrow_mut().push(judged);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn record_work(&self, update: impl FnOnce(&mut crate::test_utils::WorkCounts)) {
        if let Some(counter) = self.work.borrow().as_ref() {
            counter.record(update);
        }
    }

    pub(crate) fn open(
        path: &Path,
        read_only: bool,
        authorization: SqlAuthorization,
        settings: &crate::SqliteSettings,
        functions: &crate::SqlFunctions,
    ) -> Result<Self, DbError> {
        let flags = if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        };
        // URI interpretation is deliberately absent: this is a StoreDir file,
        // never a caller-selected SQLite URI or in-memory database.
        crate::sqlite_temporary::opening()?;
        let path = crate::sqlite_path::sqlite_path(path)?;
        let mut connection =
            Connection::open_with_flags(&path, flags | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        connection.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)?;
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)?;
        // SQL that could damage the database file fails: writing a virtual
        // table's shadow tables directly, `PRAGMA writable_schema` and
        // `PRAGMA schema_version = N`, and turning the journal off (§4).
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
        // A statement's plan never reads the values bound to it. Otherwise
        // SQLite marks a statement whose plan looked at a bound value, and
        // the statement cache clearing its bindings as each use ends makes
        // SQLite parse and plan it again on its next use, once per row of a
        // commit's lookups.
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_QPSG, true)?;
        crate::coven_functions::register(&connection, &authorization)?;
        functions.register(&connection)?;
        connection.authorizer(Some(authorization.callback()))?;
        let lookups = live_judging::LookupLimit::install(&mut connection)?;
        let db = Self {
            #[cfg(any(test, feature = "test-utils"))]
            work: std::cell::RefCell::new(None),
            connection,
            authorization,
            lookups,
            app_statements: app_statements::AppStatements::default(),
            constants: transaction_constants::TransactionConstants::default(),
            handle: None,
            observation: None,
            shapes: std::cell::RefCell::new(None),
            checkpoints: None,
            statistics: None,
            streaming: std::cell::Cell::new(false),
            #[cfg(any(test, feature = "test-utils"))]
            write_checkpoint: std::cell::RefCell::new(None),
            #[cfg(test)]
            scans: Arc::new(std::sync::Mutex::new(Vec::new())),
            #[cfg(test)]
            profiles: std::cell::Cell::new(0),
            #[cfg(test)]
            merge_loads: std::sync::Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            judged: std::cell::RefCell::new(Vec::new()),
            #[cfg(test)]
            hook_panics: Arc::default(),
        };
        db.configure(settings, read_only)?;
        db.refresh_hidden_rowids()?;
        Ok(db)
    }

    /// Migrate local tables before app migrations or recovery's waiting work.
    /// Read-only connections require the supported internal version already.
    pub(crate) fn prepare_internal_schema(&mut self) -> Result<(), OpenError> {
        let internal: i32 = self.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        let internal = internal as u32;
        if internal > internal_schema::VERSION {
            return Err(CovenMigrationError::SchemaTooNew {
                current: internal,
                supported: internal_schema::VERSION,
            }
            .into());
        }
        if internal < internal_schema::VERSION {
            if self.connection.is_readonly(rusqlite::MAIN_DB)? {
                return Err(CovenMigrationError::ReadOnly.into());
            }
            // Only the current greenfield schema is supported. An uninitialized
            // database is version zero; an unknown newer schema is never rewritten.
            self.transaction(|db| {
                db.batch(&internal_schema::initial_schema())?;
                crate::sql_versions::record_library(db)?;
                db.batch(&format!("PRAGMA application_id = {}", internal_schema::VERSION))
            })
            .map_err(|source| CovenMigrationError::Failed { source: Box::new(source) })?;
        }
        Ok(())
    }

    /// Prepare the app's schema: on a writable connection, rebuild what a
    /// new version of SQLite or of the app's collations and functions
    /// changes, then migrate, reporting both steps to `progress`; on a
    /// read-only one, require the app's versions rebuilt and the migrations
    /// done, which reports nothing.
    pub(crate) fn prepare_schema(
        &mut self,
        tables: &[SyncedTable],
        migrations: &[Migration],
        versions: &[crate::sql_versions::SqlVersion],
        author: Option<crate::migration_run::MigrationOrigin>,
        operation: Option<&crate::migration::MigrationOperation>,
        audiences: Option<&crate::audience_database::Stage<'_>>,
        progress: &mut coven_foundation::progress::ProgressReport<crate::OpenStep>,
    ) -> Result<Vec<MigrationOutcome>, OpenError> {
        let read_only = author.is_none();
        let supported = validate_versions(migrations)?;
        self.prepare_internal_schema()?;
        // Before the app's migrations, whose statements use the indexes.
        if read_only {
            crate::sql_versions::require_built(self, versions)?;
        } else {
            crate::sql_versions::rebuild(self, tables, versions, progress)?;
        }
        let current = self.schema_version()?;
        if current > supported {
            return Err(MigrationError::SchemaTooNew { current, supported }.into());
        }
        // App declarations are inputs even when their SQL ran on an earlier open.
        // Every breaking step retained its schema; other committed steps did not.
        for migration in migrations.iter().filter(|m| m.version <= current) {
            let breaking = self.query_row(
                "SELECT EXISTS(SELECT 1 FROM _coven_conversion_schemas WHERE version=?1)",
                [migration.version],
                |r| r.get(0),
            )?;
            migration.check_kind(breaking)?;
        }
        if read_only || current == supported {
            let schema = Schema::read(self, &crate::file_table::declared(tables))?;
            schema.validate(tables)?;
            crate::schema::validate_local_keys(self, &schema, tables)?;
            self.check_followed_triggers()?;
            if let Some(table) =
                crate::row_queries::owner_declaration_changed(self, &schema, tables)?
            {
                return Err(DbError::Schema(crate::SchemaError::OwnerOnlyChanged { table }).into());
            }
            if let Some(table) = crate::file_table::changed(self, &schema, tables)? {
                return Err(
                    DbError::Schema(crate::SchemaError::FileColumnsChanged { table }).into()
                );
            }
            if !read_only {
                self.transaction(|db| crate::row_queries::register_schema(db, &schema, tables))?;
            }
            self.refresh_changed_rowids()?;
            return Ok(Vec::new());
        }
        let mut at = migrations.iter().find(|m| m.version > current).expect("pending migration");
        let publishes = author.is_some();
        let device = author.as_ref().map(|origin| origin.0);
        progress.begin(
            crate::OpenStep::Migrating,
            migrations.iter().filter(|m| m.version > current).count() as u64,
        );
        // A run with a breaking migration is checked against what each
        // audience alone would make of it, from the rows as they are now (§18.3).
        let pending: Vec<&Migration> = migrations.iter().filter(|m| m.version > current).collect();
        let mut staged = None;
        if let Some(stage) = audiences.filter(|_| {
            pending.iter().any(|m| matches!(m.kind, crate::migration::MigrationKind::Breaking(_)))
        }) {
            let first = pending[0];
            let before = stage(self);
            staged = Some(before.map_err(|source| MigrationError::Failed {
                version: first.version,
                name: first.name,
                source: Box::new(source),
            })?);
        }
        let result = self.with_profile(|db| {
            db.transaction(|db| {
                let crate::migration_run::MigrationRun { outcomes, marker } =
                    crate::migration_run::run(
                        db,
                        tables,
                        migrations,
                        current,
                        supported,
                        author.expect("writable migration"),
                        &mut at,
                        progress,
                    )?;
                if let Some(staged) = staged.as_mut() {
                    crate::audience_database::verify(staged, db, &pending, tables)?;
                }
                let breaking = outcomes
                    .iter()
                    .filter(|outcome| outcome.change == crate::MigrationChange::Breaking)
                    .map(|outcome| outcome.version)
                    .max();
                let raises = if let Some(minimum) = breaking {
                    // The raise snapshots are captured through the marker before any
                    // later app write; with no store there is no shared version to
                    // raise, and an installation that loads its store has no marker.
                    let schema = crate::write_schema::WriteSchema::read(db, tables.to_vec())?;
                    // A circle still waiting for its rebuild is rebuilt to this
                    // migration's version, under this migration's write.
                    if let Some(marker) = marker {
                        crate::reload_gate::follow(db, marker)?;
                    }
                    let captured = match marker {
                        Some(marker) => {
                            crate::raise_capture::capture(db, &schema, marker, supported)?
                        }
                        None => false,
                    };
                    crate::raise_capture::require(db, minimum)?;
                    if captured {
                        crate::raise_capture::publish_at(db, supported)?;
                    }
                    true
                } else {
                    // Only additions: a raise still waiting to publish moves to
                    // this version, captured again at it (§18.5).
                    let device = device.expect("writable migration");
                    let moved = crate::raise_capture::raise_to(db, tables, device, supported)?;
                    if moved {
                        crate::raise_capture::publish_at(db, supported)?;
                    }
                    moved
                };
                if raises {
                    if let Some(operation) = operation.filter(|_| publishes) {
                        crate::raise_capture::journal(db, operation, supported)?;
                    }
                }
                crate::row_queries::register_schema(
                    db,
                    &Schema::read(db, &crate::file_table::declared(tables))?,
                    tables,
                )?;
                db.refresh_hidden_rowids()?;
                Ok(outcomes)
            })
        });
        result.map_err(|source| {
            MigrationError::Failed { version: at.version, name: at.name, source: Box::new(source) }
                .into()
        })
    }

    /// The most arguments an SQL function call can pass on this connection.
    #[cfg(test)]
    pub(crate) fn function_argument_limit(&self) -> Result<usize, DbError> {
        Ok(self.connection.limit(rusqlite::limits::Limit::SQLITE_LIMIT_FUNCTION_ARG)? as usize)
    }

    /// Refuse a queued value before allocating it, using this connection's limit.
    pub(crate) fn check_value_length(
        &self,
        field: &'static str,
        length: u64,
    ) -> Result<usize, DbError> {
        let maximum = self.connection.limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH)? as u64;
        if length > maximum {
            return Err(DbError::TooLarge { field, actual: length, maximum });
        }
        Ok(length as usize)
    }

    pub(crate) fn upload_plaintext(&self, rowid: i64) -> Result<crate::UploadBytes<'_>, DbError> {
        let _scope = self.authorization.internal();
        Ok(crate::UploadBytes::new(self.connection.blob_open(
            rusqlite::MAIN_DB,
            "_coven_uploads",
            "record",
            rowid,
            true,
        )?))
    }

    /// Run `run` on this connection. Tests trace its statements, ending the
    /// profile on every path out of `run`, a panic's included, so a profile
    /// is never left open; one inside another nests (sqlite_tests.rs).
    pub(crate) fn with_profile<T>(&mut self, run: impl FnOnce(&mut Self) -> T) -> T {
        #[cfg(not(test))]
        return run(self);
        #[cfg(test)]
        {
            struct Profiled<'a>(&'a mut DatabaseConnection);
            impl Drop for Profiled<'_> {
                fn drop(&mut self) {
                    self.0.end_profile();
                }
            }
            self.start_profile();
            let profiled = Profiled(self);
            run(&mut *profiled.0)
        }
    }

    pub(crate) fn state_transaction<T>(
        &mut self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction(run).map_err(|error| self.pending_failure(error))
    }

    /// Give this connection to the store's database what coven's views read
    /// of this process: the installation it serves, for the views of members
    /// and devices (Appendix C.1); the files it is transferring, for
    /// `coven_uploads.sending`, `coven_sending` and `coven_downloading`; and
    /// the device's network as it last heard of it, for `coven_network` and
    /// `coven_uploads.metered` (§4).
    pub(crate) fn serve_views(
        &self,
        device: DeviceId,
        transfers: &crate::transfers_in_flight::TransfersInFlight,
        network: &crate::network_facts::NetworkFacts,
    ) -> Result<(), DbError> {
        crate::coven_functions::register_device(&self.connection, device)?;
        transfers.register(&self.connection)?;
        network.register(&self.connection)?;
        Ok(())
    }

    pub(crate) fn local_device(&self) -> Option<DeviceId> {
        self.observation.as_ref().map(|o| o.device)
    }

    pub(crate) fn pending_change<T>(
        &self,
        run: impl FnOnce() -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        run().map_err(|error| self.pending_failure(error))
    }

    /// Fail the handle when removing a transaction's `hook` failed: what the
    /// connection would call next is unknown. Removal never panics, since it
    /// runs as a transaction drops, perhaps during an unwind.
    pub(crate) fn removed_hook(&self, hook: &'static str, removed: rusqlite::Result<()>) {
        if let Err(error) = removed {
            tracing::error!(hook, %error, "removing a transaction's hook failed");
            let _failed = self.pending_failure(error.into());
        }
    }

    fn pending_failure(&self, error: DbError) -> DbError {
        if matches!(error, DbError::StatePersistence(_)) {
            return error;
        }
        let error = Arc::new(error);
        if let Some(handle) = &self.handle {
            handle.fail(crate::observation::HandleFailure::State(error.clone()));
        }
        DbError::StatePersistence(error)
    }

    pub(crate) fn batch(&self, sql: &str) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        self.connection.execute_batch(sql).map_err(Into::into)
    }

    /// The collation a column declares, BINARY when it names none.
    pub(crate) fn column_collation(&self, table: &str, column: &str) -> Result<String, DbError> {
        let _scope = self.authorization.internal();
        let (_, collation, _, _, _) =
            self.connection.column_metadata(Some("main"), table, column)?;
        Ok(collation.map_or_else(|| "BINARY".into(), |c| c.to_string_lossy().into_owned()))
    }

    /// The columns `sql` reads and the functions it calls, as SQLite
    /// resolves them while preparing it; it never runs.
    pub(crate) fn reads(&self, sql: &str) -> Result<crate::authorization::StatementReads, DbError> {
        let _scope = self.authorization.internal();
        let reads = self.authorization.observe_reads();
        self.connection.prepare(sql)?;
        Ok(reads.reads())
    }

    /// Mark ordinary materialization SQL so shared triggers stay suppressed.
    /// SQLite retains ownership of foreign keys, actions and deferred checks.
    pub(crate) fn materialize<T>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let _scope = self.authorization.applying();
        run(self)
    }

    pub(crate) fn internal_execute<P: Params>(
        &self,
        sql: &str,
        params: P,
    ) -> Result<usize, DbError> {
        let _scope = self.authorization.internal();
        let changed =
            self.with_statement(sql, |statement| statement.execute(params).map_err(DbError::from))?;
        self.bound_stream_cache()?;
        Ok(changed)
    }

    pub(crate) fn query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, DbError> {
        let _scope = self.authorization.internal();
        self.authorization.begin_read_statement();
        let result = self.with_statement(sql, |statement| {
            statement.query_row(params, map).map_err(DbError::from)
        });
        self.authorization.record_statement(&crate::row_scope::Scopes::default());
        result
    }

    pub(crate) fn query<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        mut map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>, DbError> {
        let mut values = Vec::new();
        self.for_each::<_, DbError>(sql, params, |row| {
            values.push(map(row)?);
            Ok(())
        })?;
        Ok(values)
    }

    /// Stream rows to a consumer with its own error type.
    pub(crate) fn for_each<P: Params, E: From<DbError>>(
        &self,
        sql: &str,
        params: P,
        mut visit: impl FnMut(&Row<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let _scope = self.authorization.internal();
        self.authorization.begin_read_statement();
        let result = self.with_statement(sql, |statement| {
            let mut rows = statement.query(params).map_err(DbError::from)?;
            while let Some(row) = rows.next().map_err(DbError::from)? {
                visit(row)?;
            }
            Ok(())
        });
        self.authorization.record_statement(&crate::row_scope::Scopes::default());
        result
    }

    /// Run coven's own SQL on the connection's cached statement, parsed once per
    /// connection. The authorizer records reads only while a statement is
    /// prepared, so while it records, the statement is prepared afresh.
    fn with_statement<T, E: From<DbError>>(
        &self,
        sql: &str,
        run: impl FnOnce(&mut rusqlite::Statement<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        if self.authorization.recording() {
            run(&mut self.trigger_judged(self.connection.prepare(sql))?)
        } else {
            let mut statement = self.trigger_judged(self.connection.prepare_cached(sql))?;
            // The statement profile reports each execution's own full scans.
            #[cfg(test)]
            statement.reset_status(rusqlite::StatementStatus::FullscanStep);
            run(&mut statement)
        }
    }

    pub(crate) fn migration_targets(&self, sql: &str) -> rusqlite::Result<BTreeSet<String>> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization.mutations(|| self.authorization.app_result(self.connection.prepare(sql)))
    }

    pub(crate) fn app_execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        let mut statement = self.authorization.app_result(self.app_statement(sql))?;
        let _scope = self.authorization.step();
        let changed = self.authorization.app_result(self.counting_steps(
            &mut statement,
            StepsOf::AppWrite,
            |statement| statement.execute(params),
        ))?;
        drop(statement);
        self.bound_stream_cache()?;
        Ok(changed)
    }

    pub(crate) fn app_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.app_batch_tracked(sql, None)
    }

    pub(crate) fn app_batch_tracked(
        &self,
        sql: &str,
        migration: Option<&crate::MigrationContext<'_>>,
    ) -> rusqlite::Result<()> {
        self.authorization.check_sql(sql)?;
        let mut batch = Batch::new(&self.connection, sql);
        loop {
            self.begin_app_statement()?;
            let Some(mut statement) = self.authorization.app_result(batch.next())? else {
                return Ok(());
            };
            let text = migration.map(|_| statement.expanded_sql().expect("prepared SQL"));
            if let (Some(migration), Some(text)) = (migration, text.as_ref()) {
                migration.before_statement(text)?;
            }
            let cookie = migration.map(|_| self.schema_cookie()).transpose()?;
            // Step every row so later execution errors cannot be discarded.
            let result = self.counting_steps(&mut statement, StepsOf::AppWrite, |statement| {
                let mut rows = statement.raw_query();
                let _scope = self.authorization.step();
                while rows.next()?.is_some() {}
                Ok(())
            });
            self.authorization.app_result(result)?;
            if let (Some(cookie), Some(migration), Some(text)) = (cookie, migration, text) {
                if self.schema_cookie()? != cookie {
                    migration.record(&text)?;
                }
            }
        }
    }

    /// SQLite's count of changes to the main schema, read afresh.
    pub(crate) fn schema_cookie(&self) -> rusqlite::Result<i64> {
        let _scope = self.authorization.internal();
        self.connection.prepare_cached("PRAGMA main.schema_version")?.query_row([], |r| r.get(0))
    }

    /// Run one migration step inside the open migration transaction. Which
    /// tables its statements write is decided as they are prepared (§18.1).
    pub(crate) fn migrating<T>(
        &self,
        run: impl FnOnce() -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        assert!(self.observation.is_none(), "migrations precede commit observation");
        let result = run()?;
        self.require_transaction()?;
        Ok(result)
    }

    pub(crate) fn app_query_row<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut map = Some(map);
        let mut value = None;
        self.app_rows(sql, params, |row| {
            let map = map.take().expect("the first row ends the read");
            value = Some(map(row)?);
            Ok(false)
        })?;
        value.ok_or(rusqlite::Error::QueryReturnedNoRows)
    }

    pub(crate) fn app_query<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        mut map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Vec<T>> {
        let mut values = Vec::new();
        self.app_rows(sql, params, |row| {
            values.push(map(row)?);
            Ok(true)
        })?;
        Ok(values)
    }

    /// Run an app statement and hand `each` its rows in order, under the
    /// authorizer's read and step scopes, until the rows end or `each`
    /// answers false.
    fn app_rows<P: Params>(
        &self,
        sql: &str,
        params: P,
        mut each: impl FnMut(&Row<'_>) -> rusqlite::Result<bool>,
    ) -> rusqlite::Result<()> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization.begin_read_statement();
        let result = (|| {
            let mut statement = self.app_statement(sql)?;
            let parameters = crate::sql_value::parameters(&self.connection, &statement, params)?;
            let plan = self.live_scope_plan(sql, &statement)?;
            let result = self.counting_steps(&mut statement, StepsOf::AppRead, |statement| {
                let mut rows = statement.query(rusqlite::params_from_iter(&parameters))?;
                loop {
                    let row = {
                        let _scope = self.authorization.step();
                        self.authorization.app_result(rows.next())?
                    };
                    let Some(row) = row else { return Ok(()) };
                    if !each(row)? {
                        return Ok(());
                    }
                }
            });
            self.record_app_statement(plan.as_deref(), &parameters);
            result
        })();
        self.authorization.app_result(result)
    }

    /// Wake `checkpoints` after each commit this writer makes (§4).
    pub(crate) fn signal_commits(&mut self, checkpoints: crate::upkeep::Wake) {
        self.checkpoints = Some(checkpoints);
    }

    /// Tell `statistics` of the rows each commit this writer makes changed
    /// (§4).
    pub(crate) fn signal_changes(&mut self, statistics: crate::planner_statistics::ChangeWake) {
        self.statistics = Some(statistics);
    }

    /// The rows this connection's statements have changed since it opened,
    /// their triggers' included, as SQLite counts them.
    pub(crate) fn changed_rows(&self) -> u64 {
        self.connection.total_changes()
    }

    /// Serve an open handle once opening succeeds (§4).
    pub(crate) fn serve(&mut self, handle: CommitObserver) {
        assert!(self.handle.is_none(), "a connection serves one handle");
        self.authorization.serve(handle.clone());
        self.handle = Some(handle);
    }

    /// Serve the handle as its writer, capturing each commit's row changes.
    pub(crate) fn observe_commits(
        &mut self,
        handle: CommitObserver,
        device: DeviceId,
        synced: &[SyncedTable],
    ) {
        assert!(self.observation.is_none(), "commit observation installed once");
        let synced = crate::file_table::synced(synced)
            .iter()
            .map(|table| table.name.to_ascii_lowercase())
            .collect();
        self.serve(handle);
        self.observation = Some(WriterObservation { device, synced: Arc::new(synced) });
    }

    pub(crate) fn lost_values(&self) -> Result<Vec<crate::LostValue>, DbError> {
        let records = self.query(
            &format!("{} ORDER BY l.id", crate::loss_record::SELECT),
            [],
            crate::loss_record::read,
        )?;
        records.into_iter().map(crate::LostValue::from_record).collect()
    }

    fn require_transaction(&self) -> Result<(), DbError> {
        if self.connection.is_autocommit() {
            Err(DbError::TransactionEnded)
        } else {
            Ok(())
        }
    }

    fn begin_app_statement(&self) -> rusqlite::Result<()> {
        self.require_transaction()
            .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
        // Migrations may create, rename or rebuild tables between app statements.
        // Ordinary writes reuse the set without querying SQLite's schema.
        self.refresh_changed_rowids()?;
        self.authorization.begin_app_call();
        Ok(())
    }

    pub(crate) fn transaction<T>(
        &mut self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction_captured(false, run)
    }

    /// Streaming transfers publish table invalidations without a session's
    /// retained before/after BLOB values growing with the transferred object.
    /// `transaction`, for a run whose failures are `E`: a failed rollback
    /// joins the run's own error.
    pub(crate) fn transaction_as<T, E: crate::WriteFailure>(
        &mut self,
        run: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        self.transaction_captured(false, run)
    }

    pub(crate) fn stream_transaction<T>(
        &mut self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction_captured(true, run)
    }

    fn transaction_captured<T, E: crate::WriteFailure>(
        &mut self,
        streaming: bool,
        run: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut transaction = WriteTransaction::begin(self, streaming)?;
        // A streamed transaction is observed by its operations; any other,
        // on a serving writer, by a session. The write-record session ends
        // before materialization; this one covers app SQL, merge metadata,
        // materialization and local tables.
        let observing = if streaming {
            stream_capture::StreamCapture::begin(&mut transaction).map(Observing::Stream)
        } else if transaction.within(|database| database.observation.is_some()) {
            transaction_capture::SupplementalChanges::observe(&mut transaction)
                .map(Observing::Session)
        } else {
            Ok(Observing::Nothing)
        };
        let outcome = observing.map_err(E::from).and_then(|observing| {
            transaction.within(|database| {
                let (stream, mut session) = match observing {
                    Observing::Stream(stream) => (Some(stream), None),
                    Observing::Session(supplemental) => {
                        (None, Some(ChangeCapture::begin(database, supplemental)?))
                    }
                    Observing::Nothing => (None, None),
                };
                let result = run(database)?;
                database.require_transaction()?;
                let changes = match (&mut session, &stream) {
                    (Some(session), _) => session.take_changes()?,
                    (None, Some(stream)) => match stream.changes() {
                        stream_capture::StreamChanges::Rows(rows) => rows,
                        stream_capture::StreamChanges::Tables(tables) => {
                            crate::change_capture::table_changes(database, tables)?
                        }
                    },
                    (None, None) => Vec::new(),
                };
                Ok((result, changes))
            })
        });
        transaction.commit(outcome)
    }

    pub(crate) fn close(self) -> Result<(), DbError> {
        self.connection.close().map_err(|(_connection, error)| error.into())
    }

    /// Prepare a statement and never finalize it, so SQLite refuses to close
    /// this connection, as it does with `SQLITE_BUSY`.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn leave_statement_open(&self) {
        let mut statement = std::ptr::null_mut();
        // SAFETY: the handle is this live connection's and the text is a
        // NUL-terminated literal. The statement is never used or finalized.
        let prepared = unsafe {
            rusqlite::ffi::sqlite3_prepare_v2(
                self.connection.handle(),
                c"SELECT 1".as_ptr(),
                -1,
                &mut statement,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(prepared, rusqlite::ffi::SQLITE_OK);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn on_write_checkpoint(
        &self,
        callback: impl Fn(crate::test_utils::WriteCheckpoint) + Send + 'static,
    ) {
        *self.write_checkpoint.borrow_mut() = Some(Box::new(callback));
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn checkpoint(&self, point: crate::test_utils::WriteCheckpoint) {
        if let Some(callback) = self.write_checkpoint.borrow().as_ref() {
            callback(point);
        }
    }
}

/// How a write transaction's changes are observed for live queries.
enum Observing {
    Stream(stream_capture::StreamCapture),
    Session(transaction_capture::SupplementalChanges),
    Nothing,
}

/// Refuse a statement that should change exactly one row and didn't.
pub(crate) fn changed_once(changed: usize) -> Result<(), DbError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(rusqlite::Error::StatementChangedRows(changed).into())
    }
}

/// `value` as an SQLite integer, which holds up to `i64::MAX`; a larger
/// `field` is `TooLarge`.
pub(crate) fn integer(field: &'static str, value: u64) -> Result<i64, DbError> {
    i64::try_from(value).map_err(|_past_i64| DbError::TooLarge {
        field,
        actual: value,
        maximum: i64::MAX as u64,
    })
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
pub(crate) mod tests;
