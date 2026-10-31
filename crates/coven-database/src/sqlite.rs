//! The leaf SQLite capability. Its connection is never returned or borrowed out.

#[path = "app_statements.rs"]
mod app_statements;
#[path = "app_triggers.rs"]
mod app_triggers;
#[path = "bulk_write.rs"]
mod bulk_write;
#[path = "event_capture.rs"]
mod event_capture;
#[path = "local_write.rs"]
mod local_write;
#[path = "pre_update_changes.rs"]
mod pre_update_changes;
#[path = "reference_assignments.rs"]
pub(crate) mod reference_assignments;
#[path = "rowid_policy.rs"]
mod rowid_policy;
#[path = "stream_capture.rs"]
mod stream_capture;
#[path = "transaction_capture.rs"]
mod transaction_capture;
#[path = "write_constants.rs"]
mod write_constants;
pub(crate) use event_capture::Event;
use transaction_capture::ChangeCapture;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use coven_foundation::id_source::DeviceId;

use rusqlite::{fallible_iterator::FallibleIterator, Batch, Connection, OpenFlags, Params, Row};

use crate::authorization::SqlAuthorization;
use crate::internal_schema;
use crate::migration::validate_versions;
use crate::observation::{CommitObserver, ReadSet};
use crate::schema::Schema;
use crate::SqlReadContext;
use crate::{
    CovenMigrationError, CovenResult, DbError, Migration, MigrationError, MigrationOutcome,
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
    app_statements: app_statements::AppStatements,
    constants: write_constants::WriteConstants,
    /// The open handle this connection serves: it receives the commits and
    /// failures after which the handle refuses calls. None while opening, when
    /// a failure fails the open and drops the connection instead.
    handle: Option<CommitObserver>,
    observation: Option<WriterObservation>,
    /// The writer's checkpointer, woken after each commit (§4).
    checkpoints: Option<crate::wal_checkpoints::CommitSignal>,
    streaming: std::cell::Cell<bool>,
    #[cfg(any(test, feature = "test-utils"))]
    write_checkpoint: std::cell::RefCell<Option<crate::test_utils::WriteCheckpointObserver>>,
    #[cfg(test)]
    scans: std::sync::Mutex<Vec<tests::ProfiledStatement>>,
    #[cfg(test)]
    merge_loads: std::sync::Mutex<BTreeMap<String, usize>>,
    /// Each queued write's record and judgement, also of a refused one, for
    /// tests of its classification.
    #[cfg(test)]
    judged: std::cell::RefCell<Vec<(coven_format::write::WriteRecord, crate::judge::Judgement)>>,
}

struct WriterObservation {
    device: DeviceId,
    supplemental: Arc<BTreeMap<String, Option<String>>>,
    tables: Arc<pre_update_changes::TableShapes>,
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
        let connection =
            Connection::open_with_flags(path, flags | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        connection.set_prepared_statement_cache_capacity(STATEMENT_CACHE);
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)?;
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)?;
        crate::coven_functions::register(&connection, &authorization)?;
        functions.register(&connection)?;
        connection.authorizer(Some(authorization.callback()))?;
        let db = Self {
            #[cfg(any(test, feature = "test-utils"))]
            work: std::cell::RefCell::new(None),
            connection,
            authorization,
            app_statements: app_statements::AppStatements::default(),
            constants: write_constants::WriteConstants::default(),
            handle: None,
            observation: None,
            checkpoints: None,
            streaming: std::cell::Cell::new(false),
            #[cfg(any(test, feature = "test-utils"))]
            write_checkpoint: std::cell::RefCell::new(None),
            #[cfg(test)]
            scans: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            merge_loads: std::sync::Mutex::new(BTreeMap::new()),
            #[cfg(test)]
            judged: std::cell::RefCell::new(Vec::new()),
        };
        db.configure(settings, read_only)?;
        db.refresh_hidden_rowids()?;
        Ok(db)
    }

    /// Migrate local tables before app migrations or recovery's waiting work.
    /// Read-only connections require the supported internal version already.
    pub(crate) fn prepare_internal_schema(&self) -> CovenResult<()> {
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
                db.batch(&format!(
                    "PRAGMA application_id = {}",
                    internal_schema::VERSION
                ))
            })
            .map_err(|source| CovenMigrationError::Failed {
                source: Box::new(source),
            })?;
        }
        Ok(())
    }

    /// Prepare the app's schema: on a writable connection, rebuild what a
    /// new version of the app's collations and functions changes, then
    /// migrate; on a read-only one, require both already done.
    pub(crate) fn prepare_schema(
        &self,
        tables: &[SyncedTable],
        migrations: &[Migration],
        versions: &[crate::sql_versions::SqlVersion],
        author: Option<crate::migration_run::MigrationOrigin>,
        operation: Option<&crate::migration::MigrationOperation>,
    ) -> CovenResult<Vec<MigrationOutcome>> {
        let read_only = author.is_none();
        let supported = validate_versions(migrations)?;
        self.prepare_internal_schema()?;
        // Before the app's migrations, whose statements use the indexes.
        if read_only {
            crate::sql_versions::require_built(self, versions)?;
        } else {
            crate::sql_versions::rebuild(self, tables, versions)?;
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
            self.check_followed_triggers()?;
            if let Some(table) =
                crate::row_queries::owner_declaration_changed(self, &schema, tables)?
            {
                return Err(DbError::Schema(crate::SchemaError::OwnerOnlyChanged { table }).into());
            }
            if let Some(table) = crate::file_table::changed(self, &schema, tables)? {
                return Err(
                    DbError::Schema(crate::SchemaError::FileColumnsChanged { table }).into(),
                );
            }
            if !read_only {
                self.transaction(|db| crate::row_queries::register_schema(db, &schema, tables))?;
            }
            if self.authorization.tables_changed() {
                self.refresh_hidden_rowids()?;
            }
            return Ok(Vec::new());
        }
        let mut at = migrations
            .iter()
            .find(|m| m.version > current)
            .expect("pending migration");
        #[cfg(test)]
        let _profile = self.profile_statements();
        let publishes = author.is_some();
        let device = author.as_ref().map(|origin| origin.0);
        let result = self.transaction(|db| {
            let crate::migration_run::MigrationRun { outcomes, marker } =
                crate::migration_run::run(
                    db,
                    tables,
                    migrations,
                    current,
                    supported,
                    author.expect("writable migration"),
                    &mut at,
                )?;
            let breaking = outcomes
                .iter()
                .filter(|outcome| outcome.change == crate::MigrationChange::Breaking)
                .map(|outcome| outcome.version)
                .max();
            let raises = if let Some(minimum) = breaking {
                // The raise snapshots are captured through the marker before any
                // later app write; with no store there is no shared version to raise.
                let schema = crate::write_schema::WriteSchema::read(db, tables.to_vec())?;
                let marker = marker.expect("a breaking batch appends its marker");
                if crate::raise_capture::capture(db, &schema, marker, supported)? {
                    db.internal_execute(
                        "UPDATE _coven_snapshot_schema SET minimum=?1,publication=?2 WHERE singleton=1",
                        (minimum, supported),
                    )?;
                } else {
                    db.internal_execute(
                        "UPDATE _coven_snapshot_schema SET minimum=?1 WHERE singleton=1",
                        [minimum],
                    )?;
                }
                true
            } else {
                // Only additions: a raise still waiting to publish moves to
                // this version, captured again at it (§18.5).
                let device = device.expect("writable migration");
                let moved = crate::raise_capture::raise_to(db, tables, device, supported)?;
                if moved {
                    db.internal_execute(
                        "UPDATE _coven_snapshot_schema SET publication=?1 WHERE singleton=1",
                        [supported],
                    )?;
                }
                moved
            };
            if raises {
                if let Some(operation) = operation.filter(|_| publishes) {
                    let new = operation(supported)?;
                    let id = crate::operation::insert(db, &new)?;
                    crate::operation::advance(
                        db,
                        &crate::OperationUpdate {
                            id,
                            previous: 0,
                            last_step: 1,
                            data: new.data,
                        },
                    )?;
                }
            }
            crate::row_queries::register_schema(
                db,
                &Schema::read(db, &crate::file_table::declared(tables))?,
                tables,
            )?;
            db.refresh_hidden_rowids()?;
            Ok(outcomes)
        });
        result.map_err(|source| {
            MigrationError::Failed {
                version: at.version,
                name: at.name,
                source: Box::new(source),
            }
            .into()
        })
    }

    /// The most arguments an SQL function call can pass on this connection.
    #[cfg(test)]
    pub(crate) fn function_argument_limit(&self) -> Result<usize, DbError> {
        Ok(self
            .connection
            .limit(rusqlite::limits::Limit::SQLITE_LIMIT_FUNCTION_ARG)? as usize)
    }

    /// Refuse a queued value before allocating it, using this connection's limit.
    pub(crate) fn check_value_length(
        &self,
        field: &'static str,
        length: u64,
    ) -> Result<usize, DbError> {
        let maximum = self
            .connection
            .limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH)? as u64;
        if length > maximum {
            return Err(DbError::TooLarge {
                field,
                actual: length,
                maximum,
            });
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

    pub(crate) fn pending_transaction<T>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction(run)
            .map_err(|error| self.pending_failure(error))
    }

    /// Tell this connection to the store's database which installation it
    /// serves, for coven's views of members and devices (Appendix C.1).
    pub(crate) fn identify_device(&self, device: DeviceId) -> Result<(), DbError> {
        Ok(crate::coven_functions::register_device(
            &self.connection,
            device,
        )?)
    }

    /// Give this connection to the store's database the files this process
    /// is transferring, for `coven_uploads.sending`, `coven_sending` and
    /// `coven_downloading` (§4).
    pub(crate) fn serve_transfers(
        &self,
        transfers: &crate::transfers_in_flight::TransfersInFlight,
    ) -> Result<(), DbError> {
        Ok(transfers.register(&self.connection)?)
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

    fn pending_failure(&self, error: DbError) -> DbError {
        if matches!(error, DbError::PendingPersistence(_)) {
            return error;
        }
        let error = Arc::new(error);
        if let Some(handle) = &self.handle {
            handle.fail(crate::observation::HandleFailure::Pending(error.clone()));
        }
        DbError::PendingPersistence(error)
    }

    pub(crate) fn batch(&self, sql: &str) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        self.connection.execute_batch(sql).map_err(Into::into)
    }

    /// The collation a column declares, BINARY when it names none.
    pub(crate) fn column_collation(&self, table: &str, column: &str) -> Result<String, DbError> {
        let _scope = self.authorization.internal();
        let (_, collation, _, _, _) =
            self.connection
                .column_metadata(Some("main"), table, column)?;
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
        let changed = self.with_statement(sql, |statement| {
            statement.execute(params).map_err(DbError::from)
        })?;
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
        self.authorization
            .record_statement(&crate::row_scope::Scopes::default());
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
        self.authorization
            .record_statement(&crate::row_scope::Scopes::default());
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
        self.authorization
            .mutations(|| self.authorization.app_result(self.connection.prepare(sql)))
    }

    pub(crate) fn app_execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        let mut statement = self.authorization.app_result(self.app_statement(sql))?;
        let _scope = self.authorization.step();
        let changed = self.authorization.app_result(statement.execute(params))?;
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
            let result = (|| {
                let mut rows = statement.raw_query();
                let _scope = self.authorization.step();
                while rows.next()?.is_some() {}
                Ok(())
            })();
            self.authorization.app_result(result)?;
            if let (Some(cookie), Some(migration), Some(text)) = (cookie, migration, text) {
                if self.schema_cookie()? != cookie {
                    migration.record(&text)?;
                }
            }
        }
    }

    pub(crate) fn schema_cookie(&self) -> rusqlite::Result<i64> {
        let _scope = self.authorization.internal();
        self.connection
            .query_row("PRAGMA main.schema_version", [], |r| r.get(0))
    }

    /// Run one migration step inside the open migration transaction. Which
    /// tables its statements write is decided as they are prepared (§18.1).
    pub(crate) fn migrating<T>(
        &self,
        run: impl FnOnce() -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        assert!(
            self.observation.is_none(),
            "migrations precede commit observation"
        );
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
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization.begin_read_statement();
        let result = (|| {
            let mut statement = self.app_statement(sql)?;
            let parameters = crate::sql_value::parameters(&self.connection, &statement, params)?;
            let plan = self.live_scope_plan(sql, &statement)?;
            let result = (|| {
                let mut rows = statement.query(rusqlite::params_from_iter(&parameters))?;
                let row = {
                    let _scope = self.authorization.step();
                    self.authorization.app_result(rows.next())?
                };
                map(row.ok_or(rusqlite::Error::QueryReturnedNoRows)?)
            })();
            let recorded = self.record_app_statement(plan.as_deref(), &parameters);
            result.and_then(|value| recorded.map(|()| value))
        })();
        self.authorization.app_result(result)
    }

    pub(crate) fn app_query<T, P: Params>(
        &self,
        sql: &str,
        params: P,
        mut map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Vec<T>> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization.begin_read_statement();
        let result = (|| {
            let mut statement = self.app_statement(sql)?;
            let parameters = crate::sql_value::parameters(&self.connection, &statement, params)?;
            let plan = self.live_scope_plan(sql, &statement)?;
            let result = (|| {
                let mut rows = statement.query(rusqlite::params_from_iter(&parameters))?;
                let mut values = Vec::new();
                loop {
                    let row = {
                        let _scope = self.authorization.step();
                        self.authorization.app_result(rows.next())?
                    };
                    let Some(row) = row else { break };
                    values.push(map(row)?);
                }
                Ok(values)
            })();
            let recorded = self.record_app_statement(plan.as_deref(), &parameters);
            result.and_then(|value| recorded.map(|()| value))
        })();
        self.authorization.app_result(result)
    }

    /// Wake `checkpoints` after each commit this writer makes (§4).
    pub(crate) fn signal_commits(&mut self, checkpoints: crate::wal_checkpoints::CommitSignal) {
        self.checkpoints = Some(checkpoints);
    }

    /// Serve an open handle once opening succeeds (§4).
    pub(crate) fn serve(&mut self, handle: CommitObserver) {
        assert!(self.handle.is_none(), "a connection serves one handle");
        self.handle = Some(handle);
    }

    /// Serve the handle as its writer, capturing each commit's row changes.
    pub(crate) fn observe_commits(
        &mut self,
        handle: CommitObserver,
        device: DeviceId,
        synced: &[SyncedTable],
    ) -> Result<(), DbError> {
        assert!(
            self.observation.is_none(),
            "commit observation installed once"
        );
        let supplemental = crate::change_capture::supplemental_tables(self)?;
        let tables = Arc::new(pre_update_changes::TableShapes::read(
            self,
            &crate::file_table::synced(synced),
            &supplemental,
        )?);
        self.serve(handle);
        self.observation = Some(WriterObservation {
            device,
            supplemental: Arc::new(supplemental),
            tables,
        });
        Ok(())
    }

    pub(crate) fn read_snapshot<F, R, E>(&self, read: F) -> (Result<R, E>, ReadSet)
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E>,
        E: From<DbError>,
    {
        let mut reads = ReadSet::new();
        let result = self.read_transaction(|| {
            let _reading = self.authorization.reading();
            let result = read(SqlReadContext::new(self));
            reads = self.authorization.reads();
            result
        });
        (result, reads)
    }

    pub(crate) fn read_transaction<R, E: From<DbError>>(
        &self,
        read: impl FnOnce() -> Result<R, E>,
    ) -> Result<R, E> {
        self.batch("BEGIN DEFERRED")?;
        let mut guard = SqlTransaction {
            database: self,
            active: true,
        };
        // BEGIN alone does not fix a WAL snapshot. Pin it before invoking a
        // consumer that can wait while a writer commits.
        let result = (|| {
            self.query_row("SELECT count(*) FROM main.sqlite_schema", [], |r| {
                r.get::<_, i64>(0)
            })?;
            read()
        })();
        guard.active = false;
        match self.roll_back() {
            Ok(()) => result,
            // A failed read keeps its own cause; the handle now refuses calls.
            Err(rollback) => Err(match result {
                Ok(_) => DbError::from(rollback).into(),
                Err(error) => {
                    tracing::error!(%rollback, "ending a failed read's snapshot failed");
                    error
                }
            }),
        }
    }

    /// Run checks that write scratch state, such as temporary tables, in a
    /// transaction that always rolls back, so nothing they write is kept or
    /// observed. It takes no write lock until something writes the main schema.
    pub(crate) fn discarded_transaction<T>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        use crate::WriteFailure;
        self.batch("BEGIN DEFERRED")?;
        let mut guard = SqlTransaction {
            database: self,
            active: true,
        };
        let result = run(self);
        guard.active = false;
        match self.roll_back() {
            Ok(()) => result,
            Err(rollback) => Err(match result {
                Ok(_) => DbError::from(rollback),
                Err(error) => error.with_rollback(rollback),
            }),
        }
    }

    /// Roll back the open transaction unless SQLite already ended it. When
    /// ROLLBACK fails, this connection may still be inside the transaction: a
    /// writer would treat its uncommitted changes as committed and a reader
    /// would keep reading its old snapshot, so the handle refuses every later
    /// call until the store is reopened (§4).
    fn roll_back(&self) -> rusqlite::Result<()> {
        if self.connection.is_autocommit() {
            return Ok(());
        }
        let result = {
            let _scope = self.authorization.internal();
            self.connection.execute_batch("ROLLBACK")
        };
        if result.is_err() {
            if let Some(handle) = &self.handle {
                handle.fail(crate::observation::HandleFailure::Rollback);
            }
        }
        result
    }

    pub(crate) fn lost_values(&self) -> CovenResult<Vec<crate::LostValue>> {
        let records = self.query(
            &format!("{} ORDER BY l.id", crate::loss_record::SELECT),
            [],
            crate::loss_record::read,
        )?;
        records
            .into_iter()
            .map(|record| crate::LostValue::from_record(record).map_err(Into::into))
            .collect()
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
        if self.authorization.tables_changed() {
            self.refresh_hidden_rowids()?;
        }
        self.authorization.begin_app_call();
        Ok(())
    }

    pub(crate) fn transaction<T>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction_captured(false, run)
    }

    /// Streaming transfers publish table invalidations without a session's
    /// retained before/after BLOB values growing with the transferred object.
    pub(crate) fn stream_transaction<T>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        self.transaction_captured(true, run)
    }

    fn transaction_captured<T, E: crate::WriteFailure>(
        &self,
        streaming: bool,
        run: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        self.write_transaction(streaming, || {
            // The write-record session ends before materialization; this session
            // covers app SQL, merge metadata, materialization and local tables.
            let mut capture = match &self.observation {
                Some(_) if streaming => None,
                Some(observation) => Some(ChangeCapture::begin(self, observation)?),
                None => None,
            };
            let tables = if streaming {
                Some(stream_capture::StreamCapture::begin(self)?)
            } else {
                None
            };
            let result = run(self)?;
            self.require_transaction()?;
            let changes = match &mut capture {
                Some(capture) => capture.take_changes()?,
                None => match &tables {
                    Some(tables) => tables.changes()?,
                    None => Vec::new(),
                },
            };
            // Keep both captures through commit, then drop the stream hook first.
            Ok(((tables, capture), result, changes))
        })
    }

    fn write_transaction<T, E: crate::WriteFailure, C>(
        &self,
        streaming: bool,
        run: impl FnOnce() -> Result<(C, T, Vec<crate::observation::RowChange>), E>,
    ) -> Result<T, E> {
        self.batch("BEGIN IMMEDIATE")?;
        self.streaming.set(streaming);
        let mut guard = SqlTransaction {
            database: self,
            active: true,
        };
        let result = (|| {
            // Capture drops after commit/notification, or after the result on failure.
            let (_capture, result, changes) = run()?;
            self.batch("COMMIT")?;
            if let Some(checkpoints) = &self.checkpoints {
                checkpoints.committed();
            }
            #[cfg(any(test, feature = "test-utils"))]
            self.record_work(|work| work.commits += 1);
            #[cfg(any(test, feature = "test-utils"))]
            self.checkpoint(crate::test_utils::WriteCheckpoint::Committed);
            if let Some(handle) = &self.handle {
                handle.commit(changes, crate::commit_scope::committing());
            }
            #[cfg(any(test, feature = "test-utils"))]
            self.checkpoint(crate::test_utils::WriteCheckpoint::Observed);
            Ok(result)
        })();
        guard.active = false;
        match result {
            Ok(result) => Ok(result),
            Err(operation) => match self.roll_back() {
                Ok(()) => Err(operation),
                Err(rollback) => Err(operation.with_rollback(rollback)),
            },
        }
    }

    pub(crate) fn close(self) -> Result<(), DbError> {
        self.connection
            .close()
            .map_err(|(_connection, error)| error.into())
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

struct SqlTransaction<'a> {
    database: &'a DatabaseConnection,
    active: bool,
}

// Still active only while a callback's panic unwinds, when a second panic
// would abort the process. The panic propagates; a failed rollback is logged
// and fails the handle, so every later call reports it.
impl Drop for SqlTransaction<'_> {
    fn drop(&mut self) {
        self.database.streaming.set(false);
        if self.active {
            if let Err(rollback) = self.database.roll_back() {
                tracing::error!(%rollback, "rolling back a panicking transaction failed");
            }
        }
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
pub(crate) mod tests;
