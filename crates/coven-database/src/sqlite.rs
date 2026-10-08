//! The leaf SQLite capability. Its connection is never returned or borrowed out.

#[path = "stream_capture.rs"]
mod stream_capture;
#[path = "transaction_capture.rs"]
mod transaction_capture;
use transaction_capture::ChangeCapture;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use coven_foundation::id_source::DeviceId;

use rusqlite::{
    fallible_iterator::FallibleIterator, functions::FunctionFlags, Batch, Connection, OpenFlags,
    Params, Row,
};

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

pub(crate) struct DatabaseConnection {
    connection: Connection,
    authorization: SqlAuthorization,
    observation: Option<WriterObservation>,
    streaming: std::cell::Cell<bool>,
    #[cfg(test)]
    scans: std::sync::Mutex<Vec<(String, i32)>>,
    #[cfg(test)]
    merge_loads: std::sync::Mutex<BTreeMap<String, usize>>,
}

struct WriterObservation {
    observer: CommitObserver,
    supplemental: Arc<BTreeMap<String, Option<String>>>,
}

impl DatabaseConnection {
    pub(crate) fn open(
        path: &Path,
        read_only: bool,
        authorization: SqlAuthorization,
    ) -> Result<Self, DbError> {
        let flags = if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        };
        // URI interpretation is deliberately absent: this is a StoreDir file,
        // never a caller-selected SQLite URI or in-memory database.
        let connection = Connection::open_with_flags(path, flags | OpenFlags::SQLITE_OPEN_NO_MUTEX)
            .map_err(opening_error)?;
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)?;
        connection.set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)?;
        connection.create_scalar_function(
            "coven_applying",
            0,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
            authorization.applying_function(),
        )?;
        connection.create_scalar_function(
            "_coven_loss_order",
            2,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_DIRECTONLY,
            |ctx| crate::loss_record::sort_key(ctx.get(0)?, &ctx.get::<Vec<u8>>(1)?),
        )?;
        connection.create_scalar_function(
            "_coven_fingerprint_replace",
            3,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_DIRECTONLY,
            |ctx| {
                // The first leaf at this identity adds to the set without a subtraction.
                let old = ctx.get::<Option<[u8; 32]>>(1)?.unwrap_or_default();
                Ok(crate::fingerprint::replace_sum(ctx.get(0)?, old, ctx.get(2)?).to_vec())
            },
        )?;
        connection.create_scalar_function(
            "_coven_migration_key",
            -1,
            FunctionFlags::SQLITE_UTF8
                | FunctionFlags::SQLITE_DETERMINISTIC
                | FunctionFlags::SQLITE_DIRECTONLY,
            |ctx| {
                if !ctx.len().is_multiple_of(2) {
                    return Err(rusqlite::Error::InvalidParameterCount(
                        ctx.len(),
                        ctx.len() + 1,
                    ));
                }
                let mut values = Vec::new();
                let mut collations = Vec::new();
                for i in (0..ctx.len()).step_by(2) {
                    collations.push(ctx.get::<String>(i)?);
                    values.push(crate::write_encoding::value(ctx.get_raw(i + 1))?);
                }
                crate::write_rows::equality_key(&values, &collations)
                    .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
            },
        )?;
        connection.authorizer(Some(authorization.callback()))?;
        let db = Self {
            connection,
            authorization,
            observation: None,
            streaming: std::cell::Cell::new(false),
            #[cfg(test)]
            scans: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            merge_loads: std::sync::Mutex::new(BTreeMap::new()),
        };
        db.batch("PRAGMA foreign_keys = ON; PRAGMA recursive_triggers = ON; PRAGMA trusted_schema = OFF;")?;
        db.refresh_hidden_rowids().map_err(opening_error)?;
        Ok(db)
    }

    pub(crate) fn check_integrity(&self) -> Result<(), DbError> {
        let check: Vec<String> = self
            .query("PRAGMA integrity_check", [], |row| row.get(0))
            .map_err(|error| match error {
                DbError::Sqlite(error) => opening_error(error),
                error => error,
            })?;
        if check != ["ok"] {
            return Err(DbError::DamagedDatabase);
        }
        Ok(())
    }

    pub(crate) fn enable_wal(&self) -> Result<(), DbError> {
        let mode: String = self.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::WalUnavailable { mode });
        }
        self.batch("PRAGMA synchronous = FULL;")
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

    pub(crate) fn prepare_schema(
        &self,
        tables: &[SyncedTable],
        migrations: &[Migration],
        author: Option<crate::migration_run::MigrationOrigin>,
        operation: Option<&crate::migration::MigrationOperation>,
    ) -> CovenResult<Vec<MigrationOutcome>> {
        let read_only = author.is_none();
        let supported = validate_versions(migrations)?;
        self.prepare_internal_schema()?;
        let current = self.schema_version()?;
        if current > supported {
            return Err(MigrationError::SchemaTooNew { current, supported }.into());
        }
        if read_only || current == supported {
            Schema::read(self)?.validate(tables)?;
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
        let publishes = matches!(
            author,
            Some(crate::migration_run::MigrationOrigin::Device(..))
        );
        let result = self.transaction(|db| {
            let outcomes = crate::migration_run::run(
                db,
                tables,
                migrations,
                current,
                supported,
                author.expect("writable migration"),
                &mut at,
            )?;
            if let Some(minimum) = outcomes
                .iter()
                .filter(|outcome| outcome.change == crate::MigrationChange::Breaking)
                .map(|outcome| outcome.version)
                .max()
            {
                db.internal_execute(
                    "UPDATE _coven_snapshot_schema SET minimum=?1,publication=?2 WHERE singleton=1",
                    (minimum, supported),
                )?;
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

    pub(crate) fn schema_version(&self) -> Result<u32, DbError> {
        self.query_row("PRAGMA user_version", [], |r| {
            r.get::<_, i32>(0).map(|v| v as u32)
        })
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

    pub(crate) fn local_write<F, R, E>(
        &self,
        schema: &crate::write_schema::WriteSchema,
        device: DeviceId,
        now: SystemTime,
        files: &crate::file_write::FileWrite<'_>,
        sql: F,
    ) -> Result<R, E>
    where
        E: crate::WriteFailure,
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, E>,
    {
        #[cfg(test)]
        let _profile = self.profile_statements();
        self.transaction_captured(false, |database| {
            let deleted_circles = crate::store_log_tables::deleted_circles(database)?;
            let mut session = rusqlite::session::Session::new(&database.connection)?;
            for table in &schema.declarations {
                session.attach(Some(table.name.as_str()))?;
            }
            let result = {
                let _scope = database.authorization.write();
                let result = sql(crate::SqlContext::new(files))?;
                database.authorization.take_file_violation()?;
                files.attach_app_files()?;
                result
            };
            database.require_transaction()?;
            let changeset = {
                let _scope = database.authorization.internal();
                session.changeset()?
            };
            let captured = crate::write_capture::capture(&changeset, schema)?;
            {
                let _scope = database.authorization.write();
                files.clear_null_ids(&captured)?;
            }
            let changeset = {
                let _scope = database.authorization.internal();
                session.changeset()?
            };
            drop(session);
            let captured = crate::write_capture::capture(&changeset, schema)?;
            files.validate(&captured)?;
            let before = crate::write_rows::AppView::before(database, schema, &captured)?;
            let after = crate::write_rows::AppView::after(database, schema);
            let store = crate::merge_store::MergeStore::new(database, &before);
            let changes = crate::write_record::changes(
                database,
                schema,
                &before,
                &after,
                &store,
                &before.changes(&after, captured.keys().cloned())?,
                &deleted_circles,
            )?;
            let record = if changes.is_empty() {
                None
            } else {
                Some(crate::write_record::record(database, device, now, changes)?)
            };
            let affected = crate::write_apply::WriteApply::new(
                database,
                schema,
                &store,
                &before,
                &after,
                &deleted_circles,
            )
            .apply(record.as_ref(), BTreeSet::new())
            .map_err(|error| match error {
                // This device authored the record; rejecting it means our own
                // authoring or removal machinery broke its invariants.
                DbError::InvalidWrite { write, error } => {
                    panic!("locally authored write {write:?} violates merge invariant: {error}")
                }
                error => error,
            })?;
            files.retain_rows(affected, &deleted_circles)?;
            if let Some(record) = &record {
                crate::write_commit::queue(database, record)?;
            }
            files.queue_attached(now)?;
            files.before_commit()?;
            Ok(result)
        })
    }

    pub(crate) fn batch(&self, sql: &str) -> Result<(), DbError> {
        let _scope = self.authorization.internal();
        self.connection.execute_batch(sql).map_err(Into::into)
    }

    pub(crate) fn columns_read(&self, sql: &str) -> Result<Vec<String>, DbError> {
        let _scope = self.authorization.internal();
        let reads = self.authorization.observe_reads();
        self.connection.prepare(sql)?;
        Ok(reads.columns())
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
        let changed = self.connection.execute(sql, params)?;
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
        let result = self.connection.query_row(sql, params, map);
        self.authorization
            .record_statement(&self.connection, None)?;
        result.map_err(Into::into)
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
        let result = (|| {
            let mut statement = self.connection.prepare(sql).map_err(DbError::from)?;
            let mut rows = statement.query(params).map_err(DbError::from)?;
            while let Some(row) = rows.next().map_err(DbError::from)? {
                visit(row)?;
            }
            Ok(())
        })();
        self.authorization
            .record_statement(&self.connection, None)
            .map_err(DbError::from)?;
        result
    }

    pub(crate) fn migration_targets(&self, sql: &str) -> rusqlite::Result<BTreeSet<String>> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization
            .mutations(|| self.authorization.app_result(self.connection.prepare(sql)))
    }

    pub(crate) fn prepare_file_triggers(&self) -> Result<(), DbError> {
        let triggers = self.query("SELECT name,sql FROM main.sqlite_schema WHERE type='trigger' UNION ALL SELECT name,sql FROM temp.sqlite_schema WHERE type='trigger'", [], |r| Ok((r.get(0)?,r.get(1)?)))?;
        self.authorization.set_file_triggers(triggers)?;
        Ok(())
    }

    pub(crate) fn file_execute<P: Params>(&self, sql: &str, params: P) -> Result<usize, DbError> {
        let _fill = self.authorization.filling_file();
        Ok(self.app_execute(sql, params)?)
    }

    pub(crate) fn app_execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        let mut statement = self
            .authorization
            .app_result(self.connection.prepare(sql))?;
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

    pub(crate) fn migration_changes<T>(
        &self,
        run: impl FnOnce() -> Result<T, DbError>,
    ) -> Result<(T, BTreeSet<String>), DbError> {
        assert!(
            self.observation.is_none(),
            "migrations precede commit observation"
        );
        let capture = stream_capture::StreamCapture::begin(self)?;
        let result = run()?;
        self.require_transaction()?;
        let changed = capture.tables();
        Ok((result, changed))
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
            let mut statement = self.connection.prepare(sql)?;
            let parameters = crate::sql_value::parameters(&self.connection, &statement, params)?;
            let result = (|| {
                let mut rows = statement.query(rusqlite::params_from_iter(&parameters))?;
                let row = {
                    let _scope = self.authorization.step();
                    self.authorization.app_result(rows.next())?
                };
                map(row.ok_or(rusqlite::Error::QueryReturnedNoRows)?)
            })();
            self.authorization
                .record_statement(&self.connection, Some((sql, &statement, &parameters)))?;
            result
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
            let mut statement = self.connection.prepare(sql)?;
            let parameters = crate::sql_value::parameters(&self.connection, &statement, params)?;
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
            self.authorization
                .record_statement(&self.connection, Some((sql, &statement, &parameters)))?;
            result
        })();
        self.authorization.app_result(result)
    }

    pub(crate) fn observe_commits(&mut self, observer: CommitObserver) -> Result<(), DbError> {
        assert!(
            self.observation.is_none(),
            "commit observation installed once"
        );
        self.observation = Some(WriterObservation {
            observer,
            supplemental: Arc::new(crate::change_capture::supplemental_tables(self)?),
        });
        Ok(())
    }

    fn refresh_hidden_rowids(&self) -> rusqlite::Result<()> {
        let _scope = self.authorization.internal();
        // A rowid table's declared primary key has its own index unless it is
        // an INTEGER PRIMARY KEY alias. Declared alias names make SQLITE_UPDATE
        // of ROWID ambiguous, so these tables are excluded from this policy.
        let mut statement = self.connection.prepare(
            "SELECT lower(schema),lower(name) FROM pragma_table_list AS t
             WHERE schema IN ('main','temp') AND type IN ('table','shadow') AND wr=0
             AND name NOT GLOB 'sqlite_*'
             AND NOT EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name,t.schema)
                             WHERE lower(name) IN ('rowid','_rowid_','oid'))
             AND (NOT EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name,t.schema) WHERE pk>0)
                  OR EXISTS (SELECT 1 FROM pragma_index_list(t.name,t.schema) WHERE origin='pk'))",
        )?;
        let tables = statement
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?;
        self.authorization.set_hidden_rowids(tables);
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
        self.batch("ROLLBACK")
            .expect("ending read snapshot with ROLLBACK failed");
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
        self.batch("BEGIN IMMEDIATE")?;
        self.streaming.set(streaming);
        let mut guard = SqlTransaction {
            database: self,
            active: true,
        };
        let result = (|| {
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
            self.batch("COMMIT")?;
            if let Some(observation) = &self.observation {
                observation.observer.commit(changes);
            }
            Ok(result)
        })();
        guard.active = false;
        match result {
            Ok(result) => Ok(result),
            Err(operation) if self.connection.is_autocommit() => Err(operation),
            Err(operation) => match self.batch("ROLLBACK") {
                Ok(()) => Err(operation),
                Err(DbError::Sqlite(rollback)) => Err(operation.with_rollback(rollback)),
                Err(error) => panic!("internal rollback returned a non-SQLite error: {error}"),
            },
        }
    }

    fn bound_stream_cache(&self) -> rusqlite::Result<()> {
        if !self.streaming.get() {
            return Ok(());
        }
        let (mut used, mut high) = (0, 0);
        // SAFETY: this owner exclusively uses its live connection. CACHE_USED
        // counts retained pages even when global memory accounting is disabled.
        let code = unsafe {
            rusqlite::ffi::sqlite3_db_status(
                self.connection.handle(),
                rusqlite::ffi::SQLITE_DBSTATUS_CACHE_USED,
                &mut used,
                &mut high,
                0,
            )
        };
        if code != rusqlite::ffi::SQLITE_OK {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                None,
            ));
        }
        if used > 4 * 1024 * 1024 {
            // cache_size is a suggestion. Spill uncommitted pages without
            // committing the transaction, then release reusable cache pages.
            self.connection.cache_flush()?;
            self.connection.release_memory()?;
        }
        Ok(())
    }

    pub(crate) fn close(self) -> Result<(), DbError> {
        self.connection
            .close()
            .map_err(|(_connection, error)| error.into())
    }
}

struct SqlTransaction<'a> {
    database: &'a DatabaseConnection,
    active: bool,
}

impl Drop for SqlTransaction<'_> {
    fn drop(&mut self) {
        self.database.streaming.set(false);
        if self.active && !self.database.connection.is_autocommit() {
            self.database
                .batch("ROLLBACK")
                .expect("rollback panicking database transaction");
        }
    }
}

fn opening_error(error: rusqlite::Error) -> DbError {
    match &error {
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
            ) =>
        {
            DbError::DamagedDatabase
        }
        _ => error.into(),
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
