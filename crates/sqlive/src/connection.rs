//! A connection to a database, the host's SQL capability: SQLite's
//! connection is never returned or borrowed out.

#[path = "app_statements.rs"]
mod app_statements;
#[path = "bulk_write.rs"]
mod bulk_write;
#[path = "database_file_sync.rs"]
mod database_file_sync;
#[path = "event_capture.rs"]
mod event_capture;
#[path = "live_judging.rs"]
mod live_judging;
#[path = "page_cache.rs"]
mod page_cache;
#[path = "pre_update_changes.rs"]
mod pre_update_changes;
#[path = "rolled_back.rs"]
mod rolled_back;
#[path = "schema_cookie.rs"]
mod schema_cookie;
#[cfg(any(test, feature = "profiles"))]
#[path = "statement_profiles.rs"]
mod statement_profiles;
#[path = "stream_capture.rs"]
mod stream_capture;
#[path = "table_shapes.rs"]
pub(crate) mod table_shapes;
#[path = "transaction_capture.rs"]
mod transaction_capture;
#[path = "write_transaction.rs"]
mod write_transaction;
#[path = "write_with.rs"]
mod write_with;
#[cfg(any(test, feature = "profiles"))]
pub use statement_profiles::ProfiledStatement;
#[cfg(any(test, feature = "test-utils"))]
#[path = "test_hooks.rs"]
mod test_hooks;
use app_statements::StepsOf;
pub use event_capture::Operation;
pub use schema_cookie::CookieScope;
use transaction_capture::ChangeCapture;
use write_transaction::WriteTransaction;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use rusqlite::{fallible_iterator::FallibleIterator, Batch, Connection, OpenFlags, Params, Row};

use crate::authorization::PolicyError;
use crate::authorization::SqlAuthorization;
use crate::authorization::{Policy, Refresh};
use crate::connection_setup::Setup;

/// Statements kept prepared per connection, coven's and the app's: a local
/// write runs about a hundred distinct ones of coven's, which a smaller cache
/// would evict before reuse, beside the app's.
const STATEMENT_CACHE: usize = 512;

/// Whether a statement run through `app_query` belongs to a read, whose
/// statements a test can record, rather than a write or a migration.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// A read's statement, which a test's statement log records.
    Yes,
    /// A write's or a migration's statement.
    No,
}

/// A connection to a database, as its host runs SQL on it: the host's own
/// statements and the app's, each judged by the authorizer and the host's
/// policy `P`.
pub struct HostSql<P: Policy> {
    #[cfg(any(test, feature = "test-utils"))]
    work: std::cell::RefCell<Option<crate::test_records::ConnectionWorkCounter>>,
    /// Where this connection's reads push the statements they run.
    #[cfg(any(test, feature = "test-utils"))]
    statement_log: std::cell::RefCell<Option<crate::test_records::StatementLog>>,
    connection: Connection,
    authorization: SqlAuthorization<P>,
    /// What the host adds to the authorizer's rules, shared with the
    /// authorizer.
    policy: Arc<P>,
    /// Stops a lookup that runs too long (`live_judging.rs`).
    lookups: live_judging::LookupLimit,
    app_statements: app_statements::AppStatements,
    /// The schema cookie while a transaction that runs no DDL holds it.
    cookie: schema_cookie::SchemaCookie,
    /// The open handle this connection serves: it receives the commits and
    /// failures after which the handle refuses calls. None while opening, when
    /// a failure fails the open and drops the connection instead.
    handle: Option<crate::Handle<P::Failure>>,
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
    commit_points: std::cell::RefCell<Option<crate::test_records::CommitPointObserver>>,
    #[cfg(any(test, feature = "profiles"))]
    scans: Arc<std::sync::Mutex<Vec<ProfiledStatement>>>,
    /// How many profiles are open: a profile inside another, as a
    /// migration's inside the open's, leaves the outer one tracing.
    #[cfg(any(test, feature = "profiles"))]
    profiles: std::cell::Cell<usize>,
    /// Panic in the next call of a write transaction's hook, as a bug in
    /// what it captures would.
    #[cfg(any(test, feature = "test-utils"))]
    hook_panics: Arc<std::sync::atomic::AtomicBool>,
}

struct WriterObservation {
    /// The tables whose operations a write keeps whole, lowercase.
    recorded: Arc<BTreeSet<String>>,
}

impl<P: Policy> HostSql<P> {
    #[cfg(any(test, feature = "test-utils"))]
    /// Count this connection's work into `counter` from now on.
    pub fn test_track_work(&self, counter: crate::test_records::ConnectionWorkCounter) {
        *self.work.borrow_mut() = Some(counter);
    }

    #[cfg(any(test, feature = "test-utils"))]
    /// Record the statements this connection's reads run into `log` from
    /// now on.
    pub fn test_record_statements(&self, log: crate::test_records::StatementLog) {
        *self.statement_log.borrow_mut() = Some(log);
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn record_work(
        &self,
        update: impl FnOnce(&mut crate::test_records::ConnectionWork),
    ) {
        if let Some(counter) = self.work.borrow().as_ref() {
            counter.record(update);
        }
    }

    /// A connection to the database at `path`, for reading only when
    /// `read_only`, whose authorizer asks `policy` after its own rules and
    /// treats `whole_views` as a live query's whole reads. `setup` registers
    /// what the connection needs, given its policy, before any SQL runs.
    pub fn open(
        path: &Path,
        read_only: bool,
        policy: P,
        whole_views: Arc<crate::WholeViews>,
        settings: &crate::SqliteSettings,
        setup: &dyn Fn(&Setup<'_, P>, &P) -> rusqlite::Result<()>,
    ) -> Result<Self, PolicyError<P>> {
        let policy = Arc::new(policy);
        let authorization = SqlAuthorization::new(whole_views, Arc::clone(&policy));
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
        setup(&Setup::new(&connection, &authorization), &policy)?;
        connection.authorizer(Some(authorization.callback()))?;
        let lookups = live_judging::LookupLimit::install(&mut connection)?;
        let db = Self {
            #[cfg(any(test, feature = "test-utils"))]
            work: std::cell::RefCell::new(None),
            #[cfg(any(test, feature = "test-utils"))]
            statement_log: std::cell::RefCell::new(None),
            connection,
            authorization,
            policy,
            lookups,
            app_statements: app_statements::AppStatements::default(),
            cookie: schema_cookie::SchemaCookie::default(),
            handle: None,
            observation: None,
            shapes: std::cell::RefCell::new(None),
            checkpoints: None,
            statistics: None,
            streaming: std::cell::Cell::new(false),
            #[cfg(any(test, feature = "test-utils"))]
            commit_points: std::cell::RefCell::new(None),
            #[cfg(any(test, feature = "profiles"))]
            scans: Arc::new(std::sync::Mutex::new(Vec::new())),
            #[cfg(any(test, feature = "profiles"))]
            profiles: std::cell::Cell::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            hook_panics: Arc::default(),
        };
        db.configure(settings, read_only)?;
        db.refresh_policy(Refresh::Whole)?;
        Ok(db)
    }

    /// The most arguments an SQL function call can pass on this connection.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn function_argument_limit(&self) -> Result<usize, PolicyError<P>> {
        Ok(self.connection.limit(rusqlite::limits::Limit::SQLITE_LIMIT_FUNCTION_ARG)? as usize)
    }

    /// The most bytes a string or BLOB can hold on this connection.
    pub fn length_limit(&self) -> rusqlite::Result<i32> {
        self.connection.limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH)
    }

    /// Open the BLOB in `column` of the row `rowid` of the main schema's
    /// `table`, for reading only when `read_only`.
    pub fn open_blob(
        &self,
        table: &str,
        column: &str,
        rowid: i64,
        read_only: bool,
    ) -> rusqlite::Result<rusqlite::blob::Blob<'_>> {
        let _scope = self.authorization.internal();
        self.connection.blob_open(rusqlite::MAIN_DB, table, column, rowid, read_only)
    }

    /// Fail the handle this connection serves with `failure`, so every later
    /// call and live query reports it. A connection that serves no handle
    /// yet fails nothing.
    pub fn fail_handle(&self, failure: P::Failure) {
        if let Some(handle) = &self.handle {
            handle.fail(failure);
        }
    }

    /// Run `run` with this connection's policy in `scope`, leaving the scope
    /// on every path out of `run`, a panic's included.
    pub fn within<T>(&self, scope: P::Scope, run: impl FnOnce() -> T) -> T {
        struct EnteredScope<'a, P: Policy> {
            policy: &'a P,
            scope: P::Scope,
        }
        impl<P: Policy> Drop for EnteredScope<'_, P> {
            fn drop(&mut self) {
                self.policy.leave(&self.scope);
            }
        }
        self.policy.enter(&scope);
        let _entered = EnteredScope { policy: &*self.policy, scope };
        run()
    }

    /// Have this connection's policy read the schema again (`Policy::refresh`).
    pub fn refresh_policy(&self, extent: Refresh) -> rusqlite::Result<()> {
        self.policy.refresh(self, extent)
    }

    /// Whether SQLite opened the main database for reading only.
    pub fn read_only(&self) -> rusqlite::Result<bool> {
        self.connection.is_readonly(rusqlite::MAIN_DB)
    }

    /// Whether this connection is a writer that observes its commits.
    pub fn observes_commits(&self) -> bool {
        self.observation.is_some()
    }

    /// Run `run` on this connection. Tests trace its statements, ending the
    /// profile on every path out of `run`, a panic's included, so a profile
    /// is never left open; one inside another nests (sqlite_tests.rs).
    pub fn with_profile<T>(&mut self, run: impl FnOnce(&mut Self) -> T) -> T {
        #[cfg(not(any(test, feature = "profiles")))]
        return run(self);
        #[cfg(any(test, feature = "profiles"))]
        {
            struct Profiled<'a, P: Policy>(&'a mut HostSql<P>);
            impl<P: Policy> Drop for Profiled<'_, P> {
                fn drop(&mut self) {
                    self.0.end_profile();
                }
            }
            self.start_profile();
            let profiled = Profiled(self);
            run(&mut *profiled.0)
        }
    }

    /// Fail the handle when removing a transaction's `hook` failed: what the
    /// connection would call next is unknown. Removal never panics, since it
    /// runs as a transaction drops, perhaps during an unwind.
    pub(crate) fn removed_hook(&self, hook: &'static str, removed: rusqlite::Result<()>) {
        if let Err(error) = removed {
            tracing::error!(hook, %error, "removing a transaction's hook failed");
            self.fail_handle(P::Failure::from(crate::EngineFailure::Hook(error.into())));
        }
    }

    /// Run the host's own statements in `sql`, one after another.
    pub fn batch(&self, sql: &str) -> Result<(), PolicyError<P>> {
        let _scope = self.authorization.internal();
        self.connection.execute_batch(sql).map_err(Into::into)
    }

    /// The collation a column declares, BINARY when it names none.
    pub fn column_collation(&self, table: &str, column: &str) -> Result<String, PolicyError<P>> {
        let _scope = self.authorization.internal();
        let (_, collation, _, _, _) =
            self.connection.column_metadata(Some("main"), table, column)?;
        Ok(collation.map_or_else(|| "BINARY".into(), |c| c.to_string_lossy().into_owned()))
    }

    /// The columns `sql` reads and the functions it calls, as SQLite
    /// resolves them while preparing it; it never runs.
    pub fn reads(&self, sql: &str) -> Result<crate::authorization::StatementReads, PolicyError<P>> {
        let _scope = self.authorization.internal();
        let reads = self.authorization.observe_reads();
        self.connection.prepare(sql)?;
        Ok(reads.reads())
    }

    /// Run the host's own statement `sql` with `params`, returning the rows
    /// it changed.
    pub fn execute<Q: Params>(&self, sql: &str, params: Q) -> Result<usize, PolicyError<P>> {
        let _scope = self.authorization.internal();
        let changed = self.with_statement(sql, |statement| {
            statement.execute(params).map_err(crate::Error::from)
        })?;
        self.bound_stream_cache()?;
        Ok(changed)
    }

    /// The first row of the host's own query `sql` with `params`, through
    /// `map`.
    pub fn query_row<T, Q: Params>(
        &self,
        sql: &str,
        params: Q,
        map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, PolicyError<P>> {
        let _scope = self.authorization.internal();
        self.authorization.begin_read_statement();
        let result = self.with_statement(sql, |statement| {
            statement.query_row(params, map).map_err(crate::Error::from)
        });
        self.authorization.record_statement(&crate::Scopes::default());
        result
    }

    /// Every row of the host's own query `sql` with `params`, through `map`.
    pub fn query<T, Q: Params>(
        &self,
        sql: &str,
        params: Q,
        mut map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> Result<Vec<T>, PolicyError<P>> {
        let mut values = Vec::new();
        self.for_each::<_, PolicyError<P>>(sql, params, |row| {
            values.push(map(row)?);
            Ok(())
        })?;
        Ok(values)
    }

    /// Stream rows to a consumer with its own error type.
    pub fn for_each<Q: Params, E: From<PolicyError<P>>>(
        &self,
        sql: &str,
        params: Q,
        mut visit: impl FnMut(&Row<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let _scope = self.authorization.internal();
        self.authorization.begin_read_statement();
        let result = self.with_statement(sql, |statement| {
            let mut rows = statement.query(params).map_err(crate::Error::from)?;
            while let Some(row) = rows.next().map_err(crate::Error::from)? {
                visit(row)?;
            }
            Ok(())
        });
        self.authorization.record_statement(&crate::Scopes::default());
        result
    }

    /// `prepared`, or the error the authorizer refused it with.
    fn judged<T>(&self, prepared: rusqlite::Result<T>) -> Result<T, PolicyError<P>> {
        Ok(self.authorization.app_result(prepared)?)
    }

    /// Run coven's own SQL on the connection's cached statement, parsed once per
    /// connection. The authorizer records reads only while a statement is
    /// prepared, so while it records, the statement is prepared afresh.
    fn with_statement<T, E: From<PolicyError<P>>>(
        &self,
        sql: &str,
        run: impl FnOnce(&mut rusqlite::Statement<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        if self.authorization.recording() {
            run(&mut self.judged(self.connection.prepare(sql))?)
        } else {
            let mut statement = self.judged(self.connection.prepare_cached(sql))?;
            // The statement profile reports each execution's own full scans.
            #[cfg(any(test, feature = "profiles"))]
            statement.reset_status(rusqlite::StatementStatus::FullscanStep);
            run(&mut statement)
        }
    }

    /// Prepare `sql` as the host's own statement, compiling every trigger
    /// it would fire and those they fire in turn, and drop it unrun. What
    /// the authorizer refused in an app trigger it compiles is the error.
    pub fn prepare_host(&self, sql: &str) -> Result<(), PolicyError<P>> {
        let prepared = {
            let _scope = self.authorization.internal();
            self.connection.prepare(sql).map(drop)
        };
        self.judged(prepared)
    }

    /// Prepare the app statement `sql`, judged as an app statement is, and
    /// drop it unrun: what preparing it tells the policy is the result.
    pub fn prepare_app(&self, sql: &str) -> rusqlite::Result<()> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization.app_result(self.connection.prepare(sql)).map(drop)
    }

    /// Run the app's statement `sql` with `params`, judged as the app's,
    /// returning the rows it changed.
    pub fn app_execute<Q: Params>(&self, sql: &str, params: Q) -> rusqlite::Result<usize> {
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

    /// Run the app's statements in `sql`, one after another, each judged as
    /// the app's.
    pub fn app_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.app_batch_each(sql, |_, step| step())
    }

    /// Run the app's statements in `sql` one at a time, handing `each` the
    /// text of each as SQLite expanded it and a call that steps it through
    /// its rows. `each` runs that call once, between whatever it does before
    /// and after the statement.
    pub fn app_batch_each(
        &self,
        sql: &str,
        mut each: impl FnMut(&str, &mut dyn FnMut() -> rusqlite::Result<()>) -> rusqlite::Result<()>,
    ) -> rusqlite::Result<()> {
        self.authorization.check_sql(sql)?;
        let mut batch = Batch::new(&self.connection, sql);
        loop {
            self.begin_app_statement()?;
            let Some(mut statement) = self.authorization.app_result(batch.next())? else {
                return Ok(());
            };
            let text = statement.expanded_sql().expect("prepared SQL");
            let mut step = || {
                // Step every row so later execution errors cannot be
                // discarded.
                let result = self.counting_steps(&mut statement, StepsOf::AppWrite, |statement| {
                    let mut rows = statement.raw_query();
                    let _scope = self.authorization.step();
                    while rows.next()?.is_some() {}
                    Ok(())
                });
                self.authorization.app_result(result)
            };
            each(&text, &mut step)?;
        }
    }

    /// SQLite's count of changes to the main schema, read afresh.
    pub fn schema_cookie(&self) -> rusqlite::Result<i64> {
        let _scope = self.authorization.internal();
        self.connection.prepare_cached("PRAGMA main.schema_version")?.query_row([], |r| r.get(0))
    }

    /// Run one migration step inside the open migration transaction. Which
    /// tables its statements write is decided as they are prepared (§18.1).
    pub fn migrating<T, E: From<PolicyError<P>>>(
        &self,
        run: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        assert!(self.observation.is_none(), "migrations precede commit observation");
        let result = run()?;
        self.require_transaction()?;
        Ok(result)
    }

    /// The first row of the app's query `sql` with `params`, through `map`;
    /// a read's statement when `recorded` says so.
    pub fn app_query_row<T, Q: Params>(
        &self,
        recorded: Recorded,
        sql: &str,
        params: Q,
        map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut map = Some(map);
        let mut value = None;
        self.app_rows(recorded, sql, params, |row| {
            let map = map.take().expect("the first row ends the read");
            value = Some(map(row)?);
            Ok(false)
        })?;
        value.ok_or(rusqlite::Error::QueryReturnedNoRows)
    }

    /// Every row of the app's query `sql` with `params`, through `map`; a
    /// read's statement when `recorded` says so.
    pub fn app_query<T, Q: Params>(
        &self,
        recorded: Recorded,
        sql: &str,
        params: Q,
        mut map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Vec<T>> {
        let mut values = Vec::new();
        self.app_rows(recorded, sql, params, |row| {
            values.push(map(row)?);
            Ok(true)
        })?;
        Ok(values)
    }

    /// Run an app statement and hand `each` its rows in order, under the
    /// authorizer's read and step scopes, until the rows end or `each`
    /// answers false.
    fn app_rows<Q: Params>(
        &self,
        recorded: Recorded,
        sql: &str,
        params: Q,
        mut each: impl FnMut(&Row<'_>) -> rusqlite::Result<bool>,
    ) -> rusqlite::Result<()> {
        self.authorization.check_sql(sql)?;
        self.begin_app_statement()?;
        self.authorization.begin_read_statement();
        let result = (|| {
            let mut statement = self.app_statement(sql)?;
            let parameters = crate::parameters(&self.connection, &statement, params)?;
            let plan = self.live_scope_plan(sql, &statement)?;
            // A run that returns as many rows as its LIMIT depends only on the
            // rows up to its last one (§4).
            let last_row = plan.as_ref().and_then(|plan| plan.last_row(&parameters));
            let mut last = None;
            let result = self.counting_steps(&mut statement, StepsOf::AppRead, |statement| {
                let mut rows = statement.query(rusqlite::params_from_iter(&parameters))?;
                let mut returned = 0;
                loop {
                    let row = {
                        let _scope = self.authorization.step();
                        self.authorization.app_result(rows.next())?
                    };
                    let Some(row) = row else { return Ok(()) };
                    returned += 1;
                    if let Some(last_row) =
                        last_row.as_ref().filter(|found| found.limit == returned)
                    {
                        last = Some(last_row.values(row)?);
                    }
                    if !each(row)? {
                        return Ok(());
                    }
                }
            });
            self.record_app_statement(plan.as_deref(), &parameters, last.as_deref());
            #[cfg(any(test, feature = "test-utils"))]
            if recorded == Recorded::Yes {
                if let Some(log) = self.statement_log.borrow().as_ref() {
                    log.push(statement.expanded_sql().expect("prepared SQL"));
                }
            }
            #[cfg(not(any(test, feature = "test-utils")))]
            let _ = recorded;
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
    pub fn changed_rows(&self) -> u64 {
        self.connection.total_changes()
    }

    /// Serve an open handle once opening succeeds (§4).
    pub(crate) fn serve(&mut self, handle: crate::Handle<P::Failure>) {
        assert!(self.handle.is_none(), "a connection serves one handle");
        self.authorization.serve(handle.clone());
        self.handle = Some(handle);
    }

    /// Serve the handle as its writer, capturing each commit's row changes and
    /// keeping a write's operations on the `recorded` tables whole.
    pub(crate) fn observe_commits(
        &mut self,
        handle: crate::Handle<P::Failure>,
        recorded: BTreeSet<String>,
    ) {
        assert!(self.observation.is_none(), "commit observation installed once");
        self.serve(handle);
        self.observation = Some(WriterObservation { recorded: Arc::new(recorded) });
    }

    /// The scope in which the app's write SQL runs.
    pub fn write_scope(&self) -> crate::authorization::WriteSql<'_, P> {
        self.authorization.write()
    }

    /// Refuse with `TransactionEnded` unless a transaction is open: a
    /// statement may have committed or rolled back the one its caller began.
    pub fn require_transaction(&self) -> Result<(), PolicyError<P>> {
        if self.connection.is_autocommit() {
            Err(crate::Error::TransactionEnded)
        } else {
            Ok(())
        }
    }

    fn begin_app_statement(&self) -> rusqlite::Result<()> {
        self.require_transaction()
            .map_err(|error| rusqlite::Error::UserFunctionError(Box::new(error)))?;
        // Migrations may create, rename or rebuild tables between app statements.
        // Ordinary writes reuse what the policy read without querying SQLite's
        // schema.
        self.refresh_policy(Refresh::Changed)?;
        self.authorization.begin_app_call();
        Ok(())
    }

    /// Run `run` in a write transaction, whose failures are `E`: a failed
    /// rollback joins the run's own error.
    pub fn transaction<T, E: crate::RollbackJoin<P::Failure, P::Refusal>>(
        &mut self,
        run: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        self.transaction_captured(false, run)
    }

    /// Streaming transfers publish table invalidations without a session's
    /// retained before/after BLOB values growing with the transferred object.
    pub fn stream_transaction<T, E: crate::RollbackJoin<P::Failure, P::Refusal>>(
        &mut self,
        run: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, E> {
        self.transaction_captured(true, run)
    }

    fn transaction_captured<T, E: crate::RollbackJoin<P::Failure, P::Refusal>>(
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

    /// Close the connection, failing if SQLite still holds a statement of
    /// it.
    pub fn close(self) -> Result<(), PolicyError<P>> {
        self.connection.close().map_err(|(_connection, error)| error.into())
    }

    /// Prepare a statement and never finalize it, so SQLite refuses to close
    /// this connection, as it does with `SQLITE_BUSY`.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn leave_statement_open(&self) {
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

    /// Call `callback` at each point this connection's write transactions
    /// pass as they commit, from now on.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn on_commit_point(
        &self,
        callback: impl Fn(crate::test_records::CommitPoint) + Send + 'static,
    ) {
        *self.commit_points.borrow_mut() = Some(Box::new(callback));
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn commit_point(&self, point: crate::test_records::CommitPoint) {
        if let Some(callback) = self.commit_points.borrow().as_ref() {
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

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
