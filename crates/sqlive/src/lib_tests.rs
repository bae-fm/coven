//! A database over plain tables for sqlive's own tests: a writer, its
//! readers and live queries, under `NoPolicy`, in a directory of its own.

use crate::{
    Database, Handle, HandleFailure, HostSql, LiveQuery, NoPolicy, Options, PolicyError,
    ReconfigurableLiveQuery, Values,
};
use panic_as_error::locks::{lock_unpoisoned, read_unpoisoned, write_unpoisoned};
use panic_as_error::{Bug, BugSink, BugSinkRef, CarriesBug};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

/// The error a test's call fails with.
pub(crate) type TestError = PolicyError<NoPolicy>;

/// The error a test's live query fails with: sqlive's own, which may carry
/// the bug of a run that panicked.
#[derive(Debug)]
pub(crate) struct QueryError(pub(crate) TestError);

impl From<TestError> for QueryError {
    fn from(error: TestError) -> Self {
        Self(error)
    }
}

impl From<rusqlite::Error> for QueryError {
    fn from(error: rusqlite::Error) -> Self {
        Self(error.into())
    }
}

impl CarriesBug for QueryError {
    fn bug(&self) -> Option<&Bug> {
        match &self.0 {
            crate::Error::Bug(bug) | crate::Error::HandleFailed(bug) => Some(bug),
            _ => None,
        }
    }
}

/// The app's reads on one connection, as a one-shot read runs them.
#[derive(Clone, Copy)]
pub(crate) struct TestRead<'a>(pub(crate) &'a HostSql<NoPolicy>);

impl TestRead<'_> {
    /// The first row of the app's query `sql`, through `map`.
    pub(crate) fn query_row<T, Q: rusqlite::Params>(
        &self,
        sql: &str,
        params: Q,
        map: impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        self.0.app_query_row(crate::Recorded::Yes, sql, params, map)
    }

    /// Every row of the app's query `sql`, through `map`.
    pub(crate) fn query<T, Q: rusqlite::Params>(
        &self,
        sql: &str,
        params: Q,
        map: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Vec<T>> {
        self.0.app_query(crate::Recorded::Yes, sql, params, map)
    }
}

/// One statement of a test's live query, with the values it binds.
#[derive(Clone)]
pub(crate) struct TestStatement {
    kind: StatementKind,
    values: Values,
}

#[derive(Clone)]
enum StatementKind {
    App(String),
    Compiled(crate::CompiledStatement<crate::Writer>),
    Host(String),
}

impl TestStatement {
    fn live(&self) -> crate::LiveStatement<'_> {
        let values = self.values.clone();
        match &self.kind {
            StatementKind::App(sql) => crate::LiveStatement::app(sql, values),
            StatementKind::Compiled(statement) => {
                crate::LiveStatement::compiled(*statement, values)
            }
            StatementKind::Host(sql) => crate::LiveStatement::host(sql, values),
        }
    }
}

/// The app's SELECT `sql`, bound to `values`.
pub(crate) fn app(sql: impl Into<String>, values: Values) -> TestStatement {
    TestStatement { kind: StatementKind::App(sql.into()), values }
}

/// The compiled read `statement`, bound to `values`.
pub(crate) fn compiled_read(
    statement: crate::CompiledStatement<crate::Writer>,
    values: Values,
) -> TestStatement {
    TestStatement { kind: StatementKind::Compiled(statement), values }
}

/// The host's own SELECT `sql`, bound to `values`.
pub(crate) fn host(sql: impl Into<String>, values: Values) -> TestStatement {
    TestStatement { kind: StatementKind::Host(sql.into()), values }
}

/// No values, for a statement with no parameters.
pub(crate) fn no_values() -> Values {
    Values::Positional(Vec::new())
}

/// `values`, bound in order.
pub(crate) fn at(values: &[&dyn rusqlite::ToSql]) -> Values {
    Values::positional(values).unwrap()
}

/// Each of `values`, bound in order.
pub(crate) fn each_at<T: rusqlite::ToSql>(values: impl IntoIterator<Item = T>) -> Values {
    let values = values.into_iter().map(|value| crate::SqlValue::of(&value));
    Values::Positional(values.collect::<rusqlite::Result<_>>().unwrap())
}

/// `values`, bound by name.
pub(crate) fn named(values: &[(&'static str, &dyn rusqlite::ToSql)]) -> Values {
    Values::named(values).unwrap()
}

/// The first row of `rows`, a live query's result, as `query_row` reads it,
/// or `QueryReturnedNoRows`.
pub(crate) fn first<T>(rows: Vec<T>) -> Result<T, QueryError> {
    let first = rows.into_iter().next();
    Ok(first.ok_or(rusqlite::Error::QueryReturnedNoRows)?)
}

/// `sql` with the plan `probe` builds of it, as a compiler ships it, when
/// sqlive can analyze it. The test keeps each for as long as it runs.
pub(crate) fn compiled(
    probe: &crate::SchemaProbe,
    sql: &str,
) -> Option<crate::CompiledStatement<crate::Writer>> {
    let plan = probe.compiled_plan(sql).unwrap().ok()?.encode();
    let sql: &'static str = Box::leak(sql.to_owned().into_boxed_str());
    Some(crate::CompiledStatement::new(sql, Box::leak(plan.into_boxed_str())))
}

/// The bugs a test's handle hands its sink.
#[derive(Default)]
pub(crate) struct HeardBugs(Mutex<Vec<Bug>>);

impl HeardBugs {
    /// The bugs heard so far, which the sink then forgets.
    pub(crate) fn take(&self) -> Vec<Bug> {
        std::mem::take(&mut *lock_unpoisoned(&self.0))
    }
}

impl BugSink for HeardBugs {
    fn report(&self, bug: &Bug) {
        lock_unpoisoned(&self.0).push(bug.clone());
    }
}

/// An app collation: its name and the order it gives two strings.
pub(crate) type Collation = (&'static str, fn(&str, &str) -> std::cmp::Ordering);

/// What a test database opens with beyond its schema.
#[derive(Clone)]
pub(crate) struct TestOptions {
    /// The SQLite settings of every connection.
    pub(crate) settings: crate::SqliteSettings,
    /// The read-only connections beside the writer.
    pub(crate) readers: NonZeroUsize,
    /// The tables whose rows the test keeps in memory.
    pub(crate) host_tables: Option<crate::HostTables>,
    /// Checkpoint the WAL on a thread of its own at this many frames.
    pub(crate) checkpoint_pages: Option<std::num::NonZeroU32>,
    /// Keep the planner's statistics.
    pub(crate) statistics: bool,
    /// The tables whose changes no write records, since no live query reads
    /// them.
    pub(crate) unrecorded: Vec<String>,
    /// The tables, lowercase, whose operations a write keeps whole.
    pub(crate) captured: BTreeSet<String>,
    /// Open the first reader before the writer, and leave the journal as it
    /// is, as a database whose opening must not write does.
    pub(crate) prepare_on_reader: bool,
    /// The app's collations, each registered on every connection as it
    /// opens: its name and the order it gives two strings.
    pub(crate) collations: Vec<Collation>,
}

impl Default for TestOptions {
    fn default() -> Self {
        Self {
            settings: crate::SqliteSettings::default(),
            readers: NonZeroUsize::new(2).expect("two readers"),
            host_tables: None,
            checkpoint_pages: None,
            statistics: false,
            unrecorded: Vec::new(),
            captured: BTreeSet::new(),
            prepare_on_reader: false,
            collations: Vec::new(),
        }
    }
}

/// A database of plain tables in a temporary directory, whose writes the
/// handle's live queries observe.
pub(crate) struct TestDatabase {
    /// Shared with the live queries, which find it gone once it closes.
    database: Arc<RwLock<Option<Database<NoPolicy>>>>,
    handle: Handle<HandleFailure>,
    bugs: Arc<HeardBugs>,
    path: PathBuf,
    /// The pause the next run of a live query `subscribe_paused` made waits
    /// in.
    pause: Arc<Mutex<Option<Pause>>>,
    // Dropped last: the connections close before their directory goes.
    directory: tempfile::TempDir,
}

impl TestDatabase {
    /// A database whose writer created `schema`.
    pub(crate) fn open(schema: &str) -> Self {
        Self::open_with(schema, TestOptions::default())
    }

    /// A database whose writer created `schema`, opened with `options`.
    pub(crate) fn open_with(schema: &str, options: TestOptions) -> Self {
        Self::open_in(tempfile::tempdir().expect("a temporary directory"), schema, options)
    }

    /// The database in `directory`, opened with `options`, whose writer then
    /// ran `schema`.
    fn open_in(directory: tempfile::TempDir, schema: &str, options: TestOptions) -> Self {
        // The directory as SQLite names it, with every symbolic link in its
        // path resolved.
        let path =
            directory.path().canonicalize().expect("the directory exists").join("test.sqlite");
        let bugs = Arc::<HeardBugs>::default();
        let handle = Handle::new(Arc::clone(&bugs) as BugSinkRef, options.unrecorded);
        let pause = Arc::<Mutex<Option<Pause>>>::default();
        let armed = Arc::clone(&pause);
        let (database, ()) = Database::open_with(
            &path,
            handle.clone(),
            Options {
                settings: options.settings,
                readers: options.readers,
                prepare_on_reader: options.prepare_on_reader,
                policy: &|| NoPolicy,
                setup: &|setup, _| {
                    if let Some(tables) = &options.host_tables {
                        setup.host_tables(tables)?;
                    }
                    for (name, compare) in &options.collations {
                        setup.collation(name, *compare)?;
                    }
                    let armed = Arc::clone(&armed);
                    setup.scalar_function(
                        "test_pause",
                        0,
                        rusqlite::functions::FunctionFlags::SQLITE_UTF8,
                        move |_| {
                            if let Some(pause) = lock_unpoisoned(&armed).take() {
                                pause.entered.send(()).expect("the test waits for the pause");
                                pause
                                    .released
                                    .recv_timeout(HANG_GUARD)
                                    .expect("the test ends the pause");
                            }
                            Ok(0)
                        },
                    )?;
                    Ok(())
                },
                capture: Some(options.captured),
                checkpoint_pages: options.checkpoint_pages,
                statistics: options.statistics,
            },
            |writer| writer.batch(schema),
        )
        .expect("the test database opens");
        Self {
            database: Arc::new(RwLock::new(Some(database))),
            handle,
            bugs,
            path,
            pause,
            directory,
        }
    }

    /// The database's file.
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// The handle the database's connections serve.
    pub(crate) fn handle(&self) -> &Handle<HandleFailure> {
        &self.handle
    }

    /// The bugs the handle heard so far, which it then forgets.
    pub(crate) fn take_bugs(&self) -> Vec<Bug> {
        self.bugs.take()
    }

    /// Run `run` on the database's connections.
    pub(crate) fn with_database<T>(&self, run: impl FnOnce(&Database<NoPolicy>) -> T) -> T {
        let database = read_unpoisoned(&self.database);
        run(database.as_ref().expect("the test database is open"))
    }

    /// Run `run` on the writer.
    pub(crate) fn host<T>(&self, run: impl FnOnce(&mut HostSql<NoPolicy>) -> T) -> T {
        self.with_database(|database| database.host(run))
    }

    /// Run `run` on a reader.
    pub(crate) fn read<T>(&self, run: impl FnOnce(&HostSql<NoPolicy>) -> T) -> T {
        self.with_database(|database| database.reader().host(run))
    }

    /// Run `sql` as the app's write, in one transaction that the live queries
    /// then judge. The write records each of its row operations on the
    /// captured tables, one at a time, and the host adds nothing more to the
    /// transaction.
    pub(crate) fn write(&self, sql: &str) -> Result<(), TestError> {
        self.write_app(|writer| writer.app_batch(sql))
    }

    /// Run `app`, the app's statements in the write scope, as [`Self::write`]
    /// runs its SQL.
    pub(crate) fn write_app<T>(
        &self,
        app: impl FnOnce(&HostSql<NoPolicy>) -> rusqlite::Result<T>,
    ) -> Result<T, TestError> {
        self.host(|writer| {
            writer.with_profile(|writer| {
                writer.write_with(
                    |writer, _operations| {
                        let _write = writer.write_scope();
                        app(writer).map_err(TestError::from)
                    },
                    |_writer, _operations, result| Ok(result),
                )
            })
        })
    }

    /// Run `sql` as the app's bulk write, which no pre-update hook watches.
    pub(crate) fn bulk(&self, sql: &str) -> Result<(), TestError> {
        self.host(|writer| {
            writer.bulk_write(|writer| writer.app_batch(sql).map_err(TestError::from))
        })
    }

    /// Run `sql` as the host's own SQL in one transaction on the writer,
    /// which the live queries then judge.
    pub(crate) fn commit(&self, sql: &str) {
        self.host(|writer| {
            writer.with_profile(|writer| writer.transaction(|writer| writer.batch(sql)))
        })
        .unwrap_or_else(|error| panic!("{sql} commits: {error}"));
    }

    /// The single value `sql` reads on the writer.
    pub(crate) fn value<T: rusqlite::types::FromSql>(&self, sql: &str) -> T {
        self.host(|writer| writer.query_row(sql, [], |row| row.get(0)).expect(sql))
    }

    /// Run `read`, the app's one-shot read, on a reader.
    pub(crate) fn read_app<T>(
        &self,
        read: impl FnOnce(TestRead<'_>) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        self.read(|reader| reader.read_snapshot(|reader| read(TestRead(reader))))
    }

    /// A live query of the app's SELECT `sql` bound to `values`, which gives
    /// every row through `map`, and runs again when a commit changes what it
    /// read.
    pub(crate) fn subscribe_rows<T>(
        &self,
        sql: impl Into<String>,
        values: Values,
        map: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T> + Send + Sync + 'static,
    ) -> LiveQuery<Vec<T>, QueryError>
    where
        T: Send + 'static,
    {
        self.subscribe(vec![app(sql, values)], move |_, row| map(row))
    }

    /// A live query of the app's SELECT `sql` bound to `values`, as
    /// `subscribe_rows`, whose next run pauses once its snapshot has begun,
    /// before or after `sql` runs as `at` says: the run tells the receiver
    /// it returns that it paused, and goes on once the sender is told.
    pub(crate) fn subscribe_paused<T>(
        &self,
        sql: impl Into<String>,
        values: Values,
        map: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T> + Send + Sync + 'static,
        at: PauseAt,
    ) -> (
        LiveQuery<Vec<T>, QueryError>,
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    )
    where
        T: Clone + PartialEq + Send + 'static,
    {
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        *lock_unpoisoned(&self.pause) = Some(Pause { entered, released });
        let pause = host("SELECT test_pause()", no_values());
        let (statements, read) = match at {
            PauseAt::BeforeSql => (vec![pause, app(sql, values)], 1),
            PauseAt::AfterSql => (vec![app(sql, values), pause], 0),
        };
        let query = self
            .subscribe(
                statements,
                move |index, row| {
                    if index == read {
                        map(row).map(Some)
                    } else {
                        Ok(None)
                    }
                },
            )
            .process(|rows| Ok(rows.into_iter().flatten().collect()));
        (query, entry, release)
    }

    /// A live query of `statements`, read on one snapshot, which gives every
    /// row of each through `map` with the statement's place in the list.
    pub(crate) fn subscribe<T>(
        &self,
        statements: Vec<TestStatement>,
        map: impl Fn(usize, &rusqlite::Row<'_>) -> rusqlite::Result<T> + Send + Sync + 'static,
    ) -> LiveQuery<Vec<T>, QueryError>
    where
        T: Send + 'static,
    {
        LiveQuery::new(self.subscribe_reconfigurable((), move |()| statements.clone(), map))
    }

    /// A live query whose request the test can replace: `statements` gives
    /// the statements each request reads.
    pub(crate) fn subscribe_reconfigurable<Q, T>(
        &self,
        request: Q,
        statements: impl Fn(&Q) -> Vec<TestStatement> + Send + Sync + 'static,
        map: impl Fn(usize, &rusqlite::Row<'_>) -> rusqlite::Result<T> + Send + Sync + 'static,
    ) -> ReconfigurableLiveQuery<Q, Vec<T>, QueryError>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        T: Send + 'static,
    {
        let database = Arc::clone(&self.database);
        ReconfigurableLiveQuery::new(
            request,
            Arc::new(move |request, completion| {
                let slot = read_unpoisoned(&database);
                let database = slot.as_ref().ok_or(QueryError(crate::Error::Closed))?;
                let reader = database.reader();
                let statements = statements(request);
                let live: Vec<_> = statements.iter().map(TestStatement::live).collect();
                let mut rows = Vec::new();
                reader.host(|reader| {
                    reader.live_read(
                        &mut |reads, lookups| completion.finish_judging(reads, lookups),
                        &live,
                        |index, row| {
                            rows.push(map(index, row)?);
                            Ok(())
                        },
                    )
                })?;
                Ok(rows)
            }),
            QueryError,
            &self.handle,
            Arc::clone(&self.bugs) as BugSinkRef,
        )
    }

    /// Close the database, failing on any connection's failure.
    pub(crate) fn close(self) {
        drop(self.close_keeping_file());
    }

    /// Close the database, failing on any connection's failure, and keep
    /// its directory: the file at [`Self::path`] stays until it drops.
    pub(crate) fn close_keeping_file(self) -> tempfile::TempDir {
        let (directory, failures) = self.shut();
        assert!(failures.is_empty(), "closing failed: {failures:?}");
        directory
    }

    /// Close the database, and open its file again with `options`.
    pub(crate) fn reopen(self, options: TestOptions) -> Self {
        Self::open_in(self.close_keeping_file(), "", options)
    }

    /// Close the handle and every connection, keep the directory, and return
    /// it with the errors closing failed with.
    fn shut(self) -> (tempfile::TempDir, Vec<TestError>) {
        // As a host closes: its handle first, so no live query runs while
        // the connections close.
        self.handle.close();
        let database = write_unpoisoned(&self.database).take().expect("the test database is open");
        (self.directory, database.close())
    }
}

/// Where a live query's run pauses (`subscribe_paused`).
#[derive(Clone, Copy)]
pub(crate) enum PauseAt {
    /// Before the query's SQL runs, so that the run knows nothing yet of what
    /// it reads.
    BeforeSql,
    /// After the query's SQL ran, before the run ends.
    AfterSql,
}

/// A run's pause: whom it tells that it paused, and what it waits on.
struct Pause {
    entered: tokio::sync::oneshot::Sender<()>,
    released: std::sync::mpsc::Receiver<()>,
}

/// How long a test waits for a live query's answer before it fails.
pub(crate) const HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(5);

/// A live query's next result, failing a test it doesn't answer within five
/// seconds or after it closes.
pub(crate) async fn next_result<T>(query: &mut LiveQuery<T, QueryError>) -> Result<T, QueryError>
where
    T: Clone + PartialEq + Send + 'static,
{
    tokio::time::timeout(HANG_GUARD, query.next_inline())
        .await
        .expect("the live query answers in time")
        .expect("the live query is open")
}

/// Run `query` again, which a commit marked to run again, and return how
/// many times it has run.
pub(crate) fn rerun<T>(query: &mut LiveQuery<T, QueryError>) -> u64
where
    T: Clone + PartialEq + Send + 'static,
{
    assert!(query.is_marked_for_rerun(), "a commit marked the query to run again");
    drop(query.run_changed());
    query.runs().runs()
}

/// A live query's next value, failing a test it doesn't answer or answers
/// with an error.
pub(crate) async fn next<T>(query: &mut LiveQuery<T, QueryError>) -> T
where
    T: Clone + PartialEq + Send + 'static,
{
    next_result(query).await.unwrap()
}

impl TestDatabase {
    /// Count the work of each of the database's connections into `counter`
    /// from now on.
    pub(crate) fn track_work(&self, counter: &crate::ConnectionWorkCounter) {
        self.with_database(|database| {
            database.each_connection(|connection| connection.test_track_work(counter.clone()))
        });
    }
}
