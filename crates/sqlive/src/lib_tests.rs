//! A database over plain tables for sqlive's own tests: a writer, its
//! readers and live queries, under `NoPolicy`, in a directory of its own.

use crate::{
    Database, Handle, HandleFailure, HostSql, LiveQuery, NoPolicy, Options, PolicyError,
    ReconfigurableLiveQuery, WholeViews,
};
use caught::locks::{lock_unpoisoned, read_unpoisoned, write_unpoisoned};
use caught::{Bug, BugSink, BugSinkRef, CarriesBug};
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

/// The app's reads on one connection, as a live query or a one-shot read
/// runs them: each statement's reads are recorded.
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

/// What a test database opens with beyond its schema.
#[derive(Clone)]
pub(crate) struct TestOptions {
    /// The SQLite settings of every connection.
    pub(crate) settings: crate::SqliteSettings,
    /// The read-only connections beside the writer.
    pub(crate) readers: NonZeroUsize,
    /// The views and tables a live query reads whole.
    pub(crate) whole_views: Vec<&'static str>,
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
}

impl Default for TestOptions {
    fn default() -> Self {
        Self {
            settings: crate::SqliteSettings::default(),
            readers: NonZeroUsize::new(2).expect("two readers"),
            whole_views: Vec::new(),
            checkpoint_pages: None,
            statistics: false,
            unrecorded: Vec::new(),
            captured: BTreeSet::new(),
            prepare_on_reader: false,
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
        // The directory as SQLite names it, with no link on the way.
        let path =
            directory.path().canonicalize().expect("the directory exists").join("test.sqlite");
        let bugs = Arc::<HeardBugs>::default();
        let handle = Handle::new(Arc::clone(&bugs) as BugSinkRef, options.unrecorded);
        let (database, ()) = Database::open_with(
            &path,
            handle.clone(),
            Options {
                settings: options.settings,
                readers: options.readers,
                prepare_on_reader: options.prepare_on_reader,
                whole_views: Arc::new(WholeViews::new(options.whole_views)),
                policy: &|| NoPolicy,
                setup: &|_, _| Ok(()),
                capture: Some(options.captured),
                checkpoint_pages: options.checkpoint_pages,
                statistics: options.statistics,
            },
            |writer| writer.batch(schema),
        )
        .expect("the test database opens");
        Self { database: Arc::new(RwLock::new(Some(database))), handle, bugs, path, directory }
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

    /// Run `sql` as the app's write: its row operations on the captured
    /// tables kept one at a time, then nothing more of the host's, in one
    /// transaction the live queries then judge.
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

    /// A live query that runs `query`, the app's reads, on a reader, and runs
    /// it again when a commit changes what it read.
    pub(crate) fn subscribe<R, F>(&self, query: F) -> LiveQuery<R, QueryError>
    where
        F: Fn(TestRead<'_>) -> Result<R, QueryError> + Send + Sync + 'static,
        R: Send + 'static,
    {
        LiveQuery::new(self.subscribe_reconfigurable((), move |(), reader| query(reader)))
    }

    /// A live query whose request can be replaced.
    pub(crate) fn subscribe_reconfigurable<Q, R, F>(
        &self,
        request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R, QueryError>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, TestRead<'_>) -> Result<R, QueryError> + Send + Sync + 'static,
        R: Send + 'static,
    {
        let database = Arc::clone(&self.database);
        ReconfigurableLiveQuery::new(
            request,
            Arc::new(move |request, completion| {
                let slot = read_unpoisoned(&database);
                let database = slot.as_ref().ok_or(QueryError(crate::Error::Closed))?;
                let reader = database.reader();
                let result = reader.host(|reader| {
                    reader.live_read(
                        &mut |reads, lookups| completion.finish_judging(reads, lookups),
                        |reader| query(request, TestRead(reader)),
                    )
                });
                result
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

    /// Close the handle and every connection, keeping the directory; and
    /// what closing failed with.
    fn shut(self) -> (tempfile::TempDir, Vec<TestError>) {
        // As a host closes: its handle first, so no live query runs while
        // the connections close.
        self.handle.close();
        let database = write_unpoisoned(&self.database).take().expect("the test database is open");
        (self.directory, database.close())
    }
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
