use super::KeptStatements;
use crate::tests::{
    compiled_read, next, no_values, QueryError, TestDatabase, TestError, TestOptions, TestRead,
};
use crate::triggered_inserts::SCHEMA as TRIGGERED;
use crate::{MainSchema, ScopePlan, ScopePlanSize};
use std::sync::Arc;

fn notes() -> TestDatabase {
    open("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)", "notes")
}

/// A database of `schema` that records each operation of a write on
/// `captured`, with every column's value before and after it.
fn open(schema: &str, captured: &str) -> TestDatabase {
    TestDatabase::open_with(
        schema,
        TestOptions { captured: [captured.to_owned()].into(), ..TestOptions::default() },
    )
}

/// The statements the writer has prepared afresh so far.
fn fresh(db: &TestDatabase) -> usize {
    db.host(|writer| writer.app_statements.fresh.get())
}

const INSERT: &str = "INSERT INTO notes VALUES (?1, 'title')";

/// An app write's statement is prepared once, then run from the cache.
#[test]
fn a_repeated_app_statement_is_prepared_once() {
    let db = notes();
    let before = fresh(&db);
    for id in ["1", "2", "3"] {
        db.write_app(|sql| sql.app_execute(INSERT, [id])).unwrap();
    }
    assert_eq!(fresh(&db) - before, 1);
    db.close();
}

/// A live query's statement from the cache records what it reads as its
/// first preparation did, so later writes still rerun the query.
#[tokio::test]
async fn a_cached_query_still_records_its_reads() {
    let db = notes();
    let mut titles = db.subscribe_rows("SELECT title FROM notes ORDER BY id", no_values(), |r| {
        r.get::<_, String>(0)
    });
    assert!(next(&mut titles).await.is_empty());
    for (id, title) in [("1", "a"), ("2", "b"), ("3", "c")] {
        db.write_app(|sql| sql.app_execute("INSERT INTO notes VALUES (?1, ?2)", [id, title]))
            .unwrap();
        assert_eq!(next(&mut titles).await.last().unwrap(), title);
    }
    db.close();
}

/// A statement first kept from a one-shot read, which records nothing of
/// what it reads, still records its reads in a live query: later writes
/// rerun the query.
#[tokio::test]
async fn a_query_kept_from_a_one_shot_read_still_records_its_reads_live() {
    const TITLES: &str = "SELECT title FROM notes ORDER BY id";
    let db = notes();
    let titles = |sql: TestRead<'_>| -> Result<Vec<String>, QueryError> {
        Ok(sql.query(TITLES, [], |r| r.get::<_, String>(0))?)
    };
    assert!(db.read_app(titles).unwrap().is_empty());
    let mut live = db.subscribe_rows(TITLES, no_values(), |r| r.get::<_, String>(0));
    assert!(next(&mut live).await.is_empty());
    db.write("INSERT INTO notes VALUES ('1','a')").unwrap();
    assert_eq!(next(&mut live).await, ["a"]);
    db.close();
}

/// The same SQL is judged in each context it runs in: kept prepared from a
/// write, it is still refused in a read.
#[test]
fn a_statement_cached_in_a_write_is_still_refused_in_a_read() {
    let db = notes();
    db.write_app(|sql| sql.app_execute(INSERT, ["1"])).unwrap();
    let refused = db.host(|writer| {
        writer
            .transaction(|writer| {
                let _reading = writer.authorization.reading();
                Ok::<_, TestError>(writer.app_query(crate::Recorded::No, INSERT, ["2"], |_| Ok(())))
            })
            .unwrap()
    });
    match refused {
        Err(rusqlite::Error::UserFunctionError(error)) => assert!(
            matches!(
                error.downcast_ref::<TestError>(),
                Some(crate::Error::StatementForbidden {
                    operation: "writing through a read context"
                })
            ),
            "{error:?}"
        ),
        result => panic!("expected the read's refusal, received {result:?}"),
    }
    // And a refused read leaves the write's statement working.
    db.write("INSERT INTO notes VALUES ('3','title')").unwrap();
    db.close();
}

/// How many app statements and how many of the host's `reader` has prepared.
fn prepared(reader: &crate::HostSql<crate::NoPolicy>) -> (usize, usize) {
    (reader.app_statements.fresh.get(), reader.app_statements.host_fresh.get())
}

/// Run `read` on one reader of `db` as a one-shot read, then `statement`
/// three times as a live query's run, and return the app statements and the
/// host statements those four runs prepared. Each run's transaction also
/// runs sqlive's own statements, which a run before the counted ones
/// prepares.
fn prepared_in_four_runs(
    db: &TestDatabase,
    read: impl Fn(&crate::HostSql<crate::NoPolicy>) -> Result<(), TestError>,
    statement: crate::LiveStatement<'_>,
) -> (usize, usize) {
    db.with_database(|database| {
        let reader = database.reader();
        reader.host(|reader| {
            let live = |statements: &[crate::LiveStatement<'_>]| {
                reader
                    .live_read(&mut |_reads, _lookups| Ok(()), statements, |_, _| Ok(()))
                    .unwrap();
            };
            live(&[]);
            let before = prepared(reader);
            reader.read_snapshot(&read).unwrap();
            for _ in 0..3 {
                live(std::slice::from_ref(&statement));
            }
            let after = prepared(reader);
            (after.0 - before.0, after.1 - before.1)
        })
    })
}

/// A connection prepares an app read once: a one-shot read and each run of a
/// live query then run that statement.
#[test]
fn one_shot_and_live_runs_share_one_prepared_statement() {
    let db = notes();
    let app = prepared_in_four_runs(
        &db,
        |reader| {
            reader.app_query(crate::Recorded::No, TITLES, ["1"], |_| Ok(()))?;
            Ok(())
        },
        crate::LiveStatement::app(TITLES, crate::tests::at(&[&"1"])),
    );
    assert_eq!(app, (1, 0));
    db.close();
}

/// A connection prepares a host read once too, whether a live query's run
/// records what it reads or not.
#[test]
fn one_shot_and_live_runs_share_one_prepared_host_statement() {
    let db = notes();
    let host = prepared_in_four_runs(
        &db,
        |reader| {
            reader.query(TITLES, ["1"], |_| Ok(()))?;
            Ok(())
        },
        crate::LiveStatement::host(TITLES, crate::tests::at(&[&"1"])),
    );
    assert_eq!(host, (0, 1));
    db.close();
}

/// A read that a write ran is judged as reading only, so a read runs the
/// statement the write prepared.
#[test]
fn a_select_a_write_prepared_is_reused_by_a_read() {
    let db = notes();
    let titles = db.host(|writer| {
        let before = prepared(writer).0;
        writer
            .transaction(|writer| {
                let select = |writer: &crate::HostSql<crate::NoPolicy>| {
                    writer.app_query(crate::Recorded::No, TITLES, ["1"], |_| Ok(()))
                };
                {
                    let _writing = writer.write_scope();
                    select(writer)?;
                }
                let _reading = writer.authorization.reading();
                select(writer)?;
                Ok::<_, TestError>(())
            })
            .unwrap();
        prepared(writer).0 - before
    });
    assert_eq!(titles, 1);
    db.close();
}

/// Preparing `VACUUM` or a comment asks the authorizer about nothing, so
/// sqlive can't judge it, and refuses it.
#[test]
fn a_statement_the_authorizer_never_judges_is_refused() {
    let db = notes();
    for sql in ["VACUUM", "-- nothing to run"] {
        let refused = db.write_app(|writer| writer.app_execute(sql, []));
        assert!(
            matches!(
                refused,
                Err(crate::Error::StatementForbidden {
                    operation: "a statement SQLite asks nothing about"
                })
            ),
            "{sql}: {refused:?}"
        );
    }
    db.close();
}

/// A schema change drops what the cache kept: the statement is prepared and
/// judged again against the new schema.
#[test]
fn a_schema_change_prepares_again() {
    let db = notes();
    let write = |id: &str| db.write_app(|sql| sql.app_execute(INSERT, [id])).unwrap();
    write("1");
    let before = fresh(&db);
    db.host(|writer| writer.batch("CREATE TABLE elsewhere(id INTEGER PRIMARY KEY)").unwrap());
    write("2");
    write("3");
    assert_eq!(fresh(&db) - before, 1);
    let count: i64 = db
        .read_app(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?))
        .unwrap();
    assert_eq!(count, 3);
    db.close();
}

/// A live query's statement that reads one note's title.
const TITLES: &str = "SELECT title FROM notes WHERE id = ?1";

/// A database with the note `TITLES` reads, and a counter of its connections'
/// work.
fn note_of_title() -> (TestDatabase, crate::ConnectionWorkCounter) {
    let db = notes();
    db.write_app(|sql| sql.app_execute(INSERT, ["1"])).unwrap();
    let counter = crate::ConnectionWorkCounter::new();
    db.track_work(&counter);
    (db, counter)
}

/// The values that bind `TITLES` to note 1.
fn note_one() -> crate::Values {
    crate::tests::at(&[&"1"])
}

/// The titles `statement` reads as a live query's run on `reader`.
fn live_titles(
    reader: &crate::Reader<'_, crate::NoPolicy>,
    statement: crate::LiveStatement<'_>,
) -> Vec<String> {
    let mut titles = Vec::new();
    let read: Result<(), TestError> = reader.host(|reader| {
        reader.live_read(&mut |_reads, _lookups| Ok(()), &[statement], |_, row| {
            titles.push(row.get(0)?);
            Ok(())
        })
    });
    read.unwrap();
    titles
}

/// Run `TITLES` as a live query's statement on each of `db`'s two readers,
/// holding both at once so that each run is on a reader of its own, and
/// return the scope plans the runs built.
fn plans_built_on_each_reader(db: &TestDatabase, counter: &crate::ConnectionWorkCounter) -> usize {
    db.with_database(|database| {
        let (first, second) = (database.reader(), database.reader());
        for reader in [&first, &second] {
            let title = live_titles(reader, crate::LiveStatement::app(TITLES, note_one()));
            assert_eq!(title, ["title"]);
        }
    });
    counter.take().scope_plans_built
}

/// A database's readers share a statement's scope plan: two readers running
/// the same live query's statement build its plan once, and later runs build
/// none.
#[test]
fn readers_share_a_statements_scope_plan() {
    let (db, counter) = note_of_title();
    assert_eq!(plans_built_on_each_reader(&db, &counter), 1);
    assert_eq!(plans_built_on_each_reader(&db, &counter), 0);
    db.close();
}

/// A database keeps one entry for each statement against its schema, which
/// every connection shares: the writer's run of `TITLES` in a write and two
/// readers' live runs leave one judgment of it and its one plan.
#[test]
fn a_statement_is_kept_once_for_every_connection() {
    let (db, counter) = note_of_title();
    let title = db
        .write_app(|sql| {
            sql.app_query(crate::Recorded::No, TITLES, ["1"], |row| row.get::<_, String>(0))
        })
        .unwrap();
    assert_eq!(title, ["title".to_owned()]);
    assert_eq!(plans_built_on_each_reader(&db, &counter), 1);
    let kept = db.host(|writer| {
        let cookie = writer.app_schema_cookie().unwrap();
        writer.app_statements.kept.kept_of(cookie, TITLES)
    });
    assert_eq!(kept, Some((1, true)));
    db.close();
}

/// A schema change drops the plans kept against the old schema: the next
/// run builds the statement's plan again, for every reader.
#[test]
fn a_schema_change_builds_a_scope_plan_again() {
    let (db, counter) = note_of_title();
    assert_eq!(plans_built_on_each_reader(&db, &counter), 1);
    db.host(|writer| writer.batch("CREATE TABLE elsewhere(id INTEGER PRIMARY KEY)").unwrap());
    assert_eq!(plans_built_on_each_reader(&db, &counter), 1);
    db.close();
}

/// What a live run of `sql`, or a one-shot read of it, counts over two tables.
async fn work_of_plan(sql: &'static str, live: bool) -> crate::ConnectionWork {
    let db = TestDatabase::open(
        "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, album TEXT);
         CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, title TEXT)",
    );
    let counter = crate::ConnectionWorkCounter::new();
    db.track_work(&counter);
    let titles = move |sqlite: TestRead<'_>| -> Result<Vec<String>, QueryError> {
        Ok(sqlite.query(sql, [], |r| r.get::<_, String>(0))?)
    };
    if live {
        let mut query = db.subscribe_rows(sql, no_values(), |r| r.get::<_, String>(0));
        next(&mut query).await;
        drop(query);
    } else {
        db.read_app(titles).unwrap();
    }
    let work = counter.take();
    db.close();
    work
}

const ALBUM_TITLES: &str =
    "SELECT t.title FROM tracks t JOIN releases r ON t.release = r.id WHERE r.album = 'x'";

/// The equality of the join gives an edge each way.
#[tokio::test]
async fn a_live_run_counts_the_size_of_its_plan() {
    let work = work_of_plan(ALBUM_TITLES, true).await;
    assert_eq!(work.scope_plans_built, 1);
    assert_eq!(work.scope_plan_size, ScopePlanSize { sources: 2, cores: 1, edges: 2 });
    assert!(work.scope_plan_time > std::time::Duration::ZERO);
}

#[tokio::test]
async fn a_one_shot_read_counts_no_plan() {
    let work = work_of_plan(ALBUM_TITLES, false).await;
    assert_eq!(work.scope_plans_built, 0);
    assert_eq!(work.scope_plan_size, ScopePlanSize::default());
    assert_eq!(work.scope_plan_time, std::time::Duration::ZERO);
}

/// The planner stops at the NATURAL join, after it read `tracks`.
#[tokio::test]
async fn a_plan_that_stops_counts_what_it_read_before() {
    let work = work_of_plan("SELECT title FROM tracks NATURAL JOIN releases", true).await;
    assert_eq!(work.scope_plans_built, 1);
    assert_eq!(work.scope_plan_size, ScopePlanSize { sources: 1, cores: 1, edges: 0 });
}

/// A plan of some statement, for tests of the map that keeps plans.
fn some_plan() -> Arc<ScopePlan> {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let statement = connection.prepare("SELECT 1").unwrap();
    let schema = MainSchema::read(&connection).unwrap();
    Arc::new(ScopePlan::build(&connection, &schema, "SELECT 1", &statement).unwrap())
}

/// The plans built against one schema share one read of it, and a later
/// schema's plans share a read of their own.
#[test]
fn the_plans_of_a_schema_share_one_read_of_it() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let plans = KeptStatements::new();
    let mut reads = 0;
    let mut build = |cookie, sql| {
        let built = plans.plan(cookie, sql, || {
            plans.schema(cookie, || {
                reads += 1;
                MainSchema::read(&connection)
            })?;
            Ok(some_plan())
        });
        built.unwrap();
        reads
    };
    assert_eq!(build(1, "SELECT 1"), 1);
    assert_eq!(build(1, "SELECT 2"), 1);
    assert_eq!(build(2, "SELECT 1"), 2);
    assert_eq!(build(2, "SELECT 2"), 2);
}

/// What a second caller did first while another was building the plan.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    Waited,
    Built,
}

/// What two callers that needed one plan at once got.
struct TwoCallers {
    /// The first caller's result, or its panic.
    first: std::thread::Result<rusqlite::Result<Arc<ScopePlan>>>,
    /// The second caller's first step, what it got, and how often it built.
    step: Step,
    second: Arc<ScopePlan>,
    second_builds: usize,
}

/// Ask `plans` for one plan from two threads: the first caller's build ends
/// as `first` does, and only once the second caller waits for it or builds a
/// plan of its own.
fn two_callers(
    plans: &KeptStatements,
    first: impl FnOnce() -> rusqlite::Result<Arc<ScopePlan>> + Send,
) -> TwoCallers {
    let (did, second_did) = std::sync::mpsc::channel();
    let (began, building) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let waited = did.clone();
    plans.on_next_wait(move || waited.send(Step::Waited).unwrap());
    let plan = some_plan();
    std::thread::scope(|scope| {
        let builder = scope.spawn(move || {
            plans.plan(1, TITLES, || {
                began.send(()).unwrap();
                released.recv().unwrap();
                first()
            })
        });
        building.recv().unwrap();
        let second = scope.spawn(move || {
            let mut builds = 0;
            let got = plans.plan(1, TITLES, || {
                builds += 1;
                did.send(Step::Built).unwrap();
                Ok(plan)
            });
            (got.unwrap(), builds)
        });
        let step = second_did.recv().unwrap();
        release.send(()).unwrap();
        let first = builder.join();
        let (second, second_builds) = second.join().unwrap();
        TwoCallers { first, step, second, second_builds }
    })
}

/// Two first runs of a statement at the same moment build its plan once: the
/// second waits for the first's build and uses it.
#[test]
fn two_first_runs_at_once_build_a_plan_once() {
    let plans = KeptStatements::new();
    let plan = some_plan();
    let built = Arc::clone(&plan);
    let callers = two_callers(&plans, move || Ok(built));
    assert!(Arc::ptr_eq(&callers.first.unwrap().unwrap(), &plan));
    assert_eq!((callers.step, callers.second_builds), (Step::Waited, 0));
    assert!(Arc::ptr_eq(&callers.second, &plan));
}

/// A build that fails gives its caller the failure and wakes the caller
/// waiting for it, which builds the plan itself.
#[test]
fn a_failed_build_leaves_the_plan_to_the_waiting_caller() {
    let plans = KeptStatements::new();
    let callers = two_callers(&plans, || Err(rusqlite::Error::QueryReturnedNoRows));
    assert_eq!(callers.first.unwrap().map(drop).unwrap_err(), rusqlite::Error::QueryReturnedNoRows);
    assert_eq!((callers.step, callers.second_builds), (Step::Waited, 1));
    let kept = plans.plan(1, TITLES, || unreachable!("the plan is kept")).unwrap();
    assert!(Arc::ptr_eq(&kept, &callers.second));
}

/// A build that panics panics in its caller and wakes the caller waiting for
/// it, too.
#[test]
fn a_panicking_build_leaves_the_plan_to_the_waiting_caller() {
    let plans = KeptStatements::new();
    let callers = two_callers(&plans, || panic!("the build broke"));
    let panic = callers.first.map(drop).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"the build broke"));
    assert_eq!((callers.step, callers.second_builds), (Step::Waited, 1));
    let kept = plans.plan(1, TITLES, || unreachable!("the plan is kept")).unwrap();
    assert!(Arc::ptr_eq(&kept, &callers.second));
}

/// A run still on an older schema leaves the newer schema's plans kept.
#[test]
fn an_older_schemas_run_leaves_the_newer_plans_kept() {
    let plans = KeptStatements::new();
    let newer = plans.plan(2, TITLES, || Ok(some_plan())).unwrap();
    let older = plans.plan(1, TITLES, || Ok(some_plan())).unwrap();
    assert!(!Arc::ptr_eq(&older, &newer));
    let kept = plans.plan(2, TITLES, || unreachable!("the plan is kept")).unwrap();
    assert!(Arc::ptr_eq(&kept, &newer));
}

/// What one app statement whose triggers reach a view costs through sqlive,
/// against plain SQLite preparing it each time and from its cache. Run
/// explicitly.
#[test]
#[ignore = "one timing measurement, run explicitly"]
fn a_triggered_statements_cost() {
    const STATEMENTS: usize = 500;
    const INSERT: &str = "INSERT INTO scans VALUES (?1, ?2, ?3)";
    let directory = tempfile::tempdir().unwrap();
    let plain = rusqlite::Connection::open(directory.path().join("plain.db")).unwrap();
    plain
        .execute_batch(&format!("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; {TRIGGERED}"))
        .unwrap();
    let row = |i: usize| (format!("id-{i}"), format!("Artist/Album {i}"), format!("title {i}"));
    let plain_run = |cached: bool, start: usize| {
        plain.execute_batch("BEGIN").unwrap();
        let started = std::time::Instant::now();
        for i in start..start + STATEMENTS {
            if cached {
                plain.prepare_cached(INSERT).unwrap().execute(row(i)).unwrap();
            } else {
                plain.prepare(INSERT).unwrap().execute(row(i)).unwrap();
            }
        }
        let elapsed = started.elapsed();
        plain.execute_batch("COMMIT").unwrap();
        elapsed / STATEMENTS as u32
    };
    let fresh = plain_run(false, 0);
    let cached = plain_run(true, STATEMENTS);
    // The same triggers compiled into a statement that changes no row, so
    // its cost is preparing it.
    const MISS: &str = "UPDATE scans SET title=?2 WHERE id=?1";
    let plain_miss = |cached: bool| {
        let started = std::time::Instant::now();
        for i in 0..STATEMENTS {
            let row = (format!("absent-{i}"), "t");
            if cached {
                plain.prepare_cached(MISS).unwrap().execute(row).unwrap();
            } else {
                plain.prepare(MISS).unwrap().execute(row).unwrap();
            }
        }
        started.elapsed() / STATEMENTS as u32
    };
    let (fresh_miss, cached_miss) = (plain_miss(false), plain_miss(true));

    let db = open(TRIGGERED, "scans");
    let started = std::time::Instant::now();
    db.write_app(|sql| {
        for i in 0..STATEMENTS {
            sql.app_execute(INSERT, row(i))?;
        }
        Ok(())
    })
    .unwrap();
    let sqlive = started.elapsed() / STATEMENTS as u32;
    let started = std::time::Instant::now();
    db.write_app(|sql| {
        for i in 0..STATEMENTS {
            sql.app_execute(MISS, (format!("absent-{i}"), "t"))?;
        }
        Ok(())
    })
    .unwrap();
    let sqlive_miss = started.elapsed() / STATEMENTS as u32;
    db.close();
    for (metric, time) in [
        ("triggered_insert/sqlive", sqlive),
        ("triggered_insert/plain_preparing_each", fresh),
        ("triggered_insert/plain_cached", cached),
        ("triggered_miss/sqlive", sqlive_miss),
        ("triggered_miss/plain_preparing_each", fresh_miss),
        ("triggered_miss/plain_cached", cached_miss),
    ] {
        crate::report_time(metric, time);
    }
    eprintln!(
        "insert per statement: sqlive {sqlive:?}; plain SQLite preparing each {fresh:?}, from its cache {cached:?}"
    );
    eprintln!(
        "statement changing nothing: sqlive {sqlive_miss:?}; plain SQLite preparing each {fresh_miss:?}, from its cache {cached_miss:?}"
    );
}

/// A write leaves the app's prepared statements valid: SQLite recompiles
/// an expired statement on its next run, as it does after a flag PRAGMA,
/// which would cancel what the cache keeps.
#[test]
fn a_write_leaves_the_apps_statements_prepared() {
    let db = notes();
    for id in ["1", "2", "3"] {
        db.write_app(|sql| sql.app_execute(INSERT, [id])).unwrap();
    }
    let reprepared = db.host(|writer| {
        writer
            .connection
            .prepare_cached(INSERT)
            .unwrap()
            .get_status(rusqlite::StatementStatus::RePrepare)
    });
    assert_eq!(reprepared, 0);
    db.close();
}

/// `sql` with the scope plan sqlive-compose's compiler ships with it, built
/// against `schema`.
fn compiled(schema: &str, sql: &str) -> crate::CompiledStatement<crate::Writer> {
    let probe = crate::SchemaProbe::open(&[schema.to_owned()], &[], &[]).unwrap();
    crate::tests::compiled(&probe, sql).expect("sqlive analyzes the statement")
}

/// The titles the compiled `statement` reads of note 1 on `reader`.
fn compiled_titles(
    reader: &crate::HostSql<crate::NoPolicy>,
    statement: crate::CompiledStatement<crate::Writer>,
) -> rusqlite::Result<Vec<String>> {
    reader.app_query_compiled(crate::Recorded::Yes, statement, ["1"], |r| r.get(0))
}

const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)";

/// Run `statement` as a live query's statement on each of `db`'s two
/// readers, as `plans_built_on_each_reader` runs `TITLES`, and return the
/// scope plans the runs built.
fn compiled_plans_built(
    db: &TestDatabase,
    counter: &crate::ConnectionWorkCounter,
    statement: crate::CompiledStatement<crate::Writer>,
) -> usize {
    db.with_database(|database| {
        let (first, second) = (database.reader(), database.reader());
        for reader in [&first, &second] {
            let title = live_titles(reader, crate::LiveStatement::compiled(statement, note_one()));
            assert_eq!(title, ["title"]);
        }
    });
    counter.take().scope_plans_built
}

/// A compiled statement's first run builds no scope plan: the compiler built
/// it, and the statement brings it.
#[test]
fn a_compiled_statements_first_run_builds_no_plan() {
    let (db, counter) = note_of_title();
    assert_eq!(compiled_plans_built(&db, &counter, compiled(NOTES, TITLES)), 0);
    db.close();
}

/// A compiled statement's plan narrows a live query as the plan sqlive
/// builds would: a write to another note leaves the query alone.
#[tokio::test]
async fn a_compiled_plan_narrows_its_live_query() {
    let db = notes();
    db.write_app(|sql| sql.app_execute(INSERT, ["1"])).unwrap();
    let statement = compiled(NOTES, TITLES);
    let mut title =
        db.subscribe(vec![compiled_read(statement, note_one())], |_, r| r.get::<_, String>(0));
    assert_eq!(next(&mut title).await, ["title"]);
    db.write("INSERT INTO notes VALUES ('2', 'other')").unwrap();
    assert!(!title.is_marked_for_rerun());
    db.write("UPDATE notes SET title = 'renamed' WHERE id = '1'").unwrap();
    assert_eq!(next(&mut title).await, ["renamed"]);
    db.close();
}

/// A statement compiled against another definition of a table it reads
/// fails, naming the table, in a one-shot read and in a live query alike:
/// its plan could narrow by what the open database's table doesn't hold. A
/// plan of the same SQL that fits, checked before, doesn't stand in for it.
#[test]
fn a_compiled_statement_of_another_schema_fails() {
    let (db, _counter) = note_of_title();
    let fits = compiled(NOTES, TITLES);
    let title = db.read_app(move |reader| Ok::<_, QueryError>(compiled_titles(reader.0, fits)?));
    assert_eq!(title.unwrap(), ["title"]);
    let statement = compiled(
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT)",
        TITLES,
    );
    let run = move |reader: TestRead<'_>| -> Result<Vec<String>, QueryError> {
        Ok(compiled_titles(reader.0, statement)?)
    };
    let live = db
        .subscribe(vec![compiled_read(statement, note_one())], |_, r| r.get(0))
        .next_blocking()
        .expect("the query's first result");
    for result in [db.read_app(run), live] {
        match result {
            Err(QueryError(crate::Error::CompiledSchemaDiffers(crate::SchemaDiffers {
                differ,
            }))) => {
                assert_eq!(differ, ["table notes"]);
            }
            other => panic!("expected the compiled schema to differ, got {other:?}"),
        }
    }
    db.close();
}
