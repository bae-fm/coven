use super::ScopePlans;
use crate::tests::{next, QueryError, TestDatabase, TestError, TestOptions, TestRead};
use crate::triggered_inserts::SCHEMA as TRIGGERED;
use crate::ScopePlan;
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
    let mut titles = db.subscribe(|sql| {
        Ok(sql.query("SELECT title FROM notes ORDER BY id", [], |r| r.get::<_, String>(0))?)
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
    let mut live = db.subscribe(titles);
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

/// A schema change drops what the cache kept: the statement is prepared and
/// judged again against the new schema.
#[test]
fn a_schema_change_prepares_again() {
    let db = notes();
    let write = |id: &str| db.write_app(|sql| sql.app_execute(INSERT, [id])).unwrap();
    write("1");
    let before = fresh(&db);
    db.host(|writer| writer.batch("CREATE TABLE elsewhere(x)").unwrap());
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

/// Run `TITLES` as a live query's statement on each of `db`'s two readers,
/// holding both at once so that each run is on a reader of its own, and
/// return the scope plans the runs built.
fn plans_built_on_each_reader(db: &TestDatabase, counter: &crate::ConnectionWorkCounter) -> usize {
    db.with_database(|database| {
        let (first, second) = (database.reader(), database.reader());
        for reader in [&first, &second] {
            let title = reader.host(|reader| {
                reader.live_read(
                    &mut |_reads, _lookups| Ok(()),
                    |reader| -> Result<String, TestError> {
                        Ok(reader
                            .app_query_row(crate::Recorded::Yes, TITLES, ["1"], |r| r.get(0))?)
                    },
                )
            });
            assert_eq!(title.unwrap(), "title");
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

/// A schema change drops the plans kept against the old schema: the next
/// run builds the statement's plan again, for every reader.
#[test]
fn a_schema_change_builds_a_scope_plan_again() {
    let (db, counter) = note_of_title();
    assert_eq!(plans_built_on_each_reader(&db, &counter), 1);
    db.host(|writer| writer.batch("CREATE TABLE elsewhere(x)").unwrap());
    assert_eq!(plans_built_on_each_reader(&db, &counter), 1);
    db.close();
}

/// A plan of some statement, for tests of the map that keeps plans.
fn some_plan() -> Arc<ScopePlan> {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let statement = connection.prepare("SELECT 1").unwrap();
    let whole = crate::WholeViews::new([]);
    Arc::new(ScopePlan::build(&connection, "SELECT 1", &statement, &whole).unwrap())
}

/// What a second caller did while the first was building the plan.
#[derive(Debug, PartialEq, Eq)]
enum Second {
    Waited,
    Built,
}

/// Ask `plans` for one plan from two threads: `first` builds it and ends only
/// once the second caller, which would build `plan`, waits for it or builds
/// its own. The second caller's first step, what it got, and how often it
/// built.
fn second_caller_during(
    plans: &ScopePlans,
    first: impl FnOnce() -> rusqlite::Result<Arc<ScopePlan>> + Send,
) -> (Second, Arc<ScopePlan>, usize) {
    let (did, second_did) = std::sync::mpsc::channel();
    let (began, building) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let waited = did.clone();
    plans.on_next_wait(move || waited.send(Second::Waited).unwrap());
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
                did.send(Second::Built).unwrap();
                Ok(plan)
            });
            (got.unwrap(), builds)
        });
        let step = second_did.recv().unwrap();
        release.send(()).unwrap();
        // The first build may fail or panic; what the second caller then gets
        // is what the test asks.
        drop(builder.join());
        let (got, builds) = second.join().unwrap();
        (step, got, builds)
    })
}

/// Two first runs of a statement at the same moment build its plan once: the
/// second waits for the first's build and uses it.
#[test]
fn two_first_runs_at_once_build_a_plan_once() {
    let plans = ScopePlans::new();
    let first = some_plan();
    let built = Arc::clone(&first);
    let (step, got, builds) = second_caller_during(&plans, move || Ok(built));
    assert_eq!((step, builds), (Second::Waited, 0));
    assert!(Arc::ptr_eq(&got, &first));
}

/// A build that fails wakes the caller waiting for it, which builds the plan
/// itself.
#[test]
fn a_failed_build_leaves_the_plan_to_the_waiting_caller() {
    let plans = ScopePlans::new();
    let (step, got, builds) =
        second_caller_during(&plans, || Err(rusqlite::Error::QueryReturnedNoRows));
    assert_eq!((step, builds), (Second::Waited, 1));
    let kept = plans.plan(1, TITLES, || unreachable!("the plan is kept")).unwrap();
    assert!(Arc::ptr_eq(&kept, &got));
}

/// A build that panics wakes the caller waiting for it, too.
#[test]
fn a_panicking_build_leaves_the_plan_to_the_waiting_caller() {
    let plans = ScopePlans::new();
    let (step, _got, builds) = second_caller_during(&plans, || panic!("the build broke"));
    assert_eq!((step, builds), (Second::Waited, 1));
}

/// A run still on an older schema leaves the newer schema's plans kept.
#[test]
fn an_older_schemas_run_leaves_the_newer_plans_kept() {
    let plans = ScopePlans::new();
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
