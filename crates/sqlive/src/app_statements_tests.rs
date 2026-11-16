use crate::tests::{next, QueryError, TestDatabase, TestError, TestOptions, TestRead};
use crate::triggered_inserts::SCHEMA as TRIGGERED;

fn notes() -> TestDatabase {
    open("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)", "notes")
}

/// A database of `schema`, whose writes keep their operations on `captured`
/// whole.
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
