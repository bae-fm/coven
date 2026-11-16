//! Each way a write reaches live queries records the values their scopes
//! test, so a write outside a scope wakes nothing and one inside it does.
//! And over generated queries, schemas and writes, a live query is marked to
//! run again whenever a commit changes what it returns, and its rerun returns
//! what a one-shot read of the same SQL does (§4); a failure names the seed.
//! Writes change several tables at once, joins chain through joined tables,
//! and some lookups an index serves, so each write's lookups judge joins in
//! its own transaction; a marked query sometimes waits through more writes.
//! `SQLIVE_SCOPE_SEEDS` sets how many seeds run (default 40).

use crate::random_scopes::{a_value, b_value, key, query, write, Generated, Rng};
use crate::random_scopes::{CAPTURED, SCHEMA, TABLES};
use crate::tests::{next, QueryError, TestDatabase, TestOptions, TestRead, HANG_GUARD};
use crate::{LiveQuery, SqlValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const TRACKS: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release TEXT, title TEXT)";

fn titles(db: &TestDatabase) -> LiveQuery<Vec<String>, QueryError> {
    db.subscribe(|sql| {
        Ok(sql.query("SELECT title FROM tracks WHERE release='a' ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    })
}

/// Through `write`, as the app's write, the host's own transaction or a
/// streamed transaction: other releases' tracks wake nothing, this
/// release's do, and so does a track moving in or out.
async fn narrows(db: &TestDatabase, write: impl Fn(&str)) {
    write("INSERT INTO tracks VALUES ('1','a','one'),('2','b','two')");
    let mut query = titles(db);
    assert_eq!(next(&mut query).await, ["one"]);
    write("UPDATE tracks SET title='deux' WHERE id='2'");
    write("INSERT INTO tracks VALUES ('3','c','three')");
    write("DELETE FROM tracks WHERE id='3'");
    assert!(!query.is_marked_for_rerun());
    write("UPDATE tracks SET title='uno' WHERE id='1'");
    assert_eq!(next(&mut query).await, ["uno"]);
    write("UPDATE tracks SET release='a' WHERE id='2'");
    assert_eq!(next(&mut query).await, ["uno", "deux"]);
    write("UPDATE tracks SET release='b' WHERE id='1'");
    assert_eq!(next(&mut query).await, ["deux"]);
    write("UPDATE tracks SET id='9' WHERE id='1'");
    assert!(!query.is_marked_for_rerun());
}

#[tokio::test]
async fn a_captured_tables_writes_record_scoped_values() {
    let db = TestDatabase::open_with(
        TRACKS,
        TestOptions { captured: ["tracks".to_owned()].into(), ..TestOptions::default() },
    );
    narrows(&db, |sql| db.write(sql).unwrap()).await;
    db.close();
}

#[tokio::test]
async fn an_uncaptured_tables_writes_record_scoped_values() {
    let db = TestDatabase::open(TRACKS);
    narrows(&db, |sql| db.write(sql).unwrap()).await;
    db.close();
}

/// A session leaves out the columns an UPDATE didn't change; sqlive reads
/// their values from the row before the transaction commits.
#[tokio::test]
async fn host_transactions_read_unchanged_scoped_values() {
    let db = TestDatabase::open(TRACKS);
    narrows(&db, |sql| db.commit(sql)).await;
    db.close();
}

/// A streamed transaction, as a host applies another device's write in,
/// records the values a waiting scope tests, as an app's write does.
#[tokio::test]
async fn streamed_transactions_record_scoped_values() {
    let db = TestDatabase::open(TRACKS);
    narrows(&db, |sql| {
        db.host(|writer| writer.stream_transaction(|writer| writer.batch(sql))).unwrap()
    })
    .await;
    db.close();
}

/// A write made while a query is reading records no values for the scope
/// that run will have: the changes it couldn't judge still wake it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_during_the_read_that_reveals_the_scope_wakes_it() {
    let db = TestDatabase::open(TRACKS);
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let mut query = db.subscribe(move |sql| -> Result<_, QueryError> {
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released.lock().unwrap().recv_timeout(HANG_GUARD).unwrap();
        }
        Ok(sql.query("SELECT title FROM tracks WHERE release='a' ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    let task = tokio::spawn(async move {
        let value = next(&mut query).await;
        (query, value)
    });
    entry.await.unwrap();
    db.write("INSERT INTO tracks VALUES ('1','a','one')").unwrap();
    release.send(()).unwrap();
    let (mut query, first) = task.await.unwrap();
    assert!(first.is_empty());
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await, ["one"]);
    db.close();
}

type Rows = Vec<Vec<SqlValue>>;

fn read(sql: &TestRead<'_>, text: &str, parameters: &[SqlValue]) -> rusqlite::Result<Rows> {
    sql.query(text, rusqlite::params_from_iter(parameters), |row| {
        (0..row.as_ref().column_count()).map(|index| row.get::<_, SqlValue>(index)).collect()
    })
}

async fn one_shot(db: &TestDatabase, query: &Generated) -> Rows {
    let sql = query.sql.clone();
    let parameters = query.parameters.clone();
    db.read_app(move |context| Ok::<_, QueryError>(read(&context, &sql, &parameters)?)).unwrap()
}

async fn next_rows(query: &mut LiveQuery<(usize, Rows), QueryError>) -> (usize, Rows) {
    tokio::time::timeout(HANG_GUARD, query.next_inline())
        .await
        .expect("query must answer")
        .expect("the live query answers")
        .unwrap()
}

/// What one seed found: commits that left a narrowing query's result alone
/// without marking it.
async fn run_seed(seed: u64) -> usize {
    let mut rng = Rng::seeded(seed);
    let db = TestDatabase::open_with(
        SCHEMA,
        TestOptions { captured: [CAPTURED.to_owned()].into(), ..TestOptions::default() },
    );
    // Rows in every table from the start, so joins, chained ones too, find
    // partners for the writes to change.
    let mut rows = Vec::new();
    for table in TABLES {
        for id in 0..6 {
            rows.push(format!(
                "INSERT INTO {table}(id, a, b, c) VALUES ({}, {}, {}, {})",
                key(table, id),
                a_value(&mut rng),
                b_value(&mut rng),
                rng.below(10)
            ));
        }
    }
    db.write(&rows.join("; ")).unwrap();
    let queries: Vec<Generated> = (0..16).map(|_| query(&mut rng)).collect();
    let mut live = Vec::new();
    for query in &queries {
        let runs = Arc::new(AtomicUsize::new(0));
        let sql = query.sql.clone();
        let parameters = query.parameters.clone();
        let mut subscription = db.subscribe(move |context| {
            let run = runs.fetch_add(1, Ordering::SeqCst);
            Ok((run, read(&context, &sql, &parameters)?))
        });
        let (_, rows) = next_rows(&mut subscription).await;
        live.push((subscription, rows));
    }
    let mut spared = 0;
    for step in 0..24 {
        let sql = write(&mut rng).sql;
        // The host's own transaction instead of the app's write.
        if rng.chance(40) {
            db.commit(&sql);
        } else {
            db.write(&sql).unwrap();
        }
        for (query, (subscription, last)) in queries.iter().zip(live.iter_mut()) {
            let fresh = one_shot(&db, query).await;
            let marked = subscription.is_marked_for_rerun();
            let case = || {
                format!(
                    "seed {seed}, step {step}: {} {:?} after `{sql}`",
                    query.sql, query.parameters
                )
            };
            if fresh != *last {
                assert!(marked, "a changed result wasn't marked: {}", case());
            }
            // A marked query sometimes stays marked through the next write,
            // which its judging then skips.
            if marked && rng.chance(75) {
                let (_, rows) = next_rows(subscription).await;
                assert_eq!(rows, fresh, "a rerun differs from a read: {}", case());
                *last = rows;
            } else if !marked && query.narrows {
                spared += 1;
            }
        }
    }
    db.close();
    spared
}

#[tokio::test]
async fn live_queries_rerun_whenever_their_results_change() {
    let seeds: u64 = std::env::var("SQLIVE_SCOPE_SEEDS")
        .ok()
        .map_or(40, |seeds| seeds.parse().expect("SQLIVE_SCOPE_SEEDS is a count"));
    let mut spared = 0;
    for seed in 0..seeds {
        spared += run_seed(seed).await;
    }
    eprintln!("{seeds} seeds, {spared} reruns spared");
    // The test would pass with every table whole; narrowing must also spare
    // the queries it can.
    assert!(spared > 0, "no narrowing query was spared a rerun");
}
