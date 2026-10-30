//! Each way a write reaches live queries records the values their scopes
//! test, so a write outside a scope wakes nothing and one inside it does.
//! And over generated queries, schemas and writes, a live query is marked to
//! run again whenever a commit changes what it returns, and its rerun returns
//! what a one-shot read of the same SQL does (§4); a failure names the seed.
//! `COVEN_SCOPE_SEEDS` sets how many seeds run (default 40).

use crate::sql_value::SqlValue;
use crate::tests::TestStore;
use crate::{CovenResult, Database, LiveQuery, RowIdentity, SyncedTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .expect("the live query answers")
        .unwrap()
}

/// One write, run when awaited.
type Pending = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

const TRACKS: &str = "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release TEXT, title TEXT)";

fn titles(db: &Database) -> LiveQuery<Vec<String>> {
    db.subscribe(|sql| {
        Ok(sql.query(
            "SELECT title FROM tracks WHERE release='a' ORDER BY id",
            [],
            |r| r.get::<_, String>(0),
        )?)
    })
}

/// Through `write`, as the app's SQL in a local write, and through
/// `commit`, as coven's own transaction: other releases' tracks wake
/// nothing, this release's do, and so does a track moving in or out.
async fn narrows(db: &Database, write: impl Fn(&'static str) -> Pending) {
    write("INSERT INTO tracks VALUES ('1','a','one'),('2','b','two')").await;
    let mut query = titles(db);
    assert_eq!(next(&mut query).await, ["one"]);
    write("UPDATE tracks SET title='deux' WHERE id='2'").await;
    write("INSERT INTO tracks VALUES ('3','c','three')").await;
    write("DELETE FROM tracks WHERE id='3'").await;
    assert!(!query.is_marked_for_rerun());
    write("UPDATE tracks SET title='uno' WHERE id='1'").await;
    assert_eq!(next(&mut query).await, ["uno"]);
    write("UPDATE tracks SET release='a' WHERE id='2'").await;
    assert_eq!(next(&mut query).await, ["uno", "deux"]);
    write("UPDATE tracks SET release='b' WHERE id='1'").await;
    assert_eq!(next(&mut query).await, ["deux"]);
    write("UPDATE tracks SET id='9' WHERE id='1'").await;
    assert!(!query.is_marked_for_rerun());
}

fn local_write(db: &Database) -> impl Fn(&'static str) -> Pending {
    let db = db.clone();
    move |sql| {
        let db = db.clone();
        Box::pin(async move { crate::write::tests::sql(&db, sql).await.unwrap() })
    }
}

#[tokio::test]
async fn a_synced_tables_local_writes_record_scoped_values() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("tracks", RowIdentity::SharedKey)],
            TRACKS,
        )
        .await
        .unwrap();
    narrows(&db, local_write(&db)).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_device_local_tables_local_writes_record_scoped_values() {
    let store = TestStore::new();
    let db = store.schema(vec![], TRACKS).await.unwrap();
    narrows(&db, local_write(&db)).await;
    db.close().await.unwrap();
}

/// A session leaves out the columns an UPDATE didn't change; coven reads
/// their values from the row before the transaction commits.
#[tokio::test]
async fn coven_transactions_read_unchanged_scoped_values() {
    let store = TestStore::new();
    let db = store.schema(vec![], TRACKS).await.unwrap();
    let commit = {
        let db = db.clone();
        move |sql: &'static str| -> Pending {
            db.commit_writer(|writer| writer.batch(sql).unwrap());
            Box::pin(async {})
        }
    };
    narrows(&db, commit).await;
    db.close().await.unwrap();
}

/// A write made while a query is reading records no values for the scope
/// that run will have: the changes it couldn't judge still wake it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_during_the_read_that_reveals_the_scope_wakes_it() {
    let store = TestStore::new();
    let db = store.schema(vec![], TRACKS).await.unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let mut query = db.subscribe(move |sql| -> CovenResult<_> {
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        Ok(sql.query(
            "SELECT title FROM tracks WHERE release='a' ORDER BY id",
            [],
            |r| r.get::<_, String>(0),
        )?)
    });
    let task = tokio::spawn(async move {
        let value = next(&mut query).await;
        (query, value)
    });
    entry.await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO tracks VALUES ('1','a','one')")
        .await
        .unwrap();
    release.send(()).unwrap();
    let (mut query, first) = task.await.unwrap();
    assert!(first.is_empty());
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await, ["one"]);
    db.close().await.unwrap();
}

/// A small deterministic generator, so a seed reproduces a case.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

/// A device-local table keyed by integer, and a synced one keyed by text,
/// with columns whose small domains make values collide.
const SCHEMA: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);
CREATE TABLE s(id TEXT NOT NULL PRIMARY KEY, a INTEGER, b TEXT, c INTEGER);";

const TABLES: [&str; 2] = ["t", "s"];

fn key(table: &str, id: usize) -> String {
    if table == "t" {
        id.to_string()
    } else {
        format!("'k{id}'")
    }
}

fn a_value(rng: &mut Rng) -> String {
    (*rng.pick(&["NULL", "0", "1", "2", "3", "4"])).to_owned()
}

fn b_value(rng: &mut Rng) -> String {
    (*rng.pick(&["NULL", "'x'", "'y'", "'z'", "'1'"])).to_owned()
}

/// One generated live query.
struct Generated {
    sql: String,
    parameters: Vec<SqlValue>,
    /// Whether its WHERE holds a condition coven narrows by.
    narrows: bool,
}

/// A condition on one row of `table`, and whether coven can narrow by it.
fn condition(
    rng: &mut Rng,
    table: &str,
    parameters: &mut Vec<SqlValue>,
    depth: usize,
) -> (String, bool) {
    if depth < 2 && rng.chance(30) {
        let (left, left_narrows) = condition(rng, table, parameters, depth + 1);
        let (right, right_narrows) = condition(rng, table, parameters, depth + 1);
        return if rng.chance(60) {
            (
                format!("({left} AND {right})"),
                left_narrows || right_narrows,
            )
        } else {
            (
                format!("({left} OR {right})"),
                left_narrows && right_narrows,
            )
        };
    }
    let id = rng.below(10);
    match rng.below(11) {
        0 => (format!("id = {}", key(table, id)), true),
        1 => (format!("a = {}", rng.below(5)), true),
        2 => (format!("b = {}", b_value(rng)), true),
        3 => (format!("c >= {}", rng.below(10)), true),
        4 => (format!("a IN ({}, {})", rng.below(5), rng.below(5)), true),
        5 => {
            // A parameter, sometimes of a type the column would convert.
            parameters.push(if rng.chance(70) {
                SqlValue::Integer(rng.below(5) as i64)
            } else {
                SqlValue::Text(rng.below(5).to_string().into_bytes())
            });
            let narrows = matches!(parameters.last(), Some(SqlValue::Integer(_)));
            (format!("a = ?{}", parameters.len()), narrows)
        }
        6 => (format!("b <> {}", b_value(rng)), false),
        7 => ("a IS NULL".to_owned(), false),
        8 => (format!("abs(c) = {}", rng.below(10)), false),
        9 => (format!("b LIKE {}", b_value(rng)), false),
        _ => (format!("c < {}", rng.below(10)), true),
    }
}

fn query(rng: &mut Rng) -> Generated {
    let table = *rng.pick(&TABLES);
    let mut parameters = Vec::new();
    let (filter, narrows) = if rng.chance(85) {
        let (condition, narrows) = condition(rng, table, &mut parameters, 0);
        (format!(" WHERE {condition}"), narrows)
    } else {
        (String::new(), false)
    };
    let sql = match rng.below(6) {
        0 => format!("SELECT count(*) FROM {table}{filter}"),
        1 => format!("SELECT sum(c), group_concat(b) FROM {table}{filter}"),
        2 => format!("SELECT a, count(*) FROM {table}{filter} GROUP BY a ORDER BY a"),
        3 => format!(
            "SELECT id, b FROM {table}{filter} ORDER BY c, id LIMIT {}",
            1 + rng.below(3)
        ),
        _ => format!("SELECT id, a, b, c FROM {table}{filter} ORDER BY id"),
    };
    Generated {
        sql,
        parameters,
        narrows,
    }
}

/// One write's SQL, and whether it goes through coven's own transaction
/// instead of the app's local write.
fn write(rng: &mut Rng) -> (String, bool) {
    let table = *rng.pick(&TABLES);
    let mut statements = Vec::new();
    for _ in 0..1 + rng.below(3) {
        let id = key(table, rng.below(10));
        statements.push(match rng.below(7) {
            0 | 1 => format!(
                "INSERT OR IGNORE INTO {table} VALUES ({id}, {}, {}, {})",
                a_value(rng),
                b_value(rng),
                rng.below(10)
            ),
            2 => format!("UPDATE {table} SET a = {} WHERE id = {id}", a_value(rng)),
            3 => format!(
                "UPDATE {table} SET b = {}, c = c + 1 WHERE id = {id}",
                b_value(rng)
            ),
            4 => format!(
                "UPDATE {table} SET c = {} WHERE a = {}",
                rng.below(10),
                rng.below(5)
            ),
            5 if table == "t" => format!(
                "UPDATE OR IGNORE t SET id = {} WHERE id = {id}",
                rng.below(10)
            ),
            _ => format!("DELETE FROM {table} WHERE id = {id}"),
        });
    }
    // Coven's own transactions write device-local tables only here: synced
    // rows change through writes, so their merge state stays whole.
    (statements.join("; "), table == "t" && rng.chance(40))
}

type Rows = Vec<Vec<SqlValue>>;

fn read(
    sql: &crate::SqlReadContext<'_>,
    text: &str,
    parameters: &[SqlValue],
) -> rusqlite::Result<Rows> {
    sql.query(text, rusqlite::params_from_iter(parameters), |row| {
        (0..row.as_ref().column_count())
            .map(|index| row.get::<_, SqlValue>(index))
            .collect()
    })
}

async fn one_shot(db: &Database, query: &Generated) -> Rows {
    let sql = query.sql.clone();
    let parameters = query.parameters.clone();
    db.read(move |context| Ok(read(&context, &sql, &parameters)?))
        .await
        .unwrap()
}

async fn next_rows(query: &mut LiveQuery<(usize, Rows)>) -> (usize, Rows) {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .expect("the live query answers")
        .unwrap()
}

/// What one seed found: commits that left a narrowing query's result alone
/// without marking it.
async fn run_seed(seed: u64) -> usize {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let store = TestStore::new();
    let db = store
        .schema(vec![SyncedTable::new("s", RowIdentity::SharedKey)], SCHEMA)
        .await
        .unwrap();
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
        let (sql, coven) = write(&mut rng);
        if coven {
            let statement = sql.clone();
            db.commit_writer(move |writer| writer.batch(&statement).unwrap());
        } else {
            let leaked: &'static str = Box::leak(sql.clone().into_boxed_str());
            crate::write::tests::sql(&db, leaked).await.unwrap();
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
            if marked {
                let (_, rows) = next_rows(subscription).await;
                assert_eq!(rows, fresh, "a rerun differs from a read: {}", case());
                *last = rows;
            } else if query.narrows {
                spared += 1;
            }
        }
    }
    db.close().await.unwrap();
    spared
}

#[tokio::test]
async fn live_queries_rerun_whenever_their_results_change() {
    let seeds: u64 = std::env::var("COVEN_SCOPE_SEEDS").ok().map_or(40, |seeds| {
        seeds.parse().expect("COVEN_SCOPE_SEEDS is a count")
    });
    let mut spared = 0;
    for seed in 0..seeds {
        spared += run_seed(seed).await;
    }
    // The test would pass with every table whole; narrowing must also spare
    // the queries it can.
    assert!(spared > 0, "no narrowing query was spared a rerun");
}
