use crate::tests::{first, next, no_values, QueryError, TestDatabase, TestOptions};
use crate::LiveQuery;

/// A captured table of notes beside uncaptured ones: `queue`, `other`,
/// `logged_queue` with the `logged` table its delete trigger writes, and
/// `lanes` with the `lane_tracks` rows that a foreign key deletes with their
/// lane.
const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE queue(position INTEGER PRIMARY KEY, track TEXT NOT NULL);
CREATE TABLE other(id INTEGER PRIMARY KEY);
CREATE TABLE logged(id INTEGER PRIMARY KEY, track TEXT NOT NULL);
CREATE TABLE logged_queue(position INTEGER PRIMARY KEY, track TEXT NOT NULL);
CREATE TRIGGER logged_queue_removed AFTER DELETE ON logged_queue
BEGIN INSERT INTO logged(track) VALUES(old.track); END;
CREATE TABLE lanes(id INTEGER PRIMARY KEY);
CREATE TABLE lane_tracks(lane INTEGER NOT NULL REFERENCES lanes(id) ON DELETE CASCADE, track TEXT NOT NULL, PRIMARY KEY(lane, track));";

/// The rows the queue holds before a bulk write clears it.
const ROWS: i64 = 100_000;

fn open() -> TestDatabase {
    TestDatabase::open_with(
        SCHEMA,
        TestOptions { captured: ["notes".to_owned()].into(), ..TestOptions::default() },
    )
}

fn fill(table: &str, rows: i64) -> String {
    format!(
        "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<{}) \
         INSERT INTO {table}(position, track) SELECT i, printf('track-%08d', i) FROM n",
        rows - 1
    )
}

fn count(db: &TestDatabase, table: &str) -> i64 {
    db.value(&format!("SELECT count(*) FROM {table}"))
}

/// A live query counting `table`'s rows.
fn counting(db: &TestDatabase, table: &'static str) -> LiveQuery<i64, QueryError> {
    db.subscribe_rows(format!("SELECT count(*) FROM {table}"), no_values(), |r| r.get(0))
        .process(first)
}

/// With no pre-update hook watching, SQLite empties a table without a
/// trigger or foreign key in one step, where a row-by-row delete takes
/// several steps for each row.
#[test]
fn clearing_a_table_takes_one_step() {
    let db = open();
    db.bulk(&fill("queue", ROWS)).unwrap();
    assert_eq!(count(&db, "queue"), ROWS);
    db.bulk("DELETE FROM queue").unwrap();
    assert_eq!(count(&db, "queue"), 0);
    let statements = db.host(|writer| writer.profiled_statements());
    let clear = statements
        .iter()
        .find(|statement| statement.sql == "DELETE FROM queue")
        .expect("the clear ran");
    assert!(i64::from(clear.vm_steps) < ROWS, "{} steps clearing {ROWS} rows", clear.vm_steps);
    db.close();
}

/// After a bulk write commits, a live query that read a table it changed
/// runs once more and sees the write; one that read another table doesn't
/// run.
#[tokio::test]
async fn live_queries_rerun_for_the_tables_a_bulk_write_changed() {
    let db = open();
    db.bulk(&fill("queue", ROWS)).unwrap();
    let mut queue = counting(&db, "queue");
    let mut other = counting(&db, "other");
    assert_eq!(next(&mut queue).await, ROWS);
    assert_eq!(next(&mut other).await, 0);
    db.bulk("DELETE FROM queue").unwrap();
    // The writer publishes its commit before the write returns.
    assert!(queue.is_marked_for_rerun());
    assert!(!other.is_marked_for_rerun());
    assert_eq!(next(&mut queue).await, 0);
    assert_eq!(queue.runs().runs(), 2);
    assert!(!queue.is_marked_for_rerun());
    assert_eq!(other.runs().runs(), 1);
    db.close();
}

/// A trigger writing another table runs, and the live queries that read
/// that table run again.
#[tokio::test]
async fn a_trigger_writing_a_table_reruns_its_live_queries() {
    let db = open();
    db.bulk(&fill("logged_queue", 3)).unwrap();
    let mut logged = counting(&db, "logged");
    assert_eq!(next(&mut logged).await, 0);
    db.bulk("DELETE FROM logged_queue").unwrap();
    assert!(logged.is_marked_for_rerun());
    assert_eq!(next(&mut logged).await, 3);
    assert_eq!(logged.runs().runs(), 2);
    db.close();
}

/// A foreign-key action changes its table as a trigger would: the live
/// queries that read the child run again.
#[tokio::test]
async fn a_foreign_key_action_reruns_its_tables_live_queries() {
    let db = open();
    db.bulk("INSERT INTO lanes VALUES(1); INSERT INTO lane_tracks VALUES(1, 'a'), (1, 'b')")
        .unwrap();
    let mut tracks = counting(&db, "lane_tracks");
    assert_eq!(next(&mut tracks).await, 2);
    db.bulk("DELETE FROM lanes").unwrap();
    assert!(tracks.is_marked_for_rerun());
    assert_eq!(next(&mut tracks).await, 0);
    assert_eq!(tracks.runs().runs(), 2);
    db.close();
}
