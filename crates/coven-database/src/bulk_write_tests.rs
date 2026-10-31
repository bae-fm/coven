use crate::tests::TestStore;
use crate::{Database, DbError, RowIdentity, SyncedTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A synced table of notes beside device-local ones: a queue, a list of
/// other rows, a log a trigger on the queue keeps, an inbox whose trigger
/// would write a note, and a lane whose rows a foreign key ties to its
/// queue's.
const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
CREATE TABLE queue(position INTEGER PRIMARY KEY, track TEXT NOT NULL);
CREATE TABLE other(id INTEGER PRIMARY KEY);
CREATE TABLE logged(id INTEGER PRIMARY KEY, track TEXT NOT NULL);
CREATE TABLE logged_queue(position INTEGER PRIMARY KEY, track TEXT NOT NULL);
CREATE TRIGGER logged_queue_removed AFTER DELETE ON logged_queue
BEGIN INSERT INTO logged(track) VALUES(old.track); END;
CREATE TABLE inbox(id TEXT NOT NULL PRIMARY KEY);
CREATE TRIGGER inbox_note AFTER INSERT ON inbox
BEGIN INSERT INTO notes VALUES(new.id, 'from the inbox'); END;
CREATE TABLE lanes(id INTEGER PRIMARY KEY);
CREATE TABLE lane_tracks(lane INTEGER NOT NULL REFERENCES lanes(id) ON DELETE CASCADE, track TEXT NOT NULL);";

/// The rows the queue holds before it's cleared.
const ROWS: i64 = 100_000;

fn tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

fn fill(table: &str, rows: i64) -> String {
    format!(
        "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<{}) \
         INSERT INTO {table}(position, track) SELECT i, printf('track-%08d', i) FROM n",
        rows - 1
    )
}

async fn bulk(db: &Database, sql: String) -> Result<(), DbError> {
    db.write_bulk(move |context| {
        context.execute_batch(&sql)?;
        Ok(())
    })
    .await
}

/// A live query counting `table`'s rows, and how many times it has run.
fn counting(db: &Database, table: &'static str) -> (crate::LiveQuery<i64>, Arc<AtomicUsize>) {
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);
    let live = db.subscribe(move |sql| {
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
    });
    (live, runs)
}

async fn answer(live: &mut crate::LiveQuery<i64>) -> i64 {
    live.next().await.expect("the live query answers").unwrap()
}

/// With no pre-update hook watching, SQLite empties a table without a
/// trigger or foreign key in one step, where a row-by-row delete takes
/// several steps for each row.
#[tokio::test]
async fn clearing_a_local_table_takes_one_step() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    bulk(&db, fill("queue", ROWS)).await.unwrap();
    assert_eq!(crate::write::tests::count(&db, "queue"), ROWS);
    bulk(&db, "DELETE FROM queue".into()).await.unwrap();
    assert_eq!(crate::write::tests::count(&db, "queue"), 0);
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let clear = statements
        .iter()
        .find(|statement| statement.sql == "DELETE FROM queue")
        .expect("the clear ran");
    assert!(i64::from(clear.vm_steps) < ROWS, "{} steps clearing {ROWS} rows", clear.vm_steps);
    db.close().await.unwrap();
}

/// After a bulk write commits, a live query that read a table it changed
/// runs once more and sees the write; one that read another table doesn't
/// run.
#[tokio::test]
async fn live_queries_rerun_for_the_tables_a_bulk_write_changed() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    bulk(&db, fill("queue", ROWS)).await.unwrap();
    let (mut queue, queue_runs) = counting(&db, "queue");
    let (mut other, other_runs) = counting(&db, "other");
    assert_eq!(answer(&mut queue).await, ROWS);
    assert_eq!(answer(&mut other).await, 0);
    bulk(&db, "DELETE FROM queue".into()).await.unwrap();
    // The writer publishes its commit before the write returns.
    assert!(queue.is_marked_for_rerun());
    assert!(!other.is_marked_for_rerun());
    assert_eq!(answer(&mut queue).await, 0);
    assert_eq!(queue_runs.load(Ordering::SeqCst), 2);
    assert!(!queue.is_marked_for_rerun());
    assert_eq!(other_runs.load(Ordering::SeqCst), 1);
    db.close().await.unwrap();
}

/// A bulk write may read a synced table, but writing one fails the whole
/// write: nothing it did commits, and no write record is queued.
#[tokio::test]
async fn a_bulk_write_to_a_synced_table_is_refused() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES('n1', 'Groceries')").await.unwrap();
    let records = crate::write::tests::records(&db).len();
    bulk(&db, "INSERT INTO queue(position, track) SELECT 0, title FROM notes".into())
        .await
        .unwrap();
    let error =
        bulk(&db, "INSERT INTO other VALUES(1); UPDATE notes SET title = 'Shopping'".into())
            .await
            .unwrap_err();
    assert!(
        matches!(&error, DbError::SyncedTableInBulkWrite { table } if table == "notes"),
        "{error:?}"
    );
    assert_eq!(crate::write::tests::count(&db, "other"), 0);
    assert_eq!(crate::write::tests::count(&db, "queue"), 1);
    assert_eq!(crate::write::tests::records(&db).len(), records);
    db.close().await.unwrap();
}

/// A trigger that would write a synced table is refused as SQLite prepares
/// the statement that fires it, and the write rolls back.
#[tokio::test]
async fn a_trigger_writing_a_synced_table_is_refused() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    let error = bulk(&db, "INSERT INTO other VALUES(1); INSERT INTO inbox VALUES('n1')".into())
        .await
        .unwrap_err();
    assert!(
        matches!(&error, DbError::TriggerTarget { trigger, table }
            if trigger == "inbox_note" && table == "notes"),
        "{error:?}"
    );
    assert_eq!(crate::write::tests::count(&db, "other"), 0);
    assert_eq!(crate::write::tests::count(&db, "notes"), 0);
    db.close().await.unwrap();
}

/// A trigger writing another device-local table runs, and the live queries
/// that read that table run again.
#[tokio::test]
async fn a_trigger_writing_a_local_table_reruns_its_live_queries() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    bulk(&db, fill("logged_queue", 3)).await.unwrap();
    let (mut logged, runs) = counting(&db, "logged");
    assert_eq!(answer(&mut logged).await, 0);
    bulk(&db, "DELETE FROM logged_queue".into()).await.unwrap();
    assert!(logged.is_marked_for_rerun());
    assert_eq!(answer(&mut logged).await, 3);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    db.close().await.unwrap();
}

/// A foreign-key action changes its table as a trigger would: the live
/// queries that read the child run again.
#[tokio::test]
async fn a_foreign_key_action_reruns_its_tables_live_queries() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    bulk(
        &db,
        "INSERT INTO lanes VALUES(1); INSERT INTO lane_tracks VALUES(1, 'a'), (1, 'b')".into(),
    )
    .await
    .unwrap();
    let (mut tracks, runs) = counting(&db, "lane_tracks");
    assert_eq!(answer(&mut tracks).await, 2);
    bulk(&db, "DELETE FROM lanes".into()).await.unwrap();
    assert!(tracks.is_marked_for_rerun());
    assert_eq!(answer(&mut tracks).await, 0);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    db.close().await.unwrap();
}
