use super::*;
use crate::tests::TestStore;
use crate::{RowIdentity, SyncedTable};

const SCHEMA: &str = "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY);
CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES parent(id) ON DELETE CASCADE);
CREATE TABLE log(id INTEGER PRIMARY KEY, note TEXT);
CREATE TRIGGER logged AFTER DELETE ON child BEGIN INSERT INTO log(note) VALUES(old.id); END;";

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("parent", RowIdentity::SharedKey),
        SyncedTable::new("child", RowIdentity::SharedKey),
    ]
}

fn summary(events: &[Event]) -> Vec<(&str, Action)> {
    events
        .iter()
        .map(|event| (event.table.as_str(), event.action))
        .collect()
}

/// Every operation on a synced table is recorded in order: a statement's own
/// rows and the foreign-key actions they cause. The device-local `log` row
/// the trigger inserts isn't.
#[tokio::test]
async fn operations_keep_their_order() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    crate::write::tests::sql(
        &db,
        "INSERT INTO parent VALUES('p'); INSERT INTO child VALUES('c','p')",
    )
    .await
    .unwrap();
    let events = db.inspect_writer(|c| {
        c.batch("BEGIN").unwrap();
        let capture = EventCapture::begin(c).unwrap();
        c.batch("UPDATE child SET id='d' WHERE id='c'; DELETE FROM parent WHERE id='p'")
            .unwrap();
        let events = capture.events();
        drop(capture);
        c.batch("ROLLBACK").unwrap();
        events
    });
    assert_eq!(
        summary(&events),
        [
            ("child", Action::SQLITE_UPDATE),
            ("parent", Action::SQLITE_DELETE),
            ("child", Action::SQLITE_DELETE),
        ]
    );
    let text = |s: &str| {
        Some(SqlValue::from(rusqlite::types::ValueRef::Text(
            s.as_bytes(),
        )))
    };
    assert_eq!(events[0].old, [text("c"), text("p")]);
    assert_eq!(events[0].new, [text("d"), text("p")]);
    assert!(events[1].new.is_empty());
    assert_eq!(events[2].old, [text("d"), text("p")]);
    // Removing the hook leaves later statements unobserved.
    db.inspect_writer(|c| {
        c.batch("BEGIN").unwrap();
        let capture = EventCapture::begin(c).unwrap();
        drop(capture);
        c.batch("DELETE FROM parent; ROLLBACK").unwrap();
    });
    db.close().await.unwrap();
}

/// A device-local table never syncs, so its rows aren't copied into the
/// write's operations: only a synced table's are.
#[tokio::test]
async fn a_local_tables_rows_are_not_copied() {
    let store = TestStore::new();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    let events = db.inspect_writer(|c| {
        c.batch("BEGIN").unwrap();
        let capture = EventCapture::begin(c).unwrap();
        c.batch("INSERT INTO parent VALUES('p'); INSERT INTO log(note) VALUES('a'),('b')")
            .unwrap();
        let events = capture.events();
        drop(capture);
        c.batch("ROLLBACK").unwrap();
        events
    });
    assert_eq!(summary(&events), [("parent", Action::SQLITE_INSERT)]);
    db.close().await.unwrap();
}

/// What a write to a device-local table costs through coven, against the
/// same SQL in plain SQLite with coven's durability settings, with a live
/// query that reads the whole table and with one that narrows by a column,
/// whose values each write then records. Run explicitly.
#[tokio::test]
#[ignore = "one timing measurement, run explicitly"]
async fn local_table_writes_against_plain_sqlite() {
    const QUEUE: &str = "CREATE TABLE queue(lane INTEGER NOT NULL, position INTEGER NOT NULL, track TEXT NOT NULL, PRIMARY KEY(lane, position))";
    let fill = |rows: u32| {
        format!(
            "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<{}) INSERT INTO queue SELECT 0, i, printf('track-%08d', i) FROM n",
            rows - 1
        )
    };
    for rows in [5_000, 50_000, 1_000_000] {
        let directory = tempfile::tempdir().unwrap();
        let plain = rusqlite::Connection::open(directory.path().join("plain.db")).unwrap();
        plain
            .execute_batch(&format!(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; {QUEUE}"
            ))
            .unwrap();
        let started = std::time::Instant::now();
        plain
            .execute_batch(&format!("BEGIN; {}; COMMIT", fill(rows)))
            .unwrap();
        let plain_insert = started.elapsed();
        let started = std::time::Instant::now();
        plain
            .execute_batch("BEGIN; DELETE FROM queue; COMMIT")
            .unwrap();
        let plain_delete = started.elapsed();

        let store = TestStore::new();
        let db = store
            .builder(tables(), vec![crate::Migration::addition(1, "schema", "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES parent(id) ON DELETE CASCADE)"), crate::Migration::addition(2, "queue", QUEUE)])
            .open()
            .await
            .unwrap();
        let mut live = db.subscribe(|sql| {
            Ok(sql.query_row("SELECT count(*) FROM queue", [], |r| r.get::<_, i64>(0))?)
        });
        assert_eq!(
            live.next().await.expect("the live query answers").unwrap(),
            0
        );
        let insert: &'static str = Box::leak(fill(rows).into_boxed_str());
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, insert).await.unwrap();
        let coven_insert = started.elapsed();
        assert_eq!(
            live.next().await.expect("the live query answers").unwrap(),
            i64::from(rows)
        );
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, "DELETE FROM queue")
            .await
            .unwrap();
        let coven_delete = started.elapsed();
        assert_eq!(
            live.next().await.expect("the live query answers").unwrap(),
            0
        );
        drop(live);
        let mut narrow = db.subscribe(|sql| {
            Ok(
                sql.query("SELECT position FROM queue WHERE track = 'none'", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            )
        });
        assert!(narrow
            .next()
            .await
            .expect("the live query answers")
            .unwrap()
            .is_empty());
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, insert).await.unwrap();
        let narrowed_insert = started.elapsed();
        let started = std::time::Instant::now();
        crate::write::tests::sql(&db, "DELETE FROM queue")
            .await
            .unwrap();
        let narrowed_delete = started.elapsed();
        // Past the bound, the write keeps only that the table changed.
        assert_eq!(
            narrow.is_marked_for_rerun(),
            rows as usize > crate::sqlite::local_changes::ROWS_PER_TABLE
        );
        db.close().await.unwrap();
        eprintln!(
            "{rows} rows: insert {coven_insert:?} through coven, {narrowed_insert:?} narrowed, {plain_insert:?} plain; delete {coven_delete:?} through coven, {narrowed_delete:?} narrowed, {plain_delete:?} plain"
        );
    }
}
