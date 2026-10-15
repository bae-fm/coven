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

fn summary(events: &[Event]) -> Vec<(&str, Action, i32)> {
    events
        .iter()
        .map(|event| (event.table.as_str(), event.action, event.depth))
        .collect()
}

/// A statement's own rows are at depth zero; a foreign-key action runs one
/// level below the change that fired it, and a trigger it fires one more.
#[tokio::test]
async fn operations_keep_their_order_and_trigger_depth() {
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
            ("child", Action::SQLITE_UPDATE, 0),
            ("parent", Action::SQLITE_DELETE, 0),
            ("child", Action::SQLITE_DELETE, 1),
            ("log", Action::SQLITE_INSERT, 2),
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
