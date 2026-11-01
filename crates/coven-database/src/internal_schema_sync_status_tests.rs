use crate::test_utils::notes_tables;
use crate::tests::{database_error, TestStore};
use crate::{DbError, Migration};

/// The app's notes, and a local table its trigger keeps: each sync status
/// coven writes, in order.
const LOGGED: &str = "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
     CREATE TABLE status_log(id INTEGER PRIMARY KEY, state TEXT NOT NULL);
     CREATE TRIGGER status_logged AFTER UPDATE ON coven_sync_status BEGIN
         INSERT INTO status_log(state) VALUES(NEW.state);
     END;";

fn status_row(sql: crate::SqlReadContext<'_>) -> crate::CovenResult<(String, Vec<u8>)> {
    Ok(sql.query_row("SELECT state,status FROM coven_sync_status", [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?)
}

/// A new store has its status row from its creation on, saying no storage
/// is set up, so a live query on it has a status before any open writes one
/// (E4, E5).
#[tokio::test]
async fn a_new_store_reads_no_storage_set_up() {
    let store = TestStore::new();
    let db =
        store.builder(notes_tables(), crate::test_utils::notes_migrations()).open().await.unwrap();
    let mut row = db.subscribe(status_row);
    assert_eq!(
        row.next().await.unwrap().unwrap(),
        ("disconnected".into(), b"\"Disconnected\"".to_vec())
    );
    db.record_sync_status("stopped", b"\"Stopped\"".to_vec()).await.unwrap();
    assert_eq!(row.next().await.unwrap().unwrap(), ("stopped".into(), b"\"Stopped\"".to_vec()));
    db.close().await.unwrap();
}

/// A local trigger on `coven_sync_status` keeps the app's own log of each
/// status, in the transaction that writes it; a status written again
/// unchanged leaves the row alone and fires nothing (§9.4).
#[tokio::test]
async fn a_local_trigger_follows_each_sync_status_as_it_changes() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), vec![Migration::addition(1, "notes", LOGGED)])
        .open()
        .await
        .unwrap();
    let mut log = db.subscribe(|sql| {
        Ok(sql.query("SELECT state FROM status_log ORDER BY id", [], |r| r.get::<_, String>(0))?)
    });
    assert!(log.next().await.unwrap().unwrap().is_empty());
    for (state, status) in [
        ("stopped", "\"Stopped\""),
        ("syncing", "\"Syncing\""),
        ("syncing", "\"Syncing\""),
        ("offline", "{\"Offline\":{\"failure\":\"Network\",\"error\":\"one\"}}"),
        ("offline", "{\"Offline\":{\"failure\":\"Network\",\"error\":\"two\"}}"),
    ] {
        db.record_sync_status(state, status.as_bytes().to_vec()).await.unwrap();
    }
    let mut seen = log.next().await.unwrap().unwrap();
    while seen.len() < 4 {
        seen = log.next().await.unwrap().unwrap();
    }
    assert_eq!(seen, ["stopped", "syncing", "offline", "offline"]);
    db.close().await.unwrap();
}

/// A trigger on `coven_sync_status` writes only local tables, and the app
/// can't write the table itself (§9.4).
#[tokio::test]
async fn coven_refuses_a_status_trigger_that_writes_a_synced_table() {
    let store = TestStore::new();
    let error = store
        .builder(
            notes_tables(),
            vec![
                Migration::addition(
                    1,
                    "notes",
                    "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
                ),
                Migration::addition(
                    2,
                    "follow",
                    "CREATE TRIGGER titled AFTER UPDATE ON coven_sync_status BEGIN
                         UPDATE notes SET title=NEW.state;
                     END",
                ),
            ],
        )
        .open()
        .await
        .err()
        .expect("the trigger is refused");
    let expected = DbError::TriggerTarget { trigger: "titled".into(), table: "notes".into() };
    assert_eq!(database_error(error).to_string(), expected.to_string());
    let db = store
        .builder(notes_tables(), vec![Migration::addition(1, "notes", LOGGED)])
        .open()
        .await
        .unwrap();
    let error = crate::write::tests::sql(&db, "UPDATE coven_sync_status SET state='synced'")
        .await
        .unwrap_err();
    assert!(
        matches!(&error, DbError::InternalTable { table } if table == "coven_sync_status"),
        "{error:?}"
    );
    db.close().await.unwrap();
}

/// A read-only handle can't tell whether the writer that last wrote the
/// status still runs: a writer that ended without writing `Stopped` left
/// `Syncing` behind. So app SQL there may not read the row, directly or
/// through the app's view, while the app's trigger on it, which never runs
/// on a read-only handle, still lets the handle open (E5).
#[tokio::test]
async fn a_read_only_handle_cant_read_the_sync_status() {
    let store = TestStore::new();
    let migrations = || {
        vec![
            Migration::addition(1, "notes", LOGGED),
            Migration::addition(
                2,
                "view",
                "CREATE VIEW sync_state AS SELECT state FROM coven_sync_status",
            ),
        ]
    };
    let writer = store.builder(notes_tables(), migrations()).open().await.unwrap();
    writer.record_sync_status("syncing", b"\"Syncing\"".to_vec()).await.unwrap();
    assert_eq!(writer.read(status_row).await.unwrap().0, "syncing");
    let reader = store.builder(notes_tables(), migrations()).open_read_only().await.unwrap();
    for statement in ["SELECT state FROM coven_sync_status", "SELECT state FROM sync_state"] {
        let error = reader
            .read(move |sql| Ok(sql.query_row(statement, [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap_err();
        assert_eq!(
            database_error(error).to_string(),
            DbError::StatementForbidden {
                operation: "reading the writer's sync state through a read-only handle",
            }
            .to_string(),
            "{statement}"
        );
    }
    let notes: i64 = reader
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(notes, 0);
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
