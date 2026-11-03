use crate::test_utils::notes_tables;
use crate::tests::{database_error, TestStore};
use crate::{DbError, Migration, RecordedLocation, SyncStatusRow};
use std::time::{Duration, SystemTime};

/// The app's notes, and a local table its trigger keeps: each change coven
/// writes to the sync status row, in order.
const LOGGED: &str = "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
     CREATE TABLE status_log(id INTEGER PRIMARY KEY, storage TEXT NOT NULL, moving INTEGER NOT NULL);
     CREATE TRIGGER status_logged AFTER UPDATE ON coven_sync_status BEGIN
         INSERT INTO status_log(storage,moving) VALUES(NEW.storage,NEW.moving);
     END;";

fn row(sql: crate::SqlReadContext<'_>) -> crate::CovenResult<SyncStatusRow> {
    sql.sync_status_row()
}

fn at(millis: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
}

fn connected() -> RecordedLocation<String> {
    RecordedLocation::Connected("S3".into())
}

/// A new store has its row from its creation on, saying no storage is set
/// up, no pass has ended and sync never finished, so a live query on it has
/// one before any open writes it (E4, E5).
#[tokio::test]
async fn a_new_store_reads_no_storage_set_up() {
    let store = TestStore::new();
    let db =
        store.builder(notes_tables(), crate::test_utils::notes_migrations()).open().await.unwrap();
    let mut rows = db.subscribe(row);
    assert_eq!(
        rows.next().await.unwrap().unwrap(),
        SyncStatusRow {
            location: RecordedLocation::None,
            last_pass: None,
            moving: false,
            last_synced: None,
        }
    );
    db.record_opened(RecordedLocation::SignedOut("Dropbox".into()), None).await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().location,
        RecordedLocation::SignedOut("Dropbox".into())
    );
    db.close().await.unwrap();
}

/// The last synced time never goes backward: a write keeps the later of
/// the kept time and its own, so a clock set back moves nothing, and a pass
/// that didn't reach the end leaves it. Ending a pass, or opening, writes
/// that no pass moves (§19.1).
#[tokio::test]
async fn the_last_synced_time_never_goes_backward() {
    let store = TestStore::new();
    let db =
        store.builder(notes_tables(), crate::test_utils::notes_migrations()).open().await.unwrap();
    db.record_location(connected(), Some(at(5_000))).await.unwrap();
    db.record_moving().await.unwrap();
    assert!(db.read(row).await.unwrap().moving);
    db.record_pass_end(b"\"Synced\"".to_vec(), Some(at(4_000))).await.unwrap();
    let read = db.read(row).await.unwrap();
    assert_eq!((read.moving, read.last_synced), (false, Some(at(5_000))));
    db.record_pass_end(b"\"Synced\"".to_vec(), Some(at(9_000))).await.unwrap();
    assert_eq!(db.read(row).await.unwrap().last_synced, Some(at(9_000)));
    let offline = br#"{"Offline":{"failure":"Network","error":"one"}}"#.to_vec();
    db.record_pass_end(offline.clone(), None).await.unwrap();
    assert_eq!(
        db.read(row).await.unwrap(),
        SyncStatusRow {
            location: connected(),
            last_pass: Some(offline),
            moving: false,
            last_synced: Some(at(9_000)),
        }
    );
    db.record_location(connected(), Some(at(7_000))).await.unwrap();
    assert_eq!(db.read(row).await.unwrap().last_synced, Some(at(9_000)));
    db.record_moving().await.unwrap();
    db.record_opened(connected(), None).await.unwrap();
    assert!(!db.read(row).await.unwrap().moving);
    db.close().await.unwrap();
}

/// A local trigger on `coven_sync_status` keeps the app's own log of each
/// change, in the transaction that writes it; a write that changes nothing
/// leaves the row alone and fires nothing (§9.4).
#[tokio::test]
async fn a_local_trigger_follows_each_change_of_the_row() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), vec![Migration::addition(1, "notes", LOGGED)])
        .open()
        .await
        .unwrap();
    let mut log = db.subscribe(|sql| {
        Ok(sql.query("SELECT storage,moving FROM status_log ORDER BY id", [], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?))
        })?)
    });
    assert!(log.next().await.unwrap().unwrap().is_empty());
    db.record_opened(connected(), None).await.unwrap();
    db.record_opened(connected(), None).await.unwrap();
    db.record_moving().await.unwrap();
    db.record_moving().await.unwrap();
    db.record_pass_end(b"\"Synced\"".to_vec(), None).await.unwrap();
    db.record_last_pass(b"\"Synced\"".to_vec()).await.unwrap();
    let mut seen = log.next().await.unwrap().unwrap();
    while seen.len() < 3 {
        seen = log.next().await.unwrap().unwrap();
    }
    assert_eq!(
        seen,
        [("connected".into(), false), ("connected".into(), true), ("connected".into(), false)]
    );
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
                         UPDATE notes SET title=NEW.storage;
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
    let error =
        crate::write::tests::sql(&db, "UPDATE coven_sync_status SET moving=1").await.unwrap_err();
    assert!(
        matches!(&error, DbError::InternalTable { table } if table == "coven_sync_status"),
        "{error:?}"
    );
    db.close().await.unwrap();
}

/// A read-only handle can't tell whether the writer that last wrote the
/// row still runs: a writer that ended mid-pass left its move behind. So
/// app SQL there may not read the row, directly or through the app's view,
/// while the app's trigger on it, which never runs on a read-only handle,
/// still lets the handle open (E5).
#[tokio::test]
async fn a_read_only_handle_cant_read_the_sync_status() {
    let store = TestStore::new();
    let migrations = || {
        vec![
            Migration::addition(1, "notes", LOGGED),
            Migration::addition(
                2,
                "view",
                "CREATE VIEW sync_state AS SELECT storage FROM coven_sync_status",
            ),
        ]
    };
    let writer = store.builder(notes_tables(), migrations()).open().await.unwrap();
    writer.record_moving().await.unwrap();
    assert!(writer.read(row).await.unwrap().moving);
    let reader = store.builder(notes_tables(), migrations()).open_read_only().await.unwrap();
    for statement in ["SELECT storage FROM coven_sync_status", "SELECT storage FROM sync_state"] {
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
