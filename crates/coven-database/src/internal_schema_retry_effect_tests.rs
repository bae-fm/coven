use super::{RetryState, RetrySync};
use crate::test_utils::notes_tables;
use crate::tests::{database_error, TestStore};
use crate::{DbError, Migration};
use std::time::{Duration, SystemTime};

/// The app's notes, and a local table its trigger keeps: each retry effect
/// coven writes, in order.
const LOGGED: &str = "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
     CREATE TABLE effect_log(id INTEGER PRIMARY KEY, effect TEXT NOT NULL);
     CREATE TRIGGER effect_logged AFTER UPDATE ON coven_retry_effect BEGIN
         INSERT INTO effect_log(effect) VALUES(NEW.effect);
     END;";

type Row = (String, Option<i64>);

fn effect_row(sql: crate::SqlReadContext<'_>) -> Result<Row, DbError> {
    Ok(sql.query_row("SELECT effect,cooling_until FROM coven_retry_effect", [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?)
}

/// Write `state` and return it as committed.
async fn change(db: &crate::Database, state: RetryState) -> RetryState {
    db.change_retry_state(move |_| state, |committed| committed).await.unwrap()
}

fn syncing(held: bool, cooling_until: Option<SystemTime>) -> RetryState {
    RetryState { sync: RetrySync::Started { cooling_until }, held }
}

fn stopped(held: bool) -> RetryState {
    RetryState { sync: RetrySync::Stopped, held }
}

/// The effect is computed from the row's state as each change commits, and
/// a live query on the table sees each one in order: not syncing, a pass, a
/// cooldown with its end, a held failure over the cooldown, and stopping,
/// which drops the cooldown and keeps the failure (E4, E5).
#[tokio::test]
async fn a_live_query_follows_each_retry_effect_as_it_changes() {
    let store = TestStore::new();
    let db =
        store.builder(notes_tables(), crate::test_utils::notes_migrations()).open().await.unwrap();
    let mut row = crate::test_utils::Followed::new(db.subscribe(effect_row));
    assert_eq!(row.next().await, ("not_syncing".into(), None));
    change(&db, syncing(false, None)).await;
    assert_eq!(row.next().await, ("pass".into(), None));
    let until = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
    change(&db, syncing(false, Some(until))).await;
    assert_eq!(row.next().await, ("cooling_down".into(), Some(1_700_000_000_000)));
    change(&db, syncing(true, Some(until))).await;
    assert_eq!(row.next().await, ("failed".into(), Some(1_700_000_000_000)));
    let committed = change(&db, stopped(true)).await;
    assert_eq!(committed, stopped(true));
    assert_eq!(row.next().await, ("not_syncing".into(), None));
    db.close().await.unwrap();
}

/// A cooldown's end is stored rounded up to the millisecond, never before
/// the cooldown's own end, and the state the change returns is the one the
/// row holds; `then` sees the state `change` was given (§6.7).
#[tokio::test]
async fn the_committed_state_is_the_stored_one() {
    let store = TestStore::new();
    let db =
        store.builder(notes_tables(), crate::test_utils::notes_migrations()).open().await.unwrap();
    // A microsecond past the millisecond: Windows keeps SystemTime in 100ns
    // steps, so a nanosecond past it would be dropped before coven saw it.
    let until = SystemTime::UNIX_EPOCH + Duration::from_micros(1_700_000_000_000_001);
    let committed = change(&db, syncing(false, Some(until))).await;
    let stored = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_001);
    assert_eq!(committed, syncing(false, Some(stored)));
    let seen = db.change_retry_state(|state| state, |state| state).await.unwrap();
    assert_eq!(seen, committed);
    assert_eq!(
        db.read(effect_row).await.unwrap(),
        ("cooling_down".into(), Some(1_700_000_000_001))
    );
    db.close().await.unwrap();
}

/// Opening writes the stopped row with nothing held: a writer that ended
/// while syncing left its row behind, and no connection plans in the next
/// process until sync starts there.
#[tokio::test]
async fn opening_writes_not_syncing() {
    let store = TestStore::new();
    let db =
        store.builder(notes_tables(), crate::test_utils::notes_migrations()).open().await.unwrap();
    change(&db, syncing(true, None)).await;
    db.close().await.unwrap();
    let db =
        store.builder(notes_tables(), crate::test_utils::notes_migrations()).open().await.unwrap();
    assert_eq!(db.read(effect_row).await.unwrap(), ("not_syncing".into(), None));
    let seen = db.change_retry_state(|state| state, |state| state).await.unwrap();
    assert_eq!(seen, stopped(false));
    db.close().await.unwrap();
}

/// A local trigger on `coven_retry_effect` keeps the app's own log of each
/// effect, in the transaction that writes it; a state written again
/// unchanged leaves the row alone and fires nothing (§9.4).
#[tokio::test]
async fn a_local_trigger_follows_each_retry_effect_as_it_changes() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), vec![Migration::addition(1, "notes", LOGGED)])
        .open()
        .await
        .unwrap();
    let mut log = crate::test_utils::Followed::new(db.subscribe(|sql| {
        Ok::<_, crate::DbError>(sql.query(
            "SELECT effect FROM effect_log ORDER BY id",
            [],
            |r| r.get::<_, String>(0),
        )?)
    }));
    assert!(log.next().await.is_empty());
    for state in [syncing(false, None), syncing(false, None), syncing(true, None)] {
        change(&db, state).await;
    }
    let seen = log.wait_for(|seen| seen.len() >= 2).await;
    assert_eq!(seen, ["pass", "failed"]);
    db.close().await.unwrap();
}

/// While sync is stopped no owner would write a cooldown's end: the table
/// refuses a cooldown beside it, and the app can't write the table (§9.4).
#[tokio::test]
async fn the_table_refuses_a_cooldown_while_stopped_and_any_app_write() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), vec![Migration::addition(1, "notes", LOGGED)])
        .open()
        .await
        .unwrap();
    let refused = db.commit_writer(|sql| {
        sql.internal_execute("UPDATE coven_retry_effect SET cooling_until=1 WHERE singleton=1", [])
    });
    assert!(matches!(refused, Err(DbError::Sqlite(_))), "{refused:?}");
    let error =
        crate::write::tests::sql(&db, "UPDATE coven_retry_effect SET syncing=1").await.unwrap_err();
    assert!(
        matches!(&error, DbError::InternalTable { table } if table == "coven_retry_effect"),
        "{error:?}"
    );
    db.close().await.unwrap();
}

/// A read-only handle can't tell whether the writer that last wrote the
/// row still runs, so app SQL there may not read it (E5).
#[tokio::test]
async fn a_read_only_handle_cant_read_the_retry_effect() {
    let store = TestStore::new();
    let migrations = || vec![Migration::addition(1, "notes", LOGGED)];
    let writer = store.builder(notes_tables(), migrations()).open().await.unwrap();
    change(&writer, syncing(false, None)).await;
    let reader = store.builder(notes_tables(), migrations()).open_read_only().await.unwrap();
    let error = reader
        .read(|sql| {
            Ok(sql.query_row("SELECT effect FROM coven_retry_effect", [], |r| {
                r.get::<_, String>(0)
            })?)
        })
        .await
        .unwrap_err();
    assert_eq!(
        database_error(error).to_string(),
        DbError::StatementForbidden {
            operation: "reading the writer's sync state through a read-only handle",
        }
        .to_string()
    );
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

/// A stored cooldown end the clock can't name is a stored fact that doesn't
/// decode (§19.3), as a stored `last_synced` is.
#[test]
fn a_stored_cooldown_end_before_the_epoch_is_damaged_database() {
    assert!(matches!(crate::stored_time(-1), Err(DbError::DamagedDatabase)));
}
