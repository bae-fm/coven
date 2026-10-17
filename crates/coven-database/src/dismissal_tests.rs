use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Database, Migration};
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use coven_merge::Audience;
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

async fn open(store: &TestStore, schema: &'static str) -> Database {
    store
        .builder(notes(), vec![Migration::sql(1, "notes", schema)])
        .clock(Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(1),
        )))
        .open()
        .await
        .unwrap()
}

async fn data(
    db: &Database,
) -> (
    Vec<coven_merge::RowState<coven_format::value::Value>>,
    Vec<coven_format::loss::Loss>,
) {
    crate::tests::data(db, Audience::Store)
}

#[tokio::test]
async fn dismissed_cell_is_removed_on_both_devices_and_after_reopen() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa, NOTES).await;
    let b = open(&sb, NOTES).await;
    sql(&a, "INSERT INTO notes VALUES('n','initial','')")
        .await
        .unwrap();
    crate::author_judgement::tests::deliver(&a, &b).await;
    sql(&a, "UPDATE notes SET title='a'").await.unwrap();
    sql(&b, "UPDATE notes SET title='b'").await.unwrap();
    crate::author_judgement::tests::deliver(&a, &b).await;
    crate::author_judgement::tests::deliver(&b, &a).await;
    let loss = a.lost_values().await.unwrap();
    assert_eq!(loss.len(), 1);
    assert_eq!(loss, b.lost_values().await.unwrap());
    let before = data(&a).await;
    let mut live = a.subscribe_lost_values();
    assert_eq!(live.next().await.unwrap(), loss);
    a.dismiss_lost_values(&loss).await.unwrap();
    assert!(live.next().await.unwrap().is_empty());
    assert_eq!(count(&a, "_coven_lost"), 0);
    assert_ne!(data(&a).await, before);
    let dismissal = records(&a).last().unwrap().clone();
    b.receive(dismissal.clone().into()).await.unwrap();
    b.receive(dismissal.into()).await.unwrap();
    assert_eq!(count(&b, "_coven_lost"), 0);
    assert_eq!(data(&a).await, data(&b).await);
    let queued = records(&a).len();
    a.dismiss_lost_values(&loss).await.unwrap();
    assert_eq!(records(&a).len(), queued);
    a.close().await.unwrap();
    let a = open(&sa, NOTES).await;
    assert!(a.lost_values().await.unwrap().is_empty());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

const UNIQUE: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL UNIQUE,body TEXT NOT NULL)";

#[tokio::test]
async fn dismissed_removed_row_stays_deleted_and_concurrent_edit_is_lost() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa, UNIQUE).await;
    let b = open(&sb, UNIQUE).await;
    sql(&a, "INSERT INTO notes VALUES('a','same','a body')")
        .await
        .unwrap();
    sql(&b, "INSERT INTO notes VALUES('b','same','b body')")
        .await
        .unwrap();
    crate::author_judgement::tests::deliver(&a, &b).await;
    crate::author_judgement::tests::deliver(&b, &a).await;
    let loss = a.lost_values().await.unwrap();
    assert_eq!(loss.len(), 1);
    a.dismiss_lost_values(&loss).await.unwrap();
    assert_eq!(count(&a, "_coven_lost"), 0);
    sql(
        &b,
        "INSERT INTO notes VALUES('b','changed','concurrent body')",
    )
    .await
    .unwrap();
    crate::author_judgement::tests::deliver(&b, &a).await;
    crate::author_judgement::tests::deliver(&a, &b).await;
    assert_eq!(
        a.lost_values().await.unwrap(),
        b.lost_values().await.unwrap()
    );
    assert!(a.lost_values().await.unwrap().iter().any(|loss| matches!(&loss.lost, crate::Lost::Cell(cell) if cell.value == crate::types::Value::Text("concurrent body".into()))));
    assert_eq!(data(&a).await, data(&b).await);
    sql(&a, "UPDATE notes SET title='other' WHERE id='a'")
        .await
        .unwrap();
    b.receive(records(&a)[2].clone().into()).await.unwrap();
    for db in [&a, &b] {
        assert_eq!(
            db.read(|s| Ok(s
                .query_row("SELECT count(*) FROM notes WHERE id='b'", [], |r| r
                    .get::<_, i64>(0))?))
                .await
                .unwrap(),
            0
        );
    }
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn set_null_child_returns_when_its_removed_parent_is_dismissed() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let tables = || {
        vec![
            crate::SyncedTable::new("parents", crate::RowIdentity::SharedKey),
            crate::SyncedTable::new("children", crate::RowIdentity::SharedKey),
        ]
    };
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,label TEXT UNIQUE); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL); CREATE INDEX child_parent ON children(parent)";
    let a = sa.schema(tables(), schema).await.unwrap();
    let b = sb.schema(tables(), schema).await.unwrap();
    sql(&a, "INSERT INTO parents VALUES('a','same')")
        .await
        .unwrap();
    sql(
        &b,
        "INSERT INTO parents VALUES('b','same'); INSERT INTO children VALUES('c','b')",
    )
    .await
    .unwrap();
    crate::author_judgement::tests::deliver(&a, &b).await;
    crate::author_judgement::tests::deliver(&b, &a).await;
    assert_eq!(count(&a, "children"), 0);
    let losses = a
        .lost_values()
        .await
        .unwrap()
        .into_iter()
        .filter(|loss| loss.table == "parents")
        .collect::<Vec<_>>();
    a.dismiss_lost_values(&losses).await.unwrap();
    b.receive(records(&a)[1].clone().into()).await.unwrap();
    for db in [&a, &b] {
        assert_eq!(
            db.read(|s| Ok(s
                .query_row("SELECT parent FROM children WHERE id='c'", [], |r| r
                    .get::<_, Option<String>>(0))?))
                .await
                .unwrap(),
            None
        );
        assert!(db.lost_values().await.unwrap().is_empty());
    }
    assert_eq!(data(&a).await, data(&b).await);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn excluded_cells_can_be_dismissed_without_their_app_table() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store, NOTES).await;
    let receiver = receiver_store
        .builder(vec![], vec![Migration::sql(1, "empty", "SELECT 1")])
        .open()
        .await
        .unwrap();
    let empty = data(&receiver).await;
    sql(&source, "INSERT INTO notes VALUES('n','excluded','body')")
        .await
        .unwrap();
    let mut record = records(&source).remove(0);
    record.header.schema_version = 0;
    receiver.exclude_version_zero().await;
    receiver.receive(record.into()).await.unwrap();
    let losses = receiver.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    receiver.dismiss_lost_values(&losses).await.unwrap();
    assert!(receiver.lost_values().await.unwrap().is_empty());
    assert_eq!(data(&receiver).await, empty);
    assert_eq!(records(&receiver)[0].parts[0].dismissals.len(), 3);
    receiver.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_dismissal_rolls_back_its_delete_losses_position_and_queue() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let a = open(&sa, UNIQUE).await;
    let b = open(&sb, UNIQUE).await;
    sql(&a, "INSERT INTO notes VALUES('a','same','a')")
        .await
        .unwrap();
    sql(&b, "INSERT INTO notes VALUES('b','same','b')")
        .await
        .unwrap();
    a.receive(records(&b)[0].clone().into()).await.unwrap();
    let losses = a.lost_values().await.unwrap();
    let before = data(&a).await;
    a.inspect_writer(|sql| sql.fail_at("refuse", "BEFORE INSERT ON _coven_uploads", "refuse"));
    assert!(matches!(
        a.dismiss_lost_values(&losses).await,
        Err(crate::CovenError::Database(crate::DbError::Sqlite(_)))
    ));
    assert_eq!(a.lost_values().await.unwrap(), losses);
    assert_eq!(data(&a).await, before);
    assert_eq!(records(&a).len(), 1);
    a.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    a.dismiss_lost_values(&losses).await.unwrap();
    assert_eq!(records(&a)[1].header.position.number, 2);
    a.close().await.unwrap();
    b.close().await.unwrap();
}
