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
        .builder(notes(), vec![Migration::addition(1, "notes", schema)])
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
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        loss
    );
    a.dismiss_lost_values(&loss).await.unwrap();
    assert!(live
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
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
    let receiver = crate::write_boundary::tests::version_two(&receiver_store, false).await;
    receiver.exclude_later_writes(2).await;
    let empty = data(&receiver).await;
    sql(&source, "INSERT INTO notes VALUES('n','excluded','body')")
        .await
        .unwrap();
    let record = records(&source).remove(0);
    receiver.receive(record.into()).await.unwrap();
    let losses = receiver.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    receiver.dismiss_lost_values(&losses).await.unwrap();
    assert!(receiver.lost_values().await.unwrap().is_empty());
    assert_eq!(data(&receiver).await, empty);
    assert_eq!(
        records(&receiver).last().unwrap().parts[0].dismissals.len(),
        3
    );
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

#[tokio::test]
async fn dismissing_a_parent_records_its_owned_cascade_chain_in_one_write() {
    cascade_deletions(|_, _| {}).await;
}

pub(crate) async fn cascade_deletions(
    mut compare: impl FnMut(&crate::Database, &coven_format::write::WriteRecord),
) {
    use crate::author_judgement::tests::deliver;
    use crate::judge::tests::{causes, key, verdict};
    use coven_format::snapshot_rows::ChangeVerdict;
    use coven_merge::Operation;

    for select_all in [false, true] {
        let ids = SequentialIds::new();
        let sa = TestStore::with_ids(&ids);
        let sb = TestStore::with_ids(&ids);
        let tables = || {
            vec![
                crate::SyncedTable::new("parents", crate::RowIdentity::SharedKey),
                crate::SyncedTable::new("children", crate::RowIdentity::SharedKey).owner_only(),
                crate::SyncedTable::new("grandchildren", crate::RowIdentity::SharedKey)
                    .owner_only(),
            ]
        };
        let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,label TEXT UNIQUE);
        CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE CASCADE);
        CREATE TABLE grandchildren(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES children(id) ON DELETE CASCADE)";
        let a = sa.schema(tables(), schema).await.unwrap();
        let b = sb.schema(tables(), schema).await.unwrap();
        for db in [&a, &b] {
            TestStore::register(db, sa.device(), 1);
            TestStore::register(db, sb.device(), 2);
        }
        sql(&a, "INSERT INTO parents VALUES('a','same')")
            .await
            .unwrap();
        sql(
            &b,
            "INSERT INTO parents VALUES('b','same'),('other','other');
        INSERT INTO children VALUES('c','b'),('other','other');
        INSERT INTO grandchildren VALUES('g','c')",
        )
        .await
        .unwrap();
        deliver(&a, &b).await;
        deliver(&b, &a).await;
        let losses: Vec<_> = a
            .lost_values()
            .await
            .unwrap()
            .into_iter()
            .filter(|loss| select_all || loss.table == "parents")
            .collect();
        assert_eq!(losses.len(), if select_all { 3 } else { 1 });
        let before = data(&a).await;
        a.inspect_writer(|sql| sql.fail_at("refuse", "BEFORE INSERT ON _coven_uploads", "refuse"));
        assert!(a.dismiss_lost_values(&losses).await.is_err());
        assert_eq!(data(&a).await, before);
        a.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
        a.dismiss_lost_values(&losses).await.unwrap();
        let record = records(&a).pop().unwrap();
        assert_eq!(record.parts[0].rows.len(), 3);
        for change in &record.parts[0].rows {
            assert_eq!(change.change.operation, Operation::Delete);
            assert_eq!(change.change.generation, 1);
            assert_eq!(change.execution.steps.len(), 1);
        }
        b.receive(record.clone().into()).await.unwrap();
        compare(&b, &record);
        for db in [&a, &b] {
            assert_eq!(
                verdict(db, &record, &key("children", "c")),
                ChangeVerdict::Derived(causes(&record, &[("parents", "b")]))
            );
            assert_eq!(
                verdict(db, &record, &key("grandchildren", "g")),
                ChangeVerdict::Derived(causes(&record, &[("parents", "b"), ("children", "c")]))
            );
            assert!(db.lost_values().await.unwrap().is_empty());
            assert_eq!(count(db, "children"), 1);
            db.inspect_writer_schema(|writer, schema| {
                let app = crate::write_rows::AppView::after(writer, schema);
                let store = crate::merge_store::MergeStore::new(writer, &app);
                for change in &record.parts[0].rows {
                    assert_eq!(store.row(&change.row).unwrap().state.generation(), 2);
                }
            });
        }
        assert_eq!(data(&a).await, data(&b).await);
        let queued = records(&a).len();
        a.dismiss_lost_values(&losses).await.unwrap();
        assert_eq!(records(&a).len(), queued);
        sql(
            &b,
            "INSERT INTO children VALUES('c','other'); INSERT INTO grandchildren VALUES('g','c')",
        )
        .await
        .unwrap();
        let restored = records(&b).pop().unwrap();
        assert!(restored.parts[0].rows.iter().all(
            |r| r.change.generation == 2 && matches!(r.change.operation, Operation::Insert(_))
        ));
        a.close().await.unwrap();
        let a = sa.schema(tables(), schema).await.unwrap();
        assert!(a.lost_values().await.unwrap().is_empty());
        assert_eq!(count(&a, "children"), 1);
        a.close().await.unwrap();
        b.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_dismissal_records_each_row_of_a_cascade_cycle_once() {
    let ids = SequentialIds::new();
    let sa = TestStore::with_ids(&ids);
    let sb = TestStore::with_ids(&ids);
    let tables = || {
        vec![crate::SyncedTable::new(
            "nodes",
            crate::RowIdentity::SharedKey,
        )]
    };
    let schema = "CREATE TABLE nodes(id TEXT NOT NULL PRIMARY KEY,label TEXT UNIQUE,parent TEXT REFERENCES nodes(id) ON DELETE CASCADE)";
    let a = sa.schema(tables(), schema).await.unwrap();
    let b = sb.schema(tables(), schema).await.unwrap();
    sql(&a, "INSERT INTO nodes VALUES('a','same',NULL)")
        .await
        .unwrap();
    sql(&b, "INSERT INTO nodes VALUES('b','same',NULL),('c','other','b'); UPDATE nodes SET parent='c' WHERE id='b'").await.unwrap();
    crate::author_judgement::tests::deliver(&a, &b).await;
    crate::author_judgement::tests::deliver(&b, &a).await;
    let losses: Vec<_> = a
        .lost_values()
        .await
        .unwrap()
        .into_iter()
        .filter(|loss| loss.key.0 == [rusqlite::types::Value::Text("b".into())])
        .collect();
    a.dismiss_lost_values(&losses).await.unwrap();
    let record = records(&a).pop().unwrap();
    assert_eq!(record.parts[0].rows.len(), 2);
    assert!(record.parts[0]
        .rows
        .iter()
        .all(|r| r.change.operation == coven_merge::Operation::Delete));
    b.receive(record.into()).await.unwrap();
    assert!(a.lost_values().await.unwrap().is_empty());
    assert_eq!(data(&a).await, data(&b).await);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn an_empty_frozen_delete_can_be_dismissed_after_receipt_and_snapshot() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let source = open(&source_store, NOTES).await;
    sql(&source, "INSERT INTO notes VALUES('n','excluded','body')")
        .await
        .unwrap();
    sql(&source, "DELETE FROM notes").await.unwrap();
    let mut writes = records(&source);
    writes[1].parts[0].rows[0].old.clear();
    let mut other = writes[1].clone();
    other.header.position.number += 1;
    other.header.timestamp = coven_merge::Timestamp::new(
        other.header.timestamp.milliseconds() + 1,
        0,
        other.header.position.device,
    )
    .unwrap();
    writes.push(other);
    let a = crate::write_boundary::tests::version_two(&a_store, false).await;
    let b = crate::write_boundary::tests::version_two(&b_store, false).await;
    a.exclude_later_writes(2).await;
    for write in &writes {
        a.receive(write.clone().into()).await.unwrap();
    }
    for marker in records(&b) {
        a.receive(marker.into()).await.unwrap();
    }
    let losses = a.lost_values().await.unwrap();
    assert_eq!(losses.len(), 3);
    let empty = losses
        .iter()
        .find(|loss| matches!(&loss.lost, crate::Lost::Row(cells) if cells.is_empty()))
        .unwrap()
        .clone();
    crate::snapshot_write::tests::assert_loaded_losses(&a, &b).await;
    let original_count = count(&b, "_coven_authored_changes");
    let queued = records(&b).len();
    b.dismiss_lost_values(std::slice::from_ref(&empty))
        .await
        .unwrap();
    let remaining: Vec<_> = losses.into_iter().filter(|loss| loss != &empty).collect();
    assert_eq!(remaining.len(), 2);
    assert_eq!(
        b.lost_values().await.unwrap(),
        remaining,
        "the empty loss must be acknowledged"
    );
    assert_eq!(count(&b, "_coven_authored_changes"), original_count);
    assert_eq!(records(&b).len(), queued + 1);
    let dismissal = records(&b).pop().unwrap();
    assert!(dismissal.parts[0].rows.is_empty());
    assert_eq!(dismissal.parts[0].dismissals.len(), 1);
    a.receive(dismissal.clone().into()).await.unwrap();
    a.receive(dismissal.into()).await.unwrap();
    assert_eq!(a.lost_values().await.unwrap(), remaining);
    b.dismiss_lost_values(std::slice::from_ref(&empty))
        .await
        .unwrap();
    assert_eq!(records(&b).len(), queued + 1);
    b.close().await.unwrap();
    let b = crate::write_boundary::tests::version_two(&b_store, false).await;
    assert_eq!(b.lost_values().await.unwrap(), remaining);
    assert_eq!(count(&b, "_coven_authored_changes"), original_count);
    b.close().await.unwrap();
    a.close().await.unwrap();
    source.close().await.unwrap();
}
