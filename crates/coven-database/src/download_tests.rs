use super::*;
use crate::tests::{database_error, TestStore};
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Database, Migration, RowIdentity, SyncedTable};
use coven_foundation::id_source::{CircleId, KeyId};
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use rusqlite::params;
use std::sync::Arc;
use std::time::Duration;

async fn open(store: &TestStore) -> Database {
    store
        .builder(notes(), vec![Migration::sql(1, "notes", NOTES)])
        .clock(Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(1),
        )))
        .open()
        .await
        .unwrap()
}

async fn fingerprint(db: &Database, audience: Audience) -> coven_crypto::Fingerprint {
    let key = coven_crypto::StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [7; 32])
        .derive();
    db.sync_state(vec![(audience, key.fingerprint_hasher())])
        .await
        .unwrap()
        .fingerprints[0]
        .1
}

#[tokio::test]
async fn downloaded_writes_share_merge_and_observation_without_uploading() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    let receiver = open(&receiver_store).await;
    let mut query = receiver.subscribe(|sql| {
        Ok(sql.query("SELECT title FROM notes ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert!(query.next().await.unwrap().is_empty());
    sql(&source, "INSERT INTO notes VALUES('n','first','body')")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='second'")
        .await
        .unwrap();
    let writes = records(&source);
    assert_eq!(
        receiver
            .apply_downloaded(writes[1].clone().into())
            .await
            .unwrap(),
        ApplyOutcome::Waiting(WriteWait::Writes(vec![writes[0].header.position]))
    );
    assert_eq!(count(&receiver, "notes"), 0);
    assert_eq!(
        receiver
            .apply_downloaded(writes[0].clone().into())
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(query.next().await.unwrap(), ["first"]);
    assert_eq!(
        receiver
            .apply_downloaded(writes[1].clone().into())
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(query.next().await.unwrap(), ["second"]);
    assert_eq!(
        receiver
            .apply_downloaded(writes[1].clone().into())
            .await
            .unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    assert_eq!(count(&receiver, "_coven_uploads"), 0);
    assert_eq!(count(&receiver, "_coven_writes"), 2);
    assert_eq!(
        fingerprint(&source, Audience::Store).await,
        fingerprint(&receiver, Audience::Store).await
    );
    receiver.close().await.unwrap();
    let receiver = open(&receiver_store).await;
    assert_eq!(
        receiver
            .apply_downloaded(writes[0].clone().into())
            .await
            .unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    sql(&receiver, "UPDATE notes SET body='received'")
        .await
        .unwrap();
    let outgoing = records(&receiver);
    assert_eq!(outgoing[0].header.had_read.0, [writes[1].header.position]);
    assert!(outgoing[0].header.timestamp > writes[1].header.timestamp);
    receiver.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn causal_gate_covers_other_devices_and_skipped_parts_count_as_applied() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = open(&a_store).await;
    let b = open(&b_store).await;
    let c = open(&c_store).await;
    sql(&a, "INSERT INTO notes VALUES('a','a','')")
        .await
        .unwrap();
    let first = records(&a).remove(0);
    let skipped = DownloadedWrite {
        header: first.header.clone(),
        parts: vec![DownloadedPart::Skipped(Audience::Store)],
    };
    assert_eq!(
        b.apply_downloaded(skipped).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&b, "notes"), 0);
    sql(&b, "INSERT INTO notes VALUES('b','b','')")
        .await
        .unwrap();
    let dependent = records(&b).remove(0);
    assert_eq!(
        c.apply_downloaded(dependent.clone().into()).await.unwrap(),
        ApplyOutcome::Waiting(WriteWait::Writes(vec![first.header.position]))
    );
    c.apply_downloaded(first.clone().into()).await.unwrap();
    assert_eq!(
        c.apply_downloaded(dependent.into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&c, "notes"), 2);
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn waiting_downloads_do_not_advance_local_timestamps_even_after_reopen() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    let receiver = open(&receiver_store).await;
    sql(&source, "INSERT INTO notes VALUES('n','new','')")
        .await
        .unwrap();
    let mut write = records(&source).remove(0);
    write.header.schema_version = 2;
    assert_eq!(
        receiver
            .apply_downloaded(write.clone().into())
            .await
            .unwrap(),
        ApplyOutcome::Waiting(WriteWait::SchemaVersion(2))
    );
    write.header.schema_version = 1;
    write.header.timestamp = Timestamp::new(301_001, 0, write.header.position.device).unwrap();
    assert_eq!(
        receiver
            .apply_downloaded(write.clone().into())
            .await
            .unwrap(),
        ApplyOutcome::Waiting(WriteWait::Clock(write.header.timestamp))
    );
    assert_eq!(count(&receiver, "_coven_writes"), 0);
    receiver.close().await.unwrap();
    let receiver = open(&receiver_store).await;
    sql(&receiver, "INSERT INTO notes VALUES('r','local','')")
        .await
        .unwrap();
    assert_eq!(records(&receiver)[0].header.timestamp.milliseconds(), 1_000);
    assert_eq!(count(&receiver, "notes"), 1);
    receiver.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn older_schema_inserts_use_new_defaults_without_inventing_cell_setters() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    let receiver = receiver_store
        .builder(
            notes(),
            vec![
                Migration::sql(1, "notes", NOTES),
                Migration::sql(
                    2,
                    "color",
                    "ALTER TABLE notes ADD COLUMN color TEXT NOT NULL DEFAULT 'blue'",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','first','')")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='second'")
        .await
        .unwrap();
    for record in records(&source) {
        assert_eq!(
            receiver.apply_downloaded(record.into()).await.unwrap(),
            ApplyOutcome::Applied
        );
    }
    assert_eq!(
        receiver
            .read(
                |sql| Ok(sql.query_row("SELECT title,color FROM notes", [], |r| Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?
                )))?)
            )
            .await
            .unwrap(),
        ("second".into(), "blue".into())
    );
    assert_eq!(count(&receiver, "_coven_cells"), 3);
    assert_eq!(receiver.sync_state(vec![]).await.unwrap().schema_version, 2);
    assert_eq!(source.sync_state(vec![]).await.unwrap().schema_version, 1);
    receiver.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn author_marked_loss_keeps_values_and_cause_without_needing_the_old_table() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    let receiver = receiver_store
        .builder(vec![], vec![Migration::sql(1, "empty", "SELECT 1")])
        .open()
        .await
        .unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','lost','body')")
        .await
        .unwrap();
    let mut record = records(&source).remove(0);
    record.header.disposition = WriteDisposition::Lost(1);
    assert_eq!(
        receiver
            .apply_downloaded(record.clone().into())
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&receiver, "_coven_rows"), 0);
    assert_eq!(count(&receiver, "_coven_cells"), 0);
    assert_eq!(count(&receiver, "_coven_writes"), 1);
    let losses = receiver.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    assert_eq!(
        losses[0].replaced_by,
        crate::Replacement::SchemaChange { version: 1 }
    );
    assert!(
        matches!(&losses[0].lost,crate::Lost::Row(cells) if cells.len()==3 && cells.iter().all(|c| c.set_by==record.header.position))
    );
    assert_eq!(
        receiver.apply_downloaded(record.into()).await.unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    assert_eq!(count(&receiver, "_coven_lost"), 1);
    receiver.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn local_delete_trigger_refuses_only_that_device_and_rolls_back_apply() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    let receiver = receiver_store.schema(notes(),"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL DEFAULT ''); CREATE TABLE pins(note TEXT REFERENCES notes(id) ON DELETE CASCADE); CREATE TRIGGER keep_pin BEFORE DELETE ON pins BEGIN SELECT RAISE(ABORT,'pinned locally'); END").await.unwrap();
    sql(&source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let inserted = records(&source).remove(0);
    receiver.apply_downloaded(inserted.into()).await.unwrap();
    sql(&receiver, "INSERT INTO pins VALUES('n')")
        .await
        .unwrap();
    let before = fingerprint(&receiver, Audience::Store).await;
    assert!(
        matches!(sql(&receiver,"DELETE FROM notes").await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="pinned locally")
    );
    sql(&source, "DELETE FROM notes").await.unwrap();
    let deleted = records(&source).remove(1);
    assert!(
        matches!(receiver.apply_downloaded(deleted.into()).await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="pinned locally")
    );
    assert_eq!(count(&source, "notes"), 0);
    assert_eq!(count(&receiver, "notes"), 1);
    assert_eq!(count(&receiver, "pins"), 1);
    assert_eq!(count(&receiver, "_coven_writes"), 1);
    assert_eq!(count(&receiver, "_coven_lost"), 0);
    assert_eq!(count(&receiver, "_coven_uploads"), 0);
    assert_eq!(fingerprint(&receiver, Audience::Store).await, before);
    receiver.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn local_child_actions_are_checked_transitively_at_open_and_after_migration() {
    for first_action in ["CASCADE", "SET NULL"] {
        for (last_column, action, refused) in [
            ("parent TEXT", "CASCADE", false),
            ("parent TEXT", "SET NULL", false),
            ("parent TEXT", "NO ACTION", true),
            ("parent TEXT", "RESTRICT", true),
            ("parent TEXT NOT NULL", "SET NULL", true),
        ] {
            let store = TestStore::new();
            let base = format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE pins(id TEXT PRIMARY KEY,note TEXT REFERENCES NOTES(id) ON DELETE {first_action}); CREATE TABLE nested({last_column} REFERENCES pins(id) ON DELETE {action})");
            let tables = || vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
            let result = store
                .builder(
                    tables(),
                    vec![Migration::run(1, "local descendants", move |sql| {
                        sql.execute_batch(&base)?;
                        Ok(())
                    })],
                )
                .open()
                .await;
            if refused {
                assert!(
                    matches!(database_error(result.err().expect("unsafe action")),DbError::Schema(crate::SchemaError::LocalChildAction {table,column}) if table=="nested" && column=="parent")
                );
            } else {
                result.unwrap().close().await.unwrap();
            }
        }
    }
    let store = TestStore::new();
    let db = open(&store).await;
    db.inspect_writer(|sql| {
        sql.batch("CREATE TABLE pins(note TEXT REFERENCES notes(id))")
            .unwrap()
    });
    db.close().await.unwrap();
    for read_only in [false, true] {
        let builder = store.builder(notes(), vec![Migration::sql(1, "notes", NOTES)]);
        let result = if read_only {
            builder.open_read_only().await.map(|_| ())
        } else {
            builder.open().await.map(|_| ())
        };
        assert!(matches!(
            database_error(result.unwrap_err()),
            DbError::Schema(crate::SchemaError::LocalChildAction { .. })
        ));
    }
}

const REFERENCES: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL); CREATE INDEX links_parent ON links(parent)";
fn references() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("parents", RowIdentity::SharedKey),
        SyncedTable::new("links", RowIdentity::SharedKey),
    ]
}
async fn reference_db(store: &TestStore) -> Database {
    store
        .builder(
            references(),
            vec![Migration::sql(1, "references", REFERENCES)],
        )
        .clock(Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(1),
        )))
        .open()
        .await
        .unwrap()
}

#[tokio::test]
async fn lost_references_keep_the_written_parent_in_every_arrival_order() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let c_store = TestStore::with_ids(&ids);
    let a = reference_db(&a_store).await;
    let b = reference_db(&b_store).await;
    let c = reference_db(&c_store).await;
    sql(
        &a,
        "INSERT INTO parents VALUES('p'),('q'),('r'); INSERT INTO links VALUES('link','r')",
    )
    .await
    .unwrap();
    let initial = records(&a).remove(0);
    for db in [&b, &c] {
        db.apply_downloaded(initial.clone().into()).await.unwrap();
    }
    sql(&b, "UPDATE links SET parent='p'").await.unwrap();
    sql(&c, "UPDATE links SET parent='q'").await.unwrap();
    sql(&a, "DELETE FROM parents WHERE id='p'").await.unwrap();
    let changes = [
        records(&a).remove(1),
        records(&b).remove(0),
        records(&c).remove(0),
    ];
    let mut expected = None;
    let mut expected_losses = None;
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let store = TestStore::with_ids(&ids);
        let db = reference_db(&store).await;
        db.apply_downloaded(initial.clone().into()).await.unwrap();
        for (position, index) in order.into_iter().enumerate() {
            let loaded = db.inspect_writer(|db| db.merge_loads());
            db.apply_downloaded(changes[index].clone().into())
                .await
                .unwrap();
            if index == 0 && position == 2 {
                // Only the lost value points at P: its deletion need not load the child.
                assert_eq!(
                    db.inspect_writer(|db| db.merge_loads()).get("links"),
                    loaded.get("links")
                );
            }
        }
        let losses = db.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1, "{order:?}");
        assert!(
            matches!(&losses[0].lost,crate::Lost::Cell(cell) if cell.column=="parent" && cell.value==rusqlite::types::Value::Text("p".into()) && cell.set_by==changes[1].header.position),
            "{order:?}: {losses:?}"
        );
        assert_eq!(
            losses[0].replaced_by,
            crate::Replacement::Write(changes[2].header.position)
        );
        let stored = db.inspect_writer(|db| db.query("SELECT table_name,key,audience,generation,value,set_by,replacement_kind,replaced_by,retired FROM _coven_lost ORDER BY id", [], |r| Ok((r.get::<_,String>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,String>(2)?,r.get::<_,Vec<u8>>(3)?,r.get::<_,Vec<u8>>(4)?,r.get::<_,Vec<u8>>(5)?,r.get::<_,String>(6)?,r.get::<_,Vec<u8>>(7)?,r.get::<_,bool>(8)?))).unwrap());
        if let Some(expected) = &expected_losses {
            assert_eq!(&stored, expected, "{order:?}");
        } else {
            expected_losses = Some(stored);
        }
        let actual = fingerprint(&db, Audience::Store).await;
        if let Some(expected) = expected {
            assert_eq!(actual, expected, "{order:?}");
        } else {
            expected = Some(actual);
        }
        let restored_store = TestStore::with_ids(&ids);
        let restored = reference_db(&restored_store).await;
        crate::snapshot_write::tests::assert_loaded_losses(&db, &restored).await;
        restored.close().await.unwrap();
        db.close().await.unwrap();
    }
    for db in [a, b, c] {
        db.close().await.unwrap();
    }
}

const CIRCLE_SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,note TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE); CREATE INDEX child_note ON children(note)";
const NOTE: &str = "00000000-0000-4000-8000-000000000001";
fn circle_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("children", RowIdentity::IndependentUuid).audience_from("note"),
    ]
}
async fn circle_db(store: &TestStore) -> Database {
    store
        .builder(
            circle_tables(),
            vec![Migration::sql(1, "circles", CIRCLE_SCHEMA)],
        )
        .clock(Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(1),
        )))
        .open()
        .await
        .unwrap()
}
async fn circle_note(db: &Database, circle: CircleId, child: &str) {
    let child = child.to_owned();
    db.write(move |sql| {
        sql.execute(
            "INSERT INTO notes VALUES(?1,?2,'title')",
            params![NOTE, circle.to_string()],
        )?;
        sql.execute("INSERT INTO children VALUES(?1,?2)", params![child, NOTE])?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fingerprints_ignore_other_audience_removal_and_its_descendants() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let both_store = TestStore::with_ids(&ids);
    let reverse_store = TestStore::with_ids(&ids);
    let a = circle_db(&a_store).await;
    let b = circle_db(&b_store).await;
    let both = circle_db(&both_store).await;
    let reverse = circle_db(&reverse_store).await;
    let ca = CircleId(uuid::Uuid::from_u128(10));
    let cb = CircleId(uuid::Uuid::from_u128(11));
    circle_note(&a, ca, "00000000-0000-4000-8000-000000000002").await;
    circle_note(&b, cb, "00000000-0000-4000-8000-000000000003").await;
    let ra = records(&a).remove(0);
    let rb = records(&b).remove(0);
    for (db, writes) in [
        (&both, [ra.clone(), rb.clone()]),
        (&reverse, [rb.clone(), ra.clone()]),
    ] {
        for write in writes {
            db.apply_downloaded(write.into()).await.unwrap();
        }
        assert_eq!(count(db, "notes"), 1);
        assert_eq!(count(db, "children"), 1);
        assert_eq!(count(db, "_coven_lost"), 2);
        for (audience, source) in [(Audience::Circle(ca), &a), (Audience::Circle(cb), &b)] {
            assert_eq!(
                fingerprint(db, audience.clone()).await,
                fingerprint(source, audience).await
            );
        }
    }
    let b_before = fingerprint(&both, Audience::Circle(cb)).await;
    crate::store_log::tests::delete_circle(&both, ca)
        .await
        .unwrap();
    assert_eq!(fingerprint(&both, Audience::Circle(cb)).await, b_before);
    let visible: String = both
        .read(|sql| Ok(sql.query_row("SELECT audience FROM notes", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(visible, cb.to_string());
    // A key can arrive after all writes; rekeying reads the same durable root.
    let other_key =
        coven_crypto::StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([1; 16])), [9; 32])
            .derive();
    let rekeyed = both
        .sync_state(vec![(Audience::Circle(cb), other_key.fingerprint_hasher())])
        .await
        .unwrap();
    assert_ne!(rekeyed.fingerprints[0].1, b_before);
    for db in [a, b, both, reverse] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn deleted_circle_takes_out_late_downloads_and_deletion_rolls_back_on_trigger_error() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = circle_db(&source_store).await;
    let receiver = circle_db(&receiver_store).await;
    let circle = CircleId(uuid::Uuid::from_u128(10));
    crate::store_log::tests::delete_circle(&receiver, circle)
        .await
        .unwrap();
    circle_note(&source, circle, "00000000-0000-4000-8000-000000000002").await;
    assert_eq!(
        receiver
            .apply_downloaded(records(&source).remove(0).into())
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&receiver, "notes"), 0);
    assert_eq!(count(&receiver, "children"), 0);
    assert_eq!(count(&receiver, "_coven_lost"), 2);
    source.inspect_writer(|sql| sql.batch("CREATE TRIGGER refuse BEFORE DELETE ON notes BEGIN SELECT RAISE(ABORT,'keep circle'); END").unwrap());
    assert!(
        matches!(crate::store_log::tests::delete_circle(&source, circle).await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="keep circle")
    );
    assert_eq!(count(&source, "notes"), 1);
    assert_eq!(count(&source, "children"), 1);
    assert!(!source.store_log().await.unwrap().replay.state.circles[&circle].deleted);
    assert_eq!(count(&source, "_coven_lost"), 0);
    source.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    crate::store_log::tests::delete_circle(&source, circle)
        .await
        .unwrap();
    assert_eq!(
        fingerprint(&source, Audience::Circle(circle)).await,
        fingerprint(&receiver, Audience::Circle(circle)).await
    );
    for db in [source, receiver] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn surviving_children_are_retargeted_before_the_old_parent_is_deleted() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE CASCADE); CREATE INDEX links_parent ON links(parent); CREATE TABLE audit(value TEXT); CREATE TRIGGER edited AFTER UPDATE ON links BEGIN INSERT INTO audit VALUES('update'); END";
    let a = a_store.schema(references(), schema).await.unwrap();
    let b = b_store.schema(references(), schema).await.unwrap();
    sql(
        &a,
        "INSERT INTO parents VALUES('old'); INSERT INTO links VALUES('link','old')",
    )
    .await
    .unwrap();
    b.apply_downloaded(records(&a).remove(0).into())
        .await
        .unwrap();
    sql(&a,"INSERT INTO parents VALUES('new'); UPDATE links SET parent='new'; DELETE FROM parents WHERE id='old'").await.unwrap();
    b.apply_downloaded(records(&a).remove(1).into())
        .await
        .unwrap();
    assert_eq!(count(&b, "links"), 1);
    assert_eq!(count(&b, "audit"), 1);
    assert_eq!(
        fingerprint(&a, Audience::Store).await,
        fingerprint(&b, Audience::Store).await
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn applied_schema_boundaries_exclude_only_uncovered_older_writes() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    sql(&source, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let initial = records(&source).remove(0);
    for (version, included, lost) in [
        (2, vec![], true),
        (2, vec![initial.header.position], false),
        (1, vec![], false),
    ] {
        let store = TestStore::with_ids(&ids);
        let db = open(&store).await;
        let included = WritePositions(included);
        assert!(db
            .apply_breaking_change(coven_merge::Audience::Store, version, included.clone())
            .await
            .unwrap());
        db.close().await.unwrap();
        let db = open(&store).await;
        assert!(!db
            .apply_breaking_change(coven_merge::Audience::Store, version, included)
            .await
            .unwrap());
        assert_eq!(
            db.apply_downloaded(initial.clone().into()).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(count(&db, "notes"), i64::from(!lost));
        assert_eq!(count(&db, "_coven_lost"), i64::from(lost));
        if lost {
            assert_eq!(
                db.lost_values().await.unwrap()[0].replaced_by,
                crate::Replacement::SchemaChange { version: 2 }
            );
        }
        db.close().await.unwrap();
    }
    source.close().await.unwrap();
}

#[tokio::test]
async fn reset_exclusion_is_audience_scoped_and_counts_implicit_own_reads() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let source = circle_db(&source_store).await;
    let circle = CircleId(uuid::Uuid::from_u128(10));
    circle_note(&source, circle, "00000000-0000-4000-8000-000000000002").await;
    sql(&source, "UPDATE notes SET title='late'").await.unwrap();
    let writes = records(&source);
    let entry = coven_format::value::EntryId {
        device: DeviceId(99),
        number: 1,
    };
    for (audience, included, lost) in [
        (
            Audience::Circle(circle),
            vec![WriteId {
                device: DeviceId(99),
                number: 1,
            }],
            true,
        ),
        (
            Audience::Circle(circle),
            vec![writes[0].header.position],
            false,
        ),
        (
            Audience::Circle(circle),
            vec![writes[1].header.position],
            false,
        ),
        (Audience::Store, vec![], false),
    ] {
        let store = TestStore::with_ids(&ids);
        let db = circle_db(&store).await;
        let first = if lost {
            DownloadedWrite {
                header: writes[0].header.clone(),
                parts: vec![DownloadedPart::Skipped(Audience::Circle(circle))],
            }
        } else {
            writes[0].clone().into()
        };
        db.apply_downloaded(first).await.unwrap();
        let included = WritePositions(included);
        assert!(db
            .apply_reset(entry, audience.clone(), included.clone())
            .await
            .unwrap());
        db.close().await.unwrap();
        let db = circle_db(&store).await;
        assert!(!db.apply_reset(entry, audience, included).await.unwrap());
        assert_eq!(
            db.apply_downloaded(writes[1].clone().into()).await.unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(count(&db, "notes"), i64::from(!lost));
        let losses = db.lost_values().await.unwrap();
        assert_eq!(losses.len(), usize::from(lost));
        if lost {
            assert_eq!(losses[0].replaced_by, crate::Replacement::Reset(entry));
        }
        db.close().await.unwrap();
    }
    source.close().await.unwrap();
}

#[tokio::test]
async fn an_invalid_local_child_migration_rolls_back_its_schema_and_version() {
    let store = TestStore::new();
    let db = open(&store).await;
    db.close().await.unwrap();
    let result = store
        .builder(
            notes(),
            vec![
                Migration::sql(1, "notes", NOTES),
                Migration::sql(
                    2,
                    "pin",
                    "CREATE TABLE pins(note TEXT REFERENCES notes(id) ON DELETE RESTRICT)",
                ),
            ],
        )
        .open()
        .await;
    assert!(matches!(
        database_error(result.err().expect("refused migration")),
        DbError::Schema(crate::SchemaError::LocalChildAction { .. })
    ));
    let db = open(&store).await;
    assert_eq!(db.schema_version().await.unwrap(), 1);
    assert_eq!(
        db.inspect_writer(|sql| sql
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='pins'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap()),
        0
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn excluded_values_do_not_replace_a_removed_rows_merge_values() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL UNIQUE,body TEXT NOT NULL DEFAULT '')";
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
    let a = a_store
        .builder(notes(), vec![Migration::sql(1, "unique", schema)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let b = b_store
        .builder(notes(), vec![Migration::sql(1, "unique", schema)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let receiver = receiver_store
        .builder(notes(), vec![Migration::sql(1, "unique", schema)])
        .clock(clock)
        .open()
        .await
        .unwrap();
    sql(&a, "INSERT INTO notes VALUES('a','title','')")
        .await
        .unwrap();
    sql(&b, "INSERT INTO notes VALUES('b','title','')")
        .await
        .unwrap();
    for record in [records(&a).remove(0), records(&b).remove(0)] {
        receiver.apply_downloaded(record.into()).await.unwrap();
    }
    sql(&b, "UPDATE notes SET title='excluded'").await.unwrap();
    let mut excluded = records(&b).remove(1);
    excluded.header.disposition = WriteDisposition::Lost(1);
    receiver.apply_downloaded(excluded.into()).await.unwrap();
    assert_eq!(count(&receiver, "_coven_lost"), 2);
    sql(&receiver, "INSERT INTO notes VALUES('b','restored','')")
        .await
        .unwrap();
    let losses = receiver.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    assert_eq!(
        losses[0].replaced_by,
        crate::Replacement::SchemaChange { version: 1 }
    );
    for db in [a, b, receiver] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_reset_successor_can_read_another_post_reset_write() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    let receiver = open(&receiver_store).await;
    sql(&source, "INSERT INTO notes VALUES('n','initial','')")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='first successor'")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='second successor'")
        .await
        .unwrap();
    let writes = records(&source);
    receiver
        .apply_downloaded(writes[0].clone().into())
        .await
        .unwrap();
    receiver
        .apply_reset(
            coven_format::value::EntryId {
                device: DeviceId(99),
                number: 1,
            },
            Audience::Store,
            WritePositions(vec![writes[0].header.position]),
        )
        .await
        .unwrap();
    for write in &writes[1..] {
        assert_eq!(
            receiver
                .apply_downloaded(write.clone().into())
                .await
                .unwrap(),
            ApplyOutcome::Applied
        );
    }
    assert!(receiver.lost_values().await.unwrap().is_empty());
    assert_eq!(
        receiver
            .read(
                |sql| Ok(sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
            )
            .await
            .unwrap(),
        "second successor"
    );
    for db in [source, receiver] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn migration_download_advances_position_without_rows_or_losses() {
    let store = TestStore::new();
    let db = open(&store).await;
    let before = fingerprint(&db, Audience::Store).await;
    let device = DeviceId(99);
    let record = WriteRecord {
        header: WriteHeader {
            store_log_read: coven_format::value::EntryPositions(Vec::new()),
            position: WriteId { device, number: 1 },
            timestamp: Timestamp::new(1_000, 0, device).unwrap(),
            had_read: WritePositions(vec![]),
            schema_version: 1,
            disposition: WriteDisposition::Migration,
        },
        parts: vec![],
    };
    assert_eq!(
        db.apply_downloaded(record.clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        db.apply_downloaded(record.clone().into()).await.unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    assert_eq!(
        db.sync_state(vec![]).await.unwrap().positions.0,
        [record.header.position]
    );
    for table in [
        "notes",
        "_coven_rows",
        "_coven_cells",
        "_coven_lost",
        "_coven_uploads",
    ] {
        assert_eq!(count(&db, table), 0);
    }
    assert_eq!(fingerprint(&db, Audience::Store).await, before);
    sql(&db, "INSERT INTO notes VALUES('n','after migration','')")
        .await
        .unwrap();
    assert_eq!(records(&db)[0].header.had_read.0, [record.header.position]);
    db.close().await.unwrap();
}

#[path = "download_stream_tests.rs"]
mod streams;
