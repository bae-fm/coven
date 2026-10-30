use super::*;
use crate::tests::{database_error, TestStore};
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::write_boundary::tests::version_two;
use crate::{Database, Migration, RowIdentity, SyncedTable};
use coven_format::write::WriteDisposition;
use coven_format::write::{WriteHeader, WriteRecord};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_foundation::{clock::FixedClock, id_source::SequentialIds};
use coven_merge::Timestamp;
use coven_merge::{Audience, WriteId};
use rusqlite::params;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

async fn open(store: &TestStore) -> Database {
    store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES)])
        .clock(Arc::new(FixedClock::new(
            UNIX_EPOCH + Duration::from_secs(1),
        )))
        .open()
        .await
        .unwrap()
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
    assert!(query
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    sql(&source, "INSERT INTO notes VALUES('n','first','body')")
        .await
        .unwrap();
    sql(&source, "UPDATE notes SET title='second'")
        .await
        .unwrap();
    let writes = records(&source);
    assert_eq!(
        receiver.receive(writes[1].clone().into()).await.unwrap(),
        ApplyOutcome::Waiting(WriteWait::Sources(vec![
            coven_format::write_sources::ChangeId {
                write: writes[0].header.position,
                row: writes[0].parts[0].rows[0].row.clone()
            }
        ]))
    );
    assert_eq!(count(&receiver, "notes"), 0);
    assert_eq!(
        receiver.receive(writes[0].clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        query.next().await.expect("the live query answers").unwrap(),
        ["first"]
    );
    assert_eq!(
        receiver.receive(writes[1].clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        query.next().await.expect("the live query answers").unwrap(),
        ["second"]
    );
    assert_eq!(
        receiver.receive(writes[1].clone().into()).await.unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    assert_eq!(count(&receiver, "_coven_uploads"), 0);
    assert_eq!(count(&receiver, "_coven_writes"), 2);
    assert_eq!(
        crate::tests::data(&source, Audience::Store),
        crate::tests::data(&receiver, Audience::Store)
    );
    receiver.close().await.unwrap();
    let receiver = open(&receiver_store).await;
    assert_eq!(
        receiver.receive(writes[0].clone().into()).await.unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    sql(&receiver, "UPDATE notes SET body='received'")
        .await
        .unwrap();
    let outgoing = records(&receiver);
    assert_eq!(outgoing[0].header.read_through, 2);
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
    assert_eq!(b.receive(skipped).await.unwrap(), ApplyOutcome::Applied);
    assert_eq!(count(&b, "notes"), 0);
    sql(&b, "INSERT INTO notes VALUES('b','b','')")
        .await
        .unwrap();
    let dependent = records(&b).remove(0);
    assert!(matches!(
        c.receive(dependent.clone().into()).await,
        Err(DbError::WriteFormat(coven_format::Error::Invalid {
            field: "slot write read-through",
            ..
        }))
    ));
    c.receive(first.clone().into()).await.unwrap();
    assert_eq!(
        c.receive(dependent.into()).await.unwrap(),
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
    write.header.timestamp =
        Timestamp::new(31_536_000_000, 0, write.header.position.device).unwrap();
    write.header.schema_version = 2;
    assert_eq!(
        receiver.receive(write.clone().into()).await.unwrap(),
        ApplyOutcome::Waiting(WriteWait::SchemaVersion(2))
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
                Migration::addition(1, "notes", NOTES),
                Migration::breaking(
                    2,
                    "color",
                    "ALTER TABLE notes ADD COLUMN color TEXT NOT NULL DEFAULT 'blue'",
                    crate::OldWrites::Drop,
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
            receiver.receive(record.into()).await.unwrap(),
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
    assert_eq!(receiver.schema_version().await.unwrap(), 2);
    assert_eq!(source.schema_version().await.unwrap(), 1);
    receiver.close().await.unwrap();
    source.close().await.unwrap();
}
#[tokio::test]
async fn receiver_exclusion_keeps_values_and_cause_without_needing_the_old_table() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = open(&source_store).await;
    let receiver = version_two(&receiver_store, false).await;
    receiver.exclude_later_writes(2).await;
    sql(&source, "INSERT INTO notes VALUES('n','lost','body')")
        .await
        .unwrap();
    let record = records(&source).remove(0);
    assert_eq!(
        receiver.receive(record.clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&receiver, "_coven_rows"), 0);
    assert_eq!(count(&receiver, "_coven_cells"), 0);
    assert_eq!(count(&receiver, "_coven_writes"), 2);
    let losses = receiver.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    assert_eq!(
        losses[0].replaced_by,
        crate::Replacement::SchemaChange { version: 2 }
    );
    assert!(
        matches!(&losses[0].lost,crate::Lost::Row(cells) if cells.len()==3 && cells.iter().all(|c| c.set_by==record.header.position))
    );
    assert_eq!(
        receiver.receive(record.into()).await.unwrap(),
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
    receiver.receive(inserted.into()).await.unwrap();
    sql(&receiver, "INSERT INTO pins VALUES('n')")
        .await
        .unwrap();
    let before = crate::tests::data(&receiver, Audience::Store);
    assert!(
        matches!(sql(&receiver,"DELETE FROM notes").await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="pinned locally")
    );
    sql(&source, "DELETE FROM notes").await.unwrap();
    let deleted = records(&source).remove(1);
    assert!(
        matches!(receiver.receive(deleted.into()).await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="pinned locally")
    );
    assert_eq!(count(&source, "notes"), 0);
    assert_eq!(count(&receiver, "notes"), 1);
    assert_eq!(count(&receiver, "pins"), 1);
    assert_eq!(count(&receiver, "_coven_writes"), 1);
    assert_eq!(count(&receiver, "_coven_lost"), 0);
    assert_eq!(count(&receiver, "_coven_uploads"), 0);
    assert_eq!(crate::tests::data(&receiver, Audience::Store), before);
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
                    vec![Migration::addition_with(
                        1,
                        "local descendants",
                        move |sql| {
                            sql.execute_batch(&base)?;
                            Ok(())
                        },
                    )],
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
        let builder = store.builder(notes(), vec![Migration::addition(1, "notes", NOTES)]);
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
            vec![Migration::addition(1, "references", REFERENCES)],
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
        db.receive(initial.clone().into()).await.unwrap();
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
        db.receive(initial.clone().into()).await.unwrap();
        for (position, index) in order.into_iter().enumerate() {
            let loaded = db.inspect_writer(|db| db.merge_loads());
            db.receive(changes[index].clone().into()).await.unwrap();
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
        let stored = db.inspect_writer(|db| db.query("SELECT table_name,key,audience,generation,value,set_by,replacement_kind,replaced_by,frozen FROM _coven_lost ORDER BY id", [], |r| Ok((r.get::<_,String>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,String>(2)?,r.get::<_,Vec<u8>>(3)?,r.get::<_,Vec<u8>>(4)?,r.get::<_,Vec<u8>>(5)?,r.get::<_,String>(6)?,r.get::<_,Vec<u8>>(7)?,r.get::<_,bool>(8)?))).unwrap());
        if let Some(expected) = &expected_losses {
            assert_eq!(&stored, expected, "{order:?}");
        } else {
            expected_losses = Some(stored);
        }
        let actual = crate::tests::data(&db, Audience::Store);
        if let Some(expected) = &expected {
            assert_eq!(&actual, expected, "{order:?}");
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
            vec![Migration::addition(1, "circles", CIRCLE_SCHEMA)],
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
async fn deleted_circle_takes_out_late_downloads_and_deletion_rolls_back_on_trigger_error() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let receiver_store = TestStore::with_ids(&ids);
    let source = circle_db(&source_store).await;
    let receiver = circle_db(&receiver_store).await;
    let circle = CircleId(uuid::Uuid::from_u128(10));
    crate::tests::delete_circle(&receiver, circle)
        .await
        .unwrap();
    source_store.join_circle(&source, circle);
    circle_note(&source, circle, "00000000-0000-4000-8000-000000000002").await;
    assert_eq!(
        receiver
            .receive(records(&source).remove(0).into())
            .await
            .unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&receiver, "notes"), 0);
    assert_eq!(count(&receiver, "children"), 0);
    assert_eq!(count(&receiver, "_coven_lost"), 2);
    source.inspect_writer(|sql| sql.fail_at("refuse", "BEFORE DELETE ON notes", "keep circle"));
    assert!(
        matches!(crate::tests::delete_circle(&source, circle).await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="keep circle")
    );
    assert_eq!(count(&source, "notes"), 1);
    assert_eq!(count(&source, "children"), 1);
    let state = source.store_state().await.unwrap();
    assert!(!state.circle_deletions.contains_key(&circle));
    assert_eq!(count(&source, "_coven_lost"), 0);
    source.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    crate::tests::delete_circle(&source, circle).await.unwrap();
    assert_eq!(
        crate::tests::data(&source, Audience::Circle(circle)),
        crate::tests::data(&receiver, Audience::Circle(circle))
    );
    for db in [source, receiver] {
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
        let db = if version == 2 {
            version_two(&store, true).await
        } else {
            open(&store).await
        };
        let included = included.len() as u64;
        if included > 0 {
            db.receive(initial.clone().into()).await.unwrap();
        }
        assert!(db
            .install_schema_fixture(coven_merge::Audience::Store, version, included)
            .await
            .unwrap());
        db.close().await.unwrap();
        let db = if version == 2 {
            version_two(&store, true).await
        } else {
            open(&store).await
        };
        assert!(!db
            .install_schema_fixture(coven_merge::Audience::Store, version, included)
            .await
            .unwrap());
        assert_eq!(
            db.receive(initial.clone().into()).await.unwrap(),
            if included > 0 {
                ApplyOutcome::AlreadyApplied
            } else {
                ApplyOutcome::Applied
            }
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
async fn reset_ignores_unread_uncovered_parts_of_its_audience_only() {
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let source = circle_db(&source_store).await;
    let circle = CircleId(uuid::Uuid::from_u128(10));
    source_store.join_circle(&source, circle);
    circle_note(&source, circle, "00000000-0000-4000-8000-000000000002").await;
    sql(&source, "UPDATE notes SET title='late'").await.unwrap();
    let writes = records(&source);
    // Covered originals precede the reset; an uncovered old-prefix part is ignored.
    for (audience, included, skip_first, title) in [
        (Audience::Circle(circle), 0, true, None),
        (Audience::Circle(circle), 1, false, Some("title")),
        (Audience::Circle(circle), 2, false, Some("late")),
        (Audience::Store, 0, false, Some("late")),
    ] {
        let store = TestStore::with_ids(&ids);
        let db = circle_db(&store).await;
        let first = if skip_first {
            DownloadedWrite {
                header: writes[0].header.clone(),
                parts: vec![DownloadedPart::Skipped(Audience::Circle(circle))],
            }
        } else {
            writes[0].clone().into()
        };
        db.receive(first).await.unwrap();
        if included == 2 {
            db.receive(writes[1].clone().into()).await.unwrap();
        }
        let entry = crate::Slot(db.applied_slot().await.unwrap() + 1);
        assert!(db
            .install_reset_fixture(entry, audience.clone(), included)
            .await
            .unwrap());
        db.close().await.unwrap();
        let db = circle_db(&store).await;
        assert!(!db
            .install_reset_fixture(entry, audience, included)
            .await
            .unwrap());
        if skip_first {
            assert_eq!(
                db.receive(writes[0].clone().into()).await.unwrap(),
                ApplyOutcome::AlreadyApplied
            );
        }
        assert_eq!(
            db.receive(writes[1].clone().into()).await.unwrap(),
            if included == 2 {
                ApplyOutcome::AlreadyApplied
            } else {
                ApplyOutcome::Applied
            }
        );
        assert_eq!(
            db.read(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?))
                .await
                .unwrap(),
            title.into_iter().map(String::from).collect::<Vec<_>>()
        );
        // A reset ignores the write's part without a loss.
        assert!(db.lost_values().await.unwrap().is_empty());
        assert_eq!(db.applied_slot().await.unwrap(), 3);
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
                Migration::addition(1, "notes", NOTES),
                Migration::addition(
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
        .builder(notes(), vec![Migration::addition(1, "unique", schema)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let b = b_store
        .builder(notes(), vec![Migration::addition(1, "unique", schema)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let receiver = receiver_store
        .builder(
            notes(),
            vec![
                Migration::addition(1, "unique", schema),
                Migration::breaking(
                    2,
                    "index",
                    "CREATE INDEX notes_body ON notes(body)",
                    crate::OldWrites::Drop,
                ),
            ],
        )
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
        receiver.receive(record.into()).await.unwrap();
    }
    sql(&b, "UPDATE notes SET title='excluded'").await.unwrap();
    let excluded = records(&b).remove(1);
    receiver.exclude_later_writes(2).await;
    receiver.receive(excluded.into()).await.unwrap();
    assert_eq!(count(&receiver, "_coven_lost"), 2);
    sql(&receiver, "INSERT INTO notes VALUES('b','restored','')")
        .await
        .unwrap();
    let losses = receiver.lost_values().await.unwrap();
    assert_eq!(losses.len(), 1);
    assert_eq!(
        losses[0].replaced_by,
        crate::Replacement::SchemaChange { version: 2 }
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
    let mut writes = records(&source);
    let entry = crate::Slot(2);
    receiver.receive(writes[0].clone().into()).await.unwrap();
    receiver
        .install_reset_fixture(entry, Audience::Store, 1)
        .await
        .unwrap();
    // Both successors were authored after reading the reset entry.
    for write in &mut writes[1..] {
        write.header.read_through = entry.0;
    }
    for write in &writes[1..] {
        assert_eq!(
            receiver.receive(write.clone().into()).await.unwrap(),
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
    let before = crate::tests::data(&db, Audience::Store);
    let device = DeviceId(99);
    let record = WriteRecord {
        header: WriteHeader {
            position: WriteId { device, number: 1 },
            timestamp: Timestamp::new(1_000, 0, device).unwrap(),
            read_through: 0,
            schema_version: 1,
            disposition: WriteDisposition::Migration,
        },
        parts: vec![],
    };
    assert_eq!(
        db.receive(record.clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        db.receive(record.clone().into()).await.unwrap(),
        ApplyOutcome::AlreadyApplied
    );
    assert_eq!(db.applied_slot().await.unwrap(), 1);
    for table in [
        "notes",
        "_coven_rows",
        "_coven_cells",
        "_coven_lost",
        "_coven_uploads",
    ] {
        assert_eq!(count(&db, table), 0);
    }
    assert_eq!(crate::tests::data(&db, Audience::Store), before);
    sql(&db, "INSERT INTO notes VALUES('n','after migration','')")
        .await
        .unwrap();
    assert_eq!(records(&db)[0].header.read_through, 1);
    db.close().await.unwrap();
}

#[path = "download_stream_tests.rs"]
mod streams;

/// Applying a write reads each row's merge state a few times, not once for
/// every time merging and putting back its region uses it: it keeps the rows
/// it read last (§21.2).
#[tokio::test]
async fn applying_a_write_reads_each_row_a_few_times() {
    let ids = SequentialIds::new();
    let (a_store, b_store) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
    let a = reference_db(&a_store).await;
    let b = reference_db(&b_store).await;
    a.write(|sql| {
        sql.execute("INSERT INTO parents VALUES('p')", [])?;
        for i in 0..40 {
            sql.execute("INSERT INTO links VALUES(?1,'p')", [format!("l{i}")])?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let before = b.inspect_writer(|db| db.merge_loads());
    b.receive(records(&a).remove(0).into()).await.unwrap();
    let after = b.inspect_writer(|db| db.merge_loads());
    let loads = after.get("links").copied().unwrap_or_default()
        - before.get("links").copied().unwrap_or_default();
    assert_eq!(count(&b, "links"), 40);
    // Once as the write is staged and once as its region is put back: 13
    // times each when nothing is kept.
    assert_eq!(loads, 2 * 40);
    a.close().await.unwrap();
    b.close().await.unwrap();
}
