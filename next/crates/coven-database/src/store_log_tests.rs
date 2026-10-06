use super::*;
use crate::{
    tests::TestStore,
    write::tests::{count, records, sql},
    Database, RowIdentity, SyncedTable,
};
use coven_format::{
    store_log::{MemberPublicKeys, StoreChange},
    value::EntryPositions,
};
use coven_merge::Timestamp;
use std::time::Duration;

fn keys(seed: u8) -> MemberPublicKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([seed; 64]);
    let keys = coven_crypto::MemberKeys::from_secret_bytes(&bytes).unwrap();
    MemberPublicKeys {
        signing: keys.member_id(),
        sealing: keys.sealing_public_key(),
    }
}

fn key(n: u128) -> KeyId {
    KeyId(uuid::Uuid::from_u128(n))
}

fn entry(number: u64, change: StoreChange) -> StoreLogEntry {
    let device = DeviceId(u64::MAX);
    StoreLogEntry {
        position: EntryId { device, number },
        timestamp: Timestamp::new(number, 0, device).unwrap(),
        author: keys(1).signing,
        had_read: EntryPositions(vec![]),
        change,
    }
}

// Supplied replay results are fixtures for the database boundary, not computed here.
fn circle_history(circle: CircleId) -> Vec<(StoreLogEntry, StoreLogReplay)> {
    let ana = keys(1);
    let ben = keys(2);
    let store = StoreIdentity {
        id: StoreId(uuid::Uuid::from_u128(1)),
        name: "Store".into(),
        key: key(1),
    };
    let first = entry(
        1,
        StoreChange::CreateStore {
            store: store.id,
            name: store.name.clone(),
            admin: ana.clone(),
            key: store.key,
            device_name: "Ana’s phone".into(),
        },
    );
    let mut replay = StoreLogReplay::default();
    replay.state.store = Some(store);
    replay.state.members.insert(
        ana.signing.clone(),
        StoreMember {
            sealing: ana.sealing,
            role: MemberRole::Admin,
            removed: false,
        },
    );
    replay.state.devices.insert(
        first.position.device,
        StoreDevice {
            member: ana.signing.clone(),
            name: "Ana’s phone".into(),
            removed: false,
        },
    );
    replay.entries.insert(first.position, EntryOutcome::Kept);
    let mut history = vec![(first, replay.clone())];
    let add = entry(
        2,
        StoreChange::AddMember {
            keys: ben.clone(),
            role: MemberRole::Member,
        },
    );
    replay.state.members.insert(
        ben.signing.clone(),
        StoreMember {
            sealing: ben.sealing,
            role: MemberRole::Member,
            removed: false,
        },
    );
    replay.entries.insert(add.position, EntryOutcome::Kept);
    history.push((add, replay.clone()));
    let create = entry(
        3,
        StoreChange::CreateCircle {
            circle,
            name: "Gifts".into(),
            key: key(2),
        },
    );
    replay.state.circles.insert(
        circle,
        StoreCircle {
            name: "Gifts".into(),
            key: key(2),
            deleted: false,
            members: [ana.signing.clone()].into(),
        },
    );
    replay.entries.insert(create.position, EntryOutcome::Kept);
    history.push((create, replay.clone()));
    let add = entry(
        4,
        StoreChange::AddCircleMember {
            circle,
            member: ben.signing.clone(),
        },
    );
    replay
        .state
        .circles
        .get_mut(&circle)
        .unwrap()
        .members
        .insert(ben.signing.clone());
    replay.entries.insert(add.position, EntryOutcome::Kept);
    history.push((add, replay.clone()));
    let mut delete = entry(1, StoreChange::DeleteCircle { circle });
    delete.author = ben.signing;
    delete.position.device = DeviceId(u64::MAX - 1);
    delete.timestamp = Timestamp::new(6, 0, delete.position.device).unwrap();
    delete.had_read = EntryPositions(vec![history[3].0.position]);
    let gifts = replay.state.circles.get_mut(&circle).unwrap();
    gifts.deleted = true;
    gifts.members.clear();
    replay.entries.insert(delete.position, EntryOutcome::Kept);
    history.push((delete, replay));
    history
}

/// Apply the fixed Gifts history through the public transaction, including on retry.
pub(crate) async fn delete_circle(db: &Database, circle: CircleId) -> Result<(), DbError> {
    let present = db.store_log().await.unwrap();
    let history = circle_history(circle);
    for (entry, replay) in &history[..4] {
        if !present.replay.entries.contains_key(&entry.position) {
            db.apply_store_log(entry.clone(), replay.clone()).await?;
        }
    }
    let (entry, replay) = history.last().unwrap();
    db.apply_store_log(entry.clone(), replay.clone()).await
}

const NOTE: &str = "00000000-0000-4000-8000-000000000001";
const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,note TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE); CREATE INDEX children_note ON children(note)";

async fn open(store: &TestStore) -> Database {
    store
        .schema(
            vec![
                SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
                SyncedTable::new("children", RowIdentity::IndependentUuid).audience_from("note"),
            ],
            SCHEMA,
        )
        .await
        .unwrap()
}

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut crate::LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must publish")
        .unwrap()
}

#[tokio::test]
async fn deletion_and_reversal_commit_marks_and_visible_rows_together() {
    let store = TestStore::new();
    let db = open(&store).await;
    let circle = CircleId(uuid::Uuid::from_u128(2));
    let history = circle_history(circle);
    for (entry, replay) in &history[..4] {
        db.apply_store_log(entry.clone(), replay.clone())
            .await
            .unwrap();
    }
    db.write(move |sql| {
        sql.execute(
            "INSERT INTO notes VALUES(?1,?2,'gift')",
            (NOTE, circle.to_string()),
        )?;
        sql.execute(
            "INSERT INTO children VALUES('00000000-0000-4000-8000-000000000002',?1)",
            [NOTE],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let mut rows = db.subscribe(|sql| {
        Ok(sql.query_row(
            "SELECT (SELECT count(*) FROM notes),(SELECT count(*) FROM children)",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?)
    });
    let mut losses = db.subscribe_lost_values();
    assert_eq!(next(&mut rows).await, (1, 1));
    assert!(next(&mut losses).await.is_empty());
    let (delete, deleted) = history[4].clone();
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER refuse BEFORE DELETE ON notes BEGIN SELECT RAISE(ABORT,'keep circle'); END").unwrap());
    assert!(db
        .apply_store_log(delete.clone(), deleted.clone())
        .await
        .is_err());
    assert_eq!(db.store_log().await.unwrap(), before);
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(count(&db, "children"), 1);
    assert_eq!(count(&db, "coven_deleted_circles"), 0);
    assert!(!rows.is_marked_for_rerun());
    assert!(!losses.is_marked_for_rerun());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    db.apply_store_log(delete.clone(), deleted.clone())
        .await
        .unwrap();
    assert_eq!(next(&mut rows).await, (0, 0));
    assert_eq!(next(&mut losses).await.len(), 2);
    assert_eq!(db.store_log().await.unwrap().replay, deleted);
    assert_eq!(count(&db, "coven_deleted_circles"), 1);

    // §9: Ana's earlier concurrent removal of Ben beats Ben's deletion.
    let remove = entry(
        5,
        StoreChange::RemoveCircleMember {
            circle,
            member: keys(2).signing,
            key: key(3),
        },
    );
    let mut restored = history[3].1.clone();
    let gifts = restored.state.circles.get_mut(&circle).unwrap();
    gifts.members = [keys(1).signing].into();
    gifts.key = key(3);
    restored.entries.insert(remove.position, EntryOutcome::Kept);
    restored.entries.insert(
        delete.position,
        EntryOutcome::Dropped(DropReason::BeatenBy(remove.position)),
    );
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER refuse BEFORE INSERT ON notes BEGIN SELECT RAISE(ABORT,'refuse restoration'); END").unwrap());
    assert!(db
        .apply_store_log(remove.clone(), restored.clone())
        .await
        .is_err());
    assert_eq!(db.store_log().await.unwrap(), before);
    assert_eq!(count(&db, "coven_deleted_circles"), 1);
    assert_eq!(count(&db, "notes"), 0);
    assert!(!rows.is_marked_for_rerun());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    db.apply_store_log(remove.clone(), restored.clone())
        .await
        .unwrap();
    assert_eq!(next(&mut rows).await, (1, 1));
    assert!(next(&mut losses).await.is_empty());
    assert_eq!(count(&db, "coven_deleted_circles"), 0);
    assert_eq!(records(&db).len(), 1, "recomputation creates no app write");
    let committed = db.store_log().await.unwrap();
    assert_eq!(committed.replay, restored);
    assert_eq!(committed.entries[4], remove);
    assert_eq!(committed.entries[5], delete);
    db.close().await.unwrap();
    let db = open(&store).await;
    assert_eq!(db.store_log().await.unwrap(), committed);
    assert_eq!(count(&db, "notes"), 1);
    sql(&db, "UPDATE notes SET title='restored'").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn circle_deletion_commits_file_removal_with_the_entry_and_rolls_both_back() {
    use crate::file_write::tests::owned_paths;
    use crate::{CacheFill, FileDecl, Provenance, Uploads};

    let store = TestStore::new();
    let circle = CircleId(uuid::Uuid::from_u128(2));
    let declaration = SyncedTable::new("files", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .carries_files(FileDecl::new(
            "files",
            Provenance::AppProvided,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        ));
    let db = store
        .schema(
            vec![declaration],
            "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT,audience TEXT NOT NULL)",
        )
        .await
        .unwrap();
    let history = circle_history(circle);
    for (entry, replay) in &history[..4] {
        db.apply_store_log(entry.clone(), replay.clone())
            .await
            .unwrap();
    }
    db.write_with_files(
        |batch| {
            batch.put_file("files", NOTE, b"original".to_vec());
            Ok(())
        },
        move |sql| {
            sql.execute(
                "INSERT INTO files(id,size,audience) VALUES(?1,8,?2)",
                (NOTE, circle.to_string()),
            )?;
            Ok(())
        },
    )
    .await
    .unwrap();
    let paths = owned_paths(&store);
    assert_eq!(paths.len(), 1);
    let before = db.store_log().await.unwrap();
    let (entry, replay) = history[4].clone();
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER refuse_file AFTER DELETE ON coven_device_files BEGIN SELECT RAISE(ABORT,'keep file'); END").unwrap());
    assert!(db
        .apply_store_log(entry.clone(), replay.clone())
        .await
        .is_err());
    assert_eq!(db.store_log().await.unwrap(), before);
    assert_eq!(count(&db, "files"), 1);
    assert_eq!(count(&db, "coven_device_files"), 1);
    assert_eq!(owned_paths(&store), paths);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"original");
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse_file").unwrap());
    db.apply_store_log(entry, replay.clone()).await.unwrap();
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    assert_eq!(count(&db, "files"), 0);
    assert_eq!(count(&db, "coven_device_files"), 0);
    assert_eq!(count(&db, "coven_file_removals"), 0);
    assert!(owned_paths(&store).is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failure_halfway_through_replacing_state_rolls_back_the_entry_too() {
    let store = TestStore::new();
    let db = open(&store).await;
    let circle = CircleId(uuid::Uuid::from_u128(2));
    let history = circle_history(circle);
    for (entry, replay) in &history[..2] {
        db.apply_store_log(entry.clone(), replay.clone())
            .await
            .unwrap();
    }
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER coven_refuse BEFORE INSERT ON coven_circles BEGIN SELECT RAISE(ABORT,'state failed'); END").unwrap());
    let (entry, replay) = history[2].clone();
    assert!(db
        .apply_store_log(entry.clone(), replay.clone())
        .await
        .is_err());
    assert_eq!(db.store_log().await.unwrap(), before);
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER coven_refuse").unwrap());
    db.apply_store_log(entry.clone(), replay.clone())
        .await
        .unwrap();
    db.apply_store_log(entry, replay.clone()).await.unwrap();
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    db.close().await.unwrap();
}

#[tokio::test]
async fn stale_results_and_changed_entry_bytes_cannot_replace_the_applied_set() {
    let store = TestStore::new();
    let db = open(&store).await;
    let history = circle_history(CircleId(uuid::Uuid::from_u128(2)));
    let (first, initial) = &history[0];
    db.apply_store_log(first.clone(), initial.clone())
        .await
        .unwrap();
    let mut changed = first.clone();
    changed.timestamp = Timestamp::new(2, 0, changed.position.device).unwrap();
    assert!(
        matches!(db.apply_store_log(changed, initial.clone()).await, Err(DbError::StoreLogEntryChanged(id)) if id==first.position)
    );
    let (second, next) = &history[1];
    assert!(matches!(
        db.apply_store_log(second.clone(), initial.clone()).await,
        Err(DbError::StoreLogEntriesChanged)
    ));
    db.apply_store_log(second.clone(), next.clone())
        .await
        .unwrap();
    assert!(matches!(
        db.apply_store_log(first.clone(), initial.clone()).await,
        Err(DbError::StoreLogEntriesChanged)
    ));
    assert_eq!(db.store_log().await.unwrap().replay, *next);
    db.close().await.unwrap();
}

#[tokio::test]
async fn removed_identities_keys_versions_resets_and_every_drop_reason_round_trip() {
    let store = TestStore::new();
    let db = open(&store).await;
    let circle = CircleId(uuid::Uuid::from_u128(u128::MAX));
    delete_circle(&db, circle).await.unwrap();
    let mut expected = db.store_log().await.unwrap();
    let mut replay = expected.replay.clone();
    replay
        .state
        .members
        .get_mut(&keys(2).signing)
        .unwrap()
        .removed = true;
    replay
        .state
        .devices
        .get_mut(&DeviceId(u64::MAX))
        .unwrap()
        .removed = true;
    replay.state.store.as_mut().unwrap().key = key(u128::MAX);
    for (index, reason) in [
        DropReason::TargetGone,
        DropReason::NoAdminLeft,
        DropReason::NotAllowed,
        DropReason::WrongCircleKeys,
        DropReason::BeatenBy(expected.entries[0].position),
    ]
    .into_iter()
    .enumerate()
    {
        let e = entry(
            index as u64 + 5,
            StoreChange::ChangeRole {
                member: keys(2).signing,
                role: MemberRole::Member,
            },
        );
        replay
            .entries
            .insert(e.position, EntryOutcome::Dropped(reason));
        db.apply_store_log(e.clone(), replay.clone()).await.unwrap();
        expected.entries.push(e);
    }
    let snapshot = SnapshotId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
        audience: Audience::Store,
    };
    let schema = entry(
        10,
        StoreChange::RaiseSchema {
            version: u32::MAX,
            snapshot: snapshot.clone(),
        },
    );
    replay.state.schema.insert(
        Audience::Store,
        StoreVersion {
            number: u32::MAX,
            snapshot: snapshot.clone(),
            entry: schema.position,
        },
    );
    replay.entries.insert(schema.position, EntryOutcome::Kept);
    db.apply_store_log(schema.clone(), replay.clone())
        .await
        .unwrap();
    expected.entries.push(schema);
    let format = entry(
        11,
        StoreChange::RaiseFormat {
            version: u16::MAX,
            snapshot: snapshot.clone(),
        },
    );
    replay.state.format.insert(
        Audience::Store,
        StoreVersion {
            number: u16::MAX,
            snapshot,
            entry: format.position,
        },
    );
    replay.entries.insert(format.position, EntryOutcome::Kept);
    db.apply_store_log(format.clone(), replay.clone())
        .await
        .unwrap();
    expected.entries.push(format);
    for (index, audience) in [Audience::Store, Audience::Circle(circle)]
        .into_iter()
        .enumerate()
    {
        let snapshot = SnapshotId {
            device: DeviceId(u64::MAX),
            number: u64::MAX,
            audience: audience.clone(),
        };
        let reset = entry(
            index as u64 + 12,
            StoreChange::Reset {
                snapshot: snapshot.clone(),
            },
        );
        replay.state.resets.insert(audience, snapshot);
        replay.entries.insert(reset.position, EntryOutcome::Kept);
        db.apply_store_log(reset.clone(), replay.clone())
            .await
            .unwrap();
        expected.entries.push(reset);
    }
    expected.entries.sort_by_key(|e| e.timestamp);
    expected.replay = replay;
    assert_eq!(db.store_log().await.unwrap(), expected);
    db.close().await.unwrap();
    let db = open(&store).await;
    assert_eq!(db.store_log().await.unwrap(), expected);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn entries_and_result_are_read_from_one_snapshot_during_an_apply() {
    let store = TestStore::new();
    let db = open(&store).await;
    let history = circle_history(CircleId(uuid::Uuid::from_u128(2)));
    let (first, initial) = history[0].clone();
    db.apply_store_log(first, initial).await.unwrap();
    let (ready, wait_ready) = std::sync::mpsc::channel();
    let (resume, wait_resume) = std::sync::mpsc::channel();
    let reader = db.clone();
    let read = tokio::spawn(async move {
        reader
            .read(move |sql| {
                let before = sql.store_log()?;
                ready.send(()).unwrap();
                wait_resume.recv_timeout(Duration::from_secs(5)).unwrap();
                assert_eq!(sql.store_log()?, before);
                Ok(before)
            })
            .await
            .unwrap()
    });
    wait_ready.recv_timeout(Duration::from_secs(5)).unwrap();
    let (second, next) = history[1].clone();
    db.apply_store_log(second, next.clone()).await.unwrap();
    resume.send(()).unwrap();
    assert_eq!(read.await.unwrap().entries.len(), 1);
    assert_eq!(db.store_log().await.unwrap().replay, next);
    db.close().await.unwrap();
}

#[tokio::test]
async fn audience_versions_round_trip_and_a_failed_raise_keeps_every_audience() {
    let store = TestStore::new();
    let db = open(&store).await;
    let gifts = CircleId(uuid::Uuid::from_u128(2));
    let notes = CircleId(uuid::Uuid::from_u128(3));
    for (entry, replay) in &circle_history(gifts)[..4] {
        db.apply_store_log(entry.clone(), replay.clone())
            .await
            .unwrap();
    }
    let mut replay = db.store_log().await.unwrap().replay;
    let create = entry(
        5,
        StoreChange::CreateCircle {
            circle: notes,
            name: "Notes".into(),
            key: key(3),
        },
    );
    replay.state.circles.insert(
        notes,
        StoreCircle {
            name: "Notes".into(),
            key: key(3),
            deleted: false,
            members: [keys(1).signing].into(),
        },
    );
    replay.entries.insert(create.position, EntryOutcome::Kept);
    db.apply_store_log(create, replay.clone()).await.unwrap();
    let mut number = 6;
    for (audience, version) in [
        (Audience::Store, 8),
        (Audience::Circle(gifts), 3),
        (Audience::Circle(notes), 5),
    ] {
        let snapshot = SnapshotId {
            audience: audience.clone(),
            device: DeviceId(u64::MAX),
            number: u64::MAX - number,
        };
        let schema = entry(
            number,
            StoreChange::RaiseSchema {
                version,
                snapshot: snapshot.clone(),
            },
        );
        number += 1;
        replay.state.schema.insert(
            audience.clone(),
            StoreVersion {
                number: version,
                snapshot: snapshot.clone(),
                entry: schema.position,
            },
        );
        replay.entries.insert(schema.position, EntryOutcome::Kept);
        db.apply_store_log(schema, replay.clone()).await.unwrap();
        let format = entry(
            number,
            StoreChange::RaiseFormat {
                version: version as u16,
                snapshot: snapshot.clone(),
            },
        );
        number += 1;
        replay.state.format.insert(
            audience,
            StoreVersion {
                number: version as u16,
                snapshot,
                entry: format.position,
            },
        );
        replay.entries.insert(format.position, EntryOutcome::Kept);
        db.apply_store_log(format, replay.clone()).await.unwrap();
        assert_eq!(db.store_log().await.unwrap().replay, replay);
    }
    let before = db.store_log().await.unwrap();
    let snapshot = SnapshotId {
        audience: Audience::Circle(gifts),
        device: DeviceId(7),
        number: 100,
    };
    let raised = entry(
        number,
        StoreChange::RaiseSchema {
            version: 13,
            snapshot: snapshot.clone(),
        },
    );
    replay.state.schema.insert(
        snapshot.audience.clone(),
        StoreVersion {
            number: 13,
            snapshot,
            entry: raised.position,
        },
    );
    replay.entries.insert(raised.position, EntryOutcome::Kept);
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER coven_refuse BEFORE INSERT ON coven_store_state WHEN NEW.version=13 BEGIN SELECT RAISE(ABORT,'refuse circle raise'); END").unwrap());
    assert!(db
        .apply_store_log(raised.clone(), replay.clone())
        .await
        .is_err());
    assert_eq!(db.store_log().await.unwrap(), before);
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER coven_refuse").unwrap());
    db.apply_store_log(raised, replay.clone()).await.unwrap();
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    db.close().await.unwrap();
    let db = open(&store).await;
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    db.close().await.unwrap();
}
