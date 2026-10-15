use super::*;
use crate::pending::tests::Record as PendingRecord;
use crate::{
    tests::TestStore,
    write::tests::{count, records, sql},
    Database, RowIdentity, StoreLogCheck, SyncedTable,
};
use coven_format::{
    store_log::{MemberPublicKeys, StoreChange, StoreLogEntry},
    value::EntryPositions,
};
use coven_merge::Timestamp;
use std::time::Duration;

pub(crate) fn keys(seed: u8) -> MemberPublicKeys {
    let mut bytes = b"CVMK\x01".to_vec();
    bytes.extend([seed; 64]);
    coven_crypto::MemberKeys::from_secret_bytes(&bytes)
        .unwrap()
        .public_keys()
}

/// A stable member id, independent of `keys(seed)`.
pub(crate) fn member(seed: u8) -> MemberId {
    MemberId(uuid::Uuid::from_bytes([seed; 16]))
}

fn key(n: u128) -> coven_foundation::id_source::KeyId {
    coven_foundation::id_source::KeyId(uuid::Uuid::from_u128(n))
}

fn entry(number: u64, change: StoreChange) -> StoreLogEntry {
    let device = DeviceId(u64::MAX);
    StoreLogEntry {
        position: EntryId { device, number },
        timestamp: Timestamp::new(number, 0, device).unwrap(),
        author: member(1),
        had_read: EntryPositions(vec![]),
        change,
    }
}

pub(crate) fn checked(entry: StoreLogEntry) -> ReplayEntry {
    let check = match &entry.change {
        StoreChange::RemoveMember { .. } | StoreChange::RemoveCircleMember { .. } => {
            StoreLogCheck::MemberRemoval(BTreeMap::new())
        }
        _ => StoreLogCheck::Allowed,
    };
    ReplayEntry {
        entry,
        stored_at: coven_format::value::StorageTime::UNIX_EPOCH,
        check,
    }
}

// Supplied replay results are fixtures for the database boundary, not computed here.
pub(crate) fn circle_history(circle: CircleId) -> Vec<(StoreLogEntry, StoreLogReplay)> {
    let (ana, ana_keys) = (member(1), keys(1));
    let (ben, ben_keys) = (member(2), keys(2));
    let store = StoreIdentity {
        id: crate::tests::store_id(),
        name: crate::tests::STORE_NAME.into(),
    };
    let first = entry(
        1,
        StoreChange::CreateStore {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
                principal: coven_format::S3Principal::Console,
            },
            store: store.id,
            name: store.name.clone(),
            admin: ana_keys,
            key: key(1),
            device_name: "Ana’s phone".into(),
            key_hash: [17; 32],
        },
    );
    let mut replay = StoreLogReplay::default();
    replay.state.store = Some(store);
    replay.state.members.insert(
        ana,
        StoreMember {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
                principal: coven_format::S3Principal::Console,
            },
            keys: ana_keys,
            role: MemberRole::Admin,
            removed: false,
        },
    );
    replay.state.devices.insert(
        first.position.device,
        StoreDevice {
            member: ana,
            name: "Ana’s phone".into(),
            state: DeviceState::Active,
        },
    );
    replay.entries.insert(first.position, EntryOutcome::Kept);
    let mut history = vec![(first, replay.clone())];
    let add = entry(
        2,
        StoreChange::AddMember {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
                principal: coven_format::S3Principal::Console,
            },
            member: ben,
            keys: ben_keys,
            role: MemberRole::Member,
        },
    );
    replay.state.members.insert(
        ben,
        StoreMember {
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "fixture-access-key".into(),
                principal: coven_format::S3Principal::Console,
            },
            keys: ben_keys,
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
            key_hash: [17; 32],
        },
    );
    replay.state.circles.insert(
        circle,
        StoreCircle {
            name: "Gifts".into(),
            members: [ana].into(),
        },
    );
    replay.entries.insert(create.position, EntryOutcome::Kept);
    history.push((create, replay.clone()));
    let add = entry(
        4,
        StoreChange::AddCircleMember {
            circle,
            member: ben,
        },
    );
    replay
        .state
        .circles
        .get_mut(&circle)
        .unwrap()
        .members
        .insert(ben);
    replay.entries.insert(add.position, EntryOutcome::Kept);
    history.push((add, replay.clone()));
    let mut delete = entry(1, StoreChange::DeleteCircle { circle });
    delete.author = ben;
    delete.position.device = DeviceId(u64::MAX - 1);
    delete.timestamp = Timestamp::new(6, 0, delete.position.device).unwrap();
    delete.had_read = EntryPositions(vec![history[3].0.position]);
    let gifts = replay.state.circles.get_mut(&circle).unwrap();
    gifts.members.clear();
    replay
        .state
        .circle_deletions
        .insert(circle, delete.position);
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
            db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
                .await?;
        }
    }
    let (entry, replay) = history.last().unwrap();
    db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
        .await
}

/// Drop `delete_circle`'s deletion: Ana's higher-tier removal of Ben, its
/// author, beats it and the circle's rows return (§14.7).
pub(crate) async fn drop_circle_deletion(db: &Database, circle: CircleId) -> Result<(), DbError> {
    let history = circle_history(circle);
    let mut removal = history[0].0.clone();
    removal.position.number = 5;
    removal.timestamp = Timestamp::new(5, 0, removal.position.device).unwrap();
    removal.change = StoreChange::RemoveMember { member: member(2) };
    let mut replay = history[3].1.clone();
    replay.state.members.get_mut(&member(2)).unwrap().removed = true;
    replay
        .state
        .circles
        .get_mut(&circle)
        .unwrap()
        .members
        .remove(&member(2));
    replay.entries.insert(removal.position, EntryOutcome::Kept);
    replay.entries.insert(
        history[4].0.position,
        EntryOutcome::Dropped(crate::DropReason::BeatenBy(removal.position)),
    );
    db.apply_store_log::<PendingRecord>(checked(removal), replay, Vec::new())
        .await
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
        db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
            .await
            .unwrap();
    }
    store.join_circle(&db, circle);
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
    db.inspect_writer(|sql| sql.fail_at("refuse", "BEFORE DELETE ON notes", "keep circle"));
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(checked(delete.clone()), deleted.clone(), Vec::new())
            .await,
        Err(DbError::Sqlite(_))
    ));
    assert_eq!(db.store_log().await.unwrap(), before);
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(count(&db, "children"), 1);
    assert!(!db
        .store_log()
        .await
        .unwrap()
        .replay
        .state
        .circle_deletions
        .contains_key(&circle));
    assert!(!rows.is_marked_for_rerun());
    assert!(!losses.is_marked_for_rerun());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    db.apply_store_log::<PendingRecord>(checked(delete.clone()), deleted.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(next(&mut rows).await, (0, 0));
    assert_eq!(next(&mut losses).await.len(), 2);
    assert_eq!(db.store_log().await.unwrap().replay, deleted);
    assert!(db
        .store_log()
        .await
        .unwrap()
        .replay
        .state
        .circle_deletions
        .contains_key(&circle));

    // §9: Ana's earlier concurrent removal of Ben beats Ben's deletion.
    let remove = entry(5, StoreChange::RemoveMember { member: member(2) });
    let mut restored = history[3].1.clone();
    let gifts = restored.state.circles.get_mut(&circle).unwrap();
    gifts.members = [member(1)].into();
    restored.state.members.get_mut(&member(2)).unwrap().removed = true;
    restored.entries.insert(remove.position, EntryOutcome::Kept);
    restored.entries.insert(
        delete.position,
        EntryOutcome::Dropped(DropReason::BeatenBy(remove.position)),
    );
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| sql.fail_at("refuse", "BEFORE INSERT ON notes", "refuse restoration"));
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(checked(remove.clone()), restored.clone(), Vec::new())
            .await,
        Err(DbError::Sqlite(_))
    ));
    assert_eq!(db.store_log().await.unwrap(), before);
    assert!(db
        .store_log()
        .await
        .unwrap()
        .replay
        .state
        .circle_deletions
        .contains_key(&circle));
    assert_eq!(count(&db, "notes"), 0);
    assert!(!rows.is_marked_for_rerun());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER refuse").unwrap());
    db.apply_store_log::<PendingRecord>(checked(remove.clone()), restored.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(next(&mut rows).await, (1, 1));
    assert!(next(&mut losses).await.is_empty());
    assert!(!db
        .store_log()
        .await
        .unwrap()
        .replay
        .state
        .circle_deletions
        .contains_key(&circle));
    assert_eq!(records(&db).len(), 1, "recomputation creates no app write");
    let committed = db.store_log().await.unwrap();
    assert_eq!(committed.replay, restored);
    assert_eq!(committed.entries[4], checked(remove));
    assert_eq!(committed.entries[5], checked(delete));
    db.close().await.unwrap();
    let db = open(&store).await;
    assert_eq!(db.store_log().await.unwrap(), committed);
    assert_eq!(count(&db, "notes"), 1);
    store.join_circle(&db, circle);
    sql(&db, "UPDATE notes SET title='restored'").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn circle_deletion_keeps_a_queued_copy_until_its_upload_completes() {
    use crate::file_write::tests::{complete_uploads, owned_paths};
    use crate::{CacheFill, FileDecl, Provenance};

    let store = TestStore::new();
    let circle = CircleId(uuid::Uuid::from_u128(2));
    let declaration = SyncedTable::new("files", RowIdentity::IndependentUuid)
        .audience_column("audience")
        .carries_files(FileDecl::new(
            "files",
            Provenance::AppProvided,
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
        db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
            .await
            .unwrap();
    }
    store.join_circle(&db, circle);
    db.write_with_files::<_, _, _, crate::DbError>(
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
    let (entry, replay) = history[4].clone();
    db.apply_store_log::<PendingRecord>(checked(entry), replay.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    // The deleted circle hides the row; its file still uploads from its copy.
    assert_eq!(count(&db, "files"), 0);
    assert_eq!(count(&db, "_coven_device_files"), 1);
    assert_eq!(owned_paths(&store), paths);
    complete_uploads(&db).await;
    assert_eq!(count(&db, "_coven_device_files"), 0);
    assert_eq!(count(&db, "_coven_file_removals"), 0);
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
        db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
            .await
            .unwrap();
    }
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| {
        sql.fail_at(
            "_coven_refuse",
            "BEFORE INSERT ON _coven_circles",
            "state failed",
        )
    });
    let (entry, replay) = history[2].clone();
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
            .await,
        Err(DbError::Sqlite(_))
    ));
    assert_eq!(db.store_log().await.unwrap(), before);
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER _coven_refuse").unwrap());
    db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
        .await
        .unwrap();
    db.apply_store_log::<PendingRecord>(checked(entry), replay.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    db.close().await.unwrap();
}

#[tokio::test]
async fn stale_results_and_changed_entry_bytes_cannot_replace_the_applied_set() {
    let store = TestStore::new();
    let db = open(&store).await;
    let history = circle_history(CircleId(uuid::Uuid::from_u128(2)));
    let (first, initial) = &history[0];
    db.apply_store_log::<PendingRecord>(checked(first.clone()), initial.clone(), Vec::new())
        .await
        .unwrap();
    let mut changed = first.clone();
    changed.timestamp = Timestamp::new(2, 0, changed.position.device).unwrap();
    assert!(
        matches!(db.apply_store_log::<PendingRecord>(checked(changed), initial.clone(), Vec::new()).await, Err(DbError::StoreLogEntryChanged(id)) if id==first.position)
    );
    let mut changed_view = checked(first.clone());
    changed_view.check = StoreLogCheck::NotAllowed;
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(changed_view, initial.clone(), Vec::new()).await,
        Err(DbError::StoreLogEntryChanged(id)) if id == first.position
    ));
    let mut changed_time = checked(first.clone());
    changed_time.stored_at += Duration::from_millis(1);
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(changed_time, initial.clone(), Vec::new()).await,
        Err(DbError::StoreLogEntryChanged(id)) if id == first.position
    ));
    let (second, next) = &history[1];
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(checked(second.clone()), initial.clone(), Vec::new())
            .await,
        Err(DbError::StoreLogEntriesChanged)
    ));
    db.apply_store_log::<PendingRecord>(checked(second.clone()), next.clone(), Vec::new())
        .await
        .unwrap();
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(checked(first.clone()), initial.clone(), Vec::new())
            .await,
        Err(DbError::StoreLogEntriesChanged)
    ));
    assert_eq!(db.store_log().await.unwrap().replay, *next);
    db.close().await.unwrap();
}

#[tokio::test]
async fn late_verdict_and_first_publication_commit_together_and_cannot_be_reversed() {
    let store = TestStore::new();
    let db = open(&store).await;
    let (creation, mut result) = circle_history(CircleId(uuid::Uuid::from_u128(2)))[0].clone();
    db.apply_store_log::<PendingRecord>(checked(creation.clone()), result.clone(), Vec::new())
        .await
        .unwrap();
    let late = ReplayEntry {
        entry: entry(
            2,
            StoreChange::AddMember {
                member: member(2),
                keys: keys(2),
                role: MemberRole::Member,
                access: coven_format::MemberAccess::S3AccessKey {
                    access_key_id: "late-access".into(),
                    principal: coven_format::S3Principal::Console,
                },
            },
        ),
        stored_at: coven_format::value::StorageTime::new(32 * 86400, 987_654_321).unwrap(),
        check: StoreLogCheck::LandedTooLate { authorized: true },
    };
    result.entries.insert(
        late.entry.position,
        EntryOutcome::Dropped(DropReason::LandedTooLate),
    );
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| {
        sql.fail_at(
            "_coven_fail",
            "BEFORE INSERT ON _coven_members",
            "replay failed",
        )
    });
    assert!(db
        .apply_store_log::<PendingRecord>(late.clone(), result.clone(), Vec::new())
        .await
        .is_err());
    assert_eq!(db.store_log().await.unwrap(), before);
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER _coven_fail").unwrap());
    db.apply_store_log::<PendingRecord>(late.clone(), result.clone(), Vec::new())
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = open(&store).await;
    let saved = db.store_log().await.unwrap();
    assert_eq!(saved.entries[1], late);
    assert_eq!(saved.replay, result);
    let mut reversed = result.clone();
    reversed
        .entries
        .insert(late.entry.position, EntryOutcome::Kept);
    assert!(db
        .apply_store_log::<PendingRecord>(checked(creation), reversed, Vec::new())
        .await
        .is_err());
    assert_eq!(db.store_log().await.unwrap(), saved);
    let mut changed = late.clone();
    changed.check = StoreLogCheck::Allowed;
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(changed, result.clone(), Vec::new())
            .await,
        Err(DbError::StoreLogEntryChanged(_))
    ));
    let mut changed = late;
    changed.stored_at += Duration::from_millis(1);
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(changed, result, Vec::new())
            .await,
        Err(DbError::StoreLogEntryChanged(_))
    ));
    assert_eq!(db.store_log().await.unwrap(), saved);
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
    replay.state.members.get_mut(&member(2)).unwrap().removed = true;
    replay
        .state
        .devices
        .get_mut(&DeviceId(u64::MAX))
        .unwrap()
        .state = DeviceState::Removed;
    // A replaced id keeps its member and name with its own state (§10, E9).
    replay.state.devices.insert(
        DeviceId(u64::MAX - 1),
        StoreDevice {
            member: member(2),
            name: "Restored".into(),
            state: DeviceState::Replaced,
        },
    );
    for (index, reason) in [
        DropReason::TargetGone(EntryTarget::Member(member(2))),
        DropReason::TargetGone(EntryTarget::Device(DeviceId(u64::MAX))),
        DropReason::TargetGone(EntryTarget::Circle(circle)),
        DropReason::NoAdminLeft,
        DropReason::NotAllowed,
        DropReason::BeatenBy(expected.entries[0].entry.position),
    ]
    .into_iter()
    .enumerate()
    {
        let e = entry(
            index as u64 + 5,
            StoreChange::ChangeRole {
                member: member(2),
                role: MemberRole::Member,
            },
        );
        replay
            .entries
            .insert(e.position, EntryOutcome::Dropped(reason));
        db.apply_store_log::<PendingRecord>(checked(e.clone()), replay.clone(), Vec::new())
            .await
            .unwrap();
        expected.entries.push(checked(e));
    }
    let snapshot = SnapshotId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
        audience: Audience::Store,
    };
    let schema = entry(
        11,
        StoreChange::RaiseSchema {
            version: u32::MAX,
            snapshot: snapshot.clone(),
            caught_at: coven_format::value::StorageTime::UNIX_EPOCH,
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
    db.apply_store_log::<PendingRecord>(checked(schema.clone()), replay.clone(), Vec::new())
        .await
        .unwrap();
    expected.entries.push(checked(schema));
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
        db.apply_store_log::<PendingRecord>(checked(reset.clone()), replay.clone(), Vec::new())
            .await
            .unwrap();
        expected.entries.push(checked(reset));
    }
    expected.entries.sort_by_key(|e| e.entry.timestamp);
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
    db.apply_store_log::<PendingRecord>(checked(first), initial, Vec::new())
        .await
        .unwrap();
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
    db.apply_store_log::<PendingRecord>(checked(second), next.clone(), Vec::new())
        .await
        .unwrap();
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
        db.apply_store_log::<PendingRecord>(checked(entry.clone()), replay.clone(), Vec::new())
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
            key_hash: [17; 32],
        },
    );
    replay.state.circles.insert(
        notes,
        StoreCircle {
            name: "Notes".into(),
            members: [member(1)].into(),
        },
    );
    replay.entries.insert(create.position, EntryOutcome::Kept);
    db.apply_store_log::<PendingRecord>(checked(create), replay.clone(), Vec::new())
        .await
        .unwrap();
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
                caught_at: coven_format::value::StorageTime::UNIX_EPOCH,
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
        db.apply_store_log::<PendingRecord>(checked(schema), replay.clone(), Vec::new())
            .await
            .unwrap();
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
            caught_at: coven_format::value::StorageTime::UNIX_EPOCH,
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
    db.inspect_writer(|sql| {
        sql.fail_at(
            "_coven_refuse",
            "BEFORE INSERT ON _coven_boundaries WHEN NEW.version=13",
            "refuse circle raise",
        )
    });
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(checked(raised.clone()), replay.clone(), Vec::new())
            .await,
        Err(DbError::Sqlite(_))
    ));
    assert_eq!(db.store_log().await.unwrap(), before);
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER _coven_refuse").unwrap());
    db.apply_store_log::<PendingRecord>(checked(raised), replay.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    db.close().await.unwrap();
    let db = open(&store).await;
    assert_eq!(db.store_log().await.unwrap().replay, replay);
    db.close().await.unwrap();
}

#[tokio::test]
async fn store_identity_has_no_duplicate_table_and_boundaries_require_snapshots() {
    let store = TestStore::new();
    let db = open(&store).await;
    let (entry, replay) = circle_history(CircleId(uuid::Uuid::from_u128(2)))[0].clone();
    db.apply_store_log::<PendingRecord>(checked(entry), replay, Vec::new())
        .await
        .unwrap();
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| {
        assert_eq!(
            sql.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name IN ('_coven_store','_coven_one_store')",
                [],
                |row| row.get::<_, u32>(0),
            ).unwrap(),
            0,
            "creation is the sole persisted store identity"
        );
        assert!(
            sql.internal_execute(
                "INSERT INTO _coven_boundaries(kind,audience,version) VALUES('schema','store',2)",
                [],
            )
            .is_err(),
            "a version must name its snapshot and raise entry"
        );
        assert!(
            sql.internal_execute(
                "INSERT INTO _coven_boundaries(kind,audience) VALUES('reset','store')",
                [],
            )
            .is_err(),
            "a reset must name its snapshot"
        );
    });
    assert_eq!(db.store_log().await.unwrap(), before);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_drop_report_rolls_back_consumption_and_replay() {
    use crate::PendingValue;
    let store = TestStore::new();
    let db = open(&store).await;
    let (creation, mut result) = circle_history(CircleId(uuid::Uuid::from_u128(2)))[0].clone();
    db.apply_store_log::<PendingRecord>(checked(creation), result.clone(), Vec::new())
        .await
        .unwrap();
    let own = db.local_store_log().await.unwrap().device;
    let mut change = entry(
        1,
        StoreChange::AddDevice {
            device: own,
            name: "delayed device".into(),
            replaces: None,
        },
    );
    change.position.device = own;
    change.timestamp = Timestamp::new(2, 0, own).unwrap();
    change.had_read = db.store_log().await.unwrap().positions();
    let late = ReplayEntry {
        entry: change,
        stored_at: coven_format::value::StorageTime::since_epoch(Duration::from_secs(32 * 86400)),
        check: StoreLogCheck::LandedTooLate { authorized: true },
    };
    result.entries.insert(
        late.entry.position,
        EntryOutcome::Dropped(DropReason::LandedTooLate),
    );
    let before = db.store_log().await.unwrap();
    db.inspect_writer(|sql| {
        sql.fail_at(
            "fail_report",
            "BEFORE INSERT ON _coven_pending",
            "drop report failed",
        )
    });
    let failed = db
        .apply_store_log::<PendingRecord>(late.clone(), result.clone(), Vec::new())
        .await;
    assert!(
        matches!(failed, Err(DbError::PendingPersistence(_))),
        "{failed:?}"
    );
    db.close().await.unwrap();
    let db = open(&store).await;
    assert_eq!(db.store_log().await.unwrap(), before);
    assert!(db.pending::<PendingRecord>().await.unwrap().is_empty());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER fail_report").unwrap());
    let mut live = db.subscribe_pending::<PendingRecord>();
    assert!(live.next().await.unwrap().is_empty());
    db.apply_store_log::<PendingRecord>(late.clone(), result.clone(), Vec::new())
        .await
        .unwrap();
    let expected = [PendingRecord::dropped_entry(
        late.entry.position,
        DropReason::LandedTooLate,
    )];
    assert_eq!(live.next().await.unwrap(), expected);
    db.apply_store_log::<PendingRecord>(late, result.clone(), Vec::new())
        .await
        .unwrap();
    assert_eq!(db.pending::<PendingRecord>().await.unwrap(), expected);
    assert_eq!(db.store_log().await.unwrap().replay, result);
    db.close().await.unwrap();
}

#[test]
fn finality_requires_a_known_entry_strictly_before_the_certified_horizon() {
    let entry = coven_format::test_utils::store_log();
    let id = entry.position;
    let unknown = EntryId {
        number: id.number + 1,
        ..id
    };
    for dropped in [false, true] {
        let mut log = StoreLog {
            entries: vec![crate::ReplayEntry {
                entry: entry.clone(),
                stored_at: coven_format::value::StorageTime::since_epoch(Duration::from_secs(1)),
                check: crate::StoreLogCheck::Allowed,
            }],
            replay: StoreLogReplay {
                entries: [(
                    id,
                    if dropped {
                        EntryOutcome::Dropped(DropReason::NotAllowed)
                    } else {
                        EntryOutcome::Kept
                    },
                )]
                .into(),
                ..StoreLogReplay::default()
            },
            horizon: None,
        };
        let at = |since| Some(coven_format::value::StorageTime::since_epoch(since));
        for (horizon, expected) in [
            (None, false),
            (at(Duration::ZERO), false),
            (at(Duration::from_secs(1) - Duration::from_nanos(1)), false),
            (at(Duration::from_secs(1)), false),
            (at(Duration::from_secs(1) + Duration::from_nanos(1)), true),
            (at(Duration::MAX), true),
        ] {
            log.horizon = horizon;
            assert_eq!(log.is_final(id), expected, "{horizon:?}, dropped={dropped}");
            assert!(!log.is_final(unknown));
        }
    }
}
