//! What a covered object's deletion waits for, and its retention record
//! (§9, §15): every entry the decision depends on final, then every active
//! reader's post or thirty storage days.

use super::retention::post;
use super::*;
use crate::store_log_sync::snapshots::retention::waits::{reached_age, Facts};
use coven_format::value::{EntryId, StorageTime, WritePositions};
use serde_json::{json, Value};

const DAY: u64 = 86400;

fn at(days: u64, less: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(days * DAY - less)
}

/// This device's retention records, by path.
async fn records(device: &Device) -> Vec<(ObjectPath, crate::PendingReason)> {
    device
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|record| match record.subject {
            crate::PendingSubject::Retention { path } => Some((path, record.reason)),
            _ => None,
        })
        .collect()
}

async fn logs(storage: &MemoryStorage) -> Vec<ObjectPath> {
    storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path)
        .collect()
}

fn waits(
    path: &ObjectPath,
    prerequisite: crate::Prerequisite,
) -> (ObjectPath, crate::PendingReason) {
    (path.clone(), crate::PendingReason::Waits(prerequisite))
}

/// Ben removes Ana's old phone and may delete its covered log, but only once
/// his removal is final: until then a later arrival could drop it and make
/// the phone's objects its own again (§9, §15).
#[tokio::test]
async fn a_removed_devices_member_deletes_its_covered_logs_once_the_removal_is_final() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
    b.sync.reload_from_snapshots().await.unwrap();
    let removal = b
        .sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: a.device().await,
        })
        .await
        .unwrap();
    post(&b, &storage).await;
    b.sync.run_retention().await.unwrap();
    let log = logs(&storage).await;
    assert_eq!(log.len(), 1);
    assert_eq!(
        records(&b).await,
        [waits(&log[0], crate::Prerequisite::EntryFinality(removal))]
    );
    storage_clock.set(at(31, 0));
    b.clock.set(at(31, 0));
    b.sync().await;
    b.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
    assert!(records(&b).await.is_empty());
}

#[tokio::test]
/// On Google Drive the store owner cannot delete another account's file, but
/// its delete takes the file out of the store's folder, where it stays in the
/// uploader's own account (§15): the folder no longer lists it. The owner
/// deletes once the member's removal is final.
async fn removed_member_logs_belong_to_the_store_owner_and_leave_google_drives_folder() {
    for google in [false, true] {
        let (storage, storage_clock) = if google {
            let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
            let storage = MemoryStorage::builder()
                .provider(coven_storage::CloudProvider::GoogleDrive)
                .clock(clock.clone())
                .build()
                .unwrap();
            (Arc::new(storage), clock)
        } else {
            timed_snapshot_storage()
        };
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let mut b = device(storage.clone(), 2, member(2), store(1)).await;
        b.reopen(storage.clone(), notes_tables(), notes_migrations())
            .await;
        let mut c = device(storage.clone(), 3, member(3), store(1)).await;
        c.reopen(storage.clone(), notes_tables(), notes_migrations())
            .await;
        a.add(&b.member, MemberRole::Member).await;
        // An admin whose account does not hold the store cannot delete or
        // detach another member's objects.
        if google {
            a.sync
                .make_and_upload_entry(StoreChange::AddMember {
                    access: coven_format::MemberAccess::ProviderAccount("carol@example.com".into()),
                    member: c.member.member(),
                    keys: c.member.keys().public_keys(),
                    role: MemberRole::Admin,
                })
                .await
                .unwrap();
        } else {
            a.add(&c.member, MemberRole::Admin).await;
        }
        add_device(&mut b).await;
        add_device(&mut c).await;
        a.sync().await;
        write_rows(&b, 0, 1, 17, Audience::Store).await;
        a.db.apply_downloaded::<crate::PendingRecord>(
            b.db.test_queued_writes().await.unwrap().remove(0).into(),
            coven_database::test_utils::published(coven_format::value::StorageTime::UNIX_EPOCH),
        )
        .await
        .unwrap();
        upload(&b, &storage).await;
        a.write_snapshot(Audience::Store).await.unwrap();
        a.sync
            .make_and_upload_entry(StoreChange::RemoveMember {
                member: b.member.member(),
            })
            .await
            .unwrap();
        c.sync().await;
        for device in [&a, &c] {
            device
                .clock
                .set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
        }
        storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
        c.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()
                .len(),
            1
        );
        // The owner's catch-up makes the day-0 removal final (§9).
        a.sync().await;
        a.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()
                .len(),
            0,
            "google {google}"
        );
    }
}

/// §15: when coverage and finality pass, the record names the first active
/// reader by device id that has not posted past the object. A missing post
/// and an invalid one are no position. Thirty storage days end the wait at
/// equality, not a second before.
#[tokio::test]
async fn the_first_lagging_reader_names_the_wait_until_thirty_storage_days() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
    post(&a, &storage).await;
    a.sync.run_retention().await.unwrap();
    let log = logs(&storage).await;
    let ben = b.device().await;
    let carol = c.device().await;
    assert!(ben < carol);
    assert_eq!(
        records(&a).await,
        [waits(&log[0], crate::Prerequisite::Positions(ben))]
    );
    b.sync().await;
    b.sync.reload_from_snapshots().await.unwrap();
    post(&b, &storage).await;
    c.sync().await;
    c.sync.reload_from_snapshots().await.unwrap();
    post(&c, &storage).await;
    // Carol's post loses its signature: it is no position.
    let path = ObjectPath::positions(store(1), carol);
    let bytes = storage.read(&path).await.unwrap();
    let unsigned = coven_format::sealed_single::SingleChunkObject::decode(&bytes)
        .unwrap()
        .signed_bytes()
        .unwrap();
    storage.replace(&path, &unsigned).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert_eq!(
        records(&a).await,
        [waits(&log[0], crate::Prerequisite::Positions(carol))]
    );
    // The write was stored at time zero.
    for (less, deleted) in [(1, false), (0, true)] {
        storage_clock.set(at(30, less));
        a.clock.set(at(30, less));
        a.sync.run_retention().await.unwrap();
        assert_eq!(logs(&storage).await.is_empty(), deleted, "{less}");
    }
    assert!(records(&a).await.is_empty());
}

/// Ana removed Ben's phone, which never posted. While that removal is not
/// final a later arrival could restore him as a reader who needs her write,
/// so the write waits on it; once final, a later covered write goes with
/// Ana's own post alone (§9, §15).
#[tokio::test]
async fn a_silent_readers_provisional_removal_holds_deletion_until_final() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    let removal = a
        .sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: b.device().await,
        })
        .await
        .unwrap();
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
    post(&a, &storage).await;
    a.sync.run_retention().await.unwrap();
    let first = logs(&storage).await;
    assert_eq!(
        records(&a).await,
        [waits(
            &first[0],
            crate::Prerequisite::EntryFinality(removal)
        )]
    );
    storage_clock.set(at(31, 0));
    a.clock.set(at(31, 0));
    a.sync().await;
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.catch_up().await;
    a.write_snapshot(Audience::Store).await.unwrap();
    post(&a, &storage).await;
    a.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
    assert!(records(&a).await.is_empty());
}

/// A reset decides which store snapshot is current. Logs go once the newest
/// snapshot covers them under every boundary replay could still select, so
/// the reset's fate does not hold them; Ana's own snapshot that the newest
/// supersedes waits until the reset is final, and its record names it (§15).
#[tokio::test]
async fn a_provisional_reset_holds_a_superseded_own_snapshot_but_not_covered_logs() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let named = a.write_snapshot(Audience::Store).await.unwrap();
    let reset = a
        .sync
        .make_and_upload_entry(StoreChange::Reset { snapshot: named })
        .await
        .unwrap();
    a.sync.reload_from_snapshots().await.unwrap();
    let mut superseded = Vec::new();
    for row in 1..3 {
        write_rows(&a, row, 1, 17, Audience::Store).await;
        upload(&a, &storage).await;
        a.catch_up().await;
        superseded.push(a.write_snapshot(Audience::Store).await.unwrap());
    }
    let superseded =
        crate::store_log_sync::snapshots::snapshot_path(store(1), &superseded[0]).unwrap();
    post(&a, &storage).await;
    a.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
    assert!(storage.read(&superseded).await.is_ok());
    assert_eq!(
        records(&a).await,
        [waits(
            &superseded,
            crate::Prerequisite::EntryFinality(reset)
        )]
    );
    storage_clock.set(at(31, 0));
    a.clock.set(at(31, 0));
    a.sync().await;
    a.sync.run_retention().await.unwrap();
    assert!(storage.read(&superseded).await.is_err());
    assert!(records(&a).await.is_empty());
}

/// storelog-data `RetentionSafety.work` and `mayDelete`: on every
/// combination of dependency finality, reader posts and storage age, the
/// record names the model's first unmet condition and deletion happens
/// exactly when the model allows.
#[test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
fn data_differential_retention_record() {
    let entry = |number| EntryId {
        device: DeviceId(1),
        number,
    };
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for finals in 0..8u32 {
        for count in 0..=2u64 {
            for posts in 0..8u32 {
                for aged in [false, true] {
                    let facts = Facts {
                        dependencies: (1..=count)
                            .map(|n| (entry(n * 2), finals & (1 << n) != 0))
                            .collect(),
                        readers: (1..=3)
                            .map(|d| (DeviceId(d), posts & (1 << (d - 1)) != 0))
                            .collect(),
                        aged,
                    };
                    cases.push(json!({
                        "entries": facts.dependencies.iter().map(|(e, last)| json!([e.number, last])).collect::<Vec<_>>(),
                        "readers": facts.readers.iter().map(|(d, posted)| json!([d.0, posted])).collect::<Vec<_>>(),
                        "aged": aged,
                    }));
                    let unmet = facts.first_unmet();
                    actual.push(json!({
                        "delete": unmet.is_none(),
                        "record": match unmet {
                            Some(crate::Prerequisite::EntryFinality(e)) => json!(["entryFinality", e.number]),
                            Some(crate::Prerequisite::Positions(d)) => json!(["positions", d.0]),
                            Some(other) => panic!("{other:?}"),
                            None => Value::Null,
                        },
                    }));
                }
            }
        }
    }
    let output = crate::tests::run_lean("COVEN_STORELOG_DATA_LEAN", &json!({ "retention": cases }));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lean: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual, lean);
}

/// IO `Deletable` on the catalog's current snapshot: coverage, finality,
/// every active reader's posted position, or thirty storage days counting
/// at equality.
#[test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
fn io_differential_deletable() {
    const MONTH: u64 = 30 * DAY;
    let write = coven_merge::WriteId {
        device: DeviceId(0),
        number: 2,
    };
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for covered in [false, true] {
        for last in [false, true] {
            for posted in [[2, 2], [2, 1], [0, 3], [1, 0]] {
                for (stored, time) in [
                    (0, 0),
                    (0, MONTH - 1),
                    (0, MONTH),
                    (5, MONTH + 5),
                    (5, MONTH + 4),
                    (7, 2 * MONTH),
                ] {
                    let positions: std::collections::BTreeMap<_, _> = posted
                        .iter()
                        .enumerate()
                        .filter(|(_, number)| **number > 0)
                        .map(|(reader, number)| {
                            (
                                DeviceId(reader as u64 + 1),
                                WritePositions(vec![coven_merge::WriteId {
                                    device: DeviceId(0),
                                    number: *number,
                                }]),
                            )
                        })
                        .collect();
                    let facts = Facts {
                        dependencies: vec![(
                            EntryId {
                                device: DeviceId(1),
                                number: 1,
                            },
                            last,
                        )],
                        readers: (1..=2)
                            .map(|d| {
                                let device = DeviceId(d);
                                (
                                    device,
                                    positions.get(&device).is_some_and(|p| p.covers(write)),
                                )
                            })
                            .collect(),
                        aged: reached_age(
                            Some(StorageTime::new(time, 0).unwrap()),
                            StorageTime::new(stored, 0).unwrap(),
                        ),
                    };
                    actual.push(json!(covered && facts.first_unmet().is_none()));
                    cases.push(json!({
                        "check": "deletable", "number": write.number, "covered": covered,
                        "final": last, "posted": [[1, posted[0]], [2, posted[1]]],
                        "stored": stored, "time": time,
                    }));
                }
            }
        }
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}
