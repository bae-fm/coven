//! What a covered object's deletion waits for, and its retention record
//! (§9, §15): every entry the decision depends on final, then every active
//! reader's post or thirty storage days.

use super::retention::post;
use super::*;
use crate::store_log_sync::snapshots::retention::waits::{reached_age, Facts};
use coven_format::value::{EntryId, StorageTime, WritePositions};
use coven_storage::test_utils::MemoryRequest;
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
/// the entries deciding the phone's state are final: until then a later
/// arrival could drop his removal and make the phone's objects its own
/// again. The phone registered itself by creating the store, so the record
/// names that older entry first (§9, §15).
#[tokio::test]
async fn a_removed_devices_member_deletes_its_covered_logs_once_the_removal_is_final() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    let creation = a.create(key(1)).await;
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
    mature(&storage_clock, &[&b]).await;
    b.sync.run_retention().await.unwrap();
    let log = logs(&storage).await;
    assert_eq!(log.len(), 1);
    assert!(creation < removal);
    assert_eq!(
        records(&b).await,
        [waits(&log[0], crate::Prerequisite::EntryFinality(creation))]
    );
    storage_clock.set(at(31, 0));
    b.clock.set(at(31, 0));
    b.sync().await;
    let before = storage.request_trace().len();
    b.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
    assert!(records(&b).await.is_empty());
    // Ben never applied the phone's write, which the snapshot covered: he
    // read its body for the files it names before deleting it, so they
    // became candidates with its deletion (§16.5).
    let trace = storage.request_trace()[before..].to_vec();
    let read = trace
        .iter()
        .position(|r| matches!(r, MemoryRequest::Read(p, _) if *p == log[0]))
        .expect("the write's references were checked");
    let deleted = trace
        .iter()
        .position(|r| matches!(r, MemoryRequest::Delete(p) if *p == log[0]))
        .unwrap();
    assert!(read < deleted);
}

/// Ben found the write of Ana's removed phone unproven while one of its
/// parts did not open for him. Once that part opens, deleting the write reads
/// it again, so the files it names become candidates with its deletion
/// (§16.5; storelog-data `stale_unproven_counterexample`).
#[tokio::test]
async fn a_stale_unproven_write_is_read_again_before_it_goes() {
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
    b.sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: a.device().await,
        })
        .await
        .unwrap();
    post(&b, &storage).await;
    let object = storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .remove(0);
    crate::object_input::ObjectInput::observe(&b.db, &object)
        .await
        .unwrap()
        .keep_files(coven_database::FileEvidence::Unproven(
            coven_database::UnprovenFiles::Parts([(Audience::Store, key(1))].into()),
        ))
        .await
        .unwrap();
    storage_clock.set(at(31, 0));
    b.clock.set(at(31, 0));
    b.sync().await;
    mature(&storage_clock, &[&b]).await;
    let size = storage.read(&object.path).await.unwrap().len() as u64;
    let before = storage.request_trace().len();
    b.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
    // Its parts were read to their end before it went, not just its header.
    let trace = storage.request_trace()[before..].to_vec();
    let read = trace
        .iter()
        .position(|r| {
            matches!(r, MemoryRequest::Read(p, Some(range)) if *p == object.path && range.end() == size)
        })
        .expect("the unproven write was read again");
    let deleted = trace
        .iter()
        .position(|r| matches!(r, MemoryRequest::Delete(p) if *p == object.path))
        .unwrap();
    assert!(read < deleted);
}

/// Ben cannot open the header of the removed phone's write 1: its store key
/// has not reached him. That write waits with its record, while the phone's
/// covered write 2 goes once the removal is final: a wait holds only the
/// waiting write (§15; io `Retention.Eligible` judges each write alone).
#[tokio::test]
async fn a_write_waiting_for_its_key_holds_only_itself() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    for row in 0..2 {
        write_rows(&a, row, 1, 17, Audience::Store).await;
        upload(&a, &storage).await;
    }
    a.write_snapshot(Audience::Store).await.unwrap();
    let log = logs(&storage).await;
    let bytes = storage.read(&log[0]).await.unwrap();
    let length = coven_format::sealed_write::WriteObjectPrefix::length(&bytes).unwrap();
    let mut prefix =
        coven_format::sealed_write::WriteObjectPrefix::decode(&bytes[..length]).unwrap();
    prefix.store_key = key(9);
    let mut waiting = prefix.encode().unwrap();
    waiting.extend_from_slice(&bytes[length..]);
    republish(&storage, &storage_clock, &log[0], &waiting).await;
    b.sync.reload_from_snapshots().await.unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: a.device().await,
        })
        .await
        .unwrap();
    post(&b, &storage).await;
    storage_clock.set(at(31, 0));
    b.clock.set(at(31, 0));
    b.sync().await;
    mature(&storage_clock, &[&b]).await;
    b.sync.run_retention().await.unwrap();
    assert_eq!(logs(&storage).await, log[..1]);
    assert_eq!(
        records(&b).await,
        [(
            log[0].clone(),
            crate::PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key: key(9),
            }
        )]
    );
}

#[tokio::test]
/// §15: a removed member's log objects are deleted by a device of the member
/// whose provider account holds the store, once the removal is final; another
/// admin's device leaves them.
async fn removed_member_logs_belong_to_the_store_owner() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    b.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    c.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    a.add(&b.member, MemberRole::Member).await;
    a.add(&c.member, MemberRole::Admin).await;
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
        0
    );
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
    mature(&storage_clock, &[&a]).await;
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
        observe(&a).await;
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
    mature(&storage_clock, &[&a]).await;
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
    mature(&storage_clock, &[&a]).await;
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
    mature(&storage_clock, &[&a]).await;
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

/// The audiences this device has asked to cover a due write (§15).
async fn coverage_requests(device: &Device) -> Vec<Audience> {
    device
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|record| match Data::read(&record).unwrap() {
            Data::Snapshots(SnapshotTask {
                job:
                    SnapshotJob::Write {
                        audience,
                        trigger: crate::snapshot_data::SnapshotTrigger::Coverage { .. },
                        ..
                    },
                ..
            }) => Some(audience),
            _ => None,
        })
        .collect()
}

async fn snapshots(storage: &MemoryStorage) -> Vec<ObjectPath> {
    storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path)
        .collect()
}

/// §15: Ana's only write is far below the growth threshold and Ben never
/// posts. Once storage has held it thirty days, retention asks for the
/// store's first snapshot; that snapshot lets the write go
/// (io `first_snapshot_deleted`).
#[tokio::test]
async fn an_old_write_below_the_growth_threshold_requests_the_first_snapshot() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    post(&a, &storage).await;
    a.sync.write_snapshots().await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert!(snapshots(&storage).await.is_empty());
    assert!(coverage_requests(&a).await.is_empty());
    storage_clock.set(at(30, 0));
    a.clock.set(at(30, 0));
    observe(&a).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(coverage_requests(&a).await, [Audience::Store]);
    // The same due write asks once.
    a.sync.run_retention().await.unwrap();
    assert_eq!(coverage_requests(&a).await, [Audience::Store]);
    a.write_snapshots().await.unwrap();
    assert_eq!(snapshots(&storage).await.len(), 1);
    assert!(a.db.operations().await.unwrap().is_empty());
    // The write goes once that first snapshot is a storage day old.
    a.sync.run_retention().await.unwrap();
    assert_eq!(logs(&storage).await.len(), 1);
    mature(&storage_clock, &[&a]).await;
    a.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
}

/// §15: an older snapshot covered Ana's write, but Ben's newer one, with
/// more writes, does not: he prepared it before her write existed. Retention
/// asks again. Ben's later snapshot covering it lands before the request
/// runs: the request then ends without publishing, and the write goes.
#[tokio::test]
async fn coverage_a_newer_snapshot_leaves_out_is_requested_until_supplied() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    // Ben's writes stay local until his snapshot of them is prepared.
    for row in 1..4 {
        write_rows(&b, row, 1, 17, Audience::Store).await;
    }
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Write {
            audience: Audience::Store,
            device: b.device().await,
            trigger: crate::snapshot_data::SnapshotTrigger::Requested,
            attempt: None,
            abandoned: Vec::new(),
        },
        temporary: Vec::new(),
    });
    let early =
        b.db.start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
    let pass = b.caught_up_pass().await;
    b.step(early).await.unwrap();
    drop(pass);
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
    upload(&b, &storage).await;
    while b
        .db
        .operations()
        .await
        .unwrap()
        .iter()
        .any(|r| r.id == early)
    {
        b.step(early).await.unwrap();
    }
    b.writes.download_writes().await.unwrap();
    post(&a, &storage).await;
    post(&b, &storage).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(coverage_requests(&a).await, [Audience::Store]);
    assert_eq!(logs(&storage).await.len(), 4);
    b.write_snapshot(Audience::Store).await.unwrap();
    let listed = snapshots(&storage).await.len();
    a.write_snapshots().await.unwrap();
    assert!(coverage_requests(&a).await.is_empty());
    assert_eq!(snapshots(&storage).await.len(), listed);
    mature(&storage_clock, &[&a]).await;
    a.sync.run_retention().await.unwrap();
    let ana = a.device().await;
    assert!(!logs(&storage)
        .await
        .iter()
        .any(|path| path.write_id().unwrap().device == ana));
}

/// §15: a snapshot that lands more than a day after its catch-up does not
/// count, so its coverage does not stop retention asking for a timely one.
#[tokio::test]
async fn a_late_landing_does_not_supply_coverage() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    post(&a, &storage).await;
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Write {
            audience: Audience::Store,
            device: a.device().await,
            trigger: crate::snapshot_data::SnapshotTrigger::Requested,
            attempt: None,
            abandoned: Vec::new(),
        },
        temporary: Vec::new(),
    });
    let id =
        a.db.start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
    let pass = a.caught_up_pass().await;
    a.step(id).await.unwrap();
    drop(pass);
    // The upload lands a day and a second after its catch-up sample, and
    // its attempt is abandoned.
    storage_clock.set(at(1, 0) + Duration::from_secs(1));
    a.step(id).await.unwrap();
    assert_eq!(snapshots(&storage).await.len(), 1);
    a.sync.run_retention().await.unwrap();
    assert_eq!(coverage_requests(&a).await, [Audience::Store]);
    assert_eq!(logs(&storage).await.len(), 1);
}

/// §9, §15: while Ben's removal is not final he may return as a reader who
/// needs Ana's write, so it is not yet due and nothing is requested.
#[tokio::test]
async fn a_write_gated_on_a_provisional_removal_requests_nothing() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    a.sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: b.device().await,
        })
        .await
        .unwrap();
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    post(&a, &storage).await;
    a.sync.run_retention().await.unwrap();
    assert!(coverage_requests(&a).await.is_empty());
    storage_clock.set(at(31, 0));
    a.clock.set(at(31, 0));
    a.sync().await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(coverage_requests(&a).await, [Audience::Store]);
}

/// §15: Carol cannot read Gifts, whose members are offline. For Ana's
/// write with a store part and a Gifts part, Carol asks only for store
/// coverage and supplies it; the write stays for its Gifts part.
#[tokio::test]
async fn an_offline_circle_keeps_a_shared_write_while_store_coverage_progresses() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut c = device(storage.clone(), 2, member(2), store(1)).await;
    c.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    a.add(&c.member, MemberRole::Member).await;
    add_device(&mut c).await;
    a.sync().await;
    let gifts = crate::store_log_sync::tests::circle(1);
    a.sync
        .make_and_upload_entry(StoreChange::CreateCircle {
            circle: gifts,
            name: "Gifts".into(),
            key: key(2),
            key_hash: a.sync.test_key(Audience::Circle(gifts), key(2)),
        })
        .await
        .unwrap();
    let audience = gifts.to_string();
    a.db.write(move |sql| {
        for audience in ["store".to_owned(), audience] {
            sql.execute(
                "INSERT INTO notes VALUES('1',?1,'gift',x'00')",
                coven_database::params![audience],
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();
    upload(&a, &storage).await;
    post(&a, &storage).await;
    c.sync().await;
    c.writes.download_writes().await.unwrap();
    post(&c, &storage).await;
    c.sync.run_retention().await.unwrap();
    assert_eq!(coverage_requests(&c).await, [Audience::Store]);
    c.write_snapshots().await.unwrap();
    let listed = snapshots(&storage).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].snapshot_id().unwrap().audience, Audience::Store);
    c.sync.run_retention().await.unwrap();
    assert!(coverage_requests(&c).await.is_empty());
    assert_eq!(logs(&storage).await.len(), 1);
}

/// io `RetentionDeadline.refreshDue`, and `witnessedRefreshDue` for the
/// historical-witness-only trigger coven does not use: on generated
/// catalogs, reader posts, ages and finality, coven asks for coverage
/// exactly when `refreshDue` holds, including where no snapshot ever covered
/// the write and the witness-only trigger would never ask (`no_first_witness`).
#[test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
fn io_differential_refresh_due() {
    use crate::store_log_sync::snapshots::catalog::newest_first;
    use crate::store_log_sync::snapshots::retention::waits::refresh_due;
    use coven_format::sealed_snapshot::SnapshotObjectPrefix;
    use coven_format::value::EntryPositions;
    const MONTH: u64 = 30 * DAY;
    let write = coven_merge::WriteId {
        device: DeviceId(0),
        number: 2,
    };
    // (writer, number, stored, caught, position of writer 0, other writes)
    type Listed = (u64, u64, u64, u64, u64, u64);
    let catalogs: [&[Listed]; 5] = [
        &[],
        &[(1, 1, 0, 0, 2, 0)],
        // A late landing covers it; nothing counting does.
        &[(1, 1, DAY + 1, 0, 2, 0)],
        // The older snapshot covers it; the newer, with more writes, does not.
        &[(1, 1, 0, 0, 2, 0), (2, 1, 5, 5, 1, 9)],
        &[(1, 1, 0, 0, 1, 0)],
    ];
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for catalog in catalogs {
        let listed: Vec<_> = catalog
            .iter()
            .map(|(writer, number, stored, caught, position, others)| {
                let mut writes = vec![coven_merge::WriteId {
                    device: DeviceId(0),
                    number: *position,
                }];
                if *others > 0 {
                    writes.push(coven_merge::WriteId {
                        device: DeviceId(9),
                        number: *others,
                    });
                }
                (
                    coven_storage::StoredObject {
                        path: ObjectPath::snapshot(
                            store(1),
                            Audience::Store,
                            DeviceId(*writer),
                            (*number).try_into().unwrap(),
                        ),
                        size: 0,
                        stored_at: StorageTime::new(*stored, 0).unwrap(),
                        time_resolution: Duration::from_secs(1),
                        object_id: "id".into(),
                        revision: "1".into(),
                        checksum: None,
                    },
                    SnapshotObjectPrefix {
                        audience: Audience::Store,
                        key: key(1),
                        caught_at: StorageTime::new(*caught, 0).unwrap(),
                        raise: None,
                        writes: WritePositions(writes),
                        store_log: EntryPositions(Vec::new()),
                    },
                )
            })
            .collect();
        let mut counting: Vec<_> = listed
            .iter()
            .filter(|(object, prefix)| prefix.counts(object.stored_at))
            .collect();
        counting.sort_by(|a, b| newest_first((&a.0, &a.1), (&b.0, &b.1)));
        let current = counting.first().map(|(_, prefix)| &prefix.writes);
        for last in [false, true] {
            for readable in [false, true] {
                for posted in [[2, 2], [2, 1]] {
                    for time in [10, MONTH, MONTH - 1] {
                        let facts = Facts {
                            dependencies: vec![(
                                EntryId {
                                    device: DeviceId(1),
                                    number: 1,
                                },
                                last,
                            )],
                            readers: posted
                                .iter()
                                .enumerate()
                                .map(|(reader, number)| {
                                    (DeviceId(reader as u64 + 1), *number >= write.number)
                                })
                                .collect(),
                            aged: reached_age(
                                Some(StorageTime::new(time, 0).unwrap()),
                                StorageTime::new(0, 0).unwrap(),
                            ),
                        };
                        actual.push(refresh_due(&facts, readable, current, write));
                        cases.push(json!({
                            "check": "refresh", "number": write.number,
                            "posted": [[1, posted[0]], [2, posted[1]]],
                            "snapshots": catalog.iter().map(|(writer, number, stored, caught, position, others)| {
                                let mut positions = vec![json!([0, position])];
                                if *others > 0 {
                                    positions.push(json!([9, others]));
                                }
                                json!({"writer": writer, "number": number, "stored": stored,
                                       "caught": caught, "positions": positions})
                            }).collect::<Vec<_>>(),
                            "stored": 0, "time": time, "final": last, "readable": readable,
                        }));
                    }
                }
            }
        }
    }
    let lean = crate::discovery::tests::io_runner(&cases);
    assert_eq!(
        actual,
        lean.iter()
            .map(|result| result["due"].as_bool().unwrap())
            .collect::<Vec<_>>()
    );
    // The first catalog has no snapshot: refreshDue asks, the witness-only
    // trigger never would.
    assert!(lean
        .iter()
        .zip(&actual)
        .any(|(result, due)| *due && result["witnessed"] == json!(false)));
}
