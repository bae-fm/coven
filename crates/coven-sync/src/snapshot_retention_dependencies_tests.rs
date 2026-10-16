//! Which store-log entries a deletion depends on (§9, §15, §16.5): store
//! creation registers its device like an addition does, and coven's
//! dependencies, records and deletions agree with storelog-data
//! `RetentionSafety.Deletion` on replayed histories.

use super::retention::post;
use super::*;
use crate::replay::tests::{self as history, History};
use crate::store_log_sync::snapshots::retention::{
    dependencies::{deletes_for, Dependencies},
    waits::Facts,
};
use coven_database::{EntryOutcome, StoreLog};
use coven_format::value::{EntryId, StorageTime, WritePositions};
use coven_foundation::id_source::MemberId;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const DAY: u64 = 86400;

async fn logs(storage: &MemoryStorage) -> Vec<ObjectPath> {
    storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path)
        .collect()
}

/// This device's retention reasons.
async fn reasons(device: &Device) -> Vec<crate::PendingReason> {
    device
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| matches!(record.subject, crate::PendingSubject::Retention { .. }))
        .map(|record| record.reason)
        .collect()
}

/// Ana's phone registered itself by creating the store, and never posted.
/// Ben removes Ana: until that removal is final, a concurrent entry could
/// drop it and leave her phone an active reader that needs Ben's write, so
/// the write waits on the entries deciding her phone's state, the creation
/// among them. Once they are final, a later covered write goes on Ben's own
/// post (storelog-data `creation_device_counterexample`).
#[tokio::test]
async fn the_creation_devices_member_removal_holds_deletion_until_final() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    let creation = a.create(key(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    b.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    a.add(&b.member, MemberRole::Admin).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&b, 0, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    b.catch_up().await;
    b.write_snapshot(Audience::Store).await.unwrap();
    let removal = b
        .sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: a.member.member(),
        })
        .await
        .unwrap();
    // Removing a member rotates the store key before Ben writes again (§11).
    b.sync.test_rotate(Audience::Store, key(2)).await;
    post(&b, &storage).await;
    mature(&storage_clock, &[&b]).await;
    b.sync.run_retention().await.unwrap();
    assert_eq!(logs(&storage).await.len(), 1);
    let first = creation.min(removal);
    assert_eq!(
        reasons(&b).await,
        [crate::PendingReason::Waits(
            crate::Prerequisite::EntryFinality(first)
        )]
    );
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * DAY));
    b.clock.set(UNIX_EPOCH + Duration::from_secs(31 * DAY));
    b.sync().await;
    write_rows(&b, 1, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    b.catch_up().await;
    b.write_snapshot(Audience::Store).await.unwrap();
    post(&b, &storage).await;
    mature(&storage_clock, &[&b]).await;
    b.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
    assert!(reasons(&b).await.is_empty());
}

/// Replayed histories covering creation, additions, replacement, device and
/// member removals, and concurrent removals one of which drops.
fn histories() -> Vec<History> {
    use coven_format::store_log::MemberRole::{Admin, Member};
    let mut histories = Vec::new();
    // Ben removes Ana, whose phone creation registered.
    let mut h = History::new();
    h.all(0, 0, history::add(1, Admin));
    h.all(1, 1, history::device(1));
    h.all(1, 1, history::remove(0));
    histories.push(h);
    // Ana and Ben remove each other concurrently.
    let mut h = history::losing_removal();
    h.push(0, 0, &[0, 1, 2, 3], history::remove(1));
    h.push(1, 1, &[0, 1, 2, 3], history::remove(0));
    histories.push(h);
    // Ben replaces his laptop; Ana removes Carol's device.
    let mut h = history::household(Admin, Member);
    h.all(1, 5, history::replace(5, 1));
    h.all(
        0,
        0,
        StoreChange::RemoveDevice {
            device: DeviceId(2),
        },
    );
    histories.push(h);
    // Ana's concurrent removal of Ben defeats his phone's registration.
    let mut h = history::household(Admin, Member).prefix(3);
    h.push(1, 4, &[0, 1, 2], history::device(4));
    h.push(0, 0, &[0, 1, 2], history::remove(1));
    histories.push(h);
    histories
}

/// The change as the model reads it: members by first appearance.
fn change(entry: &coven_format::store_log::StoreLogEntry, members: &mut Vec<MemberId>) -> Value {
    let mut index = |member: MemberId| match members.iter().position(|m| *m == member) {
        Some(i) => i,
        None => {
            members.push(member);
            members.len() - 1
        }
    };
    match &entry.change {
        StoreChange::CreateStore { .. } => {
            json!(["create", entry.position.device.0, index(entry.author)])
        }
        StoreChange::AddDevice { replaces, .. } => json!([
            "addDevice",
            entry.position.device.0,
            index(entry.author),
            replaces.map(|d| d.0)
        ]),
        StoreChange::RemoveDevice { device } => json!(["removeDevice", device.0]),
        StoreChange::RemoveMember { member } => json!(["removeMember", index(*member)]),
        _ => json!(["other"]),
    }
}

/// Every device an entry names.
fn named(log: &StoreLog) -> BTreeSet<DeviceId> {
    let mut devices = BTreeSet::new();
    for applied in &log.entries {
        match &applied.entry.change {
            StoreChange::CreateStore { .. } => {
                devices.insert(applied.entry.position.device);
            }
            StoreChange::AddDevice { replaces, .. } => {
                devices.insert(applied.entry.position.device);
                devices.extend(*replaces);
            }
            StoreChange::RemoveDevice { device } => {
                devices.insert(*device);
            }
            _ => (),
        }
    }
    devices
}

/// storelog-data `RetentionSafety.Deletion`: on each replayed history, under
/// every finality pattern that leaves at most one entry provisional, for
/// each local device, uploader, set of posts, storage age and entitlement,
/// coven depends on the model's entries, names its first unmet condition,
/// deletes exactly when it allows, and releases the uploader's files exactly
/// when `filesReleased` does.
#[test]
#[ignore = "requires COVEN_STORELOG_DATA_LEAN, supplied by scripts/check.sh"]
fn data_differential_retention_dependencies() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for h in histories() {
        let replayed = history::checked_log(&h.entries);
        let mut positions: Vec<EntryId> =
            replayed.entries.iter().map(|e| e.entry.position).collect();
        positions.sort();
        let rank = |id: EntryId| positions.iter().position(|p| *p == id).unwrap();
        let devices: Vec<DeviceId> = named(&replayed).into_iter().collect();
        for pattern in 0..replayed.entries.len() + 2 {
            let mut log = replayed.clone();
            log.horizon = Some(StorageTime::new(100, 0).unwrap());
            for (k, applied) in log.entries.iter_mut().enumerate() {
                let last = match pattern {
                    0 => false,
                    1 => true,
                    p => k != p - 2,
                };
                applied.stored_at = StorageTime::new(if last { 10 } else { 200 }, 0).unwrap();
            }
            let mut members = Vec::new();
            let entries: Vec<Value> = log
                .entries
                .iter()
                .map(|applied| {
                    let id = applied.entry.position;
                    json!({
                        "id": rank(id),
                        "change": change(&applied.entry, &mut members),
                        "kept": log.replay.entries[&id] == EntryOutcome::Kept,
                        "final": log.is_final(id),
                    })
                })
                .collect();
            let dependencies = Dependencies::new(&log);
            for own in [DeviceId(0), DeviceId(1)] {
                for &uploader in &devices {
                    let write = coven_merge::WriteId {
                        device: uploader,
                        number: 1,
                    };
                    for posts in 0..1u32 << devices.len() {
                        let posted: Vec<DeviceId> = devices
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| posts & (1 << i) != 0)
                            .map(|(_, d)| *d)
                            .collect();
                        let positions: BTreeMap<_, _> = posted
                            .iter()
                            .map(|d| (*d, WritePositions(vec![write])))
                            .collect();
                        for aged in [false, true] {
                            let stored = StorageTime::new(1000, 0).unwrap();
                            let days = if aged { 30 } else { 1 };
                            let now = StorageTime::new(1000 + days * DAY, 0).unwrap();
                            let facts = Facts::for_write(
                                &log,
                                &dependencies,
                                dependencies.deleting(own, uploader),
                                write,
                                stored,
                                Some(now),
                                &positions,
                            );
                            for entitled in [false, true] {
                                let owned =
                                    deletes_for(&log, own, uploader, |_| Ok::<_, ()>(entitled))
                                        .unwrap();
                                let unmet = facts.first_unmet();
                                let delete = owned && unmet.is_none();
                                actual.push(json!({
                                    "dependencies": facts.dependencies.iter().map(|(e, _)| rank(*e)).collect::<Vec<_>>(),
                                    "record": match unmet.filter(|_| owned) {
                                        Some(crate::Prerequisite::EntryFinality(e)) => json!(["entryFinality", rank(e)]),
                                        Some(crate::Prerequisite::Positions(d)) => json!(["positions", d.0]),
                                        Some(other) => panic!("{other:?}"),
                                        None => Value::Null,
                                    },
                                    "delete": delete,
                                    "files": owned && dependencies.releases_files(&log, own, uploader),
                                }));
                                cases.push(json!({
                                    "entries": entries, "own": own.0, "uploader": uploader.0,
                                    "posted": posted.iter().map(|d| d.0).collect::<Vec<_>>(),
                                    "aged": aged, "covered": true, "entitled": entitled,
                                }));
                            }
                        }
                    }
                }
            }
        }
    }
    let output = crate::tests::run_lean("COVEN_STORELOG_DATA_LEAN", &json!({ "deletions": cases }));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lean: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual.len(), lean.len());
    for ((case, rust), lean) in cases.iter().zip(&actual).zip(&lean) {
        assert_eq!(rust, lean, "{case}");
    }
}
