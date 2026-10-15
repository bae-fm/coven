//! Who may sign a listed snapshot, and what its authenticated prefix
//! establishes before, or without, its body being read (§15, §19.1).

use super::*;
use coven_crypto::ObjectHasher;
use coven_format::sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix};

pub(super) fn signed_snapshot(
    unsigned: &[u8],
    path: &ObjectPath,
    prefix_author: &MemberIdentity,
    author: &MemberIdentity,
) -> Vec<u8> {
    let length = SnapshotObjectPrefix::length(unsigned).unwrap();
    let mut bytes = unsigned[..length].to_vec();
    bytes.extend_from_slice(
        prefix_author
            .keys()
            .sign_prefix(path.as_str(), &bytes)
            .as_bytes(),
    );
    bytes.extend_from_slice(&unsigned[length..]);
    let mut hash = ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(
        author
            .keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    bytes
}

async fn unsigned_snapshot(device: &Device, id: coven_format::store_log::SnapshotId) -> Vec<u8> {
    sealed_snapshot(device, id, |_| ()).await
}

/// `device`'s snapshot `id` sealed under its newest store key without either
/// signature, after `edit` changes its plaintext.
pub(super) async fn sealed_snapshot(
    device: &Device,
    id: coven_format::store_log::SnapshotId,
    edit: impl FnOnce(&mut Vec<u8>),
) -> Vec<u8> {
    let key_id = device.newest(Audience::Store).await;
    let path = ObjectPath::snapshot(
        store(1),
        id.audience.clone(),
        id.device,
        id.number.try_into().unwrap(),
    );
    let (send, receive) = std::sync::mpsc::channel();
    let begin = send.clone();
    device
        .db
        .write_snapshot(
            id,
            move |header| {
                begin
                    .send(
                        SnapshotObjectPrefix {
                            audience: header.id.audience.clone(),
                            key: key_id,
                            caught_at: coven_format::value::StorageTime::new(1, 0).unwrap(),
                            raise: None,
                            writes: header.writes.clone(),
                            store_log: header.store_log.clone(),
                        }
                        .encode()
                        .unwrap(),
                    )
                    .unwrap();
                Ok::<_, std::io::Error>(())
            },
            move |frame| {
                send.send(frame).unwrap();
                Ok(())
            },
        )
        .await
        .unwrap();
    let prefix = receive.recv().unwrap();
    let mut plain: Vec<_> = receive.into_iter().flatten().collect();
    edit(&mut plain);
    let key = device
        .custody
        .read()
        .unwrap()
        .unwrap()
        .store_key(key_id)
        .unwrap()
        .derive();
    let mut bytes = prefix.clone();
    let mut layout = SnapshotObjectLayout::new();
    for chunk in plain.chunks(coven_format::chunks::CHUNK_SIZE) {
        let sealed = key
            .seal_object_chunk(path.as_str(), &prefix, 0, layout.index(), chunk)
            .unwrap();
        bytes.extend(layout.encode_chunk(&sealed).unwrap());
    }
    bytes
}

#[tokio::test]
async fn unsigned_snapshot_cannot_replace_rows_or_authorize_log_deletion() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let id = coven_format::store_log::SnapshotId {
        audience: Audience::Store,
        device: a.device().await,
        number: 999,
    };
    let path = ObjectPath::snapshot(
        store(1),
        id.audience.clone(),
        id.device,
        id.number.try_into().unwrap(),
    );
    a.db.write(|sql| {
        sql.execute("UPDATE notes SET title='unpublished'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    let unsigned = unsigned_snapshot(&a, id).await;
    storage.delete(&path).await.unwrap();
    storage.create(&path, &unsigned).await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    a.sync.run_retention().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(
        snapshot_record(&b, &path).await,
        Some(crate::PendingReason::Refused(crate::Refusal::Signature {
            cause: None
        }))
    );
    // Without a usable snapshot the reload has no target; discovery reads the log.
    b.writes.download_writes().await.unwrap();
    assert_eq!(tables(&b).await[0].1, "note 0");
    assert_eq!(
        b.db.sync_state(Vec::new()).await.unwrap().positions.0[0].number,
        1
    );
}

#[tokio::test]
async fn another_members_snapshot_signatures_never_authorize_selection_loading_or_retention() {
    for object_owner in [false, true] {
        let (storage, storage_clock) = timed_snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let other = member(2);
        a.add(&other, MemberRole::Member).await;
        write_rows(&a, 0, 1, 17, Audience::Store).await;
        upload(&a, &storage).await;
        let id = coven_format::store_log::SnapshotId {
            audience: Audience::Store,
            device: a.device().await,
            number: 999,
        };
        let path = ObjectPath::snapshot(
            store(1),
            id.audience.clone(),
            id.device,
            id.number.try_into().unwrap(),
        );
        let unsigned = unsigned_snapshot(&a, id).await;
        let forged = signed_snapshot(
            &unsigned,
            &path,
            &other,
            if object_owner { &a.member } else { &other },
        );
        storage.delete(&path).await.unwrap();
        storage.create(&path, &forged).await.unwrap();
        a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
        storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
        observe(&a).await;
        a.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()
                .len(),
            1
        );
        let mut b = notes_device(storage.clone(), 2).await;
        add_device(&mut b).await;
        let data = Data::Snapshots(SnapshotTask {
            job: SnapshotJob::Reload {
                scope: crate::snapshot_data::ReloadScope::All,
                files: None,
            },
            temporary: Vec::new(),
        });
        let operation =
            b.db.start_operation(data.new_operation("coven").unwrap())
                .await
                .unwrap();
        let record =
            b.db.operations()
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.id == operation)
                .unwrap();
        b.sync.operation_step(&record, data).await.unwrap();
        let record =
            b.db.operations()
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.id == operation)
                .unwrap();
        let Data::Snapshots(SnapshotTask {
            job: SnapshotJob::Reload {
                files: Some(files), ..
            },
            ..
        }) = Data::read(&record).unwrap()
        else {
            panic!()
        };
        assert!(files.snapshots.is_empty());
        assert!(tables(&b).await.is_empty());
        b.sync.reload_from_snapshots().await.unwrap();
        b.writes.download_writes().await.unwrap();
        assert_eq!(tables(&b).await, tables(&a).await);
        // A new publication signed by the real author permits loading after the logs disappear.
        storage.delete(&path).await.unwrap();
        if !a.send.can_send() {
            a.catch_up().await;
        }
        a.write_snapshot(Audience::Store).await.unwrap();
        mature(&storage_clock, &[&a]).await;
        a.sync.run_retention().await.unwrap();
        assert!(storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
            .is_empty());
        b.sync.reload_from_snapshots().await.unwrap();
        assert_eq!(tables(&b).await, tables(&a).await);
    }
}

#[tokio::test]
async fn coverage_checks_read_exactly_the_signed_prefix_without_an_audience_key() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.writes.download_writes().await.unwrap();
    a.write_snapshot(Audience::Store).await.unwrap();
    let object = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .remove(0);
    b.sync
        .observe_clock(&b.db.local_store_log().await.unwrap())
        .await
        .unwrap();
    b.custody.forget().unwrap();
    let before = storage.ranges().await.len();
    b.sync.discover_snapshots().await.unwrap();
    let ranges = storage.ranges().await;
    assert_eq!(ranges.len() - before, 2);
    assert_eq!(ranges[before].start(), 0);
    let bytes = storage.read(&object.path).await.unwrap();
    let length = SnapshotObjectPrefix::length(&bytes).unwrap();
    assert_eq!(
        ranges[before..]
            .iter()
            .map(|range| range.len())
            .sum::<u64>(),
        (length + 64) as u64
    );
    assert_eq!(ranges.last().unwrap().end(), (length + 64) as u64);
}

#[tokio::test]
async fn signed_coverage_does_not_require_opening_the_body_but_loading_does() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let id = coven_format::store_log::SnapshotId {
        audience: Audience::Store,
        device: a.device().await,
        number: 999,
    };
    let path = ObjectPath::snapshot(
        store(1),
        id.audience.clone(),
        id.device,
        id.number.try_into().unwrap(),
    );
    let unsigned = unsigned_snapshot(&a, id).await;
    let wrong_body_signature = signed_snapshot(&unsigned, &path, &a.member, &member(2));
    storage.create(&path, &wrong_body_signature).await.unwrap();
    a.write_snapshots().await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    observe(&a).await;
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    assert!(b.sync.reload_from_snapshots().await.is_err());
    assert!(tables(&b).await.is_empty());
    storage.delete(&path).await.unwrap();
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.write_snapshot(Audience::Store).await.unwrap();
    b.elapse_retry_delays();
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
}

#[tokio::test]
async fn a_truncated_body_does_not_erase_signed_required_history() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.write_snapshot(Audience::Store).await.unwrap();
    mature(&storage_clock, &[&a]).await;
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
    let object = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .remove(0);
    let mut bytes = storage.read(&object.path).await.unwrap();
    bytes.truncate(SnapshotObjectPrefix::length(&bytes).unwrap() + 64);
    republish(&storage, &storage_clock, &object.path, &bytes).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    assert!(b.sync.discover_snapshots().await.is_err());
    assert!(tables(&b).await.is_empty());
    assert!(b.sync.pending_reload().await.unwrap().is_some());
}

#[tokio::test]
async fn reset_boundaries_reject_another_members_prefix_before_changing_rows() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let other = member(2);
    a.add(&other, MemberRole::Member).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let id = coven_format::store_log::SnapshotId {
        audience: Audience::Store,
        device: a.device().await,
        number: 999,
    };
    let path = ObjectPath::snapshot(
        store(1),
        id.audience.clone(),
        id.device,
        id.number.try_into().unwrap(),
    );
    let unsigned = unsigned_snapshot(&a, id.clone()).await;
    let forged = signed_snapshot(&unsigned, &path, &other, &other);
    storage.delete(&path).await.unwrap();
    storage.create(&path, &forged).await.unwrap();
    a.sync
        .make_and_upload_entry(StoreChange::Reset {
            snapshot: id.clone(),
        })
        .await
        .unwrap();
    let before = tables(&a).await;
    // The named snapshot's refusal keeps its native cause for this call and
    // stays as its own record.
    assert!(matches!(a.sync.reload_from_snapshots().await,
        Err(SyncError::Pending(record)) if record.subject == crate::PendingSubject::Snapshot(id.clone())
            && matches!(record.reason, crate::PendingReason::Refused(crate::Refusal::Signature { cause: Some(_) }))));
    assert!(matches!(
        snapshot_record(&a, &path).await,
        Some(crate::PendingReason::Refused(
            crate::Refusal::Signature { .. }
        ))
    ));
    assert_eq!(tables(&a).await, before);
}

#[tokio::test]
async fn authors_must_be_in_the_applied_store_log_and_remain_known_after_removal() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let before = a.log().await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.write_snapshot(Audience::Store).await.unwrap();
    super::retention::post(&b, &storage).await;
    let path = ObjectPath::positions(store(1), b.device().await);
    let bytes = storage.read(&path).await.unwrap();
    assert!(matches!(
        a.sync.reload_from_snapshots().await,
        Err(SyncError::Database(coven_database::DbError::Snapshot(
            coven_database::SnapshotError::Inconsistent(_)
        )))
    ));
    {
        let ring = a.custody.read().unwrap().unwrap();
        // An unregistered author's positions wait for the registration
        // their signed store-log view names (D9).
        assert_eq!(
            crate::posted_positions::open(&bytes, &path, Some(&ring), &before).unwrap(),
            Err(crate::PendingReason::Waits(crate::Prerequisite::Object(
                crate::store_log_object::path(
                    store(1),
                    coven_format::value::EntryId {
                        device: b.device().await,
                        number: 1
                    }
                )
            )))
        );
    }
    a.sync().await;
    let registered = a.log().await;
    let pending = a.db.pending::<crate::PendingRecord>().await.unwrap();
    // The unregistered author's snapshot was a wait, never a damaged object.
    let snapshot = pending
        .iter()
        .find_map(|record| match &record.subject {
            crate::PendingSubject::Snapshot(id) => Some((id.clone(), record.reason.clone())),
            _ => None,
        })
        .unwrap();
    assert_eq!(snapshot.0.device, b.device().await);
    // It waits for the registration entry its signed coverage names (D9).
    assert_eq!(
        snapshot.1,
        crate::PendingReason::Waits(crate::Prerequisite::Object(crate::store_log_object::path(
            store(1),
            coven_format::value::EntryId {
                device: b.device().await,
                number: 1
            }
        )))
    );
    let operation = pending
        .iter()
        .find(|record| matches!(record.subject, crate::PendingSubject::Operation { .. }))
        .unwrap();
    let crate::PendingSubject::Operation { id, .. } = operation.subject else {
        unreachable!()
    };
    assert_eq!(
        operation.reason,
        crate::PendingReason::Failed(crate::LocalFailure::Database)
    );
    // Explicit retry checks author validation against the applied registration;
    // the next listing finds the snapshot checks and ends its wait.
    a.db.retry_operation(id).await.unwrap();
    a.sync.reload_from_snapshots().await.unwrap();
    assert!(!a
        .db
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .iter()
        .any(|record| matches!(record.subject, crate::PendingSubject::Snapshot(_))));
    a.sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: b.device().await,
        })
        .await
        .unwrap();
    a.sync.reload_from_snapshots().await.unwrap();
    let ring = a.custody.read().unwrap().unwrap();
    for log in [registered, a.log().await] {
        crate::posted_positions::open(&bytes, &path, Some(&ring), &log)
            .unwrap()
            .unwrap();
    }
}
