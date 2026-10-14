use super::*;
use coven_crypto::ObjectHasher;
use coven_format::sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix};

pub(super) fn signed_snapshot(
    unsigned: &[u8],
    path: &ObjectPath,
    prefix_author: &MemberKeys,
    author: &MemberKeys,
) -> Vec<u8> {
    let length = SnapshotObjectPrefix::length(unsigned).unwrap();
    let mut bytes = unsigned[..length].to_vec();
    bytes.extend_from_slice(prefix_author.sign_prefix(path.as_str(), &bytes).as_bytes());
    bytes.extend_from_slice(&unsigned[length..]);
    let mut hash = ObjectHasher::new();
    hash.update(&bytes);
    bytes.extend_from_slice(author.sign_object(path.as_str(), &hash.finish()).as_bytes());
    bytes
}

async fn unsigned_snapshot(device: &Device, id: coven_format::store_log::SnapshotId) -> Vec<u8> {
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
    let plain: Vec<_> = receive.into_iter().flatten().collect();
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
        a.sync.write_snapshot(Audience::Store).await.unwrap();
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
    a.sync.write_snapshot(Audience::Store).await.unwrap();
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
    let unsigned = unsigned_snapshot(&a, id).await;
    let wrong_body_signature = signed_snapshot(&unsigned, &path, &a.member, &member(2));
    storage.create(&path, &wrong_body_signature).await.unwrap();
    a.sync.write_snapshots().await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
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
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
}

#[tokio::test]
async fn a_truncated_body_does_not_erase_signed_required_history() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.sync.write_snapshot(Audience::Store).await.unwrap();
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
    storage.delete(&object.path).await.unwrap();
    storage.create(&object.path, &bytes).await.unwrap();
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
    b.sync.write_snapshot(Audience::Store).await.unwrap();
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
        // An unregistered author's positions wait for its registration.
        assert_eq!(
            crate::posted_positions::open(&bytes, &path, Some(&ring), &before).unwrap(),
            Err(crate::PendingReason::Waits(
                crate::Prerequisite::DeviceRegistration(b.device().await)
            ))
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
    assert_eq!(
        snapshot.1,
        crate::PendingReason::Waits(crate::Prerequisite::DeviceRegistration(b.device().await))
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

/// IO `Snapshot.counts` and the late-landing rule: the real admission test
/// against Lean on exact integer-second samples. Subsecond and overflow cases
/// stay with the format's own tests.
#[test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
fn io_differential_snapshot_counts() {
    use coven_format::value::StorageTime;
    use serde_json::json;
    const DAY: u64 = StorageTime::DAY_SECONDS;
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for caught in [0, 1, 100, DAY, 1_760_400_000] {
        for stored in [
            0,
            caught,
            caught + DAY - 1,
            caught + DAY,
            caught + DAY + 1,
            caught + 2 * DAY,
        ] {
            let prefix = SnapshotObjectPrefix {
                audience: coven_merge::Audience::Store,
                key: key(1),
                caught_at: StorageTime::new(caught, 0).unwrap(),
                writes: coven_format::value::WritePositions(Vec::new()),
                store_log: coven_format::value::EntryPositions(Vec::new()),
            };
            cases.push(json!({"check": "counts", "caught": caught, "stored": stored}));
            actual.push(json!(prefix.counts(StorageTime::new(stored, 0).unwrap())));
        }
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

/// IO `catalogCurrent` with `.writes`, `late_snapshot_ignored` and the
/// repaired example's late competitor: the real filter and ordering choose
/// the current counting snapshot of each audience.
#[test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
fn io_differential_catalog_current() {
    use coven_format::value::{StorageTime, WritePositions};
    use serde_json::json;
    const DAY: u64 = StorageTime::DAY_SECONDS;
    // (audience, writer, number, stored, caught, positions)
    type Listed = (u64, u64, u64, u64, u64, Vec<(u64, u64)>);
    let mut catalogs: Vec<Vec<Listed>> = vec![
        // RepairedExamples: Ana's timely snapshot and a late, larger competitor.
        vec![
            (0, 0, 1, 0, 0, vec![(0, 1)]),
            (0, 1, 1, DAY + 1, 0, vec![(1, 2)]),
        ],
        // Equal counts: the smaller device, then the smaller number.
        vec![
            (0, 2, 1, 5, 5, vec![(0, 3)]),
            (0, 1, 2, 5, 5, vec![(1, 3)]),
            (0, 1, 1, 5, 5, vec![(2, 3)]),
        ],
        // Incomparable timely snapshots: the most covered writes wins.
        vec![
            (0, 0, 1, 0, 0, vec![(0, 5)]),
            (0, 1, 1, 10, 9, vec![(1, 4), (2, 2)]),
        ],
        // Exactly one day counts; one second more does not.
        vec![
            (0, 0, 1, DAY, 0, vec![(0, 2)]),
            (0, 1, 1, DAY + 1, 0, vec![(1, 9)]),
        ],
        // Only late snapshots: no current snapshot.
        vec![(0, 0, 1, 2 * DAY, 0, vec![(0, 1)])],
        // Smaller paths compare devices as numbers, not as path text.
        vec![
            (0, 10, 1, 0, 0, vec![(0, 2)]),
            (0, 9, 1, 0, 0, vec![(1, 2)]),
        ],
        // Per audience: a circle's larger snapshot does not compete.
        vec![(0, 0, 1, 0, 0, vec![(0, 1)]), (1, 1, 1, 0, 0, vec![(1, 7)])],
    ];
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound
    };
    for _ in 0..200 {
        let count = 1 + next(5);
        let mut catalog = Vec::new();
        for _ in 0..count {
            let caught = next(3) * DAY;
            let stored = caught + [0, DAY - 1, DAY, DAY + 1][next(4) as usize];
            let mut positions = Vec::new();
            for writer in 0..3 {
                if next(2) == 1 {
                    positions.push((writer, 1 + next(4)));
                }
            }
            catalog.push((next(2), next(3), 1 + next(3), stored, caught, positions));
        }
        catalog.sort_by_key(|s| (s.0, s.1, s.2));
        catalog.dedup_by_key(|s| (s.0, s.1, s.2));
        catalogs.push(catalog);
    }
    let audience = |n: u64| match n {
        0 => coven_merge::Audience::Store,
        n => coven_merge::Audience::Circle(coven_crypto::CircleId(uuid::Uuid::from_u128(n.into()))),
    };
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for catalog in catalogs {
        for wanted in 0..2 {
            let mut listed = Vec::new();
            for (n, writer, number, stored, caught, positions) in &catalog {
                if *n != wanted {
                    continue;
                }
                let object = coven_storage::StoredObject {
                    path: ObjectPath::snapshot(
                        store(1),
                        audience(*n),
                        DeviceId(*writer),
                        (*number).try_into().unwrap(),
                    ),
                    size: 0,
                    stored_at: UNIX_EPOCH + Duration::from_secs(*stored),
                    time_resolution: Duration::from_secs(1),
                    object_id: "id".into(),
                    revision: "1".into(),
                    checksum: None,
                };
                let prefix = SnapshotObjectPrefix {
                    audience: audience(*n),
                    key: key(1),
                    caught_at: StorageTime::new(*caught, 0).unwrap(),
                    writes: WritePositions(
                        positions
                            .iter()
                            .map(|(device, number)| coven_merge::WriteId {
                                device: DeviceId(*device),
                                number: *number,
                            })
                            .collect(),
                    ),
                    store_log: coven_format::value::EntryPositions(Vec::new()),
                };
                if prefix
                    .counts(crate::store_log_sync::snapshots::catalog::landing(&object).unwrap())
                {
                    listed.push((object, prefix));
                }
            }
            listed.sort_by(|a, b| {
                crate::store_log_sync::snapshots::catalog::newest_first((&a.0, &a.1), (&b.0, &b.1))
            });
            actual.push(match listed.first() {
                Some((object, _)) => {
                    let id = object.path.snapshot_id().unwrap();
                    json!([id.device.0, id.number])
                }
                None => serde_json::Value::Null,
            });
            let snapshots: Vec<_> = catalog
                .iter()
                .map(|(n, writer, number, stored, caught, positions)| {
                    json!({"audience": n, "writer": writer, "number": number,
                        "stored": stored, "caught": caught, "positions": positions})
                })
                .collect();
            cases.push(json!({"check": "catalog", "audience": wanted, "snapshots": snapshots}));
        }
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

#[tokio::test]
async fn a_late_landing_is_ignored_even_when_it_covers_more_writes() {
    let clock = Arc::new(coven_foundation::clock::FixedClock::new(
        UNIX_EPOCH + Duration::from_secs(1_760_400_000),
    ));
    let storage = Arc::new(
        MemoryStorage::builder()
            .transfer_limits(65536, 65536)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let timely = a.sync.write_snapshot(Audience::Store).await.unwrap();
    write_rows(&a, 1, 2, 17, Audience::Store).await;
    upload(&a, &storage).await;
    // Prepare from a catch-up, then let the upload land a day and a second later.
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Write {
            audience: Audience::Store,
            device: a.device().await,
            trigger: crate::snapshot_data::SnapshotTrigger::Requested,
            session: None,
        },
        temporary: Vec::new(),
    });
    let id =
        a.db.start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
    let record = a.operation(id).await;
    assert!(matches!(
        a.sync
            .operation_step(&record, Data::read(&record).unwrap())
            .await
            .unwrap(),
        Progress::Advanced
    ));
    clock.set(
        UNIX_EPOCH
            + Duration::from_secs(
                1_760_400_000 + coven_format::value::StorageTime::DAY_SECONDS + 1,
            ),
    );
    loop {
        let Some(record) =
            a.db.operations()
                .await
                .unwrap()
                .into_iter()
                .find(|r| r.id == id)
        else {
            break;
        };
        a.sync
            .operation_step(&record, Data::read(&record).unwrap())
            .await
            .unwrap();
    }
    let late = ObjectPath::snapshot(
        store(1),
        Audience::Store,
        a.device().await,
        NonZeroU64::new(id.0.try_into().unwrap()).unwrap(),
    );
    let bytes = storage.read(&late).await.unwrap();
    let prefix =
        SnapshotObjectPrefix::decode(&bytes[..SnapshotObjectPrefix::length(&bytes).unwrap()])
            .unwrap();
    assert!(
        crate::store_log_sync::snapshots::catalog::score(&prefix) > 1,
        "the late snapshot covers more writes"
    );
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    let timely = ObjectPath::snapshot(
        store(1),
        timely.audience,
        timely.device,
        NonZeroU64::new(timely.number).unwrap(),
    );
    assert_eq!(selected_snapshot(&mut c).await, timely);
    // A fresh preparation samples the later storage time and counts.
    let fresh = a.sync.write_snapshot(Audience::Store).await.unwrap();
    assert_eq!(
        selected_snapshot(&mut c).await,
        ObjectPath::snapshot(
            store(1),
            fresh.audience,
            fresh.device,
            NonZeroU64::new(fresh.number).unwrap()
        )
    );
}

#[tokio::test]
async fn newer_schema_snapshot_is_passed_over_for_an_older_supported_one() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    b.db.apply_downloaded(a.db.test_queued_writes().await.unwrap().remove(0).into())
        .await
        .unwrap();
    upload(&a, &storage).await;
    let supported = b.sync.write_snapshot(Audience::Store).await.unwrap();
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let mut migrations = notes_migrations();
    migrations.push(Migration::sql(
        2,
        "addition",
        "ALTER TABLE notes ADD COLUMN added TEXT",
    ));
    a.reopen(storage.clone(), notes_tables(), migrations).await;
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    assert_eq!(
        selected_snapshot(&mut c).await.snapshot_id(),
        Some(supported)
    );
    assert_eq!(tables(&a).await, tables(&c).await);
}

/// Snapshots prepared concurrently, each covering only its author's write,
/// tie on covered writes; the smaller path selects Ana's own (§15). Ben's
/// write outside it stays in his log, since deletion needs the current
/// snapshot to cover it, so her reload converges by reading that log.
#[tokio::test]
async fn a_device_reloads_its_own_tied_snapshot_and_reads_the_rest_from_logs() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    write_rows(&b, 1, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    // Ben prepares before Ana's snapshot lands, so neither covers the other.
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Write {
            audience: Audience::Store,
            device: b.device().await,
            trigger: crate::snapshot_data::SnapshotTrigger::Requested,
            session: None,
        },
        temporary: Vec::new(),
    });
    let id =
        b.db.start_operation(data.new_operation("coven").unwrap())
            .await
            .unwrap();
    let record = b.operation(id).await;
    b.sync
        .operation_step(&record, Data::read(&record).unwrap())
        .await
        .unwrap();
    let own = a.sync.write_snapshot(Audience::Store).await.unwrap();
    while let Some(record) =
        b.db.operations()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
    {
        b.sync
            .operation_step(&record, Data::read(&record).unwrap())
            .await
            .unwrap();
    }
    let mut scores = Vec::new();
    for object in storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
    {
        let bytes = storage.read(&object.path).await.unwrap();
        let length = SnapshotObjectPrefix::length(&bytes).unwrap();
        let prefix = SnapshotObjectPrefix::decode(&bytes[..length]).unwrap();
        scores.push(crate::store_log_sync::snapshots::catalog::score(&prefix));
    }
    assert_eq!(scores, [1, 1]);
    a.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(selected_snapshot(&mut a).await.snapshot_id(), Some(own));
    a.writes.download_writes().await.unwrap();
    b.writes.download_writes().await.unwrap();
    assert_eq!(tables(&a).await.len(), 2);
    assert_eq!(tables(&a).await, tables(&b).await);
}

#[tokio::test]
async fn selection_orders_coverage_then_path_and_passes_over_damaged_objects() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    b.db.apply_downloaded(a.db.test_queued_writes().await.unwrap().remove(0).into())
        .await
        .unwrap();
    upload(&a, &storage).await;
    let a_id = a.sync.write_snapshot(Audience::Store).await.unwrap();
    let b_id = b.sync.write_snapshot(Audience::Store).await.unwrap();
    let path = |s: coven_format::store_log::SnapshotId| {
        ObjectPath::snapshot(
            store(1),
            s.audience,
            s.device,
            NonZeroU64::new(s.number).unwrap(),
        )
    };
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    assert_eq!(
        selected_snapshot(&mut c).await,
        path(a_id.clone()).min(path(b_id.clone()))
    );
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let newer = path(a.sync.write_snapshot(Audience::Store).await.unwrap());
    assert_eq!(selected_snapshot(&mut c).await, newer);
    let size = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .into_iter()
        .find(|o| o.path == newer)
        .unwrap()
        .size;
    storage
        .corrupt_byte(&newer, size as usize - 1)
        .await
        .unwrap();
    // The reader that already authenticated the object keeps its checked bytes.
    assert_eq!(selected_snapshot(&mut c).await, newer);
    let mut c = notes_device(storage.clone(), 4).await;
    add_device(&mut c).await;
    c.sync.reload_from_snapshots().await.unwrap();
    assert!(matches!(
        snapshot_record(&c, &newer).await,
        Some(crate::PendingReason::Refused(_))
    ));
    // The refusal passes the snapshot over without reading its bytes again,
    // while its verified prefix still counts as required history (§19.1).
    let reads = storage.reads().await.len();
    assert_eq!(selected_snapshot(&mut c).await, path(b_id.clone()));
    assert!(!storage.reads().await[reads..]
        .iter()
        .any(|(read, _, _)| read == &newer));
    assert_eq!(tables(&a).await, tables(&c).await);
    // A malformed clear prefix is refused by itself; it establishes nothing.
    let malformed = ObjectPath::snapshot(
        store(1),
        Audience::Store,
        a.device().await,
        NonZeroU64::new(999).unwrap(),
    );
    storage.create_once(&malformed, b"x").await.unwrap();
    c.sync.reload_from_snapshots().await.unwrap();
    assert!(matches!(
        snapshot_record(&c, &malformed).await,
        Some(crate::PendingReason::Refused(crate::Refusal::Parse { .. }))
    ));
    // Without the write only the refused snapshot covers, its prefix makes
    // a new reader's reload wait for that history instead of skipping it.
    let only = coven_merge::WriteId {
        device: a.device().await,
        number: 2,
    };
    let only_path = crate::write_seal::path(store(1), only);
    let bytes = storage.read(&only_path).await.unwrap();
    storage.delete(&only_path).await.unwrap();
    let mut d = notes_device(storage.clone(), 5).await;
    add_device(&mut d).await;
    // The first attempt refuses the body; the second reads only the retained
    // prefix of the refused snapshot, and both wait for the same history.
    for _ in 0..2 {
        let result = d.sync.reload_from_snapshots().await;
        assert!(
            matches!(&result, Err(SyncError::Database(coven_database::DbError::Snapshot(
                coven_database::SnapshotError::MissingWrites { missing }
            ))) if missing.contains(&only)),
            "{result:?}"
        );
        assert!(snapshot_record(&d, &newer).await.is_some());
    }
    storage.create(&only_path, &bytes).await.unwrap();
    // Each record lasts while its object is listed.
    storage.delete(&newer).await.unwrap();
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(snapshot_record(&c, &newer).await, None);
    assert!(snapshot_record(&c, &malformed).await.is_some());
}

#[tokio::test]
async fn a_snapshot_in_a_newer_format_waits_for_an_update_while_the_logs_go_on() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let id = a.sync.write_snapshot(Audience::Store).await.unwrap();
    let path = ObjectPath::snapshot(
        store(1),
        id.audience,
        id.device,
        NonZeroU64::new(id.number).unwrap(),
    );
    let mut bytes = storage.read(&path).await.unwrap();
    bytes[2] = 2;
    storage.delete(&path).await.unwrap();
    storage.create(&path, &bytes).await.unwrap();
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    let reads = storage.reads().await.len();
    // The reload waits for the update instead of choosing without the newer
    // snapshot; the snapshot is its own record.
    assert!(matches!(
        b.sync.reload_from_snapshots().await,
        Err(SyncError::UpdateRequired(
            crate::RequiredUpdate::CovenFormat { version: 2 }
        ))
    ));
    let waits = Some(crate::PendingReason::UpdateRequired(
        crate::RequiredUpdate::CovenFormat { version: 2 },
    ));
    assert_eq!(snapshot_record(&b, &path).await, waits);
    // Not a verdict on its bytes: nothing past the prefix was read for it.
    assert!(!storage.reads().await[reads..]
        .iter()
        .any(|(read, start, _)| read == &path && *start > 0));
    // Write discovery from the logs goes on meanwhile.
    b.writes.download_writes().await.unwrap();
    assert_eq!(tables(&a).await, tables(&b).await);
    // Other work that lists the audience keeps the same record.
    b.sync.run_retention().await.unwrap();
    assert_eq!(snapshot_record(&b, &path).await, waits);
}
