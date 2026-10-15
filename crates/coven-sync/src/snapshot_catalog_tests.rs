//! Choosing the audience's current newest counting snapshot, and the reload
//! that loads it or waits on it (§15, §19.1).

use super::*;
use coven_format::sealed_snapshot::SnapshotObjectPrefix;

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
                    stored_at: coven_format::value::StorageTime::new(*stored, 0).unwrap(),
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
                if prefix.counts(object.stored_at) {
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
async fn selection_orders_coverage_then_path_and_waits_on_a_damaged_newest() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    b.db.apply_downloaded::<crate::PendingRecord>(
        a.db.test_queued_writes().await.unwrap().remove(0).into(),
        coven_database::test_utils::published(coven_format::value::StorageTime::UNIX_EPOCH),
    )
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
    // A reader whose check of the newest body fails refuses it and waits on
    // it, rather than loading the older snapshot (storelog-data
    // `missingDecision`). Its verified prefix still counts (§19.1).
    let waits_on_newer = |result: Result<(), SyncError>| {
        assert!(
            matches!(&result, Err(SyncError::Pending(waiting)) if waiting.reason
                == crate::PendingReason::Waits(crate::Prerequisite::Object(newer.clone()))),
            "{result:?}"
        );
    };
    let mut c = notes_device(storage.clone(), 4).await;
    add_device(&mut c).await;
    waits_on_newer(c.sync.reload_from_snapshots().await);
    assert!(matches!(
        snapshot_record(&c, &newer).await,
        Some(crate::PendingReason::Refused(_))
    ));
    // Waiting again does not read the refused bytes again.
    let reads = storage.reads().await.len();
    c.elapse_retry_delays();
    waits_on_newer(c.sync.reload_from_snapshots().await);
    assert!(!storage.reads().await[reads..]
        .iter()
        .any(|(read, _, _)| read == &newer));
    assert!(tables(&c).await.is_empty());
    // A malformed clear prefix is refused by itself; it establishes nothing.
    let malformed = ObjectPath::snapshot(
        store(1),
        Audience::Store,
        a.device().await,
        NonZeroU64::new(999).unwrap(),
    );
    storage.create(&malformed, b"x").await.unwrap();
    c.elapse_retry_delays();
    waits_on_newer(c.sync.reload_from_snapshots().await);
    assert!(matches!(
        snapshot_record(&c, &malformed).await,
        Some(crate::PendingReason::Refused(crate::Refusal::Parse { .. }))
    ));
    // Without the write only the refused snapshot covers, a new reader's
    // reload waits on that snapshot instead of skipping the write. The
    // first attempt refuses the body; the second reads only its retained
    // prefix.
    let only = crate::write_seal::path(
        store(1),
        coven_merge::WriteId {
            device: a.device().await,
            number: 2,
        },
    );
    let bytes = storage.read(&only).await.unwrap();
    storage.delete(&only).await.unwrap();
    let mut d = notes_device(storage.clone(), 5).await;
    add_device(&mut d).await;
    for _ in 0..2 {
        waits_on_newer(d.sync.reload_from_snapshots().await);
        assert!(snapshot_record(&d, &newer).await.is_some());
    }
    storage.create(&only, &bytes).await.unwrap();
    // Each record lasts while its object is listed. Once the damaged
    // snapshot is gone, the waiting reload loads the newest left and the
    // logs supply the rest.
    storage.delete(&newer).await.unwrap();
    c.elapse_retry_delays();
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(snapshot_record(&c, &newer).await, None);
    assert!(snapshot_record(&c, &malformed).await.is_some());
    c.writes.download_writes().await.unwrap();
    assert_eq!(tables(&a).await, tables(&c).await);
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

/// A reload parses each selected body once, inside the transaction that
/// loads it. A body that authenticates but fails the loader's checks there is
/// refused, and the reload waits on that snapshot rather than loading an
/// older one, without reading or parsing it again (§15, §19.1).
#[tokio::test]
async fn a_reload_parses_each_body_once_and_refuses_one_that_fails_to_load() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    let work = coven_database::test_utils::WorkCounter::default();
    b.db.test_track_work(work.clone()).await.unwrap();
    b.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(work.take().snapshot_reads, 1);
    assert_eq!(tables(&b).await, tables(&a).await);

    // A newer snapshot whose sealed plaintext lacks its last byte: its
    // signatures and chunks check, and only the loader finds it truncated.
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let id = coven_format::store_log::SnapshotId {
        audience: Audience::Store,
        device: a.device().await,
        number: 999,
    };
    let newer = ObjectPath::snapshot(
        store(1),
        Audience::Store,
        id.device,
        NonZeroU64::new(id.number).unwrap(),
    );
    let unsigned = super::signatures::sealed_snapshot(&a, id, |plain| {
        plain.pop();
    })
    .await;
    storage
        .create(
            &newer,
            &super::signatures::signed_snapshot(&unsigned, &newer, &a.member, &a.member),
        )
        .await
        .unwrap();
    let waits_on_newer = |result: Result<(), SyncError>| {
        assert!(
            matches!(&result, Err(SyncError::Pending(waiting)) if waiting.reason
                == crate::PendingReason::Waits(crate::Prerequisite::Object(newer.clone()))),
            "{result:?}"
        );
    };
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    let work = coven_database::test_utils::WorkCounter::default();
    c.db.test_track_work(work.clone()).await.unwrap();
    waits_on_newer(c.sync.reload_from_snapshots().await);
    assert_eq!(work.take().snapshot_reads, 1);
    assert!(matches!(
        snapshot_record(&c, &newer).await,
        Some(crate::PendingReason::Refused(_))
    ));
    assert!(tables(&c).await.is_empty());
    let reads = storage.reads().await.len();
    c.elapse_retry_delays();
    waits_on_newer(c.sync.reload_from_snapshots().await);
    assert_eq!(work.take().snapshot_reads, 0);
    assert!(!storage.reads().await[reads..]
        .iter()
        .any(|(read, _, _)| read == &newer));
    // Once it is no longer listed, the waiting reload loads the newest left,
    // and so does the one this call requests: one parse each.
    storage.delete(&newer).await.unwrap();
    c.elapse_retry_delays();
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(work.take().snapshot_reads, 2);
    c.writes.download_writes().await.unwrap();
    assert_eq!(tables(&c).await, tables(&a).await);
}
