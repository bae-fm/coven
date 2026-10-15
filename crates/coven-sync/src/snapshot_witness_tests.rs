//! A log object goes only once, for the store and each part audience, a
//! counting snapshot covering it landed strictly more than one storage day
//! ago, besides that audience's newest counting snapshot covering it (§15). The witness need
//! not be current: its evidence outlives its supersession, its deletion and
//! reopening.

use super::retention::post;
use super::*;
use coven_format::value::WritePositions;
use coven_storage::test_utils::MemoryRequest;
use std::collections::BTreeMap;
use std::time::SystemTime;

const DAY: Duration = Duration::from_secs(86400);

async fn logs(storage: &MemoryStorage) -> Vec<ObjectPath> {
    storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path)
        .collect()
}

/// Move storage and `devices`' clocks to `time` and have each observe it.
async fn at(clock: &FixedClock, time: SystemTime, devices: &[&Device]) {
    clock.set(time);
    for device in devices {
        device.clock.set(time);
        observe(device).await;
    }
}

async fn landed(storage: &MemoryStorage, id: &coven_format::store_log::SnapshotId) -> SystemTime {
    let path = crate::store_log_sync::snapshots::snapshot_path(store(1), id).unwrap();
    let stored = storage.status(&path).await.unwrap().unwrap().stored_at;
    UNIX_EPOCH + stored.elapsed_since_epoch()
}

/// §15: exactly one storage day after the covering snapshot landed the write
/// stays; one nanosecond later it goes (io `eligible_after_day`).
#[tokio::test]
async fn coverage_matures_one_unit_after_a_storage_day() {
    let (storage, clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    post(&a, &storage).await;
    clock.set(UNIX_EPOCH + Duration::new(5, 250_000_000));
    let id = a.write_snapshot(Audience::Store).await.unwrap();
    let landed = landed(&storage, &id).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(logs(&storage).await.len(), 1);
    at(&clock, landed + DAY, &[&a]).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(logs(&storage).await.len(), 1, "exactly one day is not more");
    at(&clock, landed + DAY + Duration::from_nanos(1), &[&a]).await;
    a.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
}

/// SP step 1: retention leaves the witness's maturity as the next threshold;
/// once the device clock passes it, the next pass's first phase observes
/// storage time, and that pass's retention deletes. Before, no observation.
#[tokio::test]
async fn a_pass_observes_storage_time_once_a_witness_may_have_matured() {
    let (storage, clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    post(&a, &storage).await;
    let id = a.write_snapshot(Audience::Store).await.unwrap();
    let landed = landed(&storage, &id).await;
    a.sync.run_retention().await.unwrap();
    let clocks = |from: usize| {
        storage.request_trace()[from..]
            .iter()
            .filter(|request| {
                matches!(request, MemoryRequest::Replace(path) if *path == ObjectPath::clock(store(1), DeviceId(1)))
            })
            .count()
    };
    let later = landed + DAY - Duration::from_secs(1);
    clock.set(later);
    a.clock.set(later);
    let before = storage.request_trace().len();
    a.sync().await;
    assert_eq!(clocks(before), 0);
    let later = landed + DAY + Duration::from_secs(1);
    clock.set(later);
    a.clock.set(later);
    let before = storage.request_trace().len();
    a.sync().await;
    assert_eq!(clocks(before), 1);
    a.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
}

/// §15: Ana's first snapshot covers her write 1 and lands on day 0; her
/// second covers writes 1 and 2 and lands at noon, and she deletes the first
/// as superseded. Write 1 still goes after day 1, by the deleted snapshot's
/// age, even across reopening; write 2 waits for the second snapshot's day.
/// Ben never checked the first snapshot's prefix: his write it covered
/// waits for a known witness, the second snapshot, to mature (io
/// `maturity_survives_new_snapshots`; SP "What survives a pass").
#[tokio::test]
async fn a_deleted_witness_keeps_its_age_for_the_device_that_checked_it() {
    let (storage, clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&b, 10, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.catch_up().await;
    let first = a.write_snapshot(Audience::Store).await.unwrap();
    let first_landed = landed(&storage, &first).await;
    post(&a, &storage).await;
    // Ana's retention checks the first snapshot's prefix; it is not mature.
    a.sync.run_retention().await.unwrap();
    assert_eq!(logs(&storage).await.len(), 2);
    at(&clock, first_landed + DAY / 2, &[&a, &b]).await;
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.catch_up().await;
    let second = a.write_snapshot(Audience::Store).await.unwrap();
    let second_landed = landed(&storage, &second).await;
    b.writes.download_writes().await.unwrap();
    post(&a, &storage).await;
    post(&b, &storage).await;
    a.sync.run_retention().await.unwrap();
    let first_path = crate::store_log_sync::snapshots::snapshot_path(store(1), &first).unwrap();
    assert!(storage.status(&first_path).await.unwrap().is_none());
    a.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    at(
        &clock,
        first_landed + DAY + Duration::from_secs(1),
        &[&a, &b],
    )
    .await;
    a.sync.run_retention().await.unwrap();
    b.sync.run_retention().await.unwrap();
    let ana = a.device().await;
    let kept: Vec<_> = logs(&storage)
        .await
        .into_iter()
        .map(|path| path.write_id().unwrap())
        .collect();
    assert_eq!(
        kept,
        [
            coven_merge::WriteId {
                device: ana,
                number: 2
            },
            coven_merge::WriteId {
                device: b.device().await,
                number: 1
            }
        ]
    );
    at(
        &clock,
        second_landed + DAY + Duration::from_secs(1),
        &[&a, &b],
    )
    .await;
    a.sync.run_retention().await.unwrap();
    b.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
}

/// §15: a write with a store part and a Gifts part needs a mature witness in
/// each audience; the store's landing a day earlier is not enough.
#[tokio::test]
async fn each_part_audience_needs_its_own_mature_witness() {
    let (storage, clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
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
    let store_snapshot = a.write_snapshot(Audience::Store).await.unwrap();
    let store_landed = landed(&storage, &store_snapshot).await;
    a.sync.run_retention().await.unwrap();
    at(&clock, store_landed + DAY / 2, &[&a]).await;
    a.catch_up().await;
    let gifts_snapshot = a.write_snapshot(Audience::Circle(gifts)).await.unwrap();
    let gifts_landed = landed(&storage, &gifts_snapshot).await;
    post(&a, &storage).await;
    at(&clock, store_landed + DAY + Duration::from_secs(1), &[&a]).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(logs(&storage).await.len(), 1);
    at(&clock, gifts_landed + DAY + Duration::from_secs(1), &[&a]).await;
    a.sync.run_retention().await.unwrap();
    assert!(logs(&storage).await.is_empty());
}

/// A small deterministic generator for the differential's histories.
struct Histories(u64);

impl Histories {
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % below
    }
}

/// io `SnapshotPublication.retainWrites` with `Eligible` on concurrent
/// histories: writes of two writers with parts in one or two audiences, the
/// store holding every write,
/// counting and late snapshot landings in any order, incomparable
/// selections, reader posts, per-writer finality and passes across the
/// one-day and thirty-day thresholds. Lean keeps every landing; coven keeps
/// only the listed snapshots plus its witness evidence, and a superseded
/// snapshot leaves the listing once another listed one covers everything it
/// covers. Every snapshot is listed at some pass before it leaves, so both
/// sides delete exactly the same writes at the same passes, and every
/// deleted write stays covered by its holders' current newest
/// (`SnapshotHistory.deleted_stays_covered`).
#[test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
fn io_differential_retain_writes() {
    use crate::store_log_sync::snapshots::catalog::newest_first;
    use crate::store_log_sync::snapshots::retention::waits::{reached_age, Facts};
    use crate::store_log_sync::snapshots::retention::witness::{coverage, holders, Coverage};
    use coven_format::sealed_snapshot::SnapshotObjectPrefix;
    use coven_format::value::{EntryId, EntryPositions, StorageTime};
    use serde_json::json;
    let audience = |n: u64| {
        if n == 0 {
            Audience::Store
        } else {
            Audience::Circle(crate::store_log_sync::tests::circle(1))
        }
    };
    let time = |seconds: u64| StorageTime::new(seconds, 0).unwrap();
    let writers = [1u64, 2];
    let mut cases = Vec::new();
    let mut expected = Vec::new();
    let mut random = Histories(0x9e37_79b9_7f4a_7c15);
    for _ in 0..150 {
        let mut now = 0u64;
        let mut counts = [0u64; 2];
        // (writer, number, stored, audiences)
        let mut writes: Vec<(u64, u64, u64, Vec<u64>)> = Vec::new();
        // Listed snapshots: (object, prefix, audience), and whether a pass saw them.
        let mut listed: Vec<(coven_storage::StoredObject, SnapshotObjectPrefix, u64, bool)> =
            Vec::new();
        let mut numbers = 0u64;
        let mut witnesses = coven_database::SnapshotWitnesses::default();
        let mut deleted: Vec<(u64, u64)> = Vec::new();
        let mut events = Vec::new();
        let mut passes = Vec::new();
        for _ in 0..24 {
            now += [0, 600, 43_200, 86_400, 86_401, 30 * 86_400][random.next(6) as usize];
            match random.next(4) {
                0 => {
                    let writer = random.next(2) as usize;
                    counts[writer] += 1;
                    let audiences = match random.next(3) {
                        0 => vec![0],
                        1 => vec![1],
                        _ => vec![0, 1],
                    };
                    events.push(json!({"op": "write", "writer": writers[writer],
                        "number": counts[writer], "stored": now, "audiences": audiences}));
                    writes.push((writers[writer], counts[writer], now, audiences));
                }
                1 => {
                    let part = random.next(2);
                    let mut positions = Vec::new();
                    for (index, writer) in writers.iter().enumerate() {
                        let reach = random.next(counts[index] + 1);
                        if reach > 0 {
                            positions.push((*writer, reach));
                        }
                    }
                    let caught = now.saturating_sub([0, 3600, 86_401][random.next(3) as usize]);
                    numbers += 1;
                    let author = writers[random.next(2) as usize];
                    events.push(json!({"op": "land", "audience": part, "writer": author,
                        "number": numbers, "stored": now, "caught": caught,
                        "positions": positions}));
                    listed.push((
                        coven_storage::StoredObject {
                            path: ObjectPath::snapshot(
                                store(1),
                                audience(part),
                                DeviceId(author),
                                numbers.try_into().unwrap(),
                            ),
                            size: 0,
                            stored_at: time(now),
                            time_resolution: Duration::from_secs(1),
                            object_id: "id".into(),
                            revision: "1".into(),
                            checksum: None,
                        },
                        SnapshotObjectPrefix {
                            audience: audience(part),
                            key: key(1),
                            caught_at: time(caught),
                            raise: None,
                            writes: WritePositions(
                                positions
                                    .iter()
                                    .map(|(writer, number)| coven_merge::WriteId {
                                        device: DeviceId(*writer),
                                        number: *number,
                                    })
                                    .collect(),
                            ),
                            store_log: EntryPositions(Vec::new()),
                        },
                        part,
                        false,
                    ));
                }
                2 => {
                    // Its author deletes a superseded snapshot a pass saw:
                    // another listed counting one covers all it covers.
                    let superseded = listed.iter().position(|(_, prefix, part, seen)| {
                        *seen
                            && listed.iter().any(|(other, newer, other_part, _)| {
                                other_part == part
                                    && newer.counts(other.stored_at)
                                    && newer.writes != prefix.writes
                                    && prefix.writes.0.iter().all(|id| newer.writes.covers(*id))
                            })
                    });
                    if let Some(index) = superseded {
                        listed.remove(index);
                    }
                }
                _ => {
                    let posted: Vec<(u64, u64, u64)> = writers
                        .iter()
                        .flat_map(|reader| {
                            writers
                                .iter()
                                .enumerate()
                                .map(|(index, writer)| {
                                    (*reader, *writer, random.next(counts[index] + 1))
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    let finals: Vec<u64> = writers
                        .iter()
                        .copied()
                        .filter(|_| random.next(4) != 0)
                        .collect();
                    let nested: Vec<_> = posted
                        .iter()
                        .map(|(reader, writer, position)| json!([reader, [writer, position]]))
                        .collect();
                    events.push(json!({"op": "pass", "time": now, "posted": nested,
                        "finals": finals}));
                    // The listing this pass checks becomes witness evidence.
                    let mut held: BTreeMap<Audience, Vec<WritePositions>> = BTreeMap::new();
                    for part in [0, 1] {
                        let mut counting: Vec<_> = listed
                            .iter_mut()
                            .filter(|(object, prefix, p, _)| {
                                *p == part && prefix.counts(object.stored_at)
                            })
                            .collect();
                        for (object, prefix, _, seen) in counting.iter_mut() {
                            witnesses.record(&audience(part), &prefix.writes, object.stored_at);
                            *seen = true;
                        }
                        witnesses.prune(time(now));
                        counting.sort_by(|a, b| newest_first((&a.0, &a.1), (&b.0, &b.1)));
                        if let Some((_, newest, _, _)) = counting.first() {
                            held.insert(audience(part), vec![newest.writes.clone()]);
                        }
                    }
                    let mut fresh = Vec::new();
                    for (writer, number, stored, audiences) in &writes {
                        if deleted.contains(&(*writer, *number)) {
                            continue;
                        }
                        let id = coven_merge::WriteId {
                            device: DeviceId(*writer),
                            number: *number,
                        };
                        let facts = Facts {
                            dependencies: vec![(
                                EntryId {
                                    device: DeviceId(*writer),
                                    number: 1,
                                },
                                finals.contains(writer),
                            )],
                            readers: writers
                                .iter()
                                .map(|reader| {
                                    let position = posted
                                        .iter()
                                        .find(|(r, w, _)| r == reader && w == writer)
                                        .map_or(0, |(_, _, p)| *p);
                                    (DeviceId(*reader), position >= *number)
                                })
                                .collect(),
                            aged: reached_age(Some(time(now)), time(*stored)),
                        };
                        let parts: Vec<_> = audiences.iter().map(|n| audience(*n)).collect();
                        if coverage(holders(&parts), id, &held, &witnesses, Some(time(now)))
                            == Coverage::Mature
                            && facts.first_unmet().is_none()
                        {
                            fresh.push((*writer, *number));
                        }
                    }
                    deleted.extend(&fresh);
                    // Every deleted write stays covered by its holders' current
                    // newest counting snapshots.
                    for (writer, number, _, audiences) in &writes {
                        if deleted.contains(&(*writer, *number)) {
                            let parts: Vec<_> = audiences.iter().map(|n| audience(*n)).collect();
                            for holder in holders(&parts) {
                                assert!(held.get(holder).is_some_and(|selections| {
                                    selections[0].covers(coven_merge::WriteId {
                                        device: DeviceId(*writer),
                                        number: *number,
                                    })
                                }));
                            }
                        }
                    }
                    passes.push(fresh);
                }
            }
        }
        cases.push(json!({"check": "retain", "writers": writers, "events": events}));
        expected.push(passes);
    }
    let lean = crate::discovery::tests::io_runner(&cases);
    let mut deletions = 0;
    for ((case, passes), actual) in cases.iter().zip(&lean).zip(&expected) {
        let lean: Vec<Vec<(u64, u64)>> = serde_json::from_value(passes.clone()).unwrap();
        let mut sorted: Vec<Vec<(u64, u64)>> = actual.clone();
        let mut lean_sorted = lean.clone();
        for pass in sorted.iter_mut().chain(lean_sorted.iter_mut()) {
            pass.sort();
        }
        assert_eq!(sorted, lean_sorted, "{case}");
        deletions += lean.iter().map(Vec::len).sum::<usize>();
    }
    assert!(deletions > 20, "the histories must exercise deletion");
}

/// §3's bounded storage, against io `AudienceRetention.bounded_storage`: Ana,
/// the only reader, posts her one small write at once, so it is gated from
/// its storage time, and every pass runs at a fixed spacing with publication
/// and deletion completing within the pass (`U = D = 0`). No snapshot covers
/// it at first; retention asks for each part audience's first one, which
/// lands by that audience's `B_a`, and the write goes by `R + 3P + U + D`,
/// with each part's witness strictly more than a day old and its newest
/// snapshot covering it. A single-audience write and a store-and-Gifts write.
#[tokio::test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
async fn io_differential_storage_deadline() {
    use serde_json::json;
    let mut cases = Vec::new();
    let mut observed = Vec::new();
    for pass_gap in [60u64, 3600, 6 * 3600] {
        for shared in [false, true] {
            let (storage, clock) = timed_snapshot_storage();
            let mut a = notes_device(storage.clone(), 1).await;
            a.create(key(1)).await;
            let gifts = crate::store_log_sync::tests::circle(1);
            let mut audiences = vec![Audience::Store];
            if shared {
                a.sync
                    .make_and_upload_entry(StoreChange::CreateCircle {
                        circle: gifts,
                        name: "Gifts".into(),
                        key: key(2),
                        key_hash: a.sync.test_key(Audience::Circle(gifts), key(2)),
                    })
                    .await
                    .unwrap();
                audiences.push(Audience::Circle(gifts));
            }
            let texts: Vec<String> = audiences
                .iter()
                .map(|audience| match audience {
                    Audience::Store => "store".to_owned(),
                    Audience::Circle(circle) => circle.to_string(),
                })
                .collect();
            a.db.write(move |sql| {
                for audience in &texts {
                    sql.execute(
                        "INSERT INTO notes VALUES('1',?1,'note',x'00')",
                        coven_database::params![audience],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
            upload(&a, &storage).await;
            post(&a, &storage).await;
            let stored = storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()[0]
                .stored_at
                .seconds();
            let mut now = stored;
            let mut landings: BTreeMap<Audience, u64> = BTreeMap::new();
            let deleted = loop {
                now += pass_gap;
                assert!(now < stored + 4 * 86400, "the write must go");
                clock.set(UNIX_EPOCH + Duration::from_secs(now));
                a.clock.set(UNIX_EPOCH + Duration::from_secs(now));
                a.sync().await;
                {
                    let _pass = a.caught_up_pass().await;
                    a.sync.write_snapshots().await.unwrap();
                    a.sync.run_retention().await.unwrap();
                }
                for object in storage
                    .list(&ObjectPrefix::snapshots(store(1)))
                    .await
                    .unwrap()
                {
                    let audience = object.path.snapshot_id().unwrap().audience;
                    landings
                        .entry(audience)
                        .or_insert(object.stored_at.seconds());
                }
                if logs(&storage).await.is_empty() {
                    break now;
                }
            };
            let online: Vec<_> = (0..audiences.len()).map(|a| json!([a, 0])).collect();
            cases.push(
                json!({"check": "deadline", "stored": stored, "gated": stored,
                "passGap": pass_gap, "upload": 0, "latency": 0, "online": online}),
            );
            observed.push((
                deleted,
                audiences
                    .iter()
                    .map(|audience| landings[audience])
                    .collect::<Vec<_>>(),
            ));
        }
    }
    let lean = crate::discovery::tests::io_runner(&cases);
    for ((deleted, landings), bound) in observed.iter().zip(&lean) {
        assert!(
            *deleted <= bound["deadline"].as_u64().unwrap(),
            "{deleted} {bound}"
        );
        for (landed, part) in landings.iter().zip(bound["parts"].as_array().unwrap()) {
            assert!(*landed <= part["B"].as_u64().unwrap(), "{landed} {part}");
            // The write went only once that witness was more than a day old.
            assert!(*deleted > landed + 86400, "{deleted} {landed}");
        }
    }
}
