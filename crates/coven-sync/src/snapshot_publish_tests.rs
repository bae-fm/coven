//! A snapshot is prepared only right after a completed catch-up sampled
//! fresh, must cover its audience's current newest counting snapshot, is sent
//! once its own captured prefix is confirmed, and is abandoned for a new path
//! once it can no longer count (§15, SP step 8).
use super::*;
use crate::snapshot_data::AbandonedLanding;
use coven_format::sealed_snapshot::SnapshotObjectPrefix;
use coven_format::store_log::SnapshotId;
use coven_format::value::StorageTime;
use coven_merge::WriteId;

const DAY: u64 = StorageTime::DAY_SECONDS;

#[path = "snapshot_abandoned_tests.rs"]
mod abandoned;

/// Ana and her second device Ben in one store, on storage whose provider
/// time the test sets.
async fn pair() -> (Arc<MemoryStorage>, Arc<FixedClock>, Device, Device) {
    let (storage, clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    (storage, clock, a, b)
}

/// Every listed store snapshot with its signed prefix and landing time.
async fn listed(storage: &MemoryStorage) -> Vec<(ObjectPath, SnapshotObjectPrefix, StorageTime)> {
    let mut snapshots = Vec::new();
    for object in storage
        .list(&ObjectPrefix::audience_snapshots(
            store(1),
            &Audience::Store,
        ))
        .await
        .unwrap()
    {
        let bytes = storage.read(&object.path).await.unwrap();
        let length = SnapshotObjectPrefix::signed_length(&bytes).unwrap();
        let (prefix, _) = SnapshotObjectPrefix::decode_signed(&bytes[..length]).unwrap();
        snapshots.push((object.path, prefix, object.stored_at));
    }
    snapshots
}

/// The counting store snapshot every reader selects (§15, io `catalogCurrent`).
async fn current(storage: &MemoryStorage) -> Option<ObjectPath> {
    let mut counting: Vec<_> = listed(storage)
        .await
        .into_iter()
        .filter(|(_, prefix, landed)| prefix.counts(*landed))
        .collect();
    counting.sort_by(|(a, a_prefix, _), (b, b_prefix, _)| {
        let size = |path: &ObjectPath| coven_storage::StoredObject {
            path: path.clone(),
            size: 0,
            stored_at: StorageTime::since_epoch(Duration::ZERO),
            time_resolution: Duration::from_secs(1),
            object_id: String::new(),
            revision: String::new(),
            checksum: None,
        };
        crate::store_log_sync::snapshots::catalog::newest_first(
            (&size(a), a_prefix),
            (&size(b), b_prefix),
        )
    });
    counting.into_iter().next().map(|(path, _, _)| path)
}

fn set_time(clock: &FixedClock, seconds: u64) {
    clock.set(UNIX_EPOCH + Duration::from_secs(seconds));
}

/// Start a requested snapshot without driving it.
async fn request(device: &Device) -> crate::OperationId {
    let data = Data::Snapshots(SnapshotTask {
        job: SnapshotJob::Write {
            audience: Audience::Store,
            device: device.device().await,
            trigger: crate::snapshot_data::SnapshotTrigger::Requested,
            attempt: None,
            abandoned: Vec::new(),
        },
        temporary: Vec::new(),
    });
    device
        .db
        .start_operation(data.new_operation("coven").unwrap())
        .await
        .unwrap()
}

/// Run one step of a snapshot task; `None` once it finished.
async fn step(device: &mut Device, id: crate::OperationId) -> Option<Progress> {
    let record = device
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == id)?;
    Some(
        device
            .sync
            .operation_step(&record, Data::read(&record).unwrap())
            .await
            .unwrap(),
    )
}

/// The task's last completed step and its prepared attempt, if it remains.
async fn task(device: &Device, id: crate::OperationId) -> Option<(u32, Option<SnapshotId>)> {
    let record = device
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == id)?;
    let Data::Snapshots(task) = Data::read(&record).unwrap() else {
        panic!("a snapshot task")
    };
    Some((record.last_step, task.published()))
}

fn waits(error: SyncError) -> crate::PendingReason {
    match error {
        SyncError::Pending(pending) => pending.reason,
        other => panic!("expected a pending snapshot, got {other:?}"),
    }
}

fn path_of(id: &SnapshotId) -> ObjectPath {
    ObjectPath::snapshot(
        store(1),
        id.audience.clone(),
        id.device,
        NonZeroU64::new(id.number).unwrap(),
    )
}

/// A catch-up that stops at a refused or unreadable write is not complete:
/// nothing is prepared until one completes, and the snapshot then covers it.
#[tokio::test]
async fn preparation_waits_for_a_complete_catch_up() {
    let (storage, _clock, mut a, b) = pair().await;
    write_rows(&b, 0, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    let ben = b.device().await;
    let write = crate::write_seal::path(
        store(1),
        WriteId {
            device: ben,
            number: 1,
        },
    );
    // A failed read leaves the catch-up unfinished.
    let mut faults = coven_storage::test_utils::Faults::none();
    faults
        .read_failures
        .insert(write.clone(), coven_storage::StorageFailure::Network);
    storage.set_faults(faults).await;
    assert!(a.write_snapshot(Audience::Store).await.is_err());
    assert!(listed(&storage).await.is_empty());
    storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    let id = a.write_snapshot(Audience::Store).await.unwrap();
    let (_, prefix, _) = listed(&storage)
        .await
        .into_iter()
        .find(|(path, _, _)| *path == path_of(&id))
        .unwrap();
    assert!(prefix.writes.covers(WriteId {
        device: ben,
        number: 1
    }));
    // A write this device refuses stops its log: no later catch-up completes.
    write_rows(&b, 1, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    let second = crate::write_seal::path(
        store(1),
        WriteId {
            device: ben,
            number: 2,
        },
    );
    let size = storage.status(&second).await.unwrap().unwrap().size;
    storage
        .corrupt_byte(&second, size as usize - 1)
        .await
        .unwrap();
    assert_eq!(
        waits(a.write_snapshot(Audience::Store).await.unwrap_err()),
        crate::PendingReason::Waits(crate::Prerequisite::CatchUp)
    );
    assert_eq!(listed(&storage).await.len(), 1);
}

/// A writer registered since this device's last pass is read in the fresh
/// catch-up before preparing, so the snapshot covers its writes.
#[tokio::test]
async fn a_writer_registered_since_the_last_pass_is_read_before_preparing() {
    let (storage, _clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    a.sync().await;
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    write_rows(&c, 0, 2, 17, Audience::Store).await;
    upload(&c, &storage).await;
    let carol = c.device().await;
    let id = a.write_snapshot(Audience::Store).await.unwrap();
    let (_, prefix, _) = listed(&storage)
        .await
        .into_iter()
        .find(|(path, _, _)| *path == path_of(&id))
        .unwrap();
    assert!(prefix.writes.covers(WriteId {
        device: carol,
        number: 1
    }));
}

/// Capture may precede own-upload confirmation, but the snapshot is sent
/// only once every own write through its captured position is stored; an
/// app write made after the capture stays outside its cut and does not hold
/// it back (§15; io `snapshot_covers_published`).
#[tokio::test]
async fn own_writes_through_the_cut_are_confirmed_before_sending() {
    let (storage, _clock, mut a, _b) = pair().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    let ana = a.device().await;
    let first = crate::write_seal::path(
        store(1),
        WriteId {
            device: ana,
            number: 1,
        },
    );
    assert_eq!(
        waits(a.write_snapshot(Audience::Store).await.unwrap_err()),
        crate::PendingReason::Waits(crate::Prerequisite::Object(first))
    );
    assert!(listed(&storage).await.is_empty());
    upload(&a, &storage).await;
    // A later app write is queued, unsent, when the snapshot is sent.
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    a.sync.sync_store_log().await.unwrap();
    let snapshots = listed(&storage).await;
    assert_eq!(snapshots.len(), 1);
    let writes = &snapshots[0].1.writes;
    assert!(writes.covers(WriteId {
        device: ana,
        number: 1
    }));
    assert!(!writes.covers(WriteId {
        device: ana,
        number: 2
    }));
    assert_eq!(
        a.db.unconfirmed_own_prefix(WriteId {
            device: ana,
            number: 2
        })
        .await
        .unwrap(),
        Some(WriteId {
            device: ana,
            number: 2
        })
    );
}

/// A prepared attempt survives a restart: its retry sends the same sealed
/// bytes at the same path with the same signed `T_c` (§15).
#[tokio::test]
async fn a_prepared_attempt_resumes_after_a_restart_with_its_path_and_sample() {
    let (storage, clock, mut a, _b) = pair().await;
    set_time(&clock, 1_000);
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    let id = request(&a).await;
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (prepared, Some(published)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    assert_eq!(prepared, 1);
    a.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    set_time(&clock, 2_000);
    upload(&a, &storage).await;
    a.sync.sync_store_log().await.unwrap();
    assert!(task(&a, id).await.is_none());
    let snapshots = listed(&storage).await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].0, path_of(&published));
    assert_eq!(
        snapshots[0].1.caught_at,
        StorageTime::since_epoch(Duration::from_secs(1_000))
    );
    assert!(snapshots[0].1.counts(snapshots[0].2));
}

/// A counting snapshot landing between the catch-up and the preparation
/// that covers a write the catch-up did not read holds the preparation back
/// until a later catch-up reads it; a snapshot landing after preparation does
/// not, and both then count (§15; io `SnapshotPublication.canPrepare`).
#[tokio::test]
async fn an_arrival_before_preparation_waits_and_one_after_does_not() {
    let (storage, _clock, mut a, mut b) = pair().await;
    let ben = b.device().await;
    let id = request(&a).await;
    {
        let _pass = a.caught_up_pass().await;
        write_rows(&b, 0, 1, 17, Audience::Store).await;
        upload(&b, &storage).await;
        b.write_snapshot(Audience::Store).await.unwrap();
        assert!(matches!(
            step(&mut a, id).await,
            Some(Progress::Waiting(crate::PendingReason::Waits(
                crate::Prerequisite::CatchUp
            )))
        ));
    }
    assert_eq!(task(&a, id).await, Some((0, None)));
    // The next catch-up reads Ben's write; preparing then covers his snapshot.
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (_, Some(prepared)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    // Ben's next snapshot lands after Ana's preparation and covers a write
    // hers lacks; hers still sends and counts.
    write_rows(&b, 1, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    let newer = b.write_snapshot(Audience::Store).await.unwrap();
    while step(&mut a, id).await.is_some() {}
    let snapshots = listed(&storage).await;
    let ana = snapshots
        .iter()
        .find(|(path, _, _)| *path == path_of(&prepared))
        .unwrap();
    assert!(ana.1.counts(ana.2));
    assert!(ana.1.writes.covers(WriteId {
        device: ben,
        number: 1
    }));
    assert!(!ana.1.writes.covers(WriteId {
        device: ben,
        number: 2
    }));
    assert_eq!(current(&storage).await, Some(path_of(&newer)));
}

/// A landing exactly one day after the signed sample counts. Once an
/// observed storage time passes that day before sending, the attempt is
/// abandoned without a request, and a fresh catch-up prepares a new path.
/// Never sent, it leaves nothing to settle.
#[tokio::test]
async fn an_attempt_counts_through_its_day_and_is_abandoned_after_it() {
    let (storage, clock, mut a, _b) = pair().await;
    set_time(&clock, 10_000);
    let id = request(&a).await;
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (_, Some(timely)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    set_time(&clock, 10_000 + DAY);
    while step(&mut a, id).await.is_some() {}
    assert_eq!(current(&storage).await, Some(path_of(&timely)));
    // The next attempt's sample is older than a day when Ana observes the time.
    let id = request(&a).await;
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (_, Some(expired)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    set_time(&clock, 20_000 + 2 * DAY);
    a.sync
        .observe_clock(&a.db.local_store_log().await.unwrap())
        .await
        .unwrap();
    let before = storage.request_trace().len();
    assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    assert_eq!(task(&a, id).await, Some((0, None)));
    assert!(abandoned::recorded(&a, id).await.is_empty());
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|request| matches!(
            request,
            coven_storage::test_utils::MemoryRequest::Create(path) if *path == path_of(&expired)
        )));
    // A fresh catch-up prepares afresh at a new path.
    let fresh = a.write_snapshot(Audience::Store).await.unwrap();
    assert_ne!(fresh.number, expired.number);
    assert!(listed(&storage)
        .await
        .iter()
        .all(|(path, _, _)| *path != path_of(&expired)));
}

/// A request that lands more than a day after its sample does not count:
/// the device abandons it, readers ignore it whatever it covers, the device
/// deletes it in that operation, and prepares again after a fresh catch-up
/// (§15, `late_snapshot_ignored`, `late_deletion_unseen`).
#[tokio::test]
async fn a_late_landing_is_abandoned_deleted_and_ignored_by_readers() {
    late_landing(false).await;
}

/// The deletion resumes after a crash that left its object already gone.
#[tokio::test]
async fn a_late_landings_deletion_resumes_after_a_crash() {
    late_landing(true).await;
}

async fn late_landing(resumed: bool) {
    let (storage, clock, mut a, _b) = pair().await;
    set_time(&clock, 10_000);
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let timely = a.write_snapshot(Audience::Store).await.unwrap();
    write_rows(&a, 1, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let id = request(&a).await;
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (_, Some(late)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    // Ana has not observed the time, so the request is sent and lands late.
    set_time(&clock, 10_000 + DAY + 1);
    assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    // The abandoning operation records the late attempt for deletion.
    assert_eq!(task(&a, id).await, Some((0, None)));
    assert_eq!(
        abandoned::recorded(&a, id).await,
        vec![(late.number, AbandonedLanding::Late)]
    );
    let snapshots = listed(&storage).await;
    let landed = snapshots
        .iter()
        .find(|(path, _, _)| *path == path_of(&late))
        .unwrap();
    assert!(!landed.1.counts(landed.2));
    assert!(landed.1.writes.covers(WriteId {
        device: a.device().await,
        number: 2
    }));
    assert_eq!(current(&storage).await, Some(path_of(&timely)));
    // It deletes the never-counting snapshot before preparing afresh.
    // Run again after the object is gone, as after a crash between the
    // deletion and its record, it finishes the same way (§15 T221).
    if resumed {
        storage.delete(&path_of(&late)).await.unwrap();
    }
    // Preparing afresh then waits for a catch-up.
    assert!(matches!(step(&mut a, id).await, Some(Progress::Waiting(_))));
    assert_eq!(task(&a, id).await, Some((0, None)));
    assert!(abandoned::recorded(&a, id).await.is_empty());
    assert!(storage.status(&path_of(&late)).await.unwrap().is_none());
    assert_eq!(current(&storage).await, Some(path_of(&timely)));
    // Within the same pass the abandoned sample cannot serve again.
    {
        let _pass = a.caught_up_pass().await;
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
        assert!(matches!(step(&mut a, id).await, Some(Progress::Advanced)));
    }
    let (_, Some(again)) = task(&a, id).await.unwrap() else {
        panic!("a prepared attempt")
    };
    assert_ne!(again.number, late.number);
    while step(&mut a, id).await.is_some() {}
    assert_eq!(current(&storage).await, Some(path_of(&again)));
}

/// A reload invalidates the catch-up it followed: preparation needs another
/// (§15; io `SnapshotPublication.act`).
#[tokio::test]
async fn a_reload_invalidates_the_passes_catch_up() {
    let reads = crate::pass_reads::PassReads::default();
    let _scope = reads.enter();
    reads.keep_write_catch_up(StorageTime::since_epoch(Duration::from_secs(5)));
    assert!(reads.write_catch_up().is_some());
    reads.reloaded();
    assert_eq!(reads.write_catch_up(), None);
}

/// IO `SnapshotPublication.step`/`execute` over two devices' actual
/// publications: writes landing, a catch-up sampled fresh, preparation (with
/// the other device's write or whole snapshot landing in between, at the same
/// storage time), sending, and ticks. Rust's completed catch-up and the
/// positions it reached, its admission of each preparation, whether each
/// landing counts, and the counting selection after every event match the
/// model (`completed_catchup_covers_old`, `canPrepare`, `late_snapshot_ignored`).
/// A Rust attempt abandoned before sending lands nowhere, as an attempt the
/// model leaves uploading forever.
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER; scripts/check.sh supplies the IO runner"]
async fn io_differential_publication() {
    use serde_json::{json, Value};
    struct Rng(u64);
    impl Rng {
        fn pick(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }
    /// One device's requested snapshot task.
    async fn attempt(
        device: &mut Device,
        task_id: &mut Option<crate::OperationId>,
        time: u64,
        dummy: &mut u64,
        trace: &mut Vec<(Value, Value)>,
        interleave: impl std::future::Future<Output = Vec<(Value, Value)>>,
    ) {
        let writer = device.device().await.0;
        let id = match *task_id {
            Some(id) => id,
            None => *task_id.insert(request(device).await),
        };
        let pass = device.caught_up_pass().await;
        let complete = device.sync.reads.write_catch_up().is_some();
        let positions: Vec<(u64, u64)> = device
            .db
            .sync_state(Vec::new())
            .await
            .unwrap()
            .positions
            .0
            .iter()
            .filter(|p| p.number > 0)
            .map(|p| (p.device.0, p.number))
            .collect();
        let index = trace.len();
        trace.push((json!(null), json!({"ok": complete, "positions": positions})));
        trace.extend(interleave.await);
        let prepared = matches!(step(device, id).await, Some(Progress::Advanced));
        drop(pass);
        let number = match task(device, id).await {
            Some((1, Some(published))) if prepared => published.number,
            _ => {
                *dummy += 1;
                *dummy
            }
        };
        trace[index].0 =
            json!({"event": "catchup", "writer": writer, "number": number, "time": time});
        trace.push((
            json!({"event": "prepare", "writer": writer, "time": time}),
            json!({"ok": prepared}),
        ));
    }
    /// Send a prepared attempt and finish it; a landing reports whether it counts.
    async fn send(
        device: &mut Device,
        task_id: &mut Option<crate::OperationId>,
        storage: &MemoryStorage,
        time: u64,
        trace: &mut Vec<(Value, Value)>,
    ) {
        let Some(id) = *task_id else { return };
        let Some((1, Some(published))) = task(device, id).await else {
            return;
        };
        assert!(matches!(step(device, id).await, Some(Progress::Advanced)));
        if let Some((2, _)) = task(device, id).await {
            assert!(step(device, id).await.is_some());
        }
        if task(device, id).await.is_none() {
            *task_id = None;
        }
        let landed = listed(storage)
            .await
            .into_iter()
            .find(|(path, _, _)| *path == path_of(&published));
        if let Some((_, prefix, stored)) = landed {
            trace.push((
                json!({"event": "land", "writer": published.device.0, "number": published.number, "time": time}),
                json!({"ok": true, "counts": prefix.counts(stored)}),
            ));
        }
    }
    let mut seen = [0usize; 4];
    for seed in 0..24u64 {
        let mut rng = Rng(seed * 7_919 + 17);
        let (storage, clock, a, b) = pair().await;
        let mut devices = [a, b];
        let writers = [devices[0].device().await.0, devices[1].device().await.0];
        let mut tasks = [None, None];
        let (mut time, mut row, mut dummy) = (0u64, 0usize, 10_000u64);
        let mut trace: Vec<(Value, Value)> = Vec::new();
        let mut currents = Vec::new();
        for _ in 0..18 {
            let d = rng.pick(2) as usize;
            let (first, second) = devices.split_at_mut(1);
            let (device, other) = if d == 0 {
                (&mut first[0], &mut second[0])
            } else {
                (&mut second[0], &mut first[0])
            };
            let (own, others) = if d == 0 {
                let (x, y) = tasks.split_at_mut(1);
                (&mut x[0], &mut y[0])
            } else {
                let (x, y) = tasks.split_at_mut(1);
                (&mut y[0], &mut x[0])
            };
            let before = trace.len();
            match rng.pick(7) {
                0 | 1 => {
                    row += 1;
                    write_rows(device, row, 1, 17, Audience::Store).await;
                    upload(device, &storage).await;
                    trace.push((
                        json!({"event": "write", "writer": writers[d], "time": time}),
                        json!({"ok": true}),
                    ));
                }
                2 | 3 => {
                    if let Some(id) = *own {
                        if task(device, id).await.is_some_and(|(stage, _)| stage != 0) {
                            continue;
                        }
                    }
                    let between = rng.pick(3);
                    row += 1;
                    let storage = storage.clone();
                    let other_writer = writers[1 - d];
                    let mut spare = dummy + 5_000;
                    let interleave = async move {
                        let mut events = Vec::new();
                        match between {
                            1 => {
                                write_rows(other, row, 1, 17, Audience::Store).await;
                                upload(other, &storage).await;
                                events.push((
                                    json!({"event": "write", "writer": other_writer, "time": time}),
                                    json!({"ok": true}),
                                ));
                            }
                            2 if others.is_none() => {
                                write_rows(other, row, 1, 17, Audience::Store).await;
                                upload(other, &storage).await;
                                events.push((
                                    json!({"event": "write", "writer": other_writer, "time": time}),
                                    json!({"ok": true}),
                                ));
                                let mut nested = Vec::new();
                                attempt(other, others, time, &mut spare, &mut nested, async {
                                    Vec::new()
                                })
                                .await;
                                events.extend(nested);
                                send(other, others, &storage, time, &mut events).await;
                            }
                            _ => (),
                        }
                        events
                    };
                    attempt(device, own, time, &mut dummy, &mut trace, interleave).await;
                }
                4 | 5 => send(device, own, &storage, time, &mut trace).await,
                _ => {
                    time += [600, 6 * 3600, DAY, DAY + 1][rng.pick(4) as usize];
                    set_time(&clock, time);
                    trace.push((json!({"event": "tick", "time": time}), json!({"ok": true})));
                }
            }
            // The selection every reader makes once this action's events end.
            if trace.len() > before {
                let selected = current(&storage).await.map_or(Value::Null, |path| {
                    let id = path.snapshot_id().unwrap();
                    json!([id.device.0, id.number])
                });
                currents.resize(trace.len() - 1, None);
                currents.push(Some(selected));
            }
        }
        currents.resize(trace.len(), None);
        let events: Vec<_> = trace.iter().map(|(event, _)| event.clone()).collect();
        let lean = crate::discovery::tests::io_runner(&[
            json!({"check": "publication", "writers": writers, "events": events}),
        ])
        .remove(0);
        let lean = lean.as_array().unwrap();
        assert_eq!(lean.len(), trace.len());
        for (index, ((event, rust), model)) in trace.iter().zip(lean).enumerate() {
            let context = format!("seed {seed}, event {index} {event}: {events:?}");
            for (field, value) in rust.as_object().unwrap() {
                assert_eq!(&model[field], value, "{field}, {context}");
            }
            if let Some(selected) = &currents[index] {
                assert_eq!(&model["current"], selected, "current, {context}");
            }
            match event["event"].as_str().unwrap() {
                "prepare" if rust["ok"] == json!(true) => seen[0] += 1,
                "prepare" => seen[1] += 1,
                "land" if rust["counts"] == json!(true) => seen[2] += 1,
                "land" => seen[3] += 1,
                _ => (),
            }
        }
    }
    assert!(seen.iter().all(|n| *n > 0), "{seen:?}");
}

/// IO `SnapshotWrites` on ordinary snapshots of two devices: a capture may
/// hold own writes not yet stored, but its request is admitted only once its
/// own captured position is confirmed, and every number a landed snapshot
/// covers is then published (`snapshot_covers_published`, `pending_run`).
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER; scripts/check.sh supplies the IO runner"]
async fn io_differential_ordinary_snapshot_writes() {
    use serde_json::json;
    struct Rng(u64);
    impl Rng {
        fn pick(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }
    /// The positions an attempt's sealed prefix captured.
    fn captured(device: &Device, task: &SnapshotTask) -> Vec<(u64, u64)> {
        let bytes = device
            .directory
            .file(
                coven_foundation::files::FileArea::AppProvided,
                &coven_foundation::files::FileName::new(task.temporary[0].clone()).unwrap(),
            )
            .open_reader()
            .map(|reader| reader.read_at(0, reader.size() as usize).unwrap())
            .unwrap();
        let length = SnapshotObjectPrefix::length(&bytes).unwrap();
        SnapshotObjectPrefix::decode(&bytes[..length])
            .unwrap()
            .writes
            .0
            .iter()
            .map(|id| (id.device.0, id.number))
            .collect()
    }
    let mut requests = (0, 0);
    for seed in 0..32u64 {
        let mut rng = Rng(seed * 104_729 + 5);
        let (_storage, _clock, a, b) = pair().await;
        let mut devices = [a, b];
        let mut pending: [Option<(crate::OperationId, usize)>; 2] = [None, None];
        let (mut trace, mut admitted, mut captures, mut row) = (Vec::new(), Vec::new(), 0, 0);
        for _ in 0..16 {
            let i = rng.pick(2) as usize;
            let device = &mut devices[i];
            let writer = device.device().await.0;
            match rng.pick(5) {
                0 | 1 => {
                    row += 1;
                    write_rows(device, row, 1, 17, Audience::Store).await;
                    trace.push(json!({"event": "reserve", "writer": writer}));
                }
                2 => {
                    if !device.send.can_send() {
                        device.catch_up().await;
                    }
                    for _ in device.writes.upload_writes().await.unwrap() {
                        trace.push(json!({"event": "land", "writer": writer}));
                        trace.push(json!({"event": "confirm", "writer": writer}));
                    }
                }
                _ => {
                    let (id, index) = match pending[i] {
                        Some(waiting) => waiting,
                        None => {
                            let id = request(device).await;
                            let _pass = device.caught_up_pass().await;
                            if !matches!(step(device, id).await, Some(Progress::Advanced)) {
                                device.db.finish_operation(id).await.unwrap();
                                continue;
                            }
                            let record = device.operation(id).await;
                            let Data::Snapshots(task) = Data::read(&record).unwrap() else {
                                panic!("a snapshot task")
                            };
                            trace.push(json!({"event": "capture", "writer": writer,
                                "positions": captured(device, &task)}));
                            captures += 1;
                            (id, captures - 1)
                        }
                    };
                    step(device, id).await;
                    let sent = task(device, id).await.is_some_and(|(stage, _)| stage == 2);
                    trace.push(json!({"event": "request", "snapshot": index}));
                    admitted.push(sent);
                    if sent {
                        while step(device, id).await.is_some() {}
                        trace.push(json!({"event": "landSnapshot", "snapshot": index}));
                        pending[i] = None;
                    } else {
                        pending[i] = Some((id, index));
                    }
                }
            }
        }
        let lean = crate::discovery::tests::io_runner(&[
            json!({"check": "snapshot_writes", "events": trace}),
        ])
        .remove(0);
        let mut decisions = admitted.iter();
        for (event, result) in trace.iter().zip(lean.as_array().unwrap()) {
            match event["event"].as_str().unwrap() {
                "capture" => assert_eq!(result["captured"], json!(true), "seed {seed}"),
                "request" => {
                    let rust = *decisions.next().unwrap();
                    assert_eq!(result["admitted"], json!(rust), "seed {seed}: {trace:?}");
                    if rust {
                        requests.0 += 1;
                    } else {
                        requests.1 += 1;
                    }
                }
                "landSnapshot" => assert_eq!(result["published"], json!(true), "seed {seed}"),
                _ => {}
            }
        }
    }
    assert!(requests.0 > 0 && requests.1 > 0, "{requests:?}");
}

#[tokio::test]
async fn writing_resumes_after_each_recorded_step_with_identical_ciphertext() {
    'crashes: for crash_step in 0.. {
        let storage = snapshot_storage();
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        write_rows(&a, 0, 35, 32768, Audience::Store).await;
        upload(&a, &storage).await;
        let data = Data::Snapshots(SnapshotTask {
            job: SnapshotJob::Write {
                audience: Audience::Store,
                device: a.device().await,
                trigger: crate::snapshot_data::SnapshotTrigger::Growth,
                attempt: None,
                abandoned: Vec::new(),
            },
            temporary: Vec::new(),
        });
        let id =
            a.db.start_operation(data.new_operation("coven").unwrap())
                .await
                .unwrap();
        // Preparation follows this pass's fresh catch-up (§15).
        let pass = a.caught_up_pass().await;
        for _ in 0..crash_step {
            let record = a.operation(id).await;
            match a
                .sync
                .operation_step(&record, Data::read(&record).unwrap())
                .await
                .unwrap()
            {
                Progress::Advanced => (),
                Progress::Finished(crate::operations::Output::Unit) => break 'crashes,
                _ => panic!("unexpected snapshot progress"),
            }
        }
        let before = a.db.operations().await.unwrap().remove(0);
        let Data::Snapshots(task) = Data::read(&before).unwrap() else {
            panic!()
        };
        let fixed = if before.last_step > 0 {
            Some(
                a.directory
                    .file(
                        coven_foundation::files::FileArea::AppProvided,
                        &coven_foundation::files::FileName::new(task.temporary[0].clone()).unwrap(),
                    )
                    .open_reader()
                    .map(|reader| reader.read_at(0, reader.size() as usize).unwrap())
                    .unwrap(),
            )
        } else {
            None
        };
        drop(pass);
        a.reopen(storage.clone(), notes_tables(), notes_migrations())
            .await;
        a.catch_up().await;
        let start = storage.request_trace().len();
        a.write_snapshots().await.unwrap();
        // Confirmed upload is the last storage step: resuming after it sends
        // nothing and only finishes the operation (§18.1).
        if before.last_step >= 2 {
            assert!(!storage.request_trace()[start..]
                .iter()
                .any(|request| matches!(
                    request,
                    coven_storage::test_utils::MemoryRequest::Create(_)
                        | coven_storage::test_utils::MemoryRequest::Other(_)
                )));
        }
        let objects = storage
            .list(&ObjectPrefix::snapshots(store(1)))
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        if let Some(bytes) = fixed {
            assert_eq!(storage.read(&objects[0].path).await.unwrap(), bytes);
        }
        assert!(a.db.operations().await.unwrap().is_empty());
    }
}

/// An addition-only update before a breaking migration's raise publishes
/// moves the raise to the newer version: the capture taken again after Ben's
/// edit applies is at that schema, so its header and raise agree, it
/// publishes, and Ben reloads from it once updated (§17.1).
#[tokio::test]
async fn an_addition_before_the_raise_raises_to_the_newest_version() {
    use crate::device_log_sync::tests::schema::{initial, publish_raise, seed, sync_all, update};
    use crate::device_log_sync::tests::{group, sql, storage};
    let storage = storage();
    let mut devices = group(storage.clone(), 2).await;
    seed(&mut devices).await;
    update(&mut devices[0], storage.clone(), true, true).await;
    let rename = Migration::sql(2, "change", "ALTER TABLE notes RENAME COLUMN title TO name")
        .writes(|row| {
            row.rename_column("title", "name");
            Ok(())
        });
    let color = Migration::sql(3, "color", "ALTER TABLE notes ADD COLUMN color TEXT");
    let tables = vec![SyncedTable::new("notes", RowIdentity::SharedKey)];
    let migrations = vec![initial(), rename, color];
    devices[0]
        .reopen(storage.clone(), tables.clone(), migrations.clone())
        .await;
    sql(&devices[0].db, "UPDATE notes SET color='blue'").await;
    sql(&devices[1].db, "UPDATE notes SET title='Shopping'").await;
    devices[1].writes.upload_writes().await.unwrap();
    // The capture taken again takes in the color write, sent first (§15).
    publish_raise(&mut devices[0]).await;
    publish_raise(&mut devices[0]).await;
    let log = devices[0].db.store_log().await.unwrap();
    assert_eq!(log.replay.state.schema[&Audience::Store].number, 3);
    assert!(devices[0].db.raise_captures().await.unwrap().is_empty());
    devices[1].reopen(storage.clone(), tables, migrations).await;
    sync_all(&mut devices).await;
    let both = "SELECT name || color FROM notes";
    for device in &devices {
        let row = device
            .db
            .read(|sql| Ok(sql.query_row(both, [], |r| r.get::<_, String>(0))?));
        assert_eq!(row.await.unwrap(), "Shoppingblue".to_owned());
    }
}
