use super::*;
use coven_format::value::StorageTime;
use coven_foundation::clock::FixedClock;

const DAY: u64 = 24 * 60 * 60;
const START: u64 = 1_760_400_000;

/// Snapshot storage whose provider time the test controls.
fn timed_storage() -> (Arc<MemoryStorage>, Arc<FixedClock>) {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(START)));
    let storage = Arc::new(
        MemoryStorage::builder()
            .transfer_limits(65536, 65536)
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    (storage, clock)
}

fn snapshot_lists(storage: &MemoryStorage, before: usize) -> usize {
    storage.request_trace()[before..]
        .iter()
        .filter(|request| {
            matches!(request, coven_storage::test_utils::MemoryRequest::List(prefix)
                if prefix.as_str().contains("/snapshots/"))
        })
        .count()
}

#[tokio::test]
async fn a_new_device_discovers_snapshots_before_trusting_a_miss() {
    let (storage, _) = timed_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 2, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    assert_eq!(b.db.return_checkpoint().await.unwrap(), None);
    let before = storage.request_trace().len();
    b.sync.prepare_return().await.unwrap();
    // No checkpoint means new: list snapshots, and load the one that advances.
    assert_eq!(snapshot_lists(&storage, before), 1);
    assert_eq!(tables(&a).await, tables(&b).await);
    // That discovery now certifies misses for this session; nothing is saved
    // until a completed pass confirms its positions.
    let before = storage.request_trace().len();
    b.sync.prepare_return().await.unwrap();
    assert_eq!(snapshot_lists(&storage, before), 0);
    assert_eq!(b.db.return_checkpoint().await.unwrap(), None);
}

#[tokio::test]
async fn a_snapshot_that_does_not_advance_leaves_local_state_alone() {
    let (storage, _) = timed_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.writes.download_writes().await.unwrap();
    let local = tables(&b).await;
    let snapshot = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .remove(0)
        .path;
    let bytes = storage.read(&snapshot).await.unwrap();
    let signed = coven_format::sealed_snapshot::SnapshotObjectPrefix::length(&bytes).unwrap() + 64;
    let reads = storage.reads().await.len();
    b.sync.prepare_return().await.unwrap();
    // Only the signed prefix was read: no snapshot body and no reload.
    assert!(storage.reads().await[reads..]
        .iter()
        .all(|(path, start, length)| path != &snapshot || start + length <= signed as u64));
    assert!(b.sync.pending_reload().await.unwrap().is_none());
    assert_eq!(tables(&b).await, local);
}

#[tokio::test]
async fn returning_after_thirty_days_reloads_history_deleted_meanwhile() {
    let (storage, clock) = timed_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    b.db.save_return_checkpoint(StorageTime::new(START, 0).unwrap())
        .await
        .unwrap();
    // A recent return trusts its misses without listing snapshots.
    let before = storage.request_trace().len();
    b.sync.prepare_return().await.unwrap();
    assert_eq!(snapshot_lists(&storage, before), 0);
    write_rows(&a, 0, 2, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.clock
        .set(UNIX_EPOCH + Duration::from_secs(START + 31 * DAY));
    // Thirty-one storage days pass: retention ages logs by storage time.
    clock.set(UNIX_EPOCH + Duration::from_secs(START + 31 * DAY));
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.write_snapshot(Audience::Store).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
    // Ben returns 31 storage days later; a reopened session has no sample.
    b.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    let before = storage.request_trace().len();
    b.sync.prepare_return().await.unwrap();
    // The reopened session's identity evidence lists every snapshot before
    // the clock replacement it sends (§10); the return lists the audience's
    // snapshots after that fresh storage sample (§15).
    assert_eq!(snapshot_lists(&storage, before), 2);
    assert_eq!(tables(&a).await, tables(&b).await);
}

#[tokio::test]
async fn a_failed_storage_time_observation_certifies_no_miss() {
    let (storage, _) = timed_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    b.db.save_return_checkpoint(StorageTime::new(START, 0).unwrap())
        .await
        .unwrap();
    storage
        .set_faults(coven_storage::test_utils::Faults {
            fail_next: 1,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(b.sync.prepare_return().await.is_err());
    let _pass = b.sync.begin_pass(&mut b.writes);
    let discovery = b.writes.download_writes().await.unwrap();
    assert!(discovery.needs_snapshots);
    assert!(!discovery.complete);
    assert_eq!(
        b.db.return_checkpoint().await.unwrap(),
        Some(StorageTime::new(START, 0).unwrap())
    );
}

/// Lean `PositionPublication.late_step_nine_unconfirmed`: a pass reaching step
/// 9 more than five minutes after its catch-up started still confirms an
/// unchanged post from that pass's observation, sending nothing, and moves the
/// return checkpoint (`pass_confirms_unchanged`). A changed post still waits
/// for a fresh gate (`readings_store_alike`).
#[tokio::test]
async fn a_late_step_nine_confirms_an_unchanged_post_and_moves_the_checkpoint() {
    let (storage, _) = timed_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    a.sync().await;
    assert!(a.writes.post_positions().await.unwrap());
    let pass = a.sync.begin_pass(&mut a.writes);
    a.sync.sync_store_log().await.unwrap();
    a.writes.observe_positions().await.unwrap();
    a.sync.prepare_return().await.unwrap();
    let sample = a.sync.return_sample().expect("storage time sampled");
    let discovery = a.writes.download_writes().await.unwrap();
    assert!(discovery.complete);
    let now = coven_foundation::clock::Clock::now(a.clock.as_ref());
    a.clock.set(now + Duration::from_secs(301));
    assert!(!a.send.can_send());
    let before = storage.request_trace().len();
    a.sync
        .finish_pass(&mut a.writes, discovery.complete, Some(sample))
        .await
        .unwrap();
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|r| matches!(r, coven_storage::test_utils::MemoryRequest::Replace(_))));
    assert_eq!(a.db.return_checkpoint().await.unwrap(), Some(sample));
    drop(pass);
    // Its disappearance changes the post, which needs a send.
    let path = ObjectPath::positions(store(1), a.db.local_store_log().await.unwrap().device);
    storage.delete(&path).await.unwrap();
    a.writes.observe_positions().await.unwrap();
    assert!(a.writes.post_positions().await.is_err());
    assert!(storage.status(&path).await.unwrap().is_none());
}
