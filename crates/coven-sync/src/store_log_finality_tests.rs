use super::*;
use crate::store_log_sync::tests::{device, key, member, store};
use coven_foundation::clock::FixedClock;
use coven_foundation::id_source::DeviceId;
use coven_storage::test_utils::{Faults, MemoryStorage};
use coven_storage::ObjectPrefix;
use std::time::{Duration, UNIX_EPOCH};

mod listing {
    use super::*;
    use crate::store_log_sync::tests::Device;
    use coven_format::{
        store_log::{StoreChange, StoreLogEntry},
        value::{EntryId, EntryPositions},
    };

    fn second(n: u64) -> std::time::SystemTime {
        UNIX_EPOCH + Duration::from_secs(n)
    }

    async fn registration(
        owner: &Device,
        storage: &MemoryStorage,
        clock: &FixedClock,
        writer: u64,
        time: u64,
    ) -> EntryId {
        let creation = owner.log().await.entries[0].entry.clone();
        let entry = StoreLogEntry {
            position: EntryId {
                device: DeviceId(writer),
                number: 1,
            },
            timestamp: coven_merge::Timestamp::new(1000 + writer, 0, DeviceId(writer)).unwrap(),
            author: owner.member.member_id(),
            had_read: EntryPositions(vec![creation.position]),
            change: StoreChange::AddDevice {
                device: DeviceId(writer),
                name: format!("Writer {writer}"),
                replaces: None,
            },
        };
        let ring = owner.custody.read().unwrap().unwrap();
        let bytes = crate::store_log_object::seal(
            store(1),
            &entry,
            coven_format::FormatVersion::CURRENT,
            ring.store_key(key(1)).unwrap(),
            &owner.member,
        )
        .unwrap();
        clock.set(second(time));
        storage
            .create(
                &crate::store_log_object::path(store(1), entry.position),
                &bytes,
            )
            .await
            .unwrap();
        entry.position
    }

    async fn observe(owner: &Device, clock: &FixedClock, time: u64) {
        clock.set(second(time));
        owner
            .sync
            .observe_clock(&owner.db.local_store_log().await.unwrap())
            .await
            .unwrap();
        assert!(owner.sync.scan_store_log_once(false).await.unwrap());
    }

    #[tokio::test]
    async fn conforming_lag_excludes_unlisted_writers_until_they_enter_the_certified_prefix() {
        let clock = Arc::new(FixedClock::new(second(0)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .time_resolution(Duration::from_secs(1))
                .build()
                .unwrap(),
        );
        let mut owner = device(storage.clone(), 1, member(1), store(1)).await;
        owner.create(key(1)).await;
        let first = registration(&owner, &storage, &clock, 2, 3601).await;
        let second = registration(&owner, &storage, &clock, 3, 3602).await;
        storage
            .set_faults(Faults {
                listing_lag: Some(Duration::from_secs(5)),
                ..Faults::none()
            })
            .await;
        observe(&owner, &clock, 3603).await;
        assert_eq!(owner.log().await.entries.len(), 1);
        assert_eq!(
            owner.sync.finality.lock().unwrap().through(),
            Some((Duration::from_secs(2), Duration::from_secs(1)))
        );
        assert!(storage
            .status(&crate::store_log_object::path(store(1), second))
            .await
            .unwrap()
            .is_some());
        observe(&owner, &clock, 7203).await;
        let log = owner.log().await;
        assert_eq!(log.entries.len(), 3);
        assert_eq!(
            log.replay.entries[&first],
            coven_database::EntryOutcome::Kept
        );
        assert_eq!(
            log.replay.entries[&second],
            coven_database::EntryOutcome::Kept
        );
        assert_eq!(log.horizon, Some(Duration::ZERO));
    }

    #[tokio::test]
    async fn a_writer_at_the_listing_boundary_cannot_make_the_inclusive_window_quiet() {
        let clock = Arc::new(FixedClock::new(second(0)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .time_resolution(Duration::from_secs(1))
                .build()
                .unwrap(),
        );
        let mut owner = device(storage.clone(), 1, member(1), store(1)).await;
        owner.create(key(1)).await;
        let first = registration(&owner, &storage, &clock, 2, 1).await;
        let cutoff = 30 * 86400 + 2;
        let late = registration(&owner, &storage, &clock, 3, cutoff).await;
        storage
            .set_faults(Faults {
                listing_lag: Some(Duration::from_secs(3600)),
                ..Faults::none()
            })
            .await;
        observe(&owner, &clock, cutoff + 3600).await;
        let log = owner.log().await;
        assert_eq!(log.entries.len(), 2);
        assert_eq!(log.horizon, Some(Duration::from_secs(1)));
        assert!(!log.is_final(first));
        assert!(!log.replay.entries.contains_key(&late));
        observe(&owner, &clock, cutoff + 3601).await;
        let log = owner.log().await;
        assert_eq!(log.entries.len(), 3);
        assert_eq!(
            log.replay.entries[&late],
            coven_database::EntryOutcome::Dropped(coven_database::DropReason::LandedTooLate)
        );
        assert_eq!(log.horizon, Some(Duration::from_secs(1)));
        assert!(!log.is_final(first));
        owner.restart(storage.clone()).await;
        assert_eq!(owner.log().await, log);
        let reads = storage.reads().await.len();
        observe(&owner, &clock, cutoff + 3601).await;
        assert!(storage.reads().await[reads..]
            .iter()
            .all(|(path, _, _)| path.store_log_position().is_none()));
        assert_eq!(owner.log().await, log);
    }

    #[tokio::test]
    async fn unreadable_entries_block_only_the_prefix_that_contains_their_publication() {
        let clock = Arc::new(FixedClock::new(second(0)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .time_resolution(Duration::from_secs(1))
                .build()
                .unwrap(),
        );
        let mut owner = device(storage.clone(), 1, member(1), store(1)).await;
        let creation = owner.create(key(1)).await;
        let through = 40 * 86400;
        clock.set(second(through + 1));
        let truncated = coven_format::sealed_single::SingleChunkPrefix::StoreLog { key: key(1) }
            .encode()
            .unwrap();
        storage
            .create(
                &ObjectPath::store_log(store(1), DeviceId(2), std::num::NonZeroU64::MIN),
                &truncated,
            )
            .await
            .unwrap();
        for tick in [1, 2] {
            clock.set(second(through + 3600 + tick));
            owner
                .sync
                .observe_clock(&owner.db.local_store_log().await.unwrap())
                .await
                .unwrap();
            assert!(!owner.sync.scan_store_log_once(false).await.unwrap());
            let log = owner.log().await;
            assert_eq!(log.horizon, Some(Duration::from_secs(10 * 86400)));
            assert!(log.is_final(creation));
        }
    }

    #[tokio::test]
    #[ignore = "requires COVEN_IO_LEAN, supplied by scripts/check.sh"]
    async fn io_differential_finality_discovery_receipts() {
        use serde_json::json;
        let month = 30 * 86400;
        let mut inputs = Vec::new();
        let mut expected = Vec::new();
        for (times, lag, samples) in [
            ([0, 3601, 3602], 5, vec![3603, 7203]),
            ([0, 1, month + 2], 3600, vec![month + 3602, month + 3603]),
        ] {
            let clock = Arc::new(FixedClock::new(second(0)));
            let storage = Arc::new(
                MemoryStorage::builder()
                    .clock(clock.clone())
                    .time_resolution(Duration::from_secs(1))
                    .build()
                    .unwrap(),
            );
            let mut owner = device(storage.clone(), 1, member(1), store(1)).await;
            let creation = owner.create(key(1)).await;
            let a = registration(&owner, &storage, &clock, 2, times[1]).await;
            let b = registration(&owner, &storage, &clock, 3, times[2]).await;
            storage
                .set_faults(Faults {
                    listing_lag: Some(Duration::from_secs(lag)),
                    ..Faults::none()
                })
                .await;
            for time in samples {
                observe(&owner, &clock, time).await;
                let log = owner.log().await;
                let received: Vec<_> = [creation, a, b]
                    .iter()
                    .enumerate()
                    .filter_map(|(i, id)| log.replay.entries.contains_key(id).then_some(i))
                    .collect();
                let through =
                    crate::finality::observation_time(second(time), Duration::from_secs(1))
                        .unwrap();
                let quiet = crate::finality::quiet_endpoint(&log, through, Duration::from_secs(1))
                    == Some(through);
                inputs.push(json!({"clock":time,"unit":1,"stored":times,
                    "past":[[],[0],[0]],"received":received,"window":month}));
                expected.push(json!({"horizon_input":through.as_secs(),
                    "certified":times.map(|t| Duration::from_secs(t) <= through),"through_time":true,
                    "completed_prefix":true,"observed_quiet":quiet,"whole_quiet":quiet,
                    "horizon":log.horizon.map(|h| h.as_secs())}));
            }
        }
        let output = crate::tests::run_lean("COVEN_IO_LEAN", &json!(inputs));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
            json!(expected)
        );
    }
}

fn day(n: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(n * 86400)
}

#[tokio::test]
async fn finality_clock_precedes_its_scan_and_survives_reopening() {
    let clock = Arc::new(FixedClock::new(day(0)));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    let entry = a.create(key(1)).await;
    assert_eq!(a.log().await.horizon, None);
    clock.set(day(31));
    a.clock.set(day(31));
    a.sync().await;
    let saved = a.log().await;
    assert_eq!(
        saved.horizon,
        Some(
            Duration::from_secs(86400) - crate::finality::LISTING_MARGIN - Duration::from_nanos(1)
        )
    );
    assert!(saved.is_final(entry));
    let path = ObjectPath::clock(store(1), entry.device);
    let first = storage.status(&path).await.unwrap().unwrap();
    let ring = a.custody.read().unwrap().unwrap();
    let opened = coven_format::clock::open(
        &storage.read(&path).await.unwrap(),
        &path,
        ring.store_key(key(1)).unwrap(),
        &a.member.member_id(),
    )
    .unwrap();
    assert_eq!(opened.device, entry.device);
    a.sync().await;
    assert_eq!(storage.status(&path).await.unwrap().unwrap(), first);
    a.restart(storage.clone()).await;
    assert_eq!(a.log().await, saved);
    a.clock.set(day(0));
    a.sync().await;
    assert_eq!(a.log().await.horizon, saved.horizon);
}

#[tokio::test]
async fn finality_cannot_advance_across_an_interrupted_or_failed_listing() {
    let clock = Arc::new(FixedClock::new(day(0)));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(clock.clone())
            .build()
            .unwrap(),
    );
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    clock.set(day(40));
    a.sync
        .observe_clock(&a.db.local_store_log().await.unwrap())
        .await
        .unwrap();
    let (listed, started) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_listing(ObjectPrefix::store_logs(store(1)), listed, held)
        .await;
    {
        let scan = a.sync.step();
        tokio::pin!(scan);
        tokio::select! {
            result = &mut scan => panic!("scan finished before listing: {result:?}"),
            result = started => result.unwrap(),
        }
        assert_eq!(a.log().await.horizon, None);
    }
    drop(resume);
    storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(a.sync.step().await.is_err());
    assert_eq!(a.log().await.horizon, None);
    a.sync.step().await.unwrap();
    assert_eq!(
        a.log().await.horizon,
        Some(
            Duration::from_secs(10 * 86400)
                - crate::finality::LISTING_MARGIN
                - Duration::from_nanos(1)
        )
    );
}

#[tokio::test]
async fn finality_waits_for_every_entry_in_the_certified_prefix_and_its_prerequisites() {
    // An unreadable entry and a failed read of an old entry both leave the
    // certified prefix incomplete. Logs are gap-free, so a writer's later
    // entries are only reachable through its entry 1.
    for failed_read in [false, true] {
        let clock = Arc::new(FixedClock::new(day(0)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let mut a = device(storage.clone(), 1, member(1), store(1)).await;
        a.create(key(1)).await;
        clock.set(day(1));
        let truncated = coven_format::sealed_single::SingleChunkPrefix::StoreLog { key: key(1) }
            .encode()
            .unwrap();
        let path = ObjectPath::store_log(store(1), DeviceId(2), std::num::NonZeroU64::MIN);
        storage.create(&path, &truncated).await.unwrap();
        clock.set(day(40));
        a.sync
            .observe_clock(&a.db.local_store_log().await.unwrap())
            .await
            .unwrap();
        if failed_read {
            storage
                .set_faults(Faults {
                    read_failures: [(path, coven_storage::StorageFailure::Network)].into(),
                    ..Faults::none()
                })
                .await;
            assert!(a.sync.step().await.is_err());
        } else {
            a.sync.step().await.unwrap();
        }
        assert_eq!(a.log().await.horizon, None, "failed read {failed_read}");
    }
}

#[tokio::test]
async fn failed_storage_unlock_restores_its_previous_clock_observation() {
    let clock = Arc::new(FixedClock::new(day(1)));
    let storage = Arc::new(MemoryStorage::builder().clock(clock).build().unwrap());
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.sync
        .observe_clock(&a.db.local_store_log().await.unwrap())
        .await
        .unwrap();
    let through = a.sync.finality.lock().unwrap().through();
    assert!(through.is_some());
    let candidate = Arc::new(MemoryStorage::builder().build().unwrap());
    candidate
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(a.sync.unlock_storage(candidate).await.is_err());
    assert_eq!(a.sync.finality.lock().unwrap().through(), through);
    assert_eq!(a.log().await.horizon, None);
}

#[tokio::test]
async fn clock_publication_requires_the_devices_registered_member() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.create(key(1)).await;
    a.identity.persist(&member(2)).unwrap();
    let local = a.db.local_store_log().await.unwrap();
    let requests = storage.request_count();
    assert!(matches!(
        a.sync.observe_clock(&local).await,
        Err(SyncError::Removed)
    ));
    assert_eq!(storage.request_count(), requests);
}

#[tokio::test]
async fn an_expired_clock_send_observes_membership_before_sampling_for_the_next_scan() {
    use coven_storage::test_utils::MemoryRequest;
    let storage_clock = Arc::new(coven_foundation::clock::FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::builder()
            .clock(storage_clock.clone())
            .build()
            .unwrap(),
    );
    let mut owner = device(storage.clone(), 1, member(1), store(1)).await;
    owner.create(key(1)).await;
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(100));
    owner.send.invalidate();
    let before = storage.request_trace().len();
    owner
        .sync
        .observe_clock(&owner.db.local_store_log().await.unwrap())
        .await
        .unwrap();
    let trace = storage.request_trace();
    let observed = &trace[before..];
    let clock = ObjectPath::clock(store(1), DeviceId(1));
    let sent = observed
        .iter()
        .position(|r| matches!(r, MemoryRequest::Replace(path) if *path == clock))
        .unwrap();
    assert!(observed[..sent]
        .iter()
        .any(|r| matches!(r, MemoryRequest::Other("list_writers"))));
    assert!(observed[..sent]
        .iter()
        .any(|r| matches!(r, MemoryRequest::Read(path, _) if path.store_log_position().is_some())));
    // The enabling phase keeps its own earlier sample; the clock time it
    // permitted qualifies only the next observation.
    let sampled = storage.status(&clock).await.unwrap().unwrap().stored_at;
    assert!(owner
        .sync
        .finality
        .lock()
        .unwrap()
        .caught_up()
        .is_none_or(|time| time < sampled));
    owner.sync.scan_store_log_once(true).await.unwrap();
    assert_eq!(
        owner.sync.finality.lock().unwrap().caught_up(),
        Some(sampled)
    );
}

#[tokio::test]
async fn a_suspended_first_phase_cannot_enable_its_waiting_clock_send() {
    use coven_storage::{test_utils::MemoryRequest, StorageFailure};
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut owner = device(storage.clone(), 1, member(1), store(1)).await;
    owner.create(key(1)).await;
    owner.send.invalidate();
    let (listed, started) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_next_listing(ObjectPrefix::store_logs(store(1)), listed, held)
        .await;
    let before = storage.request_trace().len();
    let local = owner.db.local_store_log().await.unwrap();
    let sending = owner.sync.observe_clock(&local);
    tokio::pin!(sending);
    tokio::select! {
        result = &mut sending => panic!("clock send finished before membership: {result:?}"),
        result = started => result.unwrap(),
    }
    owner.clock.set(UNIX_EPOCH + Duration::from_secs(301));
    resume.send(()).unwrap();
    assert!(
        matches!(sending.await, Err(SyncError::Storage(error)) if error.failure() == StorageFailure::CatchUpRequired)
    );
    assert!(!owner.send.can_send());
    assert!(storage.request_trace()[before..]
        .iter()
        .all(|r| !matches!(r, MemoryRequest::Create(_) | MemoryRequest::Replace(_))));
}
