use super::*;
use crate::replay::tests::{device, History};
use serde_json::{json, Value};

fn day(n: u64) -> Duration {
    Duration::from_secs(n * 86400)
}

/// A provider time `since` the epoch.
fn at(since: Duration) -> StorageTime {
    StorageTime::since_epoch(since)
}

fn history(late: bool) -> StoreLog {
    let mut history = History::new();
    history.push(0, 1, &[0], device(1));
    history.push(0, 2, &[0], device(2));
    if late {
        history.push(0, 2, &[0, 2], device(3));
    }
    let publications = history
        .entries
        .iter()
        .zip([0, 1, 1, 40])
        .map(|(entry, time)| (entry.position, at(day(time))))
        .collect();
    let mut log = StoreLog::default();
    for entry in history.entries {
        let (checked, replay) = crate::replay_entry(&log, entry, &publications);
        log.entries.push(checked);
        log.replay = replay;
    }
    log
}

#[test]
fn observations_wait_out_the_listing_margin_before_the_provider_tick() {
    let hour = Duration::from_secs(3600);
    for unit in [Duration::from_secs(1), Duration::from_millis(1)] {
        assert_eq!(observation_time(at(hour), unit), None);
        assert_eq!(
            observation_time(at(hour + unit), unit),
            Some(StorageTime::UNIX_EPOCH)
        );
        assert_eq!(
            observation_time(at(day(40)), unit),
            Some(at(day(40) - hour - unit))
        );
    }
}

#[test]
fn recovered_endpoint_is_the_last_provider_tick_before_the_late_entry() {
    let mut log = history(true);
    for (entry, time) in log.entries.iter_mut().zip([0, 1, 1, 30 * 86400 + 2]) {
        entry.stored_at = at(Duration::from_secs(time));
    }
    log.horizon = horizon(
        &log,
        at(day(30) + Duration::from_secs(2)),
        Duration::from_secs(1),
    );
    assert_eq!(log.horizon, Some(at(Duration::from_secs(1))));
    assert!(!log.is_final(log.entries[1].entry.position));
}

#[test]
fn observations_exclude_the_entire_clock_tick_and_reject_underflow() {
    for resolution in [
        Duration::from_secs(1),
        Duration::from_millis(1),
        Duration::from_nanos(1),
    ] {
        assert_eq!(observation_time(StorageTime::UNIX_EPOCH, resolution), None);
        assert_eq!(
            observation_time(at(LISTING_MARGIN + Duration::from_secs(1)), resolution),
            Some(at(Duration::from_secs(1) - resolution))
        );
        assert_eq!(
            observation_time(at(day(40)), resolution),
            Some(at(day(40) - LISTING_MARGIN - resolution))
        );
    }
}

#[test]
fn an_absent_horizon_differs_from_zero_and_tied_entries_share_the_strict_boundary() {
    let mut log = history(false);
    assert_eq!(log.horizon, None);
    log.horizon = horizon(&log, at(day(30)), Duration::from_secs(1));
    assert_eq!(log.horizon, Some(at(day(0))));
    assert!(!log.is_final(log.entries[0].entry.position));
    log.horizon = horizon(&log, at(day(31)), Duration::from_secs(1));
    assert_eq!(log.horizon, Some(at(day(1))));
    assert!(log.is_final(log.entries[0].entry.position));
    assert!(!log.is_final(log.entries[1].entry.position));
    assert!(!log.is_final(log.entries[2].entry.position));
    log.horizon = horizon(&log, at(day(32)), Duration::from_secs(1));
    assert!(log.entries.iter().all(|e| log.is_final(e.entry.position)));
}

#[test]
fn inclusive_late_window_recovers_earlier_checks_without_lowering_saved_finality() {
    let mut log = history(true);
    let recovered = at(day(10) - Duration::from_secs(1));
    assert_eq!(
        horizon(&log, at(day(40)), Duration::from_secs(1)),
        Some(recovered)
    );
    assert_eq!(
        horizon(&log, at(day(70)), Duration::from_secs(1)),
        Some(recovered)
    );
    assert_eq!(
        horizon(
            &log,
            at(day(70) + Duration::from_secs(1)),
            Duration::from_secs(1)
        ),
        Some(at(day(40) + Duration::from_secs(1)))
    );
    log.horizon = Some(at(day(20)));
    assert_eq!(
        horizon(&log, at(day(40)), Duration::from_secs(1)),
        Some(at(day(20)))
    );
    assert_eq!(
        horizon(&log, at(day(10)), Duration::from_secs(1)),
        Some(at(day(20)))
    );
}

#[tokio::test]
async fn probes_wait_for_a_possible_quiet_window_and_back_off() {
    use coven_foundation::clock::FixedClock;
    let clock = FixedClock::new(UNIX_EPOCH + day(30));
    let mut log = history(false);
    let mut observation = Observation::default();
    assert!(!observation.probe_due(&log, &[], &clock));
    clock.set(UNIX_EPOCH + day(31));
    assert!(observation.probe_due(&log, &[], &clock));
    assert!(!observation.probe_due(&log, &[], &clock));
    clock.set(UNIX_EPOCH + day(31) + Duration::from_secs(60));
    assert!(observation.probe_due(&log, &[], &clock));
    log = history(true);
    clock.set(UNIX_EPOCH + day(70));
    assert!(!observation.probe_due(&log, &[], &clock));
    // Completed timers are discarded, so repeated checks cannot poll a finished future.
    assert!(!observation.probe_due(&log, &[], &clock));
    clock.set(UNIX_EPOCH + day(70) + LISTING_MARGIN + Duration::from_millis(1));
    assert!(observation.probe_due(&log, &[], &clock));
}

#[tokio::test]
async fn probes_require_the_margin_and_the_observed_provider_tick() {
    use coven_foundation::clock::FixedClock;
    let unit = Duration::from_secs(1);
    let mut observation = Observation {
        sample: Some((StorageTime::UNIX_EPOCH, unit)),
        ..Default::default()
    };
    let log = history(true);
    let boundary = UNIX_EPOCH + day(70) + Duration::from_secs(3600) + unit;
    let clock = FixedClock::new(boundary);
    assert!(!observation.probe_due(&log, &[], &clock));
    clock.set(boundary + unit);
    assert!(observation.probe_due(&log, &[], &clock));
}

fn run(variable: &str, input: Value) -> Value {
    let result = crate::tests::run_lean(variable, &input);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

#[test]
#[ignore = "requires COVEN_STORELOG_LEAN, supplied by scripts/check.sh"]
fn lean_differential_finality_horizon() {
    let mut inputs = Vec::new();
    let mut expected = Vec::new();
    for times in [
        [0, 1, 1, 40],
        [0, 1, 2, 40],
        [0, 30, 30, 60],
        [0, 31, 31, 62],
    ] {
        for read_other in [false, true] {
            let mut history = History::new();
            history.push(0, 1, &[0], device(1));
            history.push(0, 2, &[0], device(2));
            history.push(
                0,
                2,
                if read_other { &[0, 1, 2] } else { &[0, 2] },
                device(3),
            );
            let checks: Vec<_> = (0..=100).collect();
            let mut input = crate::tests::input(&history, &times, &[]);
            input["checks"] = json!(checks);
            inputs.push(input);
            let publications = history
                .entries
                .iter()
                .zip(times)
                .map(|(entry, time)| (entry.position, at(Duration::from_secs(time * 86400))))
                .collect();
            let mut log = StoreLog::default();
            for entry in history.entries {
                let (checked, replay) = crate::replay_entry(&log, entry, &publications);
                log.entries.push(checked);
                log.replay = replay;
            }
            // Both implementations recover whole ticks in the same day unit.
            // Assert alignment before converting, so flooring cannot hide a
            // disagreement at the strict boundary.
            expected.push(
                checks
                    .into_iter()
                    .map(|time| {
                        let cutoff = horizon(&log, at(Duration::from_secs(time * 86400)), day(1))
                            .unwrap()
                            .elapsed_since_epoch();
                        assert_eq!(cutoff.as_nanos() % day(1).as_nanos(), 0);
                        cutoff.as_secs() / 86400
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    assert_eq!(
        run("COVEN_STORELOG_LEAN", json!({"histories": inputs})),
        json!(expected)
    );
}

#[test]
#[ignore = "requires COVEN_IO_LEAN, supplied by scripts/check.sh"]
fn io_differential_finality_observation() {
    let mut inputs = Vec::new();
    let mut expected = Vec::new();
    for unit in [1u64, 2] {
        for time in [0, 3599, 3600, 3600 + unit, 3601 + unit, 40 * 86400] {
            let stored = [0, time.saturating_sub(unit), time, time + unit];
            let through =
                observation_time(at(Duration::from_secs(time)), Duration::from_secs(unit))
                    .map(|time| time.seconds());
            inputs.push(json!({"clock":time,"unit":unit,"stored":stored}));
            expected.push(json!({
                "horizon_input": through,
                "certified": stored.map(|t| through.is_some_and(|end| t <= end)),
                "through_time": true,
            }));
        }
    }
    assert_eq!(run("COVEN_IO_LEAN", json!(inputs)), json!(expected));
}

#[test]
#[ignore = "requires COVEN_IO_LEAN, supplied by scripts/check.sh"]
fn io_differential_finality_requires_the_old_prefix_even_when_the_window_is_quiet() {
    use crate::effects::tests::snapshot;
    use coven_database::EntryOutcome;
    use coven_format::store_log::StoreChange;
    use coven_merge::Audience;
    let mut h = History::new();
    for writer in [1, 2] {
        h.push(
            0,
            writer,
            &[0],
            StoreChange::Reset {
                snapshot: snapshot(writer, Audience::Store),
            },
        );
    }
    let stored = [0, 86400, 2 * 86400];
    let publications = h
        .entries
        .iter()
        .zip(stored)
        .map(|(entry, time)| (entry.position, at(Duration::from_secs(time))))
        .collect();
    let mut log = StoreLog::default();
    for i in [0, 2] {
        let (entry, replay) = crate::replay_entry(&log, h.entries[i].clone(), &publications);
        log.entries.push(entry);
        log.replay = replay;
    }
    let sample = 32 * 86400 + 3602;
    let through =
        observation_time(at(Duration::from_secs(sample)), Duration::from_secs(1)).unwrap();
    log.horizon = horizon(&log, through, Duration::from_secs(1));
    assert!(log.is_final(h.entries[2].position));
    assert_eq!(
        log.replay.entries[&h.entries[2].position],
        EntryOutcome::Kept
    );
    let (_, replay) = crate::replay_entry(&log, h.entries[1].clone(), &publications);
    assert!(matches!(
        replay.entries[&h.entries[2].position],
        EntryOutcome::Dropped(_)
    ));
    // This provider omitted an object older than its guarantee. Quietness alone
    // cannot certify the unreceived old prefix, even though exact reads work.
    assert_eq!(
        run(
            "COVEN_IO_LEAN",
            json!([{
                "clock":sample,"unit":1,"stored":stored,"past":[[],[0],[0]],
                "received":[0,2],"window":30 * 86400,
            }])
        ),
        json!([{
            "horizon_input":through.seconds(),"certified":[true,true,true],"through_time":true,
            "completed_prefix":false,"observed_quiet":true,"whole_quiet":true,
            "horizon":null,
        }])
    );
}

/// A snapshot is prepared only once every covered write's retirement-window
/// verdict is final, judged against the latest catch-up's window (§10, §15;
/// storelog-data `prepared_final`).
mod snapshot_window {
    use crate::device_log_sync::tests::{group, rows, sql, writes, STORE_ID};
    use crate::SyncError;
    use coven_format::store_log::StoreChange;
    use coven_foundation::clock::{Clock, FixedClock};
    use coven_foundation::id_source::DeviceId;
    use coven_merge::Audience;
    use coven_storage::test_utils::MemoryStorage;
    use coven_storage::{ObjectPath, ObjectPrefix, Storage};
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    /// Device 3 receives device 1's write one second past device 1's removal's
    /// thirty days, while that removal is not yet final: its snapshot waits on
    /// the removal and nothing is published. Once thirty quiet days make it final,
    /// a requested snapshot is prepared and published.
    #[tokio::test]
    async fn a_snapshot_waits_for_the_removal_deciding_a_covered_write() {
        let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1)));
        let storage = Arc::new(
            MemoryStorage::builder()
                .transfer_limits(1024 * 1024, 64 * 1024)
                .clock(clock.clone())
                .build()
                .unwrap(),
        );
        let mut devices = group(storage.clone(), 3).await;
        sql(
            &devices[0].db,
            "INSERT INTO notes VALUES('one','title','body')",
        )
        .await;
        let record = writes::queued(&devices[0].db).await;
        let removal = devices[1]
            .sync
            .make_and_upload_entry(StoreChange::RemoveDevice {
                device: DeviceId(1),
            })
            .await
            .unwrap();
        let device_clock = devices[2].clock.clone();
        let later = |days: u64| {
            let at = clock.now() + Duration::from_secs(days * 86400 + 1);
            clock.set(at);
            device_clock.set(at);
        };
        later(30);
        writes::publish(&storage, &record).await;
        let third = &mut devices[2];
        third.sync.sync_store_log().await.unwrap();
        third.writes.download_writes().await.unwrap();
        assert!(rows(&third.db).await.is_empty());
        let Err(SyncError::Pending(pending)) = third.sync.write_snapshot(Audience::Store).await
        else {
            panic!("the snapshot waits for the removal");
        };
        assert_eq!(
            pending.reason,
            crate::PendingReason::Waits(crate::Prerequisite::EntryFinality(removal))
        );
        assert!(storage
            .list(&ObjectPrefix::snapshots(STORE_ID))
            .await
            .unwrap()
            .is_empty());
        later(31);
        let third = &mut devices[2];
        third.sync.sync_store_log().await.unwrap();
        let snapshot = third.sync.write_snapshot(Audience::Store).await.unwrap();
        let path = ObjectPath::snapshot(
            STORE_ID,
            snapshot.audience,
            snapshot.device,
            snapshot.number.try_into().unwrap(),
        );
        assert!(storage
            .list(&ObjectPrefix::snapshots(STORE_ID))
            .await
            .unwrap()
            .iter()
            .any(|object| object.path == path));
    }
}
