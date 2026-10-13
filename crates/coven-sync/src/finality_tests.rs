use super::*;
use crate::replay::tests::{device, History};
use serde_json::{json, Value};

fn day(n: u64) -> Duration {
    Duration::from_secs(n * 86400)
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
        .map(|(entry, time)| (entry.position, UNIX_EPOCH + day(time)))
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
        assert_eq!(observation_time(UNIX_EPOCH + hour, unit), None);
        assert_eq!(
            observation_time(UNIX_EPOCH + hour + unit, unit),
            Some(Duration::ZERO)
        );
        assert_eq!(
            observation_time(UNIX_EPOCH + day(40), unit),
            Some(day(40) - hour - unit)
        );
    }
}

#[test]
fn recovered_endpoint_is_the_last_provider_tick_before_the_late_entry() {
    let mut log = history(true);
    for (entry, time) in log.entries.iter_mut().zip([0, 1, 1, 30 * 86400 + 2]) {
        entry.stored_at = UNIX_EPOCH + Duration::from_secs(time);
    }
    log.horizon = horizon(
        &log,
        day(30) + Duration::from_secs(2),
        Duration::from_secs(1),
    );
    assert_eq!(log.horizon, Some(Duration::from_secs(1)));
    assert!(!log.is_final(log.entries[1].entry.position));
}

#[test]
fn observations_exclude_the_entire_clock_tick_and_reject_underflow() {
    for resolution in [
        Duration::from_secs(1),
        Duration::from_millis(1),
        Duration::from_nanos(1),
    ] {
        assert_eq!(observation_time(UNIX_EPOCH, resolution), None);
        assert_eq!(
            observation_time(
                UNIX_EPOCH + LISTING_MARGIN + Duration::from_secs(1),
                resolution
            ),
            Some(Duration::from_secs(1) - resolution)
        );
        assert_eq!(
            observation_time(UNIX_EPOCH + day(40), resolution),
            Some(day(40) - LISTING_MARGIN - resolution)
        );
    }
}

#[test]
fn an_absent_horizon_differs_from_zero_and_tied_entries_share_the_strict_boundary() {
    let mut log = history(false);
    assert_eq!(log.horizon, None);
    log.horizon = horizon(&log, day(30), Duration::from_secs(1));
    assert_eq!(log.horizon, Some(day(0)));
    assert!(!log.is_final(log.entries[0].entry.position));
    log.horizon = horizon(&log, day(31), Duration::from_secs(1));
    assert_eq!(log.horizon, Some(day(1)));
    assert!(log.is_final(log.entries[0].entry.position));
    assert!(!log.is_final(log.entries[1].entry.position));
    assert!(!log.is_final(log.entries[2].entry.position));
    log.horizon = horizon(&log, day(32), Duration::from_secs(1));
    assert!(log.entries.iter().all(|e| log.is_final(e.entry.position)));
}

#[test]
fn inclusive_late_window_recovers_earlier_checks_without_lowering_saved_finality() {
    let mut log = history(true);
    let recovered = day(10) - Duration::from_secs(1);
    assert_eq!(
        horizon(&log, day(40), Duration::from_secs(1)),
        Some(recovered)
    );
    assert_eq!(
        horizon(&log, day(70), Duration::from_secs(1)),
        Some(recovered)
    );
    assert_eq!(
        horizon(
            &log,
            day(70) + Duration::from_secs(1),
            Duration::from_secs(1)
        ),
        Some(day(40) + Duration::from_secs(1))
    );
    log.horizon = Some(day(20));
    assert_eq!(
        horizon(&log, day(40), Duration::from_secs(1)),
        Some(day(20))
    );
    assert_eq!(
        horizon(&log, day(10), Duration::from_secs(1)),
        Some(day(20))
    );
}

#[tokio::test]
async fn probes_wait_for_a_possible_quiet_window_and_back_off() {
    use coven_foundation::clock::FixedClock;
    let clock = FixedClock::new(UNIX_EPOCH + day(30));
    let mut log = history(false);
    let mut observation = Observation::default();
    assert!(!observation.probe_due(&log, &clock));
    clock.set(UNIX_EPOCH + day(31));
    assert!(observation.probe_due(&log, &clock));
    assert!(!observation.probe_due(&log, &clock));
    clock.set(UNIX_EPOCH + day(31) + Duration::from_secs(60));
    assert!(observation.probe_due(&log, &clock));
    log = history(true);
    clock.set(UNIX_EPOCH + day(70));
    assert!(!observation.probe_due(&log, &clock));
    // Completed timers are discarded, so repeated checks cannot poll a finished future.
    assert!(!observation.probe_due(&log, &clock));
    clock.set(UNIX_EPOCH + day(70) + LISTING_MARGIN + Duration::from_millis(1));
    assert!(observation.probe_due(&log, &clock));
}

#[tokio::test]
async fn probes_require_the_margin_and_the_observed_provider_tick() {
    use coven_foundation::clock::FixedClock;
    let unit = Duration::from_secs(1);
    let mut observation = Observation {
        sample: Some((UNIX_EPOCH, unit)),
        ..Default::default()
    };
    let log = history(true);
    let boundary = UNIX_EPOCH + day(70) + Duration::from_secs(3600) + unit;
    let clock = FixedClock::new(boundary);
    assert!(!observation.probe_due(&log, &clock));
    clock.set(boundary + unit);
    assert!(observation.probe_due(&log, &clock));
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
                .map(|(entry, time)| {
                    (
                        entry.position,
                        UNIX_EPOCH + Duration::from_secs(time * 86400),
                    )
                })
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
                        let cutoff =
                            horizon(&log, Duration::from_secs(time * 86400), day(1)).unwrap();
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
            let through = observation_time(
                UNIX_EPOCH + Duration::from_secs(time),
                Duration::from_secs(unit),
            )
            .map(|time| time.as_secs());
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
        .map(|(entry, time)| (entry.position, UNIX_EPOCH + Duration::from_secs(time)))
        .collect();
    let mut log = StoreLog::default();
    for i in [0, 2] {
        let (entry, replay) = crate::replay_entry(&log, h.entries[i].clone(), &publications);
        log.entries.push(entry);
        log.replay = replay;
    }
    let sample = 32 * 86400 + 3602;
    let through = observation_time(
        UNIX_EPOCH + Duration::from_secs(sample),
        Duration::from_secs(1),
    )
    .unwrap();
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
            "horizon_input":through.as_secs(),"certified":[true,true,true],"through_time":true,
            "completed_prefix":false,"observed_quiet":true,"whole_quiet":true,
            "horizon":null,
        }])
    );
}
