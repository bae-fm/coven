//! A waiting operation wakes for a change to what it waits on, and for
//! nothing else (§3.1, SP waiting; IO `WaitingEvidence`).

use super::*;
use coven_database::RetrySubject;
use futures_util::FutureExt;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

/// Other work posts positions for these devices.
#[derive(Default)]
struct Peers(BTreeMap<DeviceId, u8>);

impl Peers {
    /// One commit changing `device`'s posted positions.
    async fn post(&mut self, d: &Device, device: u64) {
        *self.0.entry(DeviceId(device)).or_default() += 1;
        let observed = self
            .0
            .iter()
            .map(|(device, value)| (*device, vec![*value]))
            .collect();
        d.db.complete_positions_observation(observed, Vec::<PendingRecord>::new(), vec![], vec![])
            .await
            .unwrap();
    }
}

fn positions(device: u64) -> PendingReason {
    PendingReason::Waits(Prerequisite::Positions(DeviceId(device)))
}

/// One unsuccessful attempt ending on `reason`, after other work's commits
/// in `racing` landed while it ran.
async fn attempt(
    d: &Device,
    id: OperationId,
    peers: &mut Peers,
    racing: &[u64],
    reason: PendingReason,
) {
    let record = d.operation(id).await;
    d.waits.begin_attempt(id);
    for device in racing {
        peers.post(d, *device).await;
    }
    d.sync
        .record_operation_condition(&record, reason)
        .await
        .unwrap();
}

async fn ready(d: &Device, id: OperationId) -> bool {
    let record = d.operation(id).await;
    d.sync.operation_ready(&record).await.unwrap()
}

async fn failures(d: &Device, id: OperationId) -> u32 {
    d.db.retry_wait(RetrySubject::Operation(id))
        .await
        .unwrap()
        .map_or(0, |wait| wait.failures)
}

/// T215: a capped wait on Ben's positions ignores commits to anything else,
/// in any number, and wakes at once when Ben's positions change, keeping its
/// backoff. A change landing while the attempt runs wakes the wait it ends in
/// (IO `racing_commit_wakes`); one the attempt makes itself does not.
#[tokio::test]
async fn an_unrelated_commit_leaves_a_wait_and_its_own_prerequisite_wakes_it() {
    let storage = google();
    let [mut a, _b, _c] = accounts(storage.clone()).await;
    let id = begin(&mut a, Command::CreateCircle("Gifts".into())).await;
    let mut peers = Peers::default();
    for _ in 0..9 {
        attempt(&a, id, &mut peers, &[], positions(7)).await;
        a.elapse_retry_delays();
    }
    attempt(&a, id, &mut peers, &[], positions(7)).await;
    let capped =
        a.db.retry_wait(RetrySubject::Operation(id))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(capped.wait, Duration::from_secs(300));
    assert!(!ready(&a, id).await);
    // Other devices' positions change, and the worker's wake stays asleep.
    let mut wake = a.sync.next_operation_wake().unwrap();
    for _ in 0..3 {
        peers.post(&a, 8).await;
        assert!((&mut wake).now_or_never().is_none());
        assert!(!ready(&a, id).await);
    }
    // Ben's do: the worker wakes, and the wait is due at once.
    peers.post(&a, 7).await;
    tokio::time::timeout(Duration::from_secs(5), wake)
        .await
        .unwrap();
    assert!(ready(&a, id).await);
    // The backoff continues; the attempt's own change to Ben's positions is
    // no evidence, so the wait it ends in runs its delay.
    let record = a.operation(id).await;
    let scope = a.waits.begin_attempt(id);
    scope.run(peers.post(&a, 7)).await;
    a.sync
        .record_operation_condition(&record, positions(7))
        .await
        .unwrap();
    assert_eq!(failures(&a, id).await, 11);
    assert!(!ready(&a, id).await);
    // Other work changes it while the next attempt runs: due at once.
    a.elapse_retry_delays();
    attempt(&a, id, &mut peers, &[8, 7], positions(7)).await;
    assert!(ready(&a, id).await);
    // A storage failure waits on the connection, which a new one ends.
    attempt(&a, id, &mut peers, &[7], storage_failure()).await;
    assert!(!ready(&a, id).await);
    a.sync.connection_changed();
    assert!(ready(&a, id).await);
}

fn storage_failure() -> PendingReason {
    PendingReason::Storage {
        provider: coven_storage::CloudProvider::GoogleDrive,
        failure: StorageFailure::Network,
    }
}

/// IO `WaitingEvidence` through the real owner: one operation's wait through
/// monotonic time, other work's commits to devices' positions, and attempts
/// that fail waiting on one device's positions after other work committed
/// some while they ran. The model decides when each attempt is due and the
/// failure count it leaves.
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER"]
async fn io_differential_waiting_evidence() {
    let storage = google();
    let [mut a, _b, _c] = accounts(storage.clone()).await;
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut pick = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    let mut peers = Peers::default();
    for case in 0..24 {
        let id = begin(&mut a, Command::CreateCircle(format!("circle {case}"))).await;
        let mut actions = Vec::new();
        let mut attempts = Vec::new();
        for _ in 0..24 {
            match pick(4) {
                0 => {
                    let device = 1 + pick(3);
                    peers.post(&a, device).await;
                    actions.push(json!({"op": "commit", "prereq": device}));
                }
                1 => {
                    let seconds = pick(400);
                    a.clock.set(
                        coven_foundation::clock::Clock::now(a.clock.as_ref())
                            + Duration::from_secs(seconds),
                    );
                    actions.push(json!({"op": "advance", "seconds": seconds}));
                }
                _ => {
                    let device = 1 + pick(3);
                    let racing: Vec<u64> = (0..pick(3)).map(|_| 1 + pick(3)).collect();
                    let due = ready(&a, id).await;
                    if due {
                        attempt(&a, id, &mut peers, &racing, positions(device)).await;
                    }
                    attempts.push(json!({"due": due, "failures": failures(&a, id).await}));
                    actions.push(json!({"op": "attempt", "prereq": device, "raced": racing}));
                }
            }
        }
        cases.push(json!({"check": "evidence", "actions": actions}));
        actual.push(json!(attempts));
    }
    let text = actual.iter().map(Value::to_string).collect::<String>();
    assert!(text.contains("true") && text.contains("false"));
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}
