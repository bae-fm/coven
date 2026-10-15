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

/// IO `WaitingEvidence`: a wait for a key this device lacks is due as soon
/// as the key arrives, keeping its backoff; other commits and other keys are
/// no evidence. A wait armed while the key was already held is not woken by
/// holding it.
#[tokio::test]
async fn a_key_wait_wakes_when_its_key_arrives() {
    let storage = google();
    let [mut a, _b, _c] = accounts(storage.clone()).await;
    let id = begin(&mut a, Command::CreateCircle("Gifts".into())).await;
    let lacking = PendingReason::KeyUnavailable {
        audience: Audience::Store,
        key: key(90),
    };
    let mut peers = Peers::default();
    attempt(&a, id, &mut peers, &[], lacking.clone()).await;
    attempt_after_delay(&a, id, &mut peers, lacking.clone()).await;
    assert!(!ready(&a, id).await);
    peers.post(&a, 7).await;
    a.sync.test_key(Audience::Store, key(91));
    assert!(!ready(&a, id).await);
    // The key arrives: due at once, with the count kept.
    a.sync.test_key(Audience::Store, key(90));
    assert!(ready(&a, id).await);
    assert_eq!(failures(&a, id).await, 2);
    // The next attempt still fails on that key: the key it held from the
    // start is no evidence, so the wait runs its delay.
    attempt(&a, id, &mut peers, &[], lacking).await;
    assert!(!ready(&a, id).await);
    a.elapse_retry_delays();
    assert!(ready(&a, id).await);
}

/// One unsuccessful attempt once the operation's delay has run.
async fn attempt_after_delay(
    d: &Device,
    id: OperationId,
    peers: &mut Peers,
    reason: PendingReason,
) {
    d.elapse_retry_delays();
    assert!(ready(d, id).await);
    attempt(d, id, peers, &[], reason).await;
}

/// IO `DriveOrder`: a raise holding the writer slot goes before a pending
/// reload while it waits on anything else (`raise_keeps_its_place`); once its
/// recorded condition names the reload, it yields and the drive runs the
/// reload (`no_cycle`). A raise waiting on a write, such as its own migration
/// marker's upload, waits on the reload too: step 6 sends and receives writes
/// only once no reload is pending.
#[tokio::test]
async fn a_raise_waiting_on_a_reload_yields_to_it() {
    use crate::snapshot_data::{ReloadScope, SnapshotJob, SnapshotTask, SnapshotTrigger};
    let marker = coven_merge::WriteId {
        device: DeviceId(1),
        number: 1,
    };
    for (raise_waits, reload_runs) in [
        (positions(7), false),
        (
            PendingReason::Waits(Prerequisite::Reload(Audience::Store)),
            true,
        ),
        (
            PendingReason::Waits(Prerequisite::Object(crate::write_seal::path(
                store(1),
                marker,
            ))),
            true,
        ),
    ] {
        let storage = storage();
        let mut a = device(storage, 1, member(1), store(1)).await;
        a.create(key(1)).await;
        let task = |job| {
            crate::operation_data::Data::Snapshots(SnapshotTask {
                job,
                temporary: Vec::new(),
            })
            .new_operation("coven")
            .unwrap()
        };
        let raise =
            a.db.start_operation(task(SnapshotJob::Write {
                audience: Audience::Store,
                device: a.device().await,
                trigger: SnapshotTrigger::Raise {
                    version: 2,
                    entry: None,
                },
                attempt: None,
                abandoned: Vec::new(),
            }))
            .await
            .unwrap();
        let reload =
            a.db.start_operation(task(SnapshotJob::Reload {
                scope: ReloadScope::All,
                files: None,
            }))
            .await
            .unwrap();
        let record = a.operation(raise).await;
        a.sync
            .record_operation_condition(&record, raise_waits.clone())
            .await
            .unwrap();
        let untouched = a.operation(reload).await;
        let db = a.db.clone();
        let files = file_owner(&a);
        let operations = operation_owner(a.sync, files);
        operations.barrier().await.unwrap();
        // The reload ran only if the raise yielded to it.
        let ran = db
            .operation_pending::<PendingRecord>(reload)
            .await
            .unwrap()
            .is_some()
            || db
                .operation(reload)
                .await
                .unwrap()
                .is_none_or(|record| record.last_step != untouched.last_step);
        assert_eq!(ran, reload_runs, "{raise_waits:?}");
        if !reload_runs {
            // Nothing the raise waits on changed: it still waits its delay.
            let pending = db.operation_pending::<PendingRecord>(raise).await.unwrap();
            assert_eq!(pending.unwrap().reason, raise_waits);
        }
        operations.close().await.unwrap();
    }
}

fn storage_failure() -> PendingReason {
    PendingReason::Storage {
        provider: coven_storage::CloudProvider::GoogleDrive,
        failure: StorageFailure::Network,
    }
}

/// IO `WaitingEvidence` and `WaitingReopen` through the real owner: one
/// operation's wait through monotonic time and a wall clock that can be set
/// back, other work's commits to devices' positions, attempts that fail
/// waiting on one device's positions after other work committed some while
/// they ran, and reopens, some cutting an attempt short. The model decides
/// when each attempt is due and the failure count it leaves.
#[tokio::test]
#[ignore = "requires COVEN_IO_RUNNER"]
async fn io_differential_waiting_evidence() {
    let storage = google();
    let [mut a, _b, _c] = accounts(storage.clone()).await;
    // Room for the wall clock to be set back.
    a.clock
        .set(std::time::UNIX_EPOCH + Duration::from_secs(1_000_000));
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
    // Reopens that cut an attempt short, and wall-clock changes.
    let (mut cut_short, mut set_back) = (0, 0);
    for case in 0..24 {
        let id = begin(&mut a, Command::CreateCircle(format!("circle {case}"))).await;
        let mut actions = Vec::new();
        let mut attempts = Vec::new();
        for _ in 0..24 {
            match pick(8) {
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
                2 => {
                    // The wall clock is set back; the monotonic clock is not.
                    let seconds = pick(400);
                    set_back += 1;
                    a.clock.set(
                        coven_foundation::clock::Clock::now(a.clock.as_ref())
                            - Duration::from_secs(seconds),
                    );
                    actions.push(json!({"op": "back", "seconds": seconds}));
                }
                3 => {
                    // Reopen, cutting short an attempt begun when due.
                    let racing: Vec<u64> = (0..pick(3)).map(|_| 1 + pick(3)).collect();
                    let cut = pick(2) == 0;
                    let due = cut && ready(&a, id).await;
                    if due {
                        cut_short += 1;
                        a.waits.begin_attempt(id);
                        for device in &racing {
                            peers.post(&a, *device).await;
                        }
                    }
                    a.reopen(storage.clone(), vec![], vec![]).await;
                    if cut {
                        attempts.push(json!({"due": due, "failures": failures(&a, id).await}));
                        actions.push(json!({"op": "cut", "raced": racing}));
                    } else {
                        actions.push(json!({"op": "reopen"}));
                    }
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
    assert!(cut_short > 0 && set_back > 0, "{cut_short} {set_back}");
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

/// What each recorded condition's wait watches, as the model names it
/// (IO `PrerequisiteRows.needs`).
fn audience_json(audience: &Audience) -> Value {
    match audience {
        Audience::Store => json!("store"),
        Audience::Circle(circle) => json!(circle.to_string()),
    }
}

fn prereq(rows: &coven_database::PrerequisiteRows) -> Value {
    match rows {
        coven_database::PrerequisiteRows::Entry(entry) => {
            json!({"kind": "entry", "device": entry.device.0, "number": entry.number})
        }
        coven_database::PrerequisiteRows::KeyCopy { writer, number } => {
            json!({"kind": "key_copy", "writer": writer.0, "number": number})
        }
        coven_database::PrerequisiteRows::Write(write) => {
            json!({"kind": "write", "device": write.device.0, "number": write.number})
        }
        coven_database::PrerequisiteRows::Device(device) => {
            json!({"kind": "device", "device": device.0})
        }
        coven_database::PrerequisiteRows::PeerPositions(device) => {
            json!({"kind": "peer_positions", "device": device.0})
        }
        coven_database::PrerequisiteRows::Reload(scope) => {
            json!({"kind": "reload", "audience": audience_json(scope)})
        }
        coven_database::PrerequisiteRows::Finality => json!({"kind": "finality"}),
        coven_database::PrerequisiteRows::Boundaries => json!({"kind": "boundaries"}),
        coven_database::PrerequisiteRows::ProviderAccess(account) => {
            json!({"kind": "provider_access", "account": account})
        }
        coven_database::PrerequisiteRows::Operation(id) => json!({"kind": "operation", "id": id.0}),
    }
}

/// Every kind of recorded condition, with what the model names it.
fn wait_conditions() -> Vec<(PendingReason, Option<OperationId>, Value)> {
    let store = store(1);
    let n = |n: u64| std::num::NonZeroU64::new(n).unwrap();
    let objects = [
        (
            ObjectPath::store_log(store, DeviceId(2), n(3)),
            json!({"kind": "entry", "device": 2, "number": 3}),
        ),
        (
            ObjectPath::key_copy(store, DeviceId(2), n(4)),
            json!({"kind": "key_copy", "writer": 2, "number": 4}),
        ),
        (
            ObjectPath::device_log(store, DeviceId(2), n(5)),
            json!({"kind": "write", "device": 2, "number": 5}),
        ),
        (
            ObjectPath::positions(store, DeviceId(2)),
            json!({"kind": "stored"}),
        ),
    ];
    let mut conditions = Vec::new();
    for (path, object) in objects {
        let model = json!({"kind": "object", "object": object});
        conditions.push((
            PendingReason::Missing { path: path.clone() },
            None,
            model.clone(),
        ));
        conditions.push((
            PendingReason::Waits(Prerequisite::Object(path)),
            None,
            model,
        ));
    }
    let account = || "ana@example.com".to_owned();
    let provider = json!({"kind": "provider_access", "account": "ana@example.com"});
    let other = json!({"kind": "other"});
    conditions.extend([
        (
            PendingReason::Waits(Prerequisite::CatchUp),
            None,
            json!({"kind": "catch_up"}),
        ),
        (
            PendingReason::Waits(Prerequisite::DeviceRegistration(DeviceId(6))),
            None,
            json!({"kind": "registration", "device": 6}),
        ),
        (
            PendingReason::Waits(Prerequisite::SchemaPublication {
                audience: Audience::Store,
                version: 2,
            }),
            None,
            json!({"kind": "schema_publication"}),
        ),
        (
            PendingReason::Waits(Prerequisite::Reload(Audience::Store)),
            Some(OperationId(9)),
            json!({"kind": "reload", "audience": "store"}),
        ),
        (
            PendingReason::Waits(Prerequisite::Reload(Audience::Store)),
            None,
            json!({"kind": "reload", "audience": "store"}),
        ),
        (
            PendingReason::Waits(Prerequisite::Positions(DeviceId(7))),
            None,
            json!({"kind": "positions", "device": 7}),
        ),
        (
            PendingReason::Waits(Prerequisite::EntryFinality(coven_database::EntryId {
                device: DeviceId(2),
                number: 3,
            })),
            None,
            json!({"kind": "entry_finality"}),
        ),
        (
            PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key: KeyId(uuid::Uuid::from_u128(5)),
            },
            None,
            json!({"kind": "key_unavailable"}),
        ),
        (
            PendingReason::ProviderPending {
                action: crate::ProviderAccessAction::Grant,
                account: account(),
            },
            None,
            provider.clone(),
        ),
        (
            PendingReason::PendingOwner { account: account() },
            None,
            provider.clone(),
        ),
        (
            PendingReason::AccountInUse { account: account() },
            None,
            provider,
        ),
        (
            PendingReason::Storage {
                provider: coven_storage::CloudProvider::GoogleDrive,
                failure: coven_storage::StorageFailure::Network,
            },
            None,
            json!({"kind": "storage"}),
        ),
        (PendingReason::NoStorage, None, json!({"kind": "storage"})),
        (PendingReason::Removed, None, other.clone()),
        (PendingReason::Paused, None, other.clone()),
        (PendingReason::Disagrees, None, other.clone()),
        (PendingReason::AdminAccessRequired, None, other.clone()),
        (
            PendingReason::Failed(crate::LocalFailure::Database),
            None,
            other,
        ),
    ]);
    conditions
}

/// IO `PrerequisiteRows.needs`: each condition's wait watches the model's
/// prerequisites, key and connection; a condition only storage or time can
/// end names none (`time_only`).
#[test]
#[ignore = "requires COVEN_IO_RUNNER"]
fn io_differential_wait_rows() {
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for (reason, reload, model) in wait_conditions() {
        let (rows, key, connection) = crate::RetryWaits::watches(&reason, reload);
        cases.push(json!({
            "check": "wait_rows",
            "reason": model,
            "reload": reload.map(|id| id.0),
        }));
        actual.push(json!({
            "prereqs": rows.iter().map(prereq).collect::<Vec<_>>(),
            "key": key.is_some(),
            "connection": connection,
        }));
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}
