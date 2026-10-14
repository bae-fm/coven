use super::*;
use serde_json::{json, Value};

fn reserved(writes: u64, entries: u64, key_copies: u64, snapshots: u64) -> IdentityReservations {
    IdentityReservations {
        writes,
        entries,
        key_copies,
        snapshots,
    }
}

fn evidence(listed: Counters) -> Evidence {
    Evidence {
        listed,
        ..Evidence::default()
    }
}

fn verdict(gate: &Gate) -> String {
    match gate {
        Gate::Send => "send".into(),
        Gate::Pending(_) => "pending".into(),
        Gate::Stopped => "stopped".into(),
        Gate::Reset(reason) => format!(
            "reset:{}",
            match reason {
                DeviceResetReason::IdentityMismatch => "identity-mismatch",
                DeviceResetReason::StorageAhead => "storage-ahead",
                DeviceResetReason::SlotMismatch => "slot-mismatch",
                DeviceResetReason::Replaced => "replaced",
                DeviceResetReason::DatabaseDamage => "damage",
            }
        ),
    }
}

#[test]
fn equal_counters_send_and_any_larger_stored_number_resets() {
    let device = DeviceId(4);
    let mine = reserved(5, 2, 3, 9);
    let equal = Counters {
        logs: Ends {
            writes: 5,
            entries: 2,
        },
        key_copies: 3,
        snapshots: 9,
    };
    // A reserved upload already in storage is not stale (§10).
    assert!(matches!(
        check(device, &mine, Some(device), Ok(evidence(equal))),
        Gate::Send
    ));
    for ahead in [
        Counters {
            logs: Ends {
                writes: 6,
                ..equal.logs
            },
            ..equal
        },
        Counters {
            logs: Ends {
                entries: 3,
                ..equal.logs
            },
            ..equal
        },
        Counters {
            key_copies: 4,
            ..equal
        },
        Counters {
            snapshots: 10,
            ..equal
        },
    ] {
        assert!(matches!(
            check(device, &mine, Some(device), Ok(evidence(ahead))),
            Gate::Reset(DeviceResetReason::StorageAhead)
        ));
    }
    // A log deleted after a rollback is still exposed by coverage or a post.
    for (covered, posted) in [
        (
            Ends {
                writes: 6,
                entries: 0,
            },
            Ends::default(),
        ),
        (
            Ends::default(),
            Ends {
                writes: 0,
                entries: 3,
            },
        ),
    ] {
        let found = Evidence {
            covered,
            posted,
            ..Evidence::default()
        };
        assert!(matches!(
            check(device, &mine, Some(device), Ok(found)),
            Gate::Reset(DeviceResetReason::StorageAhead)
        ));
    }
}

#[test]
fn missing_custody_resets_and_a_failed_scan_sends_nothing() {
    let device = DeviceId(4);
    let mine = reserved(1, 1, 0, 0);
    for custody in [None, Some(DeviceId(5))] {
        assert!(matches!(
            check(device, &mine, custody, Ok(Evidence::default())),
            Gate::Reset(DeviceResetReason::IdentityMismatch)
        ));
    }
    assert!(matches!(
        check(
            device,
            &mine,
            Some(device),
            Err(StorageFailure::Network.into())
        ),
        Gate::Pending(_)
    ));
    assert!(matches!(
        check(
            device,
            &mine,
            Some(device),
            Ok(Evidence {
                replaced: true,
                ..Evidence::default()
            })
        ),
        Gate::Reset(DeviceResetReason::Replaced)
    ));
}

fn ends_json(ends: Ends) -> Value {
    json!({"writes": ends.writes, "entries": ends.entries})
}

fn scan_json(scan: &Result<Evidence, ()>) -> Value {
    match scan {
        Err(()) => json!({"error": true}),
        Ok(e) => json!({
            "listed": {"writes": e.listed.logs.writes, "entries": e.listed.logs.entries,
                       "snapshot": e.listed.snapshots},
            "covered": ends_json(e.covered),
            "posted": ends_json(e.posted),
            "replaced": e.replaced,
            "removed": e.removed,
        }),
    }
}

fn scan_result(scan: &Result<Evidence, ()>) -> Result<Evidence, SyncError> {
    scan.map_err(|()| StorageFailure::Network.into())
}

fn identity_runner(cases: &[Value]) -> Vec<Value> {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let runner = std::env::var("COVEN_IDENTITY_LEAN").expect("Lean identity runner");
    let mut child = Command::new(runner)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(cases).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The model has no key-copy counter; those cases keep it equal on both sides.
fn scans() -> Vec<Result<Evidence, ()>> {
    let mut scans = vec![Err(())];
    for writes in [0, 2, 3] {
        for entries in [0, 1, 2] {
            for snapshots in [0, 4, 5] {
                for (covered, posted) in [(0, 0), (3, 0), (0, 2)] {
                    for (replaced, removed) in
                        [(false, false), (true, false), (false, true), (true, true)]
                    {
                        scans.push(Ok(Evidence {
                            listed: Counters {
                                logs: Ends { writes, entries },
                                key_copies: 0,
                                snapshots,
                            },
                            covered: Ends {
                                writes: covered,
                                entries: 0,
                            },
                            posted: Ends {
                                writes: 0,
                                entries: posted,
                            },
                            replaced,
                            removed,
                        }));
                    }
                }
            }
        }
    }
    scans
}

#[test]
#[ignore = "requires COVEN_IDENTITY_LEAN"]
fn st_differential_identity() {
    let mine = reserved(2, 1, 0, 4);
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for custody in [None, Some(7), Some(8)] {
        for scan in scans() {
            cases.push(json!({
                "check": "identity",
                "device": 7,
                "reserved": {"writes": mine.writes, "entries": mine.entries,
                             "snapshot": mine.snapshots},
                "custody": custody,
                "scan": scan_json(&scan),
            }));
            actual.push(json!(verdict(&check(
                DeviceId(7),
                &mine,
                custody.map(DeviceId),
                scan_result(&scan),
            ))));
        }
    }
    assert_eq!(actual, identity_runner(&cases));
}

/// The real stop: the catch-up that applies a kept replacement checks it with
/// custody already compared at opening, and the stopped gate answers later.
#[test]
#[ignore = "requires COVEN_IDENTITY_LEAN"]
fn st_differential_replacement_read() {
    let device = DeviceId(7);
    let mine = reserved(2, 1, 0, 4);
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    let quiet = Ok(Evidence::default());
    for olds in [vec![], vec![3], vec![7], vec![3, 7], vec![7, 3]] {
        for later in [quiet, Err(())] {
            for custody in [Some(7), None] {
                let gate = SendGate::new(std::sync::Arc::new(
                    coven_foundation::clock::FixedClock::new(std::time::UNIX_EPOCH),
                ));
                let mut events = Vec::new();
                let mut results = Vec::new();
                let mut kept = Vec::new();
                for (index, old) in olds.iter().enumerate() {
                    kept.push(DeviceId(*old));
                    events.push(
                        json!({"op":"observe","old":old,"fresh":100 + index,"own_fresh":200}),
                    );
                    let observed = Evidence {
                        replaced: replaced_by(kept.iter().copied(), device),
                        ..Evidence::default()
                    };
                    if let Gate::Reset(reason) = check(device, &mine, Some(device), Ok(observed)) {
                        gate.stop(reason);
                    }
                    events.push(json!({"op":"gate","custody":custody,"scan":scan_json(&later)}));
                    results.push(json!(verdict(&match gate.stopped() {
                        Some(reason) => Gate::Reset(reason),
                        None => check(device, &mine, custody.map(DeviceId), scan_result(&later),),
                    })));
                }
                cases.push(json!({
                    "check": "replacement",
                    "device": 7,
                    "reserved": {"writes": mine.writes, "entries": mine.entries,
                                 "snapshot": mine.snapshots},
                    "events": events,
                }));
                actual.push(json!(results));
            }
        }
    }
    assert_eq!(actual, identity_runner(&cases));
}
