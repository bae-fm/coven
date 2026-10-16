use super::*;
use crate::replay::tests::{checked_log, household, make, replace, History};
use coven_format::store_log::{MemberRole, StoreChange, StoreLogEntry};
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
        Gate::InUse(evidence) => format!(
            "reset:{}",
            match evidence {
                DeviceIdEvidence::StorageAhead => "storage-ahead",
                DeviceIdEvidence::DifferentBytes => "different-bytes",
                DeviceIdEvidence::Replaced => "replaced",
            }
        ),
    }
}

#[test]
fn equal_counters_send_and_any_larger_stored_number_stops() {
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
    assert!(matches!(check(&mine, Ok(evidence(equal))), Gate::Send));
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
            check(&mine, Ok(evidence(ahead))),
            Gate::InUse(DeviceIdEvidence::StorageAhead)
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
            check(&mine, Ok(found)),
            Gate::InUse(DeviceIdEvidence::StorageAhead)
        ));
    }
}

#[test]
fn a_failed_scan_sends_nothing_and_a_kept_replacement_stops() {
    let mine = reserved(1, 1, 0, 0);
    assert!(matches!(
        check(&mine, Err(StorageFailure::Network.into())),
        Gate::Pending(_)
    ));
    assert!(matches!(
        check(
            &mine,
            Ok(Evidence {
                replaced: true,
                ..Evidence::default()
            })
        ),
        Gate::InUse(DeviceIdEvidence::Replaced)
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
                       "key_copies": e.listed.key_copies, "snapshot": e.listed.snapshots},
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

pub(crate) fn identity_runner(cases: &[Value]) -> Vec<Value> {
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

/// Every kind of evidence below, at, and beyond the reservations of
/// `st_differential_identity`.
fn scans() -> Vec<Result<Evidence, ()>> {
    let mut scans = vec![Err(())];
    for writes in [0, 2, 3] {
        for entries in [0, 1, 2] {
            for key_copies in [0, 3, 4] {
                for snapshots in [0, 4, 5] {
                    for (covered, posted) in [(0, 0), (3, 0), (0, 2)] {
                        for (replaced, removed) in
                            [(false, false), (true, false), (false, true), (true, true)]
                        {
                            scans.push(Ok(Evidence {
                                listed: Counters {
                                    logs: Ends { writes, entries },
                                    key_copies,
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
    }
    scans
}

#[test]
#[ignore = "requires COVEN_IDENTITY_LEAN"]
fn st_differential_identity() {
    let mine = reserved(2, 1, 3, 4);
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    // Custody names this device: opening refuses any other (T276).
    for scan in scans() {
        cases.push(json!({
            "check": "identity",
            "device": 7,
            "reserved": {"writes": mine.writes, "entries": mine.entries,
                         "key_copies": mine.key_copies, "snapshot": mine.snapshots},
            "custody": 7,
            "scan": scan_json(&scan),
        }));
        actual.push(json!(verdict(&check(&mine, scan_result(&scan)))));
    }
    assert_eq!(actual, identity_runner(&cases));
}

/// The real stop: the catch-up that applies a kept replacement checks it,
/// with custody already compared at opening, and the stopped gate answers
/// later. Stopping chooses no fresh id.
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
            let gate = SendGate::new(std::sync::Arc::new(
                coven_foundation::clock::FixedClock::new(std::time::UNIX_EPOCH),
            ));
            let mut events = Vec::new();
            let mut results = Vec::new();
            // Ana's devices 3 and 7; each restored copy registers a new id.
            let mut history = History::new();
            history.all(0, 3, crate::replay::tests::device(3));
            history.all(0, 7, crate::replay::tests::device(7));
            for (index, old) in olds.iter().enumerate() {
                let fresh = 100 + index as u64;
                history.all(0, fresh, replace(fresh, *old));
                events.push(json!({"op":"observe","old":old,"fresh":fresh}));
                let observed = Evidence {
                    replaced: replaced(&checked_log(&history.entries), device),
                    ..Evidence::default()
                };
                if let Gate::InUse(evidence) = check(&mine, Ok(observed)) {
                    gate.stop(evidence);
                }
                events.push(json!({"op":"gate","custody":7,"scan":scan_json(&later)}));
                results.push(json!(verdict(&match gate.stopped() {
                    Some(evidence) => Gate::InUse(evidence),
                    None => check(&mine, scan_result(&later)),
                })));
            }
            cases.push(json!({
                "check": "replacement",
                "device": 7,
                "reserved": {"writes": mine.writes, "entries": mine.entries,
                             "key_copies": mine.key_copies, "snapshot": mine.snapshots},
                "events": events,
            }));
            actual.push(json!(results));
        }
    }
    assert_eq!(actual, identity_runner(&cases));
}

/// A kept registration, as `ReplacementRead.Registration` names it.
fn registrations(replay: &coven_database::StoreLogReplay, entries: &[StoreLogEntry]) -> Vec<Value> {
    entries
        .iter()
        .filter_map(|entry| match entry.change {
            StoreChange::AddDevice {
                replaces: Some(old),
                ..
            } if replay.entries[&entry.position] == coven_database::EntryOutcome::Kept => {
                Some(json!({"old": old.0, "fresh": entry.position.device.0,
                       "number": entry.position.number}))
            }
            _ => None,
        })
        .collect()
}

fn past_json(entries: &[StoreLogEntry], read: impl Fn(&StoreLogEntry) -> bool) -> Vec<Value> {
    entries
        .iter()
        .filter(|entry| read(entry))
        .map(|entry| json!({"device": entry.position.device.0, "number": entry.position.number}))
        .collect()
}

/// The registrations a replay of exactly these entries keeps.
fn view_registrations(entries: Vec<StoreLogEntry>) -> Vec<Value> {
    registrations(&crate::replay::tests::replay(&entries), &entries)
}

/// One old-id write and one old-id entry for each `(author, old id, past)`,
/// judged by the real write authority and entry replay. Each object carries
/// its recorded past and the registrations the replay of that past keeps.
fn admission_case(base: &History, objects: &[(u8, u64, Vec<usize>)]) -> (Value, Vec<bool>) {
    let log = checked_log(&base.entries);
    let mut replays = crate::replay_cache::ReplayCache::new(&log);
    let mut judged = Vec::new();
    let mut verdicts = Vec::new();
    for (author, old, past) in objects {
        let (author, old) = (*author, *old);
        // An old-id write recording exactly this past.
        let store_log_read = coven_format::value::EntryPositions(
            past.iter()
                .fold(std::collections::BTreeMap::new(), |mut ends, &index| {
                    let id = base.entries[index].position;
                    let end = ends.entry(id.device).or_insert(id);
                    if id.number > end.number {
                        *end = id;
                    }
                    ends
                })
                .into_values()
                .collect(),
        );
        let header = coven_format::write::WriteHeader {
            position: coven_merge::WriteId {
                device: DeviceId(old),
                number: 1,
            },
            timestamp: coven_merge::Timestamp::new(1_000, 0, DeviceId(old)).unwrap(),
            had_read: coven_format::value::WritePositions(Vec::new()),
            store_log_read: store_log_read.clone(),
            schema_version: 1,
            disposition: coven_format::write::WriteDisposition::Apply,
        };
        verdicts.push(
            match crate::write_object::authority(
                &log,
                &mut replays,
                &coven_format::write_stream::WriteHeaderFrame {
                    header,
                    parts: Vec::new(),
                },
            ) {
                Ok(signer) => {
                    assert_eq!(signer, crate::replay::tests::keys(author).signing);
                    true
                }
                Err(crate::Refusal::NotAuthorized) => false,
                Err(other) => panic!("{other:?}"),
            },
        );
        let read = |e: &StoreLogEntry| store_log_read.covers(e.position);
        judged.push(json!({
            "device": old, "number": judged.len() + 1, "kind": "write",
            "past": past_json(&base.entries, read),
            "view": view_registrations(base.entries.iter().filter(|e| read(e)).cloned().collect()),
        }));
        // An old-id entry with the same past, replayed with the history.
        let mut history = base.clone();
        history.push(author, old, past, make(40, "Gifts"));
        let entry = history.entries.last().unwrap().clone();
        let replay = crate::replay::tests::replay(&history.entries);
        assert_eq!(
            registrations(&replay, &history.entries),
            registrations(&log.replay, &base.entries)
        );
        verdicts.push(match &replay.entries[&entry.position] {
            coven_database::EntryOutcome::Kept => true,
            coven_database::EntryOutcome::Dropped(coven_database::DropReason::NotAllowed) => false,
            other => panic!("{other:?}"),
        });
        let read = |e: &StoreLogEntry| crate::replay::had_read(&entry, e);
        judged.push(json!({
            "device": old, "number": judged.len() + 1, "kind": "entry",
            "past": past_json(&base.entries, read),
            "view": view_registrations(base.entries.iter().filter(|e| read(e)).cloned().collect()),
        }));
    }
    (json!({"check": "admission", "objects": judged}), verdicts)
}

/// The real write authority and entry replay against `ReplacementRead.acceptsView`:
/// Ben's phone 1 and Carol's tablet 2, with every subset of replacements by
/// Ben's restored copies 5 and 6 (concurrent), Carol's copy 8 of her tablet
/// and Carol's unauthorized claim 7 on Ben's phone, and every subset of them
/// read. One entry ever registers an id (§10), so only its author's removal
/// drops a replacement, which also excludes his objects; the history where an
/// object's own view keeps a replacement the full replay drops is pinned by
/// `a_dropped_replacement_stops_replacing_without_rejudging_recorded_pasts`.
#[test]
#[ignore = "requires COVEN_IDENTITY_LEAN"]
fn st_differential_replacement_admission() {
    let candidates = [(1u8, 5u64, 1u64), (1, 6, 1), (2, 7, 1), (2, 8, 2)];
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    // Every judged object is distinct, so `loadViews` applies exactly the accepted ones.
    let mut judge = |base: &History, objects: &[(u8, u64, Vec<usize>)]| {
        let (case, verdicts) = admission_case(base, objects);
        cases.push(case);
        actual.push(json!({"accepts": verdicts, "loaded": verdicts}));
        verdicts
    };
    for chosen in 0..1u32 << candidates.len() {
        let mut base = household(MemberRole::Admin, MemberRole::Member);
        let registered: Vec<usize> = (0..base.entries.len()).collect();
        let mut replacements = Vec::new();
        for (bit, (author, new, old)) in candidates.iter().enumerate() {
            if chosen & (1 << bit) != 0 {
                base.push(*author, *new, &registered, replace(*new, *old));
                replacements.push(base.entries.len() - 1);
            }
        }
        let mut objects = Vec::new();
        for read in 0..1u32 << replacements.len() {
            let mut past = registered.clone();
            past.extend(
                replacements
                    .iter()
                    .enumerate()
                    .filter(|(bit, _)| read & (1 << bit) != 0)
                    .map(|(_, index)| *index),
            );
            for (author, old) in [(1u8, 1u64), (2, 2)] {
                objects.push((author, old, past.clone()));
            }
        }
        judge(&base, &objects);
    }
    assert_eq!(actual, identity_runner(&cases));
}

/// A replaced local id stops this copy with that evidence; a removed one
/// stops it for good, even when a kept replacement also names it (§10).
#[test]
fn local_checks_stop_a_replaced_id_and_keep_a_removed_one_stopped() {
    let ben = crate::store_log_sync::tests::member(2);
    assert_eq!(ben.member(), crate::replay::tests::member(1));
    let mut replaced_phone = household(MemberRole::Admin, MemberRole::Member);
    replaced_phone.all(1, 5, replace(5, 1));
    let log = checked_log(&replaced_phone.entries);
    assert!(replaced(&log, DeviceId(1)));
    assert!(matches!(
        crate::write_seal::check_member(&log, &ben, DeviceId(1)),
        Err(SyncError::DeviceIdInUse(DeviceIdEvidence::Replaced))
    ));
    assert!(crate::write_seal::check_member(&log, &ben, DeviceId(5)).is_ok());
    for removal in [
        StoreChange::RemoveDevice {
            device: DeviceId(1),
        },
        StoreChange::RemoveMember {
            member: crate::replay::tests::member(1),
        },
    ] {
        let mut removed = replaced_phone.clone();
        removed.all(0, 0, removal);
        let log = checked_log(&removed.entries);
        assert!(!replaced(&log, DeviceId(1)));
        assert!(log.replay.state.devices[&DeviceId(1)].removed());
        assert!(matches!(
            crate::write_seal::check_member(&log, &ben, DeviceId(1)),
            Err(SyncError::Removed)
        ));
    }
}
