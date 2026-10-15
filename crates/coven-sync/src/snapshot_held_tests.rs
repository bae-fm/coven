//! `RaiseHistory.Held` against Rust's selection under each possible boundary.

use crate::store_log_sync::snapshots::catalog::{newest_first, Verified};
use crate::store_log_sync::snapshots::held::{held_selections, PossibleBoundary};
use crate::store_log_sync::tests::store;
use coven_format::sealed_snapshot::SnapshotObjectPrefix;
use coven_format::store_log::SnapshotId;
use coven_format::value::{EntryId, EntryPositions, StorageTime, WritePositions};
use coven_foundation::id_source::{DeviceId, KeyId};
use coven_merge::{Audience, WriteId};
use coven_storage::{ObjectPath, StoredObject};
use serde_json::json;
use std::time::Duration;

/// One generated listing: `(writer, number, stored, caught, positions,
/// reads)`, where `reads` indexes the snapshots whose entry it read.
type Listed = (u64, u64, u64, u64, Vec<(u64, u64)>, Vec<usize>);

fn id(writer: u64, number: u64) -> SnapshotId {
    SnapshotId {
        audience: Audience::Store,
        device: DeviceId(writer),
        number,
    }
}

/// The entry naming listed snapshot `index`: its own device, so no reader
/// covers another snapshot's entry by position.
fn entry(index: usize) -> EntryId {
    EntryId {
        device: DeviceId(100 + index as u64),
        number: 1,
    }
}

fn verified(listing: &[Listed]) -> Vec<Verified> {
    let mut verified: Vec<_> = listing
        .iter()
        .map(|(writer, number, stored, caught, positions, reads)| {
            let mut entries: Vec<_> = reads.iter().map(|index| entry(*index)).collect();
            entries.sort_by_key(|entry| entry.device);
            Verified {
                object: StoredObject {
                    path: ObjectPath::snapshot(
                        store(1),
                        Audience::Store,
                        DeviceId(*writer),
                        (*number).try_into().unwrap(),
                    ),
                    size: 0,
                    stored_at: StorageTime::new(*stored, 0).unwrap(),
                    time_resolution: Duration::from_secs(1),
                    object_id: "id".into(),
                    revision: "1".into(),
                    checksum: None,
                },
                prefix: SnapshotObjectPrefix {
                    audience: Audience::Store,
                    key: KeyId(uuid::Uuid::from_u128(1)),
                    caught_at: StorageTime::new(*caught, 0).unwrap(),
                    raise: None,
                    writes: WritePositions(
                        positions
                            .iter()
                            .map(|(device, number)| WriteId {
                                device: DeviceId(*device),
                                number: *number,
                            })
                            .collect(),
                    ),
                    store_log: EntryPositions(entries),
                },
                load_waits: None,
            }
        })
        // The catalog lists only snapshots that count (§15).
        .filter(|snapshot| snapshot.prefix.counts(snapshot.object.stored_at))
        .collect();
    verified.sort_by(|a, b| newest_first((&a.object, &a.prefix), (&b.object, &b.prefix)));
    verified
}

/// Rust's selection under each possible boundary, compared with
/// `RaiseHistory.Held` for one part audience on generated listings: late
/// landings, readers of each named snapshot's entry, and none or several
/// possible boundaries.
#[test]
#[ignore = "requires COVEN_IO_RUNNER, supplied by scripts/check.sh"]
fn io_differential_held() {
    let mut generator = crate::tests::Generator(0x5eed_4e1d);
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for _ in 0..400 {
        let count = 1 + generator.pick(4);
        let mut listing: Vec<Listed> = Vec::new();
        for index in 0..count {
            let writer = 1 + generator.pick(3) as u64;
            let number = 1 + index as u64;
            let caught = 10 * generator.pick(3) as u64;
            let stored = caught + [0, 5, 86_400, 86_401][generator.pick(4)];
            let positions = (1..=2u64)
                .filter_map(|device| {
                    let number = generator.pick(4) as u64;
                    (number > 0).then_some((device, number))
                })
                .collect();
            let reads = (0..index).filter(|_| generator.pick(3) == 0).collect();
            listing.push((writer, number, stored, caught, positions, reads));
        }
        let mut possible = Vec::new();
        if generator.pick(2) == 0 {
            possible.push(None);
        }
        for index in 0..count {
            if generator.pick(2) == 0 {
                possible.push(Some(index));
            }
        }
        let (writer, number) = (1 + generator.pick(2) as u64, 1 + generator.pick(3) as u64);
        let boundaries: Vec<_> = possible
            .iter()
            .map(|named| {
                named.map(|index: usize| PossibleBoundary {
                    snapshot: id(listing[index].0, listing[index].1),
                    entry: Some(entry(index)),
                })
            })
            .collect();
        let write = WriteId {
            device: DeviceId(writer),
            number,
        };
        actual.push(json!(held_selections(&boundaries, &verified(&listing))
            .is_some_and(|selections| selections
                .iter()
                .all(|p| p.covers(write)))));
        cases.push(json!({
            "check": "held",
            "audience": 0,
            "writer": writer,
            "number": number,
            "snapshots": listing.iter().map(|(w, n, stored, caught, positions, reads)| json!({
                "writer": w, "number": n, "stored": stored, "caught": caught,
                "positions": positions,
                "reads": reads.iter().map(|index| [listing[*index].0, listing[*index].1]).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "possible": possible.iter().map(|named| match named {
                Some(index) => json!([listing[*index].0, listing[*index].1]),
                None => serde_json::Value::Null,
            }).collect::<Vec<_>>(),
        }));
    }
    assert!(actual.contains(&json!(true)) && actual.contains(&json!(false)));
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}

/// `kept_late_held`: Ana's listed raise snapshot omits Ben's write, which his
/// newer snapshot covers; under her snapshot's boundary nothing covers it.
#[test]
fn a_listed_raise_snapshot_holds_what_it_omits() {
    let listing: Vec<Listed> = vec![
        (1, 1, 60, 50, vec![(1, 1)], vec![]),
        (2, 2, 200, 200, vec![(1, 1), (2, 1)], vec![]),
    ];
    let snapshots = verified(&listing);
    let write = WriteId {
        device: DeviceId(2),
        number: 1,
    };
    let covered = |possible: &[Option<PossibleBoundary>]| {
        held_selections(possible, &snapshots)
            .is_some_and(|selections| selections.iter().all(|p| p.covers(write)))
    };
    assert!(covered(&[None]));
    let raise = Some(PossibleBoundary {
        snapshot: id(1, 1),
        entry: None,
    });
    assert!(!covered(&[None, raise.clone()]));
    // A snapshot that read the raise entry and covers the write releases it.
    let mut read = listing.clone();
    read.push((2, 3, 300, 300, vec![(1, 1), (2, 1)], vec![0]));
    let snapshots = verified(&read);
    let raise = Some(PossibleBoundary {
        snapshot: id(1, 1),
        entry: Some(entry(0)),
    });
    assert!(held_selections(&[None, raise], &snapshots)
        .is_some_and(|selections| selections.iter().all(|p| p.covers(write))));
}

/// T216 (`abandoned_raise_released`): Ana's raise snapshot is listed but
/// her device is removed before any entry names it. A raise entry counts
/// only within a day of its `T_c` (§9), so once every entry stored by
/// then is received and none names it, it no longer holds Ben's write.
#[test]
fn a_listed_raise_snapshot_stops_holding_a_day_after_its_catch_up() {
    use crate::store_log_sync::snapshots::held::held_coverage;
    let listing: Vec<Listed> = vec![
        (1, 1, 60, 50, vec![(1, 1)], vec![]),
        (2, 2, 200, 200, vec![(1, 1), (2, 1)], vec![]),
    ];
    let mut snapshots = verified(&listing);
    for snapshot in &mut snapshots {
        if snapshot.object.path.device() == Some(DeviceId(1)) {
            snapshot.prefix.raise = std::num::NonZeroU32::new(2);
        }
    }
    let write = WriteId {
        device: DeviceId(2),
        number: 1,
    };
    let log = coven_database::StoreLog::default();
    let covered = |through: Option<u64>| {
        held_coverage(
            &log,
            &Audience::Store,
            &snapshots,
            through.map(|seconds| StorageTime::new(seconds, 0).unwrap()),
        )
        .is_some_and(|selections| selections.iter().all(|p| p.covers(write)))
    };
    let day = StorageTime::DAY_SECONDS;
    // Unknown, or exactly a day after its `T_c`: an entry could still name it.
    assert!(!covered(None));
    assert!(!covered(Some(50 + day)));
    // Every entry stored by then received and none names it: it is out.
    assert!(covered(Some(50 + day + 1)));
}

/// T225 (`dropped_reset_held`): Carl's final reset `F` and his kept reset `C`
/// both select Ben's snapshot, which covers his write 5; Ana's concurrent
/// reset `R`, dropped now but not final, selects her own snapshot, which has
/// more writes but not his. Replay can still keep `R`, and Ben read it, so
/// the hold keeps write 5; once `R` is final, or landed too late, it never
/// can be kept.
#[test]
fn a_dropped_reset_that_is_not_final_holds_what_its_selection_omits() {
    use crate::store_log_sync::snapshots::held::held_coverage;
    use coven_database::{
        DropReason, EntryOutcome, ReplayEntry, StoreLog, StoreLogCheck, StoreLogReplay,
    };
    use coven_format::store_log::{StoreChange, StoreLogEntry};
    use coven_foundation::id_source::MemberId;
    // Devices: Ana 1, Ben 2, Carl 3. Indexes: F's snapshot, Ana's reset
    // snapshot, C's snapshot, Ben's snapshot; `reads` names the resets read.
    let listing: Vec<Listed> = vec![
        (3, 1, 93, 92, vec![(1, 40), (3, 1)], vec![]),
        (1, 1, 101, 90, vec![(1, 50)], vec![]),
        (3, 2, 99, 95, vec![(1, 40), (3, 2)], vec![0]),
        (2, 1, 105, 100, vec![(1, 40), (2, 5), (3, 2)], vec![0, 2, 1]),
    ];
    let snapshots = verified(&listing);
    let write = WriteId {
        device: DeviceId(2),
        number: 5,
    };
    // The reset naming listed snapshot `index`, stored at `stored`.
    let reset = |index: usize, stored: u64, check: StoreLogCheck| ReplayEntry {
        entry: StoreLogEntry {
            position: entry(index),
            timestamp: coven_merge::Timestamp::new(stored * 1000, 0, entry(index).device).unwrap(),
            author: MemberId(uuid::Uuid::from_u128(index as u128 + 1)),
            had_read: EntryPositions(Vec::new()),
            change: StoreChange::Reset {
                snapshot: id(listing[index].0, listing[index].1),
            },
        },
        stored_at: StorageTime::new(stored, 0).unwrap(),
        check,
    };
    let covered = |ana: StoreLogCheck, horizon: u64| {
        let ana_outcome = if ana.landed_too_late() {
            EntryOutcome::Dropped(DropReason::LandedTooLate)
        } else {
            EntryOutcome::Dropped(DropReason::BeatenBy(entry(2)))
        };
        let log = StoreLog {
            entries: vec![
                reset(0, 94, StoreLogCheck::Allowed),
                reset(2, 100, StoreLogCheck::Allowed),
                reset(1, 102, ana),
            ],
            replay: StoreLogReplay {
                state: Default::default(),
                entries: [
                    (entry(0), EntryOutcome::Kept),
                    (entry(2), EntryOutcome::Kept),
                    (entry(1), ana_outcome),
                ]
                .into(),
            },
            horizon: Some(StorageTime::new(horizon, 0).unwrap()),
        };
        held_coverage(&log, &Audience::Store, &snapshots, None)
            .is_some_and(|selections| selections.iter().all(|p| p.covers(write)))
    };
    // `F` final; `C` and `R` not: `R` may still be kept, and its selection,
    // Ana's snapshot, omits write 5.
    assert!(!covered(StoreLogCheck::Allowed, 95));
    // Every reset final: `R` stays dropped, and `C` and `F` select Ben's.
    assert!(covered(StoreLogCheck::Allowed, 103));
    // `R` landed too late: replay never keeps it.
    assert!(covered(
        StoreLogCheck::LandedTooLate { authorized: true },
        95
    ));
}

/// IO `FinalReset.select`: one audience's kept raise and reset entries in
/// replay order, every sequence of up to four drawn from raises to versions
/// 1 and 2 and resets, each naming one of two snapshots. Rust's
/// `StoreLog::boundaries` keeps exactly the entries the model finds
/// effective, so the boundary it replays is the model's.
#[test]
#[ignore = "scripts/check.sh supplies the Lean IO runner"]
fn io_differential_boundary_select() {
    use coven_database::{EntryOutcome, ReplayEntry, StoreLog, StoreLogCheck};
    use coven_format::store_log::{StoreChange, StoreLogEntry};
    let kinds: Vec<(Option<u32>, u64)> = [None, Some(1), Some(2)]
        .into_iter()
        .flat_map(|version| [1, 2].map(|snapshot| (version, snapshot)))
        .collect();
    let mut sequences: Vec<Vec<(Option<u32>, u64)>> = vec![Vec::new()];
    let mut frontier = sequences.clone();
    for _ in 0..4 {
        frontier = frontier
            .iter()
            .flat_map(|sequence| {
                kinds.iter().map(move |kind| {
                    let mut next = sequence.clone();
                    next.push(*kind);
                    next
                })
            })
            .collect();
        sequences.extend(frontier.iter().cloned());
    }
    let mut cases = Vec::new();
    let mut actual = Vec::new();
    for sequence in sequences {
        let mut log = StoreLog::default();
        for (index, (version, snapshot)) in sequence.iter().enumerate() {
            let snapshot = SnapshotId {
                audience: Audience::Store,
                device: DeviceId(1),
                number: *snapshot,
            };
            let change = match version {
                Some(version) => StoreChange::RaiseSchema {
                    version: *version,
                    snapshot,
                    caught_at: StorageTime::UNIX_EPOCH,
                },
                None => StoreChange::Reset { snapshot },
            };
            let position = entry(index);
            log.entries.push(ReplayEntry {
                entry: StoreLogEntry {
                    position,
                    timestamp: coven_merge::Timestamp::new(index as u64 + 1, 0, DeviceId(2))
                        .unwrap(),
                    author: crate::store_log_sync::tests::member(1).member(),
                    had_read: EntryPositions(Vec::new()),
                    change,
                },
                stored_at: StorageTime::UNIX_EPOCH,
                check: StoreLogCheck::Allowed,
            });
            log.replay.entries.insert(position, EntryOutcome::Kept);
        }
        actual.push(json!(log
            .boundaries()
            .map(|boundary| boundary.entry().device.0 - 100)
            .collect::<Vec<_>>()));
        cases.push(json!({
            "check": "boundaries",
            "entries": sequence.iter().map(|(version, snapshot)| match version {
                Some(version) => json!(["raise", version, snapshot]),
                None => json!(["reset", snapshot]),
            }).collect::<Vec<_>>(),
        }));
    }
    assert_eq!(actual, crate::discovery::tests::io_runner(&cases));
}
