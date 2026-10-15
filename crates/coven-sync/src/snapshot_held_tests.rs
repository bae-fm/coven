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
