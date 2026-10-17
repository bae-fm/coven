use super::*;
use crate::{tests::*, *};
use coven_foundation::id_source::DeviceId;

#[test]
fn authored_prefix_and_own_predecessors_match_causality() {
    // Causality.own_predecessor_is_read and intervening_write_not_read.
    let reader = WriteId {
        device: DeviceId(1),
        number: 4,
    };
    for read_through in 0..5 {
        for slot in 1..7 {
            for device in 1..=2 {
                for number in 1..6 {
                    let earlier = WriteId {
                        device: DeviceId(device),
                        number,
                    };
                    let past = SlotPast::new(reader, read_through, |_| Some(Slot(slot)), None);
                    assert_eq!(
                        past.contains(&earlier),
                        slot <= read_through || (device == 1 && number < 4)
                    );
                }
            }
        }
    }
    assert!(had_read(
        reader,
        0,
        WriteId {
            number: 3,
            ..reader
        },
        None
    ));
    assert!(!had_read(reader, 0, id(2), None));
    assert!(!had_read(reader, 0, id(2), Some(Slot(0))));
}

#[test]
fn unique_values_use_slot_then_write_then_primary_key() {
    let mut view = MemoryView::default();
    for n in 1..=3 {
        view.present(row(n), 1, stamp(100 - n));
    }
    let claimed = |slot, number| ClaimOrder::Published {
        slot: Slot(slot),
        number,
    };
    view.claim(&row(1), ["title"], "Plan", claimed(2, 1));
    view.claim(&row(2), ["title"], "Plan", claimed(1, 2));
    view.claim(&row(3), ["title"], "Plan", claimed(1, 1));
    let result = removals(&view).unwrap();
    assert!(!result.removed.contains_key(&row(3)));
    assert!(result.removed.contains_key(&row(1)));
    assert!(result.removed.contains_key(&row(2)));
    view.claim(&row(2), ["title"], "Plan", claimed(1, 1));
    assert!(!removals(&view).unwrap().removed.contains_key(&row(2)));
    view.claim(&row(1), ["title"], "Plan", ClaimOrder::Local { number: 1 });
    assert!(removals(&view).unwrap().removed.contains_key(&row(1)));
}

#[test]
fn older_live_loser_returns_before_a_new_claim() {
    // LiveClaims.older_loser_returns: claims 1 and 2, delete 1, then claim 3.
    let mut view = MemoryView::default();
    for n in 1..=3 {
        view.present(row(n), 1, stamp(4 - n));
        view.claim(
            &row(n),
            ["title"],
            "Plan",
            ClaimOrder::Published {
                slot: Slot(n),
                number: 1,
            },
        );
    }
    view.data
        .insert(row(1), RemovalRow::Absent { generation: 2 });
    let result = removals(&view).unwrap();
    assert!(!result.removed.contains_key(&row(2)));
    assert!(result.removed.contains_key(&row(3)));
}

mod differential {
    use super::*;
    use coven_foundation::id_source::DeviceId;
    use serde_json::{json, Value};
    use std::{
        ffi::OsStr,
        io::Write,
        process::{Command, Stdio},
    };

    fn compare(runner: &OsStr, writes: &[(WriteId, u64, u64)]) {
        let input = json!({"kind":"past", "writes": writes.iter().map(|(id,slot,read)|
            json!({"author":id.device.0,"number":id.number,"slot":slot,"read_through":read}))
            .collect::<Vec<_>>()});
        let rust: Vec<Vec<usize>> = writes
            .iter()
            .map(|(reader, _, read)| {
                let past = SlotPast::new(
                    *reader,
                    *read,
                    |id: &WriteId| {
                        writes
                            .iter()
                            .find(|(w, _, _)| w == id)
                            .map(|(_, s, _)| Slot(*s))
                    },
                    None,
                );
                writes
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (id, _, _))| past.contains(id).then_some(i))
                    .collect()
            })
            .collect();
        let mut child = Command::new(runner)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.to_string().as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{input}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let lean: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json!(rust), lean, "{input}");
    }

    fn enumerate(runner: &OsStr, writes: &mut Vec<(WriteId, u64, u64)>, remaining: usize) -> usize {
        compare(runner, writes);
        let mut count = 1;
        if remaining == 0 {
            return count;
        }
        let slot = writes.len() as u64 + 1;
        for author in 0..2 {
            let prior = writes.iter().rev().find(|(w, _, _)| w.device.0 == author);
            let number = prior.map_or(1, |(w, _, _)| w.number + 1);
            for read in prior.map_or(0, |(_, _, r)| *r)..slot {
                writes.push((
                    WriteId {
                        device: DeviceId(author),
                        number,
                    },
                    slot,
                    read,
                ));
                count += enumerate(runner, writes, remaining - 1);
                writes.pop();
            }
        }
        count
    }

    #[test]
    #[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
    fn sequence_past_matches_lean() {
        let runner = std::env::var_os("COVEN_SEQUENCE_LEAN").expect("sequenceRunner");
        let exhaustive = enumerate(&runner, &mut Vec::new(), 4);
        let mut random = 0;
        for seed in 1..=128u64 {
            let mut rng = seed;
            let mut pick = |n| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng % n
            };
            let mut numbers = [0; 4];
            let mut reads = [0; 4];
            let mut writes = Vec::new();
            for slot in 1..=16 {
                let author = pick(4) as usize;
                for _ in 0..=pick(3) {
                    numbers[author] += 1;
                    reads[author] += pick(slot - reads[author]);
                    writes.push((
                        WriteId {
                            device: DeviceId(author as u64),
                            number: numbers[author],
                        },
                        slot,
                        reads[author],
                    ));
                }
            }
            for i in (1..writes.len()).rev() {
                let j = pick((i + 1) as u64) as usize;
                writes.swap(i, j);
            }
            compare(&runner, &writes);
            random += 1;
        }
        eprintln!("sequence past: {exhaustive} exhaustive + {random} seeded histories");
    }
}
