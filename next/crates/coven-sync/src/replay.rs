//! Appendix C: `authorViews`, `scan`, and `settle`.

use std::collections::BTreeMap;

use coven_database::{DropReason, EntryOutcome, StoreLogReplay, StoreLogState};
use coven_format::{store_log::StoreLogEntry, value::EntryId};

use crate::{conflicts, effects};

/// Replay the applied, checked entries, independently of their arrival order (§9).
///
/// The input is a set: positions and timestamps are unique, creation is first,
/// and every entry's recorded past (including its device's own earlier entries)
/// is present and causally closed. These are Appendix C's `Valid` assumptions.
/// Checking downloaded entries and deciding when they are ready precedes this call.
/// Authority, required targets and circle-key lists are checked by this replay;
/// their failures produce dropped marks, not errors or partial state.
pub fn replay(entries: &[StoreLogEntry]) -> StoreLogReplay {
    let mut entries: Vec<_> = entries.iter().collect();
    entries.sort_by_key(|entry| entry.timestamp);
    let mut views = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let past = (0..index)
            .filter(|&prior| had_read(entry, entries[prior]))
            .collect::<Vec<_>>();
        views.push(settle(&entries, &views, &past).state);
    }
    settle(&entries, &views, &(0..entries.len()).collect::<Vec<_>>())
}

pub(crate) fn had_read(entry: &StoreLogEntry, prior: &StoreLogEntry) -> bool {
    (entry.position.device == prior.position.device
        && entry.position.number > prior.position.number)
        || entry.had_read.covers(prior.position)
}

fn settle(
    entries: &[&StoreLogEntry],
    views: &[StoreLogState],
    selected: &[usize],
) -> StoreLogReplay {
    let mut dropped = BTreeMap::<EntryId, DropReason>::new();
    // Each restart drops at least one kept entry. Exhausting this bound is an
    // implementation defect, never a reason to publish an unfinished replay.
    for _ in 0..=selected.len() {
        let mut state = StoreLogState::default();
        let mut kept = Vec::<usize>::new();
        let mut restart = false;
        for &index in selected {
            let entry = entries[index];
            if dropped.contains_key(&entry.position) {
                continue;
            }
            if let Err(reason) = effects::authorize(&views[index], entry) {
                dropped.insert(entry.position, reason);
                continue;
            }
            if effects::already_in_place(&state, entry) {
                kept.push(index);
                continue;
            }
            let next = match effects::effect(&state, &views[index], entry) {
                Ok(next) => next,
                Err(reason) => {
                    dropped.insert(entry.position, reason);
                    continue;
                }
            };
            let opponents: Vec<_> = kept
                .iter()
                .rev()
                .copied()
                .filter(|&prior| {
                    conflicts::conflict(entry, &views[index], entries[prior], &views[prior])
                })
                .collect();
            if let Some(&winner) = opponents
                .iter()
                .find(|&&prior| !conflicts::before(entry, entries[prior]))
            {
                dropped.insert(
                    entry.position,
                    DropReason::BeatenBy(entries[winner].position),
                );
            } else if opponents.is_empty() {
                state = next;
                kept.push(index);
            } else {
                for prior in opponents {
                    let old = dropped.insert(
                        entries[prior].position,
                        DropReason::BeatenBy(entry.position),
                    );
                    assert!(old.is_none(), "a restart must add new drops");
                }
                restart = true;
                break;
            }
        }
        if !restart {
            let entries = kept
                .into_iter()
                .map(|index| (entries[index].position, EntryOutcome::Kept))
                .chain(
                    dropped
                        .into_iter()
                        .map(|(id, reason)| (id, EntryOutcome::Dropped(reason))),
                )
                .collect();
            return StoreLogReplay { state, entries };
        }
    }
    unreachable!("store-log replay exceeded Appendix C's termination bound")
}

#[cfg(test)]
#[path = "replay_tests.rs"]
pub(crate) mod tests;
