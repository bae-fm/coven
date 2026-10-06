//! Appendix C: `authorViews`, `scan`, and `settle`.

use std::collections::BTreeMap;

use coven_database::{
    DropReason, EntryOutcome, ReplayEntry, StoreLog, StoreLogCheck, StoreLogReplay, StoreLogState,
};
use coven_format::{store_log::StoreLogEntry, value::EntryId};
use coven_foundation::id_source::DeviceId;

use crate::{conflicts, effects};

/// Replay the applied, checked entries, independently of their arrival order (§9).
///
/// This is a pure function of the entry set, with no clock, storage or database reads.
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
        let view = settle(&entries, &views, &past).state;
        views.push(effects::check(&view, entry));
    }
    settle(&entries, &views, &(0..entries.len()).collect::<Vec<_>>())
}

/// Replay one ready entry using checks retained with the previously applied entries.
///
/// `log` must be a committed result (or the equivalent pure result), and `entry`
/// must be new and have its entire recorded past in that log. The returned entry
/// and the whole result for the previous entries plus this entry are passed
/// together to [`coven_database::Database::apply_store_log`]. This function is pure;
/// past checks never change, and kept/dropped marks are recomputed from scratch.
pub fn replay_entry(log: &StoreLog, entry: StoreLogEntry) -> (ReplayEntry, StoreLogReplay) {
    let mut prior: Vec<_> = log.entries.iter().collect();
    prior.sort_by_key(|applied| applied.entry.timestamp);
    let mut entries: Vec<_> = prior.iter().map(|applied| &applied.entry).collect();
    let mut views: Vec<_> = prior.iter().map(|applied| applied.check.clone()).collect();
    let past: Vec<_> = (0..entries.len())
        .filter(|&i| had_read(&entry, entries[i]))
        .collect();
    let check = if past.len() == entries.len() {
        effects::check(&log.replay.state, &entry)
    } else {
        effects::check(&settle(&entries, &views, &past).state, &entry)
    };
    let index = entries.partition_point(|prior| prior.timestamp < entry.timestamp);
    entries.insert(index, &entry);
    views.insert(index, check.clone());
    let result = settle(&entries, &views, &(0..entries.len()).collect::<Vec<_>>());
    (ReplayEntry { entry, check }, result)
}

pub(crate) fn had_read(entry: &StoreLogEntry, prior: &StoreLogEntry) -> bool {
    (entry.position.device == prior.position.device
        && entry.position.number > prior.position.number)
        || entry.had_read.covers(prior.position)
}

fn settle(
    entries: &[&StoreLogEntry],
    views: &[StoreLogCheck],
    selected: &[usize],
) -> StoreLogReplay {
    let mut dropped = BTreeMap::<EntryId, DropReason>::new();
    // Each restart drops at least one kept entry. Exhausting this bound is an
    // implementation defect, never a reason to publish an unfinished replay.
    for _ in 0..=selected.len() {
        let mut state = StoreLogState::default();
        let mut kept = Vec::<usize>::new();
        let mut devices = BTreeMap::<DeviceId, Vec<usize>>::new();
        let mut restart = false;
        for &index in selected {
            let entry = entries[index];
            if dropped.contains_key(&entry.position) {
                continue;
            }
            match &views[index] {
                StoreLogCheck::NotAllowed => {
                    dropped.insert(entry.position, DropReason::NotAllowed);
                    continue;
                }
                StoreLogCheck::WrongCircleKeys => {
                    dropped.insert(entry.position, DropReason::WrongCircleKeys);
                    continue;
                }
                _ => {}
            }
            if effects::already_in_place(&state, entry) {
                kept.push(index);
                devices
                    .entry(entry.position.device)
                    .or_default()
                    .push(index);
                continue;
            }
            if let Err(reason) = effects::check_effect(&state, &views[index], entry) {
                dropped.insert(entry.position, reason);
                continue;
            }
            // Had-read positions cover a prefix of each device's entries.
            // Keep the remaining candidates in reverse timestamp order so the
            // reported winner matches Appendix C's reverse scan of kept entries.
            let mut concurrent = Vec::new();
            for indices in devices.values() {
                let read = indices.partition_point(|&i| had_read(entry, entries[i]));
                concurrent.extend_from_slice(&indices[read..]);
            }
            concurrent.sort_unstable_by(|a, b| b.cmp(a));
            let opponents: Vec<_> = concurrent
                .into_iter()
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
                effects::apply_effect(&mut state, entry);
                kept.push(index);
                devices
                    .entry(entry.position.device)
                    .or_default()
                    .push(index);
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
