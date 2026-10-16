//! Appendix C: `authorViews`, `scan`, and `settle`.

use coven_format::value::StorageTime;
use std::collections::BTreeMap;
use std::time::Duration;

use coven_database::{
    DropReason, EntryOutcome, ReplayEntry, StoreLog, StoreLogCheck, StoreLogReplay, StoreLogState,
};
use coven_format::{store_log::StoreLogEntry, value::EntryId};
use coven_foundation::id_source::DeviceId;

use crate::{conflicts, effects};

/// The landing deadline and finality's quiet window must use the same duration (§9).
pub(crate) const ENTRY_WINDOW: Duration = Duration::from_secs(30 * 24 * 60 * 60);

#[path = "replay_landing.rs"]
mod landing;
use landing::landed_too_late;

#[path = "replay_circles.rs"]
pub(crate) mod circles;

/// Replay the applied, checked entries, independently of their arrival order (§9).
///
/// This is a pure function of the entry set, with no clock, storage or database reads.
///
/// The input is a set: positions and timestamps are unique, creation is first,
/// and every entry's recorded past (including its device's own earlier entries)
/// is present and causally closed. These are Appendix C's `Valid` assumptions.
/// Checking downloaded entries and deciding when they are ready precedes this call.
/// Authority and required targets are checked by this replay; failures produce
/// dropped marks, not errors or partial state. Circle-deleting removals use the
/// author's view; collectively empty circles finish after replay. None of those
/// facts can be established by decoding one store-log frame.
/// `publications` includes every stored entry, even entries outside this received
/// set and entries whose replay drops. Complete the storage scan before calling.
pub fn replay(
    entries: &[StoreLogEntry],
    publications: &BTreeMap<EntryId, StorageTime>,
) -> StoreLogReplay {
    let mut entries: Vec<_> = entries.iter().collect();
    entries.sort_by_key(|entry| entry.timestamp);
    let mut views = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let past = (0..index)
            .filter(|&prior| had_read(entry, entries[prior]))
            .collect::<Vec<_>>();
        let view = settle(&entries, &views, &past).state;
        views.push(check(&view, entry, landed_too_late(entry, publications)));
    }
    settle(&entries, &views, &(0..entries.len()).collect::<Vec<_>>())
}

/// Replay one ready entry using checks retained with the previously applied entries.
///
/// `log` must be a committed result (or the equivalent pure result), and `entry`
/// must be new and have its entire recorded past in that log. The returned entry
/// and the whole result for the previous entries plus this entry are passed
/// together to [`coven_database::Database::apply_store_log`]. This function is pure;
/// past checks never change; replay recomputes all other kept/dropped marks.
/// `publications` contains the complete stored history through this entry's
/// landing, including dropped entries and writers not yet applied in `log`.
pub fn replay_entry(
    log: &StoreLog,
    entry: StoreLogEntry,
    publications: &BTreeMap<EntryId, StorageTime>,
) -> (ReplayEntry, StoreLogReplay) {
    let mut batch = ReplayBatch::new(log);
    batch.push_checked(entry, publications);
    let (mut entries, replay) = batch.finish().expect("one ready entry");
    (entries.pop().expect("one entry"), replay)
}

/// Checked inputs for one transaction. Author views are immutable projections;
/// no current membership state is exposed before the whole received set settles.
pub(crate) struct ReplayBatch {
    entries: Vec<ReplayEntry>,
    received: Vec<ReplayEntry>,
    views: BTreeMap<Vec<EntryId>, StoreLogReplay>,
}

impl ReplayBatch {
    pub(crate) fn new(log: &StoreLog) -> Self {
        let mut entries = log.entries.clone();
        entries.sort_by_key(|entry| entry.entry.timestamp);
        Self {
            entries,
            received: Vec::new(),
            views: [(log.positions().0, log.replay.clone())].into(),
        }
    }

    pub(crate) fn contains(&self, id: EntryId) -> bool {
        self.entries.iter().any(|entry| entry.entry.position == id)
    }

    /// The applied entries and those checked into this batch so far.
    pub(crate) fn entries(&self) -> &[ReplayEntry] {
        &self.entries
    }

    /// Check a received entry against its own recorded past once that past
    /// has applied: its device's registered owner, then its signature with the
    /// author's key in that view (§9).
    pub(crate) fn push(
        &mut self,
        opened: &crate::store_log_object::OpenedEntry,
        publications: &BTreeMap<EntryId, StorageTime>,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<bool, crate::Refusal> {
        let entry = &opened.entry;
        if !crate::store_log_object::causally_ready(&self.entries, entry, store)? {
            return Ok(false);
        }
        let view = &self.author_view(entry).state;
        if !crate::store_log_object::author_matches_view(view, entry) {
            return Err(crate::Refusal::WrongIdentity { cause: None });
        }
        opened.verify(view)?;
        self.push_checked(entry.clone(), publications);
        Ok(true)
    }

    fn author_view(&mut self, entry: &StoreLogEntry) -> &StoreLogReplay {
        let positions = author_positions(entry);
        crate::replay_cache::cached_view(&mut self.views, &self.entries, &positions)
    }

    pub(crate) fn push_checked(
        &mut self,
        entry: StoreLogEntry,
        publications: &BTreeMap<EntryId, StorageTime>,
    ) {
        let late = landed_too_late(&entry, publications);
        let check = check(&self.author_view(&entry).state, &entry, late);
        let incoming = ReplayEntry {
            stored_at: publications[&entry.position],
            entry,
            check,
        };
        let index = self
            .entries
            .partition_point(|prior| prior.entry.timestamp < incoming.entry.timestamp);
        self.entries.insert(index, incoming.clone());
        self.received.push(incoming);
    }

    pub(crate) fn finish(self) -> Option<(Vec<ReplayEntry>, StoreLogReplay)> {
        if self.received.is_empty() {
            return None;
        }
        let entries: Vec<_> = self.entries.iter().map(|entry| &entry.entry).collect();
        let checks: Vec<_> = self
            .entries
            .iter()
            .map(|entry| entry.check.clone())
            .collect();
        let replay = settle(&entries, &checks, &(0..entries.len()).collect::<Vec<_>>());
        Some((self.received, replay))
    }
}

/// Lateness excludes an entry from every replay, but its authority in the
/// recorded view still decides whether its key introduction counts (§9, §11).
fn check(view: &StoreLogState, entry: &StoreLogEntry, late: bool) -> StoreLogCheck {
    let check = effects::check(view, entry);
    if late {
        StoreLogCheck::LandedTooLate {
            authorized: check.authorized(),
        }
    } else {
        check
    }
}

pub(crate) fn author_positions(entry: &StoreLogEntry) -> coven_format::value::EntryPositions {
    let mut positions = entry.had_read.clone();
    if entry.position.number > 1 {
        let index = positions
            .0
            .partition_point(|id| id.device < entry.position.device);
        positions.0.insert(
            index,
            EntryId {
                number: entry.position.number - 1,
                ..entry.position
            },
        );
    }
    positions
}

pub(crate) fn had_read(entry: &StoreLogEntry, prior: &StoreLogEntry) -> bool {
    had_read_position(entry, prior.position)
}

/// The immutable authoring view of a write. The caller checks the supplied
/// frontier is present and causally closed before requesting this projection.
pub(crate) fn at(
    log: &StoreLog,
    positions: &coven_format::value::EntryPositions,
) -> StoreLogReplay {
    at_entries(&log.entries, positions)
}

pub(crate) fn at_entries(
    applied: &[ReplayEntry],
    positions: &coven_format::value::EntryPositions,
) -> StoreLogReplay {
    let mut prior: Vec<_> = applied.iter().collect();
    prior.sort_by_key(|applied| applied.entry.timestamp);
    let entries: Vec<_> = prior.iter().map(|applied| &applied.entry).collect();
    let checks: Vec<_> = prior.iter().map(|applied| applied.check.clone()).collect();
    let selected: Vec<_> = entries
        .iter()
        .enumerate()
        .filter_map(|(i, e)| positions.covers(e.position).then_some(i))
        .collect();
    settle(&entries, &checks, &selected)
}

pub(crate) fn had_read_position(entry: &StoreLogEntry, prior: EntryId) -> bool {
    #[cfg(test)]
    tests::causal_check();
    (entry.position.device == prior.device && entry.position.number > prior.number)
        || entry.had_read.covers(prior)
}

fn settle(
    entries: &[&StoreLogEntry],
    views: &[StoreLogCheck],
    selected: &[usize],
) -> StoreLogReplay {
    let mut dropped: BTreeMap<_, _> = selected
        .iter()
        .filter_map(|&i| {
            views[i]
                .landed_too_late()
                .then_some((entries[i].position, DropReason::LandedTooLate))
        })
        .collect();
    // Each restart drops at least one kept entry. Exhausting this bound is an
    // implementation defect, never a reason to publish an unfinished replay.
    for _ in 0..=selected.len() {
        let mut state = StoreLogState::default();
        let mut kept = Vec::<usize>::new();
        let mut devices = BTreeMap::<DeviceId, Vec<usize>>::new();
        let mut restart = false;
        for &index in selected {
            #[cfg(test)]
            tests::visit_entry();
            let entry = entries[index];
            if dropped.contains_key(&entry.position) {
                continue;
            }
            match effects::validate_effect(
                &state,
                &views[index],
                &entry.author,
                entry.position.device,
                &entry.change,
            ) {
                Err(reason) => {
                    dropped.insert(entry.position, reason);
                    continue;
                }
                Ok(true) => {
                    kept.push(index);
                    devices
                        .entry(entry.position.device)
                        .or_default()
                        .push(index);
                    continue;
                }
                Ok(false) => (),
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
            if let Some(&winner) = opponents.iter().find(|&&prior| {
                !conflicts::before(entry, &views[index], entries[prior], &views[prior])
            }) {
                dropped.insert(
                    entry.position,
                    DropReason::BeatenBy(entries[winner].position),
                );
            } else if opponents.is_empty() {
                effects::apply_effect(&mut state, &views[index], entry);
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
            circles::finish(&mut state, entries, views, &kept);
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
