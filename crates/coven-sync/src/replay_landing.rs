//! Permanent admission against every unread stored entry, and a raise's own
//! landing window (§9, §15, C10).

use super::{had_read_position, ENTRY_WINDOW};
use coven_format::value::StorageTime;
use coven_format::{
    store_log::{StoreChange, StoreLogEntry},
    value::EntryId,
};
use std::collections::BTreeMap;

pub(crate) fn landed_too_late(
    entry: &StoreLogEntry,
    publications: &BTreeMap<EntryId, StorageTime>,
) -> bool {
    let stored_at = publications[&entry.position];
    raise_too_late(entry, stored_at)
        || publications.iter().any(|(prior, time)| {
            stored_at
                .duration_since(*time)
                .is_some_and(|elapsed| elapsed > ENTRY_WINDOW)
                && !had_read_position(entry, *prior)
        })
}

/// A raise entry counts only if it lands at most one day after its named
/// snapshot's `T_c`. Retention stops holding writes for a listed raise
/// snapshot once that day is past and no received entry names it, so a
/// later entry naming it must never become a boundary (io
/// `RaiseHistory.expire`, `expired_stays_out`).
fn raise_too_late(entry: &StoreLogEntry, stored_at: StorageTime) -> bool {
    matches!(&entry.change, StoreChange::RaiseSchema { caught_at, .. }
        if !caught_at.within_day(stored_at))
}

#[cfg(test)]
#[path = "replay_landing_tests.rs"]
mod tests;
