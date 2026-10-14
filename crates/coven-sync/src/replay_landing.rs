//! Permanent admission against every unread stored entry (§9, C10).

use super::{had_read_position, ENTRY_WINDOW};
use coven_format::value::StorageTime;
use coven_format::{store_log::StoreLogEntry, value::EntryId};
use std::collections::BTreeMap;

pub(crate) fn landed_too_late(
    entry: &StoreLogEntry,
    publications: &BTreeMap<EntryId, StorageTime>,
) -> bool {
    let stored_at = publications[&entry.position];
    publications.iter().any(|(prior, time)| {
        stored_at
            .duration_since(*time)
            .is_some_and(|elapsed| elapsed > ENTRY_WINDOW)
            && !had_read_position(entry, *prior)
    })
}

#[cfg(test)]
#[path = "replay_landing_tests.rs"]
mod tests;
