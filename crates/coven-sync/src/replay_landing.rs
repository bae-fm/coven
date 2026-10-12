//! Permanent admission against every unread stored entry (§9, C10).

use super::had_read_position;
use coven_format::{store_log::StoreLogEntry, value::EntryId};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

pub(crate) fn landed_too_late(
    entry: &StoreLogEntry,
    publications: &BTreeMap<EntryId, SystemTime>,
) -> bool {
    let stored_at = publications[&entry.position];
    publications.iter().any(|(prior, time)| {
        stored_at
            .duration_since(*time)
            .is_ok_and(|elapsed| elapsed > Duration::from_secs(30 * 24 * 60 * 60))
            && !had_read_position(entry, *prior)
    })
}

#[cfg(test)]
#[path = "replay_landing_tests.rs"]
mod tests;
