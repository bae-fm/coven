//! Read-view replays retained for one device-log download step (§9, §10, §14.4).

use coven_database::{StoreLog, StoreLogReplay};
use coven_format::value::{EntryId, EntryPositions};
use std::collections::BTreeMap;

/// Borrows one immutable log snapshot; dropping the step drops all its views.
/// This holds in-memory values only, not an owner or an external capability.
pub(crate) struct ReplayCache<'log> {
    log: &'log StoreLog,
    views: BTreeMap<Vec<EntryId>, StoreLogReplay>,
}

impl<'log> ReplayCache<'log> {
    pub(crate) fn new(log: &'log StoreLog) -> Self {
        Self {
            log,
            views: BTreeMap::new(),
        }
    }

    /// Reuse the full replay, including dropped marks, for this exact frontier.
    /// Write frontiers are already canonical after header decoding. As with
    /// `replay::at`, the caller checks presence and causal closure first; a cache
    /// hit does not replace the checks specific to each write's author or stamp.
    pub(crate) fn at(&mut self, positions: &EntryPositions) -> &StoreLogReplay {
        self.views
            .entry(positions.0.clone())
            .or_insert_with(|| crate::replay::at(self.log, positions))
    }
}

#[cfg(test)]
#[path = "replay_cache_tests.rs"]
mod tests;
