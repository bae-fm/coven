//! The time and bytes one read of the sequence spent, logged once its slots
//! apply.

use crate::{object_input::ObjectInput, SyncError};

/// What one read of the sequence spent, logged once its slots apply.
pub(super) struct SlotReading {
    started: std::time::Duration,
    /// Slot reads, from what the device holds or from storage, including
    /// the miss that ends them.
    requests: usize,
    bytes: u64,
    fetching: std::time::Duration,
    /// Checking signatures and opening each slot's sealed prefix.
    pub(super) opening: std::time::Duration,
    /// Existence checks that bracket the sequence's end.
    pub(super) locating: std::time::Duration,
    pub(super) floor: std::time::Duration,
}

impl SlotReading {
    pub(super) fn new(started: std::time::Duration) -> Self {
        Self {
            started,
            requests: 0,
            bytes: 0,
            fetching: std::time::Duration::ZERO,
            opening: std::time::Duration::ZERO,
            locating: std::time::Duration::ZERO,
            floor: std::time::Duration::ZERO,
        }
    }

    pub(super) fn fetched(
        &mut self,
        time: std::time::Duration,
        fetched: &Result<Option<ObjectInput>, SyncError>,
    ) {
        self.requests += 1;
        self.fetching += time;
        if let Ok(Some(input)) = fetched {
            self.bytes += input.object().size;
        }
    }

    /// Log once the slots read from `self.started` until `applying` have
    /// applied, at `applied_at`.
    pub(super) fn log(
        &self,
        applied: usize,
        writes: usize,
        applying: std::time::Duration,
        applied_at: std::time::Duration,
    ) {
        tracing::info!(
            slots = applied,
            writes,
            requests = self.requests,
            bytes = self.bytes,
            fetching = ?self.fetching,
            opening = ?self.opening,
            locating = ?self.locating,
            floor = ?self.floor,
            reading = ?(applying - self.started),
            applying = ?(applied_at - applying),
            "read and applied slots"
        );
    }
}
