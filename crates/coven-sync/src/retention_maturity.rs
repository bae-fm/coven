//! When a snapshot has passed grace: the provider's publication time of its
//! announcement's slot, observed once (§16.6, §20.5).
use super::GRACE;
use crate::{object_input::ObjectInput, sequence_sync::SequenceSync, SyncError};
use coven_format::sequence::SnapshotAnnouncement;
use coven_merge::Slot;
use coven_storage::{ObjectPath, StorageFailure, StoredObject};
use std::time::UNIX_EPOCH;

impl SequenceSync {
    /// One immutable observation supplies publication age for all later passes.
    pub(crate) async fn immutable_metadata(
        &self,
        path: &ObjectPath,
    ) -> Result<StoredObject, SyncError> {
        Ok(self.observed_metadata(path).await?.ok_or(StorageFailure::NotFound)?)
    }

    /// The object's one immutable observation, or none when storage holds no
    /// object at `path`.
    async fn observed_metadata(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<StoredObject>, SyncError> {
        if let Some(input) = ObjectInput::retained(&self.database, path).await? {
            return Ok(Some(input.object().clone()));
        }
        let Some(object) = self.storage.status(path).await? else {
            return Ok(None);
        };
        ObjectInput::observe(&self.database, &object).await?;
        Ok(Some(object))
    }

    /// The publication of the announcement at the kept slot `slot`, at or
    /// above the floor's first kept slot. Missing from storage, it is damaged,
    /// as a pass reports a kept slot it can't find (§6.1, §16.6).
    async fn kept_announcement_metadata(&self, slot: Slot) -> Result<StoredObject, SyncError> {
        let path = self.path(slot.0)?;
        self.observed_metadata(&path).await?.ok_or_else(|| {
            crate::write_object::damaged(&path, crate::Refusal::Parse { cause: None })
        })
    }

    pub(crate) fn past_grace(&self, object: &StoredObject) -> bool {
        let Ok(now) = self.clock.now().duration_since(UNIX_EPOCH) else {
            return false;
        };
        object
            .stored_at
            .elapsed_since_epoch()
            .checked_add(object.time_resolution)
            .and_then(|at| at.checked_add(GRACE))
            .is_some_and(|deadline| deadline <= now)
    }

    /// Candidates are in descending announcement-slot order. An immature
    /// announcement cannot hide an older snapshot whose grace has passed.
    /// Only the announcements at or above the floor's first kept slot, `first`,
    /// are candidates: the slot of one below it may be deleted, taking its
    /// publication time with it. One below it never bounds `first` past where
    /// it is, since it covers less than `first - 1`, and a body it alone would
    /// replace stays until a kept replacement passes grace
    /// (`Retention.kept_candidates_same_first`, `Retention.kept_replacement`).
    pub(super) async fn newest_mature_snapshot<'a>(
        &self,
        first: u64,
        candidates: impl Iterator<Item = (&'a Slot, &'a SnapshotAnnouncement)>,
    ) -> Result<Option<(&'a Slot, &'a SnapshotAnnouncement)>, SyncError> {
        for (slot, snapshot) in candidates.take_while(|(slot, _)| slot.0 >= first) {
            if self.past_grace(&self.kept_announcement_metadata(*slot).await?) {
                return Ok(Some((slot, snapshot)));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "retention_maturity_tests.rs"]
mod tests;
