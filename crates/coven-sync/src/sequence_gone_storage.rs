//! Clearing what a device gone for good left in storage besides its files:
//! the uploads it never finished and the snapshot bodies no announcement
//! names (§16.6, §17.10). It never returns to clear them itself, and the
//! devices that delete its files delete these too.

use super::SequenceSync;
use crate::{history::History, SyncError};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use coven_storage::{ObjectPrefix, StorageFailure};
use std::collections::BTreeSet;

impl SequenceSync {
    /// End the uploads `device` left, then delete each snapshot body in its
    /// folders that no announcement names. Uploads go first: a session ended
    /// here can't store a body after the listing. A failure leaves the
    /// caller to clear again from the start.
    pub(super) async fn clear_gone_storage(
        &self,
        history: &History,
        device: DeviceId,
    ) -> Result<(), SyncError> {
        self.state.check()?;
        self.storage.end_device_uploads(self.store(), device).await?;
        self.list_gone_snapshots(history, device).await
    }

    /// List `device`'s folder of each audience's snapshots and delete every
    /// body there that no announcement names. The device is gone for good,
    /// so none can be announced any more; each body the history announced
    /// waits for the rules its own deletion follows. A failure leaves the
    /// operation to list again from the start.
    async fn list_gone_snapshots(
        &self,
        history: &History,
        device: DeviceId,
    ) -> Result<(), SyncError> {
        let audiences: BTreeSet<_> = std::iter::once(Audience::Store)
            .chain(history.introductions.iter().map(|intro| intro.audience.clone()))
            .collect();
        for audience in audiences {
            let folder = ObjectPrefix::device_snapshots(self.store(), audience.clone(), device);
            for object in self.storage.list(&folder).await? {
                let id = object
                    .path
                    .snapshot_id()
                    .filter(|id| id.audience == audience && id.device == device)
                    .ok_or_else(|| {
                        StorageFailure::Protocol
                            .with_source("snapshot listing names another folder")
                    })?;
                if history.announcements.values().any(|announcement| announcement.snapshot == id) {
                    continue;
                }
                self.state.check()?;
                self.storage.delete(&object.path).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sequence_gone_storage_tests.rs"]
mod tests;
