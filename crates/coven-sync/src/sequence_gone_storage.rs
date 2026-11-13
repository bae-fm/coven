//! Listing snapshot folders for bodies no kept announcement names: this
//! device's own, once it forgets an announcement of its own (§16.8), and
//! those of a device gone for good, with the uploads it never finished and
//! the floor bodies the floor does not name (§16.6, §17.10). A gone device
//! never returns to clear them itself, and the devices that delete its files
//! delete these too.

use super::{SequenceSync, Session};
use crate::{history::History, SyncError};
use coven_format::admin::SnapshotId;
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPrefix, StorageFailure};
use std::collections::BTreeSet;

impl SequenceSync {
    /// End the uploads `device` left, with every session at a slot this
    /// device has applied, whoever began it, then delete each snapshot body
    /// in its folders that no announcement names, and each floor body the
    /// floor does not name. Uploads go first: a session ended here can't
    /// store a body after the listing. A failure leaves the caller to clear
    /// again from the start.
    pub(super) async fn clear_gone_storage(
        &self,
        session: &Session,
        device: DeviceId,
    ) -> Result<(), SyncError> {
        self.state.check()?;
        self.storage.end_device_uploads(self.store(), device).await?;
        self.storage.end_settled_claims(self.store(), session.through).await?;
        self.list_gone_snapshots(&session.history, device).await?;
        self.list_gone_floor_bodies(session, device).await
    }

    /// List `device`'s floor bodies and delete every one the floor does not
    /// name. The body it does name is deleted once a newer floor replaces it
    /// (`Retention::LiveBody`).
    async fn list_gone_floor_bodies(
        &self,
        session: &Session,
        device: DeviceId,
    ) -> Result<(), SyncError> {
        let named = session.floor.as_ref().map(|floor| floor.body());
        for object in self.storage.list(&ObjectPrefix::device_floors(self.store(), device)).await? {
            let (writer, number) = object
                .path
                .floor_body_id()
                .filter(|(writer, _)| *writer == device)
                .ok_or_else(|| {
                    StorageFailure::Protocol.with_source("floor body listing names another folder")
                })?;
            if named == Some((writer, number.into())) {
                continue;
            }
            self.state.check()?;
            self.storage.delete(&object.path).await?;
        }
        Ok(())
    }

    /// List this device's own snapshot folders and delete every body there
    /// that no kept announcement and no open operation names (`journaled`,
    /// read in this pass before the listing): a body uploaded and not yet
    /// announced is its operation's (§16.8). A failure leaves the operation
    /// to list again from the start.
    pub(super) async fn list_own_snapshots(
        &self,
        history: &History,
        journaled: &BTreeSet<SnapshotId>,
    ) -> Result<(), SyncError> {
        self.list_snapshots(history, self.device, journaled).await
    }

    /// List `device`'s folder of each audience's snapshots and delete every
    /// body there that no kept announcement names. The device is gone for
    /// good, so none can be announced any more; each body a kept
    /// announcement names waits for the rules its own deletion follows. A
    /// failure leaves the operation to list again from the start.
    async fn list_gone_snapshots(
        &self,
        history: &History,
        device: DeviceId,
    ) -> Result<(), SyncError> {
        self.list_snapshots(history, device, &BTreeSet::new()).await
    }

    /// List `device`'s folder of each audience's snapshots and delete every
    /// body there that neither a kept announcement nor `journaled` names.
    async fn list_snapshots(
        &self,
        history: &History,
        device: DeviceId,
        journaled: &BTreeSet<SnapshotId>,
    ) -> Result<(), SyncError> {
        for audience in history.introduced_audiences() {
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
                if journaled.contains(&id)
                    || history
                        .announcements
                        .values()
                        .any(|announcement| announcement.snapshot == id)
                {
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
