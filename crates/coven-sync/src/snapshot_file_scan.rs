//! Listing a removed uploader's file folder: once when its removal applies,
//! and again whenever the app asks (§17.9). Nothing the device writes after
//! its cut-off counts (§14.1), so every file a listing finds is judged by the
//! reference evidence alone, whenever the listing is taken.
use crate::{history::History, sequence_sync::SequenceSync, SyncError};
use coven_format::{admin::StoreChange, MemberAccess};
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::Slot;
use coven_storage::{ObjectPrefix, StorageFailure};
use std::collections::BTreeSet;

pub(crate) struct UploaderScan {
    device: DeviceId,
    removal: Slot,
}

/// Which removed uploaders' folders to list.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Listing {
    /// Each folder not yet listed at its latest counted removal: what a pass
    /// lists, so it lists a folder once per removal.
    AtRemoval,
    /// Every folder again, for the app's cleanup: a removed device whose key
    /// still works may have uploaded since.
    Every,
}

impl History {
    pub(crate) fn deletes_for(&self, own: DeviceId, member: MemberId, uploader: DeviceId) -> bool {
        if own == uploader {
            return true;
        }
        let Some(device) = self.state.devices.get(&uploader) else {
            return false;
        };
        if device.active() {
            return false;
        }
        device.member == member
            || (self
                .state
                .members
                .get(&device.member)
                .is_some_and(|value| value.removed)
                && match self.state.members.get(&member).map(|value| &value.access) {
                    // Access-key IDs do not identify an S3 account. The creator is
                    // the only account owner the authenticated history establishes.
                    Some(MemberAccess::S3AccessKey { .. }) => self
                        .recorded_access()
                        .any(|(owner, at, _)| owner == member && at == 1),
                    _ => self.owns_storage(member),
                })
    }

    /// The latest counted removal that cut `device` off: its own removal or
    /// replacement, or its member's removal.
    fn removal(&self, device: DeviceId) -> Option<Slot> {
        let member = self.state.devices.get(&device)?.member;
        self.cutoffs
            .iter()
            .rev()
            .find_map(|(slot, change)| match change {
                StoreChange::RemoveMember { member: removed } if *removed == member => Some(*slot),
                StoreChange::RemoveDevice { device: removed } if *removed == device => Some(*slot),
                StoreChange::AddDevice {
                    replaces: Some(replaced),
                    ..
                } if *replaced == device => Some(*slot),
                _ => None,
            })
    }
}

impl SequenceSync {
    pub(crate) async fn uploader_scans(
        &self,
        history: &History,
        uploaders: &BTreeSet<DeviceId>,
        listing: Listing,
    ) -> Result<Vec<UploaderScan>, SyncError> {
        let mut scans = Vec::new();
        for device in uploaders {
            if history
                .state
                .devices
                .get(device)
                .is_none_or(|device| device.active())
            {
                continue;
            }
            let Some(removal) = history.removal(*device) else {
                continue;
            };
            if listing == Listing::AtRemoval
                && self.database.file_scan(*device).await? == Some(removal)
            {
                continue;
            }
            scans.push(UploaderScan {
                device: *device,
                removal,
            });
        }
        Ok(scans)
    }

    pub(crate) async fn scan_uploaders(&self, scans: Vec<UploaderScan>) -> Result<(), SyncError> {
        for scan in scans {
            let mut files = BTreeSet::new();
            for object in self
                .storage
                .list(&ObjectPrefix::device_files(self.store(), scan.device))
                .await?
            {
                match (object.path.device(), object.path.file_id()) {
                    (Some(device), Some(file)) if device == scan.device => {
                        files.insert(file);
                    }
                    _ => {
                        return Err(StorageFailure::Protocol
                            .with_source("file listing names another folder")
                            .into())
                    }
                }
            }
            if !self
                .database
                .complete_file_scan(scan.device, scan.removal, files)
                .await?
            {
                return Err(StorageFailure::CatchUpRequired.into());
            }
        }
        Ok(())
    }
}
