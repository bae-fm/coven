//! Listing the file folder of an uploader gone for good: once when the entry
//! that stopped it applies, and again whenever the app asks (§17.10).
//! Nothing the device writes after that entry counts (§14.1), so every file a
//! listing finds is judged by the reference evidence alone, whenever the
//! listing is taken.
use crate::{history::History, sequence_sync::SequenceSync, SyncError};
use coven_format::MemberAccess;
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::Slot;
use coven_storage::{ObjectPrefix, StorageFailure};
use std::collections::BTreeSet;

pub(crate) struct UploaderScan {
    device: DeviceId,
    stop: Slot,
}

/// Which gone uploaders' folders to list.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Listing {
    /// Each folder not yet listed at the latest counted entry that stopped
    /// its device: what a pass lists, so it lists a folder once per entry.
    AtStop,
    /// Every folder again, for the app's cleanup: a gone device whose key
    /// still works may have uploaded since.
    Every,
}

impl History {
    /// Whether this device, `own` of `member`, deletes `uploader`'s stored
    /// files and snapshots: its own, and those of a device gone for good
    /// that is its member's. When the gone device's member has no device
    /// left that isn't gone, removed or not, the storage owner's devices
    /// delete them instead (§17.10).
    pub(crate) fn deletes_for(&self, own: DeviceId, member: MemberId, uploader: DeviceId) -> bool {
        if own == uploader {
            return true;
        }
        let Some(device) = self.state.devices.get(&uploader) else {
            return false;
        };
        if self.gone_for_good(uploader).is_none() {
            return false;
        }
        let member_left =
            self.state.devices.iter().any(|(id, value)| {
                value.member == device.member && self.gone_for_good(*id).is_none()
            });
        device.member == member
            || (!member_left
                && match self.state.members.get(&member).map(|value| &value.access) {
                    // Access-key IDs do not identify an S3 account. The creator is
                    // the only account owner the authenticated history establishes.
                    Some(MemberAccess::S3AccessKey { .. }) => {
                        self.recorded_access().any(|(owner, at, _)| owner == member && at == 1)
                    }
                    _ => self.owns_storage(member),
                })
    }

    /// The latest counted entry that stopped `device` for good (§14.1).
    fn latest_stop(&self, device: DeviceId) -> Option<Slot> {
        self.stops(device).next_back().map(|(slot, _)| slot)
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
            let Some(stop) = history.latest_stop(*device) else {
                continue;
            };
            if listing == Listing::AtStop && self.database.file_scan(*device).await? == Some(stop) {
                continue;
            }
            scans.push(UploaderScan { device: *device, stop });
        }
        Ok(scans)
    }

    pub(crate) async fn scan_uploaders(&self, scans: Vec<UploaderScan>) -> Result<(), SyncError> {
        for scan in scans {
            let mut files = BTreeSet::new();
            for object in
                self.storage.list(&ObjectPrefix::device_files(self.store(), scan.device)).await?
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
            if !self.database.complete_file_scan(scan.device, scan.stop, files).await? {
                return Err(StorageFailure::CatchUpRequired.into());
            }
        }
        Ok(())
    }
}
