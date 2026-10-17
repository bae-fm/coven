//! A removed uploader's complete folder listing after confirmed provider cut-off.
use crate::{history::History, sequence_sync::SequenceSync, SyncError};
use coven_format::{admin::StoreChange, MemberAccess};
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::Slot;
use coven_storage::{ObjectPrefix, StorageFailure};
use std::collections::BTreeSet;

pub(crate) struct UploaderScan {
    device: DeviceId,
    removal: Slot,
    accounts: BTreeSet<String>,
    keys: BTreeSet<String>,
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
}

impl SequenceSync {
    pub(crate) async fn uploader_scans(
        &self,
        history: &History,
        uploaders: &BTreeSet<DeviceId>,
    ) -> Result<Vec<UploaderScan>, SyncError> {
        let mut scans = Vec::new();
        for device in uploaders {
            let Some(state) = history
                .state
                .devices
                .get(device)
                .filter(|device| !device.active())
            else {
                continue;
            };
            if self.database.file_scan(*device).await?.is_some() {
                continue;
            }
            // A later member removal closes folders of devices removed while
            // their member still had provider access, including in-flight uploads.
            let removal = history
                .cutoffs
                .iter()
                .rev()
                .find_map(|(slot, change)| match change {
                    StoreChange::RemoveMember { member } if *member == state.member => {
                        Some((*slot, u64::MAX))
                    }
                    StoreChange::RemoveDevice { device: removed } if removed == device => {
                        Some((*slot, slot.0))
                    }
                    StoreChange::AddDevice {
                        replaces: Some(replaced),
                        ..
                    } if replaced == device => Some((*slot, slot.0)),
                    _ => None,
                });
            let Some((removal, through)) = removal else {
                continue;
            };
            let mut accounts = BTreeSet::new();
            let mut keys = BTreeSet::new();
            for (_, _, access) in history
                .recorded_access()
                .filter(|(member, at, _)| *member == state.member && *at <= through)
            {
                match access {
                    MemberAccess::ProviderAccount(account) => {
                        accounts.insert(account.trim().to_ascii_lowercase());
                    }
                    MemberAccess::S3AccessKey { access_key_id, .. } => {
                        keys.insert(access_key_id.clone());
                    }
                }
            }
            if self
                .database
                .access_taken_away(accounts.clone(), keys.clone())
                .await?
            {
                scans.push(UploaderScan {
                    device: *device,
                    removal,
                    accounts,
                    keys,
                });
            }
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
                .complete_file_scan(scan.device, scan.removal, files, scan.accounts, scan.keys)
                .await?
            {
                return Err(StorageFailure::CatchUpRequired.into());
            }
        }
        Ok(())
    }
}
