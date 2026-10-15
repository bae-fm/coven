//! The one-time listing of a removed uploader's file folder after its
//! cut-off (§13, §16.5).

use super::StoreLogSync;
use crate::SyncError;
use coven_database::StoreLog;
use coven_format::{store_log::StoreChange, value::EntryId, MemberAccess};
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_storage::{ObjectPrefix, StorageFailure};
use std::collections::BTreeSet;

impl StoreLogSync {
    /// List each removed uploader's file folder once, after its access was
    /// confirmed taken away, so files whose attaching rows never reached this
    /// device become candidates too (§16.5). Access taken away again after
    /// being restored is a new cut-off, which scans again. A failed listing
    /// records nothing and fails this retention visibly; the next one lists
    /// again.
    pub(super) async fn scan_removed_uploaders(
        &self,
        log: &StoreLog,
        uploaders: &BTreeSet<DeviceId>,
    ) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        for device in uploaders {
            let Some(state) = log.replay.state.devices.get(device) else {
                continue;
            };
            if !state.removed() {
                continue;
            }
            let Some((removal, held)) = retiring_removal(log, *device, &state.member) else {
                continue;
            };
            let (accounts, keys) = recorded_access(held, &state.member);
            let scanned = self.database.file_scan(*device).await?.is_some();
            if !self.database.access_taken_away(accounts, keys).await? {
                if scanned {
                    // Restored access ends the cut-off that scan followed.
                    self.database.forget_file_scan(*device).await?;
                }
                continue;
            }
            if scanned {
                continue;
            }
            let mut files = BTreeSet::new();
            for object in storage
                .list(&ObjectPrefix::device_files(self.directory.id(), *device))
                .await?
            {
                match (object.path.device(), object.path.file_id()) {
                    (Some(owner), Some(file)) if owner == *device => files.insert(file),
                    _ => {
                        return Err(StorageFailure::Protocol
                            .with_source("file listing names another folder")
                            .into())
                    }
                };
            }
            self.database
                .complete_file_scan(*device, removal, files)
                .await?;
        }
        Ok(())
    }
}

/// The first kept entry removing `device` or its member, with the entries
/// whose recorded access that removal must take away: a member removal
/// takes away everything ever recorded for the member, kept or dropped; a
/// device removal while the member stays takes away what the member held
/// before it, the access the removed device could have (§13).
fn retiring_removal<'a>(
    log: &'a StoreLog,
    device: DeviceId,
    member: &MemberId,
) -> Option<(EntryId, &'a [coven_database::ReplayEntry])> {
    log.entries.iter().enumerate().find_map(|(index, applied)| {
        let held = match &applied.entry.change {
            StoreChange::RemoveMember { member: removed } if removed == member => &log.entries[..],
            StoreChange::RemoveDevice { device: removed } if *removed == device => {
                &log.entries[..index]
            }
            _ => return None,
        };
        (log.replay.entries.get(&applied.entry.position)
            == Some(&coven_database::EntryOutcome::Kept))
        .then_some((applied.entry.position, held))
    })
}

/// Every provider account and S3 key these entries record for `member`: all
/// of it must be taken away (§13).
fn recorded_access(
    entries: &[coven_database::ReplayEntry],
    member: &MemberId,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut accounts = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for applied in entries {
        match crate::access_keys::recorded(&applied.entry) {
            Some((recorded, MemberAccess::ProviderAccount(account))) if recorded == member => {
                accounts.insert(account.trim().to_ascii_lowercase());
            }
            Some((recorded, MemberAccess::S3AccessKey { access_key_id, .. }))
                if recorded == member =>
            {
                keys.insert(access_key_id.clone());
            }
            _ => (),
        }
    }
    (accounts, keys)
}

#[cfg(test)]
#[path = "snapshot_file_scan_tests.rs"]
mod tests;
