//! S3 keys coven made that no entry records, found by listing the provider's
//! keys (§13, T237).
//!
//! Every key's provider-side name carries its store and minting device (§4).
//! Only that device's operation records a key it made, so once the device is
//! retired with a final entry and drained, its every entry is here and none
//! that records the key can still be kept (§6, §10). A key it made that no
//! stored entry records is then stray. A live device's key, recorded or still
//! in flight, is never taken (KeyListing `reclaimed_safe`,
//! `reclaimed_never_recorded`), and one catch-up leaves every retired device's
//! key recorded or pending deletion (`retired_keys_settle`), whether its
//! database reset, was lost or was never copied.

use super::StoreLogSync;
use crate::{effects::Authority, PendingRecord, SyncError};
use coven_database::{LogKind, StoreLog};
use coven_format::MemberAccess;
use coven_foundation::id_source::DeviceId;
use coven_storage::{AccessKeyAdmin, ListedKey};
use std::collections::BTreeSet;

impl StoreLogSync {
    /// List this store's keys with admin access and keep each stray one
    /// pending `DeleteAccessKey`, in one transaction. Keys already pending or
    /// confirmed deleted stay as they are. Deleting them is the ordinary
    /// verified deletion (§13).
    pub(super) async fn reclaim_access_keys(
        &self,
        admin: &dyn AccessKeyAdmin,
    ) -> Result<(), SyncError> {
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        self.check_stopped(&local, &member)?;
        Authority::Admin.check(&local.log.replay.state, &member.member())?;
        // Read before the listing: a drain, applied entries and received
        // input only grow, so a later listing judges a key by no newer facts.
        let drained = self.database.drained_devices().await?;
        let received: Vec<_> = self
            .database
            .log_inputs(LogKind::Entry)
            .await?
            .into_iter()
            .map(|input| (input.writer, input.number.get()))
            .collect();
        let listed = admin.list_keys(local.store).await?;
        let deletions = stray(&local.log, &received, &drained, listed)
            .into_iter()
            .map(|key| {
                let access = MemberAccess::S3AccessKey {
                    access_key_id: key.access_key_id,
                    principal: key.principal,
                };
                crate::access_keys::deletion(&access, None).expect("an S3 key")
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.database
            .record_access_key_deletions::<PendingRecord>(None, deletions)
            .await?;
        Ok(())
    }
}

/// The listed keys no stored entry records whose minting device is drained
/// and has every received entry applied. An entry still waiting could record
/// its key once applied, so that device's keys wait with it.
pub(crate) fn stray(
    log: &StoreLog,
    received: &[(DeviceId, u64)],
    drained: &BTreeSet<DeviceId>,
    listed: Vec<ListedKey>,
) -> Vec<ListedKey> {
    let applied: BTreeSet<(DeviceId, u64)> = log
        .entries
        .iter()
        .map(|entry| (entry.entry.position.device, entry.entry.position.number))
        .collect();
    let waiting: BTreeSet<DeviceId> = received
        .iter()
        .filter(|position| !applied.contains(position))
        .map(|(device, _)| *device)
        .collect();
    // Kept or dropped, an entry recording a key leaves it to §13's rules.
    let recorded: BTreeSet<&str> = log
        .entries
        .iter()
        .filter_map(|entry| match crate::access_keys::recorded(&entry.entry)? {
            (_, MemberAccess::S3AccessKey { access_key_id, .. }) => Some(access_key_id.as_str()),
            (_, MemberAccess::ProviderAccount(_)) => None,
        })
        .collect();
    listed
        .into_iter()
        .filter(|key| {
            drained.contains(&key.device)
                && !waiting.contains(&key.device)
                && !recorded.contains(key.access_key_id.as_str())
        })
        .collect()
}

#[cfg(test)]
#[path = "stray_access_keys_tests.rs"]
mod tests;
