//! The joining device's store-log and snapshot steps (§12).

use super::*;
use coven_database::EntryOutcome;
use coven_storage::StorageInvitation;

/// The joining member's outcome after checking storage (§12.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinOutcome {
    /// The request exists, or sealed keys arrived before the membership entry.
    Waiting,
    /// This member is effective in the checked store log.
    Admitted,
    /// The request is absent and no sealed store key has arrived.
    Declined,
}

impl StoreLogSync {
    /// Complete provider acceptance before submitting any join request. Repeated
    /// acceptance and publication use the same invitation and ciphertext.
    pub async fn accept_join(&mut self, invitation: &StorageInvitation) -> Result<(), SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        storage.join(self.directory.id(), invitation).await?;
        Ok(())
    }

    /// Publish the previously recorded immutable request after provider acceptance.
    pub async fn submit_join(&mut self, request: &crate::JoiningIdentity) -> Result<(), SyncError> {
        request
            .publish(self.storage.as_deref().ok_or(SyncError::NoStorage)?)
            .await
    }

    /// Apply the signed store log and open this member's sealed keys. Returns
    /// false until membership is effective. A refused entry stops admission
    /// and fails loudly with its pending record; a damaged key copy is passed
    /// over for a later copy, as any reader does (§11, §19.1). Snapshot work
    /// waits until the member has actually been admitted.
    pub async fn bootstrap_member(&mut self) -> Result<bool, SyncError> {
        self.step().await?;
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        if crate::effects::member(&local.log.replay.state, &member.member_id()).is_some() {
            return Ok(true);
        }
        for entry in &local.log.entries {
            if matches!(&entry.entry.change, StoreChange::AddMember { keys: added, .. } if added.signing == member.member_id())
            {
                if let EntryOutcome::Dropped(reason) =
                    &local.log.replay.entries[&entry.entry.position]
                {
                    return Err(SyncError::Rejected(reason.clone()));
                }
            }
        }
        if let Some(record) = crate::pending_logs::local_refusals(&self.database)
            .await?
            .into_iter()
            .find(|record| matches!(record.object, coven_database::LogObject::Entry(_)))
        {
            return Err(crate::pending_logs::required(&self.database, record).await?);
        }
        Ok(false)
    }

    /// Poll the invitation's key-copy tail before reading membership history.
    /// A deleted request triggers a final tail read, since admission may race status.
    pub async fn join_outcome(
        &mut self,
        request: &crate::JoiningIdentity,
    ) -> Result<JoinOutcome, SyncError> {
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        if self.store_keys.read()?.is_none() {
            let key = match request
                .initial_key(&self.database, self.download_limit, storage)
                .await?
            {
                Some(key) => Some(key),
                None if request.pending(storage).await? => return Ok(JoinOutcome::Waiting),
                // Deletion may follow admission between our key scan and status.
                None => {
                    request
                        .initial_key(&self.database, self.download_limit, storage)
                        .await?
                }
            };
            let Some(key) = key else {
                return Ok(JoinOutcome::Declined);
            };
            self.store_keys
                .persist(&coven_crypto::StoreKeyring::new(key))?;
        }
        Ok(if self.bootstrap_member().await? {
            JoinOutcome::Admitted
        } else {
            JoinOutcome::Waiting
        })
    }

    /// Register this installation with its member's signature, then atomically
    /// load snapshots and later writes through the ordinary snapshot owner.
    pub async fn load_new_device(&mut self, name: &str) -> Result<(), SyncError> {
        let member = self.operation_member()?;
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &member)?;
        if crate::effects::member(&local.log.replay.state, &member.member_id()).is_none() {
            return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
        }
        // Snapshots that fail their checks are passed over for the next latest
        // or the logs (§19.1); only a reload that cannot finish fails this call.
        self.resume_snapshots().await?;
        match local.log.replay.state.devices.get(&local.device) {
            Some(device) if device.member != member.member_id() => {
                return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
            }
            Some(_) => {}
            None => {
                self.make_and_upload_entry(StoreChange::AddDevice {
                    device: local.device,
                    name: name.into(),
                    replaces: None,
                })
                .await?;
            }
        }
        self.reload_from_snapshots().await?;
        // A reload still waiting, e.g. for history every usable snapshot
        // lacks, leaves this device unloaded: report its first unmet condition.
        if let Some(id) = self.pending_reload().await? {
            let pending = self
                .database
                .operation_pending::<crate::PendingRecord>(id)
                .await?
                .ok_or(SyncError::ReloadPending(id))?;
            return Err(SyncError::Pending(Box::new(pending)));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
