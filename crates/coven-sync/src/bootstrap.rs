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
        storage.join(invitation).await?;
        Ok(())
    }

    /// Publish the previously recorded immutable request after provider acceptance.
    pub async fn submit_join(&mut self, request: &crate::JoiningIdentity) -> Result<(), SyncError> {
        request
            .publish(self.storage.as_deref().ok_or(SyncError::NoStorage)?)
            .await
    }

    /// Apply the signed store log and open this member's sealed keys. Returns
    /// false until membership is effective; damaged required objects fail loudly.
    /// Snapshot work waits until the member has actually been admitted.
    pub async fn bootstrap_member(&mut self) -> Result<bool, SyncError> {
        let damages = self.step().await?;
        if let Some(damage) = damages.into_iter().next() {
            return Err(damage.into());
        }
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
        if let Some(record) = self
            .database
            .sync_state(Vec::new())
            .await?
            .stuck
            .into_iter()
            .find(|record| matches!(record.object, coven_database::LogObject::Entry(_)))
        {
            return Err(SyncError::StuckLog(record));
        }
        Ok(false)
    }

    /// Check membership before and after observing a deleted request: approval
    /// can publish between the first listing and that deletion. Sealed keys
    /// without a membership entry keep waiting; permission failures reach callers.
    pub async fn join_outcome(
        &mut self,
        request: &crate::JoiningIdentity,
    ) -> Result<JoinOutcome, SyncError> {
        if self.bootstrap_member().await? {
            return Ok(JoinOutcome::Admitted);
        }
        if request
            .pending(self.storage.as_deref().ok_or(SyncError::NoStorage)?)
            .await?
        {
            return Ok(JoinOutcome::Waiting);
        }
        if self.bootstrap_member().await? {
            return Ok(JoinOutcome::Admitted);
        }
        Ok(if self.store_keys.read()?.is_some() {
            JoinOutcome::Waiting
        } else {
            JoinOutcome::Declined
        })
    }

    /// Register this installation with its member's signature, then atomically
    /// load snapshots and later writes through the ordinary snapshot owner.
    pub async fn load_new_device(&mut self, name: &str) -> Result<(), SyncError> {
        let member = self.operation_member()?;
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &member)?;
        if crate::effects::member(&local.log.replay.state, &member.member_id()).is_none() {
            return Err(SyncError::NotStoreMember(member.member_id()));
        }
        let damages = self.resume_snapshots().await?;
        if let Some(damage) = damages.into_iter().next() {
            return Err(damage.into());
        }
        match local.log.replay.state.devices.get(&local.device) {
            Some(device) if device.member != member.member_id() => {
                return Err(SyncError::PermissionDenied)
            }
            Some(_) => {}
            None => {
                self.make_and_upload_entry(StoreChange::AddDevice {
                    device: local.device,
                    name: name.into(),
                })
                .await?;
            }
        }
        let damages = self.reload_from_snapshots().await?;
        if let Some(damage) = damages.into_iter().next() {
            return Err(damage.into());
        }
        Ok(())
    }
}
