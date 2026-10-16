//! The joining device's store-log and snapshot steps (§12).

use super::*;
use crate::join_admission::BindingSource;
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
            .publish(
                self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                &self.send,
            )
            .await
    }

    /// Apply the signed store log and open this member's sealed keys. Returns
    /// false until the binding `admission` requires is kept (§12). A refused
    /// entry stops admission and fails loudly with its pending record; a
    /// damaged key copy is passed over for a later copy, as any reader does
    /// (§11, §19.1). Snapshot work waits until the member has actually been
    /// admitted.
    pub async fn bootstrap_member(&mut self, admission: BindingSource) -> Result<bool, SyncError> {
        self.step().await?;
        let local = self.database.local_store_log().await?;
        let member = self.operation_member()?;
        if crate::join_admission::admitted(
            &local.log,
            &member.member(),
            &member.keys().public_keys(),
            admission,
        )? {
            return Ok(true);
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
        let admission = BindingSource::Join {
            approver: request.approver(),
        };
        Ok(if self.bootstrap_member(admission).await? {
            JoinOutcome::Admitted
        } else {
            JoinOutcome::Waiting
        })
    }

    /// Wait until the next approval poll is due: its persisted delay, armed
    /// again after reopening, and any provider cooldown (§16.5, SP waiting).
    pub async fn join_poll_due(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        loop {
            let persisted = self.database.retry_wait(subject).await?;
            if self.waits.due(subject, persisted.as_ref()) {
                return Ok(());
            }
            let wake = self
                .waits
                .next_wake(|candidate| *candidate == subject)
                .or_else(|| self.waits.cooldown_wake())
                .ok_or_else(|| {
                    coven_storage::StorageFailure::File
                        .with_source("the approval poll's delay was not persisted")
                })?;
            wake.await;
        }
    }

    /// A poll that found the request still waiting persists its next delay
    /// before waiting, continuing the backoff across reopening.
    pub async fn join_poll_waiting(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        let previous = self.database.retry_wait(subject).await?;
        self.waits.fail(subject, previous.as_ref()).await
    }

    /// The outcome is known: approval polling ends.
    pub async fn join_poll_settled(&self) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Join;
        if self.database.retry_wait(subject).await?.is_some() {
            self.database.clear_retry(subject).await?;
        }
        self.waits.forget(subject);
        Ok(())
    }

    /// Register this installation with its member's signature, then atomically
    /// load snapshots and later writes through the ordinary snapshot owner.
    pub async fn load_new_device(
        &mut self,
        name: &str,
        replaces: Option<coven_foundation::id_source::DeviceId>,
    ) -> Result<(), SyncError> {
        let member = self.operation_member()?;
        let local = self.database.local_store_log().await?;
        self.check_stopped(&local, &member)?;
        // Registration needs this pair's current kept binding, not merely
        // an active member id (§10, §12.2).
        if crate::effects::member(&local.log.replay.state, &member.member())
            .is_none_or(|held| held.keys != member.keys().public_keys())
        {
            return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed));
        }
        // A snapshot that fails its checks is recorded; a reload whose current
        // newest snapshot this device cannot load waits on it (§19.1), and
        // only a reload that cannot finish fails this call.
        self.resume_snapshots().await?;
        // A registration an interrupted attempt queued is this installation's
        // one registration (§10): publish it instead of making another. Once
        // a registration of this id is stored, replay alone decides it; a
        // dropped or late one refuses another below, and only a new device
        // with a fresh id registers again.
        let local = if local.upload.is_some() {
            {
                let _scope = self.reads.enter();
                self.reads.reset_discovery();
                self.settle_queued_entry().await?;
            }
            self.database.local_store_log().await?
        } else {
            local
        };
        match local.log.replay.state.devices.get(&local.device) {
            Some(device) if device.member != member.member() => {
                return Err(SyncError::Rejected(coven_database::DropReason::NotAllowed))
            }
            Some(_) => {}
            None => {
                self.make_and_upload_entry(StoreChange::AddDevice {
                    name: name.into(),
                    replaces,
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
