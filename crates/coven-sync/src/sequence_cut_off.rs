//! The person's word that a lost owner device is cut off (§14.2, §20.9).

use super::{SequenceSync, Session};
use crate::{PendingRecord, SyncError};
use coven_foundation::id_source::DeviceId;
use coven_storage::StorageFailure;

impl SequenceSync {
    /// The person did the provider step for `device`, a lost device of the
    /// owner's (§14.2): nothing it sent can land any more. Every account its
    /// invite shares named that nothing intends now is taken back by a
    /// request of its own, which runs after this call and so after anything
    /// the device's grants did: its grant may have landed after this device's
    /// earlier revocation of the account. An account a member or an open
    /// invite uses stays shared.
    ///
    /// Refused with `CutOffRefused`, naming why: `NoAccountShared` on S3,
    /// `NotStoreOwner` unless this device's member holds the store's account,
    /// and `DeviceNotGone` unless the applied history has `device` gone for
    /// good, which the next pass may bring. Fails with `ProviderRequestRunning`, changing
    /// nothing, while a request to one of those accounts runs; the call is
    /// made again once it ends.
    pub async fn confirm_device_cut_off(&self, device: DeviceId) -> Result<(), SyncError> {
        let mut session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        self.retake_accounts(&session, member.member(), device).await
    }

    /// Whether this device's member reaches storage through the account that
    /// holds the store (§20.9): only their devices share it and take sharing
    /// back, so only they can confirm a device cut off. Reads the sequence
    /// forward first, so it answers for the history as it stands.
    pub async fn holds_store(&self) -> Result<bool, SyncError> {
        let mut session = self.session.lock().await;
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        Ok(session.history.holds_store(member.member()))
    }

    async fn retake_accounts(
        &self,
        session: &Session,
        member: coven_foundation::id_source::MemberId,
        device: DeviceId,
    ) -> Result<(), SyncError> {
        let changes = session.history.cut_off_retakes(member, device)?;
        Ok(self.database.retake_provider_access::<PendingRecord>(changes).await?)
    }
}

#[cfg(test)]
#[path = "sequence_cut_off_tests.rs"]
mod tests;
