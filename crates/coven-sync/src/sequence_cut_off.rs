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
    /// Refused with `Rejected(NotAllowed)` unless this device is the owner's
    /// and the applied history has `device` gone for good, and on S3, where
    /// no account is shared. Fails with `ProviderRequestRunning`, changing
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
