//! Replacing a member's signing and sealing pair on an open handle (E9).

use super::*;

impl CovenHandle {
    /// Replace this member's signing and sealing pair (§10, §12.3). App writes
    /// return `DbError::MemberKeysChanging` until it finishes; this
    /// installation's queued writes and entries are confirmed first. A fresh
    /// pair is kept in identity custody before its key copies and the entry
    /// signed by the current pair are published. Returns once its slot binds
    /// the pair and custody hold the new pair, returning its restore code.
    /// A failure keeps both pairs and the attempt for retry;
    /// a rejection is returned and never
    /// overridden. Other installations holding the old pair report
    /// `MemberKeysReplaced` and need the current keys.
    pub async fn replace_member_keys(&self) -> Result<SecretText, SyncError> {
        self.storage.replace_member_keys().await
    }
    /// Publishes a prepared public pair for another active member, in a
    /// write of its own (`CovenWrite::replace_member_keys_for`).
    pub async fn replace_member_keys_for(
        &self,
        member: &MemberId,
        keys: MemberPublicKeys,
    ) -> Result<Option<OperationId>, SyncError> {
        let member = *member;
        self.write(move |write| Ok(write.replace_member_keys_for(&member, keys)?))
            .await
            .map_err(SyncError::from)
    }
}
