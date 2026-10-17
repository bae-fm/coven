//! Replacing a member's signing and sealing pair on an open handle (E9).

use super::*;

impl CovenHandle {
    /// Replace this member's signing and sealing pair (§9, §11). App writes
    /// return `DbError::MemberKeysChanging` until it finishes; this
    /// installation's queued writes and entries are confirmed first. A fresh
    /// pair is kept in identity custody before its key copies and the entry
    /// signed by the current pair are published. Returns once replay keeps
    /// the binding and custody hold the new pair, returning its restore code.
    /// A failure keeps both pairs and the attempt for retry;
    /// a replay rejection is returned and never
    /// overridden. Other installations holding the old pair report
    /// `MemberKeysReplaced` and need the current keys.
    pub async fn replace_member_keys(&self) -> Result<SecretText, SyncError> {
        self.codes.replace_member_keys().await
    }
    /// Publish a prepared public pair for another active member, as any
    /// admin under §9's authority rules, independently of provider ownership.
    /// The member keeps the private pair; this call changes no private
    /// custody and no provider access. Confirms the audience-key copies to
    /// the new credential first and returns once replay keeps the entry. A
    /// rejection is returned without publishing again over the winner.
    /// Replacing this installation's own pair uses `replace_member_keys`.
    pub async fn replace_member_keys_for(
        &self,
        member: &MemberId,
        keys: MemberPublicKeys,
    ) -> Result<(), SyncError> {
        self.operations.replace_member_keys_for(member, keys).await
    }
}
