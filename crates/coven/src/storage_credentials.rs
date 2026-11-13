//! Restore-code access and S3 credential changes on an open handle.

use super::*;

impl CovenHandle {
    /// This member's current restore code, for their other devices (§13.1).
    /// It carries no S3 key.
    pub async fn restore_code(&self) -> Result<String, SyncError> {
        self.codes.restore_code().await
    }

    /// Take an S3 key the person made in the provider's console, whatever the
    /// reason, and record it as this member's (E9, §14.3). See the spec for
    /// the call's steps and what each failure leaves.
    pub async fn use_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        self.storage.use_access_key(access_key_id, secret_access_key).await
    }
}
