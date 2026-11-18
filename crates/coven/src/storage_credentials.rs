//! Restore-code access and S3 credential changes on an open handle.

use super::*;

impl CovenHandle {
    /// This member's current restore code, for their other devices (§13.1).
    /// It carries no S3 key.
    pub async fn restore_code(&self) -> Result<String, SyncError> {
        self.codes.restore_code().await
    }

    /// Check an S3 key the person made in the provider's console with
    /// storage, then, once storage accepts it, take it, whatever the
    /// reason, and record it as this member's (E9, §14.3). `cancel` stops
    /// the check; once the check passed, the call finishes. See the spec
    /// for the call's steps and what each failure leaves.
    pub async fn use_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<AccessKeyUsed, UseAccessKeyError> {
        let key = S3Credentials { access_key_id, secret_access_key };
        self.storage.use_access_key(key, cancel.clone()).await
    }
}
