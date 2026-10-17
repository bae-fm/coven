//! Restore-code access and S3 credential changes on an open handle.

use super::*;

impl CovenHandle {
    /// This member's current restore code, for their other devices (§13.1).
    pub async fn restore_code(&self) -> Result<String, SyncError> {
        self.storage.restore_code().await
    }

    /// Commit a replacement S3 key and publish its id; the restore code is unchanged (E9).
    /// A publication failure retains the credentials; retry with the same key.
    /// Removal lists this key for deletion even if its access entry is refused.
    pub async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        self.storage
            .replace_access_key(access_key_id, secret_access_key)
            .await
    }

    /// Install this member's separately supplied S3 key (E9). Identity, location
    /// and restore code stay unchanged; disconnected handles keep it for connecting.
    pub async fn update_credentials(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        self.storage
            .update_credentials(access_key_id, secret_access_key)
            .await
    }
}
