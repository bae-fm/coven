//! Restore-code access and S3 credential changes on an open handle.

use super::*;

impl CovenHandle {
    /// This member's current restore code, for their other devices (§13.1).
    /// It carries no S3 key.
    pub async fn restore_code(&self) -> Result<String, SyncError> {
        self.codes.restore_code().await
    }

    /// This member's current restore code for the app to show in a QR code
    /// to the person's own new device, behind the same tap (§13.1, E10). On
    /// S3 it carries the key this device uses now, so the new device asks for
    /// none; after a replacement it is the new key. Without a key, on other
    /// providers or when this device holds none, it is `restore_code`'s.
    pub async fn restore_code_for_transfer(&self) -> Result<String, SyncError> {
        self.codes.transfer_code().await
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
