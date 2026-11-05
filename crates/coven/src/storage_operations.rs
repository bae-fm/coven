//! Administrative and credential work for the open store (E6, E9-E12).
//! These calls need the connected sequence owner and without a connection
//! report `NoStorage`; credential reads and disconnected credential changes
//! use the store's own credential owner. Administrative changes journaled by
//! an app's write are `OperationSteps`.

use super::{ConnectionState, StorageConnections};
use crate::*;
use coven_format::admin::StoreChange;

impl StorageConnections {
    /// Publish this device's removal, for deleting the store from it, and
    /// return once its slot applies (§14.1). Applying it stops this device:
    /// that is the removal taking effect, not a failure.
    pub(crate) async fn retire(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        match live.sync.administer(StoreChange::RemoveDevice { device: self.device }).await {
            Err(SyncError::Removed) => Ok(()),
            result => result,
        }
    }

    /// Snapshot an audience and claim its reset (§19.4).
    pub(crate) async fn reset(&self, audience: Audience) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.reset(audience).await
    }

    pub(crate) async fn reload_from_snapshot(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.reload_from_snapshot().await
    }

    pub(crate) async fn clean_up_gone_devices_files(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.clean_up_gone_devices_files().await
    }

    /// The restore code for the pair the applied history binds now (§13.1).
    /// It reads only committed state, so it never waits for the connection.
    pub(crate) async fn restore_code(&self) -> Result<String, SyncError> {
        self.codes.restore_code().await
    }

    /// The restore code with the S3 key this device's custody holds now
    /// (§13.1). Like `restore_code`, it never waits for the connection.
    pub(crate) async fn restore_code_for_transfer(&self) -> Result<String, SyncError> {
        self.codes.transfer_code().await
    }

    /// Install and record a replacement S3 key; it needs the connection.
    pub(crate) async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.codes.replace_access_key(access_key_id, secret_access_key).await
    }

    /// Keep a separately supplied S3 key, on the connection when there is one.
    pub(crate) async fn update_credentials(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        match &*live {
            ConnectionState::Active(live) => {
                live.codes.update_credentials(access_key_id, secret_access_key).await
            }
            _ => {
                self.codes.update_credentials(access_key_id, secret_access_key).await?;
                self.waits.connection_changed();
                // Custody holds credentials now: a waiting connector uses
                // them at its next attempt.
                self.record_custody().await
            }
        }
    }

    /// Replace this member's pair, returning the code for it (§12.3, E9).
    pub(crate) async fn replace_member_keys(&self) -> Result<SecretText, SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.codes.replace_member_keys().await
    }

    /// Whether custody holds storage credentials for the recorded location.
    pub(crate) async fn configured(&self) -> Result<bool, SyncError> {
        Ok(self.codes.connection().await?.is_some())
    }
}

#[cfg(test)]
#[path = "storage_operations_tests.rs"]
mod tests;
