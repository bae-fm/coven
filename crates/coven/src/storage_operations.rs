//! Administrative and credential work for the open store (E6, E9-E12).
//! These calls need the connected sequence owner and without a connection
//! report `NoStorage`; credential changes run on the store's credential
//! owner, given the connection's sequence owner when there is one.
//! Administrative changes journaled by an app's write are `OperationSteps`.

use super::{ConnectionState, StorageConnections};
use crate::*;
use coven_format::admin::StoreChange;
use std::sync::Arc;

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
            // A device never admitted has no registration to remove (§13.2).
            Err(SyncError::Removed | SyncError::NotAdmitted) => Ok(()),
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

    /// The person did the provider step for a lost owner device (§14.2).
    pub(crate) async fn confirm_device_cut_off(&self, device: DeviceId) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.confirm_device_cut_off(device).await?;
        // The requests it queued run in a pass.
        self.sync_now();
        Ok(())
    }

    /// Whether this device's member holds the store's account (§20.9).
    pub(crate) async fn holds_store(&self) -> Result<bool, SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.holds_store().await
    }

    /// Use an S3 key the person made in the provider's console, for any
    /// reason, and record it as this member's (§14.3, E9): the entry setting
    /// this member's access record to it is journaled, custody and the
    /// connection take the key, and the entry is claimed with it; a record
    /// that already names the key journals nothing. Without a connection, the
    /// call connects with the new key first, committing nothing. The whole
    /// call runs as one task, so a dropped app future can't stop it between
    /// the entry and the key. A failure before the key is taken ends the entry
    /// and changes nothing.
    pub(crate) async fn use_access_key(
        self: &Arc<Self>,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        self.call(move |owner| async move {
            let mut live = owner.live.lock().await;
            owner.ready(&mut live).await?;
            let key = coven_storage::S3Credentials {
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
            };
            let fresh = !matches!(&*live, ConnectionState::Active(_));
            if fresh {
                let next = owner.codes.s3_connection(key).await?;
                owner.connect_with(&mut live, next).await?;
            }
            let ConnectionState::Active(connection) = &*live else {
                return Err(SyncError::NoStorage);
            };
            let sync = connection.sync.clone();
            let used =
                owner.codes.use_access_key(sync, access_key_id.clone(), secret_access_key, fresh);
            let used = used.await;
            let held = owner.codes.connection().await;
            let taken = matches!(&held, Ok(Some(coven_storage::ConnectionCredentials {
                credentials: coven_storage::StorageCredentials::S3(key), ..
            })) if key.access_key_id == access_key_id);
            if taken {
                // Custody holds credentials now: a waiting connector uses
                // them at its next attempt.
                owner.waits.connection_changed();
                owner.record_custody().await?;
            } else if fresh {
                // The connection signs with a key custody doesn't hold.
                let released = owner.release(&mut live).await;
                return match (used, released) {
                    (Err(error), Err(cleanup)) => Err(SyncError::cleanup(error, cleanup)),
                    (Err(error), Ok(())) => Err(error),
                    (Ok(()), Err(cleanup)) => Err(cleanup),
                    (Ok(()), Ok(())) => Ok(()),
                };
            }
            used
        })
        .await
    }

    /// Replace this member's pair, returning the code for it (§12.3, E9).
    pub(crate) async fn replace_member_keys(&self) -> Result<SecretText, SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let ConnectionState::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        self.codes.replace_member_keys(live.sync.clone()).await
    }
}

#[cfg(test)]
#[path = "storage_operations_tests.rs"]
mod tests;
