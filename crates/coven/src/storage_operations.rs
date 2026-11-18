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
            Err(SyncError::Removed(_)) => Ok(()),
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
    /// reason, and record it as this member's (§14.3, E9). The key's check
    /// comes first, beside the store's connection, which keeps syncing; a
    /// cancel stops it between its steps, and the call fails `Cancelled`.
    /// Once the check passes the call runs to its end: the entry setting
    /// this member's access record to the key is journaled, custody and the
    /// store's connection take the key, and the entry is claimed through
    /// the checked connection; a record that already names the key journals
    /// nothing. Without a connection, the checked one becomes the store's,
    /// and goes again if the key isn't taken. The whole call runs as one
    /// task, so a dropped app future can't stop it between the entry and
    /// the key. Once the key is taken, a storage failure of the claim leaves
    /// the entry for passes and the call succeeds; any other failure fails
    /// it (§14.3).
    pub(crate) async fn use_access_key(
        self: &Arc<Self>,
        key: coven_storage::S3Credentials,
        cancel: tokio::sync::watch::Receiver<bool>,
    ) -> Result<AccessKeyUsed, UseAccessKeyError> {
        self.call(move |owner| async move {
            let next = owner.codes.s3_connection(key.clone()).await?;
            let checked = owner
                .connector
                .connect(
                    next.location.clone(),
                    owner.codes.provider_auth(next.credentials.clone()),
                    owner.device,
                    owner.state.clone(),
                    owner.waits.clone(),
                    owner.transfers.clone(),
                )
                .await
                .map_err(|error| UseAccessKeyError::Check(error.into()))?;
            let path = ObjectPath::file(owner.store(), owner.device, ObjectId(owner.ids.new_id()));
            let checking = checked.during_setup(coven_storage::SetupWaits::default());
            check_access_key(&checking, owner.store(), &owner.state, path, &cancel).await?;
            // The check's minute of rate-limit waits ends with it.
            drop(checking);
            let mut live = owner.live.lock().await;
            owner.ready(&mut live).await?;
            let fresh = !matches!(&*live, ConnectionState::Active(_));
            if fresh {
                let store = owner.store();
                super::record_reached(
                    &owner.codes,
                    &owner.database,
                    store,
                    &next.location,
                    &checked,
                )
                .await?;
                owner.attach(&mut live, checked.clone()).await?;
            }
            let ConnectionState::Active(connection) = &*live else {
                return Err(SyncError::NoStorage.into());
            };
            // The entry runs through the checked connection; the store's
            // takes the key with custody.
            let (sync, store) = match fresh {
                true => (connection.sync.clone(), None),
                false => {
                    (Arc::new(connection.sync.reconnect(checked)), Some(connection.sync.clone()))
                }
            };
            let used = owner.codes.use_access_key(sync, store, key.clone()).await;
            let held = owner.codes.connection().await;
            let taken = matches!(&held, Ok(Some(coven_storage::ConnectionCredentials {
                credentials: coven_storage::StorageCredentials::S3(held), ..
            })) if held.access_key_id == key.access_key_id);
            if taken {
                // Custody holds credentials now: a waiting connector uses
                // them at its next attempt.
                owner.waits.connection_changed();
                owner.record_custody().await?;
            } else if fresh {
                // The connection signs with a key custody doesn't hold.
                let released = owner.release(&mut live).await;
                return match (used, released) {
                    (Err(error), Err(cleanup)) => Err(SyncError::cleanup(error, cleanup).into()),
                    (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error.into()),
                    (Ok(replaced), Ok(())) => Ok(AccessKeyUsed { replaced }),
                };
            }
            Ok(AccessKeyUsed { replaced: used? })
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

/// Check a new S3 key with storage before anything uses it (§14.3, §20.8):
/// over `checking`, a connection signed with the key that spends one minute
/// on rate limits as a setup call's own requests do, read `store`'s location
/// claim and run the provider check with a test object at `path`. Detection
/// doesn't run: the location's settings are recorded. `cancel` stops the
/// check between its steps.
async fn check_access_key(
    checking: &coven_storage::StorageConnection,
    store: StoreId,
    state: &coven_sync::SequenceState,
    path: ObjectPath,
    cancel: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), UseAccessKeyError> {
    let cancelled = || *cancel.borrow();
    if cancelled() {
        return Err(UseAccessKeyError::Cancelled);
    }
    checking
        .check_location_claim(store)
        .await
        .map_err(|error| UseAccessKeyError::Check(error.into()))?;
    match super::setup::check_provider(checking, state, path, cancelled)
        .await?
        .map_err(UseAccessKeyError::Check)?
    {
        coven_storage::CheckEnd::Passed => Ok(()),
        coven_storage::CheckEnd::Stopped => Err(UseAccessKeyError::Cancelled),
    }
}

#[cfg(test)]
#[path = "storage_operations_tests.rs"]
mod tests;
