//! Administrative and credential work the connected sequence owner does for
//! the open store (E6, E9-E12). Without a connection each call reports
//! `NoStorage`; credential reads and disconnected credential changes use the
//! store's own credential owner.

use super::{Connection, StorageConnections};
use crate::*;
use coven_format::admin::StoreChange;

impl StorageConnections {
    /// Validate, journal and claim one administrative change (§11).
    pub(crate) async fn administer(&self, change: StoreChange) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.administer(change).await
    }

    /// Remove a member. A member removing themselves leaves the store: this
    /// device applies the removal, deleting the person's synced entries, and
    /// stops (§14.1). That stop is the removal taking effect, not a failure.
    pub(crate) async fn remove_member(&self, member: MemberId) -> Result<(), SyncError> {
        match self.administer(StoreChange::RemoveMember { member }).await {
            Err(SyncError::Removed)
                if self
                    .database
                    .store_state()
                    .await?
                    .members
                    .get(&member)
                    .is_some_and(|removed| removed.removed)
                    && self
                        .identity
                        .read()?
                        .is_some_and(|identity| identity.member() == member) =>
            {
                Ok(())
            }
            result => result,
        }
    }

    /// Remove a device, then say how its member signs out of the provider.
    pub(crate) async fn remove_device(
        &self,
        device: DeviceId,
    ) -> Result<ProviderSignOut, SyncError> {
        match self.administer(StoreChange::RemoveDevice { device }).await {
            // Applying this device's own removal stops it: the removal took effect.
            Err(SyncError::Removed) if device == self.device => {}
            result => result?,
        }
        Ok(self
            .codes
            .location()
            .await?
            .ok_or(SyncError::NoStorage)?
            .provider()
            .sign_out())
    }

    pub(crate) async fn create_circle(&self, name: String) -> Result<CircleId, SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.create_circle(name).await
    }

    /// Snapshot an audience and claim its reset (§19.4).
    pub(crate) async fn reset(&self, audience: Audience) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.reset(audience).await
    }

    pub(crate) async fn reload_from_snapshot(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.reload_from_snapshot().await
    }

    pub(crate) async fn retry_pending_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.retry_pending_operation(operation).await.map(drop)
    }

    pub(crate) async fn replace_member_keys_for(
        &self,
        member: MemberId,
        keys: MemberPublicKeys,
    ) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.replace_member_keys_for(member, keys).await
    }

    pub(crate) async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
    ) -> Result<Invite, SyncError> {
        self.prepare_invite().await?;
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.create_invite(target, access).await
    }

    pub(crate) async fn approve_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.approve_join_request(request).await
    }

    pub(crate) async fn decline_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.decline_join_request(request).await
    }

    pub(crate) async fn cancel_invite(&self, invite: InviteId) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.cancel_invite(invite).await
    }

    /// The restore code for the pair the applied history binds now (§13.1).
    /// It reads only committed state, so it never waits for the connection.
    pub(crate) async fn restore_code(&self) -> Result<String, SyncError> {
        self.codes.restore_code().await
    }

    /// The S3 key this device's custody holds now, read where the connection
    /// reads it, so a replaced key is the current one (§13.1). It never waits
    /// for the connection; any other provider, or no key, gives None.
    pub(crate) async fn s3_credentials_for_transfer(
        &self,
    ) -> Result<Option<S3Credentials>, SyncError> {
        Ok(match self.codes.connection().await? {
            Some(coven_storage::ConnectionCredentials {
                credentials: coven_storage::StorageCredentials::S3(keys),
                ..
            }) => Some(keys),
            _ => None,
        })
    }

    /// Install and record a replacement S3 key; it needs the connection.
    pub(crate) async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.codes
            .replace_access_key(access_key_id, secret_access_key)
            .await
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
            Connection::Active(live) => {
                live.codes
                    .update_credentials(access_key_id, secret_access_key)
                    .await
            }
            _ => {
                self.codes
                    .update_credentials(access_key_id, secret_access_key)
                    .await?;
                self.waits.connection_changed();
                // A waiting connector publishes its next attempt; an idle
                // installation only updates whether storage is configured.
                self.configured
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                if !self
                    .attempts
                    .lock()
                    .expect("connection attempts poisoned")
                    .as_ref()
                    .is_some_and(|attempt| {
                        attempt.epoch == self.epoch.load(std::sync::atomic::Ordering::SeqCst)
                            && !attempt.task.is_finished()
                    })
                {
                    self.publications.publish(SyncStatus::Stopped);
                }
                Ok(())
            }
        }
    }

    /// Replace this member's pair, returning the code for it (§12.3, E9).
    pub(crate) async fn replace_member_keys(&self) -> Result<SecretText, SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
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
