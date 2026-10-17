//! Administrative and credential work the connected sequence owner does for
//! the open store (E6, E9-E12). Without a connection each call reports
//! `NoStorage`; credential reads and disconnected credential changes use the
//! store's own credential owner.

use super::StorageConnections;
use crate::*;
use coven_format::admin::StoreChange;
use std::sync::Arc;

impl StorageConnections {
    /// Validate, journal and claim one administrative change (§18).
    pub(crate) async fn administer(&self, change: StoreChange) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .administer(change)
            .await
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
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .create_circle(name)
            .await
    }

    /// Snapshot an audience and claim its reset (§19.3).
    pub(crate) async fn reset(&self, audience: Audience) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .reset(audience)
            .await
    }

    pub(crate) async fn reload_from_snapshot(&self) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .reload_from_snapshot()
            .await
    }

    pub(crate) async fn retry_pending_operation(
        &self,
        operation: OperationId,
    ) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .retry_pending_operation(operation)
            .await
            .map(drop)
    }

    pub(crate) async fn delete_access_key(
        &self,
        operation: OperationId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .delete_access_key(operation, admin.as_ref())
            .await
    }

    pub(crate) async fn replace_member_access_key(
        &self,
        member: MemberId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<AccessKeyReplacement, SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .replace_member_access_key(member, admin)
            .await
    }

    pub(crate) async fn retry_with_admin_access(
        &self,
        operation: OperationId,
        admin: Arc<dyn AccessKeyAdmin>,
    ) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .retry_with_admin_access(operation, admin)
            .await
    }

    pub(crate) async fn replace_member_keys_for(
        &self,
        member: MemberId,
        keys: MemberPublicKeys,
    ) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .replace_member_keys_for(member, keys)
            .await
    }

    pub(crate) async fn create_invite(
        &self,
        target: InviteTarget,
        access: InviteAccess,
    ) -> Result<Invite, SyncError> {
        self.prepare_invite().await?;
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .create_invite(target, access)
            .await
    }

    pub(crate) async fn approve_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .approve_join_request(request)
            .await
    }

    pub(crate) async fn decline_join_request(
        &self,
        request: &JoinRequest,
    ) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .decline_join_request(request)
            .await
    }

    pub(crate) async fn cancel_invite(&self, invite: InviteId) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .sync
            .cancel_invite(invite)
            .await
    }

    /// The restore code for the pair the applied history binds now (§12.1).
    /// It reads only committed state, so it never waits for the connection.
    pub(crate) async fn restore_code(&self) -> Result<String, SyncError> {
        self.codes.restore_code().await
    }

    /// Install and record a replacement S3 key; it needs the connection.
    pub(crate) async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .codes
            .replace_access_key(access_key_id, secret_access_key)
            .await
    }

    /// Keep a separately supplied S3 key, on the connection when there is one.
    pub(crate) async fn update_credentials(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<(), SyncError> {
        match self.live.lock().await.as_ref() {
            Some(live) => {
                live.codes
                    .update_credentials(access_key_id, secret_access_key)
                    .await
            }
            None => {
                self.codes
                    .update_credentials(access_key_id, secret_access_key)
                    .await?;
                // Kept credentials make the recorded location set up again.
                self.publish_released(Ok(()))
            }
        }
    }

    /// Replace this member's pair, returning the code for it (§11, E9).
    pub(crate) async fn replace_member_keys(&self) -> Result<SecretText, SyncError> {
        let live = self.live.lock().await;
        live.as_ref()
            .ok_or(SyncError::NoStorage)?
            .codes
            .replace_member_keys()
            .await
    }

    /// Whether custody holds storage credentials for the recorded location.
    pub(crate) async fn configured(&self) -> Result<bool, SyncError> {
        Ok(self.codes.connection().await?.is_some())
    }
}
