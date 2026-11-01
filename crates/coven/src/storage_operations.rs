//! Administrative and credential work for the open store (E6, E9-E12).
//! Administrative changes are steps of an app's write, journaled with or
//! without a connection, which a pass runs. The other calls need the
//! connected sequence owner and without a connection report `NoStorage`;
//! credential reads and disconnected credential changes use the store's own
//! credential owner.

use super::{Connection, StorageConnections};
use crate::*;
use coven_format::admin::StoreChange;

impl StorageConnections {
    /// Journal one administrative change as a step of an app's write: once
    /// the write commits, a pass is asked for, which runs it (§11, E6).
    pub(crate) fn administer(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        change: StoreChange,
    ) -> Result<Option<OperationId>, SyncError> {
        coven_sync::administer(
            sql,
            self.device,
            &self.held()?,
            self.publications.state.clone(),
            change,
        )
    }

    /// The member pair this installation holds, which an admin step checks
    /// the applied binding against.
    fn held(&self) -> Result<MemberIdentity, SyncError> {
        Ok(self.identity.read()?.ok_or(StorageFailure::MemberKeysMissing)?)
    }

    /// Journal a device's removal as a step of an app's write, and say how
    /// its member signs out of the provider, if anything is left to do
    /// there, from the recorded location (§14.2).
    pub(crate) fn remove_device(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        device: DeviceId,
    ) -> Result<(Option<OperationId>, Option<ProviderSignOut>), SyncError> {
        let location = self.settings.read()?.ok_or(SyncError::NoStorage)?;
        let operation = self.administer(sql, StoreChange::RemoveDevice { device })?;
        Ok((operation, location.provider().sign_out()))
    }

    /// Journal the replacement of another member's public pair as a step of
    /// an app's write (§12.3, E9).
    pub(crate) fn replace_member_keys_for(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        member: MemberId,
        keys: MemberPublicKeys,
    ) -> Result<Option<OperationId>, SyncError> {
        coven_sync::replace_member_keys_for(
            sql,
            self.device,
            &self.held()?,
            self.publications.state.clone(),
            member,
            keys,
        )
    }

    /// Publish this device's removal, for deleting the store from it, and
    /// return once its slot applies (§14.1). Applying it stops this device:
    /// that is the removal taking effect, not a failure.
    pub(crate) async fn retire(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        match live.sync.administer(StoreChange::RemoveDevice { device: self.device }).await {
            Err(SyncError::Removed) => Ok(()),
            result => result,
        }
    }

    /// Journal making a circle as a step of an app's write (§15.5, E12).
    pub(crate) fn create_circle(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        name: String,
    ) -> Result<(CircleId, OperationId), SyncError> {
        coven_sync::create_circle(
            sql,
            self.device,
            &self.held()?,
            self.ids.as_ref(),
            self.publications.state.clone(),
            name,
        )
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

    pub(crate) async fn clean_up_removed_files(&self) -> Result<(), SyncError> {
        let mut live = self.live.lock().await;
        self.ready(&mut live).await?;
        let Connection::Active(live) = &*live else {
            return Err(SyncError::NoStorage);
        };
        live.sync.clean_up_removed_files().await
    }

    /// Make an invite as a step of an app's write, from the recorded
    /// location: once the write commits, a pass is asked for, which checks
    /// it again, grants its access and fixes its code (§13.2, E10). A store
    /// with no recorded location fails with `NoStorage`.
    pub(crate) fn create_invite(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        invite: coven_sync::NewInvite,
    ) -> Result<(InviteId, MemberId, OperationId), SyncError> {
        let location = self.settings.read()?.ok_or(SyncError::NoStorage)?;
        coven_sync::create_invite(
            sql,
            self.device,
            &self.held()?,
            self.ids.as_ref(),
            self.clock.now(),
            &location,
            self.publications.state.clone(),
            invite,
        )
    }

    /// Approve a join request as a step of an app's write, returning the
    /// operation that binds its pair: once the write commits, a pass is
    /// asked for, which runs it (§13.2, E10).
    pub(crate) fn approve_join_request(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        request: &JoinRequest,
    ) -> Result<OperationId, SyncError> {
        coven_sync::approve_join_request(
            sql,
            self.device,
            &self.held()?,
            self.clock.now(),
            self.waits.clone(),
            self.publications.state.clone(),
            request,
        )
    }

    /// Decline a join request as a step of an app's write: once the write
    /// commits, a pass is asked for, which deletes the request and takes
    /// back the invite's access (§13.2, E10).
    pub(crate) fn decline_join_request(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        request: &JoinRequest,
    ) -> Result<(), SyncError> {
        coven_sync::decline_join_request(
            sql,
            self.clock.now(),
            self.waits.clone(),
            self.publications.state.clone(),
            request,
        )
    }

    /// Cancel an invite as a step of an app's write: once the write
    /// commits, a pass is asked for, which deletes a waiting request and
    /// takes back the invite's access (§13.2, E10).
    pub(crate) fn cancel_invite(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        invite: InviteId,
    ) -> Result<(), SyncError> {
        coven_sync::cancel_invite(
            sql,
            self.clock.now(),
            self.waits.clone(),
            self.publications.state.clone(),
            invite,
        )
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
        let Connection::Active(live) = &*live else {
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
            Connection::Active(live) => {
                live.codes.update_credentials(access_key_id, secret_access_key).await
            }
            _ => {
                self.codes.update_credentials(access_key_id, secret_access_key).await?;
                self.waits.connection_changed();
                // A waiting connector publishes its next attempt; an idle
                // installation only updates whether storage is configured.
                self.configured.store(true, std::sync::atomic::Ordering::SeqCst);
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
                    self.show(SyncStatus::Stopped).await?;
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
