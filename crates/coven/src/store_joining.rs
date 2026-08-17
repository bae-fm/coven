use crate::store_membership::StoreMembership;
use crate::store_sync::{StoreSync, SyncError};
use coven_database::StoreDatabase;
use coven_protocol::membership::MemberRole;

#[derive(Debug, thiserror::Error)]
pub enum BeginDeviceInviteError {
    #[error("join request: {0}")]
    JoinRequest(#[from] coven_domain::joining::JoinRequestError),
    #[error("sync: {0}")]
    Sync(#[from] SyncError),
    #[error("device invitation: {0}")]
    DeviceInvite(#[from] coven_domain::joining::DeviceInviteError),
}

/// The two parts of device joining that are not a plain sync command: minting
/// an invite, which pairs a membership invite with the attempt's transport
/// bundle, and reading the join journal the database holds. Every other step is
/// a command on [`StoreSync`], which its caller issues there.
#[derive(Clone)]
pub(crate) struct StoreJoining {
    database: StoreDatabase,
    membership: StoreMembership,
    sync: StoreSync,
}

impl StoreJoining {
    pub(crate) fn new(
        database: StoreDatabase,
        membership: StoreMembership,
        sync: StoreSync,
    ) -> Self {
        Self {
            database,
            membership,
            sync,
        }
    }

    pub(crate) async fn begin_invite(
        &self,
        join_request_code: &str,
        role: MemberRole,
    ) -> Result<coven_domain::joining::DeviceJoinInvite, BeginDeviceInviteError> {
        let request = coven_domain::joining::decode_join_request(join_request_code)?;
        let invitation = self
            .membership
            .admit(&request.public_key, request.email.as_deref(), role)
            .await?;
        let bundle = self
            .sync
            .begin_device_join_bundle(&request.public_key)
            .await?;
        Ok(coven_domain::joining::DeviceJoinInvite::new(
            invitation, bundle,
        )?)
    }

    pub(crate) async fn status(
        &self,
        attempt_id: crate::DeviceJoinAttemptId,
        role: crate::DeviceJoinRole,
    ) -> Result<Option<crate::DeviceJoinStatus>, SyncError> {
        Ok(self.database.device_join_status(attempt_id, role).await?)
    }

    pub(crate) async fn resumable_actions(
        &self,
    ) -> Result<Vec<crate::DeviceJoinAction>, SyncError> {
        Ok(self.database.device_join_actions().await?)
    }
}
