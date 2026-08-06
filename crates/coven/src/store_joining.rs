use crate::store_membership::StoreMembership;
use crate::store_sync::{StoreSync, SyncError};
use coven_database::StoreDatabase;
use coven_protocol::membership::MemberRole;

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
    ) -> Result<crate::joining::DeviceJoinInvite, SyncError> {
        let member_pubkey = crate::joining::decode_join_request(join_request_code)
            .map_err(|error| SyncError::InvalidJoinRequest(error.to_string()))?
            .public_key;
        let invite_code = self.membership.invite(&member_pubkey, None, role).await?;
        let bundle = self.sync.begin_device_join_bundle(&member_pubkey).await?;
        Ok(crate::joining::DeviceJoinInvite::new(invite_code, bundle))
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

    #[cfg(test)]
    pub(crate) async fn prepare_test_join_snapshot(
        &self,
        store: &crate::sync::test_helpers::TestStore,
        owner: &coven_keys::keys::UserKeypair,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), String> {
        let owner_device = store.bind_store_device(&self.database, owner).await?;
        let snapshot = self
            .database
            .capture_snapshot_image_for_test(store.root.clone(), snapshot_path, None)
            .await
            .map_err(|error| error.to_string())?;
        let coverage =
            coven_protocol::store_commit::CommitFrontier(std::collections::BTreeMap::new());
        owner_device
            .publish_snapshot(snapshot, coverage.clone())
            .await?;
        owner_device.publish_acknowledgement(coverage).await?;
        Ok(())
    }
}
