use std::sync::Arc;

use crate::database::StoreDatabase;
use crate::protocol::membership::MemberRole;
use crate::store_membership::StoreMembership;
use crate::store_sync::{StoreSync, SyncError};
use crate::sync::Store;

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

    fn store(&self) -> Result<Arc<Store>, SyncError> {
        self.sync.active_store()
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
        let bundle = self
            .store()?
            .begin_device_join_bundle(&member_pubkey)
            .await?;
        Ok(crate::joining::DeviceJoinInvite::new(invite_code, bundle))
    }

    pub(crate) async fn drive(
        &self,
        invite: &crate::joining::DeviceJoinInvite,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, SyncError> {
        let store = self.store()?;
        Ok(store
            .device_join_transport()
            .drive(&invite.bundle, policy, access_administrator, timing)
            .await?)
    }

    pub(crate) async fn cancel_invite(
        &self,
        invite: &crate::joining::DeviceJoinInvite,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        let store = self.store()?;
        Ok(store
            .device_join_transport()
            .cancel(&invite.bundle, timing)
            .await?)
    }

    pub(crate) async fn abandon_invite(
        &self,
        invite: &crate::joining::DeviceJoinInvite,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        let store = self.store()?;
        Ok(store
            .device_join_transport()
            .abandon(&invite.bundle)
            .await?)
    }

    pub(crate) async fn begin(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, SyncError> {
        Ok(self.store()?.begin_device_join(member_pubkey).await?)
    }

    pub(crate) async fn abandon(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        Ok(self.store()?.abandon_device_join(offer).await?)
    }

    pub(crate) async fn authorize_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, SyncError> {
        Ok(self
            .store()?
            .authorize_device_provider_access(request, access_administrator)
            .await?)
    }

    pub(crate) async fn accept_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, SyncError> {
        Ok(self
            .store()?
            .accept_device_registration_request(request)
            .await?)
    }

    pub(crate) async fn publish_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, SyncError> {
        Ok(self
            .store()?
            .publish_device_provider_challenge(bootstrap)
            .await?)
    }

    pub(crate) async fn complete_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, SyncError> {
        Ok(self
            .store()?
            .complete_device_provider_admission(readiness)
            .await?)
    }

    pub(crate) async fn finalize(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, SyncError> {
        Ok(self.store()?.finalize_device_join(completion).await?)
    }

    pub(crate) async fn cancel(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, SyncError> {
        Ok(self.store()?.cancel_device_join(attempt).await?)
    }

    pub(crate) async fn close_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        Ok(self
            .store()?
            .close_device_provider_admission(cancellation)
            .await?)
    }

    pub(crate) async fn revoke_provider_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        Ok(self
            .store()?
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await?)
    }

    pub(crate) async fn revoke_joiner_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, SyncError> {
        Ok(self
            .store()?
            .revoke_joining_device_writes(cancellation, executor)
            .await?)
    }

    pub(crate) async fn activate_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        Ok(self.store()?.activate_device_join_cleanup(receipt).await?)
    }

    pub(crate) async fn complete_cancelled(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), SyncError> {
        self.store()?
            .complete_owner_device_join_cleanup(activation)
            .await?;
        Ok(())
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
        owner: &crate::keys::UserKeypair,
        snapshot_path: std::path::PathBuf,
    ) -> Result<(), String> {
        let owner_device = store.bind_store_device(&self.database, owner).await?;
        let snapshot = self
            .database
            .capture_snapshot_image_for_test(store.root.clone(), snapshot_path, None)
            .await
            .map_err(|error| error.to_string())?;
        let coverage =
            crate::protocol::store_commit::CommitFrontier(std::collections::BTreeMap::new());
        owner_device
            .publish_snapshot(snapshot, coverage.clone())
            .await?;
        owner_device.publish_acknowledgement(coverage).await?;
        Ok(())
    }
}
