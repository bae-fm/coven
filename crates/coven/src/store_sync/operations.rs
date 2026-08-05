use super::*;

pub(super) struct ConnectedSyncOperation {
    pub(super) loop_handle: Arc<SyncLoopHandle>,
}

impl ConnectedSyncOperation {
    pub(super) fn trigger(&self) {
        self.loop_handle.trigger();
    }

    pub(super) fn is_running(&self) -> bool {
        self.loop_handle.is_running()
    }

    pub(super) fn config(&self) -> Config {
        self.loop_handle.config().clone()
    }

    pub(super) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.loop_handle.blob_path_scheme()
    }

    pub(super) fn uploader(&self) -> String {
        self.loop_handle.self_uploader()
    }

    pub(super) async fn members(
        &self,
    ) -> Result<Vec<crate::protocol::membership::MemberInfo>, crate::sync::store::MembershipOpsError>
    {
        self.loop_handle.members().await
    }

    pub(super) async fn membership_conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, crate::sync::store::MembershipOpsError> {
        self.loop_handle.membership_conflict().await
    }

    pub(super) async fn restore_membership(
        &self,
    ) -> Result<
        crate::sync::store::owner::StoreRestoreMembership,
        crate::sync::store::MembershipOpsError,
    > {
        self.loop_handle.restore_membership().await
    }

    pub(super) fn host_write_blob_staging(&self) -> crate::sync::store::HostWriteBlobStaging {
        self.loop_handle
            .host_write_blob_staging(tokio::runtime::Handle::current())
    }

    #[cfg(test)]
    pub(super) fn uses_store_dir(&self, store_dir: &crate::store_dir::StoreDir) -> bool {
        self.loop_handle.uses_store_dir_for_test(store_dir)
    }

    #[cfg(test)]
    pub(super) fn adopt_key_rotation(
        &self,
        encryption: EncryptionService,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .adopt_key_rotation_for_test(encryption)
            .map(|_| ())
            .map_err(SyncError::from)
    }

    #[cfg(test)]
    pub(super) fn encryption_generation(&self) -> Option<u64> {
        self.loop_handle.encryption_generation_for_test()
    }

    #[cfg(test)]
    pub(super) fn open_sealed_blob(
        &self,
        bytes: &[u8],
        context: &[u8],
    ) -> Result<(crate::encryption::KeyFingerprint, Vec<u8>), StorageError> {
        self.loop_handle
            .open_sealed_blob_for_test(bytes, context)
            .map_err(StorageError::Storage)
    }
}

pub(super) struct ActiveSyncOperation {
    pub(super) loop_handle: Arc<SyncLoopHandle>,
}

impl ActiveSyncOperation {
    pub(super) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        self.loop_handle.make_remote(root_table, root_id, pin).await
    }

    pub(super) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        self.loop_handle
            .cancel_make_remote(root_table, root_id)
            .await
    }

    pub(super) async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), MakeLocalError> {
        self.loop_handle
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    pub(super) async fn drain_uploads(
        &self,
    ) -> Result<crate::protocol::blob::DrainOutcome, DbError> {
        self.loop_handle.drain_uploads().await
    }

    pub(super) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::sync::store::StoreError> {
        self.loop_handle.discard_blocked_write(write_id).await
    }

    pub(super) async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOfferBundle, crate::sync::store::DeviceJoinTransportError> {
        self.loop_handle
            .begin_device_join_bundle(member_pubkey)
            .await
    }

    pub(super) async fn drive_device_join(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, crate::sync::store::DeviceJoinTransportError> {
        self.loop_handle
            .drive_device_join(bundle, policy, access_administrator, timing)
            .await
    }

    pub(super) async fn cancel_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::sync::store::DeviceJoinTransportError>
    {
        self.loop_handle
            .cancel_device_join_transport(bundle, timing)
            .await
    }

    pub(super) async fn abandon_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
    ) -> Result<crate::DeviceJoinAbandonment, crate::sync::store::DeviceJoinTransportError> {
        self.loop_handle.abandon_device_join_transport(bundle).await
    }

    pub(super) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, crate::DeviceJoinError> {
        self.loop_handle.begin_device_join(member_pubkey).await
    }

    pub(super) async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, crate::DeviceJoinError> {
        self.loop_handle.abandon_device_join(offer).await
    }

    pub(super) async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, crate::DeviceJoinError> {
        self.loop_handle
            .authorize_device_provider_access(request, access_administrator)
            .await
    }

    pub(super) async fn accept_device_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, crate::DeviceJoinError> {
        self.loop_handle.accept_device_registration(request).await
    }

    pub(super) async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, crate::DeviceJoinError> {
        self.loop_handle
            .publish_device_provider_challenge(bootstrap)
            .await
    }

    pub(super) async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, crate::DeviceJoinError> {
        self.loop_handle
            .complete_device_provider_admission(readiness)
            .await
    }

    pub(super) async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, crate::DeviceJoinError> {
        self.loop_handle.finalize_device_join(completion).await
    }

    pub(super) async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, crate::DeviceJoinError> {
        self.loop_handle.cancel_device_join(attempt).await
    }

    pub(super) async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.loop_handle
            .close_device_provider_admission(cancellation)
            .await
    }

    pub(super) async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.loop_handle
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await
    }

    pub(super) async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, crate::DeviceJoinError> {
        self.loop_handle
            .revoke_joining_device_writes(cancellation, executor)
            .await
    }

    pub(super) async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::DeviceJoinError> {
        self.loop_handle.activate_device_join_cleanup(receipt).await
    }

    pub(super) async fn complete_owner_device_join_cleanup(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), crate::DeviceJoinError> {
        self.loop_handle
            .complete_owner_device_join_cleanup(activation)
            .await
            .map(|_| ())
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        self.loop_handle.is_encrypted()
    }

    pub(crate) fn store_name(&self) -> &str {
        &self.loop_handle.config().store_name
    }

    pub(crate) async fn invite(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
    ) -> Result<crate::join_code::InviteCode, crate::sync::store::MembershipOpsError> {
        self.loop_handle
            .invite_member(public_key_hex, invitee_email, role, self.store_name())
            .await
    }

    pub(crate) async fn remove(
        &self,
        public_key_hex: &str,
    ) -> Result<String, crate::sync::store::MembershipOpsError> {
        self.loop_handle.remove_member(public_key_hex).await
    }

    pub(crate) async fn resolve(
        &self,
        choice: &crate::MembershipConflictChoice,
    ) -> Result<(), crate::sync::store::MembershipOpsError> {
        self.loop_handle.resolve_membership_conflict(choice).await
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::StoreDeviceExclusionProposalRef, SyncError> {
        self.loop_handle
            .propose_device_exclusion(device_id)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .cancel_device_exclusion(proposal)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .finalize_device_exclusion(proposal)
            .await
            .map_err(|error| SyncError::DeviceExclusion(error.to_string()))
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionRequest, SyncError> {
        self.loop_handle
            .begin_owner_promotion(device_id)
            .await
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionAcceptance, SyncError> {
        self.loop_handle
            .accept_owner_promotion(request)
            .await
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), SyncError> {
        self.loop_handle
            .finalize_owner_promotion(acceptance)
            .await
            .map_err(|error| SyncError::OwnerPromotion(error.to_string()))
    }

    pub(super) async fn create_circle(
        &self,
        name: &str,
    ) -> Result<crate::CircleId, crate::sync::store::CircleOperationError> {
        self.loop_handle.create_circle(name).await
    }

    pub(super) async fn rename_circle(
        &self,
        circle_id: crate::CircleId,
        name: &str,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle.rename_circle(circle_id, name).await
    }

    pub(super) async fn add_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
        role: crate::CircleRole,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .add_circle_member(circle_id, member_pubkey, role)
            .await
    }

    pub(super) async fn remove_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
    ) -> Result<crate::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.loop_handle
            .remove_circle_member(circle_id, member_pubkey)
            .await
    }

    pub(super) async fn resolve_circle(
        &self,
        circle_id: crate::CircleId,
        chosen: crate::CircleControlCoord,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .resolve_circle_control(circle_id, chosen)
            .await
    }

    pub(super) async fn cancel_circle_close(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.loop_handle.cancel_circle_epoch_close(circle_id).await
    }

    pub(super) async fn exclude_circle_close_device(
        &self,
        circle_id: crate::CircleId,
        device_id: crate::StoreDeviceId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .exclude_circle_close_device(circle_id, device_id)
            .await
    }

    pub(super) async fn delete_circle(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle.delete_circle(circle_id).await
    }

    pub(super) async fn retry_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle.retry_circle_operation(operation_id).await
    }

    pub(super) async fn discard_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.loop_handle
            .discard_circle_operation(operation_id)
            .await
    }

    pub(super) async fn circle_close_status(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleCloseStatus, crate::sync::store::CircleOperationError> {
        self.loop_handle.circle_close_status(circle_id).await
    }
}
