use super::*;

impl StoreSync {
    pub(crate) async fn members(
        &self,
    ) -> Result<Vec<coven_protocol::membership::MemberInfo>, SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.ensure_command_authority().await?;
        let authority = installed_command_authority!(self);
        match authority {
            CommandAuthority::Connected(sync) => sync.members().await.map_err(Into::into),
            CommandAuthority::CommandOnly(store) => store.members().await.map_err(Into::into),
        }
    }

    pub(crate) async fn membership_conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.ensure_command_authority().await?;
        let authority = installed_command_authority!(self);
        match authority {
            CommandAuthority::Connected(sync) => {
                sync.membership_conflict().await.map_err(Into::into)
            }
            CommandAuthority::CommandOnly(store) => {
                store.membership_conflict().await.map_err(Into::into)
            }
        }
    }

    pub(crate) async fn restore_membership(
        &self,
    ) -> Result<coven_replication::sync::store::StoreRestoreMembership, SyncError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.ensure_command_authority().await?;
        let authority = installed_command_authority!(self);
        match authority {
            CommandAuthority::Connected(sync) => {
                sync.restore_membership().await.map_err(Into::into)
            }
            CommandAuthority::CommandOnly(store) => {
                store.restore_membership().await.map_err(Into::into)
            }
        }
    }

    pub(crate) async fn admit_member(
        &self,
        public_key_hex: &str,
        member_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
    ) -> Result<coven_replication::sync::MemberAdmission, SyncError> {
        let active = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        if !active.is_encrypted() {
            return Err(SyncError::NotEncryptedHome);
        }
        active
            .admit_member(
                public_key_hex,
                member_email,
                role,
                &active.config().store_name,
            )
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn remove_store_member(&self, public_key_hex: &str) -> Result<(), SyncError> {
        let active = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        if !active.is_encrypted() {
            return Err(SyncError::NotEncryptedHome);
        }
        active
            .remove_member(public_key_hex)
            .await
            .map(drop)
            .map_err(Into::into)
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &crate::MembershipConflictChoice,
    ) -> Result<(), SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .resolve_membership_conflict(choice)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<coven_protocol::store_commit::StoreDeviceExclusionProposalRef, SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .propose_device_exclusion(device_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .cancel_device_exclusion(proposal)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .finalize_device_exclusion(proposal)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<coven_protocol::store_commit::OwnerPromotionRequest, SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .begin_owner_promotion(device_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: coven_protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<coven_protocol::store_commit::OwnerPromotionAcceptance, SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .accept_owner_promotion(request)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .finalize_owner_promotion(acceptance)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn create_circle(
        &self,
        name: &str,
    ) -> Result<crate::CircleId, crate::CircleError> {
        active_circle_sync!(self)?
            .create_circle(name)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn rename_circle(
        &self,
        circle_id: crate::CircleId,
        name: &str,
    ) -> Result<(), crate::CircleError> {
        active_circle_sync!(self)?
            .rename_circle(circle_id, name)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn add_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
        role: crate::CircleRole,
    ) -> Result<(), crate::CircleError> {
        active_circle_sync!(self)?
            .add_circle_member(circle_id, member_pubkey, role)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn remove_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
    ) -> Result<crate::CircleOperationId, crate::CircleError> {
        active_circle_sync!(self)?
            .remove_circle_member(circle_id, member_pubkey)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn resolve_circle(
        &self,
        circle_id: crate::CircleId,
        chosen: crate::CircleControlCoord,
    ) -> Result<(), crate::CircleError> {
        active_circle_sync!(self)?
            .resolve_circle_control(circle_id, chosen)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn cancel_circle_close(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleOperationId, crate::CircleError> {
        active_circle_sync!(self)?
            .cancel_circle_epoch_close(circle_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn exclude_circle_close_device(
        &self,
        circle_id: crate::CircleId,
        device_id: crate::StoreDeviceId,
    ) -> Result<(), crate::CircleError> {
        active_circle_sync!(self)?
            .exclude_circle_close_device(circle_id, device_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn delete_circle(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<(), crate::CircleError> {
        active_circle_sync!(self)?
            .delete_circle(circle_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn retry_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::CircleError> {
        active_circle_sync!(self)?
            .retry_circle_operation(operation_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn discard_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::CircleError> {
        active_circle_sync!(self)?
            .discard_circle_operation(operation_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn circle_close_status(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleCloseStatus, crate::CircleError> {
        active_circle_sync!(self)?
            .circle_close_status(circle_id)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOfferBundle, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .begin_device_join_bundle(member_pubkey)
            .await?)
    }

    pub(crate) async fn drive_device_join(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .drive_device_join(bundle, policy, access_administrator, timing)
            .await?)
    }

    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .begin_device_join(member_pubkey)
            .await?)
    }

    pub(crate) async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .abandon_device_join(offer)
            .await?)
    }

    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .authorize_device_provider_access(request, access_administrator)
            .await?)
    }

    pub(crate) async fn accept_device_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .accept_device_registration(request)
            .await?)
    }

    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .publish_device_provider_challenge(bootstrap)
            .await?)
    }

    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .complete_device_provider_admission(readiness)
            .await?)
    }

    pub(crate) async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .finalize_device_join(completion)
            .await?)
    }

    pub(crate) async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .cancel_device_join(attempt)
            .await?)
    }

    pub(crate) async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .close_device_provider_admission(cancellation)
            .await?)
    }

    pub(crate) async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await?)
    }

    pub(crate) async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .revoke_joining_device_writes(cancellation, executor)
            .await?)
    }

    pub(crate) async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .activate_device_join_cleanup(receipt)
            .await?)
    }

    pub(crate) async fn complete_owner_device_join_cleanup(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), SyncError> {
        active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .complete_owner_device_join_cleanup(activation)
            .await?;
        Ok(())
    }
}
