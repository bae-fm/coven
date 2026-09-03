use super::*;

use coven_replication::sync::store::timed_owner_join_step;

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

    pub(crate) async fn retry_stuck_reclaim(
        &self,
        operation_id: crate::ObjectHash,
    ) -> Result<(), SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .retry_stuck_reclaim(operation_id)
            .await?)
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
        on_progress: &(dyn Fn(crate::AdmittingDeviceJoinProgress) + Send + Sync),
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .drive_device_join(bundle, policy, access_administrator, on_progress, timing)
            .await?)
    }

    pub(crate) async fn abort_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
    ) -> Result<(), SyncError> {
        Ok(active_sync!(self)
            .ok_or(SyncError::LoopNotRunning)?
            .abort_device_join_transport(bundle)
            .await?)
    }

    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        Ok(timed_owner_join_step(
            "publish offer",
            sync.provider_requests(),
            sync.begin_device_join(member_pubkey),
        )
        .await?)
    }

    pub(crate) async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        Ok(timed_owner_join_step(
            "abandon offer",
            sync.provider_requests(),
            sync.abandon_device_join(offer),
        )
        .await?)
    }

    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        Ok(timed_owner_join_step(
            "authorize provider access",
            sync.provider_requests(),
            sync.authorize_device_provider_access(request, access_administrator),
        )
        .await?)
    }

    pub(crate) async fn accept_device_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        Ok(timed_owner_join_step(
            "accept registration",
            sync.provider_requests(),
            sync.accept_device_registration(request),
        )
        .await?)
    }

    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        Ok(timed_owner_join_step(
            "publish provider challenge",
            sync.provider_requests(),
            sync.publish_device_provider_challenge(bootstrap),
        )
        .await?)
    }

    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        Ok(timed_owner_join_step(
            "complete provider admission",
            sync.provider_requests(),
            sync.complete_device_provider_admission(readiness),
        )
        .await?)
    }

    pub(crate) async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, SyncError> {
        let sync = active_sync!(self).ok_or(SyncError::LoopNotRunning)?;
        Ok(timed_owner_join_step(
            "publish activation",
            sync.provider_requests(),
            sync.finalize_device_join(completion),
        )
        .await?)
    }
}
