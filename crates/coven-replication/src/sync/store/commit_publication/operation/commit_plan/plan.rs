use super::*;

pub(crate) enum StoreOperationBatch {
    Acknowledgement {
        reference: super::store_commit::StoreAckRef,
        value: super::store_commit::StoreAck,
        circle_acknowledgements: Vec<CircleAckActivation>,
    },

    ProviderAccessGrant(super::provider::StoreMemberProviderAccessGrantRef),
    Attempt(DeviceJoinAttemptRef),
    Abandonment(coven_protocol::store_commit::DeviceJoinAbandonmentRef),
    Outcome {
        outcome: DeviceJoinOutcomeRef,
        registration: Option<Box<ActivatedStoreDeviceRegistration>>,
    },
    CleanupReceipt(coven_protocol::store_commit::DeviceJoinCleanupReceiptRef),
    DeviceExclusionProposal(super::store_commit::RetainedStoreDeviceExclusionProposal),
    DeviceExclusionOutcome(super::store_commit::RetainedStoreDeviceExclusionOutcome),
    ReclaimAuthorization(Box<coven_protocol::reclaim::ReclaimAuthorizationRef>),
    ReclaimReceipt(Box<coven_protocol::reclaim::ReclaimReceiptRef>),
    OwnerPromotionRequest(super::store_commit::OwnerPromotionRequest),
    MergeMembershipActivation {
        transition: super::membership::MergeMembershipHeadTransition,
        stream_activations: Vec<super::store_commit::StreamActivation>,
    },
}

pub struct StoreOperationPlanCommon {
    /// This device's turn to author its own next Store commit, taken when the
    /// position this plan's order extends was read. A plan is the live claim on
    /// that position: hold it until the commit has published its head, or until
    /// the candidate is durably persisted for a later publisher to activate.
    pub(super) _authorship: coven_database::OwnStreamAuthorship,
    pub(super) writer: std::sync::Arc<LocalStoreWriter>,
    pub(super) root: StoreRootRef,
    pub(super) coord: StoreCommitCoord,
    pub(super) order: StoreCommitOrder,
    pub(super) membership_state: super::circle_control::StoreMembershipStateRef,
    pub(super) device_state: super::store_commit::StoreDeviceStateRef,
    pub(super) membership_authority: StoreOperationMembershipAuthority,
    pub(super) owner_grant: Option<super::membership::MembershipGrantId>,
}

pub struct StoreOperationCommitPlan {
    pub(super) common: StoreOperationPlanCommon,
    pub(super) membership: MembershipChain,
    pub(super) predecessor_state: super::store_commit::ResolvedStoreDeviceState,
}

impl std::ops::Deref for StoreOperationCommitPlan {
    type Target = StoreOperationPlanCommon;

    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl StoreOperationPlanCommon {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        authorship: coven_database::OwnStreamAuthorship,
        writer: std::sync::Arc<LocalStoreWriter>,
        root: StoreRootRef,
        coord: StoreCommitCoord,
        order: StoreCommitOrder,
        membership_state: super::circle_control::StoreMembershipStateRef,
        device_state: super::store_commit::StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        owner_grant: Option<super::membership::MembershipGrantId>,
    ) -> Self {
        Self {
            _authorship: authorship,
            writer,
            root,
            coord,
            order,
            membership_state,
            device_state,
            membership_authority,
            owner_grant,
        }
    }

    pub(crate) fn validate_acknowledgement(
        &self,
        acknowledgement: &super::store_commit::StoreAck,
    ) -> Result<(), StoreError> {
        let predecessor_cut = self
            .order
            .predecessor_cut()
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        if !self
            .writer
            .is_authored_by_registration(&acknowledgement.registration)
            || acknowledgement.store_cut != predecessor_cut
            || acknowledgement.device_state != self.device_state
        {
            return Err(StoreError::InvalidOutbound(
                "Store acknowledgement differs from its operation commit predecessor".to_string(),
            ));
        }
        Ok(())
    }

    fn sign_batch(
        &self,
        write_id: coven_protocol::write::WriteId,
        batch: StoreOperationBatch,
    ) -> Result<(StoreBatchCommit, Option<ActivatedStoreDeviceRegistration>), StoreError> {
        self.writer.sign_operation_batch(
            write_id,
            StoreOperationSigningContext {
                root: self.root.clone(),
                coord: self.coord.clone(),
                order: self.order.clone(),
                membership_state: self.membership_state.clone(),
                device_state: self.device_state.clone(),
                membership_authority: self.membership_authority.clone(),
            },
            batch,
        )
    }
}

impl StoreOperationCommitPlan {
    pub(crate) fn new(
        common: StoreOperationPlanCommon,
        membership: MembershipChain,
        predecessor_state: super::store_commit::ResolvedStoreDeviceState,
    ) -> Self {
        Self {
            common,
            membership,
            predecessor_state,
        }
    }

    pub(crate) fn common(&self) -> &StoreOperationPlanCommon {
        &self.common
    }

    pub(crate) fn membership(&self) -> &MembershipChain {
        &self.membership
    }

    pub(crate) fn predecessor_state(&self) -> &super::store_commit::ResolvedStoreDeviceState {
        &self.predecessor_state
    }

    pub(crate) fn sign_batch(
        &self,
        write_id: coven_protocol::write::WriteId,
        batch: StoreOperationBatch,
    ) -> Result<(StoreBatchCommit, Option<ActivatedStoreDeviceRegistration>), StoreError> {
        self.common.sign_batch(write_id, batch)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_owner_promotion_request(
        &self,
        promotion_id: super::store_commit::OwnerPromotionId,
        member_registration: super::store_commit::StoreDeviceRegistrationRef,
        member_pubkey: String,
        member_grant: super::membership::MembershipGrantId,
        finalization: super::store_commit::OwnerPromotionFinalization,
    ) -> Result<super::store_commit::OwnerPromotionRequest, StoreError> {
        let promoter_owner_grant = self.owner_grant.clone().ok_or_else(|| {
            StoreError::InvalidOutbound(
                "Owner-promotion request author has no active Owner grant".to_string(),
            )
        })?;
        self.writer.sign_owner_promotion_request(
            promotion_id,
            &self.root,
            promoter_owner_grant,
            member_pubkey,
            member_grant,
            member_registration,
            self.membership_state.clone(),
            self.device_state.clone(),
            finalization,
        )
    }

    pub(crate) fn predecessor_cut(&self) -> Result<StoreHistoryCut, StoreError> {
        self.order
            .predecessor_cut()
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn membership_state(&self) -> &super::circle_control::StoreMembershipStateRef {
        &self.membership_state
    }

    pub(crate) fn device_state(&self) -> &super::store_commit::StoreDeviceStateRef {
        &self.device_state
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        &self.root
    }

    pub(crate) fn coord(&self) -> &StoreCommitCoord {
        &self.coord
    }

    pub(crate) fn device_id(&self) -> &super::store_commit::StoreDeviceId {
        self.writer.device_id()
    }

    pub(crate) fn author_pubkey(&self) -> String {
        self.writer.author_pubkey()
    }

    pub(crate) fn is_local_registration(
        &self,
        registration: &super::store_commit::StoreDeviceRegistrationRef,
    ) -> bool {
        self.writer.is_authored_by_registration(registration)
    }

    pub(crate) fn retain_device_exclusion_proposal(
        &self,
        reference: super::store_commit::StoreDeviceExclusionProposalRef,
        proposal: &super::store_commit::StoreDeviceExclusionProposal,
        target: &super::store_commit::StoreDeviceRegistration,
    ) -> Result<super::store_commit::RetainedStoreDeviceExclusionProposal, StoreError> {
        self.writer
            .retain_device_exclusion_proposal(reference, proposal, target)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn retain_device_exclusion_outcome(
        &self,
        reference: &super::store_commit::StoreDeviceExclusionOutcomeRef,
        proposal: super::store_commit::RetainedStoreDeviceExclusionProposal,
        outcome: &super::store_commit::StoreDeviceExclusionOutcome,
    ) -> Result<super::store_commit::RetainedStoreDeviceExclusionOutcome, StoreError> {
        self.writer
            .retain_device_exclusion_outcome(reference, proposal, outcome)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn announcement_activation_id(
        &self,
    ) -> Result<super::store_commit::StreamActivationId, StoreError> {
        self.writer
            .announcement_activation_id()
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn verify_prepared_commit(
        &self,
        bytes: &[u8],
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<super::store_commit::VerifiedStoreBatchCommit, StoreError> {
        self.writer
            .verify_prepared_commit(bytes, self.root.store_root_hash, self.coord.clone(), object)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) async fn retain_acknowledgement(
        &self,
        history: &AuthorizedStoreHistory<'_>,
        activating_commit: &super::store_commit::StoreBatchCommitRef,
        activating_commit_value: &super::store_commit::StoreBatchCommit,
        reference: super::store_commit::StoreAckRef,
        value: super::store_commit::StoreAck,
    ) -> Result<super::store_commit::RetainedVerifiedActivatedAck, pull::StorePullError> {
        self.writer
            .retain_acknowledgement(
                history,
                activating_commit,
                activating_commit_value,
                reference,
                value,
            )
            .await
    }

    pub(crate) fn owner_grant(&self) -> Option<&super::membership::MembershipGrantId> {
        self.owner_grant.as_ref()
    }

    pub(crate) fn effective_provider_admin_grant(
        &self,
        state: &coven_protocol::provider::ProviderAdminState,
    ) -> Option<coven_protocol::provider::ProviderAdminGrantId> {
        self.writer.effective_provider_admin_grant(state)
    }

    pub(crate) fn sign_reclaim_evidence(
        &self,
        claim: coven_protocol::reclaim::ReclaimClaim,
    ) -> Result<coven_protocol::reclaim::ReclaimEvidence, StoreError> {
        self.writer
            .sign_reclaim_evidence(self.root.store_root_hash, claim)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_reclaim_authorization(
        &self,
        target: coven_protocol::reclaim::ReclaimTarget,
        evidence: coven_protocol::reclaim::ReclaimEvidenceRef,
        authority: coven_protocol::reclaim::StoreReclaimAuthority,
    ) -> coven_protocol::reclaim::ReclaimAuthorization {
        self.writer.sign_reclaim_authorization(
            self.root.store_root_hash,
            target,
            evidence,
            authority,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_exclusion_proposal(
        &self,
        proposal_id: super::store_commit::StoreDeviceExclusionProposalId,
        target: super::store_commit::StoreDeviceRegistrationRef,
        target_registration: &super::store_commit::StoreDeviceRegistration,
        outcome_slot: coven_protocol::objects::ObjectSlot,
        owner_grant: super::membership::MembershipGrantId,
    ) -> Result<super::store_commit::StoreDeviceExclusionProposal, StoreError> {
        self.writer.sign_device_exclusion_proposal(
            self.root.store_root_hash,
            proposal_id,
            target,
            target_registration,
            self.device_state.clone(),
            outcome_slot,
            owner_grant,
        )
    }

    pub(crate) fn sign_device_exclusion_cancellation(
        &self,
        proposal: super::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &super::store_commit::StoreDeviceExclusionProposal,
        owner_grant: super::membership::MembershipGrantId,
    ) -> Result<super::store_commit::StoreDeviceExclusionCancellation, StoreError> {
        self.writer
            .sign_device_exclusion_cancellation(proposal, proposal_value, owner_grant)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_exclusion(
        &self,
        proposal: super::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &super::store_commit::StoreDeviceExclusionProposal,
        target: super::store_commit::StoreDeviceRegistrationRef,
        target_registration: &super::store_commit::StoreDeviceRegistration,
        proof: super::store_commit::StoreDeviceExclusionProof,
        owner_grant: super::membership::MembershipGrantId,
    ) -> Result<super::store_commit::StoreDeviceExclusion, StoreError> {
        self.writer.sign_device_exclusion(
            proposal,
            proposal_value,
            target,
            target_registration,
            proof,
            owner_grant,
        )
    }

    pub(crate) fn sign_device_head(
        &self,
        commit: super::store_commit::StoreBatchCommitRef,
        successor: super::store_commit::SuccessorLink,
    ) -> Result<super::store_commit::StoreDeviceHead, StoreError> {
        self.writer
            .sign_device_head(self.root.store_root_hash, commit, successor)
    }

    pub(crate) fn sign_reclaim_receipt(
        &self,
        authorization: coven_protocol::reclaim::ReclaimAuthorizationRef,
        provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
    ) -> Result<coven_protocol::reclaim::ReclaimReceipt, StoreError> {
        self.writer.sign_reclaim_receipt(
            self.root.store_root_hash,
            authorization,
            self.membership_state.clone(),
            provider_admin_grant,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn local_registration_reference_for_test(
        &self,
    ) -> super::store_commit::StoreDeviceRegistrationRef {
        self.writer.registration_reference_for_test()
    }
}
