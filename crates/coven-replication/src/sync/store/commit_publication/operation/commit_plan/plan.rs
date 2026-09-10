use super::*;

pub(crate) enum StoreOperationBatch {
    Circle {
        reference: super::store_commit::CircleControlRef,
        stream_activations: Vec<super::store_commit::StreamActivation>,
    },
    Acknowledgement {
        reference: super::store_commit::StoreAckRef,
        value: super::store_commit::StoreAck,
        circle_acknowledgements: Vec<CircleAckActivation>,
    },

    ProviderAccessGrant(super::provider::StoreMemberProviderAccessGrantRef),
    Attempt(coven_protocol::store_commit::DeviceJoinAttemptId),
    SamePrincipalDeviceJoin {
        attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
        registration: Box<ActivatedStoreDeviceRegistration>,
        transition: super::membership::MergeMembershipHeadTransition,
    },
    Abandonment(coven_protocol::store_commit::DeviceJoinAbandonmentRef),
    AbandonCandidates(Vec<coven_protocol::store_commit::CandidateCleanupManifest>),
    JoinActivation {
        registration: Box<ActivatedStoreDeviceRegistration>,
        transition: super::membership::MergeMembershipHeadTransition,
    },
    DeviceExclusionProposal {
        proposal: super::store_commit::RetainedStoreDeviceExclusionProposal,
        transition: super::membership::MergeMembershipHeadTransition,
    },
    DeviceExclusionOutcome {
        outcome: super::store_commit::RetainedStoreDeviceExclusionOutcome,
        transition: super::membership::MergeMembershipHeadTransition,
    },
    ReclaimAuthorization(Box<coven_protocol::reclaim::ReclaimAuthorizationRef>),
    ReclaimReceipt(Box<coven_protocol::reclaim::ReclaimReceiptRef>),
    OwnerPromotionRequest(super::store_commit::OwnerPromotionRequest),
    MergeMembershipActivation {
        transition: super::membership::MergeMembershipHeadTransition,
        stream_activations: Vec<super::store_commit::StreamActivation>,
    },
}

pub struct StoreOperationCommitPlan {
    /// This device's turn to author its own next Store commit, taken when the
    /// position this plan's order extends was read. A plan is the live claim on
    /// that position: keep it through publication, or transfer it back to the
    /// operation that will continue publishing the persisted candidate.
    _authorship: coven_database::OwnStreamAuthorship,
    writer: std::sync::Arc<LocalStoreWriter>,
    root: StoreRootRef,
    coord: StoreCommitCoord,
    order: StoreCommitOrder,
    publication_previous: coven_database::ObservedStorePublication,
    membership_state: super::circle_control::StoreMembershipStateRef,
    device_state: super::store_commit::StoreDeviceStateRef,
    membership_authority: MembershipCoord,
    owner_grant: Option<super::membership::MembershipGrantId>,
    membership: MembershipChain,
    predecessor_state: super::store_commit::ResolvedStoreDeviceState,
}

impl StoreOperationCommitPlan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        authorship: coven_database::OwnStreamAuthorship,
        writer: std::sync::Arc<LocalStoreWriter>,
        root: StoreRootRef,
        coord: StoreCommitCoord,
        order: StoreCommitOrder,
        publication_previous: coven_database::ObservedStorePublication,
        membership_state: super::circle_control::StoreMembershipStateRef,
        device_state: super::store_commit::StoreDeviceStateRef,
        membership_authority: MembershipCoord,
        owner_grant: Option<super::membership::MembershipGrantId>,
        membership: MembershipChain,
        predecessor_state: super::store_commit::ResolvedStoreDeviceState,
    ) -> Self {
        Self {
            _authorship: authorship,
            writer,
            root,
            coord,
            order,
            publication_previous,
            membership_state,
            device_state,
            membership_authority,
            owner_grant,
            membership,
            predecessor_state,
        }
    }

    pub(crate) fn validate_acknowledgement(
        &self,
        acknowledgement: &super::store_commit::StoreAck,
    ) -> Result<(), StoreError> {
        let predecessor_cut = self.order.predecessor_cut().map_err(StoreError::from)?;
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

    pub(crate) fn sign_batch(
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
                publication_base: self.publication_previous.record().publication_base(),
                membership_state: self.membership_state.clone(),
                device_state: self.device_state.clone(),
                membership_authority: self.membership_authority.clone(),
            },
            batch,
        )
    }

    pub(crate) fn into_authorship(self) -> coven_database::OwnStreamAuthorship {
        self._authorship
    }

    pub(crate) fn membership(&self) -> &MembershipChain {
        &self.membership
    }

    pub(crate) fn predecessor_state(&self) -> &super::store_commit::ResolvedStoreDeviceState {
        &self.predecessor_state
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_owner_promotion_request(
        &self,
        promotion_id: super::store_commit::OwnerPromotionId,
        member_registration: super::store_commit::StoreDeviceRegistrationRef,
        member_pubkey: String,
        member_grant: super::membership::MembershipGrantId,
        finalization: super::store_commit::OwnerPromotionFinalization,
        publication_slot: coven_protocol::objects::ObjectSlot,
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
            publication_slot,
        )
    }

    pub(crate) fn candidate_family(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> super::store_commit::CandidateFamilyId {
        self.writer
            .candidate_family_id(self.root.store_root_hash, write_id, &self.order)
    }

    pub(crate) fn predecessor_cut(&self) -> Result<StoreHistoryCut, StoreError> {
        self.order.predecessor_cut().map_err(StoreError::from)
    }

    pub(crate) fn membership_state(&self) -> &super::circle_control::StoreMembershipStateRef {
        &self.membership_state
    }

    pub(crate) fn membership_authority(&self) -> &MembershipCoord {
        &self.membership_authority
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

    pub(crate) fn publication_previous(&self) -> &coven_database::ObservedStorePublication {
        &self.publication_previous
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
            .map_err(StoreError::from)
    }

    pub(crate) fn retain_device_exclusion_outcome(
        &self,
        reference: &super::store_commit::StoreDeviceExclusionOutcomeRef,
        proposal: super::store_commit::RetainedStoreDeviceExclusionProposal,
        outcome: &super::store_commit::StoreDeviceExclusionOutcome,
    ) -> Result<super::store_commit::RetainedStoreDeviceExclusionOutcome, StoreError> {
        self.writer
            .retain_device_exclusion_outcome(reference, proposal, outcome)
            .map_err(StoreError::from)
    }

    pub(crate) fn verify_prepared_commit(
        &self,
        bytes: &[u8],
        object: coven_protocol::objects::ExactObjectRef,
    ) -> Result<super::store_commit::VerifiedStoreBatchCommit, StoreError> {
        self.writer
            .verify_prepared_commit(bytes, self.root.store_root_hash, self.coord.clone(), object)
            .map_err(StoreError::from)
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
            .map_err(StoreError::from)
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
        owner_grant: super::membership::MembershipGrantId,
    ) -> Result<super::store_commit::StoreDeviceExclusion, StoreError> {
        self.writer.sign_device_exclusion(
            proposal,
            proposal_value,
            target,
            target_registration,
            owner_grant,
        )
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
