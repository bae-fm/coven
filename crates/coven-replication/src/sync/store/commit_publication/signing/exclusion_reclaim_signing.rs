use super::*;

impl LocalStoreWriter {
    pub(crate) fn sign_reclaim_evidence(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        claim: coven_protocol::reclaim::ReclaimClaim,
    ) -> Result<
        coven_protocol::reclaim::ReclaimEvidence,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::reclaim::ReclaimEvidence::signed(store_root_hash, claim, &self.identity)
    }

    pub(crate) fn sign_reclaim_authorization(
        &self,
        store_root_hash: coven_protocol::store_commit::ObjectHash,
        target: coven_protocol::reclaim::ReclaimTarget,
        evidence: coven_protocol::reclaim::ReclaimEvidenceRef,
        authority: coven_protocol::reclaim::StoreReclaimAuthority,
    ) -> coven_protocol::reclaim::ReclaimAuthorization {
        coven_protocol::reclaim::ReclaimAuthorization::signed(
            store_root_hash,
            target,
            evidence,
            authority,
            &self.identity,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_exclusion_proposal(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        proposal_id: coven_protocol::store_commit::StoreDeviceExclusionProposalId,
        target: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        target_registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        outcome_slot: coven_protocol::objects::ObjectSlot,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceExclusionProposal,
        crate::sync::store::StoreError,
    > {
        coven_protocol::store_commit::StoreDeviceExclusionProposal::signed(
            root_hash,
            proposal_id,
            target,
            target_registration,
            device_state,
            outcome_slot,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_device_exclusion_cancellation(
        &self,
        proposal: coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &coven_protocol::store_commit::StoreDeviceExclusionProposal,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceExclusionCancellation,
        crate::sync::store::StoreError,
    > {
        coven_protocol::store_commit::StoreDeviceExclusionCancellation::signed(
            proposal,
            proposal_value,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn retain_device_exclusion_proposal(
        &self,
        reference: coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposal,
        target: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        coven_protocol::store_commit::RetainedStoreDeviceExclusionProposal,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::RetainedStoreDeviceExclusionProposal::from_exact(
            reference,
            proposal,
            target,
            self.registration.value(),
        )
    }

    pub(crate) fn retain_device_exclusion_outcome(
        &self,
        reference: &coven_protocol::store_commit::StoreDeviceExclusionOutcomeRef,
        proposal: coven_protocol::store_commit::RetainedStoreDeviceExclusionProposal,
        outcome: &coven_protocol::store_commit::StoreDeviceExclusionOutcome,
    ) -> Result<
        coven_protocol::store_commit::RetainedStoreDeviceExclusionOutcome,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::RetainedStoreDeviceExclusionOutcome::from_exact(
            reference,
            proposal,
            outcome,
            self.registration.value(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_exclusion(
        &self,
        proposal: coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &coven_protocol::store_commit::StoreDeviceExclusionProposal,
        target: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        target_registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        proof: coven_protocol::store_commit::StoreDeviceExclusionProof,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<coven_protocol::store_commit::StoreDeviceExclusion, crate::sync::store::StoreError>
    {
        coven_protocol::store_commit::StoreDeviceExclusion::signed(
            proposal,
            proposal_value,
            target,
            target_registration,
            proof,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_device_head(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        commit: coven_protocol::store_commit::StoreBatchCommitRef,
        successor: coven_protocol::store_commit::SuccessorLink,
    ) -> Result<coven_protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError> {
        coven_protocol::store_commit::StoreDeviceHead::signed(
            root_hash,
            self.registration.reference().clone(),
            commit,
            successor,
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_reclaim_receipt(
        &self,
        root_hash: coven_protocol::store_commit::ObjectHash,
        authorization: coven_protocol::reclaim::ReclaimAuthorizationRef,
        membership_state: coven_protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
    ) -> Result<coven_protocol::reclaim::ReclaimReceipt, crate::sync::store::StoreError> {
        coven_protocol::reclaim::ReclaimReceipt::signed(
            root_hash,
            authorization,
            membership_state,
            provider_admin_grant,
            self.registration.reference().clone(),
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }
}
