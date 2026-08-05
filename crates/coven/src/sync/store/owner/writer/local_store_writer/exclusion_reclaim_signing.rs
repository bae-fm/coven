use super::*;

impl LocalStoreWriter {
    pub(crate) fn sign_reclaim_evidence(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        claim: crate::protocol::reclaim::ReclaimClaim,
    ) -> Result<
        crate::protocol::reclaim::ReclaimEvidence,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::reclaim::ReclaimEvidence::signed(store_root_hash, claim, &self.identity)
    }

    pub(crate) fn sign_reclaim_authorization(
        &self,
        store_root_hash: crate::protocol::store_commit::ObjectHash,
        target: crate::protocol::reclaim::ReclaimTarget,
        evidence: crate::protocol::reclaim::ReclaimEvidenceRef,
        authority: crate::protocol::reclaim::StoreReclaimAuthority,
    ) -> crate::protocol::reclaim::ReclaimAuthorization {
        crate::protocol::reclaim::ReclaimAuthorization::signed(
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
        root_hash: crate::protocol::store_commit::ObjectHash,
        proposal_id: crate::protocol::store_commit::StoreDeviceExclusionProposalId,
        target: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        target_registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        device_state: crate::protocol::store_commit::StoreDeviceStateRef,
        outcome_slot: crate::protocol::objects::ObjectSlot,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<
        crate::protocol::store_commit::StoreDeviceExclusionProposal,
        crate::sync::store::StoreError,
    > {
        crate::protocol::store_commit::StoreDeviceExclusionProposal::signed(
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
        proposal: crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &crate::protocol::store_commit::StoreDeviceExclusionProposal,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<
        crate::protocol::store_commit::StoreDeviceExclusionCancellation,
        crate::sync::store::StoreError,
    > {
        crate::protocol::store_commit::StoreDeviceExclusionCancellation::signed(
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
        reference: crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposal,
        target: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        crate::protocol::store_commit::RetainedStoreDeviceExclusionProposal,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::RetainedStoreDeviceExclusionProposal::from_exact(
            reference,
            proposal,
            target,
            self.registration.value(),
        )
    }

    pub(crate) fn retain_device_exclusion_outcome(
        &self,
        reference: &crate::protocol::store_commit::StoreDeviceExclusionOutcomeRef,
        proposal: crate::protocol::store_commit::RetainedStoreDeviceExclusionProposal,
        outcome: &crate::protocol::store_commit::StoreDeviceExclusionOutcome,
    ) -> Result<
        crate::protocol::store_commit::RetainedStoreDeviceExclusionOutcome,
        crate::protocol::store_commit::StoreProtocolError,
    > {
        crate::protocol::store_commit::RetainedStoreDeviceExclusionOutcome::from_exact(
            reference,
            proposal,
            outcome,
            self.registration.value(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_exclusion(
        &self,
        proposal: crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &crate::protocol::store_commit::StoreDeviceExclusionProposal,
        target: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        target_registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        proof: crate::protocol::store_commit::StoreDeviceExclusionProof,
        owner_grant: crate::protocol::membership::MembershipGrantId,
    ) -> Result<crate::protocol::store_commit::StoreDeviceExclusion, crate::sync::store::StoreError>
    {
        crate::protocol::store_commit::StoreDeviceExclusion::signed(
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
        root_hash: crate::protocol::store_commit::ObjectHash,
        commit: crate::protocol::store_commit::StoreBatchCommitRef,
        history_summary: crate::protocol::store_commit::ObjectHash,
        successor: crate::protocol::store_commit::SuccessorLink,
    ) -> Result<crate::protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
    {
        crate::protocol::store_commit::StoreDeviceHead::signed(
            root_hash,
            self.registration.reference().clone(),
            commit,
            history_summary,
            successor,
            &self.device_signer,
        )
        .map_err(|error| crate::sync::store::StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_reclaim_receipt(
        &self,
        root_hash: crate::protocol::store_commit::ObjectHash,
        authorization: crate::protocol::reclaim::ReclaimAuthorizationRef,
        membership_state: crate::protocol::circle_control::StoreMembershipStateRef,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    ) -> Result<crate::protocol::reclaim::ReclaimReceipt, crate::sync::store::StoreError> {
        crate::protocol::reclaim::ReclaimReceipt::signed(
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
