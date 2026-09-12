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

    pub(crate) fn sign_device_exclusion_cancellation(
        &self,
        proposal: coven_protocol::store_commit::StoreDeviceExclusionProposal,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceExclusionCancellation,
        crate::sync::store::StoreError,
    > {
        coven_protocol::store_commit::StoreDeviceExclusionCancellation::signed(
            proposal,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::StoreError::from)
    }

    pub(crate) fn retain_device_exclusion_proposal(
        &self,
        proposal: coven_protocol::store_commit::StoreDeviceExclusionProposal,
        target: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        coven_protocol::store_commit::RetainedStoreDeviceExclusionProposal,
        coven_protocol::store_commit::StoreProtocolError,
    > {
        coven_protocol::store_commit::RetainedStoreDeviceExclusionProposal::from_exact(
            proposal, target,
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

    pub(crate) fn sign_device_exclusion(
        &self,
        proposal: coven_protocol::store_commit::StoreDeviceExclusionProposal,
        target: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        target_registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        owner_grant: coven_protocol::membership::MembershipGrantId,
    ) -> Result<coven_protocol::store_commit::StoreDeviceExclusion, crate::sync::store::StoreError>
    {
        coven_protocol::store_commit::StoreDeviceExclusion::signed(
            proposal,
            target,
            target_registration,
            self.registration.reference().clone(),
            owner_grant,
            self.registration.value(),
            &self.device_signer,
        )
        .map_err(crate::sync::store::StoreError::from)
    }
}
