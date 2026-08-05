use super::identifiers::commit_stream_id;
use super::operation_refs::{
    validate_commit_acknowledgement, validate_commit_circle_acknowledgements,
    validate_device_exclusion_refs, validate_device_join_attempt_decision_refs,
    validate_device_join_cleanup_receipt_refs, validate_device_join_outcome_refs,
    validate_device_registration_refs, validate_provider_access_refs,
};
use super::validation::{
    validate_commit_order, validate_commit_predecessor_states, validate_membership_authority,
    validate_operation_membership_authority,
};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommitBody {
    pub store_root_hash: ObjectHash,
    pub write_id: WriteId,
    pub author_registration: StoreDeviceRegistrationRef,
    pub order: StoreCommitOrder,
    pub membership_state: StoreMembershipStateRef,
    pub device_state: StoreDeviceStateRef,
    pub membership_authority: Option<MembershipGrantCreationAuthority>,
    pub candidate_objects: CandidateObjectManifest,
    pub body: StoreCommitBody,
}

impl SignedBody for StoreBatchCommitBody {
    const DOMAIN: &'static [u8] = COMMIT_DOMAIN;
}

pub(crate) type StoreBatchCommit = Signed<StoreBatchCommitBody>;

mod authoring;
mod validation;
pub(super) use validation::candidate_manifest;
#[cfg(test)]
pub(super) use validation::validate_stream_activations;

impl StoreBatchCommit {
    pub(crate) fn verified_candidate_objects(
        &self,
    ) -> Result<&CandidateObjectManifest, StoreProtocolError> {
        let expected = candidate_manifest(self.candidate_family(), &self.body)?;
        if self.candidate_objects != expected {
            return Err(StoreProtocolError::Malformed(
                "candidate object manifest differs from exact commit body graph".to_string(),
            ));
        }
        Ok(&self.candidate_objects)
    }

    pub fn seq(&self) -> u64 {
        self.order.seq()
    }

    pub fn candidate_family(&self) -> CandidateFamilyId {
        CandidateFamilyId::derive(
            self.store_root_hash,
            &self.author_registration,
            &self.write_id,
            &self.order,
        )
    }

    pub fn operations(&self) -> Option<&StoreCommitOperations> {
        match &self.body {
            StoreCommitBody::Operations(operations) => Some(operations),
            StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn control(&self) -> Option<&StoreControl> {
        self.operations()
            .and_then(|operations| operations.control.as_ref())
    }

    pub fn acknowledgement(&self) -> Option<&StoreAckRef> {
        self.operations()
            .and_then(|operations| operations.acknowledgement.as_ref())
    }

    pub fn circle_acknowledgements(&self) -> &[CircleAckRef] {
        self.operations().map_or(&[], |operations| {
            operations.circle_acknowledgements.as_slice()
        })
    }

    pub(crate) fn retained_operation_objects(
        &self,
    ) -> Result<Vec<ExactObjectRef>, StoreProtocolError> {
        let objects = self
            .acknowledgement()
            .map(|reference| reference.object.clone())
            .into_iter()
            .chain(
                self.circle_acknowledgements()
                    .iter()
                    .map(|reference| reference.object.clone()),
            )
            .chain(
                self.device_exclusion_proposals()
                    .iter()
                    .map(|reference| reference.object.clone()),
            )
            .chain(
                self.device_exclusion_outcomes()
                    .iter()
                    .map(|reference| reference.object().clone()),
            )
            .chain(
                self.reclaim_authorization()
                    .into_iter()
                    .flat_map(|reference| {
                        [reference.evidence.object.clone(), reference.object.clone()]
                    }),
            )
            .chain(
                self.reclaim_receipt()
                    .map(|reference| reference.object.clone()),
            )
            .collect::<Vec<_>>();
        if objects
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != objects.len()
        {
            return Err(StoreProtocolError::Malformed(
                "Store operation publication repeats a retained authority object".to_string(),
            ));
        }
        Ok(objects)
    }

    pub fn abandoned_candidates(&self) -> &[CandidateCleanupManifest] {
        match &self.body {
            StoreCommitBody::AbandonCandidates { manifests } => manifests,
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. } => &[],
        }
    }

    pub fn reclaim_authorization(
        &self,
    ) -> Option<&crate::protocol::reclaim::ReclaimAuthorizationRef> {
        match &self.body {
            StoreCommitBody::ReclaimAuthorization { authorization } => Some(authorization.as_ref()),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn reclaim_receipt(&self) -> Option<&crate::protocol::reclaim::ReclaimReceiptRef> {
        match &self.body {
            StoreCommitBody::ReclaimReceipt { receipt } => Some(receipt.as_ref()),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn device_join_attempt_decisions(&self) -> &[DeviceJoinAttemptDecisionRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_join_attempt_decisions.as_slice()
        })
    }

    pub fn device_join_outcomes(&self) -> &[DeviceJoinOutcomeRef] {
        self.operations()
            .map_or(&[], |operations| operations.device_join_outcomes.as_slice())
    }

    pub fn device_join_cleanup_receipts(
        &self,
    ) -> &[crate::protocol::store_commit::DeviceJoinCleanupReceiptRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_join_cleanup_receipts.as_slice()
        })
    }

    pub fn provider_access_grants(
        &self,
    ) -> &[crate::protocol::provider::StoreMemberProviderAccessGrantRef] {
        self.operations().map_or(&[], |operations| {
            operations.provider_access_grants.as_slice()
        })
    }

    pub fn device_registrations(&self) -> &[ActivatedStoreDeviceRegistrationRef] {
        match &self.body {
            StoreCommitBody::Operations(operations) => operations.device_registrations.as_slice(),
            StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. } => &[],
            StoreCommitBody::AbandonCandidates { .. } => &[],
        }
    }

    pub fn device_exclusion_proposals(&self) -> &[StoreDeviceExclusionProposalRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_exclusion_proposals.as_slice()
        })
    }

    pub fn device_exclusion_outcomes(&self) -> &[StoreDeviceExclusionOutcomeRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_exclusion_outcomes.as_slice()
        })
    }

    pub fn stream_activations(&self) -> &[StreamActivation] {
        self.operations()
            .map_or(&[], |operations| operations.stream_activations.as_slice())
    }

    pub fn owner_promotion_request(&self) -> Option<&OwnerPromotionRequest> {
        match &self.body {
            StoreCommitBody::OwnerPromotionRequest { request } => Some(request),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn circle_controls(&self) -> &[CircleControlRef] {
        self.operations()
            .map_or(&[], |operations| operations.circle_controls.as_slice())
    }

    pub fn store_package(&self) -> Option<&StorePackageRef> {
        self.operations()
            .and_then(|operations| operations.store_package.as_ref())
    }

    pub fn circle_packages(&self) -> &[CirclePackageRef] {
        self.operations()
            .map_or(&[], |operations| operations.circle_packages.as_slice())
    }
}
