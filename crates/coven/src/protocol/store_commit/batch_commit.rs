use super::identifiers::commit_stream_id;
use super::operation_refs::{
    validate_commit_acknowledgement, validate_commit_circle_acknowledgements,
    validate_device_exclusion_refs, validate_device_join_attempt_decision_refs,
    validate_device_join_cleanup_receipt_refs, validate_device_join_outcome_refs,
    validate_device_registration_refs, validate_provider_access_refs,
};
use super::validation::{
    require_version, validate_commit_order, validate_commit_predecessor_states,
    validate_membership_authority, validate_operation_membership_authority,
};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommit {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub write_id: WriteId,
    pub author_registration: StoreDeviceRegistrationRef,
    pub order: StoreCommitOrder,
    pub membership_state: StoreMembershipStateRef,
    pub device_state: StoreDeviceStateRef,
    pub membership_authority: Option<MembershipGrantCreationAuthority>,
    pub candidate_objects: CandidateObjectManifest,
    pub body: StoreCommitBody,
    pub signature: String,
}

#[derive(Serialize)]
struct CommitSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    write_id: &'a WriteId,
    author_registration: &'a StoreDeviceRegistrationRef,
    order: &'a StoreCommitOrder,
    membership_state: &'a StoreMembershipStateRef,
    device_state: &'a StoreDeviceStateRef,
    membership_authority: Option<&'a MembershipGrantCreationAuthority>,
    candidate_objects: &'a CandidateObjectManifest,
    body: &'a StoreCommitBody,
}

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

    #[cfg(test)]
    pub(crate) fn operations_membership_authority(
        &self,
    ) -> Result<StoreOperationMembershipAuthority, StoreProtocolError> {
        if self.operations().is_none() {
            return Err(StoreProtocolError::Malformed(
                "Store commit does not carry operations".to_string(),
            ));
        }
        let predecessor = self.membership_authority.clone().ok_or_else(|| {
            StoreProtocolError::Malformed(
                "operations commit omits its predecessor membership grant authority".to_string(),
            )
        })?;
        validate_operation_membership_authority(&predecessor)?;
        Ok(StoreOperationMembershipAuthority { predecessor })
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
    ) -> &[crate::sync::store::DeviceJoinCleanupReceiptRef] {
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

    #[allow(clippy::too_many_arguments)]
    pub fn signed_reclaim_authorization(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        authorization: crate::protocol::reclaim::ReclaimAuthorizationRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::ReclaimAuthorization {
                authorization: Box::new(authorization),
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_reclaim_receipt(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        receipt: crate::protocol::reclaim::ReclaimReceiptRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::ReclaimReceipt {
                receipt: Box::new(receipt),
            },
            signer,
        )
    }

    pub fn merge_dependencies(&self) -> &BTreeMap<AuthorStreamId, StoreBatchCommitRef> {
        &self.order.dependencies
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        package: StorePackageInput<'_>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: Some(package),
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_registrations(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations,
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_owner_promotion_request(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        request: OwnerPromotionRequest,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let membership_authority = membership_authority.into_commit_authority();
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            Some(&membership_authority),
            signer,
        )?;
        validate_owner_promotion_request_for_commit(
            &request,
            store_root_hash,
            &author_registration,
            author,
            &membership_state,
            &device_state,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            Some(membership_authority),
            StoreCommitBody::OwnerPromotionRequest {
                request: Box::new(request),
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_candidate_abandonment(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        mut manifests: Vec<CandidateCleanupManifest>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        manifests.sort();
        validate_candidate_abandonment(
            &manifests,
            store_root_hash,
            &author_registration,
            &coord,
            &order,
            author,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::AbandonCandidates { manifests },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_attempts(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        attempts: Vec<DeviceJoinAttemptRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: attempts
                    .into_iter()
                    .map(DeviceJoinAttemptDecisionRef::Attempt)
                    .collect(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_outcomes(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        device_join_outcomes: Vec<DeviceJoinOutcomeRef>,
        device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes,
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations,
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_abandonments(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        abandonments: Vec<crate::sync::store::DeviceJoinAbandonmentRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: abandonments
                    .into_iter()
                    .map(DeviceJoinAttemptDecisionRef::Abandoned)
                    .collect(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_cleanup_receipts(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        receipts: Vec<crate::sync::store::DeviceJoinCleanupReceiptRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: receipts,
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_device_exclusions(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        proposals: Vec<StoreDeviceExclusionProposalRef>,
        outcomes: Vec<StoreDeviceExclusionOutcomeRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: proposals,
                device_exclusion_outcomes: outcomes,
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_provider_access(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        provider_access_grants: Vec<crate::protocol::provider::StoreMemberProviderAccessGrantRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                circle_acknowledgements: Vec::new(),
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants,
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_operations(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        input: StoreCommitOperationsInput<'_>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let membership_authority = membership_authority.into_commit_authority();
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            Some(&membership_authority),
            signer,
        )?;
        let StoreCommitOperationsInput {
            acknowledgement,
            circle_acknowledgements,
            control,
            device_join_attempt_decisions,
            device_join_outcomes,
            device_join_cleanup_receipts,
            provider_access_grants,
            device_registrations,
            device_exclusion_proposals,
            device_exclusion_outcomes,
            stream_activations,
            circle_controls,
            store_package,
            circle_packages,
        } = input;
        validate_control(
            &author_registration,
            &author.author_pubkey,
            &membership_state,
            control.as_ref(),
        )?;
        validate_commit_acknowledgement(&acknowledgement, &author_registration)?;
        validate_commit_circle_acknowledgements(&circle_acknowledgements, &author_registration)?;
        let stream_id = commit_stream_id(&coord);
        let seq = order.seq();
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
        let store_package = store_package
            .map(|input| {
                if input.candidate_family != candidate_family {
                    return Err(StoreProtocolError::Malformed(
                        "Store package candidate family differs from its commit".to_string(),
                    ));
                }
                let semantic_prefix = package_semantic_prefix(
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(input.bytes),
                );
                package_ref(&semantic_prefix, &input)
            })
            .transpose()?;
        validate_device_join_attempt_decision_refs(&device_join_attempt_decisions)?;
        validate_device_join_outcome_refs(&device_join_outcomes)?;
        validate_device_join_cleanup_receipt_refs(&device_join_cleanup_receipts)?;
        validate_provider_access_refs(&provider_access_grants)?;
        validate_device_registration_refs(&device_registrations)?;
        validate_device_exclusion_refs(&device_exclusion_proposals, &device_exclusion_outcomes)?;
        validate_stream_activations(
            store_root_hash,
            &author_registration,
            control.as_ref(),
            &stream_activations,
        )?;
        let mut seen_circles = BTreeSet::new();
        let circle_packages = circle_packages
            .iter()
            .map(|input| {
                if !seen_circles.insert(input.circle_id) {
                    return Err(StoreProtocolError::DuplicateCirclePackage(input.circle_id));
                }
                validate_circle_control_coord(&input.control)?;
                if input.package.candidate_family != candidate_family {
                    return Err(StoreProtocolError::Malformed(
                        "Circle package candidate family differs from its commit".to_string(),
                    ));
                }
                let semantic_prefix = circle_package_semantic_prefix(
                    input.circle_id,
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(input.package.bytes),
                );
                let package = package_ref(&semantic_prefix, &input.package)?;
                Ok(CirclePackageRef {
                    circle_id: input.circle_id,
                    control: input.control.clone(),
                    package,
                    key_fingerprint: input.key_fingerprint,
                })
            })
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        validate_circle_control_refs(&circle_controls)?;
        let operations = StoreCommitOperations {
            acknowledgement,
            circle_acknowledgements,
            control,
            device_join_attempt_decisions,
            device_join_outcomes,
            device_join_cleanup_receipts,
            provider_access_grants,
            device_registrations,
            device_exclusion_proposals,
            device_exclusion_outcomes,
            stream_activations,
            circle_controls,
            store_package,
            circle_packages,
        };
        if operations.is_empty() {
            return Err(StoreProtocolError::EmptyBatch);
        }
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            Some(membership_authority),
            StoreCommitBody::Operations(operations),
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_signed_body(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        author_registration: StoreDeviceRegistrationRef,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: Option<MembershipGrantCreationAuthority>,
        body: StoreCommitBody,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
        validate_commit_body(store_root_hash, &body, &author_registration)?;
        let candidate_objects = candidate_manifest(family, &body)?;
        let mut commit = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            membership_authority,
            candidate_objects,
            body,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &commit.canonical_signed_bytes());
        commit.signature = signature;
        Ok(commit)
    }

    pub fn canonical_signed_bytes(&self) -> Vec<u8> {
        let fields = CommitSignedFields {
            version: self.version,
            store_root_hash: self.store_root_hash,
            write_id: &self.write_id,
            author_registration: &self.author_registration,
            order: &self.order,
            membership_state: &self.membership_state,
            device_state: &self.device_state,
            membership_authority: self.membership_authority.as_ref(),
            candidate_objects: &self.candidate_objects,
            body: &self.body,
        };
        domain_json(COMMIT_DOMAIN, &fields)
    }

    pub fn commit_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreBatchCommit serialization cannot fail")
    }

    pub fn verify_at(
        &self,
        expected_store_root_hash: ObjectHash,
        expected_coord: &StoreCommitCoord,
        author: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        crate::protocol::objects::verify_store_root(
            expected_store_root_hash,
            self.store_root_hash,
        )?;
        let stream_id = commit_stream_id(expected_coord);
        if self.order.seq() != expected_coord.sequence() {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: commit_slot_prefix(&stream_id, expected_coord.sequence()),
                actual: commit_slot_prefix(&stream_id, self.order.seq()),
            });
        }
        self.author_registration.verify_registration(author)?;
        let family = self.candidate_family();
        if let Some(package) = self.store_package() {
            if package.candidate_family != self.candidate_family() {
                return Err(StoreProtocolError::Malformed(
                    "Store package candidate family differs from its commit".to_string(),
                ));
            }
            let expected =
                package_semantic_prefix(family, &stream_id, self.order.seq(), package.content_hash);
            if package.object.slot().logical_key() != format!("{expected}.pkg") {
                return Err(StoreProtocolError::RelocatedPackage {
                    expected,
                    actual: package.object.slot().logical_key().to_string(),
                });
            }
        }
        let mut seen_circles = BTreeSet::new();
        for circle_package in self.circle_packages() {
            if circle_package.package.candidate_family != self.candidate_family() {
                return Err(StoreProtocolError::Malformed(
                    "Circle package candidate family differs from its commit".to_string(),
                ));
            }
            if !seen_circles.insert(circle_package.circle_id) {
                return Err(StoreProtocolError::DuplicateCirclePackage(
                    circle_package.circle_id,
                ));
            }
            validate_circle_control_coord(&circle_package.control)?;
            let expected = circle_package_semantic_prefix(
                circle_package.circle_id,
                family,
                &stream_id,
                self.seq(),
                circle_package.package.content_hash,
            );
            if circle_package.package.object.slot().logical_key() != format!("{expected}.pkg") {
                return Err(StoreProtocolError::RelocatedCirclePackage {
                    circle_id: circle_package.circle_id,
                    expected,
                    actual: circle_package
                        .package
                        .object
                        .slot()
                        .logical_key()
                        .to_string(),
                });
            }
        }
        validate_commit_body(self.store_root_hash, &self.body, &self.author_registration)?;
        if matches!(self.body, StoreCommitBody::Operations(_)) {
            validate_operation_membership_authority(
                self.membership_authority.as_ref().ok_or_else(|| {
                    StoreProtocolError::Malformed(
                        "operations commit omits membership authority".to_string(),
                    )
                })?,
            )?;
        }
        if let StoreCommitBody::AbandonCandidates { manifests } = &self.body {
            validate_candidate_abandonment(
                manifests,
                self.store_root_hash,
                &self.author_registration,
                expected_coord,
                &self.order,
                author,
            )?;
        }
        if let StoreCommitBody::OwnerPromotionRequest { request } = &self.body {
            validate_owner_promotion_request_for_commit(
                request,
                self.store_root_hash,
                &self.author_registration,
                author,
                &self.membership_state,
                &self.device_state,
            )?;
        }
        self.verified_candidate_objects()?;
        validate_commit_order(&self.order)?;
        validate_commit_predecessor_states(
            &self.order,
            &self.membership_state,
            &self.device_state,
        )?;
        if let Some(authority) = self.membership_authority.as_ref() {
            validate_membership_authority(authority)?;
        }
        validate_parsed_control(self, author)?;
        if !keys::verify_signature_hex(
            &author.device_signing_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    pub fn verify_store_package(&self, package_bytes: &[u8]) -> Result<(), StoreProtocolError> {
        let package = self
            .store_package()
            .ok_or(StoreProtocolError::MissingStorePackage)?;
        verify_package_ref(package, package_bytes)
    }

    pub fn verify_circle_package(
        &self,
        circle_id: CircleId,
        package_bytes: &[u8],
    ) -> Result<(), StoreProtocolError> {
        let package = self
            .circle_packages()
            .iter()
            .find(|package| package.circle_id == circle_id)
            .ok_or(StoreProtocolError::MissingCirclePackage(circle_id))?;
        verify_package_ref(&package.package, package_bytes)
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_commit_envelope(
    store_root_hash: ObjectHash,
    coord: &StoreCommitCoord,
    author_registration: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    order: &StoreCommitOrder,
    membership_state: &StoreMembershipStateRef,
    device_state: &StoreDeviceStateRef,
    membership_authority: Option<&MembershipGrantCreationAuthority>,
    signer: &UserKeypair,
) -> Result<(), StoreProtocolError> {
    author_registration.verify_registration(author)?;
    if keys::public_key_hex(signer) != author.device_signing_pubkey {
        return Err(StoreProtocolError::InvalidSignature);
    }
    if order.seq() == 0 {
        return Err(StoreProtocolError::InvalidSequence(0));
    }
    validate_commit_order(order)?;
    validate_commit_predecessor_states(order, membership_state, device_state)?;
    if coord.sequence() != order.seq() {
        return Err(StoreProtocolError::Malformed(
            "Store commit coordinate disagrees with its order".to_string(),
        ));
    }
    if let Some(authority) = membership_authority {
        validate_membership_authority(authority)?;
    }
    crate::protocol::objects::verify_store_root(
        store_root_hash,
        author.store_root.store_root_hash,
    )?;
    Ok(())
}

fn validate_commit_body(
    store_root_hash: ObjectHash,
    body: &StoreCommitBody,
    author: &StoreDeviceRegistrationRef,
) -> Result<(), StoreProtocolError> {
    match body {
        StoreCommitBody::Operations(operations) => {
            if operations.is_empty() {
                return Err(StoreProtocolError::EmptyBatch);
            }
            validate_circle_control_refs(&operations.circle_controls)?;
            validate_commit_acknowledgement(&operations.acknowledgement, author)?;
            validate_commit_circle_acknowledgements(&operations.circle_acknowledgements, author)?;
            validate_device_join_attempt_decision_refs(&operations.device_join_attempt_decisions)?;
            validate_device_join_outcome_refs(&operations.device_join_outcomes)?;
            validate_device_join_cleanup_receipt_refs(&operations.device_join_cleanup_receipts)?;
            validate_provider_access_refs(&operations.provider_access_grants)?;
            validate_device_registration_refs(&operations.device_registrations)?;
            validate_device_exclusion_refs(
                &operations.device_exclusion_proposals,
                &operations.device_exclusion_outcomes,
            )?;
            validate_stream_activations(
                store_root_hash,
                author,
                operations.control.as_ref(),
                &operations.stream_activations,
            )?;
        }
        StoreCommitBody::ReclaimAuthorization { .. } => {}
        StoreCommitBody::ReclaimReceipt { .. } => {}
        StoreCommitBody::OwnerPromotionRequest { request } => {
            if request.store_root_hash != store_root_hash
                || request.promoter_registration != *author
            {
                return Err(StoreProtocolError::OwnerPromotionMismatch);
            }
        }
        StoreCommitBody::AbandonCandidates { manifests } => {
            if manifests.is_empty() {
                return Err(StoreProtocolError::Malformed(
                    "candidate abandonment has no candidates".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_owner_promotion_request_for_commit(
    request: &OwnerPromotionRequest,
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    membership_state: &StoreMembershipStateRef,
    device_state: &StoreDeviceStateRef,
) -> Result<(), StoreProtocolError> {
    request.verify(&author.store_root, author)?;
    if request.store_root_hash != store_root_hash
        || request.promoter_registration != *author_registration
        || request.predecessor_membership != *membership_state
        || request.predecessor_devices != *device_state
    {
        return Err(StoreProtocolError::OwnerPromotionMismatch);
    }
    Ok(())
}

pub(super) fn validate_stream_activations(
    store_root_hash: ObjectHash,
    author: &StoreDeviceRegistrationRef,
    control: Option<&StoreControl>,
    activations: &[StreamActivation],
) -> Result<(), StoreProtocolError> {
    if activations.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::Malformed(
            "stream activations are not strictly sorted and unique".to_string(),
        ));
    }
    let mut activation_ids = BTreeSet::new();
    let mut stream_ids = BTreeSet::new();
    let mut first_slots = BTreeSet::new();
    for activation in activations {
        crate::protocol::objects::verify_store_root(store_root_hash, activation.store_root_hash())?;
        let owner_promotion = control.is_some();
        if activation.author_registration() != author && !owner_promotion {
            return Err(StoreProtocolError::Malformed(
                "stream activation registration differs from its commit author".to_string(),
            ));
        }
        let allowed_anchor = matches!(
            (control, activation),
            (
                Some(StoreControl { .. }),
                StreamActivation::GrantAuthorized {
                    anchor: GrantStreamAnchor::StoreMembership { .. }
                        | GrantStreamAnchor::OwnerRecovery { .. },
                    ..
                }
            ) | (
                _,
                StreamActivation::GrantAuthorized {
                    anchor: GrantStreamAnchor::CircleControl { .. }
                        | GrantStreamAnchor::CircleRoster { .. }
                        | GrantStreamAnchor::CircleMetadata { .. },
                    ..
                }
            )
        );
        if !allowed_anchor {
            return Err(StoreProtocolError::Malformed(
                "Store commit contains a root- or registration-authorized stream anchor"
                    .to_string(),
            ));
        }
        if !activation_ids.insert(activation.activation_id())
            || !stream_ids.insert(activation.author_stream_id())
            || !first_slots.insert(activation.first_slot().clone())
        {
            return Err(StoreProtocolError::Malformed(
                "stream activations repeat an activation, author stream, or first slot".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_candidate_abandonment(
    manifests: &[CandidateCleanupManifest],
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    coord: &StoreCommitCoord,
    order: &StoreCommitOrder,
    author: &StoreDeviceRegistration,
) -> Result<(), StoreProtocolError> {
    if manifests.is_empty() {
        return Err(StoreProtocolError::Malformed(
            "candidate abandonment has no candidates".to_string(),
        ));
    }
    if manifests.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::Malformed(
            "candidate abandonment manifests are not strictly sorted and unique".to_string(),
        ));
    }
    for manifest in manifests {
        if &manifest.candidate.coord != coord {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate occupies a different competition point".to_string(),
            ));
        }
        let candidate = manifest
            .candidate
            .verify_candidate(store_root_hash, author)?;
        if &candidate.author_registration != author_registration {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate has a different author registration".to_string(),
            ));
        }
        let shares_predecessor = candidate.order.predecessor == order.predecessor;
        if !shares_predecessor {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate has a different predecessor".to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) fn candidate_manifest(
    family: CandidateFamilyId,
    body: &StoreCommitBody,
) -> Result<CandidateObjectManifest, StoreProtocolError> {
    let mut objects = Vec::new();
    match body {
        StoreCommitBody::Operations(operations) => {
            objects.extend(
                operations
                    .store_package
                    .iter()
                    .cloned()
                    .map(CandidateExclusiveObjectRef::StorePackage),
            );
            objects.extend(
                operations
                    .circle_packages
                    .iter()
                    .cloned()
                    .map(CandidateExclusiveObjectRef::CirclePackage),
            );
            for control in &operations.circle_controls {
                let circle_id = control.circle_id();
                if let Some(reference) = &control.objects().close_intent {
                    objects.push(CandidateExclusiveObjectRef::CircleEpochCloseIntent {
                        circle_id,
                        reference: reference.clone(),
                    });
                }
                if let Some(reference) = &control.objects().close_outcome {
                    objects.push(CandidateExclusiveObjectRef::CircleEpochCloseOutcome {
                        circle_id,
                        reference: reference.clone(),
                    });
                }
                if let Some(reference) = &control.objects().close_cancellation {
                    objects.push(CandidateExclusiveObjectRef::CircleEpochCloseCancellation {
                        circle_id,
                        reference: reference.clone(),
                    });
                }
                if control
                    .objects()
                    .access
                    .iter()
                    .any(|access| access.envelope.control_hash != control.control().control_hash())
                {
                    return Err(StoreProtocolError::Malformed(
                        "Circle access envelope differs from its activating control".to_string(),
                    ));
                }
                objects.extend(
                    control.objects().access.iter().cloned().map(|access| {
                        CandidateExclusiveObjectRef::CircleAccess { circle_id, access }
                    }),
                );
            }
        }
        StoreCommitBody::ReclaimAuthorization { .. } => {}
        StoreCommitBody::ReclaimReceipt { .. } => {}
        StoreCommitBody::OwnerPromotionRequest { .. }
        | StoreCommitBody::AbandonCandidates { .. } => {}
    }
    objects.sort_by_cached_key(|object| {
        serde_json::to_vec(object).expect("candidate object serialization cannot fail")
    });
    let mut exact_refs = BTreeSet::new();
    let mut access_keys = BTreeSet::new();
    for object in &objects {
        validate_candidate_object_path(family, object)?;
        match object {
            CandidateExclusiveObjectRef::CircleAccess { circle_id, access } => {
                let key = (
                    *circle_id,
                    access.leaf.owner_pubkey.clone(),
                    access.leaf.recipient_slot.clone(),
                    access.envelope.control_hash,
                );
                if !access_keys.insert(key) {
                    return Err(StoreProtocolError::Malformed(
                        "candidate object manifest repeats a Circle access semantic key"
                            .to_string(),
                    ));
                }
                insert_candidate_exact_ref(&mut exact_refs, &access.leaf.object)?;
                insert_candidate_exact_ref(&mut exact_refs, &access.envelope.object)?;
            }
            CandidateExclusiveObjectRef::CircleEpochCloseIntent { reference, .. } => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::CircleEpochCloseOutcome { reference, .. } => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::CircleEpochCloseCancellation { reference, .. } => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::StorePackage(reference) => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::CirclePackage(reference) => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.package.object)?;
            }
        }
    }
    Ok(CandidateObjectManifest { family, objects })
}

fn insert_candidate_exact_ref<'a>(
    exact_refs: &mut BTreeSet<&'a ExactObjectRef>,
    object: &'a ExactObjectRef,
) -> Result<(), StoreProtocolError> {
    if !exact_refs.insert(object) {
        return Err(StoreProtocolError::Malformed(
            "candidate object manifest repeats an exact object reference".to_string(),
        ));
    }
    Ok(())
}

fn validate_candidate_object_path(
    family: CandidateFamilyId,
    candidate: &CandidateExclusiveObjectRef,
) -> Result<(), StoreProtocolError> {
    match candidate {
        CandidateExclusiveObjectRef::StorePackage(reference) => {
            if reference.candidate_family != family {
                return Err(StoreProtocolError::Malformed(
                    "Store package candidate family differs from its manifest".to_string(),
                ));
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CirclePackage(reference) => {
            if reference.package.candidate_family != family {
                return Err(StoreProtocolError::Malformed(
                    "Circle package candidate family differs from its manifest".to_string(),
                ));
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleAccess { circle_id, access } => {
            validate_circle_access_ref(*circle_id, family, access)?;
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleEpochCloseIntent {
            circle_id,
            reference,
        } => {
            let expected = format!(
                "{}.json",
                crate::protocol::circle::circle_epoch_close_intent_semantic_prefix(
                    *circle_id,
                    reference.close_id,
                    reference.intent_hash,
                )
            );
            if reference.object.slot().logical_key() != expected {
                return Err(StoreProtocolError::RelocatedCandidateObject {
                    expected,
                    actual: reference.object.slot().logical_key().to_string(),
                });
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleEpochCloseOutcome {
            circle_id,
            reference,
        } => {
            let expected = format!(
                "{}.json",
                crate::protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                    *circle_id,
                    reference.close_id,
                )
            );
            if reference.object.slot().logical_key() != expected {
                return Err(StoreProtocolError::RelocatedCandidateObject {
                    expected,
                    actual: reference.object.slot().logical_key().to_string(),
                });
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleEpochCloseCancellation {
            circle_id,
            reference,
        } => {
            let expected = format!(
                "{}.json",
                crate::protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                    *circle_id,
                    reference.close_id,
                )
            );
            if reference.object.slot().logical_key() != expected {
                return Err(StoreProtocolError::RelocatedCandidateObject {
                    expected,
                    actual: reference.object.slot().logical_key().to_string(),
                });
            }
            Ok(())
        }
    }
}

fn validate_circle_access_ref(
    circle_id: CircleId,
    family: CandidateFamilyId,
    access: &CircleAccessObjectRef,
) -> Result<(), StoreProtocolError> {
    if access.leaf.owner_pubkey != access.envelope.owner_pubkey
        || access.leaf.recipient_slot != access.envelope.recipient_slot
        || access.leaf.leaf_id != access.envelope.leaf_id
        || access.leaf.leaf_hash != access.envelope.leaf_hash
        || access.leaf.leaf_hash != access.leaf.object.stored_hash()
    {
        return Err(StoreProtocolError::Malformed(
            "paired Circle access leaf and envelope references differ".to_string(),
        ));
    }
    let leaf_expected = circle_access_leaf_semantic_prefix(
        circle_id,
        family,
        &access.leaf.owner_pubkey,
        access.leaf.epoch_id,
        &access.leaf.recipient_slot,
        access.leaf.leaf_id,
    );
    if access.leaf.object.slot().logical_key() != leaf_expected {
        return Err(StoreProtocolError::RelocatedCandidateObject {
            expected: leaf_expected,
            actual: access.leaf.object.slot().logical_key().to_string(),
        });
    }
    let envelope_expected = format!(
        "{}.json",
        circle_access_envelope_semantic_prefix(
            circle_id,
            family,
            &access.envelope.owner_pubkey,
            &access.envelope.recipient_slot,
            access.envelope.control_hash,
        )
    );
    if access.envelope.object.slot().logical_key() != envelope_expected {
        return Err(StoreProtocolError::RelocatedCandidateObject {
            expected: envelope_expected,
            actual: access.envelope.object.slot().logical_key().to_string(),
        });
    }
    Ok(())
}

fn package_ref(
    semantic_prefix: &str,
    input: &StorePackageInput<'_>,
) -> Result<StorePackageRef, StoreProtocolError> {
    let package_bytes = input.bytes;
    let changeset_size =
        u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
    let content_hash = ObjectHash::digest(package_bytes);
    let expected_key = format!("{semantic_prefix}.pkg");
    if input.object.slot().logical_key() != expected_key {
        return Err(StoreProtocolError::RelocatedPackage {
            expected: expected_key,
            actual: input.object.slot().logical_key().to_string(),
        });
    }
    Ok(StorePackageRef {
        candidate_family: input.candidate_family,
        content_hash,
        schema_version: input.schema_version,
        changeset_size,
        object: input.object.clone(),
    })
}

fn verify_package_ref(
    package: &StorePackageRef,
    package_bytes: &[u8],
) -> Result<(), StoreProtocolError> {
    let length =
        u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
    if length != package.changeset_size {
        return Err(StoreProtocolError::PackageLengthMismatch {
            expected: package.changeset_size,
            actual: length,
        });
    }
    let actual = ObjectHash::digest(package_bytes);
    if actual != package.content_hash {
        return Err(StoreProtocolError::PackageHashMismatch {
            expected: package.content_hash,
            actual,
        });
    }
    Ok(())
}

fn validate_control(
    author_registration: &StoreDeviceRegistrationRef,
    author_pubkey: &str,
    _membership_state: &StoreMembershipStateRef,
    control: Option<&StoreControl>,
) -> Result<(), StoreProtocolError> {
    let Some(control) = control else {
        return Ok(());
    };
    let transition = &control.transition;
    if transition.body.author_registration != *author_registration
        || transition.body.entry.coord.author_pubkey != author_pubkey
        || transition.body.entry.coord.seq == 0
    {
        return Err(StoreProtocolError::InvalidMergeMembershipControl);
    }
    Ok(())
}

fn validate_parsed_control(
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> Result<(), StoreProtocolError> {
    validate_control(
        &commit.author_registration,
        &author.author_pubkey,
        &commit.membership_state,
        commit.control(),
    )
}

fn validate_circle_control_coord(coord: &CircleControlCoord) -> Result<(), StoreProtocolError> {
    coord
        .validate()
        .map_err(|_| StoreProtocolError::InvalidCircleControlCoord)?;
    Ok(())
}

fn validate_circle_control_refs(controls: &[CircleControlRef]) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for control_ref in controls {
        if !seen.insert(control_ref.circle_id()) {
            return Err(StoreProtocolError::DuplicateCircleControl(
                control_ref.circle_id(),
            ));
        }
        validate_circle_control_coord(control_ref.control())?;
    }
    Ok(())
}
