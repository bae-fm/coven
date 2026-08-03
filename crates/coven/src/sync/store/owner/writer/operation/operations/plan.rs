use super::*;

pub(crate) enum StoreOperationBatch {
    Acknowledgement {
        reference: super::store_commit::StoreAckRef,
        value: super::store_commit::StoreAck,
        circle_acknowledgements: Vec<CircleAckActivation>,
    },

    ProviderAccessGrant(super::provider::StoreMemberProviderAccessGrantRef),
    Attempt(DeviceJoinAttemptRef),
    Abandonment(super::device_join::DeviceJoinAbandonmentRef),
    Outcome {
        outcome: DeviceJoinOutcomeRef,
        registration: Option<Box<ActivatedStoreDeviceRegistration>>,
    },
    CleanupReceipt(super::device_join::DeviceJoinCleanupReceiptRef),
    DeviceExclusionProposal(super::store_commit::RetainedStoreDeviceExclusionProposal),
    DeviceExclusionOutcome(super::store_commit::RetainedStoreDeviceExclusionOutcome),
    ReclaimAuthorization(Box<crate::protocol::reclaim::ReclaimAuthorizationRef>),
    ReclaimReceipt(Box<crate::protocol::reclaim::ReclaimReceiptRef>),
    OwnerPromotionRequest(super::store_commit::OwnerPromotionRequest),
    MergeMembershipActivation {
        transition: super::membership::MergeMembershipHeadTransition,
        stream_activations: Vec<super::store_commit::StreamActivation>,
    },
}

/// One Circle acknowledgement object riding an activating Store commit: its
/// exact reference (named in the signed commit body) and the exact object the
/// commit uploads and takes ownership of.
#[derive(Debug, Clone)]
pub(crate) struct CircleAckActivation {
    pub reference: super::store_commit::CircleAckRef,
    pub ack: crate::database::ExactProtocolObject<super::store_commit::CircleAck>,
}

pub(crate) struct StoreOperationPlanCommon {
    /// This device's turn to author its own next Store commit, taken when the
    /// position this plan's order extends was read. A plan is the live claim on
    /// that position: hold it until the commit has published its head, or until
    /// the candidate is durably persisted for a later publisher to activate.
    pub(super) _authorship: crate::database::OwnStreamAuthorship,
    pub(super) root: StoreRootRef,
    pub(super) registration_ref: StoreDeviceRegistrationRef,
    pub(super) registration: Box<StoreDeviceRegistration>,
    device_signer: UserKeypair,
    pub(super) coord: StoreCommitCoord,
    pub(super) order: StoreCommitOrder,
    pub(super) membership_state: super::circle_control::StoreMembershipStateRef,
    pub(super) device_state: super::store_commit::StoreDeviceStateRef,
    pub(super) membership_authority: StoreOperationMembershipAuthority,
    pub(super) owner_grant: Option<super::membership::MembershipGrantId>,
}

pub(crate) struct StoreOperationCommitPlan {
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
        authorship: crate::database::OwnStreamAuthorship,
        root: StoreRootRef,
        registration_ref: StoreDeviceRegistrationRef,
        registration: StoreDeviceRegistration,
        device_signer: UserKeypair,
        coord: StoreCommitCoord,
        order: StoreCommitOrder,
        membership_state: super::circle_control::StoreMembershipStateRef,
        device_state: super::store_commit::StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        owner_grant: Option<super::membership::MembershipGrantId>,
    ) -> Self {
        Self {
            _authorship: authorship,
            root,
            registration_ref,
            registration: Box::new(registration),
            device_signer,
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
        if acknowledgement.registration != self.registration_ref
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
        write_id: crate::WriteId,
        batch: StoreOperationBatch,
    ) -> Result<(StoreBatchCommit, Option<ActivatedStoreDeviceRegistration>), StoreError> {
        let registration_activation = match &batch {
            StoreOperationBatch::Outcome { registration, .. } => registration.as_deref().cloned(),
            _ => None,
        };
        let commit = match batch {
            StoreOperationBatch::Acknowledgement {
                reference: acknowledgement,
                value: _,
                circle_acknowledgements,
            } => StoreBatchCommit::signed_operations(
                self.root.store_root_hash,
                write_id,
                self.coord.clone(),
                self.registration_ref.clone(),
                &self.registration,
                self.order.clone(),
                self.membership_state.clone(),
                self.device_state.clone(),
                self.membership_authority.clone(),
                StoreCommitOperationsInput {
                    acknowledgement: Some(acknowledgement),
                    circle_acknowledgements: circle_acknowledgements
                        .iter()
                        .map(|circle| circle.reference.clone())
                        .collect(),
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
                    store_package: None,
                    circle_packages: &[],
                },
                &self.device_signer,
            ),
            StoreOperationBatch::ProviderAccessGrant(grant) => {
                StoreBatchCommit::signed_with_provider_access(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    self.membership_authority.clone(),
                    vec![grant],
                    &self.device_signer,
                )
            }
            StoreOperationBatch::Attempt(attempt) => StoreBatchCommit::signed_with_join_attempts(
                self.root.store_root_hash,
                write_id,
                self.coord.clone(),
                self.registration_ref.clone(),
                &self.registration,
                self.order.clone(),
                self.membership_state.clone(),
                self.device_state.clone(),
                self.membership_authority.clone(),
                vec![attempt],
                &self.device_signer,
            ),
            StoreOperationBatch::Abandonment(abandonment) => {
                StoreBatchCommit::signed_with_join_abandonments(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    self.membership_authority.clone(),
                    vec![abandonment],
                    &self.device_signer,
                )
            }
            StoreOperationBatch::Outcome {
                outcome,
                registration,
            } => StoreBatchCommit::signed_with_join_outcomes(
                self.root.store_root_hash,
                write_id,
                self.coord.clone(),
                self.registration_ref.clone(),
                &self.registration,
                self.order.clone(),
                self.membership_state.clone(),
                self.device_state.clone(),
                self.membership_authority.clone(),
                vec![outcome],
                registration
                    .into_iter()
                    .map(|activation| {
                        activation
                            .activated_reference()
                            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                &self.device_signer,
            ),
            StoreOperationBatch::CleanupReceipt(receipt) => {
                StoreBatchCommit::signed_with_join_cleanup_receipts(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    self.membership_authority.clone(),
                    vec![receipt],
                    &self.device_signer,
                )
            }
            StoreOperationBatch::DeviceExclusionProposal(proposal) => {
                StoreBatchCommit::signed_with_device_exclusions(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    self.membership_authority.clone(),
                    vec![proposal.reference().clone()],
                    Vec::new(),
                    &self.device_signer,
                )
            }
            StoreOperationBatch::DeviceExclusionOutcome(outcome) => {
                StoreBatchCommit::signed_with_device_exclusions(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    self.membership_authority.clone(),
                    Vec::new(),
                    vec![outcome.wire_reference()],
                    &self.device_signer,
                )
            }
            StoreOperationBatch::ReclaimAuthorization(authorization) => {
                StoreBatchCommit::signed_reclaim_authorization(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    *authorization,
                    &self.device_signer,
                )
            }
            StoreOperationBatch::ReclaimReceipt(receipt) => {
                StoreBatchCommit::signed_reclaim_receipt(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    *receipt,
                    &self.device_signer,
                )
            }
            StoreOperationBatch::OwnerPromotionRequest(request) => {
                StoreBatchCommit::signed_with_owner_promotion_request(
                    self.root.store_root_hash,
                    write_id,
                    self.coord.clone(),
                    self.registration_ref.clone(),
                    &self.registration,
                    self.order.clone(),
                    self.membership_state.clone(),
                    self.device_state.clone(),
                    self.membership_authority.clone(),
                    request,
                    &self.device_signer,
                )
            }
            StoreOperationBatch::MergeMembershipActivation {
                transition,
                stream_activations,
            } => StoreBatchCommit::signed_operations(
                self.root.store_root_hash,
                write_id,
                self.coord.clone(),
                self.registration_ref.clone(),
                &self.registration,
                self.order.clone(),
                self.membership_state.clone(),
                self.device_state.clone(),
                self.membership_authority.clone(),
                StoreCommitOperationsInput {
                    acknowledgement: None,
                    circle_acknowledgements: Vec::new(),
                    control: Some(StoreControl { transition }),
                    device_join_attempt_decisions: Vec::new(),
                    device_join_outcomes: Vec::new(),
                    device_join_cleanup_receipts: Vec::new(),
                    provider_access_grants: Vec::new(),
                    device_registrations: Vec::new(),
                    device_exclusion_proposals: Vec::new(),
                    device_exclusion_outcomes: Vec::new(),
                    stream_activations,
                    circle_controls: Vec::new(),
                    store_package: None,
                    circle_packages: &[],
                },
                &self.device_signer,
            ),
        }
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok((commit, registration_activation))
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
        write_id: crate::WriteId,
        batch: StoreOperationBatch,
    ) -> Result<(StoreBatchCommit, Option<ActivatedStoreDeviceRegistration>), StoreError> {
        self.common.sign_batch(write_id, batch)
    }
}

impl StoreOperationCommitPlan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_owner_promotion_request(
        &self,
        promotion_id: super::store_commit::OwnerPromotionId,
        member_registration: super::store_commit::StoreDeviceRegistrationRef,
        member_pubkey: String,
        member_grant: super::membership::MembershipGrantId,
        finalization: super::store_commit::OwnerPromotionFinalization,
        identity_signer: &UserKeypair,
    ) -> Result<super::store_commit::OwnerPromotionRequest, StoreError> {
        let promoter_owner_grant = self.owner_grant.clone().ok_or_else(|| {
            StoreError::InvalidOutbound(
                "Owner-promotion request author has no active Owner grant".to_string(),
            )
        })?;
        super::store_commit::OwnerPromotionRequest::signed(
            promotion_id,
            &self.root,
            self.registration_ref.clone(),
            &self.registration,
            promoter_owner_grant,
            member_pubkey,
            member_grant,
            member_registration,
            self.membership_state.clone(),
            self.device_state.clone(),
            finalization,
            identity_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }
}

impl StoreOperationCommitPlan {
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

    pub(crate) fn registration_ref(&self) -> &StoreDeviceRegistrationRef {
        &self.registration_ref
    }

    pub(crate) fn registration(&self) -> &StoreDeviceRegistration {
        &self.registration
    }

    pub(crate) fn coord(&self) -> &StoreCommitCoord {
        &self.coord
    }

    pub(crate) fn owner_grant(&self) -> Option<&super::membership::MembershipGrantId> {
        self.owner_grant.as_ref()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign_device_exclusion_proposal(
        &self,
        proposal_id: super::store_commit::StoreDeviceExclusionProposalId,
        target: super::store_commit::StoreDeviceRegistrationRef,
        target_registration: &super::store_commit::StoreDeviceRegistration,
        outcome_slot: crate::storage::cloud::ObjectSlot,
        owner_grant: super::membership::MembershipGrantId,
    ) -> Result<super::store_commit::StoreDeviceExclusionProposal, StoreError> {
        super::store_commit::StoreDeviceExclusionProposal::signed(
            self.root.store_root_hash,
            proposal_id,
            target,
            target_registration,
            self.device_state.clone(),
            outcome_slot,
            self.registration_ref.clone(),
            owner_grant,
            &self.registration,
            &self.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_device_exclusion_cancellation(
        &self,
        proposal: super::store_commit::StoreDeviceExclusionProposalRef,
        proposal_value: &super::store_commit::StoreDeviceExclusionProposal,
        owner_grant: super::membership::MembershipGrantId,
    ) -> Result<super::store_commit::StoreDeviceExclusionCancellation, StoreError> {
        super::store_commit::StoreDeviceExclusionCancellation::signed(
            proposal,
            proposal_value,
            self.registration_ref.clone(),
            owner_grant,
            &self.registration,
            &self.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
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
        super::store_commit::StoreDeviceExclusion::signed(
            proposal,
            proposal_value,
            target,
            target_registration,
            proof,
            self.registration_ref.clone(),
            owner_grant,
            &self.registration,
            &self.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_device_head(
        &self,
        commit: super::store_commit::StoreBatchCommitRef,
        history_summary: ObjectHash,
        successor: super::store_commit::SuccessorLink,
    ) -> Result<super::store_commit::StoreDeviceHead, StoreError> {
        super::store_commit::StoreDeviceHead::signed(
            self.root.store_root_hash,
            self.registration_ref.clone(),
            commit,
            history_summary,
            successor,
            &self.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }

    pub(crate) fn sign_reclaim_receipt(
        &self,
        authorization: crate::protocol::reclaim::ReclaimAuthorizationRef,
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    ) -> Result<crate::protocol::reclaim::ReclaimReceipt, StoreError> {
        crate::protocol::reclaim::ReclaimReceipt::signed(
            self.root.store_root_hash,
            authorization,
            self.membership_state.clone(),
            provider_admin_grant,
            self.registration_ref.clone(),
            &self.registration,
            &self.device_signer,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))
    }
}
