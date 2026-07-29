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
        registration: Option<Box<DeviceJoinRegistrationActivation>>,
    },
    CleanupReceipt(super::device_join::DeviceJoinCleanupReceiptRef),
    DeviceExclusionProposal(super::store_commit::RetainedStoreDeviceExclusionProposal),
    DeviceExclusionOutcome(super::store_commit::RetainedStoreDeviceExclusionOutcome),
    ReclaimAuthorization(Box<super::store_reclaim::ReclaimAuthorizationRef>),
    ReclaimReceipt(Box<super::store_reclaim::ReclaimReceiptRef>),
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeviceJoinRegistrationActivation {
    pub reference: ActivatedStoreDeviceRegistrationRef,
    pub registration: StoreDeviceRegistration,
    pub authority: super::store_commit::StoreDeviceRegistrationActivation,
}

pub(crate) struct StoreOperationPlanCommon {
    /// This device's turn to author its own next Store commit, taken when the
    /// position this plan's order extends was read. A plan is the live claim on
    /// that position: hold it until the commit has published its head, or until
    /// the candidate is durably persisted for a later publisher to activate.
    pub(super) _authorship: super::super::database::OwnStreamAuthorship,
    pub(super) root: StoreRootRef,
    pub(super) registration_ref: StoreDeviceRegistrationRef,
    pub(super) registration: Box<StoreDeviceRegistration>,
    pub(super) device_signer: UserKeypair,
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
        authorship: super::super::database::OwnStreamAuthorship,
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

    pub(crate) fn device_signer(&self) -> &UserKeypair {
        &self.device_signer
    }

    pub(crate) fn owner_grant(&self) -> Option<&super::membership::MembershipGrantId> {
        self.owner_grant.as_ref()
    }
}
