use super::validation::require_version;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePackageRef {
    pub candidate_family: CandidateFamilyId,
    pub content_hash: ObjectHash,
    pub schema_version: u32,
    pub changeset_size: u64,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CirclePackageRef {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub package: StorePackageRef,
    pub key_fingerprint: KeyFingerprint,
}

/// Exact recipient-visible access envelope paired with its sealed leaf.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessEnvelopeObjectRef {
    pub owner_pubkey: String,
    pub recipient_slot: String,
    pub control_hash: ObjectHash,
    pub leaf_id: AccessLeafId,
    pub leaf_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// Exact recipient-sealed access-leaf object named by a Store activation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessLeafObjectRef {
    pub owner_pubkey: String,
    pub epoch_id: CircleEpochId,
    pub recipient_slot: String,
    pub leaf_id: AccessLeafId,
    pub leaf_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessObjectRef {
    pub leaf: CircleAccessLeafObjectRef,
    pub envelope: CircleAccessEnvelopeObjectRef,
}

/// Exact Circle-metadata object and the epoch key that must open it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataObjectRef {
    pub key_fingerprint: KeyFingerprint,
    pub object: ExactObjectRef,
}

/// Closed exact object graph needed to verify one Store-activated Circle control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleActivationObjects {
    pub control: ExactObjectRef,
    #[serde(with = "ordered_map_entries")]
    pub roster_entries: BTreeMap<CircleRosterCoord, ExactObjectRef>,
    pub roster_heads: Vec<CircleRosterHeadRef>,
    #[serde(with = "ordered_map_entries")]
    pub roster_resolutions: BTreeMap<CircleRosterConflictResolutionRef, ExactObjectRef>,
    #[serde(with = "ordered_map_entries")]
    pub metadata_entries: BTreeMap<CircleMetadataCoord, CircleMetadataObjectRef>,
    pub metadata_heads: Vec<CircleMetadataHeadRef>,
    pub access: Vec<CircleAccessObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlRef {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub head_hash: ObjectHash,
    pub head_object: ExactObjectRef,
    pub objects: CircleActivationObjects,
}

impl CircleControlRef {
    pub fn circle_id(&self) -> CircleId {
        self.circle_id
    }

    pub fn control(&self) -> &CircleControlCoord {
        &self.control
    }

    pub fn head_hash(&self) -> ObjectHash {
        self.head_hash
    }

    pub fn head_object(&self) -> &ExactObjectRef {
        &self.head_object
    }

    pub fn objects(&self) -> &CircleActivationObjects {
        &self.objects
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRegistrationRef {
    pub device_id: StoreDeviceId,
    pub registration_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl StoreDeviceRegistrationRef {
    pub fn from_registration(
        registration: &StoreDeviceRegistration,
        object: ExactObjectRef,
    ) -> Self {
        Self {
            device_id: registration.device_id,
            registration_hash: registration.registration_hash(),
            object,
        }
    }

    pub fn verify_registration(
        &self,
        registration: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        if registration.device_id != self.device_id
            || registration.registration_hash() != self.registration_hash
        {
            return Err(StoreProtocolError::DeviceRegistrationRefMismatch {
                device_id: self.device_id.to_string(),
                revision: 1,
                expected: self.registration_hash,
                actual: registration.registration_hash(),
            });
        }
        Ok(())
    }
}

pub struct CirclePackageInput<'a> {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub key_fingerprint: KeyFingerprint,
    pub package: StorePackageInput<'a>,
}

pub struct StorePackageInput<'a> {
    pub candidate_family: CandidateFamilyId,
    pub schema_version: u32,
    pub bytes: &'a [u8],
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreControl {
    pub transition: crate::sync::membership::MergeMembershipHeadTransition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OwnerPromotionId(ObjectHash);

impl OwnerPromotionId {
    pub fn from_generated(value: String) -> Self {
        Self(ObjectHash::digest(
            &[
                b"coven.owner-promotion-id.v1\0".as_slice(),
                value.as_bytes(),
            ]
            .concat(),
        ))
    }
}

impl fmt::Display for OwnerPromotionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionFinalization {
    pub author_stream: AuthorStreamId,
    pub seq: u64,
    pub previous_hash: Option<ObjectHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionRequest {
    pub version: u32,
    pub promotion_id: OwnerPromotionId,
    pub store_root_hash: ObjectHash,
    pub promoter_registration: StoreDeviceRegistrationRef,
    pub promoter_owner_grant: MembershipGrantId,
    pub member_pubkey: String,
    pub member_grant: MembershipGrantId,
    pub member_registration: StoreDeviceRegistrationRef,
    pub intended_owner_grant: MembershipGrantId,
    pub predecessor_membership: StoreMembershipStateRef,
    pub predecessor_devices: StoreDeviceStateRef,
    pub finalization: OwnerPromotionFinalization,
    pub signature: String,
}

impl OwnerPromotionRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        promotion_id: OwnerPromotionId,
        root: &StoreRootRef,
        promoter_registration: StoreDeviceRegistrationRef,
        promoter: &StoreDeviceRegistration,
        promoter_owner_grant: MembershipGrantId,
        member_pubkey: String,
        member_grant: MembershipGrantId,
        member_registration: StoreDeviceRegistrationRef,
        predecessor_membership: StoreMembershipStateRef,
        predecessor_devices: StoreDeviceStateRef,
        finalization: OwnerPromotionFinalization,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let intended_owner_grant =
            derive_owner_promotion_grant(root.store_root_hash, promotion_id, &member_pubkey);
        let mut request = Self {
            version: STORE_PROTOCOL_VERSION,
            promotion_id,
            store_root_hash: root.store_root_hash,
            promoter_registration,
            promoter_owner_grant,
            member_pubkey,
            member_grant,
            member_registration,
            intended_owner_grant,
            predecessor_membership,
            predecessor_devices,
            finalization,
            signature: String::new(),
        };
        request.validate_shape(root, promoter)?;
        let device_signer = promoter.device_signer(signer)?;
        request.signature = keys::sign_hex(&device_signer, &request.canonical_bytes()).1;
        Ok(request)
    }

    pub fn verify(
        &self,
        root: &StoreRootRef,
        promoter: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.validate_shape(root, promoter)?;
        if !keys::verify_signature_hex(
            &promoter.device_signing_pubkey,
            &self.signature,
            &self.canonical_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    fn validate_shape(
        &self,
        root: &StoreRootRef,
        promoter: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        self.promoter_registration.verify_registration(promoter)?;
        if self.store_root_hash != root.store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: root.store_root_hash,
                actual: self.store_root_hash,
            });
        }
        if promoter.store_root != *root
            || promoter.author_pubkey == self.member_pubkey
            || self.member_pubkey.is_empty()
            || self.intended_owner_grant
                != derive_owner_promotion_grant(
                    self.store_root_hash,
                    self.promotion_id,
                    &self.member_pubkey,
                )
            || self.finalization.seq == 0
        {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        Ok(())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            version: u32,
            promotion_id: OwnerPromotionId,
            store_root_hash: ObjectHash,
            promoter_registration: &'a StoreDeviceRegistrationRef,
            promoter_owner_grant: &'a MembershipGrantId,
            member_pubkey: &'a str,
            member_grant: &'a MembershipGrantId,
            member_registration: &'a StoreDeviceRegistrationRef,
            intended_owner_grant: &'a MembershipGrantId,
            predecessor_membership: &'a StoreMembershipStateRef,
            predecessor_devices: &'a StoreDeviceStateRef,
            finalization: &'a OwnerPromotionFinalization,
        }
        domain_json(
            b"coven.owner-promotion-request.v1\0",
            &Signed {
                version: self.version,
                promotion_id: self.promotion_id,
                store_root_hash: self.store_root_hash,
                promoter_registration: &self.promoter_registration,
                promoter_owner_grant: &self.promoter_owner_grant,
                member_pubkey: &self.member_pubkey,
                member_grant: &self.member_grant,
                member_registration: &self.member_registration,
                intended_owner_grant: &self.intended_owner_grant,
                predecessor_membership: &self.predecessor_membership,
                predecessor_devices: &self.predecessor_devices,
                finalization: &self.finalization,
            },
        )
    }
}

pub fn derive_owner_promotion_grant(
    store_root_hash: ObjectHash,
    promotion_id: OwnerPromotionId,
    member_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(&domain_json(
        b"coven.owner-promotion-grant.v1\0",
        &(store_root_hash, promotion_id, member_pubkey),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionRequestActivation {
    pub commit: StoreBatchCommitRef,
    pub head: StoreDeviceHeadRef,
}

impl OwnerPromotionRequestActivation {
    pub fn commit(&self) -> &StoreBatchCommitRef {
        &self.commit
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionAnchors {
    pub membership: GrantStreamAnchor,
    pub recovery: GrantStreamAnchor,
}

impl OwnerPromotionAnchors {
    pub fn recovery(&self) -> &GrantStreamAnchor {
        &self.recovery
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionAcceptance {
    pub request: Box<OwnerPromotionRequest>,
    pub activation: OwnerPromotionRequestActivation,
    pub anchors: OwnerPromotionAnchors,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionStatus {
    Preparing {
        member_registration: StoreDeviceRegistrationRef,
    },
    RequestPending {
        request: OwnerPromotionRequest,
    },
    AwaitingAcceptance {
        request: OwnerPromotionRequest,
        activation: OwnerPromotionRequestActivation,
    },
    AcceptanceReady {
        acceptance: OwnerPromotionAcceptance,
    },
    FinalizationPending {
        acceptance: OwnerPromotionAcceptance,
    },
    Finalized {
        membership: StoreMembershipStateRef,
    },
    Nonactivated {
        request: OwnerPromotionRequest,
    },
    Stale {
        acceptance: OwnerPromotionAcceptance,
        reason: OwnerPromotionStaleReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionStaleReason {
    MergeFinalizationPointOccupied { winner: MembershipHeadRef },
    MergeActivationRejected,
}

impl OwnerPromotionAcceptance {
    pub fn signed(
        request: OwnerPromotionRequest,
        activation: OwnerPromotionRequestActivation,
        anchors: OwnerPromotionAnchors,
        candidate: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let mut acceptance = Self {
            request: Box::new(request),
            activation,
            anchors,
            signature: String::new(),
        };
        acceptance.validate_shape(candidate)?;
        let device_signer = candidate.device_signer(signer)?;
        acceptance.signature = keys::sign_hex(&device_signer, &acceptance.canonical_bytes()).1;
        Ok(acceptance)
    }

    pub fn verify(&self, candidate: &StoreDeviceRegistration) -> Result<(), StoreProtocolError> {
        self.validate_shape(candidate)?;
        if !keys::verify_signature_hex(
            &candidate.device_signing_pubkey,
            &self.signature,
            &self.canonical_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    fn validate_shape(
        &self,
        candidate: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.request
            .member_registration
            .verify_registration(candidate)?;
        if candidate.store_root.store_root_hash != self.request.store_root_hash
            || candidate.author_pubkey != self.request.member_pubkey
            || !matches!(
                self.anchors.recovery(),
                GrantStreamAnchor::OwnerRecovery { .. }
            )
        {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        {
            let membership = &self.anchors.membership;
            let recovery = &self.anchors.recovery;
            if !matches!(membership, GrantStreamAnchor::StoreMembership { .. }) {
                return Err(StoreProtocolError::OwnerPromotionMismatch);
            }
            let membership_stream = StreamActivation::grant_authorized_stream_id(
                self.request.store_root_hash,
                &self.request.member_registration,
                &self.request.intended_owner_grant,
                StreamAnchorDomain::StoreMembership,
            );
            let membership_key = format!(
                "{}.json",
                membership_head_slot_prefix(
                    &self.request.member_pubkey,
                    &self.request.intended_owner_grant,
                    membership_stream,
                    1,
                )
            );
            let recovery_key = format!(
                "{}.json",
                owner_recovery_semantic_prefix(
                    &self.request.member_pubkey,
                    self.request.intended_owner_grant.clone(),
                    1,
                )
            );
            if membership.first_slot().logical_key() != membership_key
                || recovery.first_slot().logical_key() != recovery_key
                || matches!(
                    (membership.first_slot().physical(), recovery.first_slot().physical()),
                    (
                        crate::storage::cloud::PhysicalObjectLocator::Opaque(left),
                        crate::storage::cloud::PhysicalObjectLocator::Opaque(right),
                    ) if left == right
                )
            {
                return Err(StoreProtocolError::OwnerPromotionMismatch);
            }
        }
        Ok(())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        domain_json(
            b"coven.owner-promotion-acceptance.v1\0",
            &(&self.request, &self.activation, &self.anchors),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerConflictResolutionAcceptance {
    pub store_root_hash: ObjectHash,
    pub owner_grant: MembershipGrantId,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub provider: ProviderDeviceBinding,
    pub membership: GrantStreamAnchor,
    pub recovery: GrantStreamAnchor,
    pub device_state: StoreDeviceStateRef,
    pub signature: String,
}

impl OwnerConflictResolutionAcceptance {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        owner_grant: MembershipGrantId,
        owner_registration: StoreDeviceRegistrationRef,
        membership: GrantStreamAnchor,
        recovery: GrantStreamAnchor,
        device_state: StoreDeviceStateRef,
        registration: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let mut acceptance = Self {
            store_root_hash,
            owner_grant,
            owner_registration,
            provider: registration.provider.clone(),
            membership,
            recovery,
            device_state,
            signature: String::new(),
        };
        acceptance.validate_shape(registration)?;
        let device_signer = registration.device_signer(signer)?;
        acceptance.signature = keys::sign_hex(&device_signer, &acceptance.canonical_bytes()).1;
        Ok(acceptance)
    }

    pub fn verify(&self, registration: &StoreDeviceRegistration) -> Result<(), StoreProtocolError> {
        self.validate_shape(registration)?;
        if !keys::verify_signature_hex(
            &registration.device_signing_pubkey,
            &self.signature,
            &self.canonical_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    fn validate_shape(
        &self,
        registration: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.owner_registration.verify_registration(registration)?;
        if registration.store_root.store_root_hash != self.store_root_hash
            || registration.provider != self.provider
            || !matches!(
                registration.store_commits,
                DeviceStreamAnchor::StoreAnnouncements { .. }
            )
            || !matches!(self.membership, GrantStreamAnchor::StoreMembership { .. })
            || !matches!(self.recovery, GrantStreamAnchor::OwnerRecovery { .. })
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        domain_json(
            b"coven.owner-conflict-resolution-acceptance.v1\0",
            &(
                self.store_root_hash,
                &self.owner_grant,
                &self.owner_registration,
                &self.provider,
                &self.membership,
                &self.recovery,
                &self.device_state,
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateObjectManifest {
    pub family: CandidateFamilyId,
    pub objects: Vec<CandidateExclusiveObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateExclusiveObjectRef {
    StorePackage(StorePackageRef),
    CirclePackage(CirclePackageRef),
    CircleAccess {
        circle_id: CircleId,
        access: CircleAccessObjectRef,
    },
    SelfRetirement(StoreDeviceSelfRetirementRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinAttemptDecisionRef {
    Attempt(DeviceJoinAttemptRef),
    Abandoned(crate::sync::device_join::DeviceJoinAbandonmentRef),
}

impl DeviceJoinAttemptDecisionRef {
    pub fn attempt_id(&self) -> DeviceJoinAttemptId {
        match self {
            Self::Attempt(reference) => reference.attempt_id,
            Self::Abandoned(reference) => reference.attempt_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCommitOperations {
    pub acknowledgement: Option<StoreAckRef>,
    pub control: Option<StoreControl>,
    pub device_join_attempt_decisions: Vec<DeviceJoinAttemptDecisionRef>,
    pub device_join_outcomes: Vec<DeviceJoinOutcomeRef>,
    pub device_join_cleanup_receipts: Vec<crate::sync::device_join::DeviceJoinCleanupReceiptRef>,
    pub provider_access_grants: Vec<crate::sync::provider::StoreMemberProviderAccessGrantRef>,
    pub provider_access_withdrawals:
        Vec<crate::sync::provider::StoreMemberProviderAccessWithdrawalReceiptRef>,
    pub device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
    pub device_exclusion_proposals: Vec<StoreDeviceExclusionProposalRef>,
    pub device_exclusion_outcomes: Vec<StoreDeviceExclusionOutcomeRef>,
    pub stream_activations: Vec<StreamActivation>,
    pub circle_controls: Vec<CircleControlRef>,
    pub store_package: Option<StorePackageRef>,
    pub circle_packages: Vec<CirclePackageRef>,
}

impl StoreCommitOperations {
    pub(super) fn is_empty(&self) -> bool {
        self.acknowledgement.is_none() && self.has_no_other_operations()
    }

    pub(crate) fn is_circle_control_activation_only(&self) -> bool {
        self.acknowledgement.is_none()
            && self.control.is_none()
            && self.device_join_attempt_decisions.is_empty()
            && self.device_join_outcomes.is_empty()
            && self.device_join_cleanup_receipts.is_empty()
            && self.provider_access_grants.is_empty()
            && self.provider_access_withdrawals.is_empty()
            && self.device_registrations.is_empty()
            && self.device_exclusion_proposals.is_empty()
            && self.device_exclusion_outcomes.is_empty()
            && self.circle_controls.len() == 1
            && self.store_package.is_none()
            && self.circle_packages.is_empty()
    }

    fn has_no_other_operations(&self) -> bool {
        self.control.is_none()
            && self.device_join_attempt_decisions.is_empty()
            && self.device_join_outcomes.is_empty()
            && self.device_join_cleanup_receipts.is_empty()
            && self.provider_access_grants.is_empty()
            && self.provider_access_withdrawals.is_empty()
            && self.device_registrations.is_empty()
            && self.device_exclusion_proposals.is_empty()
            && self.device_exclusion_outcomes.is_empty()
            && self.stream_activations.is_empty()
            && self.circle_controls.is_empty()
            && self.store_package.is_none()
            && self.circle_packages.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitBody {
    Operations(StoreCommitOperations),
    ReclaimAuthorization {
        authorization: Box<crate::sync::store_reclaim::ReclaimAuthorizationRef>,
    },
    ReclaimReceipt {
        receipt: Box<crate::sync::store_reclaim::ReclaimReceiptRef>,
    },
    SelfRetirement {
        retirement: StoreDeviceSelfRetirementRef,
    },
    OwnerPromotionRequest {
        request: Box<OwnerPromotionRequest>,
    },
    AbandonCandidates {
        manifests: Vec<CandidateCleanupManifest>,
    },
}

pub struct StoreCommitOperationsInput<'a> {
    pub acknowledgement: Option<StoreAckRef>,
    pub control: Option<StoreControl>,
    pub device_join_attempt_decisions: Vec<DeviceJoinAttemptDecisionRef>,
    pub device_join_outcomes: Vec<DeviceJoinOutcomeRef>,
    pub device_join_cleanup_receipts: Vec<crate::sync::device_join::DeviceJoinCleanupReceiptRef>,
    pub provider_access_grants: Vec<crate::sync::provider::StoreMemberProviderAccessGrantRef>,
    pub provider_access_withdrawals:
        Vec<crate::sync::provider::StoreMemberProviderAccessWithdrawalReceiptRef>,
    pub device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
    pub device_exclusion_proposals: Vec<StoreDeviceExclusionProposalRef>,
    pub device_exclusion_outcomes: Vec<StoreDeviceExclusionOutcomeRef>,
    pub stream_activations: Vec<StreamActivation>,
    pub circle_controls: Vec<CircleControlRef>,
    pub store_package: Option<StorePackageInput<'a>>,
    pub circle_packages: &'a [CirclePackageInput<'a>],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreOperationMembershipAuthority {
    pub predecessor: MembershipGrantCreationAuthority,
}

impl StoreOperationMembershipAuthority {
    pub(super) fn into_commit_authority(self) -> MembershipGrantCreationAuthority {
        self.predecessor
    }
}
