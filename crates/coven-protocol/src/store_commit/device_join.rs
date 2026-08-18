use super::*;

const STORE_DEVICE_ID_DOMAIN: &[u8] = b"coven.store-device-id.v1\0";

/// The stable identity of one device in a Store, derived from the Store root and
/// the device's registration origin. It names a device across the protocol — in
/// membership, commit authorship, and epoch-close participation — and is what
/// `Circles::exclude_close_device` and `Circles::close_status` address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreDeviceId(ObjectHash);

impl StoreDeviceId {
    pub fn derive(store_root: &StoreRootRef, origin: &StoreDeviceRegistrationOrigin) -> Self {
        let mut material = STORE_DEVICE_ID_DOMAIN.to_vec();
        material.extend(
            serde_json::to_vec(&(store_root, origin.external_id()))
                .expect("Store device identity serialization cannot fail"),
        );
        Self(ObjectHash::digest(&material))
    }
}

impl fmt::Display for StoreDeviceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl FromStr for StoreDeviceId {
    type Err = StoreProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.parse()?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreCreationId(ObjectHash);

impl StoreCreationId {
    pub fn from_random_bytes(bytes: [u8; 32]) -> Self {
        Self(ObjectHash::from_digest(bytes))
    }

    pub(super) fn object_hash(self) -> ObjectHash {
        self.0
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_nonce(nonce: &str) -> Self {
        Self(ObjectHash::digest(nonce.as_bytes()))
    }
}

impl fmt::Display for StoreCreationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceJoinAttemptId(ObjectHash);

impl DeviceJoinAttemptId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }

    pub(super) fn object_hash(self) -> ObjectHash {
        self.0
    }
}

impl fmt::Display for DeviceJoinAttemptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceRecoveryId(ObjectHash);

impl DeviceRecoveryId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }

    pub(super) fn object_hash(self) -> ObjectHash {
        self.0
    }
}

impl fmt::Display for DeviceRecoveryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAttemptRef {
    pub attempt_id: DeviceJoinAttemptId,
    pub attempt_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// The wire body of a device-join attempt. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAttemptBody {
    pub store_root: StoreRootRef,
    pub attempt_id: DeviceJoinAttemptId,
    pub attempt_slot: ObjectSlot,
    pub expected_registration: StoreDeviceRegistration,
    pub registration_slot: ObjectSlot,
    pub outcome_slot: ObjectSlot,
    pub bootstrap_cut: StoreHistoryCut,
    pub membership: StoreMembershipStateRef,
    pub provider_admin_grant: crate::provider::ProviderAdminGrantId,
    pub provider_approval:
        crate::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
    pub provider_response:
        crate::store_commit::device_join_exchange::DeviceProviderResponseReservation,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for DeviceJoinAttemptBody {
    const DOMAIN: &'static [u8] = DEVICE_JOIN_ATTEMPT_DOMAIN;
}

pub type DeviceJoinAttempt = Signed<DeviceJoinAttemptBody>;

pub(crate) struct UnverifiedDeviceJoinAttempt(DeviceJoinAttempt);

impl UnverifiedDeviceJoinAttempt {
    pub(crate) fn verify_at(
        self,
        expected: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<DeviceJoinAttempt, StoreProtocolError> {
        let attempt = self.0;
        attempt.body().validate_shape()?;
        if attempt.attempt_id != expected.attempt_id
            || attempt.attempt_hash() != expected.attempt_hash
            || &attempt.attempt_slot != expected.object.slot()
        {
            return Err(StoreProtocolError::JoinAttemptMismatch);
        }
        attempt.owner_registration.verify_registration(owner)?;
        attempt.verify_by(&owner.device_signing_pubkey)?;
        Ok(attempt)
    }
}

impl DeviceJoinAttempt {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root: StoreRootRef,
        attempt_id: DeviceJoinAttemptId,
        attempt_slot: ObjectSlot,
        expected_registration: StoreDeviceRegistration,
        registration_slot: ObjectSlot,
        outcome_slot: ObjectSlot,
        bootstrap_cut: StoreHistoryCut,
        membership: StoreMembershipStateRef,
        provider_admin_grant: crate::provider::ProviderAdminGrantId,
        provider_approval: crate::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        provider_response: crate::store_commit::device_join_exchange::DeviceProviderResponseReservation,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let body = DeviceJoinAttemptBody {
            store_root,
            attempt_id,
            attempt_slot,
            expected_registration,
            registration_slot,
            outcome_slot,
            bootstrap_cut,
            membership,
            provider_admin_grant,
            provider_approval,
            provider_response,
            owner_registration,
            owner_grant,
        };
        body.validate_shape()?;
        Ok(Signed::sign(body, owner_device_signer))
    }

    pub fn attempt_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        expected.object.verify(bytes)?;
        Self::parse_unverified(bytes)?.verify_at(expected, owner)
    }

    pub(crate) fn parse_unverified(
        bytes: &[u8],
    ) -> Result<UnverifiedDeviceJoinAttempt, StoreProtocolError> {
        serde_json::from_slice(bytes)
            .map(UnverifiedDeviceJoinAttempt)
            .map_err(StoreProtocolError::from)
    }
}

impl DeviceJoinAttemptBody {
    fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        validate_store_history_cut(&self.bootstrap_cut)?;
        if self.expected_registration.store_root != self.store_root
            || self.expected_registration.device_id
                != StoreDeviceId::derive(&self.store_root, &self.expected_registration.origin)
            || self.attempt_slot == self.registration_slot
            || self.attempt_slot == self.outcome_slot
            || self.registration_slot == self.outcome_slot
            || self.provider_admin_grant
                != self.provider_approval.request.offer.provider_admin.grant_id
            || self.provider_approval.request.offer.store_root != self.store_root
            || self.provider_approval.request.offer.attempt_id != self.attempt_id
            || self.provider_approval.request.offer.attempt_slot != self.attempt_slot
            || self.provider_approval.request.offer.outcome_slot != self.outcome_slot
            || self.provider_approval.request.offer.owner_registration != self.owner_registration
            || self.provider_approval.request.offer.owner_grant != self.owner_grant
            || self.provider_approval.request.offer.member_pubkey
                != self.expected_registration.author_pubkey
            || self.provider_approval.request.peer_provider != self.expected_registration.provider
        {
            return Err(StoreProtocolError::JoinAttemptMismatch);
        }
        match (&self.provider_approval.admission, &self.provider_response) {
            (
                crate::store_commit::device_join_exchange::DeviceProviderAdmission::SamePrincipal,
                crate::store_commit::device_join_exchange::DeviceProviderResponseReservation::SamePrincipal,
            )
            | (
                crate::store_commit::device_join_exchange::DeviceProviderAdmission::CrossPrincipal { .. },
                crate::store_commit::device_join_exchange::DeviceProviderResponseReservation::CrossPrincipal { .. },
            ) => {}
            _ => return Err(StoreProtocolError::JoinAttemptMismatch),
        }
        match &self.expected_registration.origin {
            StoreDeviceRegistrationOrigin::Join {
                attempt_id,
                attempt_slot,
                outcome_slot,
            } if *attempt_id == self.attempt_id
                && attempt_slot == &self.attempt_slot
                && outcome_slot == &self.outcome_slot =>
            {
                Ok(())
            }
            _ => Err(StoreProtocolError::JoinAttemptMismatch),
        }
    }
}

/// The wire body of a joining device's readiness proof. Every field here is
/// signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceReadinessProofBody {
    pub store_root_hash: ObjectHash,
    pub attempt: DeviceJoinAttemptRef,
    pub registration: StoreDeviceRegistrationRef,
    pub initial_ack: StoreAckRef,
    pub bootstrap_cut: StoreHistoryCut,
}

impl SignedBody for DeviceReadinessProofBody {
    const DOMAIN: &'static [u8] = DEVICE_READINESS_DOMAIN;
}

pub type DeviceReadinessProof = Signed<DeviceReadinessProofBody>;

impl DeviceReadinessProof {
    pub fn signed(
        attempt: DeviceJoinAttemptRef,
        registration: StoreDeviceRegistrationRef,
        initial_ack: StoreAckRef,
        bootstrap_cut: StoreHistoryCut,
        registration_value: &StoreDeviceRegistration,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        registration.verify_registration(registration_value)?;
        if keys::public_key_hex(device_signer) != registration_value.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let body = DeviceReadinessProofBody {
            store_root_hash: registration_value.store_root.store_root_hash,
            attempt,
            registration,
            initial_ack,
            bootstrap_cut,
        };
        validate_store_history_cut(&body.bootstrap_cut)?;
        Ok(Signed::sign(body, device_signer))
    }

    pub fn verify(
        &self,
        attempt_ref: &DeviceJoinAttemptRef,
        attempt: &DeviceJoinAttempt,
        registration: &StoreDeviceRegistration,
        initial_ack_ref: &StoreAckRef,
        initial_ack: &StoreAck,
    ) -> Result<(), StoreProtocolError> {
        if &self.attempt != attempt_ref
            || attempt_ref.attempt_id != attempt.attempt_id
            || attempt_ref.attempt_hash != attempt.attempt_hash()
            || self.store_root_hash != registration.store_root.store_root_hash
            || self.registration.device_id != registration.device_id
            || self.bootstrap_cut != attempt.bootstrap_cut
        {
            return Err(StoreProtocolError::DeviceReadinessMismatch);
        }
        self.registration.verify_registration(registration)?;
        if initial_ack.registration != self.registration
            || initial_ack.sequence != 1
            || initial_ack.successor.predecessor.is_some()
            || initial_ack_ref != &self.initial_ack
            || initial_ack_ref.registration != self.registration
            || initial_ack_ref.sequence != initial_ack.sequence
            || initial_ack_ref.ack_hash != initial_ack.ack_hash()
            || initial_ack.store_cut != self.bootstrap_cut
        {
            return Err(StoreProtocolError::DeviceReadinessMismatch);
        }
        validate_store_history_cut(&self.bootstrap_cut)?;
        self.verify_by(&registration.device_signing_pubkey)
    }
}

/// How a join attempt ended: with the joining device active, or cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinDisposition {
    Activated {
        registration: StoreDeviceRegistrationRef,
    },
    Cancelled,
}

/// The wire body of a join attempt's outcome. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinOutcomeBody {
    pub store_root_hash: ObjectHash,
    pub attempt: DeviceJoinAttemptRef,
    pub disposition: DeviceJoinDisposition,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
}

impl SignedBody for DeviceJoinOutcomeBody {
    const DOMAIN: &'static [u8] = DEVICE_JOIN_OUTCOME_DOMAIN;
}

pub type DeviceJoinOutcome = Signed<DeviceJoinOutcomeBody>;

impl DeviceJoinOutcome {
    pub fn signed(
        attempt: DeviceJoinAttemptRef,
        disposition: DeviceJoinDisposition,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(Signed::sign(
            DeviceJoinOutcomeBody {
                store_root_hash: owner.store_root.store_root_hash,
                attempt,
                disposition,
                owner_registration,
                owner_grant,
            },
            owner_device_signer,
        ))
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn verify_at(
        &self,
        expected: &DeviceJoinOutcomeRef,
        attempt: &DeviceJoinAttempt,
        owner: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        let bytes = self.to_bytes();
        expected.object().verify(&bytes)?;
        expected.verify_outcome(self)?;
        if &self.attempt != expected.attempt()
            || self.attempt.attempt_id != attempt.attempt_id
            || self.store_root_hash != attempt.store_root.store_root_hash
            || self.owner_registration != attempt.owner_registration
            || self.owner_grant != attempt.owner_grant
        {
            return Err(StoreProtocolError::JoinOutcomeMismatch);
        }
        self.owner_registration.verify_registration(owner)?;
        self.verify_by(&owner.device_signing_pubkey)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinOutcomeRef {
    Activated {
        attempt: DeviceJoinAttemptRef,
        outcome_hash: ObjectHash,
        object: ExactObjectRef,
    },
    Cancelled {
        attempt: DeviceJoinAttemptRef,
        outcome_hash: ObjectHash,
        object: ExactObjectRef,
    },
}

impl DeviceJoinOutcomeRef {
    pub fn slot(&self) -> &ObjectSlot {
        self.object().slot()
    }

    pub fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Activated { object, .. } | Self::Cancelled { object, .. } => object,
        }
    }

    pub fn attempt(&self) -> &DeviceJoinAttemptRef {
        match self {
            Self::Activated { attempt, .. } | Self::Cancelled { attempt, .. } => attempt,
        }
    }

    pub fn verify_outcome(&self, outcome: &DeviceJoinOutcome) -> Result<(), StoreProtocolError> {
        let (attempt, expected_hash, expects_activated) = match self {
            Self::Activated {
                attempt,
                outcome_hash,
                ..
            } => (attempt, outcome_hash, true),
            Self::Cancelled {
                attempt,
                outcome_hash,
                ..
            } => (attempt, outcome_hash, false),
        };
        if &outcome.attempt != attempt || outcome.outcome_hash() != *expected_hash {
            return Err(StoreProtocolError::JoinOutcomeMismatch);
        }
        if expects_activated
            != matches!(outcome.disposition, DeviceJoinDisposition::Activated { .. })
        {
            return Err(StoreProtocolError::JoinOutcomeMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecoveryNodeRef {
    pub owner_pubkey: String,
    pub owner_grant: MembershipGrantId,
    pub sequence: u64,
    pub node_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OwnerRecoveryActivationId(ObjectHash);

impl OwnerRecoveryActivationId {
    pub fn derive(
        root: &StoreRootRef,
        owner_pubkey: &str,
        owner_grant: &MembershipGrantId,
        anchor: &GrantStreamAnchor,
    ) -> Result<Self, StoreProtocolError> {
        if !matches!(anchor, GrantStreamAnchor::OwnerRecovery { .. }) {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(Self(ObjectHash::digest(&domain_json(
            b"coven.owner-recovery-activation.v1\0",
            &(root, owner_pubkey, owner_grant, anchor),
        ))))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerRecoveryPosition {
    BeforeFirst {
        activation: OwnerRecoveryActivationId,
    },
    At {
        node: OwnerRecoveryNodeRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecoveryCursor {
    pub owner_grant: MembershipGrantId,
    pub position: OwnerRecoveryPosition,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAbandonmentRef {
    pub attempt_id: DeviceJoinAttemptId,
    pub abandonment_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinCleanupReceiptRef {
    pub attempt_id: DeviceJoinAttemptId,
    pub receipt_hash: ObjectHash,
    pub object: ExactObjectRef,
}
