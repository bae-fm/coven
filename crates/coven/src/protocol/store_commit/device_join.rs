use super::validation::require_version;
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
        value.parse().map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreCreationId(ObjectHash);

impl StoreCreationId {
    pub fn from_random_bytes(bytes: [u8; 32]) -> Self {
        Self(ObjectHash::from_digest(bytes))
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[cfg(test)]
    pub(crate) fn from_nonce(nonce: &str) -> Self {
        Self(ObjectHash::digest(nonce.as_bytes()))
    }

    pub(super) fn object_hash(self) -> ObjectHash {
        self.0
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAttempt {
    pub version: u32,
    pub store_root: StoreRootRef,
    pub attempt_id: DeviceJoinAttemptId,
    pub attempt_slot: ObjectSlot,
    pub expected_registration: StoreDeviceRegistration,
    pub registration_slot: ObjectSlot,
    pub outcome_slot: ObjectSlot,
    pub bootstrap_cut: StoreHistoryCut,
    pub membership: StoreMembershipStateRef,
    pub provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    pub provider_approval: crate::sync::store::DeviceProviderAdmissionApproval,
    pub provider_response: crate::sync::store::DeviceProviderResponseReservation,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

pub(crate) struct UnverifiedDeviceJoinAttempt(DeviceJoinAttempt);

impl UnverifiedDeviceJoinAttempt {
    pub(crate) fn verify_at(
        self,
        expected: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<DeviceJoinAttempt, StoreProtocolError> {
        let attempt = self.0;
        require_version(attempt.version)?;
        attempt.validate_shape()?;
        if attempt.attempt_id != expected.attempt_id
            || attempt.attempt_hash() != expected.attempt_hash
            || &attempt.attempt_slot != expected.object.slot()
        {
            return Err(StoreProtocolError::JoinAttemptMismatch);
        }
        attempt.owner_registration.verify_registration(owner)?;
        if !keys::verify_signature_hex(
            &owner.device_signing_pubkey,
            &attempt.signature,
            &attempt.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(attempt)
    }
}

#[derive(Serialize)]
struct DeviceJoinAttemptSignedFields<'a> {
    version: u32,
    store_root: &'a StoreRootRef,
    attempt_id: DeviceJoinAttemptId,
    attempt_slot: &'a ObjectSlot,
    expected_registration: &'a StoreDeviceRegistration,
    registration_slot: &'a ObjectSlot,
    outcome_slot: &'a ObjectSlot,
    bootstrap_cut: &'a StoreHistoryCut,
    membership: &'a StoreMembershipStateRef,
    provider_admin_grant: &'a crate::protocol::provider::ProviderAdminGrantId,
    provider_approval: &'a crate::sync::store::DeviceProviderAdmissionApproval,
    provider_response: &'a crate::sync::store::DeviceProviderResponseReservation,
    owner_registration: &'a StoreDeviceRegistrationRef,
    owner_grant: &'a MembershipGrantId,
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
        provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
        provider_approval: crate::sync::store::DeviceProviderAdmissionApproval,
        provider_response: crate::sync::store::DeviceProviderResponseReservation,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut attempt = Self {
            version: STORE_PROTOCOL_VERSION,
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
            signature: String::new(),
        };
        attempt.validate_shape()?;
        let (_, signature) = keys::sign_hex(owner_device_signer, &attempt.canonical_signed_bytes());
        attempt.signature = signature;
        Ok(attempt)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_JOIN_ATTEMPT_DOMAIN,
            &DeviceJoinAttemptSignedFields {
                version: self.version,
                store_root: &self.store_root,
                attempt_id: self.attempt_id,
                attempt_slot: &self.attempt_slot,
                expected_registration: &self.expected_registration,
                registration_slot: &self.registration_slot,
                outcome_slot: &self.outcome_slot,
                bootstrap_cut: &self.bootstrap_cut,
                membership: &self.membership,
                provider_admin_grant: &self.provider_admin_grant,
                provider_approval: &self.provider_approval,
                provider_response: &self.provider_response,
                owner_registration: &self.owner_registration,
                owner_grant: &self.owner_grant,
            },
        )
    }

    pub fn attempt_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("DeviceJoinAttempt serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        Self::parse_unverified(bytes)?.verify_at(expected, owner)
    }

    pub(crate) fn parse_unverified(
        bytes: &[u8],
    ) -> Result<UnverifiedDeviceJoinAttempt, StoreProtocolError> {
        serde_json::from_slice(bytes)
            .map(UnverifiedDeviceJoinAttempt)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
    }

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
                crate::sync::store::DeviceProviderAdmissionChallenge::SamePrincipal,
                crate::sync::store::DeviceProviderResponseReservation::SamePrincipal,
            )
            | (
                crate::sync::store::DeviceProviderAdmissionChallenge::CrossPrincipal(_),
                crate::sync::store::DeviceProviderResponseReservation::CrossPrincipal { .. },
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceReadinessProof {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub attempt: DeviceJoinAttemptRef,
    pub registration: StoreDeviceRegistrationRef,
    pub initial_ack: StoreAckRef,
    pub bootstrap_cut: StoreHistoryCut,
    pub signature: String,
}

#[derive(Serialize)]
struct DeviceReadinessSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    attempt: &'a DeviceJoinAttemptRef,
    registration: &'a StoreDeviceRegistrationRef,
    initial_ack: &'a StoreAckRef,
    bootstrap_cut: &'a StoreHistoryCut,
}

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
        let mut proof = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: registration_value.store_root.store_root_hash,
            attempt,
            registration,
            initial_ack,
            bootstrap_cut,
            signature: String::new(),
        };
        validate_store_history_cut(&proof.bootstrap_cut)?;
        let (_, signature) = keys::sign_hex(device_signer, &proof.canonical_signed_bytes());
        proof.signature = signature;
        Ok(proof)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_READINESS_DOMAIN,
            &DeviceReadinessSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                attempt: &self.attempt,
                registration: &self.registration,
                initial_ack: &self.initial_ack,
                bootstrap_cut: &self.bootstrap_cut,
            },
        )
    }

    pub fn verify(
        &self,
        attempt_ref: &DeviceJoinAttemptRef,
        attempt: &DeviceJoinAttempt,
        registration: &StoreDeviceRegistration,
        initial_ack_ref: &StoreAckRef,
        initial_ack: &StoreAck,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
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
        if !keys::verify_signature_hex(
            &registration.device_signing_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinOutcomeBody {
    Activated { readiness: DeviceReadinessProof },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinOutcome {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub attempt: DeviceJoinAttemptRef,
    pub body: DeviceJoinOutcomeBody,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Serialize)]
struct DeviceJoinOutcomeSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    attempt: &'a DeviceJoinAttemptRef,
    body: &'a DeviceJoinOutcomeBody,
    owner_registration: &'a StoreDeviceRegistrationRef,
    owner_grant: &'a MembershipGrantId,
}

impl DeviceJoinOutcome {
    pub fn signed(
        attempt: DeviceJoinAttemptRef,
        body: DeviceJoinOutcomeBody,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut outcome = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: owner.store_root.store_root_hash,
            attempt,
            body,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(owner_device_signer, &outcome.canonical_signed_bytes());
        outcome.signature = signature;
        Ok(outcome)
    }

    pub(crate) fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_JOIN_OUTCOME_DOMAIN,
            &DeviceJoinOutcomeSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                attempt: &self.attempt,
                body: &self.body,
                owner_registration: &self.owner_registration,
                owner_grant: &self.owner_grant,
            },
        )
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("DeviceJoinOutcome serialization cannot fail")
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
        if expects_activated != matches!(outcome.body, DeviceJoinOutcomeBody::Activated { .. }) {
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
