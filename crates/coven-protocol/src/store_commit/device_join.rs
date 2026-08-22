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

/// The wire body of a joining device's readiness proof. Every field here is
/// signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceReadinessProofBody {
    pub store_root_hash: ObjectHash,
    pub attempt_id: DeviceJoinAttemptId,
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
        attempt_id: DeviceJoinAttemptId,
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
            attempt_id,
            registration,
            initial_ack,
            bootstrap_cut,
        };
        validate_store_history_cut(&body.bootstrap_cut)?;
        Ok(Signed::sign(body, device_signer))
    }

    /// Check a readiness proof against the attempt commit it answers.
    ///
    /// `attempt_cut` is that commit's predecessor cut — the history the
    /// admitting device declared the joining device would install from. The
    /// joiner echoes it here, so the two have to agree.
    pub fn verify(
        &self,
        attempt_id: DeviceJoinAttemptId,
        attempt_cut: &StoreHistoryCut,
        registration: &StoreDeviceRegistration,
        initial_ack_ref: &StoreAckRef,
        initial_ack: &StoreAck,
    ) -> Result<(), StoreProtocolError> {
        if self.attempt_id != attempt_id
            || self.store_root_hash != registration.store_root.store_root_hash
            || self.registration.device_id != registration.device_id
            || &self.bootstrap_cut != attempt_cut
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

impl OwnerRecoveryPosition {
    /// The position two predecessor states of one Owner grant agree on. The
    /// grant's recovery nodes form one chain in exact slots, one node per
    /// sequence, so two positions on it are ordered: a node is past the
    /// activation it follows, and a higher sequence is past a lower one. Two
    /// states that name different nodes at one sequence, or different
    /// activations before the first node, are not on one chain.
    pub fn merge(&self, other: &Self) -> Result<Self, super::StoreProtocolError> {
        match (self, other) {
            (Self::BeforeFirst { activation }, Self::BeforeFirst { activation: other })
                if activation == other =>
            {
                Ok(self.clone())
            }
            (Self::BeforeFirst { .. }, Self::BeforeFirst { .. }) => {
                Err(super::StoreProtocolError::OwnerRecoveryMismatch)
            }
            (Self::BeforeFirst { .. }, Self::At { .. }) => Ok(other.clone()),
            (Self::At { .. }, Self::BeforeFirst { .. }) => Ok(self.clone()),
            (Self::At { node }, Self::At { node: other_node }) => {
                match node.sequence.cmp(&other_node.sequence) {
                    std::cmp::Ordering::Less => Ok(other.clone()),
                    std::cmp::Ordering::Greater => Ok(self.clone()),
                    std::cmp::Ordering::Equal if node == other_node => Ok(self.clone()),
                    std::cmp::Ordering::Equal => {
                        Err(super::StoreProtocolError::OwnerRecoveryMismatch)
                    }
                }
            }
        }
    }
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
