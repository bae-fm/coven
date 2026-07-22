use super::validation::require_version;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceRecoveryReadiness {
    pub registration: StoreDeviceRegistrationRef,
    pub initial_ack: StoreAckRef,
    pub bootstrap_cut: StoreHistoryCut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecoveryNode {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub recovery_id: DeviceRecoveryId,
    pub owner_pubkey: String,
    pub owner_grant: MembershipGrantId,
    pub sequence: u64,
    pub membership: StoreMembershipStateRef,
    pub predecessor: Option<OwnerRecoveryNodeRef>,
    pub readiness: DeviceRecoveryReadiness,
    pub next_slot: ObjectSlot,
    pub signature: String,
}

impl OwnerRecoveryNode {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        recovery_id: DeviceRecoveryId,
        owner_grant: MembershipGrantId,
        sequence: u64,
        membership: StoreMembershipStateRef,
        predecessor: Option<OwnerRecoveryNodeRef>,
        readiness: DeviceRecoveryReadiness,
        next_slot: ObjectSlot,
        owner_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let owner_pubkey = keys::public_key_hex(owner_signer);
        let mut node = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            recovery_id,
            owner_pubkey,
            owner_grant,
            sequence,
            membership,
            predecessor,
            readiness,
            next_slot,
            signature: String::new(),
        };
        node.validate_shape()?;
        let (_, signature) = keys::sign_hex(owner_signer, &node.canonical_signed_bytes());
        node.signature = signature;
        Ok(node)
    }

    pub fn parse_at(
        bytes: &[u8],
        store_root: &StoreRootRef,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<Self, StoreProtocolError> {
        let node: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(node.version)?;
        node.validate_shape()?;
        if node.store_root_hash != store_root.store_root_hash
            || node.owner_pubkey != reference.owner_pubkey
            || node.owner_grant != reference.owner_grant
            || node.sequence != reference.sequence
            || node.node_hash() != reference.node_hash
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        if !keys::verify_signature_hex(
            &node.owner_pubkey,
            &node.signature,
            &node.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(node)
    }

    fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        let predecessor_matches = match &self.predecessor {
            None => self.sequence == 1,
            Some(predecessor) => {
                predecessor.owner_pubkey == self.owner_pubkey
                    && predecessor.owner_grant == self.owner_grant
                    && predecessor.sequence.checked_add(1) == Some(self.sequence)
            }
        };
        if !predecessor_matches || self.readiness.initial_ack.sequence != 1 {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(())
    }

    pub(crate) fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            OWNER_RECOVERY_NODE_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                self.recovery_id,
                &self.owner_pubkey,
                &self.owner_grant,
                self.sequence,
                &self.membership,
                &self.predecessor,
                &self.readiness,
                &self.next_slot,
            ),
        )
    }

    pub fn node_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("OwnerRecoveryNode serialization cannot fail")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationOrigin {
    Founder {
        creation_id: StoreCreationId,
    },
    Join {
        attempt_id: DeviceJoinAttemptId,
        attempt_slot: ObjectSlot,
        outcome_slot: ObjectSlot,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        recovery_slot: ObjectSlot,
        owner_grant: MembershipGrantId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationActivation {
    Founder {
        root: StoreRootRef,
    },
    Join {
        attempt_id: DeviceJoinAttemptId,
        outcome: DeviceJoinOutcomeRef,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        node: OwnerRecoveryNodeRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatedStoreDeviceRegistrationRef {
    pub registration: StoreDeviceRegistrationRef,
    pub authority: StoreDeviceRegistrationActivationRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationActivationRef {
    Join {
        attempt_id: DeviceJoinAttemptId,
        outcome: DeviceJoinOutcomeRef,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        node: OwnerRecoveryNodeRef,
    },
}

impl StoreDeviceRegistrationOrigin {
    pub(super) fn external_id(&self) -> ObjectHash {
        match self {
            Self::Founder { creation_id } => creation_id.object_hash(),
            Self::Join { attempt_id, .. } => attempt_id.object_hash(),
            Self::Recovery { recovery_id, .. } => recovery_id.object_hash(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceStreamAnchor {
    StoreAnnouncements { first_slot: ObjectSlot },
    StoreAcknowledgements { first_slot: ObjectSlot },
    StoreSnapshots { first_slot: ObjectSlot },
}

impl DeviceStreamAnchor {
    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::StoreAnnouncements { first_slot }
            | Self::StoreAcknowledgements { first_slot }
            | Self::StoreSnapshots { first_slot } => first_slot,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum GrantStreamAnchor {
    StoreMembership {
        first_slot: ObjectSlot,
    },
    OwnerRecovery {
        first_slot: ObjectSlot,
    },
    CircleControl {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
    CircleRoster {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
    CircleMetadata {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
}

impl GrantStreamAnchor {
    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::StoreMembership { first_slot }
            | Self::OwnerRecovery { first_slot }
            | Self::CircleControl { first_slot, .. }
            | Self::CircleRoster { first_slot, .. }
            | Self::CircleMetadata { first_slot, .. } => first_slot,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitAnchor {
    MergeConcurrent { announcements: DeviceStreamAnchor },
    Serial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRegistration {
    pub version: u32,
    pub store_root: StoreRootRef,
    pub device_id: StoreDeviceId,
    pub author_pubkey: String,
    pub device_signing_pubkey: String,
    pub origin: StoreDeviceRegistrationOrigin,
    pub provider: ProviderDeviceBinding,
    pub store_commits: StoreCommitAnchor,
    pub acknowledgements: DeviceStreamAnchor,
    pub snapshots: DeviceStreamAnchor,
    pub identity_signature: String,
}

#[derive(Serialize)]
struct RegistrationSignedFields<'a> {
    version: u32,
    store_root: &'a StoreRootRef,
    device_id: StoreDeviceId,
    author_pubkey: &'a str,
    device_signing_pubkey: &'a str,
    origin: &'a StoreDeviceRegistrationOrigin,
    provider: &'a ProviderDeviceBinding,
    store_commits: &'a StoreCommitAnchor,
    acknowledgements: &'a DeviceStreamAnchor,
    snapshots: &'a DeviceStreamAnchor,
}

impl StoreDeviceRegistration {
    fn device_stream_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
        anchor: &DeviceStreamAnchor,
    ) -> Result<StreamActivation, StoreProtocolError> {
        reference.verify_registration(self)?;
        Ok(StreamActivation::device_authorized(
            self.store_root.store_root_hash,
            reference.clone(),
            anchor.clone(),
        ))
    }

    pub fn store_announcement_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        let StoreCommitAnchor::MergeConcurrent { announcements } = &self.store_commits else {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            });
        };
        self.device_stream_activation(reference, announcements)
    }

    pub fn store_acknowledgement_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        self.device_stream_activation(reference, &self.acknowledgements)
    }

    pub fn store_snapshot_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        self.device_stream_activation(reference, &self.snapshots)
    }

    pub fn signed(
        store_root: StoreRootRef,
        origin: StoreDeviceRegistrationOrigin,
        provider: ProviderDeviceBinding,
        store_commits: StoreCommitAnchor,
        acknowledgements: DeviceStreamAnchor,
        snapshots: DeviceStreamAnchor,
        identity_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_registration_anchors(&store_commits, &acknowledgements, &snapshots)?;
        let author_pubkey = keys::public_key_hex(identity_signer);
        let device_signer = derive_device_signer(identity_signer, &store_root, &origin);
        let device_signing_pubkey = keys::public_key_hex(&device_signer);
        let device_id = StoreDeviceId::derive(&store_root, &origin);
        let mut registration = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root,
            device_id,
            author_pubkey,
            device_signing_pubkey,
            origin,
            provider,
            store_commits,
            acknowledgements,
            snapshots,
            identity_signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(identity_signer, &registration.canonical_signed_bytes());
        registration.identity_signature = signature;
        Ok(registration)
    }

    pub(crate) fn device_signer(
        &self,
        identity_signer: &UserKeypair,
    ) -> Result<UserKeypair, StoreProtocolError> {
        if keys::public_key_hex(identity_signer) != self.author_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let signer = derive_device_signer(identity_signer, &self.store_root, &self.origin);
        if keys::public_key_hex(&signer) != self.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(signer)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            REGISTRATION_DOMAIN,
            &RegistrationSignedFields {
                version: self.version,
                store_root: &self.store_root,
                device_id: self.device_id,
                author_pubkey: &self.author_pubkey,
                device_signing_pubkey: &self.device_signing_pubkey,
                origin: &self.origin,
                provider: &self.provider,
                store_commits: &self.store_commits,
                acknowledgements: &self.acknowledgements,
                snapshots: &self.snapshots,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreDeviceRegistration serialization cannot fail")
    }

    pub fn registration_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root: &StoreRootRef,
        expected_device: StoreDeviceId,
    ) -> Result<Self, StoreProtocolError> {
        let registration: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(registration.version)?;
        if &registration.store_root != expected_store_root {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root.store_root_hash,
                actual: registration.store_root.store_root_hash,
            });
        }
        if registration.device_id != expected_device {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: registration_slot_prefix(&expected_device.to_string()),
                actual: registration_slot_prefix(&registration.device_id.to_string()),
            });
        }
        if registration.device_id
            != StoreDeviceId::derive(&registration.store_root, &registration.origin)
        {
            return Err(StoreProtocolError::Malformed(
                "Store device id differs from its root and origin".to_string(),
            ));
        }
        validate_registration_anchors(
            &registration.store_commits,
            &registration.acknowledgements,
            &registration.snapshots,
        )?;
        if !keys::verify_signature_hex(
            &registration.author_pubkey,
            &registration.identity_signature,
            &registration.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(registration)
    }
}

fn derive_device_signer(
    identity_signer: &UserKeypair,
    store_root: &StoreRootRef,
    origin: &StoreDeviceRegistrationOrigin,
) -> UserKeypair {
    const DOMAIN: &[u8] = b"coven.store-device-signing-key.v1\0";
    let context = serde_json::to_vec(&(store_root, origin))
        .expect("Store device signing context serialization cannot fail");
    identity_signer.derive_signing_key(DOMAIN, &context)
}

fn validate_registration_anchors(
    commits: &StoreCommitAnchor,
    acknowledgements: &DeviceStreamAnchor,
    snapshots: &DeviceStreamAnchor,
) -> Result<(), StoreProtocolError> {
    if !matches!(
        acknowledgements,
        DeviceStreamAnchor::StoreAcknowledgements { .. }
    ) || !matches!(snapshots, DeviceStreamAnchor::StoreSnapshots { .. })
        || !matches!(
            commits,
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { .. }
            }
        ) && !matches!(commits, StoreCommitAnchor::Serial)
    {
        return Err(StoreProtocolError::Malformed(
            "Store device registration contains mismatched permanent stream anchors".to_string(),
        ));
    }
    Ok(())
}
