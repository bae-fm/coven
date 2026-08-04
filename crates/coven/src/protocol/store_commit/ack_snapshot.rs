use super::validation::{
    require_version, validate_ack_state, validate_commit_frontier, validate_store_device_state_ref,
    validate_successor_sequence,
};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAck {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub registration: StoreDeviceRegistrationRef,
    pub sequence: u64,
    pub store_cut: StoreHistoryCut,
    pub device_state: StoreDeviceStateRef,
    pub snapshot: Option<StoreSnapshotLocator>,
    pub exclusions: StoreAckExclusionState,
    pub last_sync: String,
    pub successor: SuccessorLink,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAckRef {
    pub registration: StoreDeviceRegistrationRef,
    pub sequence: u64,
    pub ack_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshotLocator {
    pub author_registration: StoreDeviceRegistrationRef,
    pub snapshot: StoreSnapshotRef,
}

/// The exact membership and device state represented by one Store snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreSnapshotState {
    pub membership: StoreMembershipStateRef,
    pub devices: StoreDeviceStateRef,
}

impl StoreSnapshotState {
    fn validate(
        &self,
        store_root_hash: ObjectHash,
        coverage: &CommitFrontier,
    ) -> Result<(), StoreProtocolError> {
        self.membership.validate_shape()?;
        validate_store_device_state_ref(&self.devices)?;
        if self.membership.recovery() != self.devices.recovery() {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        if self.devices.frontier() != coverage {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let _ = store_root_hash;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAckExclusionState {
    pub proposal_freezes: Vec<StoreDeviceProposalAck>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceProposalAck {
    pub proposal: StoreDeviceExclusionProposalRef,
    pub target_cut: StoreHistoryCut,
}

#[derive(Serialize)]
struct AckSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    registration: &'a StoreDeviceRegistrationRef,
    sequence: u64,
    store_cut: &'a StoreHistoryCut,
    device_state: &'a StoreDeviceStateRef,
    snapshot: Option<&'a StoreSnapshotLocator>,
    exclusions: &'a StoreAckExclusionState,
    last_sync: &'a str,
    successor: &'a SuccessorLink,
}

impl StoreAck {
    pub fn signed(
        store_root_hash: ObjectHash,
        registration: StoreDeviceRegistrationRef,
        sequence: u64,
        store_cut: StoreHistoryCut,
        device_state: StoreDeviceStateRef,
        snapshot: Option<StoreSnapshotLocator>,
        exclusions: StoreAckExclusionState,
        last_sync: String,
        successor: SuccessorLink,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_successor_sequence(sequence, &successor)?;
        validate_ack_state(
            store_root_hash,
            &registration,
            &store_cut,
            &device_state,
            &exclusions,
        )?;
        let mut ack = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            registration,
            sequence,
            store_cut,
            device_state,
            snapshot,
            exclusions,
            last_sync,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &ack.canonical_signed_bytes());
        ack.signature = signature;
        Ok(ack)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            ACK_DOMAIN,
            &AckSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                registration: &self.registration,
                sequence: self.sequence,
                store_cut: &self.store_cut,
                device_state: &self.device_state,
                snapshot: self.snapshot.as_ref(),
                exclusions: &self.exclusions,
                last_sync: &self.last_sync,
                successor: &self.successor,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreAck serialization cannot fail")
    }

    pub fn ack_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn semantic_hash_from_bytes(bytes: &[u8]) -> Result<ObjectHash, StoreProtocolError> {
        let ack: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        Ok(ack.ack_hash())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root: &StoreRootRef,
        expected: &StoreAckRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let ack: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        require_version(ack.version)?;
        crate::protocol::objects::verify_store_root(
            expected_store_root.store_root_hash,
            ack.store_root_hash,
        )?;
        ack.registration.verify_registration(author)?;
        if ack.registration != expected.registration {
            return Err(StoreProtocolError::DeviceRegistrationRefMismatch {
                device_id: expected.registration.device_id.to_string(),
                expected: expected.registration.registration_hash,
                actual: ack.registration.registration_hash,
            });
        }
        if ack.sequence != expected.sequence {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: ack_slot_prefix(&author.device_id.to_string(), expected.sequence),
                actual: ack_slot_prefix(&author.device_id.to_string(), ack.sequence),
            });
        }
        validate_successor_sequence(ack.sequence, &ack.successor)?;
        validate_ack_state(
            ack.store_root_hash,
            &ack.registration,
            &ack.store_cut,
            &ack.device_state,
            &ack.exclusions,
        )?;
        let activation = author
            .store_acknowledgement_activation(&ack.registration)?
            .activation_id();
        if ack.successor.activation != activation {
            return Err(StoreProtocolError::Malformed(
                "Store acknowledgement successor uses another stream activation".to_string(),
            ));
        }
        if !keys::verify_signature_hex(
            &author.device_signing_pubkey,
            &ack.signature,
            &ack.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        if ack.ack_hash() != expected.ack_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.ack_hash,
                actual: ack.ack_hash(),
            });
        }
        Ok(ack)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotMeta {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub generation: u64,
    pub predecessor: Option<StoreSnapshotRef>,
    pub image: SnapshotImageRef,
    pub coverage: CommitFrontier,
    pub state: StoreSnapshotState,
    pub history_summary: RetainedVerifiedMergeHistorySummary,
    pub schema_version: u32,
    pub created_at: String,
    pub successor: SnapshotSuccessorLink,
    pub signature: String,
}

impl RetainedVerifiedMergeHistorySummary {
    fn validate(
        &self,
        store_root_hash: ObjectHash,
        coverage: &CommitFrontier,
        state: &StoreSnapshotState,
    ) -> Result<(), StoreProtocolError> {
        self.validate_snapshot_baseline()?;
        if self.store_root_hash != store_root_hash
            || self.frontier()? != coverage.0
            || self.post_state != state.devices
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotImageRef {
    pub image_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshotRef {
    pub generation: u64,
    pub snapshot_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotSuccessorLink {
    pub activation: StreamActivationId,
    pub predecessor: Option<StoreSnapshotRef>,
    pub next_slot: ObjectSlot,
}

#[derive(Serialize)]
struct SnapshotSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    author_registration: &'a StoreDeviceRegistrationRef,
    generation: u64,
    predecessor: Option<&'a StoreSnapshotRef>,
    image: &'a SnapshotImageRef,
    coverage: &'a CommitFrontier,
    state: &'a StoreSnapshotState,
    history_summary: &'a RetainedVerifiedMergeHistorySummary,
    schema_version: u32,
    created_at: &'a str,
    successor: &'a SnapshotSuccessorLink,
}

impl SnapshotMeta {
    pub(crate) fn signed(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        generation: u64,
        predecessor: Option<StoreSnapshotRef>,
        image: SnapshotImageRef,
        coverage: CommitFrontier,
        state: StoreSnapshotState,
        history_summary: RetainedVerifiedMergeHistorySummary,
        schema_version: u32,
        created_at: String,
        successor: SnapshotSuccessorLink,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_snapshot_generation(generation, predecessor.as_ref())?;
        validate_commit_frontier(&coverage)?;
        state.validate(store_root_hash, &coverage)?;
        history_summary.validate(store_root_hash, &coverage, &state)?;
        let mut meta = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            author_registration,
            generation,
            predecessor,
            image,
            coverage,
            state,
            history_summary,
            schema_version,
            created_at,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &meta.canonical_signed_bytes());
        meta.signature = signature;
        Ok(meta)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            SNAPSHOT_DOMAIN,
            &SnapshotSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                author_registration: &self.author_registration,
                generation: self.generation,
                predecessor: self.predecessor.as_ref(),
                image: &self.image,
                coverage: &self.coverage,
                state: &self.state,
                history_summary: &self.history_summary,
                schema_version: self.schema_version,
                created_at: &self.created_at,
                successor: &self.successor,
            },
        )
    }

    pub(crate) fn snapshot_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub(crate) fn semantic_hash_from_bytes(bytes: &[u8]) -> Result<ObjectHash, StoreProtocolError> {
        let meta: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        Ok(meta.snapshot_hash())
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("SnapshotMeta serialization cannot fail")
    }

    pub(crate) fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected: &StoreSnapshotRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let meta: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        require_version(meta.version)?;
        crate::protocol::objects::verify_store_root(
            expected_store_root_hash,
            meta.store_root_hash,
        )?;
        meta.author_registration.verify_registration(author)?;
        if meta.generation != expected.generation {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: snapshot_semantic_prefix(
                    &author.device_id.to_string(),
                    expected.snapshot_hash,
                ),
                actual: snapshot_semantic_prefix(
                    &author.device_id.to_string(),
                    meta.snapshot_hash(),
                ),
            });
        }
        validate_snapshot_generation(meta.generation, meta.predecessor.as_ref())?;
        validate_commit_frontier(&meta.coverage)?;
        meta.state
            .validate(expected_store_root_hash, &meta.coverage)?;
        meta.history_summary
            .validate(expected_store_root_hash, &meta.coverage, &meta.state)?;
        if !keys::verify_signature_hex(
            &author.device_signing_pubkey,
            &meta.signature,
            &meta.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let actual = meta.snapshot_hash();
        if actual != expected.snapshot_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.snapshot_hash,
                actual,
            });
        }
        Ok(meta)
    }

    pub(crate) fn parse_stream_entry_at(
        bytes: &[u8],
        expected_store_root: &StoreRootRef,
        expected_registration: &StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        expected: &StoreSnapshotRef,
    ) -> Result<Self, StoreProtocolError> {
        let meta = Self::parse_at(bytes, expected_store_root.store_root_hash, expected, author)?;
        let next_generation = expected.generation.checked_add(1).ok_or_else(|| {
            StoreProtocolError::Malformed("Store snapshot generation overflow".to_string())
        })?;
        let activation = author
            .store_snapshot_activation(expected_registration)?
            .activation_id();
        if meta.author_registration != *expected_registration
            || meta.successor.activation != activation
            || meta.successor.predecessor != meta.predecessor
            || meta.successor.next_slot.logical_key()
                != format!(
                    "{}.json",
                    snapshot_slot_prefix(&author.device_id.to_string(), next_generation)
                )
        {
            return Err(StoreProtocolError::Malformed(
                "Store snapshot metadata is outside its activated exact stream".to_string(),
            ));
        }
        Ok(meta)
    }
}

fn validate_snapshot_generation(
    generation: u64,
    predecessor: Option<&StoreSnapshotRef>,
) -> Result<(), StoreProtocolError> {
    match (generation, predecessor) {
        (0, None) => Ok(()),
        (0, Some(_)) | (_, None) => Err(StoreProtocolError::Malformed(
            "Store snapshot generation and predecessor disagree".to_string(),
        )),
        (generation, Some(predecessor)) => {
            let expected = predecessor.generation.checked_add(1).ok_or_else(|| {
                StoreProtocolError::Malformed("Store snapshot generation overflow".to_string())
            })?;
            if generation != expected {
                return Err(StoreProtocolError::Malformed(
                    "Store snapshot generation does not follow its predecessor".to_string(),
                ));
            }
            Ok(())
        }
    }
}
