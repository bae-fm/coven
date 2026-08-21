use super::validation::{
    validate_ack_state, validate_commit_frontier, validate_store_device_state_ref,
    validate_successor_sequence,
};
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAckBody {
    pub store_root_hash: ObjectHash,
    pub registration: StoreDeviceRegistrationRef,
    pub sequence: u64,
    pub store_cut: StoreHistoryCut,
    pub device_state: StoreDeviceStateRef,
    pub snapshot: Option<StoreSnapshotLocator>,
    pub exclusions: StoreAckExclusionState,
    pub last_sync: String,
    pub successor: SuccessorLink,
}

impl SignedBody for StoreAckBody {
    const DOMAIN: &'static [u8] = ACK_DOMAIN;
}

/// What a Store acknowledgement asserts, apart from the bookkeeping every
/// acknowledgement carries fresh: its sequence, the wall clock it was written
/// at, and its links to the neighbours in the device's acknowledgement chain.
///
/// Two acknowledgements with equal assertions tell every reader the same thing.
/// That matters because publishing an acknowledgement appends a commit, and a
/// commit that says nothing new still lands in every device's history, every
/// retained materialization, and every snapshot — a store that is doing nothing
/// grows one commit per device per cycle, forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAckAssertion {
    pub registration: StoreDeviceRegistrationRef,
    pub store_cut: StoreHistoryCut,
    pub device_state: StoreDeviceStateRef,
    pub snapshot: Option<StoreSnapshotLocator>,
    pub exclusions: StoreAckExclusionState,
}

/// The acknowledgement a device currently stands behind: what it asserted, and
/// the commit that carried it.
///
/// Kept so the next cycle can ask whether that acknowledgement still says
/// everything true, instead of publishing another one to find out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandingStoreAck {
    pub assertion: StoreAckAssertion,
    /// The commit that published it, when it activated one.
    ///
    /// An acknowledgement cannot cover the commit that carries it — that commit
    /// does not exist until the acknowledgement is signed. So this is the one
    /// position in the Store's history the assertion leaves behind on purpose,
    /// and the one advance that is not a reason to acknowledge again. Without it
    /// a device acknowledges its own acknowledgement, cycle after cycle, forever.
    pub activating_commit: Option<StoreBatchCommitRef>,
}

impl StandingStoreAck {
    /// Whether `assertion` says exactly what this acknowledgement already said.
    ///
    /// Destructured exhaustively for the same reason [`StoreAckBody::assertion`]
    /// is: a field added to the assertion has to be compared here, or the
    /// acknowledgement could change without anything noticing.
    pub fn still_holds(&self, assertion: &StoreAckAssertion) -> bool {
        let StoreAckAssertion {
            registration,
            store_cut,
            device_state,
            snapshot,
            exclusions,
        } = assertion;
        if registration != &self.assertion.registration
            || snapshot != &self.assertion.snapshot
            || exclusions != &self.assertion.exclusions
        {
            return false;
        }
        // A device-state reference names both the device set and the cut it was
        // read at. The cut half moves with `store_cut` and is compared there; the
        // device set is what this reference actually asserts.
        if device_state.state_hash() != self.assertion.device_state.state_hash()
            || device_state.recovery() != self.assertion.device_state.recovery()
        {
            return false;
        }
        *store_cut == self.covered_cut()
    }

    /// The Store history this acknowledgement leaves behind it: what it asserted,
    /// plus the commit that carried it. A frontier equal to this one holds
    /// nothing the standing acknowledgement has not already accounted for.
    fn covered_cut(&self) -> StoreHistoryCut {
        let mut cut = self.assertion.store_cut.0.clone();
        if let Some(commit) = &self.activating_commit {
            cut.insert(commit.coord.stream_id, commit.clone());
        }
        StoreHistoryCut(cut)
    }
}

impl StoreAckBody {
    /// The assertion this body makes.
    ///
    /// Destructured exhaustively on purpose: a field added to the body has to be
    /// classified here as asserted or as bookkeeping, or this stops compiling. A
    /// silently unclassified field would be one an acknowledgement could change
    /// without anything noticing it had changed.
    pub fn assertion(&self) -> StoreAckAssertion {
        let Self {
            store_root_hash: _,
            registration,
            sequence: _,
            store_cut,
            device_state,
            snapshot,
            exclusions,
            last_sync: _,
            successor: _,
        } = self;
        StoreAckAssertion {
            registration: registration.clone(),
            store_cut: store_cut.clone(),
            device_state: device_state.clone(),
            snapshot: snapshot.clone(),
            exclusions: exclusions.clone(),
        }
    }
}

pub type StoreAck = Signed<StoreAckBody>;

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
pub struct StoreSnapshotState {
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

impl StoreAck {
    pub fn signed(
        store_root_hash: ObjectHash,
        sequence: u64,
        assertion: StoreAckAssertion,
        last_sync: String,
        successor: SuccessorLink,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_successor_sequence(sequence, &successor)?;
        let StoreAckAssertion {
            registration,
            store_cut,
            device_state,
            snapshot,
            exclusions,
        } = assertion;
        validate_ack_state(
            store_root_hash,
            &registration,
            &store_cut,
            &device_state,
            &exclusions,
        )?;
        Ok(Signed::sign(
            StoreAckBody {
                store_root_hash,
                registration,
                sequence,
                store_cut,
                device_state,
                snapshot,
                exclusions,
                last_sync,
                successor,
            },
            device_signer,
        ))
    }

    pub fn ack_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn semantic_hash_from_bytes(bytes: &[u8]) -> Result<ObjectHash, StoreProtocolError> {
        let ack: Self = crate::objects::decode_protocol_object(bytes)?;
        Ok(ack.ack_hash())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root: &StoreRootRef,
        expected: &StoreAckRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let ack: Self = crate::objects::decode_protocol_object(bytes)?;
        ack.require_version()?;
        crate::objects::verify_store_root(
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
        ack.verify_by(&author.device_signing_pubkey)?;
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
pub struct SnapshotMetaBody {
    pub store_root_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub generation: u64,
    pub predecessor: Option<StoreSnapshotRef>,
    pub image: SnapshotImageRef,
    /// The membership objects a reader needs to reach `state.membership`,
    /// published beside the image. A joining device opens the Store keyring out
    /// of the membership chain, so it cannot read anything the keyring
    /// protects — this reference is what lets it take the chain in one read
    /// instead of two round trips per membership change.
    pub membership_rollup: MembershipRollupRef,
    pub coverage: CommitFrontier,
    pub state: StoreSnapshotState,
    pub history_summary: RetainedVerifiedMergeHistorySummary,
    pub schema_version: u32,
    pub created_at: String,
    pub successor: SnapshotSuccessorLink,
}

impl SignedBody for SnapshotMetaBody {
    const DOMAIN: &'static [u8] = SNAPSHOT_DOMAIN;
}

pub type SnapshotMeta = Signed<SnapshotMetaBody>;

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
pub struct SnapshotSuccessorLink {
    pub activation: StreamActivationId,
    pub predecessor: Option<StoreSnapshotRef>,
    pub next_slot: ObjectSlot,
}

impl SnapshotMeta {
    pub fn signed(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        generation: u64,
        predecessor: Option<StoreSnapshotRef>,
        image: SnapshotImageRef,
        membership_rollup: MembershipRollupRef,
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
        Ok(Signed::sign(
            SnapshotMetaBody {
                store_root_hash,
                author_registration,
                generation,
                predecessor,
                image,
                membership_rollup,
                coverage,
                state,
                history_summary,
                schema_version,
                created_at,
                successor,
            },
            device_signer,
        ))
    }

    pub fn snapshot_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn semantic_hash_from_bytes(bytes: &[u8]) -> Result<ObjectHash, StoreProtocolError> {
        let meta: Self = crate::objects::decode_protocol_object(bytes)?;
        Ok(meta.snapshot_hash())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected: &StoreSnapshotRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let meta: Self = crate::objects::decode_protocol_object(bytes)?;
        meta.require_version()?;
        crate::objects::verify_store_root(expected_store_root_hash, meta.store_root_hash)?;
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
        meta.verify_by(&author.device_signing_pubkey)?;
        let actual = meta.snapshot_hash();
        if actual != expected.snapshot_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.snapshot_hash,
                actual,
            });
        }
        Ok(meta)
    }

    pub fn parse_stream_entry_at(
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
