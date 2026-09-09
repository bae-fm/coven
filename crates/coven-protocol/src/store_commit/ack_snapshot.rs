use super::validation::{
    validate_ack_state, validate_commit_frontier, validate_successor_sequence,
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
    /// This comparison needs no historical bodies when the cut differs only by
    /// the activating commit. Other acknowledgement-only advances are proved by
    /// the verified history owner.
    pub fn still_holds(&self, assertion: &StoreAckAssertion) -> bool {
        self.assertion.same_state_as(assertion) && assertion.store_cut == self.covered_cut()
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

impl StoreAckAssertion {
    /// Compare the asserted device state. The history
    /// owner separately decides whether a changed cut contains new work.
    pub fn same_state_as(&self, assertion: &Self) -> bool {
        let StoreAckAssertion {
            registration,
            store_cut: _,
            device_state,
        } = assertion;
        if registration != &self.registration {
            return false;
        }
        // A device-state reference names both the device set and the cut it was
        // read at. The cut is compared separately; the device set is what this
        // reference asserts independently of that cut.
        if device_state.state_hash() != self.device_state.state_hash()
            || device_state.recovery() != self.device_state.recovery()
        {
            return false;
        }
        true
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
            last_sync: _,
            successor: _,
        } = self;
        StoreAckAssertion {
            registration: registration.clone(),
            store_cut: store_cut.clone(),
            device_state: device_state.clone(),
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

/// The exact membership and device state represented by one Store snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshotState {
    pub membership: StoreMembershipStateRef,
    pub devices: ResolvedStoreDeviceState,
}

impl StoreSnapshotState {
    fn validate(&self) -> Result<(), StoreProtocolError> {
        self.membership.validate_shape()?;
        self.devices.validate_canonical()?;
        if self.membership.recovery() != self.devices.recovery {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(())
    }
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
        } = assertion;
        validate_ack_state(&store_cut, &device_state)?;
        Ok(Signed::sign(
            StoreAckBody {
                store_root_hash,
                registration,
                sequence,
                store_cut,
                device_state,
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
        validate_ack_state(&ack.store_cut, &ack.device_state)?;
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
    /// The exact accepted publication boundary whose complete shared state is
    /// represented by this image. Snapshot acceptance extends this record;
    /// after compaction it remains the authenticated start of the retained
    /// publication interval.
    pub publication_predecessor: StoreCurrentPublicationRecord,
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
            || self.post_state
                != StoreDeviceStateRef::from_resolved(coverage.clone(), &state.devices)?
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
    pub snapshot_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl StoreSnapshotRef {
    /// A fresh metadata slot owns its image and rollup. An artifact from another
    /// candidate cannot acquire a new owner after its old snapshot is retired.
    pub fn validate_artifact_slots(
        &self,
        image: &SnapshotImageRef,
        rollup: &MembershipRollupRef,
    ) -> Result<(), StoreProtocolError> {
        for (object, expected) in [
            (
                &image.object,
                format!(
                    "{}.db",
                    snapshot_image_semantic_prefix(self.object.slot(), image.image_hash)
                ),
            ),
            (
                &rollup.object,
                format!(
                    "{}.json",
                    membership_rollup_semantic_prefix(self.object.slot(), rollup.rollup_hash)
                ),
            ),
        ] {
            if object.slot().logical_key() != expected {
                return Err(StoreProtocolError::RelocatedSlot {
                    expected,
                    actual: object.slot().logical_key().into(),
                });
            }
        }
        Ok(())
    }
}

impl SnapshotMeta {
    pub fn signed(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        publication_predecessor: StoreCurrentPublicationRecord,
        image: SnapshotImageRef,
        membership_rollup: MembershipRollupRef,
        coverage: CommitFrontier,
        state: StoreSnapshotState,
        history_summary: RetainedVerifiedMergeHistorySummary,
        schema_version: u32,
        created_at: String,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        if publication_predecessor.store_root_hash != store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: store_root_hash,
                actual: publication_predecessor.store_root_hash,
            });
        }
        validate_commit_frontier(&coverage)?;
        state.validate()?;
        history_summary.validate(store_root_hash, &coverage, &state)?;
        Ok(Signed::sign(
            SnapshotMetaBody {
                store_root_hash,
                author_registration,
                publication_predecessor,
                image,
                membership_rollup,
                coverage,
                state,
                history_summary,
                schema_version,
                created_at,
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
        meta.verify_bytes_at(bytes, expected_store_root_hash, expected, author)?;
        Ok(meta)
    }

    /// Verify an already decoded snapshot against its exact canonical object.
    pub fn verify_at(
        &self,
        expected_store_root_hash: ObjectHash,
        expected: &StoreSnapshotRef,
        author: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.verify_bytes_at(&self.to_bytes(), expected_store_root_hash, expected, author)
    }

    fn verify_bytes_at(
        &self,
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected: &StoreSnapshotRef,
        author: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.require_version()?;
        crate::objects::verify_store_root(expected_store_root_hash, self.store_root_hash)?;
        self.author_registration.verify_registration(author)?;
        crate::objects::verify_store_root(
            expected_store_root_hash,
            author.store_root.store_root_hash,
        )?;
        crate::objects::verify_store_root(
            expected_store_root_hash,
            self.publication_predecessor.store_root_hash,
        )?;
        let prefix = semantic_prefix_from_exact_object(&expected.object, ".json")?;
        let author_prefix = format!("{STORE_SNAPSHOT_META_PREFIX}{}/", author.device_id);
        let candidate = prefix.strip_prefix(&author_prefix).ok_or_else(|| {
            StoreProtocolError::Malformed(
                "Store snapshot candidate is outside its author's metadata path".to_string(),
            )
        })?;
        coven_foundation::store_dir::validate_path_token(candidate).map_err(|error| {
            StoreProtocolError::Malformed(format!("invalid Store snapshot candidate: {error}"))
        })?;
        crate::objects::ProtocolObjectContext::signed_plaintext(
            expected_store_root_hash,
            crate::objects::ProtocolObjectDomain::StoreSnapshotMeta,
        )
        .validate_reference(&expected.object, &prefix)?;
        expected.object.verify(bytes)?;
        validate_commit_frontier(&self.coverage)?;
        self.state.validate()?;
        self.history_summary
            .validate(expected_store_root_hash, &self.coverage, &self.state)?;
        self.verify_by(&author.device_signing_pubkey)?;
        let actual = self.snapshot_hash();
        if actual != expected.snapshot_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.snapshot_hash,
                actual,
            });
        }
        expected.validate_artifact_slots(&self.image, &self.membership_rollup)?;
        Ok(())
    }
}
