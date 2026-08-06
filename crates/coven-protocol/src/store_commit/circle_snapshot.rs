use super::*;

/// Exact coordinate of one signed Circle snapshot on its author's per-Circle
/// snapshot stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleSnapshotRef {
    pub generation: u64,
    pub snapshot_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// The device-authorized activation binding one device's per-Circle snapshot
/// stream to its Circle. Such a stream has no first slot in the registration —
/// like the per-Circle acknowledgement stream, it is anchored on the deterministic
/// generation-zero slot both the author and every reader compute.
pub fn circle_snapshot_stream_activation(
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    circle_id: CircleId,
    device_id: &str,
) -> Result<StreamActivationId, StoreProtocolError> {
    let first_slot = ObjectSlot::logical(format!(
        "{}.json",
        circle_snapshot_slot_prefix(circle_id, device_id, 0)
    ))
    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    Ok(StreamActivation::device_authorized(
        store_root_hash,
        author_registration.clone(),
        DeviceStreamAnchor::CircleSnapshots {
            circle_id,
            first_slot,
        },
    )
    .activation_id())
}

/// The exact predecessor and create-once successor slot binding one Circle
/// snapshot into its per-(device, Circle) stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleSnapshotSuccessorLink {
    pub activation: StreamActivationId,
    pub predecessor: Option<CircleSnapshotRef>,
    pub next_slot: ObjectSlot,
}

/// One device's signed, Circle-sealed snapshot of the private Circle history it
/// holds at an exact Store frontier. The installable payload is a
/// `CircleBootstrapRef` — the same image format a member-addition bootstrap
/// carries — so a verifier installs a snapshot with the bootstrap machinery. The
/// metadata additionally binds the exact control, epoch, and key fingerprint the
/// image derives from and the per-(device, Circle) snapshot stream position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleSnapshotMetaBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub author_registration: StoreDeviceRegistrationRef,
    pub control: CircleControlCoord,
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    pub generation: u64,
    /// The exact cut, schema, routing hash, image, and pinned blob refs the
    /// image contains — the same shape a member-addition bootstrap carries. The
    /// cut is `bootstrap.coverage`.
    pub bootstrap: CircleBootstrapRef,
    pub created_at: String,
    pub successor: CircleSnapshotSuccessorLink,
}

impl SignedBody for CircleSnapshotMetaBody {
    const DOMAIN: &'static [u8] = CIRCLE_SNAPSHOT_DOMAIN;
}

pub type CircleSnapshotMeta = Signed<CircleSnapshotMetaBody>;

impl CircleSnapshotMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        author_registration: StoreDeviceRegistrationRef,
        control: CircleControlCoord,
        epoch_id: CircleEpochId,
        key_fingerprint: KeyFingerprint,
        generation: u64,
        bootstrap: CircleBootstrapRef,
        created_at: String,
        successor: CircleSnapshotSuccessorLink,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_circle_snapshot_generation(generation, successor.predecessor.as_ref())?;
        validate_circle_snapshot_state(&control, &bootstrap.coverage)?;
        Ok(Signed::sign(
            CircleSnapshotMetaBody {
                store_root_hash,
                circle_id,
                author_registration,
                control,
                epoch_id,
                key_fingerprint,
                generation,
                bootstrap,
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

    /// Verify one exact Circle snapshot against its expected reference and author
    /// registration. The successor's stream activation is not recomputed here:
    /// a Circle snapshot stream has no per-(device, Circle) first slot in the
    /// registration for a reader to derive, so the create-once successor slot and
    /// predecessor chain establish stream position, exactly as the Circle
    /// acknowledgement stream does.
    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected: &CircleSnapshotRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let meta: Self = crate::objects::decode_protocol_object(bytes)?;
        meta.require_version()?;
        crate::objects::verify_store_root(expected_store_root_hash, meta.store_root_hash)?;
        meta.author_registration.verify_registration(author)?;
        if meta.generation != expected.generation {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: circle_snapshot_slot_prefix(
                    meta.circle_id,
                    &author.device_id.to_string(),
                    expected.generation,
                ),
                actual: circle_snapshot_slot_prefix(
                    meta.circle_id,
                    &author.device_id.to_string(),
                    meta.generation,
                ),
            });
        }
        validate_circle_snapshot_generation(meta.generation, meta.successor.predecessor.as_ref())?;
        validate_circle_snapshot_state(&meta.control, &meta.bootstrap.coverage)?;
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
}

fn validate_circle_snapshot_state(
    control: &CircleControlCoord,
    coverage: &CommitFrontier,
) -> Result<(), StoreProtocolError> {
    control
        .validate()
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    super::validation::validate_commit_frontier(coverage)
}

fn validate_circle_snapshot_generation(
    generation: u64,
    predecessor: Option<&CircleSnapshotRef>,
) -> Result<(), StoreProtocolError> {
    match (generation, predecessor) {
        (0, None) => Ok(()),
        (0, Some(_)) | (_, None) => Err(StoreProtocolError::Malformed(
            "Circle snapshot generation and predecessor disagree".to_string(),
        )),
        (generation, Some(predecessor)) => {
            let expected = predecessor.generation.checked_add(1).ok_or_else(|| {
                StoreProtocolError::Malformed("Circle snapshot generation overflow".to_string())
            })?;
            if generation != expected {
                return Err(StoreProtocolError::Malformed(
                    "Circle snapshot generation does not follow its predecessor".to_string(),
                ));
            }
            Ok(())
        }
    }
}
