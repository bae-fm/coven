use super::validation::require_version;
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
pub(crate) fn circle_snapshot_stream_activation(
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
pub struct CircleSnapshotMeta {
    pub version: u32,
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
    pub signature: String,
}

#[derive(Serialize)]
struct CircleSnapshotSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    author_registration: &'a StoreDeviceRegistrationRef,
    control: &'a CircleControlCoord,
    epoch_id: CircleEpochId,
    key_fingerprint: KeyFingerprint,
    generation: u64,
    bootstrap: &'a CircleBootstrapRef,
    created_at: &'a str,
    successor: &'a CircleSnapshotSuccessorLink,
}

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
        let mut meta = Self {
            version: STORE_PROTOCOL_VERSION,
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
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &meta.canonical_signed_bytes());
        meta.signature = signature;
        Ok(meta)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            CIRCLE_SNAPSHOT_DOMAIN,
            &CircleSnapshotSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                circle_id: self.circle_id,
                author_registration: &self.author_registration,
                control: &self.control,
                epoch_id: self.epoch_id,
                key_fingerprint: self.key_fingerprint,
                generation: self.generation,
                bootstrap: &self.bootstrap,
                created_at: &self.created_at,
                successor: &self.successor,
            },
        )
    }

    pub fn snapshot_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn semantic_hash_from_bytes(bytes: &[u8]) -> Result<ObjectHash, StoreProtocolError> {
        let meta: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        Ok(meta.snapshot_hash())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("CircleSnapshotMeta serialization cannot fail")
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
        let meta: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(meta.version)?;
        crate::sync::store_objects::verify_store_root(
            expected_store_root_hash,
            meta.store_root_hash,
        )?;
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
