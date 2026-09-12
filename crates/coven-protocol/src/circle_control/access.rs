use super::*;

/// Exact Circle bootstrap rows changeset offered when one recipient becomes
/// active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleBootstrapRef {
    pub coverage: CommitFrontier,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
    pub image: SnapshotImageRef,
    pub blobs: Vec<crate::blob::RowBlobRef>,
}

impl CircleBootstrapRef {
    pub(crate) fn verify_for_access(&self, access: &CircleAccessLeaf) -> bool {
        if crate::store_commit::validate_commit_frontier(&self.coverage).is_err() {
            return false;
        }
        let blobs_are_canonical = self.blobs.windows(2).all(|pair| {
            serde_json::to_vec(&pair[0]).expect("row blob reference serialization cannot fail")
                < serde_json::to_vec(&pair[1])
                    .expect("row blob reference serialization cannot fail")
        });
        if !blobs_are_canonical
            || self.blobs.iter().any(|blob| {
                !matches!(
                    blob.authority(),
                    crate::blob::RowBlobAuthority::Remote(
                        crate::audience_package::PackageAudience::Circle {
                            circle_id,
                            ..
                        }
                    ) if *circle_id == access.circle_id
                ) || blob.stored().is_none_or(|stored| {
                    stored.locator().audience()
                        != crate::blob::locator::RemoteAudience::Circle(access.circle_id)
                })
            })
        {
            return false;
        }
        let semantic_prefix = crate::store_commit::circle_bootstrap_image_semantic_prefix(
            access.circle_id,
            access.candidate_family,
            &access.owner_pubkey,
            access.epoch_id,
            &access.recipient_slot,
            self.image.image_hash,
        );
        self.image.object.slot().logical_key() == format!("{semantic_prefix}.changeset")
    }
}

/// The exact retained bootstrap coverage a recipient device's live Circle
/// projection was seeded from: the activating Store commit, the control it
/// activated under, and the bootstrap reference (its exact cut and image hash
/// live inside that reference, not re-declared here). Names one row of
/// `circle_bootstrap_coverage`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleBootstrapCoverageRef {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub activation_commit: StoreBatchCommitRef,
    pub bootstrap: CircleBootstrapRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleAccessDisposition {
    Active {
        keyring: String,
        key_fingerprint: KeyFingerprint,
        roster: CircleRosterStateRef,
        bootstrap: Option<CircleBootstrapRef>,
    },
    Inactive,
}

/// The wire body of one recipient's Circle access leaf. Every field here is
/// signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessLeafBody {
    pub store_root_hash: ObjectHash,
    pub candidate_family: crate::store_commit::CandidateFamilyId,
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub owner_pubkey: String,
    pub recipient_pubkey: String,
    pub recipient_slot: String,
    pub disposition: CircleAccessDisposition,
    pub store_membership: StoreMembershipStateRef,
}

impl SignedBody for CircleAccessLeafBody {
    const DOMAIN: &'static [u8] = ACCESS_DOMAIN;
}

pub type CircleAccessLeaf = Signed<CircleAccessLeafBody>;

impl CircleAccessLeaf {
    pub fn verify_signature(&self) -> bool {
        self.verify_by(&self.owner_pubkey).is_ok()
    }

    pub(crate) fn verify_for_control(
        &self,
        control: &PreparedCircleControl,
        candidate_family: crate::store_commit::CandidateFamilyId,
    ) -> bool {
        self.verify_signature()
            && self.store_root_hash == control.value.store_root_hash
            && self.candidate_family == candidate_family
            && self.circle_id == control.value.circle_id
            && self.epoch_id == control.value.epoch_id()
            && self.store_membership == control.value.store_membership_state_ref()
            && match &self.disposition {
                CircleAccessDisposition::Active {
                    keyring,
                    key_fingerprint,
                    roster,
                    bootstrap,
                } => {
                    roster == &control.value.roster_state_ref()
                        && *key_fingerprint == control.value.key_fingerprint()
                        && MasterKeyring::from_serialized(keyring).is_ok_and(|keyring| {
                            EncryptionService::from(keyring).seal_key_fingerprint()
                                == *key_fingerprint
                        })
                        && bootstrap
                            .as_ref()
                            .is_none_or(|bootstrap| bootstrap.verify_for_access(self))
                }
                CircleAccessDisposition::Inactive => true,
            }
            && self.owner_pubkey == control.value.author_pubkey
    }
}

/// One recipient's sealed access entry inside a signed Circle control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessEntry {
    /// Hex-encoded sealed box (`seal_box_encrypt` output) carrying the
    /// Owner-signed `CircleAccessLeaf` JSON for this slot's recipient.
    pub sealed: String,
    /// Digest of the signed leaf's canonical JSON, binding a locally retained
    /// decrypted leaf to this exact control entry.
    pub value_hash: ObjectHash,
}

/// Every Store member's sealed access entry at one control, keyed by the
/// opaque recipient slot. Canonical by construction (`BTreeMap`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CircleAccessMap(BTreeMap<String, CircleAccessEntry>);

impl CircleAccessMap {
    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    /// One entry per leaf; a repeated recipient slot is a contradiction in the
    /// leaf set the caller sealed.
    pub fn from_leaves(leaves: &[PreparedAccessLeaf]) -> Result<Self, CircleTransitionError> {
        let mut map = Self::empty();
        for leaf in leaves {
            if map
                .insert(leaf.value.recipient_slot.clone(), leaf.entry())
                .is_some()
            {
                return Err(CircleTransitionError::InvalidCurrentState);
            }
        }
        Ok(map)
    }

    pub fn insert(
        &mut self,
        recipient_slot: String,
        entry: CircleAccessEntry,
    ) -> Option<CircleAccessEntry> {
        self.0.insert(recipient_slot, entry)
    }

    pub fn entry(&self, recipient_slot: &str) -> Option<&CircleAccessEntry> {
        self.0.get(recipient_slot)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// The value an epoch-close outcome commits to.
    pub fn digest(&self) -> ObjectHash {
        ObjectHash::digest(&crate::store_commit::domain_json(ACCESS_MAP_DOMAIN, self))
    }

    pub(crate) fn verify_shape(&self) -> bool {
        self.0
            .values()
            .all(|entry| hex::decode(&entry.sealed).is_ok_and(|bytes| !bytes.is_empty()))
    }
}
