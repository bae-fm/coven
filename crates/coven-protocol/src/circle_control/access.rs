use super::*;

/// Exact Circle database image offered when one recipient becomes active.
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
        self.image.object.slot().logical_key() == format!("{semantic_prefix}.db")
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
    pub leaf_id: AccessLeafId,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MerkleStep {
    Left(ObjectHash),
    Right(ObjectHash),
}

fn merkle_parent(left: ObjectHash, right: ObjectHash) -> ObjectHash {
    let mut bytes = Vec::with_capacity(1 + 64);
    bytes.push(1);
    bytes.extend_from_slice(left.as_bytes());
    bytes.extend_from_slice(right.as_bytes());
    ObjectHash::digest(&bytes)
}

pub fn verify_merkle_proof(mut hash: ObjectHash, proof: &[MerkleStep], root: ObjectHash) -> bool {
    for step in proof {
        hash = match step {
            MerkleStep::Left(left) => merkle_parent(*left, hash),
            MerkleStep::Right(right) => merkle_parent(hash, *right),
        };
    }
    hash == root
}

pub fn merkle_root_and_proofs(hashes: &[ObjectHash]) -> (ObjectHash, Vec<Vec<MerkleStep>>) {
    assert!(
        !hashes.is_empty(),
        "a circle control has at least one access leaf"
    );
    let mut indexed = hashes
        .iter()
        .copied()
        .enumerate()
        .collect::<Vec<(usize, ObjectHash)>>();
    indexed.sort_by_key(|(index, hash)| (*hash, *index));
    let mut proofs = vec![Vec::new(); hashes.len()];
    let mut layer = indexed
        .into_iter()
        .map(|(index, hash)| (hash, vec![index]))
        .collect::<Vec<_>>();
    while layer.len() > 1 {
        let mut next = Vec::with_capacity(layer.len().div_ceil(2));
        for pair in layer.chunks(2) {
            let (left_hash, left_indices) = &pair[0];
            if let Some((right_hash, right_indices)) = pair.get(1) {
                for index in left_indices {
                    proofs[*index].push(MerkleStep::Right(*right_hash));
                }
                for index in right_indices {
                    proofs[*index].push(MerkleStep::Left(*left_hash));
                }
                let mut indices = left_indices.clone();
                indices.extend(right_indices);
                next.push((merkle_parent(*left_hash, *right_hash), indices));
            } else {
                for index in left_indices {
                    proofs[*index].push(MerkleStep::Right(*left_hash));
                }
                next.push((merkle_parent(*left_hash, *left_hash), left_indices.clone()));
            }
        }
        layer = next;
    }
    (layer[0].0, proofs)
}
