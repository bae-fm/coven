//! Circle metadata, access records, controls, and creation objects.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::causal_grants::AuthorStreamId;
use super::circle::CircleEpochCloseId;
use super::circle::{generated_id_digest, AccessLeafId, CircleEpochId, CircleId};
use super::circle_roster::{
    CircleAuthorStreamKey, CircleGrantCreationAuthority, CircleMaterializedRoster,
    CircleRosterChain, CircleRosterEntry, CircleRosterError, CircleRosterHead, CircleRosterHeadRef,
    CircleRosterStateRef, MergeCircleRosterStateRef, ResolvedCircleRoster,
};
use super::membership::{MemberRole, MembershipGrantCreationAuthority, MembershipGrantId};
use super::membership::{MembershipHeadRef, StoreMembershipConflictResolutionRef};
use super::store_commit::{
    CommitFrontier, ObjectHash, OwnerRecoveryCursor, SnapshotImageRef, StoreBatchCommitRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreDeviceStateRef, SuccessorLink,
    STORE_PROTOCOL_VERSION,
};
use crate::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::ObjectSlot;
use crate::storage::ExactObjectRef;

const RECIPIENT_SLOT_DOMAIN: &[u8] = b"coven.circle-recipient-slot.v1\0";
const METADATA_DOMAIN: &str = "coven.circle-metadata.v1";
const METADATA_HEAD_DOMAIN: &str = "coven.circle-metadata-head.v1";
const ACCESS_DOMAIN: &str = "coven.circle-access-leaf.v1";
const CONTROL_DOMAIN: &str = "coven.circle-control.v1";
const ENVELOPE_DOMAIN: &str = "coven.circle-access-envelope.v1";
const OWNER_GRANT_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-owner-grant-id-generation.v1\0";

/// Exact coordinate of one signed circle control entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlCoord {
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub control_hash: ObjectHash,
}

impl CircleControlCoord {
    pub fn control_hash(&self) -> ObjectHash {
        self.control_hash
    }

    pub fn validate(&self) -> Result<(), CircleControlCoordError> {
        if self.device_id.is_empty() || self.author_pubkey.is_empty() || self.seq == 0 {
            Err(CircleControlCoordError)
        } else {
            Ok(())
        }
    }

    pub fn stream_key(&self) -> CircleAuthorStreamKey {
        CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
        }
    }

    /// A well-formed coordinate that names no real control, for API dispatch tests
    /// that only need a value to send through the command channel.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn placeholder(seed: u8) -> Self {
        let hash = ObjectHash::digest(&[seed]);
        Self {
            device_id: format!("device-{seed}"),
            stream_id: AuthorStreamId::from_digest(hash),
            author_pubkey: format!("pubkey-{seed}"),
            author_owner_grant: MembershipGrantId(hash),
            seq: 1,
            control_hash: hash,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("circle control coordinate has an empty device/author or zero sequence/generation")]
pub struct CircleControlCoordError;

/// The exact Store membership state whose identities require access dispositions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipStateRef {
    pub heads: Vec<MembershipHeadRef>,
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
    pub recovery: Vec<OwnerRecoveryCursor>,
    pub state_hash: ObjectHash,
}

impl StoreMembershipStateRef {
    pub fn from_parts(
        mut heads: Vec<MembershipHeadRef>,
        mut resolutions: Vec<StoreMembershipConflictResolutionRef>,
        recovery: Vec<OwnerRecoveryCursor>,
        membership_state_hash: ObjectHash,
    ) -> Result<Self, super::store_commit::StoreProtocolError> {
        heads.sort();
        resolutions.sort();
        let recovery = super::store_commit::canonical_recovery_cursors(recovery)?;
        Ok(Self {
            heads,
            resolutions,
            state_hash: membership_state_ref_hash(membership_state_hash, &recovery),
            recovery,
        })
    }

    pub fn state_hash(&self) -> ObjectHash {
        self.state_hash
    }

    pub fn recovery(&self) -> &[OwnerRecoveryCursor] {
        &self.recovery
    }

    pub(crate) fn validate_shape(&self) -> Result<(), super::store_commit::StoreProtocolError> {
        super::store_commit::validate_recovery_cursors(self.recovery())?;
        if self.heads.windows(2).any(|pair| pair[0] >= pair[1])
            || self.resolutions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(super::store_commit::StoreProtocolError::Malformed(
                "Store membership state reference is not canonical".to_string(),
            ));
        }
        Ok(())
    }
}

fn membership_state_ref_hash(
    membership_state_hash: ObjectHash,
    recovery: &[OwnerRecoveryCursor],
) -> ObjectHash {
    ObjectHash::digest(
        &serde_json::to_vec(&(
            "coven.store-membership-state-ref.v1",
            membership_state_hash,
            recovery,
        ))
        .expect("Store membership state hash serialization cannot fail"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataCoord {
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub metadata_hash: ObjectHash,
}

impl CircleMetadataCoord {
    pub fn stream_key(&self) -> CircleAuthorStreamKey {
        CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleMetadata {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub name: String,
    pub seq: u64,
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleMetadataCoord>,
    pub metadata_stamp: String,
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub author_roster: CircleRosterStateRef,
    pub key_fingerprint: KeyFingerprint,
    pub signature: String,
}

impl CircleMetadata {
    fn founder(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        epoch_id: CircleEpochId,
        name: &str,
        metadata_stamp: &str,
        device_id: &str,
        stream_id: AuthorStreamId,
        owner_grant: MembershipGrantId,
        author_roster: CircleRosterStateRef,
        key_fingerprint: KeyFingerprint,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        if name.trim().is_empty() {
            return Err(CircleTransitionError::EmptyName);
        }
        let author_pubkey = keys::public_key_hex(signer);
        let mut metadata = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            epoch_id,
            name: name.to_string(),
            seq: 1,
            previous_hash: None,
            dependencies: Vec::new(),
            metadata_stamp: metadata_stamp.to_string(),
            author_pubkey,
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant: owner_grant,
            author_roster,
            key_fingerprint,
            signature: String::new(),
        };
        metadata.signature = keys::sign_hex(signer, &metadata.canonical_bytes()).1;
        Ok(metadata)
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            epoch_id: CircleEpochId,
            name: &'a str,
            seq: u64,
            previous_hash: Option<ObjectHash>,
            dependencies: &'a [CircleMetadataCoord],
            metadata_stamp: &'a str,
            author_pubkey: &'a str,
            device_id: &'a str,
            stream_id: AuthorStreamId,
            author_owner_grant: &'a MembershipGrantId,
            author_roster: &'a CircleRosterStateRef,
            key_fingerprint: KeyFingerprint,
        }
        serde_json::to_vec(&Signed {
            domain: METADATA_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            epoch_id: self.epoch_id,
            name: &self.name,
            seq: self.seq,
            previous_hash: self.previous_hash,
            dependencies: &self.dependencies,
            metadata_stamp: &self.metadata_stamp,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
            stream_id: self.stream_id,
            author_owner_grant: &self.author_owner_grant,
            author_roster: &self.author_roster,
            key_fingerprint: self.key_fingerprint,
        })
        .expect("circle metadata serialization cannot fail")
    }

    pub(crate) fn metadata_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle metadata serialization cannot fail"),
        )
    }

    pub(crate) fn coord(&self) -> CircleMetadataCoord {
        CircleMetadataCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            metadata_hash: self.metadata_hash(),
        }
    }

    pub(crate) fn verify(&self) -> bool {
        let position_is_valid = (self.seq == 1
            && self.previous_hash.is_none()
            && self
                .dependencies
                .iter()
                .all(|dependency| dependency.stream_key() != self.coord().stream_key()))
            || (self.seq > 1 && self.previous_hash.is_some());
        self.version == STORE_PROTOCOL_VERSION
            && !self.name.trim().is_empty()
            && position_is_valid
            && self
                .dependencies
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleMetadataHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub tip_hash: ObjectHash,
    pub tip: ExactObjectRef,
    pub successor: SuccessorLink,
    pub signature: String,
}

impl CircleMetadataHead {
    pub(crate) fn signed(
        metadata: &CircleMetadata,
        tip: ExactObjectRef,
        successor: SuccessorLink,
        signer: &UserKeypair,
    ) -> Self {
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: metadata.store_root_hash,
            circle_id: metadata.circle_id,
            author_pubkey: metadata.author_pubkey.clone(),
            device_id: metadata.device_id.clone(),
            stream_id: metadata.stream_id,
            author_owner_grant: metadata.author_owner_grant.clone(),
            seq: metadata.seq,
            tip_hash: metadata.metadata_hash(),
            tip,
            successor,
            signature: String::new(),
        };
        head.signature = keys::sign_hex(signer, &head.canonical_bytes()).1;
        head
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            author_pubkey: &'a str,
            device_id: &'a str,
            stream_id: AuthorStreamId,
            author_owner_grant: &'a MembershipGrantId,
            seq: u64,
            tip_hash: ObjectHash,
            tip: &'a ExactObjectRef,
            successor: &'a SuccessorLink,
        }
        serde_json::to_vec(&Signed {
            domain: METADATA_HEAD_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
            stream_id: self.stream_id,
            author_owner_grant: &self.author_owner_grant,
            seq: self.seq,
            tip_hash: self.tip_hash,
            tip: &self.tip,
            successor: &self.successor,
        })
        .expect("circle metadata head serialization cannot fail")
    }

    pub(crate) fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle metadata head serialization cannot fail"),
        )
    }

    pub(crate) fn coord(&self) -> CircleMetadataCoord {
        CircleMetadataCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            metadata_hash: self.tip_hash,
        }
    }

    pub(crate) fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.seq > 0
            && !self.device_id.is_empty()
            && self.device_id == registration.device_id.to_string()
            && keys::verify_signature_hex(
                &registration.device_signing_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataHeadRef {
    pub coord: CircleMetadataCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleMetadataHeadRef {
    pub(crate) fn from_stored_head(head: &CircleMetadataHead, object: ExactObjectRef) -> Self {
        Self {
            coord: head.coord(),
            head_hash: head.head_hash(),
            object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeCircleMetadataStateRef {
    pub heads: Vec<CircleMetadataHeadRef>,
    pub selected: CircleMetadataCoord,
    pub state_hash: ObjectHash,
}

pub(crate) type CircleMetadataStateRef = MergeCircleMetadataStateRef;

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
        if super::store_commit::validate_commit_frontier(&self.coverage).is_err() {
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
                        super::audience_package::PackageAudience::Circle {
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
        let semantic_prefix = super::store_commit::circle_bootstrap_image_semantic_prefix(
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
pub(crate) enum CircleAccessDisposition {
    Active {
        keyring: String,
        key_fingerprint: KeyFingerprint,
        roster: CircleRosterStateRef,
        bootstrap: Option<CircleBootstrapRef>,
    },
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleAccessLeaf {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub candidate_family: super::store_commit::CandidateFamilyId,
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub leaf_id: AccessLeafId,
    pub owner_pubkey: String,
    pub recipient_pubkey: String,
    pub recipient_slot: String,
    pub disposition: CircleAccessDisposition,
    pub store_membership: StoreMembershipStateRef,
    pub signature: String,
}

impl CircleAccessLeaf {
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            candidate_family: super::store_commit::CandidateFamilyId,
            circle_id: CircleId,
            epoch_id: CircleEpochId,
            leaf_id: AccessLeafId,
            owner_pubkey: &'a str,
            recipient_pubkey: &'a str,
            recipient_slot: &'a str,
            disposition: &'a CircleAccessDisposition,
            store_membership: &'a StoreMembershipStateRef,
        }
        serde_json::to_vec(&Signed {
            domain: ACCESS_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            candidate_family: self.candidate_family,
            circle_id: self.circle_id,
            epoch_id: self.epoch_id,
            leaf_id: self.leaf_id,
            owner_pubkey: &self.owner_pubkey,
            recipient_pubkey: &self.recipient_pubkey,
            recipient_slot: &self.recipient_slot,
            disposition: &self.disposition,
            store_membership: &self.store_membership,
        })
        .expect("circle access serialization cannot fail")
    }

    pub(crate) fn verify_signature(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && keys::verify_signature_hex(
                &self.owner_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn verify_for_control(
        &self,
        control: &PreparedCircleControl,
        candidate_family: super::store_commit::CandidateFamilyId,
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
pub(crate) enum MerkleStep {
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

pub(crate) fn verify_merkle_proof(
    mut hash: ObjectHash,
    proof: &[MerkleStep],
    root: ObjectHash,
) -> bool {
    for step in proof {
        hash = match step {
            MerkleStep::Left(left) => merkle_parent(*left, hash),
            MerkleStep::Right(right) => merkle_parent(hash, *right),
        };
    }
    hash == root
}

pub(crate) fn merkle_root_and_proofs(hashes: &[ObjectHash]) -> (ObjectHash, Vec<Vec<MerkleStep>>) {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeCircleControlOrder {
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub previous_control_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleControlCoord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveCircleEpochCore {
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    pub owners: Vec<String>,
    pub access_root: ObjectHash,
    pub origin: CircleEpochOrigin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochOrigin {
    Founder,
    Closed {
        closed_epoch_id: CircleEpochId,
        close_control: CircleControlCoord,
        close_id: CircleEpochCloseId,
        outcome_hash: ObjectHash,
        cutoff: CommitFrontier,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeActiveCircleEpoch {
    pub common: ActiveCircleEpochCore,
    pub metadata: MergeCircleMetadataStateRef,
    pub roster: MergeCircleRosterStateRef,
    pub store_membership: StoreMembershipStateRef,
    pub covered_control_heads: Vec<MergeCircleControlHeadRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseParticipant {
    pub registration: StoreDeviceRegistrationRef,
    pub response_slot: ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseIntent {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub epoch_id: CircleEpochId,
    pub predecessor_roster: MergeCircleRosterStateRef,
    pub removal: CircleRosterEntry,
    pub remaining_roster_state_hash: ObjectHash,
    pub owner_pubkey: String,
    pub signature: String,
}

impl CircleEpochCloseIntent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn signed(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        close_id: CircleEpochCloseId,
        epoch_id: CircleEpochId,
        predecessor_roster: MergeCircleRosterStateRef,
        removal: CircleRosterEntry,
        remaining_roster_state_hash: ObjectHash,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let owner_pubkey = keys::public_key_hex(signer);
        let mut intent = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            close_id,
            epoch_id,
            predecessor_roster,
            removal,
            remaining_roster_state_hash,
            owner_pubkey,
            signature: String::new(),
        };
        if !intent.verify_shape() {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        intent.signature = keys::sign_hex(signer, &intent.canonical_bytes()).1;
        Ok(intent)
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            close_id: CircleEpochCloseId,
            epoch_id: CircleEpochId,
            predecessor_roster: &'a MergeCircleRosterStateRef,
            removal: &'a CircleRosterEntry,
            remaining_roster_state_hash: ObjectHash,
            owner_pubkey: &'a str,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-epoch-close-intent.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            close_id: self.close_id,
            epoch_id: self.epoch_id,
            predecessor_roster: &self.predecessor_roster,
            removal: &self.removal,
            remaining_roster_state_hash: self.remaining_roster_state_hash,
            owner_pubkey: &self.owner_pubkey,
        })
        .expect("Circle epoch-close intent serialization cannot fail")
    }

    fn verify_shape(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.removal.verify()
            && self.removal.store_root_hash == self.store_root_hash
            && self.removal.circle_id == self.circle_id
            && self.removal.author_pubkey == self.owner_pubkey
            && matches!(
                self.removal.change,
                super::circle_roster::CircleRosterChange::RemoveMember { .. }
            )
    }

    pub(crate) fn verify(&self) -> bool {
        self.verify_shape()
            && keys::verify_signature_hex(
                &self.owner_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn intent_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("Circle epoch-close intent serialization cannot fail"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleEpochCloseIntentRef {
    pub close_id: CircleEpochCloseId,
    pub intent_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseIntentRef {
    pub(crate) fn from_intent(
        intent: &CircleEpochCloseIntent,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        let reference = Self {
            close_id: intent.close_id,
            intent_hash: intent.intent_hash(),
            object,
        };
        if reference.object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_intent_semantic_prefix(
                    intent.circle_id,
                    intent.close_id,
                    intent.intent_hash(),
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(reference)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochClose {
    pub close_id: CircleEpochCloseId,
    pub frozen_epoch: MergeActiveCircleEpoch,
    pub intent: CircleEpochCloseIntentRef,
    pub frozen_device_state: StoreDeviceStateRef,
    pub participants: Vec<CircleEpochCloseParticipant>,
    pub provisional_frontier: CommitFrontier,
    pub outcome_slot: ObjectSlot,
}

impl CircleEpochClose {
    fn verify_shape(&self, circle_id: CircleId) -> bool {
        super::store_commit::validate_commit_frontier(&self.provisional_frontier).is_ok()
            && self.intent.close_id == self.close_id
            && !self.participants.is_empty()
            && self
                .participants
                .windows(2)
                .all(|pair| pair[0].registration.device_id < pair[1].registration.device_id)
            && self.participants.iter().all(|participant| {
                participant.response_slot.logical_key()
                    == format!(
                        "{}.json",
                        circle_epoch_close_response_semantic_prefix(
                            circle_id,
                            self.close_id,
                            participant.registration.device_id,
                        )
                    )
            })
            && self.outcome_slot.logical_key()
                == format!(
                    "{}.json",
                    circle_epoch_close_outcome_semantic_prefix(circle_id, self.close_id)
                )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseResponse {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub registration: StoreDeviceRegistrationRef,
    pub frontier: CommitFrontier,
    pub signature: String,
}

impl CircleEpochCloseResponse {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        registration: StoreDeviceRegistrationRef,
        frontier: CommitFrontier,
        author: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let mut response = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: control.value.store_root_hash,
            circle_id: control.value.circle_id,
            close_id: close.close_id,
            close_control: control.coord.clone(),
            registration,
            frontier,
            signature: String::new(),
        };
        response.signature = keys::sign_hex(signer, &response.canonical_bytes()).1;
        if !response.verify_for(control, author) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(response)
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            close_id: CircleEpochCloseId,
            close_control: &'a CircleControlCoord,
            registration: &'a StoreDeviceRegistrationRef,
            frontier: &'a CommitFrontier,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-epoch-close-response.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            close_id: self.close_id,
            close_control: &self.close_control,
            registration: &self.registration,
            frontier: &self.frontier,
        })
        .expect("Circle epoch-close response serialization cannot fail")
    }

    pub(crate) fn verify_for(
        &self,
        control: &PreparedCircleControl,
        author: &StoreDeviceRegistration,
    ) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        self.version == STORE_PROTOCOL_VERSION
            && control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && super::store_commit::validate_commit_frontier(&self.frontier).is_ok()
            && self.frontier.covers(&close.provisional_frontier)
            && self.registration.verify_registration(author).is_ok()
            && author.store_root.store_root_hash == self.store_root_hash
            && close
                .participants
                .iter()
                .any(|participant| participant.registration == self.registration)
            && keys::verify_signature_hex(
                &author.device_signing_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn response_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self)
                .expect("Circle epoch-close response serialization cannot fail"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseResponseRef {
    pub registration: StoreDeviceRegistrationRef,
    pub frontier: CommitFrontier,
    pub response_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseResponseRef {
    pub(crate) fn from_response(
        response: &CircleEpochCloseResponse,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_response_semantic_prefix(
                    response.circle_id,
                    response.close_id,
                    response.registration.device_id,
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            registration: response.registration.clone(),
            frontier: response.frontier.clone(),
            response_hash: response.response_hash(),
            object,
        })
    }

    pub(crate) fn verify_response(&self, response: &CircleEpochCloseResponse) -> bool {
        self.registration == response.registration
            && self.frontier == response.frontier
            && self.response_hash == response.response_hash()
    }
}

/// One Owner-signed exclusion of an unavailable participant device. It competes
/// at that device's create-once response slot; activating it excludes the device
/// from the close cutoff and forces it to reset from the successor bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseExclusion {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub excluded: StoreDeviceRegistrationRef,
    pub owner_pubkey: String,
    pub signature: String,
}

impl CircleEpochCloseExclusion {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        excluded: StoreDeviceRegistrationRef,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let mut exclusion = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: control.value.store_root_hash,
            circle_id: control.value.circle_id,
            close_id: close.close_id,
            close_control: control.coord.clone(),
            excluded,
            owner_pubkey: keys::public_key_hex(signer),
            signature: String::new(),
        };
        if !exclusion.verify_shape(control) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        exclusion.signature = keys::sign_hex(signer, &exclusion.canonical_bytes()).1;
        Ok(exclusion)
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            close_id: CircleEpochCloseId,
            close_control: &'a CircleControlCoord,
            excluded: &'a StoreDeviceRegistrationRef,
            owner_pubkey: &'a str,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-epoch-close-exclusion.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            close_id: self.close_id,
            close_control: &self.close_control,
            excluded: &self.excluded,
            owner_pubkey: &self.owner_pubkey,
        })
        .expect("Circle epoch-close exclusion serialization cannot fail")
    }

    fn verify_shape(&self, control: &PreparedCircleControl) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        self.version == STORE_PROTOCOL_VERSION
            && control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && close
                .participants
                .iter()
                .any(|participant| participant.registration == self.excluded)
            && close
                .frozen_epoch
                .common
                .owners
                .contains(&self.owner_pubkey)
    }

    pub(crate) fn verify_for(&self, control: &PreparedCircleControl) -> bool {
        self.verify_shape(control)
            && keys::verify_signature_hex(
                &self.owner_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn exclusion_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self)
                .expect("Circle epoch-close exclusion serialization cannot fail"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseExclusionRef {
    pub registration: StoreDeviceRegistrationRef,
    pub exclusion_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseExclusionRef {
    pub(crate) fn from_exclusion(
        exclusion: &CircleEpochCloseExclusion,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_response_semantic_prefix(
                    exclusion.circle_id,
                    exclusion.close_id,
                    exclusion.excluded.device_id,
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            registration: exclusion.excluded.clone(),
            exclusion_hash: exclusion.exclusion_hash(),
            object,
        })
    }

    pub(crate) fn verify_exclusion(&self, exclusion: &CircleEpochCloseExclusion) -> bool {
        self.registration == exclusion.excluded && self.exclusion_hash == exclusion.exclusion_hash()
    }
}

/// The exactly-one value a participant's create-once close-response slot holds:
/// the device's own signed response, or an Owner-signed exclusion of that device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochCloseResponseSlotValue {
    Response(CircleEpochCloseResponse),
    Exclusion(CircleEpochCloseExclusion),
}

impl CircleEpochCloseResponseSlotValue {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self)
            .expect("Circle epoch-close response slot value serialization cannot fail")
    }

    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, CircleTransitionError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        if value.to_bytes() != bytes {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(value)
    }
}

/// One participant's contribution to a close outcome: either its verified device
/// response (whose frontier joins the cutoff) or an Owner exclusion (which does
/// not). The outcome carries one per participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochCloseSettlement {
    Response(CircleEpochCloseResponseRef),
    Exclusion(CircleEpochCloseExclusionRef),
}

impl CircleEpochCloseSettlement {
    pub(crate) fn registration(&self) -> &StoreDeviceRegistrationRef {
        match self {
            Self::Response(reference) => &reference.registration,
            Self::Exclusion(reference) => &reference.registration,
        }
    }

    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Response(reference) => &reference.object,
            Self::Exclusion(reference) => &reference.object,
        }
    }

    pub(crate) fn response_frontier(&self) -> Option<&CommitFrontier> {
        match self {
            Self::Response(reference) => Some(&reference.frontier),
            Self::Exclusion(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochSuccessor {
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    pub owners: Vec<String>,
    pub access_root: ObjectHash,
    pub metadata: MergeCircleMetadataStateRef,
    pub roster: MergeCircleRosterStateRef,
    pub store_membership: StoreMembershipStateRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseOutcome {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub intent: CircleEpochCloseIntentRef,
    pub responses: Vec<CircleEpochCloseSettlement>,
    pub cutoff: CommitFrontier,
    pub successor: CircleEpochSuccessor,
    pub owner_pubkey: String,
    pub signature: String,
}

impl CircleEpochCloseOutcome {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        intent: &CircleEpochCloseIntent,
        responses: Vec<CircleEpochCloseSettlement>,
        successor: CircleEpochSuccessor,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let cutoff = responses
            .iter()
            .filter_map(CircleEpochCloseSettlement::response_frontier)
            .try_fold(close.provisional_frontier.clone(), |cutoff, frontier| {
                cutoff.join(frontier.clone())
            })
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        let mut outcome = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: control.value.store_root_hash,
            circle_id: control.value.circle_id,
            close_id: close.close_id,
            close_control: control.coord.clone(),
            intent: close.intent.clone(),
            responses,
            cutoff,
            successor,
            owner_pubkey: keys::public_key_hex(signer),
            signature: String::new(),
        };
        if !outcome.verify_shape(control) || !outcome.verify_intent(intent) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        outcome.signature = keys::sign_hex(signer, &outcome.canonical_bytes()).1;
        Ok(outcome)
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            close_id: CircleEpochCloseId,
            close_control: &'a CircleControlCoord,
            intent: &'a CircleEpochCloseIntentRef,
            responses: &'a [CircleEpochCloseSettlement],
            cutoff: &'a CommitFrontier,
            successor: &'a CircleEpochSuccessor,
            owner_pubkey: &'a str,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-epoch-close-outcome.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            close_id: self.close_id,
            close_control: &self.close_control,
            intent: &self.intent,
            responses: &self.responses,
            cutoff: &self.cutoff,
            successor: &self.successor,
            owner_pubkey: &self.owner_pubkey,
        })
        .expect("Circle epoch-close outcome serialization cannot fail")
    }

    fn verify_shape(&self, control: &PreparedCircleControl) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        let responses_are_canonical = self
            .responses
            .windows(2)
            .all(|pair| pair[0].registration().device_id < pair[1].registration().device_id);
        let responses_match_participants =
            self.responses.len() == close.participants.len()
                && self.responses.iter().zip(&close.participants).all(
                    |(settlement, participant)| {
                        settlement.registration() == &participant.registration
                            && settlement.object().slot() == &participant.response_slot
                            && settlement
                                .response_frontier()
                                .is_none_or(|frontier| frontier.covers(&close.provisional_frontier))
                    },
                );
        let expected_cutoff = self
            .responses
            .iter()
            .filter_map(CircleEpochCloseSettlement::response_frontier)
            .try_fold(close.provisional_frontier.clone(), |cutoff, frontier| {
                cutoff.join(frontier.clone())
            });
        self.version == STORE_PROTOCOL_VERSION
            && control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && self.intent == close.intent
            && responses_are_canonical
            && responses_match_participants
            && expected_cutoff.is_ok_and(|cutoff| cutoff == self.cutoff)
            && super::store_commit::validate_commit_frontier(&self.cutoff).is_ok()
            && self.successor.epoch_id != close.frozen_epoch.common.epoch_id
            && self.successor.key_fingerprint != close.frozen_epoch.common.key_fingerprint
            && !self.successor.owners.is_empty()
            && self
                .successor
                .owners
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            && close
                .frozen_epoch
                .common
                .owners
                .contains(&self.owner_pubkey)
    }

    fn verify_intent(&self, intent: &CircleEpochCloseIntent) -> bool {
        intent.verify()
            && intent.close_id == self.close_id
            && intent.circle_id == self.circle_id
            && intent.store_root_hash == self.store_root_hash
            && intent.intent_hash() == self.intent.intent_hash
            && self.successor.roster.state_hash == intent.remaining_roster_state_hash
    }

    pub(crate) fn verify_for(
        &self,
        control: &PreparedCircleControl,
        intent: &CircleEpochCloseIntent,
        settlements: &[(
            CircleEpochCloseSettlement,
            CircleEpochCloseResponseSlotValue,
        )],
    ) -> bool {
        self.verify_shape(control)
            && self.verify_intent(intent)
            && self.responses.len() == settlements.len()
            && self
                .responses
                .iter()
                .zip(settlements)
                .all(|(expected, (settlement, slot_value))| {
                    expected == settlement
                        && match (settlement, slot_value) {
                            (
                                CircleEpochCloseSettlement::Response(reference),
                                CircleEpochCloseResponseSlotValue::Response(response),
                            ) => {
                                reference.verify_response(response)
                                    && response.close_control == self.close_control
                            }
                            (
                                CircleEpochCloseSettlement::Exclusion(reference),
                                CircleEpochCloseResponseSlotValue::Exclusion(exclusion),
                            ) => {
                                reference.verify_exclusion(exclusion)
                                    && exclusion.verify_for(control)
                                    && exclusion.close_control == self.close_control
                            }
                            _ => false,
                        }
                })
            && keys::verify_signature_hex(
                &self.owner_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn verify_signature(&self) -> bool {
        keys::verify_signature_hex(&self.owner_pubkey, &self.signature, &self.canonical_bytes())
    }

    pub(crate) fn outcome_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self)
                .expect("Circle epoch-close outcome serialization cannot fail"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleEpochCloseOutcomeRef {
    pub close_id: CircleEpochCloseId,
    pub outcome_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseOutcomeRef {
    pub(crate) fn from_outcome(
        outcome: &CircleEpochCloseOutcome,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_outcome_semantic_prefix(outcome.circle_id, outcome.close_id)
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            close_id: outcome.close_id,
            outcome_hash: outcome.outcome_hash(),
            object,
        })
    }
}

/// One Owner-signed cancellation of an epoch close. It competes at the same
/// create-once outcome slot as the final outcome; activating it reopens the
/// frozen epoch instead of rotating to a successor epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleEpochCloseCancellation {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub close_control: CircleControlCoord,
    pub intent: CircleEpochCloseIntentRef,
    pub owner_pubkey: String,
    pub signature: String,
}

impl CircleEpochCloseCancellation {
    pub(crate) fn signed(
        control: &PreparedCircleControl,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let mut cancellation = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: control.value.store_root_hash,
            circle_id: control.value.circle_id,
            close_id: close.close_id,
            close_control: control.coord.clone(),
            intent: close.intent.clone(),
            owner_pubkey: keys::public_key_hex(signer),
            signature: String::new(),
        };
        if !cancellation.verify_shape(control) {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        cancellation.signature = keys::sign_hex(signer, &cancellation.canonical_bytes()).1;
        Ok(cancellation)
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            close_id: CircleEpochCloseId,
            close_control: &'a CircleControlCoord,
            intent: &'a CircleEpochCloseIntentRef,
            owner_pubkey: &'a str,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-epoch-close-cancellation.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            close_id: self.close_id,
            close_control: &self.close_control,
            intent: &self.intent,
            owner_pubkey: &self.owner_pubkey,
        })
        .expect("Circle epoch-close cancellation serialization cannot fail")
    }

    fn verify_shape(&self, control: &PreparedCircleControl) -> bool {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return false;
        };
        self.version == STORE_PROTOCOL_VERSION
            && control.verify()
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && self.close_id == close.close_id
            && self.close_control == control.coord
            && self.intent == close.intent
            && close
                .frozen_epoch
                .common
                .owners
                .contains(&self.owner_pubkey)
    }

    pub(crate) fn verify_for(&self, control: &PreparedCircleControl) -> bool {
        self.verify_shape(control)
            && keys::verify_signature_hex(
                &self.owner_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn verify_signature(&self) -> bool {
        keys::verify_signature_hex(&self.owner_pubkey, &self.signature, &self.canonical_bytes())
    }

    pub(crate) fn cancellation_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self)
                .expect("Circle epoch-close cancellation serialization cannot fail"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleEpochCloseCancellationRef {
    pub close_id: CircleEpochCloseId,
    pub cancellation_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleEpochCloseCancellationRef {
    pub(crate) fn from_cancellation(
        cancellation: &CircleEpochCloseCancellation,
        object: ExactObjectRef,
    ) -> Result<Self, CircleTransitionError> {
        if object.slot().logical_key()
            != format!(
                "{}.json",
                circle_epoch_close_outcome_semantic_prefix(
                    cancellation.circle_id,
                    cancellation.close_id
                )
            )
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(Self {
            close_id: cancellation.close_id,
            cancellation_hash: cancellation.cancellation_hash(),
            object,
        })
    }
}

/// The exactly-one value the create-once epoch-close outcome slot holds. Readers
/// parse this tagged form and dispatch on the settled arm: a final outcome
/// rotates to a successor epoch, a cancellation reopens the frozen epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleEpochCloseSlotValue {
    Outcome(CircleEpochCloseOutcome),
    Cancellation(CircleEpochCloseCancellation),
}

impl CircleEpochCloseSlotValue {
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Circle epoch-close slot value serialization cannot fail")
    }

    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, CircleTransitionError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        if value.to_bytes() != bytes {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(value)
    }
}

/// A terminal deletion. It freezes the epoch spine it terminated — the same
/// `MergeActiveCircleEpoch` an `EpochClose` freezes — so historical package
/// verification and exact reclamation keep the epoch, key fingerprint, and
/// roster-head spine with no live access material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeletedCircle {
    pub frozen_epoch: MergeActiveCircleEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleControlState {
    ActiveEpoch(MergeActiveCircleEpoch),
    EpochClose(CircleEpochClose),
    Deleted(DeletedCircle),
}

impl CircleControlState {
    pub(crate) fn access_epoch(&self) -> &MergeActiveCircleEpoch {
        match self {
            Self::ActiveEpoch(active) => active,
            Self::EpochClose(close) => &close.frozen_epoch,
            Self::Deleted(deleted) => &deleted.frozen_epoch,
        }
    }

    pub(crate) fn access_epoch_mut(&mut self) -> &mut MergeActiveCircleEpoch {
        match self {
            Self::ActiveEpoch(active) => active,
            Self::EpochClose(close) => &mut close.frozen_epoch,
            Self::Deleted(deleted) => &mut deleted.frozen_epoch,
        }
    }

    pub(crate) fn active_epoch(&self) -> Option<&MergeActiveCircleEpoch> {
        match self {
            Self::ActiveEpoch(active) => Some(active),
            Self::EpochClose(_) | Self::Deleted(_) => None,
        }
    }

    pub(crate) fn active_epoch_mut(&mut self) -> Option<&mut MergeActiveCircleEpoch> {
        match self {
            Self::ActiveEpoch(active) => Some(active),
            Self::EpochClose(_) | Self::Deleted(_) => None,
        }
    }

    pub(crate) fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeCircleControlHeadRef {
    pub coord: CircleControlCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// One losing branch of a resolved control conflict, carried so the resolution
/// can cover every branch's frontier rather than only the chosen branch's: the
/// branch's control head, its metadata and roster head frontiers, and the
/// metadata entry that branch selected. The resolution unions these into its own
/// frontier so no author-stream head is re-allocated once the conflict collapses,
/// and re-derives its name as the deterministic metadata selection across the
/// union.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedConflictBranch {
    pub control_head: MergeCircleControlHeadRef,
    pub metadata_heads: Vec<CircleMetadataHeadRef>,
    pub roster_heads: Vec<CircleRosterHeadRef>,
    pub selected_metadata: CircleMetadata,
}

/// Insert `head` into a frontier keyed by author stream, keeping the deeper
/// (higher-sequence) head when the stream already carries one. Merging every
/// conflicting branch's heads this way yields the union frontier: each stream is
/// covered at its deepest position across all branches, so a device that authored
/// on that stream continues from its own head instead of re-allocating it.
pub(crate) fn merge_frontier_head<H>(
    frontier: &mut Vec<H>,
    head: H,
    stream_key: impl Fn(&H) -> CircleAuthorStreamKey,
    seq: impl Fn(&H) -> u64,
) {
    let key = stream_key(&head);
    match frontier
        .iter_mut()
        .find(|existing| stream_key(existing) == key)
    {
        Some(existing) if seq(&head) > seq(existing) => *existing = head,
        Some(_) => {}
        None => frontier.push(head),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MergeCircleOwnerAuthorityRef {
    Roster {
        roster: MergeCircleRosterStateRef,
        grant_id: MembershipGrantId,
        created_at: super::circle_roster::CircleRosterCoord,
    },
    ConflictResolution {
        conflict_hash: ObjectHash,
        resolution_hash: ObjectHash,
    },
}

impl MergeCircleOwnerAuthorityRef {
    pub(crate) fn grant_id(&self, author_pubkey: &str) -> MembershipGrantId {
        match self {
            Self::Roster { grant_id, .. } => grant_id.clone(),
            Self::ConflictResolution { conflict_hash, .. } => {
                super::circle_roster::derive_circle_resolution_grant(conflict_hash, author_pubkey)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleControlValue {
    pub order: MergeCircleControlOrder,
    pub state: CircleControlState,
    pub author_authority: MergeCircleOwnerAuthorityRef,
    pub membership_authority: MembershipGrantCreationAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleControl {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub value: CircleControlValue,
    pub author_pubkey: String,
    pub signature: String,
}

impl CircleControl {
    pub(crate) fn state(&self) -> &CircleControlState {
        &self.value.state
    }

    pub(crate) fn active_epoch(&self) -> Option<&MergeActiveCircleEpoch> {
        self.value.state.active_epoch()
    }

    pub(crate) fn access_epoch(&self) -> &MergeActiveCircleEpoch {
        self.value.state.access_epoch()
    }

    pub(crate) fn active_common(&self) -> &ActiveCircleEpochCore {
        &self.access_epoch().common
    }

    pub(crate) fn epoch_id(&self) -> CircleEpochId {
        self.active_common().epoch_id
    }

    pub(crate) fn key_fingerprint(&self) -> KeyFingerprint {
        self.active_common().key_fingerprint
    }

    pub(crate) fn owners(&self) -> &[String] {
        &self.active_common().owners
    }

    pub(crate) fn access_root(&self) -> ObjectHash {
        self.active_common().access_root
    }

    pub(crate) fn roster_state_ref(&self) -> CircleRosterStateRef {
        self.access_epoch().roster.clone()
    }

    pub(crate) fn metadata_state_ref(&self) -> CircleMetadataStateRef {
        self.access_epoch().metadata.clone()
    }

    pub(crate) fn store_membership_state_ref(&self) -> StoreMembershipStateRef {
        self.access_epoch().store_membership.clone()
    }

    #[cfg(test)]
    pub(crate) fn membership_authority(&self) -> &MembershipGrantCreationAuthority {
        &self.value.membership_authority
    }

    pub(crate) fn previous_control_hash(&self) -> Option<ObjectHash> {
        self.value.order.previous_control_hash
    }

    pub(crate) fn is_founder(&self) -> bool {
        self.value.order.seq == 1
            && self.value.order.previous_control_hash.is_none()
            && self.value.order.dependencies.is_empty()
    }

    pub(crate) fn causally_covers(&self, prior: &Self) -> bool {
        if self.store_root_hash != prior.store_root_hash || self.circle_id != prior.circle_id {
            return false;
        }
        self.value.order.previous_control_hash == Some(prior.control_hash())
            || self
                .value
                .order
                .dependencies
                .binary_search(&prior.coord())
                .is_ok()
    }

    pub(crate) fn ordinal(&self) -> u64 {
        self.value.order.seq
    }

    pub(crate) fn author_grant_id(&self) -> MembershipGrantId {
        self.value.author_authority.grant_id(&self.author_pubkey)
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            value: &'a CircleControlValue,
            author_pubkey: &'a str,
        }
        serde_json::to_vec(&Signed {
            domain: CONTROL_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            value: &self.value,
            author_pubkey: &self.author_pubkey,
        })
        .expect("circle control serialization cannot fail")
    }

    pub(crate) fn control_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle control serialization cannot fail"),
        )
    }

    pub(crate) fn verify(&self) -> bool {
        let order = &self.value.order;
        let access_epoch = self.access_epoch();
        let author_authority = &self.value.author_authority;
        let grant_id = author_authority.grant_id(&self.author_pubkey);
        let stream_key = CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: order.device_id.clone(),
            stream_id: order.stream_id,
            author_owner_grant: order.author_owner_grant.clone(),
        };
        let covered_are_canonical = access_epoch
            .covered_control_heads
            .windows(2)
            .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key());
        let own_predecessor = access_epoch
            .covered_control_heads
            .iter()
            .find(|head| head.coord.stream_key() == stream_key);
        let expected_dependencies = access_epoch
            .covered_control_heads
            .iter()
            .filter(|head| head.coord.stream_key() != stream_key)
            .map(|head| head.coord.clone())
            .collect::<Vec<_>>();
        let order_is_valid = !order.device_id.is_empty()
            && order.seq > 0
            && order.author_owner_grant == grant_id
            && covered_are_canonical
            && order.dependencies == expected_dependencies;
        let authority_is_founder_roster = matches!(
            author_authority,
            MergeCircleOwnerAuthorityRef::Roster { roster, .. }
                if roster == &access_epoch.roster
        );
        let founder = order.seq == 1 && access_epoch.covered_control_heads.is_empty();
        let continuity_is_valid = match (order.seq, own_predecessor) {
            (1, None) => order.previous_control_hash.is_none(),
            (seq, Some(predecessor)) if seq > 1 => {
                predecessor.coord.seq.checked_add(1) == Some(seq)
                    && order.previous_control_hash == Some(predecessor.coord.control_hash)
            }
            _ => false,
        };
        let founder_identity_is_valid = !founder
            || (authority_is_founder_roster
                && self.circle_id
                    == CircleId::founder(self.store_root_hash, &self.author_pubkey, &grant_id));
        let common = &access_epoch.common;
        let owners_are_canonical =
            !common.owners.is_empty() && common.owners.windows(2).all(|pair| pair[0] < pair[1]);
        let origin_is_valid = match &common.origin {
            CircleEpochOrigin::Founder => true,
            CircleEpochOrigin::Closed { cutoff, .. } => {
                super::store_commit::validate_commit_frontier(cutoff).is_ok()
            }
        };
        let state_is_valid = match &self.value.state {
            CircleControlState::ActiveEpoch(_) => true,
            CircleControlState::EpochClose(close) => !founder && close.verify_shape(self.circle_id),
            // A deletion is always a successor of a live control; the frozen
            // epoch it carries is validated by the shared access-epoch checks
            // above.
            CircleControlState::Deleted(_) => !founder,
        };
        self.version == STORE_PROTOCOL_VERSION
            && owners_are_canonical
            && origin_is_valid
            && state_is_valid
            && order_is_valid
            && continuity_is_valid
            && founder_identity_is_valid
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn coord(&self) -> CircleControlCoord {
        let order = &self.value.order;
        CircleControlCoord {
            device_id: order.device_id.clone(),
            stream_id: order.stream_id,
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: order.author_owner_grant.clone(),
            seq: order.seq,
            control_hash: self.control_hash(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleControlHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub entry: ExactObjectRef,
    pub successor: SuccessorLink,
    pub signature: String,
}

impl CircleControlHead {
    pub(crate) fn signed(
        control: &CircleControl,
        entry: ExactObjectRef,
        successor: SuccessorLink,
        signer: &UserKeypair,
    ) -> Self {
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: control.store_root_hash,
            circle_id: control.circle_id,
            control: control.coord(),
            entry,
            successor,
            signature: String::new(),
        };
        head.signature = keys::sign_hex(signer, &head.canonical_bytes()).1;
        head
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            control: &'a CircleControlCoord,
            entry: &'a ExactObjectRef,
            successor: &'a SuccessorLink,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-control-head.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            control: &self.control,
            entry: &self.entry,
            successor: &self.successor,
        })
        .expect("circle control head serialization cannot fail")
    }

    pub(crate) fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle control head serialization cannot fail"),
        )
    }

    pub(crate) fn verify(&self, registration: &StoreDeviceRegistration) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.control.validate().is_ok()
            && self.control.device_id == registration.device_id.to_string()
            && keys::verify_signature_hex(
                &registration.device_signing_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AccessEnvelope {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub candidate_family: super::store_commit::CandidateFamilyId,
    pub circle_id: CircleId,
    pub owner_pubkey: String,
    pub recipient_slot: String,
    pub control_hash: ObjectHash,
    pub leaf_id: AccessLeafId,
    pub leaf_hash: ObjectHash,
    pub value_hash: ObjectHash,
    pub proof: Vec<MerkleStep>,
    pub signature: String,
}

impl AccessEnvelope {
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            candidate_family: super::store_commit::CandidateFamilyId,
            circle_id: CircleId,
            owner_pubkey: &'a str,
            recipient_slot: &'a str,
            control_hash: ObjectHash,
            leaf_id: AccessLeafId,
            leaf_hash: ObjectHash,
            value_hash: ObjectHash,
            proof: &'a [MerkleStep],
        }
        serde_json::to_vec(&Signed {
            domain: ENVELOPE_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            candidate_family: self.candidate_family,
            circle_id: self.circle_id,
            owner_pubkey: &self.owner_pubkey,
            recipient_slot: &self.recipient_slot,
            control_hash: self.control_hash,
            leaf_id: self.leaf_id,
            leaf_hash: self.leaf_hash,
            value_hash: self.value_hash,
            proof: &self.proof,
        })
        .expect("access envelope serialization cannot fail")
    }

    pub(crate) fn verify(
        &self,
        control: &PreparedCircleControl,
        candidate_family: super::store_commit::CandidateFamilyId,
    ) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.store_root_hash == control.value.store_root_hash
            && self.candidate_family == candidate_family
            && self.circle_id == control.value.circle_id
            && self.owner_pubkey == control.value.author_pubkey
            && self.control_hash == control.coord.control_hash()
            && keys::verify_signature_hex(
                &self.owner_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
            && verify_merkle_proof(self.leaf_hash, &self.proof, control.value.access_root())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleControl {
    pub coord: CircleControlCoord,
    pub bytes: Vec<u8>,
    pub value: CircleControl,
}

impl PreparedCircleControl {
    pub(crate) fn verify(&self) -> bool {
        self.bytes
            == serde_json::to_vec(&self.value).expect("circle control serialization cannot fail")
            && self.value.verify()
            && self.coord == self.value.coord()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedAccessLeaf {
    pub bytes: Vec<u8>,
    pub value: CircleAccessLeaf,
    pub leaf_hash: ObjectHash,
}

impl PreparedAccessLeaf {
    pub(crate) fn verify(
        &self,
        control: &PreparedCircleControl,
        candidate_family: super::store_commit::CandidateFamilyId,
    ) -> bool {
        self.value.verify_for_control(control, candidate_family)
            && ObjectHash::digest(&self.bytes) == self.leaf_hash
    }

    pub(crate) fn verify_envelope(
        &self,
        control: &PreparedCircleControl,
        envelope: &AccessEnvelope,
        candidate_family: super::store_commit::CandidateFamilyId,
    ) -> bool {
        self.verify(control, candidate_family)
            && envelope.verify(control, candidate_family)
            && self.leaf_hash == envelope.leaf_hash
            && envelope.value_hash
                == ObjectHash::digest(
                    &serde_json::to_vec(&self.value)
                        .expect("circle access leaf serialization cannot fail"),
                )
            && self.value.leaf_id == envelope.leaf_id
            && self.value.owner_pubkey == envelope.owner_pubkey
            && self.value.recipient_slot == envelope.recipient_slot
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleAccess {
    pub leaf: PreparedAccessLeaf,
    pub envelope: AccessEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleRosterPolicyObjects {
    pub entry: CircleRosterEntry,
    pub head: CircleRosterHead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleTransitionPolicyObjects {
    pub roster: Option<CircleRosterPolicyObjects>,
    pub metadata_head: Option<CircleMetadataHead>,
    pub control_head: CircleControlHead,
}

#[derive(Debug, Clone)]
pub(crate) enum CircleRosterDraftPolicy {
    Inherited,
    Founder {
        entry: CircleRosterEntry,
    },
    Successor {
        predecessor: CircleRosterChain,
        entry: CircleRosterEntry,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct CircleTransitionDraftPolicy {
    pub roster: CircleRosterDraftPolicy,
    pub metadata_successor: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CircleTransitionDraft {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleMaterializedRoster,
    pub policy: CircleTransitionDraftPolicy,
    pub metadata: CircleMetadata,
    pub close_intent: Option<CircleEpochCloseIntent>,
    pub close_finalization: Option<CircleEpochCloseFinalizationDraft>,
    pub close_cancellation: Option<CircleEpochCloseCancellationDraft>,
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
}

#[derive(Debug, Clone)]
pub(crate) struct CircleEpochCloseFinalizationDraft {
    pub close_control: PreparedCircleControl,
    pub intent: CircleEpochCloseIntent,
    pub responses: Vec<CircleEpochCloseSettlement>,
    pub outcome_slot: ObjectSlot,
}

#[derive(Debug, Clone)]
pub(crate) struct CircleEpochCloseCancellationDraft {
    pub close_control: PreparedCircleControl,
    pub outcome_slot: ObjectSlot,
}

#[derive(Debug, Clone)]
struct FounderRosterObjects {
    entry: CircleRosterEntry,
    resolved: ResolvedCircleRoster,
}

struct CircleAccessDraft<'identity> {
    store_root_hash: ObjectHash,
    candidate_family: super::store_commit::CandidateFamilyId,
    circle_id: CircleId,
    access_root: ObjectHash,
    leaves: Vec<PreparedAccessLeaf>,
    proofs: Vec<Vec<MerkleStep>>,
    signer: &'identity UserKeypair,
}

impl<'identity> CircleAccessDraft<'identity> {
    #[allow(clippy::too_many_arguments)]
    fn prepare(
        store_root_hash: ObjectHash,
        candidate_family: super::store_commit::CandidateFamilyId,
        circle_id: CircleId,
        epoch_id: CircleEpochId,
        keyring: &str,
        key_fingerprint: KeyFingerprint,
        roster_state: &CircleRosterStateRef,
        roster_members: &std::collections::BTreeMap<String, super::circle::CircleRole>,
        store_membership: &StoreMembershipStateRef,
        store_members: &[(String, MemberRole)],
        bootstraps: &std::collections::BTreeMap<String, CircleBootstrapRef>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &'identity UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let author_pubkey = keys::public_key_hex(signer);
        let leaves = store_members
            .iter()
            .map(|(recipient_pubkey, _)| {
                let recipient_slot = recipient_slot(signer, recipient_pubkey, circle_id)?;
                let disposition = if roster_members.contains_key(recipient_pubkey) {
                    CircleAccessDisposition::Active {
                        keyring: keyring.to_string(),
                        key_fingerprint,
                        roster: roster_state.clone(),
                        bootstrap: bootstraps.get(recipient_pubkey).cloned(),
                    }
                } else {
                    CircleAccessDisposition::Inactive
                };
                let mut value = CircleAccessLeaf {
                    version: STORE_PROTOCOL_VERSION,
                    store_root_hash,
                    candidate_family,
                    circle_id,
                    epoch_id,
                    leaf_id: AccessLeafId::generate(ids),
                    owner_pubkey: author_pubkey.clone(),
                    recipient_pubkey: recipient_pubkey.clone(),
                    recipient_slot,
                    disposition,
                    store_membership: store_membership.clone(),
                    signature: String::new(),
                };
                value.signature = keys::sign_hex(signer, &value.canonical_bytes()).1;
                let recipient_ed25519: [u8; keys::SIGN_PUBLICKEYBYTES] =
                    hex::decode(recipient_pubkey)
                        .map_err(|_| {
                            CircleTransitionError::InvalidRecipient(recipient_pubkey.clone())
                        })?
                        .try_into()
                        .map_err(|_| {
                            CircleTransitionError::InvalidRecipient(recipient_pubkey.clone())
                        })?;
                let recipient_x25519 = keys::ed25519_to_x25519_public_key(&recipient_ed25519)
                    .map_err(|_| {
                        CircleTransitionError::InvalidRecipient(recipient_pubkey.clone())
                    })?;
                let plaintext =
                    serde_json::to_vec(&value).expect("circle access serialization cannot fail");
                let bytes = keys::seal_box_encrypt(&plaintext, &recipient_x25519);
                let leaf_hash = ObjectHash::digest(&bytes);
                Ok::<PreparedAccessLeaf, CircleTransitionError>(PreparedAccessLeaf {
                    bytes,
                    value,
                    leaf_hash,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let leaf_hashes = leaves.iter().map(|leaf| leaf.leaf_hash).collect::<Vec<_>>();
        let (access_root, proofs) = merkle_root_and_proofs(&leaf_hashes);
        Ok(Self {
            store_root_hash,
            candidate_family,
            circle_id,
            access_root,
            leaves,
            proofs,
            signer,
        })
    }

    fn access_root(&self) -> ObjectHash {
        self.access_root
    }

    fn finish(
        self,
        control: &PreparedCircleControl,
    ) -> Result<Vec<PreparedCircleAccess>, CircleTransitionError> {
        let author_pubkey = keys::public_key_hex(self.signer);
        if control.value.store_root_hash != self.store_root_hash
            || control.value.circle_id != self.circle_id
            || control.value.author_pubkey != author_pubkey
            || control.value.access_root() != self.access_root
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        Ok(self
            .leaves
            .into_iter()
            .zip(self.proofs)
            .map(|(leaf, proof)| {
                let mut envelope = AccessEnvelope {
                    version: STORE_PROTOCOL_VERSION,
                    store_root_hash: self.store_root_hash,
                    candidate_family: self.candidate_family,
                    circle_id: self.circle_id,
                    owner_pubkey: author_pubkey.clone(),
                    recipient_slot: leaf.value.recipient_slot.clone(),
                    control_hash: control.coord.control_hash(),
                    leaf_id: leaf.value.leaf_id,
                    leaf_hash: leaf.leaf_hash,
                    value_hash: ObjectHash::digest(
                        &serde_json::to_vec(&leaf.value)
                            .expect("circle access leaf serialization cannot fail"),
                    ),
                    proof,
                    signature: String::new(),
                };
                envelope.signature = keys::sign_hex(self.signer, &envelope.canonical_bytes()).1;
                PreparedCircleAccess { leaf, envelope }
            })
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedCircleTransition {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleMaterializedRoster,
    pub policy_objects: CircleTransitionPolicyObjects,
    pub metadata: CircleMetadata,
    pub close_intent: Option<CircleEpochCloseIntent>,
    pub close_outcome: Option<CircleEpochCloseOutcome>,
    pub close_cancellation: Option<CircleEpochCloseCancellation>,
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
}

impl PreparedCircleTransition {
    pub(crate) fn resolved_roster(&self) -> CircleMaterializedRoster {
        self.roster.clone()
    }

    pub(crate) fn control_ref(
        &self,
        objects: super::store_commit::CircleActivationObjects,
        head_object: Option<ExactObjectRef>,
    ) -> super::store_commit::CircleControlRef {
        let head_object =
            head_object.expect("prepared Circle transition must contain its stored head");
        super::store_commit::CircleControlRef {
            circle_id: self.circle_id,
            control: self.control.coord.clone(),
            head_hash: self.policy_objects.control_head.head_hash(),
            head_object,
            objects,
        }
    }
}

struct CircleSuccessorContext<'a> {
    store_members: Vec<(String, MemberRole)>,
    author_pubkey: String,
    epoch: &'a MergeActiveCircleEpoch,
    grant_id: MembershipGrantId,
    author_authority: MergeCircleOwnerAuthorityRef,
    key_fingerprint: KeyFingerprint,
}

/// The successor context for a command that publishes a new active epoch: the
/// current control must be `ActiveEpoch`, so a closing or deleted control is
/// refused.
fn circle_successor_context<'a>(
    store_members: Vec<(String, MemberRole)>,
    current_control: &'a PreparedCircleControl,
    current_roster: &CircleMaterializedRoster,
    current_metadata: &CircleMetadata,
    keyring: &str,
    signer: &UserKeypair,
) -> Result<CircleSuccessorContext<'a>, CircleTransitionError> {
    let epoch = current_control
        .value
        .active_epoch()
        .ok_or(CircleTransitionError::InvalidCurrentState)?;
    circle_authored_successor_context(
        store_members,
        current_control,
        current_roster,
        current_metadata,
        keyring,
        signer,
        epoch,
    )
}

/// The successor context for a terminal deletion, which supersedes an in-flight
/// close. It authors over the control's access epoch — the active epoch itself,
/// or a close's frozen epoch — so a `Closing` control resolves to the frozen
/// spine the deletion freezes, rather than being refused for lacking an active
/// epoch.
fn circle_delete_successor_context<'a>(
    store_members: Vec<(String, MemberRole)>,
    current_control: &'a PreparedCircleControl,
    current_roster: &CircleMaterializedRoster,
    current_metadata: &CircleMetadata,
    keyring: &str,
    signer: &UserKeypair,
) -> Result<CircleSuccessorContext<'a>, CircleTransitionError> {
    let epoch = current_control.value.access_epoch();
    circle_authored_successor_context(
        store_members,
        current_control,
        current_roster,
        current_metadata,
        keyring,
        signer,
        epoch,
    )
}

fn circle_authored_successor_context<'a>(
    mut store_members: Vec<(String, MemberRole)>,
    current_control: &PreparedCircleControl,
    current_roster: &CircleMaterializedRoster,
    current_metadata: &CircleMetadata,
    keyring: &str,
    signer: &UserKeypair,
    epoch: &'a MergeActiveCircleEpoch,
) -> Result<CircleSuccessorContext<'a>, CircleTransitionError> {
    if !current_control.verify()
        || !current_roster.verify()
        || !current_metadata.verify()
        || current_control.value.circle_id != current_metadata.circle_id
        || current_control.value.epoch_id() != current_metadata.epoch_id
    {
        return Err(CircleTransitionError::InvalidCurrentState);
    }
    let author_pubkey = keys::public_key_hex(signer);
    store_members.sort_by(|left, right| left.0.cmp(&right.0));
    store_members.dedup_by(|left, right| left.0 == right.0);
    if !store_members
        .iter()
        .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
    {
        return Err(CircleTransitionError::AuthorNotStoreWriter);
    }
    if current_roster.members().get(&author_pubkey) != Some(&super::circle::CircleRole::Owner) {
        return Err(CircleTransitionError::AuthorNotCircleOwner);
    }
    let key_fingerprint = EncryptionService::from(
        MasterKeyring::from_serialized(keyring)
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?,
    )
    .seal_key_fingerprint();
    if key_fingerprint != current_control.value.key_fingerprint()
        || current_metadata.key_fingerprint != key_fingerprint
    {
        return Err(CircleTransitionError::InvalidCurrentState);
    }
    let (grant_id, record) = current_roster
        .active_grants()
        .find(|(_, record)| {
            record.member_pubkey == author_pubkey && record.role == super::circle::CircleRole::Owner
        })
        .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
    let author_authority = match &record.creation_authority {
        CircleGrantCreationAuthority::Entry(created_at) => MergeCircleOwnerAuthorityRef::Roster {
            roster: epoch.roster.clone(),
            grant_id: grant_id.clone(),
            created_at: created_at.clone(),
        },
        CircleGrantCreationAuthority::ConflictResolution(resolution) => {
            MergeCircleOwnerAuthorityRef::ConflictResolution {
                conflict_hash: resolution.conflict_hash,
                resolution_hash: resolution.resolution_hash,
            }
        }
    };
    Ok(CircleSuccessorContext {
        store_members,
        author_pubkey,
        epoch,
        grant_id: grant_id.clone(),
        author_authority,
        key_fingerprint,
    })
}

impl CircleTransitionDraft {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn founder(
        store_root_hash: ObjectHash,
        candidate_family: super::store_commit::CandidateFamilyId,
        device_id: &str,
        name: &str,
        metadata_stamp: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        mut store_members: Vec<(String, MemberRole)>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let author_pubkey = keys::public_key_hex(signer);
        store_members.sort_by(|left, right| left.0.cmp(&right.0));
        store_members.dedup_by(|left, right| left.0 == right.0);
        if !store_members
            .iter()
            .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
        {
            return Err(CircleTransitionError::AuthorNotStoreWriter);
        }
        let owner_grant =
            MembershipGrantId(generated_id_digest(ids, OWNER_GRANT_ID_GENERATION_DOMAIN));
        let author_stream_id = AuthorStreamId::from_digest(generated_id_digest(
            ids,
            b"coven.circle-transition-draft-stream.v1\0",
        ));
        let circle_id = CircleId::founder(store_root_hash, &author_pubkey, &owner_grant);
        let epoch_id = CircleEpochId::generate(ids);
        let keyring = MasterKeyring::generate();
        let encryption = EncryptionService::from(keyring.clone());
        let key_fingerprint = encryption.seal_key_fingerprint();
        let entry = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            device_id,
            author_stream_id,
            owner_grant.clone(),
            signer,
        );
        let roster_objects = FounderRosterObjects {
            resolved: CircleRosterChain::from_entries(vec![entry.clone()])?.resolved(),
            entry,
        };
        let roster_state = MergeCircleRosterStateRef {
            heads: Vec::new(),
            resolutions: Vec::new(),
            state_hash: roster_objects.resolved.state_hash,
        };
        let metadata = CircleMetadata::founder(
            store_root_hash,
            circle_id,
            epoch_id,
            name,
            metadata_stamp,
            device_id,
            author_stream_id,
            owner_grant.clone(),
            roster_state.clone(),
            key_fingerprint,
            signer,
        )?;
        let metadata_state = MergeCircleMetadataStateRef {
            heads: Vec::new(),
            selected: metadata.coord(),
            state_hash: metadata.metadata_hash(),
        };
        let roster = roster_objects.resolved.clone();
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            &keyring.to_serialized(),
            key_fingerprint,
            &roster_state,
            &roster.members(),
            &store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        let common = ActiveCircleEpochCore {
            epoch_id,
            key_fingerprint,
            owners: vec![author_pubkey.clone()],
            access_root: access.access_root(),
            origin: CircleEpochOrigin::Founder,
        };
        let value = CircleControlValue {
            order: MergeCircleControlOrder {
                device_id: device_id.to_string(),
                stream_id: author_stream_id,
                author_owner_grant: owner_grant.clone(),
                seq: 1,
                previous_control_hash: None,
                dependencies: Vec::new(),
            },
            state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                common,
                metadata: metadata_state,
                roster: roster_state.clone(),
                store_membership,
                covered_control_heads: Vec::new(),
            }),
            author_authority: MergeCircleOwnerAuthorityRef::Roster {
                roster: roster_state,
                grant_id: owner_grant.clone(),
                created_at: roster_objects.entry.coord(),
            },
            membership_authority,
        };
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value,
            author_pubkey: author_pubkey.clone(),
            signature: String::new(),
        };
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let policy_objects = CircleTransitionDraftPolicy {
            roster: CircleRosterDraftPolicy::Founder {
                entry: roster_objects.entry,
            },
            metadata_successor: true,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_serialized(),
            roster,
            policy: policy_objects,
            metadata,
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_member(
        candidate_family: super::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_roster_chain: CircleRosterChain,
        current_metadata: &CircleMetadata,
        keyring: &str,
        roster_stream: AuthorStreamId,
        member_pubkey: String,
        role: super::circle::CircleRole,
        bootstrap: CircleBootstrapRef,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        if current_roster_chain.try_resolved()? != *current_roster {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let context = circle_successor_context(
            store_members,
            current_control,
            current_roster,
            current_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members,
            author_pubkey,
            epoch: active_epoch,
            grant_id,
            author_authority,
            key_fingerprint,
        } = context;
        if !store_members
            .iter()
            .any(|(pubkey, _)| pubkey == &member_pubkey)
        {
            return Err(CircleTransitionError::MemberNotInStore(member_pubkey));
        }
        let entry = current_roster_chain.signed_set_member(
            device_id,
            roster_stream,
            member_pubkey.clone(),
            role,
            signer,
        )?;
        let roster = current_roster_chain.resolved_with_successor(entry.clone())?;
        let roster_state = MergeCircleRosterStateRef {
            heads: active_epoch.roster.heads.clone(),
            resolutions: active_epoch.roster.resolutions.clone(),
            state_hash: roster.state_hash,
        };
        let store_root_hash = current_control.value.store_root_hash;
        let circle_id = current_control.value.circle_id;
        let epoch_id = current_control.value.epoch_id();
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: roster_stream,
                    author_owner_grant: grant_id.clone(),
                    seq: current_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(current_control.coord.control_hash()),
                    dependencies: vec![current_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: active_epoch.common.clone(),
                    metadata: active_epoch.metadata.clone(),
                    roster: roster_state.clone(),
                    store_membership,
                    covered_control_heads: active_epoch.covered_control_heads.clone(),
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let bootstraps =
            std::collections::BTreeMap::from([(member_pubkey.clone(), bootstrap.clone())]);
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &roster.members(),
            &control_value
                .value
                .state
                .active_epoch()
                .expect("member addition constructs an active epoch")
                .store_membership,
            &store_members,
            &bootstraps,
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("member addition constructs an active epoch")
            .common
            .access_root = access.access_root();
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster,
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Successor {
                    predecessor: current_roster_chain,
                    entry,
                },
                metadata_successor: false,
            },
            metadata: current_metadata.clone(),
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn close_epoch(
        candidate_family: super::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        close_id: CircleEpochCloseId,
        close_intent: CircleEpochCloseIntent,
        intent: CircleEpochCloseIntentRef,
        frozen_device_state: StoreDeviceStateRef,
        participants: Vec<CircleEpochCloseParticipant>,
        provisional_frontier: CommitFrontier,
        outcome_slot: ObjectSlot,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let context = circle_successor_context(
            store_members,
            current_control,
            current_roster,
            current_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members,
            author_pubkey,
            epoch: active_epoch,
            grant_id,
            author_authority,
            key_fingerprint,
        } = context;
        let store_root_hash = current_control.value.store_root_hash;
        let circle_id = current_control.value.circle_id;
        let epoch_id = current_control.value.epoch_id();
        if close_intent.close_id != close_id
            || close_intent.intent_hash() != intent.intent_hash
            || close_intent.circle_id != circle_id
            || close_intent.epoch_id != epoch_id
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let roster_state = active_epoch.roster.clone();
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: active_epoch
                        .covered_control_heads
                        .iter()
                        .find(|head| {
                            head.coord.author_pubkey == author_pubkey
                                && head.coord.device_id == device_id
                                && head.coord.author_owner_grant == grant_id
                        })
                        .map_or_else(
                            || {
                                AuthorStreamId::from_digest(generated_id_digest(
                                    ids,
                                    b"coven.circle-transition-draft-stream.v1\0",
                                ))
                            },
                            |head| head.coord.stream_id,
                        ),
                    author_owner_grant: grant_id,
                    seq: current_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(current_control.coord.control_hash()),
                    dependencies: vec![current_control.coord.clone()],
                },
                state: CircleControlState::EpochClose(CircleEpochClose {
                    close_id,
                    frozen_epoch: MergeActiveCircleEpoch {
                        common: active_epoch.common.clone(),
                        metadata: active_epoch.metadata.clone(),
                        roster: roster_state.clone(),
                        store_membership,
                        covered_control_heads: active_epoch.covered_control_heads.clone(),
                    },
                    intent,
                    frozen_device_state,
                    participants,
                    provisional_frontier,
                    outcome_slot,
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &current_roster.members(),
            &control_value.access_epoch().store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .access_epoch_mut()
            .common
            .access_root = access.access_root();
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Inherited,
                metadata_successor: false,
            },
            metadata: current_metadata.clone(),
            close_intent: Some(close_intent),
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finalize_epoch_close(
        candidate_family: super::store_commit::CandidateFamilyId,
        device_id: &str,
        author_registration: &StoreDeviceRegistrationRef,
        metadata_stamp: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        mut store_members: Vec<(String, MemberRole)>,
        close_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_roster_chain: CircleRosterChain,
        current_metadata: &CircleMetadata,
        keyring: &str,
        intent: CircleEpochCloseIntent,
        responses: Vec<CircleEpochCloseSettlement>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = close_control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        if !close_control.verify()
            || current_roster_chain.try_resolved()? != *current_roster
            || current_roster.state_hash() != close.frozen_epoch.roster.state_hash
            || current_metadata.coord() != close.frozen_epoch.metadata.selected
            || current_metadata.epoch_id != close.frozen_epoch.common.epoch_id
            || current_metadata.key_fingerprint != close.frozen_epoch.common.key_fingerprint
            || intent.intent_hash() != close.intent.intent_hash
            || intent.close_id != close.close_id
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let author_pubkey = keys::public_key_hex(signer);
        store_members.sort_by(|left, right| left.0.cmp(&right.0));
        store_members.dedup_by(|left, right| left.0 == right.0);
        if !store_members
            .iter()
            .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
        {
            return Err(CircleTransitionError::AuthorNotStoreWriter);
        }
        let (grant_id, owner_record) = current_roster
            .active_grants()
            .find(|(_, record)| {
                record.member_pubkey == author_pubkey
                    && record.role == super::circle::CircleRole::Owner
            })
            .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
        let author_authority = match &owner_record.creation_authority {
            CircleGrantCreationAuthority::Entry(created_at) => {
                MergeCircleOwnerAuthorityRef::Roster {
                    roster: close.frozen_epoch.roster.clone(),
                    grant_id: grant_id.clone(),
                    created_at: created_at.clone(),
                }
            }
            CircleGrantCreationAuthority::ConflictResolution(resolution) => {
                MergeCircleOwnerAuthorityRef::ConflictResolution {
                    conflict_hash: resolution.conflict_hash,
                    resolution_hash: resolution.resolution_hash,
                }
            }
        };
        let old_encryption = EncryptionService::from(
            MasterKeyring::from_serialized(keyring)
                .map_err(|_| CircleTransitionError::InvalidCurrentState)?,
        );
        if old_encryption.seal_key_fingerprint() != close.frozen_epoch.common.key_fingerprint {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let new_generation = old_encryption
            .current_generation()
            .checked_add(1)
            .ok_or(CircleTransitionError::SequenceOverflow)?;
        let encryption = old_encryption
            .with_appended_generation(new_generation, crate::encryption::generate_random_key())
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        let keyring = encryption
            .to_keyring_string()
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        let key_fingerprint = encryption.seal_key_fingerprint();
        let epoch_id = CircleEpochId::generate(ids);
        let roster = current_roster_chain.resolved_with_successor(intent.removal.clone())?;
        if roster.state_hash() != intent.remaining_roster_state_hash {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let roster_members = roster.members();
        let owners = roster_members
            .iter()
            .filter_map(|(pubkey, role)| {
                (*role == super::circle::CircleRole::Owner).then_some(pubkey.clone())
            })
            .collect::<Vec<_>>();
        if owners.is_empty() {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let roster_state = MergeCircleRosterStateRef {
            heads: close.frozen_epoch.roster.heads.clone(),
            resolutions: close.frozen_epoch.roster.resolutions.clone(),
            state_hash: roster.state_hash(),
        };
        let metadata_stream = super::store_commit::StreamActivation::grant_authorized_stream_id(
            close_control.value.store_root_hash,
            author_registration,
            grant_id,
            super::store_commit::StreamAnchorDomain::CircleMetadata {
                circle_id: close_control.value.circle_id,
            },
        );
        let prior_metadata = close
            .frozen_epoch
            .metadata
            .heads
            .iter()
            .find(|head| head.coord.stream_id == metadata_stream);
        let mut metadata = current_metadata.clone();
        metadata.epoch_id = epoch_id;
        metadata.metadata_stamp = metadata_stamp.to_string();
        metadata.author_pubkey = author_pubkey.clone();
        metadata.device_id = device_id.to_string();
        metadata.stream_id = metadata_stream;
        metadata.author_owner_grant = grant_id.clone();
        metadata.seq = prior_metadata.map_or(Ok(1), |head| {
            head.coord
                .seq
                .checked_add(1)
                .ok_or(CircleTransitionError::SequenceOverflow)
        })?;
        metadata.previous_hash = prior_metadata.map(|head| head.coord.metadata_hash);
        metadata.dependencies = close
            .frozen_epoch
            .metadata
            .heads
            .iter()
            .map(|head| head.coord.clone())
            .collect();
        metadata.author_roster = roster_state.clone();
        metadata.key_fingerprint = key_fingerprint;
        metadata.signature = keys::sign_hex(signer, &metadata.canonical_bytes()).1;
        let metadata_state = MergeCircleMetadataStateRef {
            heads: close.frozen_epoch.metadata.heads.clone(),
            selected: metadata.coord(),
            state_hash: metadata.metadata_hash(),
        };
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: close_control.value.store_root_hash,
            circle_id: close_control.value.circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: close_control.value.value.order.stream_id,
                    author_owner_grant: grant_id.clone(),
                    seq: close_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(close_control.coord.control_hash()),
                    dependencies: vec![close_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: ActiveCircleEpochCore {
                        epoch_id,
                        key_fingerprint,
                        owners,
                        access_root: close.frozen_epoch.common.access_root,
                        origin: close.frozen_epoch.common.origin.clone(),
                    },
                    metadata: metadata_state,
                    roster: roster_state.clone(),
                    store_membership,
                    covered_control_heads: close.frozen_epoch.covered_control_heads.clone(),
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let access = CircleAccessDraft::prepare(
            control_value.store_root_hash,
            candidate_family,
            control_value.circle_id,
            epoch_id,
            &keyring,
            key_fingerprint,
            &roster_state,
            &roster_members,
            &control_value.access_epoch().store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("Circle finalization constructs an active epoch")
            .common
            .access_root = access.access_root();
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("Circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id: control.value.circle_id,
            epoch_id,
            keyring,
            roster,
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Successor {
                    predecessor: current_roster_chain,
                    entry: intent.removal.clone(),
                },
                metadata_successor: true,
            },
            metadata,
            close_intent: None,
            close_finalization: Some(CircleEpochCloseFinalizationDraft {
                close_control: close_control.clone(),
                intent,
                responses,
                outcome_slot: close.outcome_slot.clone(),
            }),
            close_cancellation: None,
            access,
            control,
        })
    }

    /// Reopen a frozen epoch by cancelling its close. The successor restores the
    /// frozen epoch's protocol identity — same epoch, key generation, roster and
    /// metadata frontiers, and origin — re-issuing only the control-bound access
    /// material to the reopening control.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reopen_epoch(
        candidate_family: super::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        mut store_members: Vec<(String, MemberRole)>,
        close_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = close_control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let frozen = &close.frozen_epoch;
        if !close_control.verify()
            || current_roster.state_hash() != frozen.roster.state_hash
            || current_metadata.coord() != frozen.metadata.selected
            || current_metadata.epoch_id != frozen.common.epoch_id
            || current_metadata.key_fingerprint != frozen.common.key_fingerprint
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let author_pubkey = keys::public_key_hex(signer);
        store_members.sort_by(|left, right| left.0.cmp(&right.0));
        store_members.dedup_by(|left, right| left.0 == right.0);
        if !store_members
            .iter()
            .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
        {
            return Err(CircleTransitionError::AuthorNotStoreWriter);
        }
        let (grant_id, owner_record) = current_roster
            .active_grants()
            .find(|(_, record)| {
                record.member_pubkey == author_pubkey
                    && record.role == super::circle::CircleRole::Owner
            })
            .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
        let author_authority = match &owner_record.creation_authority {
            CircleGrantCreationAuthority::Entry(created_at) => {
                MergeCircleOwnerAuthorityRef::Roster {
                    roster: frozen.roster.clone(),
                    grant_id: grant_id.clone(),
                    created_at: created_at.clone(),
                }
            }
            CircleGrantCreationAuthority::ConflictResolution(resolution) => {
                MergeCircleOwnerAuthorityRef::ConflictResolution {
                    conflict_hash: resolution.conflict_hash,
                    resolution_hash: resolution.resolution_hash,
                }
            }
        };
        let encryption = EncryptionService::from(
            MasterKeyring::from_serialized(keyring)
                .map_err(|_| CircleTransitionError::InvalidCurrentState)?,
        );
        let key_fingerprint = encryption.seal_key_fingerprint();
        if key_fingerprint != frozen.common.key_fingerprint {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let epoch_id = frozen.common.epoch_id;
        let roster_state = frozen.roster.clone();
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: close_control.value.store_root_hash,
            circle_id: close_control.value.circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: close_control.value.value.order.stream_id,
                    author_owner_grant: grant_id.clone(),
                    seq: close_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(close_control.coord.control_hash()),
                    dependencies: vec![close_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: ActiveCircleEpochCore {
                        epoch_id,
                        key_fingerprint,
                        owners: frozen.common.owners.clone(),
                        access_root: frozen.common.access_root,
                        origin: frozen.common.origin.clone(),
                    },
                    metadata: frozen.metadata.clone(),
                    roster: roster_state.clone(),
                    store_membership,
                    covered_control_heads: frozen.covered_control_heads.clone(),
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let access = CircleAccessDraft::prepare(
            control_value.store_root_hash,
            candidate_family,
            control_value.circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &current_roster.members(),
            &control_value.access_epoch().store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("Circle reopen constructs an active epoch")
            .common
            .access_root = access.access_root();
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("Circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id: control.value.circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Inherited,
                metadata_successor: false,
            },
            metadata: current_metadata.clone(),
            close_intent: None,
            close_finalization: None,
            close_cancellation: Some(CircleEpochCloseCancellationDraft {
                close_control: close_control.clone(),
                outcome_slot: close.outcome_slot.clone(),
            }),
            access,
            control,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rename(
        candidate_family: super::store_commit::CandidateFamilyId,
        device_id: &str,
        name: &str,
        metadata_stamp: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        if name.trim().is_empty() {
            return Err(CircleTransitionError::EmptyName);
        }
        let context = circle_successor_context(
            store_members,
            current_control,
            current_roster,
            current_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members,
            author_pubkey,
            epoch: active_epoch,
            grant_id,
            author_authority,
            key_fingerprint,
        } = context;
        let store_root_hash = current_control.value.store_root_hash;
        let circle_id = current_control.value.circle_id;
        let epoch_id = current_control.value.epoch_id();
        let roster_state = current_control.value.roster_state_ref();

        let own_head = active_epoch.metadata.heads.iter().find(|head| {
            head.coord.author_pubkey == author_pubkey
                && head.coord.device_id == device_id
                && head.coord.author_owner_grant == grant_id
        });
        let author_stream_id = own_head.map_or_else(
            || {
                AuthorStreamId::from_digest(generated_id_digest(
                    ids,
                    b"coven.circle-transition-draft-stream.v1\0",
                ))
            },
            |head| head.coord.stream_id,
        );
        let metadata_seq = match own_head {
            Some(head) => head
                .coord
                .seq
                .checked_add(1)
                .ok_or(CircleTransitionError::SequenceOverflow)?,
            None => 1,
        };
        let metadata_previous = own_head.map(|head| head.coord.metadata_hash);
        let metadata_dependencies = active_epoch
            .metadata
            .heads
            .iter()
            .map(|head| head.coord.clone())
            .collect::<Vec<_>>();
        let metadata_state = active_epoch.metadata.clone();
        let mut control_value = CircleControlValue {
            order: MergeCircleControlOrder {
                device_id: device_id.to_string(),
                stream_id: author_stream_id,
                author_owner_grant: grant_id.clone(),
                seq: current_control
                    .value
                    .ordinal()
                    .checked_add(1)
                    .ok_or(CircleTransitionError::SequenceOverflow)?,
                previous_control_hash: Some(current_control.coord.control_hash()),
                dependencies: vec![current_control.coord.clone()],
            },
            state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                common: active_epoch.common.clone(),
                metadata: active_epoch.metadata.clone(),
                roster: active_epoch.roster.clone(),
                store_membership,
                covered_control_heads: active_epoch.covered_control_heads.clone(),
            }),
            author_authority,
            membership_authority,
        };
        let author_owner_grant = grant_id.clone();

        let mut metadata = CircleMetadata {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            epoch_id,
            name: name.to_string(),
            seq: metadata_seq,
            previous_hash: metadata_previous,
            dependencies: metadata_dependencies,
            metadata_stamp: metadata_stamp.to_string(),
            author_pubkey: author_pubkey.clone(),
            device_id: device_id.to_string(),
            stream_id: author_stream_id,
            author_owner_grant,
            author_roster: roster_state.clone(),
            key_fingerprint,
            signature: String::new(),
        };
        metadata.signature = keys::sign_hex(signer, &metadata.canonical_bytes()).1;

        let mut metadata_state = metadata_state;
        let selected = [current_metadata, &metadata]
            .into_iter()
            .max_by_key(|entry| {
                (
                    entry.metadata_stamp.as_str(),
                    entry.author_pubkey.as_str(),
                    entry.device_id.as_str(),
                    entry.metadata_hash(),
                )
            })
            .expect("current and successor metadata are non-empty");
        metadata_state.selected = selected.coord();
        metadata_state.state_hash = selected.metadata_hash();

        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &current_roster.members(),
            &control_value
                .state
                .active_epoch()
                .expect("rename constructs an active epoch")
                .store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        let active_epoch = control_value
            .state
            .active_epoch_mut()
            .expect("rename constructs an active epoch");
        active_epoch.common.access_root = access.access_root();
        active_epoch.metadata = metadata_state;
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: control_value,
            author_pubkey,
            signature: String::new(),
        };
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let policy_objects = CircleTransitionDraftPolicy {
            roster: CircleRosterDraftPolicy::Inherited,
            metadata_successor: true,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: policy_objects,
            metadata,
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    /// Build a successor of the chosen conflicting branch that causally covers
    /// every branch, collapsing a retained `ControlConflict` to a single
    /// resolved control. The successor inherits the chosen branch's epoch, key
    /// generation, and roster contents verbatim — it changes no membership, keys,
    /// or deletion intent. It merges the control, metadata, and roster head
    /// frontiers across every branch (the union of covered heads), so a device
    /// that authored a losing branch continues its own author streams instead of
    /// re-allocating their head slots. The name is not inherited from the chosen
    /// branch but re-derived as the deterministic metadata selection over the
    /// merged frontier: the metadata layer resolves its own conflict — the
    /// canonical maximum across every covered head — independent of which control
    /// branch the Owner chose. `losing_branches` are the retained branches other
    /// than `chosen`; preparation adds the chosen branch head, so the resolved
    /// control's causal dependencies and predecessor together name every branch.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve(
        candidate_family: super::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        chosen_control: &PreparedCircleControl,
        chosen_roster: &CircleMaterializedRoster,
        chosen_metadata: &CircleMetadata,
        keyring: &str,
        losing_branches: Vec<ResolvedConflictBranch>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let context = circle_successor_context(
            store_members,
            chosen_control,
            chosen_roster,
            chosen_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members,
            author_pubkey,
            epoch: active_epoch,
            grant_id: _,
            author_authority,
            key_fingerprint,
        } = context;
        let store_root_hash = chosen_control.value.store_root_hash;
        let circle_id = chosen_control.value.circle_id;
        let epoch_id = chosen_control.value.epoch_id();
        let roster_state = chosen_control.value.roster_state_ref();

        // Merge the control, metadata, and roster head frontiers across every
        // branch: the chosen branch's frontiers extended by each losing branch's,
        // one head per author stream at its deepest position. Every branch's
        // heads become covered, so no author-stream head is re-allocated once the
        // conflict collapses. Preparation adds the chosen branch head and derives
        // the predecessor and dependencies from the control frontier, so the
        // resolved control directly names every branch.
        let mut covered_control_heads = active_epoch.covered_control_heads.clone();
        let mut metadata = active_epoch.metadata.clone();
        let mut roster = active_epoch.roster.clone();
        for branch in &losing_branches {
            merge_frontier_head(
                &mut covered_control_heads,
                branch.control_head.clone(),
                |head| head.coord.stream_key(),
                |head| head.coord.seq,
            );
            for head in &branch.metadata_heads {
                merge_frontier_head(
                    &mut metadata.heads,
                    head.clone(),
                    |head| head.coord.stream_key(),
                    |head| head.coord.seq,
                );
            }
            for head in &branch.roster_heads {
                merge_frontier_head(
                    &mut roster.heads,
                    head.clone(),
                    |head| head.coord.stream_key(),
                    |head| head.coord.seq,
                );
            }
        }
        covered_control_heads.sort_by_key(|head| head.coord.stream_key());
        metadata.heads.sort_by_key(|head| head.coord.stream_key());
        roster.heads.sort_by_key(|head| head.coord.stream_key());

        // The name is the deterministic metadata selection across the merged
        // frontier. Each branch's selected metadata is already the canonical
        // maximum over its own covered history, so the maximum across the branch
        // selections is the canonical selection over their union.
        let selected_metadata = std::iter::once(chosen_metadata)
            .chain(
                losing_branches
                    .iter()
                    .map(|branch| &branch.selected_metadata),
            )
            .max_by_key(|entry| {
                (
                    entry.metadata_stamp.clone(),
                    entry.author_pubkey.clone(),
                    entry.device_id.clone(),
                    entry.metadata_hash(),
                )
            })
            .expect("a resolution names at least the chosen branch's metadata")
            .clone();
        metadata.selected = selected_metadata.coord();
        metadata.state_hash = selected_metadata.metadata_hash();

        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: active_epoch
                        .covered_control_heads
                        .iter()
                        .find(|head| head.coord.stream_key().author_pubkey == author_pubkey)
                        .map_or_else(
                            || {
                                AuthorStreamId::from_digest(generated_id_digest(
                                    ids,
                                    b"coven.circle-transition-draft-stream.v1\0",
                                ))
                            },
                            |head| head.coord.stream_id,
                        ),
                    author_owner_grant: chosen_metadata.author_owner_grant.clone(),
                    seq: chosen_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(chosen_control.coord.control_hash()),
                    dependencies: vec![chosen_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: active_epoch.common.clone(),
                    metadata,
                    roster,
                    store_membership,
                    covered_control_heads,
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &chosen_roster.members(),
            &control_value
                .value
                .state
                .active_epoch()
                .expect("control resolution constructs an active epoch")
                .store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("control resolution constructs an active epoch")
            .common
            .access_root = access.access_root();
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: chosen_roster.clone(),
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Inherited,
                metadata_successor: false,
            },
            metadata: selected_metadata,
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    /// Build the terminal deletion: a successor of the current control whose
    /// state is `Deleted`, freezing the epoch spine for historical verification
    /// and reclamation. It publishes no roster successor, metadata successor,
    /// access material, or bootstraps — its control inherits the predecessor's
    /// access root.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delete(
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleTransitionError> {
        let context = circle_delete_successor_context(
            store_members,
            current_control,
            current_roster,
            current_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members: _,
            author_pubkey,
            epoch,
            grant_id,
            author_authority,
            key_fingerprint: _,
        } = context;
        let store_root_hash = current_control.value.store_root_hash;
        let circle_id = current_control.value.circle_id;
        let epoch_id = current_control.value.epoch_id();
        let frozen_epoch = MergeActiveCircleEpoch {
            common: epoch.common.clone(),
            metadata: epoch.metadata.clone(),
            roster: epoch.roster.clone(),
            store_membership,
            covered_control_heads: epoch.covered_control_heads.clone(),
        };
        let stream_id = epoch
            .covered_control_heads
            .iter()
            .find(|head| head.coord.stream_key().author_pubkey == author_pubkey)
            .map_or_else(
                || {
                    AuthorStreamId::from_digest(generated_id_digest(
                        ids,
                        b"coven.circle-transition-draft-stream.v1\0",
                    ))
                },
                |head| head.coord.stream_id,
            );
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id,
                    author_owner_grant: grant_id,
                    seq: current_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(current_control.coord.control_hash()),
                    dependencies: vec![current_control.coord.clone()],
                },
                state: CircleControlState::Deleted(DeletedCircle { frozen_epoch }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Inherited,
                metadata_successor: false,
            },
            metadata: current_metadata.clone(),
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access: Vec::new(),
            control,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CircleSemanticSlot<'a> {
    Control {
        circle_id: CircleId,
        control: &'a CircleControlCoord,
    },
    ControlHead {
        circle_id: CircleId,
        control: &'a CircleControlCoord,
    },
    RosterEntry {
        circle_id: CircleId,
        coord: &'a super::circle_roster::CircleRosterCoord,
    },
    RosterHead {
        circle_id: CircleId,
        head: &'a CircleRosterHeadRef,
    },
    RosterResolution {
        circle_id: CircleId,
        resolution: &'a super::circle_roster::CircleRosterConflictResolutionRef,
    },
    MetadataEntry {
        circle_id: CircleId,
        coord: &'a CircleMetadataCoord,
    },
    MetadataHead {
        circle_id: CircleId,
        head: &'a CircleMetadataHeadRef,
    },
}

pub(crate) fn circle_semantic_prefix(slot: CircleSemanticSlot<'_>) -> String {
    match slot {
        CircleSemanticSlot::Control { circle_id, control } => format!(
            "circle-control/{}/merge/entries/{author_pubkey}/{device_id}/{author_owner_grant}/{stream_id}/{seq}/{control_hash}",
            circle_id,
            author_pubkey = control.author_pubkey,
            device_id = control.device_id,
            author_owner_grant = control.author_owner_grant,
            stream_id = control.stream_id,
            seq = control.seq,
            control_hash = control.control_hash,
        ),
        CircleSemanticSlot::ControlHead { circle_id, control } => {
            circle_control_head_prefix(
                circle_id,
                &CircleAuthorStreamKey {
                    author_pubkey: control.author_pubkey.clone(),
                    device_id: control.device_id.clone(),
                    stream_id: control.stream_id,
                    author_owner_grant: control.author_owner_grant.clone(),
                },
                control.seq,
            )
        }
        CircleSemanticSlot::RosterEntry { circle_id, coord } => format!(
            "circles/{circle_id}/roster/entries/{}/{}/{}/{}/{}/{}",
            coord.author_pubkey,
            coord.device_id,
            coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.entry_hash
        ),
        CircleSemanticSlot::RosterHead { circle_id, head } => {
            circle_roster_head_prefix(circle_id, &head.coord.stream_key(), head.coord.seq)
        }
        CircleSemanticSlot::RosterResolution {
            circle_id,
            resolution,
        } => format!(
            "circles/{circle_id}/roster/resolutions/{}/{}/{}",
            resolution.conflict_hash,
            resolution.resolver_pubkey,
            resolution.resolution_hash
        ),
        CircleSemanticSlot::MetadataEntry { circle_id, coord } => format!(
            "circles/{circle_id}/metadata/entries/{}/{}/{}/{}/{}/{}",
            coord.author_pubkey,
            coord.device_id,
            coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.metadata_hash
        ),
        CircleSemanticSlot::MetadataHead { circle_id, head } => {
            circle_metadata_head_prefix(circle_id, &head.coord.stream_key(), head.coord.seq)
        }
    }
}

pub(crate) fn circle_control_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circle-control/{circle_id}/merge/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub(crate) fn circle_roster_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circles/{circle_id}/roster/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub(crate) fn circle_metadata_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circles/{circle_id}/metadata/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub(crate) fn circle_epoch_close_outcome_semantic_prefix(
    circle_id: CircleId,
    close_id: CircleEpochCloseId,
) -> String {
    format!("circles/{circle_id}/epoch-close/{close_id}/outcome")
}

pub(crate) fn circle_epoch_close_intent_semantic_prefix(
    circle_id: CircleId,
    close_id: CircleEpochCloseId,
    intent_hash: ObjectHash,
) -> String {
    format!("circles/{circle_id}/epoch-close/{close_id}/intent/{intent_hash}")
}

pub(crate) fn circle_epoch_close_response_semantic_prefix(
    circle_id: CircleId,
    close_id: CircleEpochCloseId,
    device_id: super::store_commit::StoreDeviceId,
) -> String {
    format!("circles/{circle_id}/epoch-close/{close_id}/responses/{device_id}")
}

pub(crate) fn verify_circle_semantic_prefix(
    actual: &str,
    slot: CircleSemanticSlot<'_>,
) -> Result<(), CircleSemanticPathError> {
    let expected = circle_semantic_prefix(slot);
    if actual == expected {
        Ok(())
    } else {
        Err(CircleSemanticPathError {
            expected,
            actual: actual.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Circle object path {actual:?} does not match signed coordinate path {expected:?}")]
pub(crate) struct CircleSemanticPathError {
    pub expected: String,
    pub actual: String,
}

pub(crate) fn recipient_slot(
    owner: &UserKeypair,
    recipient_pubkey: &str,
    circle_id: CircleId,
) -> Result<String, CircleTransitionError> {
    recipient_slot_with_peer(owner, recipient_pubkey, circle_id)
}

pub(crate) fn recipient_slot_with_peer(
    local_identity: &UserKeypair,
    peer_pubkey: &str,
    circle_id: CircleId,
) -> Result<String, CircleTransitionError> {
    let peer_ed25519: [u8; keys::SIGN_PUBLICKEYBYTES] = hex::decode(peer_pubkey)
        .map_err(|_| CircleTransitionError::InvalidRecipient(peer_pubkey.to_string()))?
        .try_into()
        .map_err(|_| CircleTransitionError::InvalidRecipient(peer_pubkey.to_string()))?;
    let peer_x25519 = keys::ed25519_to_x25519_public_key(&peer_ed25519)
        .map_err(|_| CircleTransitionError::InvalidRecipient(peer_pubkey.to_string()))?;
    let shared = keys::x25519_shared_secret(local_identity.to_x25519_secret_key(), peer_x25519)
        .map_err(|_| CircleTransitionError::InvalidRecipient(peer_pubkey.to_string()))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&shared).expect("HMAC accepts X25519 output");
    mac.update(RECIPIENT_SLOT_DOMAIN);
    mac.update(circle_id.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CircleTransitionError {
    #[error("circle name cannot be empty")]
    EmptyName,
    #[error("circle creator is not a current Store writer")]
    AuthorNotStoreWriter,
    #[error("circle operation author is not a current Circle Owner")]
    AuthorNotCircleOwner,
    #[error("circle operation current state is invalid")]
    InvalidCurrentState,
    #[error("circle transition sequence overflow")]
    SequenceOverflow,
    #[error("circle recipient has an invalid Ed25519 public key: {0}")]
    InvalidRecipient(String),
    #[error("circle member is not a current Store member: {0}")]
    MemberNotInStore(String),
    #[error("circle roster: {0}")]
    Roster(#[from] CircleRosterError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::circle_roster;

    #[test]
    fn semantic_paths_bind_writer_and_author_stream_id() {
        let grant = MembershipGrantId(ObjectHash::digest(b"path grant"));
        let first = circle_roster::CircleRosterCoord {
            author_pubkey: "owner".to_string(),
            device_id: "device".to_string(),
            stream_id: AuthorStreamId::from_bytes([1; 32]),
            author_owner_grant: grant.clone(),
            seq: 1,
            entry_hash: ObjectHash::digest(b"entry"),
        };
        let mut substituted = first.clone();
        substituted.stream_id = AuthorStreamId::from_bytes([2; 32]);
        let circle_id = CircleId::from_bytes([7; 16]);
        let first_path = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
            circle_id,
            coord: &first,
        });

        assert!(first_path.contains(&first.stream_id.to_string()));
        assert!(verify_circle_semantic_prefix(
            &first_path,
            CircleSemanticSlot::RosterEntry {
                circle_id,
                coord: &substituted,
            },
        )
        .is_err());
    }
}
