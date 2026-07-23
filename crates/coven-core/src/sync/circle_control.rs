//! Circle metadata, access records, controls, and creation objects.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::causal_grants::AuthorStreamId;
use super::circle::{generated_id_digest, AccessLeafId, CircleEpochId, CircleId};
use super::circle_roster::{
    CircleAuthorStreamKey, CircleGrantCreationAuthority, CircleMaterializedRoster,
    CircleRosterChain, CircleRosterEntry, CircleRosterError, CircleRosterHead, CircleRosterHeadRef,
    CircleRosterStateRef, MergeCircleRosterStateRef, ResolvedCircleRoster,
};
use super::membership::{MemberRole, MembershipGrantCreationAuthority, MembershipGrantId};
use super::membership::{MembershipHeadRef, StoreMembershipConflictResolutionRef};
use super::storage::ExactObjectRef;
use super::store_commit::{
    CommitFrontier, ObjectHash, OwnerRecoveryCursor, SnapshotImageRef, StoreDeviceRegistration,
    SuccessorLink, STORE_PROTOCOL_VERSION,
};
use crate::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};
use crate::keys::{self, UserKeypair};

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
pub struct CircleMetadata {
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

    pub fn metadata_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle metadata serialization cannot fail"),
        )
    }

    pub fn coord(&self) -> CircleMetadataCoord {
        CircleMetadataCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            metadata_hash: self.metadata_hash(),
        }
    }

    pub fn verify(&self) -> bool {
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
pub struct CircleMetadataHead {
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

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle metadata head serialization cannot fail"),
        )
    }

    pub fn coord(&self) -> CircleMetadataCoord {
        CircleMetadataCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            metadata_hash: self.tip_hash,
        }
    }

    pub fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
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
pub struct MergeCircleMetadataStateRef {
    pub heads: Vec<CircleMetadataHeadRef>,
    pub selected: CircleMetadataCoord,
    pub state_hash: ObjectHash,
}

pub type CircleMetadataStateRef = MergeCircleMetadataStateRef;

/// Exact Circle database image offered when one recipient becomes active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleBootstrapRef {
    pub coverage: CommitFrontier,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
    pub image: SnapshotImageRef,
    pub blobs: Vec<crate::blob::locator::StoredBlobRef>,
}

impl CircleBootstrapRef {
    pub(crate) fn verify_for_access(&self, access: &CircleAccessLeaf) -> bool {
        if super::store_commit::validate_commit_frontier(&self.coverage).is_err() {
            return false;
        }
        let blobs_are_canonical = self.blobs.windows(2).all(|pair| {
            serde_json::to_vec(&pair[0]).expect("stored blob reference serialization cannot fail")
                < serde_json::to_vec(&pair[1])
                    .expect("stored blob reference serialization cannot fail")
        });
        if !blobs_are_canonical
            || self.blobs.iter().any(|blob| {
                blob.locator().audience()
                    != crate::blob::locator::RemoteAudience::Circle(access.circle_id)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessLeaf {
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

    pub fn verify_signature(&self) -> bool {
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
pub struct MergeCircleControlOrder {
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub previous_control_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleControlCoord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveCircleEpochCore {
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    pub owners: Vec<String>,
    pub access_root: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeActiveCircleEpoch {
    pub common: ActiveCircleEpochCore,
    pub metadata: MergeCircleMetadataStateRef,
    pub roster: MergeCircleRosterStateRef,
    pub store_membership: StoreMembershipStateRef,
    pub covered_control_heads: Vec<MergeCircleControlHeadRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCircleControlHeadRef {
    pub coord: CircleControlCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MergeCircleOwnerAuthorityRef {
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
    pub fn grant_id(&self, author_pubkey: &str) -> MembershipGrantId {
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
pub struct CircleControlValue {
    pub order: MergeCircleControlOrder,
    pub active_epoch: MergeActiveCircleEpoch,
    pub author_authority: MergeCircleOwnerAuthorityRef,
    pub membership_authority: MembershipGrantCreationAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControl {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub value: CircleControlValue,
    pub author_pubkey: String,
    pub signature: String,
}

impl CircleControl {
    pub fn active_common(&self) -> &ActiveCircleEpochCore {
        &self.value.active_epoch.common
    }

    pub fn epoch_id(&self) -> CircleEpochId {
        self.active_common().epoch_id
    }

    pub fn key_fingerprint(&self) -> KeyFingerprint {
        self.active_common().key_fingerprint
    }

    pub fn owners(&self) -> &[String] {
        &self.active_common().owners
    }

    pub fn access_root(&self) -> ObjectHash {
        self.active_common().access_root
    }

    pub fn roster_state_ref(&self) -> CircleRosterStateRef {
        self.value.active_epoch.roster.clone()
    }

    pub fn metadata_state_ref(&self) -> CircleMetadataStateRef {
        self.value.active_epoch.metadata.clone()
    }

    pub fn store_membership_state_ref(&self) -> StoreMembershipStateRef {
        self.value.active_epoch.store_membership.clone()
    }

    pub fn membership_authority(&self) -> &MembershipGrantCreationAuthority {
        &self.value.membership_authority
    }

    pub fn previous_control_hash(&self) -> Option<ObjectHash> {
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

    pub fn ordinal(&self) -> u64 {
        self.value.order.seq
    }

    pub fn author_grant_id(&self) -> MembershipGrantId {
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

    pub fn control_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle control serialization cannot fail"),
        )
    }

    pub fn verify(&self) -> bool {
        let order = &self.value.order;
        let active_epoch = &self.value.active_epoch;
        let author_authority = &self.value.author_authority;
        let grant_id = author_authority.grant_id(&self.author_pubkey);
        let stream_key = CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: order.device_id.clone(),
            stream_id: order.stream_id,
            author_owner_grant: order.author_owner_grant.clone(),
        };
        let covered_are_canonical = active_epoch
            .covered_control_heads
            .windows(2)
            .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key());
        let own_predecessor = active_epoch
            .covered_control_heads
            .iter()
            .find(|head| head.coord.stream_key() == stream_key);
        let expected_dependencies = active_epoch
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
                if roster == &active_epoch.roster
        );
        let founder = order.seq == 1 && active_epoch.covered_control_heads.is_empty();
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
        let common = &active_epoch.common;
        let owners_are_canonical =
            !common.owners.is_empty() && common.owners.windows(2).all(|pair| pair[0] < pair[1]);
        self.version == STORE_PROTOCOL_VERSION
            && owners_are_canonical
            && order_is_valid
            && continuity_is_valid
            && founder_identity_is_valid
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub fn coord(&self) -> CircleControlCoord {
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
pub struct CircleControlHead {
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

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle control head serialization cannot fail"),
        )
    }

    pub fn verify(&self, registration: &StoreDeviceRegistration) -> bool {
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
pub struct AccessEnvelope {
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

    pub fn verify(
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
pub struct PreparedCircleControl {
    pub coord: CircleControlCoord,
    pub bytes: Vec<u8>,
    pub value: CircleControl,
}

impl PreparedCircleControl {
    pub fn verify(&self) -> bool {
        self.bytes
            == serde_json::to_vec(&self.value).expect("circle control serialization cannot fail")
            && self.value.verify()
            && self.coord == self.value.coord()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedAccessLeaf {
    pub bytes: Vec<u8>,
    pub value: CircleAccessLeaf,
    pub leaf_hash: ObjectHash,
}

impl PreparedAccessLeaf {
    pub fn verify(
        &self,
        control: &PreparedCircleControl,
        candidate_family: super::store_commit::CandidateFamilyId,
    ) -> bool {
        self.value.verify_for_control(control, candidate_family)
            && ObjectHash::digest(&self.bytes) == self.leaf_hash
    }

    pub fn verify_envelope(
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
pub struct PreparedCircleAccess {
    pub leaf: PreparedAccessLeaf,
    pub envelope: AccessEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterPolicyObjects {
    pub entry: CircleRosterEntry,
    pub head: CircleRosterHead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleTransitionPolicyObjects {
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
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
}

#[derive(Debug, Clone)]
struct FounderRosterObjects {
    entry: CircleRosterEntry,
    resolved: ResolvedCircleRoster,
}

struct PreparedAccessMaterial {
    value: CircleAccessLeaf,
    bytes: Vec<u8>,
    leaf_hash: ObjectHash,
}

#[allow(clippy::too_many_arguments)]
fn prepare_access_material(
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
    bootstrap_recipient: Option<(&str, &CircleBootstrapRef)>,
    ids: &dyn crate::id_provider::IdProvider,
    signer: &UserKeypair,
) -> Result<Vec<PreparedAccessMaterial>, CircleTransitionError> {
    let author_pubkey = keys::public_key_hex(signer);
    store_members
        .iter()
        .map(|(recipient_pubkey, _)| {
            let recipient_slot = recipient_slot(signer, recipient_pubkey, circle_id)?;
            let disposition = if roster_members.contains_key(recipient_pubkey) {
                CircleAccessDisposition::Active {
                    keyring: keyring.to_string(),
                    key_fingerprint,
                    roster: roster_state.clone(),
                    bootstrap: bootstrap_recipient
                        .filter(|(target, _)| *target == recipient_pubkey)
                        .map(|(_, bootstrap)| bootstrap.clone()),
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
            let recipient_ed25519: [u8; keys::SIGN_PUBLICKEYBYTES] = hex::decode(recipient_pubkey)
                .map_err(|_| CircleTransitionError::InvalidRecipient(recipient_pubkey.clone()))?
                .try_into()
                .map_err(|_| CircleTransitionError::InvalidRecipient(recipient_pubkey.clone()))?;
            let recipient_x25519 = keys::ed25519_to_x25519_public_key(&recipient_ed25519)
                .map_err(|_| CircleTransitionError::InvalidRecipient(recipient_pubkey.clone()))?;
            let plaintext =
                serde_json::to_vec(&value).expect("circle access serialization cannot fail");
            let bytes = keys::seal_box_encrypt(&plaintext, &recipient_x25519);
            let leaf_hash = ObjectHash::digest(&bytes);
            Ok(PreparedAccessMaterial {
                value,
                bytes,
                leaf_hash,
            })
        })
        .collect()
}

fn prepare_access_envelopes(
    store_root_hash: ObjectHash,
    candidate_family: super::store_commit::CandidateFamilyId,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    leaves: Vec<PreparedAccessMaterial>,
    proofs: Vec<Vec<MerkleStep>>,
    signer: &UserKeypair,
) -> Vec<PreparedCircleAccess> {
    let author_pubkey = keys::public_key_hex(signer);
    leaves
        .into_iter()
        .zip(proofs)
        .map(|(leaf, proof)| {
            let mut envelope = AccessEnvelope {
                version: STORE_PROTOCOL_VERSION,
                store_root_hash,
                candidate_family,
                circle_id,
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
            envelope.signature = keys::sign_hex(signer, &envelope.canonical_bytes()).1;
            PreparedCircleAccess {
                leaf: PreparedAccessLeaf {
                    bytes: leaf.bytes,
                    value: leaf.value,
                    leaf_hash: leaf.leaf_hash,
                },
                envelope,
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCircleTransition {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleMaterializedRoster,
    pub policy_objects: CircleTransitionPolicyObjects,
    pub metadata: CircleMetadata,
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
}

impl PreparedCircleTransition {
    pub fn resolved_roster(&self) -> CircleMaterializedRoster {
        self.roster.clone()
    }

    pub fn control_ref(
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
    active_epoch: &'a MergeActiveCircleEpoch,
    grant_id: MembershipGrantId,
    author_authority: MergeCircleOwnerAuthorityRef,
    key_fingerprint: KeyFingerprint,
}

fn circle_successor_context<'a>(
    mut store_members: Vec<(String, MemberRole)>,
    current_control: &'a PreparedCircleControl,
    current_roster: &CircleMaterializedRoster,
    current_metadata: &CircleMetadata,
    keyring: &str,
    signer: &UserKeypair,
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
    let active_epoch = &current_control.value.value.active_epoch;
    let (grant_id, record) = current_roster
        .active_grants()
        .find(|(_, record)| {
            record.member_pubkey == author_pubkey && record.role == super::circle::CircleRole::Owner
        })
        .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
    let author_authority = match &record.creation_authority {
        CircleGrantCreationAuthority::Entry(created_at) => MergeCircleOwnerAuthorityRef::Roster {
            roster: active_epoch.roster.clone(),
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
        active_epoch,
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
        let leaves = prepare_access_material(
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
            None,
            ids,
            signer,
        )?;
        let leaf_hashes = leaves.iter().map(|leaf| leaf.leaf_hash).collect::<Vec<_>>();
        let (access_root, proofs) = merkle_root_and_proofs(&leaf_hashes);
        let common = ActiveCircleEpochCore {
            epoch_id,
            key_fingerprint,
            owners: vec![author_pubkey.clone()],
            access_root,
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
            active_epoch: MergeActiveCircleEpoch {
                common,
                metadata: metadata_state,
                roster: roster_state.clone(),
                store_membership,
                covered_control_heads: Vec::new(),
            },
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
        let access = prepare_access_envelopes(
            store_root_hash,
            candidate_family,
            circle_id,
            &control,
            leaves,
            proofs,
            signer,
        );
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_serialized(),
            roster,
            policy: policy_objects,
            metadata,
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
            active_epoch,
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
                active_epoch: MergeActiveCircleEpoch {
                    common: active_epoch.common.clone(),
                    metadata: active_epoch.metadata.clone(),
                    roster: roster_state.clone(),
                    store_membership,
                    covered_control_heads: active_epoch.covered_control_heads.clone(),
                },
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let leaves = prepare_access_material(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &roster.members(),
            &control_value.value.active_epoch.store_membership,
            &store_members,
            Some((&member_pubkey, &bootstrap)),
            ids,
            signer,
        )?;
        let leaf_hashes = leaves.iter().map(|leaf| leaf.leaf_hash).collect::<Vec<_>>();
        let (access_root, proofs) = merkle_root_and_proofs(&leaf_hashes);
        control_value.value.active_epoch.common.access_root = access_root;
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let access = prepare_access_envelopes(
            store_root_hash,
            candidate_family,
            circle_id,
            &control,
            leaves,
            proofs,
            signer,
        );
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
            active_epoch,
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
            active_epoch: MergeActiveCircleEpoch {
                common: active_epoch.common.clone(),
                metadata: active_epoch.metadata.clone(),
                roster: active_epoch.roster.clone(),
                store_membership,
                covered_control_heads: active_epoch.covered_control_heads.clone(),
            },
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

        let leaves = prepare_access_material(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &current_roster.members(),
            &control_value.active_epoch.store_membership,
            &store_members,
            None,
            ids,
            signer,
        )?;
        let leaf_hashes = leaves.iter().map(|leaf| leaf.leaf_hash).collect::<Vec<_>>();
        let (access_root, proofs) = merkle_root_and_proofs(&leaf_hashes);
        control_value.active_epoch.common.access_root = access_root;
        control_value.active_epoch.metadata = metadata_state;
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
        let access = prepare_access_envelopes(
            store_root_hash,
            candidate_family,
            circle_id,
            &control,
            leaves,
            proofs,
            signer,
        );
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: policy_objects,
            metadata,
            access,
            control,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CircleSemanticSlot<'a> {
    Control {
        circle_id: CircleId,
        control: &'a CircleControlCoord,
    },
    ControlHead {
        circle_id: CircleId,
        control: &'a CircleControlCoord,
        head_hash: ObjectHash,
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

pub fn circle_semantic_prefix(slot: CircleSemanticSlot<'_>) -> String {
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
        CircleSemanticSlot::ControlHead {
            circle_id,
            control,
            head_hash: _,
        } => {
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

pub fn circle_control_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circle-control/{circle_id}/merge/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub fn circle_roster_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circles/{circle_id}/roster/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub fn circle_metadata_head_prefix(
    circle_id: CircleId,
    stream: &CircleAuthorStreamKey,
    seq: u64,
) -> String {
    format!(
        "circles/{circle_id}/metadata/heads/{}/{}/{}/{}/{seq}",
        stream.author_pubkey, stream.device_id, stream.author_owner_grant, stream.stream_id
    )
}

pub fn verify_circle_semantic_prefix(
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
pub struct CircleSemanticPathError {
    pub expected: String,
    pub actual: String,
}

pub fn recipient_slot(
    owner: &UserKeypair,
    recipient_pubkey: &str,
    circle_id: CircleId,
) -> Result<String, CircleTransitionError> {
    recipient_slot_with_peer(owner, recipient_pubkey, circle_id)
}

pub fn recipient_slot_with_peer(
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
    #[error("circle Store membership reference does not hash the supplied member state")]
    MembershipStateMismatch,
    #[error("circle creator is not a current Store writer")]
    AuthorNotStoreWriter,
    #[error("circle operation author is not a current Circle Owner")]
    AuthorNotCircleOwner,
    #[error("circle operation current state is invalid")]
    InvalidCurrentState,
    #[error("circle transition sequence overflow")]
    SequenceOverflow,
    #[error("circle creator Store grant does not match the Store policy")]
    MembershipGrantPolicy,
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

    #[test]
    fn semantic_paths_bind_writer_and_author_stream_id() {
        let grant = MembershipGrantId(ObjectHash::digest(b"path grant"));
        let first = super::super::circle_roster::CircleRosterCoord {
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
