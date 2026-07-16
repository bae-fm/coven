//! Circle metadata, access records, controls, and creation objects.

use std::collections::BTreeMap;

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::causal_grants::AuthorStreamId;
use super::circle::{generated_id_digest, AccessLeafId, CircleEpochId, CircleId};
use super::circle_roster::{
    CircleAuthorStreamKey, CircleMaterializedRoster, CircleRosterChain, CircleRosterEntry,
    CircleRosterError, CircleRosterHead, CircleRosterHeadRef, CircleRosterStateRef,
    ResolvedCircleRoster, SerialCircleRoster,
};
use super::membership::{MemberRole, MembershipCoord, MembershipGrantId};
use super::store_commit::{CommitPosition, ObjectHash, STORE_PROTOCOL_VERSION};
use crate::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};
use crate::keys::{self, UserKeypair};

const RECIPIENT_SLOT_DOMAIN: &[u8] = b"coven.circle-recipient-slot.v1\0";
const MEMBERSHIP_STATE_DOMAIN: &str = "coven.circle-store-membership-state.v1";
const METADATA_DOMAIN: &str = "coven.circle-metadata.v1";
const METADATA_HEAD_DOMAIN: &str = "coven.circle-metadata-head.v1";
const ACCESS_DOMAIN: &str = "coven.circle-access-leaf.v1";
const CONTROL_DOMAIN: &str = "coven.circle-control.v1";
const ENVELOPE_DOMAIN: &str = "coven.circle-access-envelope.v1";
const OWNER_GRANT_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-owner-grant-id-generation.v1\0";

/// Exact policy-shaped coordinate of one signed circle control entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleControlCoord {
    MergeConcurrent {
        device_id: String,
        stream_id: AuthorStreamId,
        author_pubkey: String,
        author_owner_grant: MembershipGrantId,
        seq: u64,
        control_hash: ObjectHash,
    },
    Serial {
        author_pubkey: String,
        generation: u64,
        control_hash: ObjectHash,
    },
}

impl CircleControlCoord {
    pub fn control_hash(&self) -> ObjectHash {
        match self {
            Self::MergeConcurrent { control_hash, .. } | Self::Serial { control_hash, .. } => {
                *control_hash
            }
        }
    }

    pub fn validate(&self) -> Result<(), CircleControlCoordError> {
        match self {
            Self::MergeConcurrent {
                device_id,
                stream_id,
                author_pubkey,
                seq,
                ..
            } if device_id.is_empty() || author_pubkey.is_empty() || *seq == 0 => {
                Err(CircleControlCoordError)
            }
            Self::Serial {
                author_pubkey,
                generation,
                ..
            } if author_pubkey.is_empty() || *generation == 0 => Err(CircleControlCoordError),
            _ => Ok(()),
        }
    }

    pub fn stream_key(&self) -> Option<CircleAuthorStreamKey> {
        match self {
            Self::MergeConcurrent {
                device_id,
                stream_id,
                author_pubkey,
                author_owner_grant,
                ..
            } => Some(CircleAuthorStreamKey {
                author_pubkey: author_pubkey.clone(),
                device_id: device_id.clone(),
                stream_id: *stream_id,
                author_owner_grant: author_owner_grant.clone(),
            }),
            Self::Serial { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("circle control coordinate has an empty device/author or zero sequence/generation")]
pub struct CircleControlCoordError;

/// The exact Store membership state whose identities require access dispositions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreMembershipStateRef {
    MergeConcurrent {
        heads: Vec<MembershipCoord>,
        state_hash: ObjectHash,
    },
    Serial {
        position: Option<CommitPosition>,
        state_hash: ObjectHash,
    },
}

impl StoreMembershipStateRef {
    pub fn merge_concurrent(
        mut heads: Vec<MembershipCoord>,
        members: &[(String, MemberRole)],
    ) -> Self {
        heads.sort();
        Self::MergeConcurrent {
            heads,
            state_hash: store_membership_state_hash(members),
        }
    }

    pub fn serial(position: Option<CommitPosition>, members: &[(String, MemberRole)]) -> Self {
        Self::Serial {
            position,
            state_hash: store_membership_state_hash(members),
        }
    }

    pub fn state_hash(&self) -> ObjectHash {
        match self {
            Self::MergeConcurrent { state_hash, .. } | Self::Serial { state_hash, .. } => {
                *state_hash
            }
        }
    }

    pub fn write_policy(&self) -> crate::WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => crate::WritePolicy::MergeConcurrent,
            Self::Serial { .. } => crate::WritePolicy::Serial,
        }
    }
}

pub fn store_membership_state_hash(members: &[(String, MemberRole)]) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        members: &'a BTreeMap<&'a str, &'a MemberRole>,
    }
    let sorted = members
        .iter()
        .map(|(pubkey, role)| (pubkey.as_str(), role))
        .collect::<BTreeMap<_, _>>();
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: MEMBERSHIP_STATE_DOMAIN,
            members: &sorted,
        })
        .expect("membership-state serialization cannot fail"),
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
    ) -> Result<Self, CircleCreateError> {
        if name.trim().is_empty() {
            return Err(CircleCreateError::EmptyName);
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
    pub signature: String,
}

impl CircleMetadataHead {
    pub(crate) fn signed(metadata: &CircleMetadata, signer: &UserKeypair) -> Self {
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
            signature: String::new(),
        };
        head.signature = keys::sign_hex(signer, &head.canonical_bytes()).1;
        head
    }

    fn canonical_bytes(&self) -> Vec<u8> {
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

    pub fn verify(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.seq > 0
            && !self.device_id.is_empty()
            && keys::verify_signature_hex(
                &self.author_pubkey,
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
}

impl CircleMetadataHeadRef {
    pub(crate) fn from_head(head: &CircleMetadataHead) -> Self {
        Self {
            coord: head.coord(),
            head_hash: head.head_hash(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleMetadataStateRef {
    MergeConcurrent {
        heads: Vec<CircleMetadataHeadRef>,
        selected: CircleMetadataCoord,
        state_hash: ObjectHash,
    },
    Serial {
        current: CircleMetadataCoord,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleAccessDisposition {
    Active {
        keyring: String,
        key_fingerprint: KeyFingerprint,
        roster: CircleRosterStateRef,
    },
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessLeaf {
    pub version: u32,
    pub store_root_hash: ObjectHash,
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
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleControlOrder {
    MergeConcurrent {
        device_id: String,
        stream_id: AuthorStreamId,
        author_owner_grant: MembershipGrantId,
        seq: u64,
        previous_control_hash: Option<ObjectHash>,
        dependencies: Vec<CircleControlCoord>,
    },
    Serial {
        generation: u64,
        previous_control_hash: Option<ObjectHash>,
        roster: SerialCircleRoster,
    },
}

impl CircleControlOrder {
    pub(crate) fn previous_control_hash(&self) -> Option<ObjectHash> {
        match self {
            Self::MergeConcurrent {
                previous_control_hash,
                ..
            }
            | Self::Serial {
                previous_control_hash,
                ..
            } => *previous_control_hash,
        }
    }

    pub(crate) fn ordinal(&self) -> u64 {
        match self {
            Self::MergeConcurrent { seq, .. } => *seq,
            Self::Serial { generation, .. } => *generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleOwnerAuthorityRef {
    MergeConcurrent {
        roster: CircleRosterStateRef,
        grant_id: MembershipGrantId,
        created_at: super::circle_roster::CircleRosterCoord,
    },
    Serial {
        roster_state_hash: ObjectHash,
        grant_id: MembershipGrantId,
        created_at_generation: u64,
    },
}

impl CircleOwnerAuthorityRef {
    pub fn grant_id(&self) -> &MembershipGrantId {
        match self {
            Self::MergeConcurrent { grant_id, .. } | Self::Serial { grant_id, .. } => grant_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControl {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub order: CircleControlOrder,
    pub key_fingerprint: KeyFingerprint,
    pub metadata: CircleMetadataStateRef,
    pub owners: Vec<String>,
    pub roster: CircleRosterStateRef,
    pub access_root: ObjectHash,
    pub store_membership: StoreMembershipStateRef,
    pub author_pubkey: String,
    pub author_authority: CircleOwnerAuthorityRef,
    pub membership_grant: Option<MembershipCoord>,
    pub signature: String,
}

impl CircleControl {
    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            epoch_id: CircleEpochId,
            order: &'a CircleControlOrder,
            key_fingerprint: KeyFingerprint,
            metadata: &'a CircleMetadataStateRef,
            owners: &'a [String],
            roster: &'a CircleRosterStateRef,
            access_root: ObjectHash,
            store_membership: &'a StoreMembershipStateRef,
            author_pubkey: &'a str,
            author_authority: &'a CircleOwnerAuthorityRef,
            membership_grant: Option<&'a MembershipCoord>,
        }
        serde_json::to_vec(&Signed {
            domain: CONTROL_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            epoch_id: self.epoch_id,
            order: &self.order,
            key_fingerprint: self.key_fingerprint,
            metadata: &self.metadata,
            owners: &self.owners,
            roster: &self.roster,
            access_root: self.access_root,
            store_membership: &self.store_membership,
            author_pubkey: &self.author_pubkey,
            author_authority: &self.author_authority,
            membership_grant: self.membership_grant.as_ref(),
        })
        .expect("circle control serialization cannot fail")
    }

    pub fn control_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle control serialization cannot fail"),
        )
    }

    pub fn verify(&self) -> bool {
        let owners_are_canonical = !self.owners.is_empty()
            && self.owners.windows(2).all(|pair| pair[0] < pair[1])
            && self.owners.binary_search(&self.author_pubkey).is_ok();
        let order_is_valid = match (
            &self.order,
            &self.store_membership,
            &self.membership_grant,
            &self.author_authority,
        ) {
            (
                CircleControlOrder::MergeConcurrent {
                    device_id,
                    author_owner_grant,
                    seq,
                    ..
                },
                StoreMembershipStateRef::MergeConcurrent { .. },
                Some(_),
                CircleOwnerAuthorityRef::MergeConcurrent { grant_id, .. },
            ) => !device_id.is_empty() && *seq > 0 && author_owner_grant == grant_id,
            (
                CircleControlOrder::Serial {
                    generation, roster, ..
                },
                StoreMembershipStateRef::Serial { .. },
                None,
                CircleOwnerAuthorityRef::Serial { .. },
            ) => *generation > 0 && roster.verify(),
            _ => false,
        };
        let continuity_is_valid = match self.order.previous_control_hash() {
            None => {
                let authority_is_founder_roster = match (&self.author_authority, &self.roster) {
                    (
                        CircleOwnerAuthorityRef::MergeConcurrent { roster, .. },
                        CircleRosterStateRef::MergeConcurrent { .. },
                    ) => roster == &self.roster,
                    (
                        CircleOwnerAuthorityRef::Serial {
                            roster_state_hash, ..
                        },
                        CircleRosterStateRef::Serial { state_hash },
                    ) => roster_state_hash == state_hash,
                    _ => false,
                };
                self.order.ordinal() == 1
                    && authority_is_founder_roster
                    && self.circle_id
                        == CircleId::founder(
                            self.store_root_hash,
                            &self.author_pubkey,
                            self.author_authority.grant_id(),
                        )
            }
            Some(_) => self.order.ordinal() > 1,
        };
        self.version == STORE_PROTOCOL_VERSION
            && owners_are_canonical
            && order_is_valid
            && continuity_is_valid
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub fn coord(&self) -> CircleControlCoord {
        let control_hash = self.control_hash();
        match &self.order {
            CircleControlOrder::MergeConcurrent {
                device_id,
                stream_id,
                author_owner_grant,
                seq,
                ..
            } => CircleControlCoord::MergeConcurrent {
                device_id: device_id.clone(),
                stream_id: *stream_id,
                author_pubkey: self.author_pubkey.clone(),
                author_owner_grant: author_owner_grant.clone(),
                seq: *seq,
                control_hash,
            },
            CircleControlOrder::Serial { generation, .. } => CircleControlCoord::Serial {
                author_pubkey: self.author_pubkey.clone(),
                generation: *generation,
                control_hash,
            },
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
    pub signature: String,
}

impl CircleControlHead {
    pub(crate) fn signed(control: &CircleControl, signer: &UserKeypair) -> Self {
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: control.store_root_hash,
            circle_id: control.circle_id,
            control: control.coord(),
            signature: String::new(),
        };
        head.signature = keys::sign_hex(signer, &head.canonical_bytes()).1;
        head
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            control: &'a CircleControlCoord,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-control-head.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            control: &self.control,
        })
        .expect("circle control head serialization cannot fail")
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle control head serialization cannot fail"),
        )
    }

    pub fn verify(&self) -> bool {
        let author = match &self.control {
            CircleControlCoord::MergeConcurrent { author_pubkey, .. }
            | CircleControlCoord::Serial { author_pubkey, .. } => author_pubkey,
        };
        self.version == STORE_PROTOCOL_VERSION
            && self.control.validate().is_ok()
            && keys::verify_signature_hex(author, &self.signature, &self.canonical_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessEnvelope {
    pub version: u32,
    pub store_root_hash: ObjectHash,
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

    pub fn verify(&self, control: &PreparedCircleControl) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.store_root_hash == control.value.store_root_hash
            && self.circle_id == control.value.circle_id
            && control
                .value
                .owners
                .binary_search(&self.owner_pubkey)
                .is_ok()
            && self.control_hash == control.coord.control_hash()
            && keys::verify_signature_hex(
                &self.owner_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
            && verify_merkle_proof(self.leaf_hash, &self.proof, control.value.access_root)
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
    pub fn verify(&self, control: &PreparedCircleControl) -> bool {
        self.value.verify_signature()
            && self.value.store_root_hash == control.value.store_root_hash
            && self.value.circle_id == control.value.circle_id
            && self.value.epoch_id == control.value.epoch_id
            && self.value.store_membership == control.value.store_membership
            && match &self.value.disposition {
                CircleAccessDisposition::Active { roster, .. } => roster == &control.value.roster,
                CircleAccessDisposition::Inactive => true,
            }
            && control
                .value
                .owners
                .binary_search(&self.value.owner_pubkey)
                .is_ok()
            && ObjectHash::digest(&self.bytes) == self.leaf_hash
    }

    pub fn verify_envelope(
        &self,
        control: &PreparedCircleControl,
        envelope: &AccessEnvelope,
    ) -> bool {
        self.verify(control)
            && envelope.verify(control)
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
pub enum CircleCreationPolicyObjects {
    MergeConcurrent {
        roster_entry: CircleRosterEntry,
        roster_head: CircleRosterHead,
        metadata_head: CircleMetadataHead,
        control_head: CircleControlHead,
    },
    Serial {
        roster: SerialCircleRoster,
    },
}

#[derive(Debug, Clone)]
enum FounderRosterObjects {
    MergeConcurrent {
        entry: CircleRosterEntry,
        head: CircleRosterHead,
        resolved: ResolvedCircleRoster,
    },
    Serial {
        roster: SerialCircleRoster,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleCreation {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub policy_objects: CircleCreationPolicyObjects,
    pub metadata: CircleMetadata,
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
}

impl CircleCreation {
    pub fn resolved_roster(&self) -> CircleMaterializedRoster {
        match &self.policy_objects {
            CircleCreationPolicyObjects::MergeConcurrent { roster_entry, .. } => {
                CircleMaterializedRoster::MergeConcurrent(
                    CircleRosterChain::from_entries(vec![roster_entry.clone()])
                        .expect("verified founder roster entry resolves")
                        .resolved(),
                )
            }
            CircleCreationPolicyObjects::Serial { roster } => {
                CircleMaterializedRoster::Serial(roster.clone())
            }
        }
    }

    pub fn control_ref(&self) -> super::store_commit::CircleControlRef {
        match &self.policy_objects {
            CircleCreationPolicyObjects::MergeConcurrent { control_head, .. } => {
                super::store_commit::CircleControlRef::MergeConcurrent {
                    circle_id: self.circle_id,
                    control: self.control.coord.clone(),
                    head_hash: control_head.head_hash(),
                }
            }
            CircleCreationPolicyObjects::Serial { .. } => {
                super::store_commit::CircleControlRef::Serial {
                    circle_id: self.circle_id,
                    control: self.control.coord.clone(),
                }
            }
        }
    }
}

impl CircleCreation {
    #[allow(clippy::too_many_arguments)]
    pub fn founder(
        store_root_hash: ObjectHash,
        device_id: &str,
        name: &str,
        metadata_stamp: &str,
        store_membership: StoreMembershipStateRef,
        membership_grant: Option<MembershipCoord>,
        mut store_members: Vec<(String, MemberRole)>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &UserKeypair,
    ) -> Result<Self, CircleCreateError> {
        let author_pubkey = keys::public_key_hex(signer);
        store_members.sort_by(|left, right| left.0.cmp(&right.0));
        store_members.dedup_by(|left, right| left.0 == right.0);
        if store_membership.state_hash() != store_membership_state_hash(&store_members) {
            return Err(CircleCreateError::MembershipStateMismatch);
        }
        if !store_members
            .iter()
            .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
        {
            return Err(CircleCreateError::AuthorNotStoreWriter);
        }
        let owner_grant =
            MembershipGrantId(generated_id_digest(ids, OWNER_GRANT_ID_GENERATION_DOMAIN));
        let author_stream_id = AuthorStreamId::generate(ids);
        let circle_id = CircleId::founder(store_root_hash, &author_pubkey, &owner_grant);
        let epoch_id = CircleEpochId::generate(ids);
        let keyring = MasterKeyring::generate();
        let encryption = EncryptionService::from(keyring.clone());
        let key_fingerprint = encryption.seal_key_fingerprint();
        let roster_objects = match store_membership.write_policy() {
            crate::WritePolicy::MergeConcurrent => {
                let entry = CircleRosterEntry::founder(
                    store_root_hash,
                    circle_id,
                    device_id,
                    author_stream_id,
                    owner_grant.clone(),
                    signer,
                );
                let head = CircleRosterHead::signed(&entry, signer);
                let resolved = CircleRosterChain::from_entries(vec![entry.clone()])?.resolved();
                FounderRosterObjects::MergeConcurrent {
                    entry,
                    head,
                    resolved,
                }
            }
            crate::WritePolicy::Serial => FounderRosterObjects::Serial {
                roster: SerialCircleRoster::founder(author_pubkey.clone(), owner_grant.clone(), 1),
            },
        };
        let roster_state = match &roster_objects {
            FounderRosterObjects::MergeConcurrent { head, resolved, .. } => {
                CircleRosterStateRef::MergeConcurrent {
                    heads: vec![CircleRosterHeadRef::from_head(head)],
                    state_hash: resolved.state_hash,
                }
            }
            FounderRosterObjects::Serial { roster } => CircleRosterStateRef::Serial {
                state_hash: roster.state_hash,
            },
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
        let metadata_head = CircleMetadataHead::signed(&metadata, signer);
        let metadata_state = match store_membership.write_policy() {
            crate::WritePolicy::MergeConcurrent => CircleMetadataStateRef::MergeConcurrent {
                heads: vec![CircleMetadataHeadRef::from_head(&metadata_head)],
                selected: metadata.coord(),
                state_hash: metadata.metadata_hash(),
            },
            crate::WritePolicy::Serial => CircleMetadataStateRef::Serial {
                current: metadata.coord(),
            },
        };
        let mut leaves = Vec::with_capacity(store_members.len());
        for (recipient_pubkey, _) in &store_members {
            let recipient_slot = recipient_slot(signer, recipient_pubkey, circle_id)?;
            let disposition = if recipient_pubkey == &author_pubkey {
                CircleAccessDisposition::Active {
                    keyring: keyring.to_serialized(),
                    key_fingerprint,
                    roster: roster_state.clone(),
                }
            } else {
                CircleAccessDisposition::Inactive
            };
            let mut value = CircleAccessLeaf {
                version: STORE_PROTOCOL_VERSION,
                store_root_hash,
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
                .map_err(|_| CircleCreateError::InvalidRecipient(recipient_pubkey.clone()))?
                .try_into()
                .map_err(|_| CircleCreateError::InvalidRecipient(recipient_pubkey.clone()))?;
            let recipient_x25519 = keys::ed25519_to_x25519_public_key(&recipient_ed25519)
                .map_err(|_| CircleCreateError::InvalidRecipient(recipient_pubkey.clone()))?;
            let plaintext =
                serde_json::to_vec(&value).expect("circle access serialization cannot fail");
            let bytes = keys::seal_box_encrypt(&plaintext, &recipient_x25519);
            let leaf_hash = ObjectHash::digest(&bytes);
            leaves.push((value, bytes, leaf_hash));
        }
        let leaf_hashes = leaves.iter().map(|leaf| leaf.2).collect::<Vec<_>>();
        let (access_root, proofs) = merkle_root_and_proofs(&leaf_hashes);
        match (&store_membership, &membership_grant) {
            (StoreMembershipStateRef::MergeConcurrent { .. }, Some(_))
            | (StoreMembershipStateRef::Serial { .. }, None) => {}
            _ => return Err(CircleCreateError::MembershipGrantPolicy),
        }
        let order = match &roster_objects {
            FounderRosterObjects::MergeConcurrent { .. } => CircleControlOrder::MergeConcurrent {
                device_id: device_id.to_string(),
                stream_id: author_stream_id,
                author_owner_grant: owner_grant.clone(),
                seq: 1,
                previous_control_hash: None,
                dependencies: Vec::new(),
            },
            FounderRosterObjects::Serial { roster } => CircleControlOrder::Serial {
                generation: 1,
                previous_control_hash: None,
                roster: roster.clone(),
            },
        };
        let author_authority = match &roster_objects {
            FounderRosterObjects::MergeConcurrent { entry, .. } => {
                CircleOwnerAuthorityRef::MergeConcurrent {
                    roster: roster_state.clone(),
                    grant_id: owner_grant.clone(),
                    created_at: entry.coord(),
                }
            }
            FounderRosterObjects::Serial { roster } => CircleOwnerAuthorityRef::Serial {
                roster_state_hash: roster.state_hash,
                grant_id: owner_grant.clone(),
                created_at_generation: 1,
            },
        };
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            epoch_id,
            order,
            key_fingerprint,
            metadata: metadata_state,
            owners: vec![author_pubkey.clone()],
            roster: roster_state,
            access_root,
            store_membership,
            author_pubkey: author_pubkey.clone(),
            author_authority,
            membership_grant,
            signature: String::new(),
        };
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let policy_objects = match roster_objects {
            FounderRosterObjects::MergeConcurrent { entry, head, .. } => {
                CircleCreationPolicyObjects::MergeConcurrent {
                    roster_entry: entry,
                    roster_head: head,
                    metadata_head,
                    control_head: CircleControlHead::signed(&control.value, signer),
                }
            }
            FounderRosterObjects::Serial { roster } => {
                CircleCreationPolicyObjects::Serial { roster }
            }
        };
        let access = leaves
            .into_iter()
            .zip(proofs)
            .map(|((value, bytes, leaf_hash), proof)| {
                let mut envelope = AccessEnvelope {
                    version: STORE_PROTOCOL_VERSION,
                    store_root_hash,
                    circle_id,
                    owner_pubkey: author_pubkey.clone(),
                    recipient_slot: value.recipient_slot.clone(),
                    control_hash: control.coord.control_hash(),
                    leaf_id: value.leaf_id,
                    leaf_hash,
                    value_hash: ObjectHash::digest(
                        &serde_json::to_vec(&value)
                            .expect("circle access leaf serialization cannot fail"),
                    ),
                    proof,
                    signature: String::new(),
                };
                envelope.signature = keys::sign_hex(signer, &envelope.canonical_bytes()).1;
                PreparedCircleAccess {
                    leaf: PreparedAccessLeaf {
                        bytes,
                        value,
                        leaf_hash,
                    },
                    envelope,
                }
            })
            .collect();
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_serialized(),
            policy_objects,
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
    MetadataEntry {
        circle_id: CircleId,
        coord: &'a CircleMetadataCoord,
    },
    MetadataHead {
        circle_id: CircleId,
        head: &'a CircleMetadataHeadRef,
    },
    AccessLeaf {
        circle_id: CircleId,
        owner_pubkey: &'a str,
        epoch_id: CircleEpochId,
        recipient_slot: &'a str,
        leaf_id: AccessLeafId,
    },
    AccessEnvelope {
        circle_id: CircleId,
        owner_pubkey: &'a str,
        recipient_slot: &'a str,
        control_hash: ObjectHash,
    },
}

pub fn circle_semantic_prefix(slot: CircleSemanticSlot<'_>) -> String {
    match slot {
        CircleSemanticSlot::Control { circle_id, control } => match control {
        CircleControlCoord::MergeConcurrent {
            device_id,
            stream_id,
            author_pubkey,
            author_owner_grant,
            seq,
            control_hash,
        } => format!(
            "circle-control/{}/merge/entries/{author_pubkey}/{device_id}/{author_owner_grant}/{stream_id}/{seq}/{control_hash}",
            circle_id
        ),
        CircleControlCoord::Serial {
            author_pubkey,
            generation,
            control_hash,
        } => format!(
            "circle-control/{}/serial/{author_pubkey}/{generation}/{control_hash}",
            circle_id
        ),
        },
        CircleSemanticSlot::ControlHead {
            circle_id,
            control,
            head_hash,
        } => {
            let CircleControlCoord::MergeConcurrent {
                author_pubkey,
                device_id,
                stream_id,
                author_owner_grant,
                seq,
                ..
            } = control
            else {
                unreachable!("Serial controls have no independent head")
            };
            format!(
                "circle-control/{circle_id}/merge/heads/{author_pubkey}/{device_id}/{author_owner_grant}/{stream_id}/{seq}/{head_hash}"
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
        CircleSemanticSlot::RosterHead { circle_id, head } => format!(
            "circles/{circle_id}/roster/heads/{}/{}/{}/{}/{}/{}",
            head.coord.author_pubkey,
            head.coord.device_id,
            head.coord.author_owner_grant,
            head.coord.stream_id,
            head.coord.seq,
            head.head_hash
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
        CircleSemanticSlot::MetadataHead { circle_id, head } => format!(
            "circles/{circle_id}/metadata/heads/{}/{}/{}/{}/{}/{}",
            head.coord.author_pubkey,
            head.coord.device_id,
            head.coord.author_owner_grant,
            head.coord.stream_id,
            head.coord.seq,
            head.head_hash
        ),
        CircleSemanticSlot::AccessLeaf {
            circle_id,
            owner_pubkey,
            epoch_id,
            recipient_slot,
            leaf_id,
        } => format!(
            "circles/{circle_id}/access-leaves/{owner_pubkey}/{epoch_id}/{recipient_slot}/{leaf_id}"
        ),
        CircleSemanticSlot::AccessEnvelope {
            circle_id,
            owner_pubkey,
            recipient_slot,
            control_hash,
        } => format!(
            "circles/{circle_id}/access-envelopes/{owner_pubkey}/{recipient_slot}/{control_hash}"
        ),
    }
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
) -> Result<String, CircleCreateError> {
    recipient_slot_with_peer(owner, recipient_pubkey, circle_id)
}

pub fn recipient_slot_with_peer(
    local_identity: &UserKeypair,
    peer_pubkey: &str,
    circle_id: CircleId,
) -> Result<String, CircleCreateError> {
    let peer_ed25519: [u8; keys::SIGN_PUBLICKEYBYTES] = hex::decode(peer_pubkey)
        .map_err(|_| CircleCreateError::InvalidRecipient(peer_pubkey.to_string()))?
        .try_into()
        .map_err(|_| CircleCreateError::InvalidRecipient(peer_pubkey.to_string()))?;
    let peer_x25519 = keys::ed25519_to_x25519_public_key(&peer_ed25519)
        .map_err(|_| CircleCreateError::InvalidRecipient(peer_pubkey.to_string()))?;
    let shared = keys::x25519_shared_secret(local_identity.to_x25519_secret_key(), peer_x25519)
        .map_err(|_| CircleCreateError::InvalidRecipient(peer_pubkey.to_string()))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&shared).expect("HMAC accepts X25519 output");
    mac.update(RECIPIENT_SLOT_DOMAIN);
    mac.update(circle_id.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CircleCreateError {
    #[error("circle name cannot be empty")]
    EmptyName,
    #[error("circle Store membership reference does not hash the supplied member state")]
    MembershipStateMismatch,
    #[error("circle creator is not a current Store writer")]
    AuthorNotStoreWriter,
    #[error("circle creator Store grant does not match the Store policy")]
    MembershipGrantPolicy,
    #[error("circle recipient has an invalid Ed25519 public key: {0}")]
    InvalidRecipient(String),
    #[error("circle roster: {0}")]
    Roster(#[from] CircleRosterError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serial_creation(
        owner: &UserKeypair,
        peers: &[&UserKeypair],
        id_seed: &str,
    ) -> CircleCreation {
        let mut members = vec![(keys::public_key_hex(owner), MemberRole::Owner)];
        members.extend(
            peers
                .iter()
                .map(|peer| (keys::public_key_hex(peer), MemberRole::Member)),
        );
        CircleCreation::founder(
            ObjectHash::digest(b"Circle control test Store"),
            "owner-device",
            "Household",
            "0000000001000-0000-owner-device",
            StoreMembershipStateRef::serial(None, &members),
            None,
            members,
            &crate::id_provider::SequentialIdProvider::new(id_seed),
            owner,
        )
        .expect("create Serial Circle")
    }

    #[test]
    fn founder_metadata_and_control_bind_the_exact_grant_bearing_roster() {
        let owner = UserKeypair::generate();
        let creation = serial_creation(&owner, &[], "historical-founder-authority");
        let CircleOwnerAuthorityRef::Serial {
            roster_state_hash,
            grant_id,
            created_at_generation,
        } = &creation.control.value.author_authority
        else {
            panic!("Serial creation must carry Serial Owner authority")
        };
        let CircleCreationPolicyObjects::Serial { roster } = &creation.policy_objects else {
            panic!("Serial creation must carry a Serial roster")
        };

        assert_eq!(*roster_state_hash, roster.state_hash);
        assert_eq!(*created_at_generation, 1);
        assert!(roster.authorizes_owner_grant(
            &keys::public_key_hex(&owner),
            grant_id,
            *created_at_generation,
        ));
        assert_eq!(
            creation.metadata.author_roster,
            creation.control.value.roster
        );
        assert_eq!(
            creation.metadata.key_fingerprint,
            creation.control.value.key_fingerprint
        );
    }

    #[test]
    fn semantic_paths_bind_writer_and_author_stream_id() {
        let grant = MembershipGrantId(ObjectHash::digest(b"path grant"));
        let first = super::super::circle_roster::CircleRosterCoord {
            author_pubkey: "owner".to_string(),
            device_id: "device".to_string(),
            stream_id: AuthorStreamId::from_bytes([1; 16]),
            author_owner_grant: grant.clone(),
            seq: 1,
            entry_hash: ObjectHash::digest(b"entry"),
        };
        let mut substituted = first.clone();
        substituted.stream_id = AuthorStreamId::from_bytes([2; 16]);
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

    #[test]
    fn receiver_rejects_ciphertext_bound_to_another_plaintext_hash() {
        let owner = UserKeypair::generate();
        let peer = UserKeypair::generate();
        let mut creation = serial_creation(&owner, &[&peer], "access-ciphertext-binding");
        let peer_pubkey = keys::public_key_hex(&peer);
        let access = creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == peer_pubkey)
            .expect("peer access")
            .clone();
        let mut substituted_value = access.leaf.value.clone();
        substituted_value.leaf_id = AccessLeafId::generate(
            &crate::id_provider::SequentialIdProvider::new("substituted-plaintext"),
        );
        substituted_value.signature =
            keys::sign_hex(&owner, &substituted_value.canonical_bytes()).1;
        let peer_x25519 = keys::ed25519_to_x25519_public_key(&peer.public_key())
            .expect("convert peer public key");
        let substituted_bytes = keys::seal_box_encrypt(
            &serde_json::to_vec(&substituted_value).expect("serialize substituted leaf"),
            &peer_x25519,
        );
        let substituted_hash = ObjectHash::digest(&substituted_bytes);
        creation.control.value.access_root = substituted_hash;
        creation.control.value.signature =
            keys::sign_hex(&owner, &creation.control.value.canonical_bytes()).1;
        creation.control.coord = creation.control.value.coord();
        creation.control.bytes =
            serde_json::to_vec(&creation.control.value).expect("serialize substituted control");
        let mut envelope = access.envelope;
        envelope.control_hash = creation.control.coord.control_hash();
        envelope.leaf_hash = substituted_hash;
        envelope.proof.clear();
        envelope.signature = keys::sign_hex(&owner, &envelope.canonical_bytes()).1;
        let decrypted = keys::seal_box_decrypt(&substituted_bytes, &peer.to_x25519_secret_key())
            .expect("open substituted leaf");
        let decrypted_value = serde_json::from_slice(&decrypted).expect("parse substituted leaf");
        let prepared = PreparedAccessLeaf {
            bytes: substituted_bytes,
            value: decrypted_value,
            leaf_hash: substituted_hash,
        };

        assert!(!prepared.verify_envelope(&creation.control, &envelope));
    }
}
