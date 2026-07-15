//! Circle identities, audience routing, and control coordinates.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Sha256;

use super::membership::{MemberRole, MembershipCoord, OwnerGrantId};
use super::store_commit::{CommitPosition, ObjectHash, STORE_PROTOCOL_VERSION};
use crate::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};
use crate::keys::{self, UserKeypair};

const CIRCLE_ID_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const CIRCLE_ID_LENGTH: usize = 26;
const ROW_ROUTING_KEY_DOMAIN: &[u8] = b"coven.row-routing.v1";
const ROW_ROUTING_ID_DOMAIN: &[u8] = b"coven.row-routing-id.v1\0";
const RECIPIENT_SLOT_DOMAIN: &[u8] = b"coven.circle-recipient-slot.v1\0";
const MEMBERSHIP_STATE_DOMAIN: &str = "coven.circle-store-membership-state.v1";
const ROSTER_DOMAIN: &str = "coven.circle-roster.v1";
const METADATA_DOMAIN: &str = "coven.circle-metadata.v1";
const ACCESS_DOMAIN: &str = "coven.circle-access-leaf.v1";
const CONTROL_DOMAIN: &str = "coven.circle-control.v1";
const ENVELOPE_DOMAIN: &str = "coven.circle-access-envelope.v1";
const CIRCLE_ID_FOUNDER_DOMAIN: &str = "coven.circle-id-founder.v1";
const CIRCLE_EPOCH_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-epoch-id-generation.v1\0";
const ACCESS_LEAF_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-access-leaf-id-generation.v1\0";
const OWNER_GRANT_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-owner-grant-id-generation.v1\0";

pub const CIRCLE_CONTROL_PREFIX: &str = "circle-control/";
pub const CIRCLE_ROSTER_PREFIX: &str = "circles/";
pub const CIRCLE_METADATA_PREFIX: &str = "circles/";
pub const CIRCLE_ACCESS_LEAF_PREFIX: &str = "circles/";
pub const CIRCLE_ACCESS_ENVELOPE_PREFIX: &str = "circles/";

/// A self-certifying 128-bit circle identity encoded as canonical lowercase base32.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CircleId([u8; 16]);

impl CircleId {
    pub(crate) fn founder(
        store_root_hash: ObjectHash,
        author_pubkey: &str,
        owner_grant: &OwnerGrantId,
    ) -> Self {
        #[derive(Serialize)]
        struct Founder<'a> {
            domain: &'static str,
            store_root_hash: ObjectHash,
            author_pubkey: &'a str,
            owner_grant: &'a OwnerGrantId,
        }
        let digest = ObjectHash::digest(
            &serde_json::to_vec(&Founder {
                domain: CIRCLE_ID_FOUNDER_DOMAIN,
                store_root_hash,
                author_pubkey,
                owner_grant,
            })
            .expect("Circle ID founder serialization cannot fail"),
        );
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        Self(bytes)
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for CircleId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for CircleId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&encode_base32(&self.0))
    }
}

impl FromStr for CircleId {
    type Err = CircleIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = decode_base32(value)?;
        let id = Self(bytes);
        if id.to_string() != value || value == "local" {
            return Err(CircleIdError(value.to_string()));
        }
        Ok(id)
    }
}

impl Serialize for CircleId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CircleId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("circle id must be canonical 128-bit lowercase base32: {0:?}")]
pub struct CircleIdError(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircleRole {
    Owner,
    Member,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircleInfo {
    pub id: CircleId,
    pub name: String,
    pub role: CircleRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleOperationState {
    Pending,
    Blocked { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircleOperationInfo {
    pub circle_id: CircleId,
    pub name: String,
    pub state: CircleOperationState,
}

/// The one audience a synced row belongs to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Audience {
    Store,
    Circle(CircleId),
    Local,
}

impl Audience {
    pub fn from_column(value: Option<&str>) -> Result<Self, CircleIdError> {
        match value {
            None => Ok(Self::Store),
            Some("local") => Ok(Self::Local),
            Some(circle) => circle.parse().map(Self::Circle),
        }
    }

    pub fn column_value(&self) -> Option<String> {
        match self {
            Self::Store => None,
            Self::Circle(circle) => Some(circle.to_string()),
            Self::Local => Some("local".to_string()),
        }
    }
}

/// Exact policy-shaped coordinate of one signed circle control entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleControlCoord {
    MergeConcurrent {
        device_id: String,
        author_pubkey: String,
        author_owner_grant: OwnerGrantId,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("circle control coordinate has an empty device/author or zero sequence/generation")]
pub struct CircleControlCoordError;

macro_rules! generated_hex_id {
    ($name:ident, $domain:ident) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name([u8; 16]);

        impl $name {
            fn generate(ids: &dyn crate::id_provider::IdProvider) -> Self {
                Self(generated_id_bytes(ids, $domain))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&hex::encode(self.0))
            }
        }
    };
}

generated_hex_id!(CircleEpochId, CIRCLE_EPOCH_ID_GENERATION_DOMAIN);
generated_hex_id!(AccessLeafId, ACCESS_LEAF_ID_GENERATION_DOMAIN);

fn generated_id_digest(ids: &dyn crate::id_provider::IdProvider, domain: &[u8]) -> ObjectHash {
    let id = ids.new_id();
    let mut material = Vec::with_capacity(domain.len() + id.len());
    material.extend_from_slice(domain);
    material.extend_from_slice(id.as_bytes());
    ObjectHash::digest(&material)
}

fn generated_id_bytes(ids: &dyn crate::id_provider::IdProvider, domain: &[u8]) -> [u8; 16] {
    generated_id_digest(ids, domain).as_bytes()[..16]
        .try_into()
        .expect("SHA-256 digest prefix has fixed length")
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRoster {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub owner_grant: OwnerGrantId,
    pub members: BTreeMap<String, CircleRole>,
    pub author_pubkey: String,
    pub device_id: String,
    pub signature: String,
}

impl CircleRoster {
    fn founder(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        owner_grant: OwnerGrantId,
        device_id: &str,
        signer: &UserKeypair,
    ) -> Self {
        let author_pubkey = keys::public_key_hex(signer);
        let mut roster = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            owner_grant,
            members: BTreeMap::from([(author_pubkey.clone(), CircleRole::Owner)]),
            author_pubkey,
            device_id: device_id.to_string(),
            signature: String::new(),
        };
        roster.signature = keys::sign_hex(signer, &roster.canonical_bytes()).1;
        roster
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            owner_grant: &'a OwnerGrantId,
            members: &'a BTreeMap<String, CircleRole>,
            author_pubkey: &'a str,
            device_id: &'a str,
        }
        serde_json::to_vec(&Signed {
            domain: ROSTER_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            owner_grant: &self.owner_grant,
            members: &self.members,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
        })
        .expect("circle roster serialization cannot fail")
    }

    pub fn roster_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle roster serialization cannot fail"),
        )
    }

    pub fn verify(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.members.get(&self.author_pubkey) == Some(&CircleRole::Owner)
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
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
    pub previous_metadata_hash: Option<ObjectHash>,
    pub metadata_stamp: String,
    pub author_pubkey: String,
    pub device_id: String,
    pub owner_grant: OwnerGrantId,
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
        owner_grant: OwnerGrantId,
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
            previous_metadata_hash: None,
            metadata_stamp: metadata_stamp.to_string(),
            author_pubkey,
            device_id: device_id.to_string(),
            owner_grant,
            signature: String::new(),
        };
        metadata.signature = keys::sign_hex(signer, &metadata.canonical_bytes()).1;
        Ok(metadata)
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            epoch_id: CircleEpochId,
            name: &'a str,
            previous_metadata_hash: Option<ObjectHash>,
            metadata_stamp: &'a str,
            author_pubkey: &'a str,
            device_id: &'a str,
            owner_grant: &'a OwnerGrantId,
        }
        serde_json::to_vec(&Signed {
            domain: METADATA_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            epoch_id: self.epoch_id,
            name: &self.name,
            previous_metadata_hash: self.previous_metadata_hash,
            metadata_stamp: &self.metadata_stamp,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
            owner_grant: &self.owner_grant,
        })
        .expect("circle metadata serialization cannot fail")
    }

    pub fn metadata_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle metadata serialization cannot fail"),
        )
    }

    pub fn verify(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && !self.name.trim().is_empty()
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleAccessDisposition {
    Active {
        keyring: String,
        key_fingerprint: KeyFingerprint,
        roster_hash: ObjectHash,
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
    fn canonical_bytes(&self) -> Vec<u8> {
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

fn verify_merkle_proof(mut hash: ObjectHash, proof: &[MerkleStep], root: ObjectHash) -> bool {
    for step in proof {
        hash = match step {
            MerkleStep::Left(left) => merkle_parent(*left, hash),
            MerkleStep::Right(right) => merkle_parent(hash, *right),
        };
    }
    hash == root
}

fn merkle_root_and_proofs(hashes: &[ObjectHash]) -> (ObjectHash, Vec<Vec<MerkleStep>>) {
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
        author_owner_grant: OwnerGrantId,
        seq: u64,
        previous_control_hash: Option<ObjectHash>,
        roster_heads: Vec<ObjectHash>,
    },
    Serial {
        generation: u64,
        previous_control_hash: Option<ObjectHash>,
        roster: CircleRoster,
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

    fn owner_grant(&self) -> &OwnerGrantId {
        match self {
            Self::MergeConcurrent {
                author_owner_grant, ..
            } => author_owner_grant,
            Self::Serial { roster, .. } => &roster.owner_grant,
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
    pub metadata_hash: ObjectHash,
    pub owners: Vec<String>,
    pub roster_hash: ObjectHash,
    pub access_root: ObjectHash,
    pub store_membership: StoreMembershipStateRef,
    pub author_pubkey: String,
    pub membership_grant: Option<MembershipCoord>,
    pub signature: String,
}

impl CircleControl {
    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            epoch_id: CircleEpochId,
            order: &'a CircleControlOrder,
            key_fingerprint: KeyFingerprint,
            metadata_hash: ObjectHash,
            owners: &'a [String],
            roster_hash: ObjectHash,
            access_root: ObjectHash,
            store_membership: &'a StoreMembershipStateRef,
            author_pubkey: &'a str,
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
            metadata_hash: self.metadata_hash,
            owners: &self.owners,
            roster_hash: self.roster_hash,
            access_root: self.access_root,
            store_membership: &self.store_membership,
            author_pubkey: &self.author_pubkey,
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
        let order_is_valid = match (&self.order, &self.store_membership, &self.membership_grant) {
            (
                CircleControlOrder::MergeConcurrent { device_id, seq, .. },
                StoreMembershipStateRef::MergeConcurrent { .. },
                Some(_),
            ) => !device_id.is_empty() && *seq > 0,
            (
                CircleControlOrder::Serial {
                    generation, roster, ..
                },
                StoreMembershipStateRef::Serial { .. },
                None,
            ) => {
                *generation > 0
                    && roster.verify()
                    && roster.store_root_hash == self.store_root_hash
                    && roster.circle_id == self.circle_id
                    && roster.roster_hash() == self.roster_hash
            }
            _ => false,
        };
        let continuity_is_valid = match self.order.previous_control_hash() {
            None => {
                self.order.ordinal() == 1
                    && self.circle_id
                        == CircleId::founder(
                            self.store_root_hash,
                            &self.author_pubkey,
                            self.order.owner_grant(),
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
                author_owner_grant,
                seq,
                ..
            } => CircleControlCoord::MergeConcurrent {
                device_id: device_id.clone(),
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
pub struct AccessEnvelope {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub owner_pubkey: String,
    pub recipient_slot: String,
    pub control_hash: ObjectHash,
    pub leaf_id: AccessLeafId,
    pub leaf_hash: ObjectHash,
    pub proof: Vec<MerkleStep>,
    pub signature: String,
}

impl AccessEnvelope {
    fn canonical_bytes(&self) -> Vec<u8> {
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
pub struct CircleCreation {
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub keyring: String,
    pub roster: CircleRoster,
    pub metadata: CircleMetadata,
    pub access: Vec<PreparedCircleAccess>,
    pub control: PreparedCircleControl,
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
        let owner_grant = OwnerGrantId(generated_id_digest(ids, OWNER_GRANT_ID_GENERATION_DOMAIN));
        let circle_id = CircleId::founder(store_root_hash, &author_pubkey, &owner_grant);
        let epoch_id = CircleEpochId::generate(ids);
        let keyring = MasterKeyring::generate();
        let encryption = EncryptionService::from(keyring.clone());
        let key_fingerprint = encryption.seal_key_fingerprint();
        let roster = CircleRoster::founder(
            store_root_hash,
            circle_id,
            owner_grant.clone(),
            device_id,
            signer,
        );
        let metadata = CircleMetadata::founder(
            store_root_hash,
            circle_id,
            epoch_id,
            name,
            metadata_stamp,
            device_id,
            owner_grant.clone(),
            signer,
        )?;
        let roster_hash = roster.roster_hash();
        let mut leaves = Vec::with_capacity(store_members.len());
        for (recipient_pubkey, _) in &store_members {
            let recipient_slot = recipient_slot(signer, recipient_pubkey, circle_id)?;
            let disposition = if recipient_pubkey == &author_pubkey {
                CircleAccessDisposition::Active {
                    keyring: keyring.to_serialized(),
                    key_fingerprint,
                    roster_hash,
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
        let order = match store_membership.write_policy() {
            crate::WritePolicy::MergeConcurrent => CircleControlOrder::MergeConcurrent {
                device_id: device_id.to_string(),
                author_owner_grant: owner_grant,
                seq: 1,
                previous_control_hash: None,
                roster_heads: vec![roster_hash],
            },
            crate::WritePolicy::Serial => CircleControlOrder::Serial {
                generation: 1,
                previous_control_hash: None,
                roster: roster.clone(),
            },
        };
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            epoch_id,
            order,
            key_fingerprint,
            metadata_hash: metadata.metadata_hash(),
            owners: vec![author_pubkey.clone()],
            roster_hash,
            access_root,
            store_membership,
            author_pubkey: author_pubkey.clone(),
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
            roster,
            metadata,
            access,
            control,
        })
    }
}

pub fn circle_control_semantic_prefix(circle_id: CircleId, control: &CircleControlCoord) -> String {
    match control {
        CircleControlCoord::MergeConcurrent {
            device_id,
            author_pubkey,
            author_owner_grant,
            seq,
            control_hash,
        } => format!(
            "circle-control/{}/merge/entries/{author_pubkey}/{device_id}/{author_owner_grant}/{seq}/{control_hash}",
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
    }
}

pub fn circle_roster_semantic_prefix(roster: &CircleRoster) -> String {
    format!(
        "circles/{}/roster/entries/{}/{}/{}/1/{}",
        roster.circle_id,
        roster.author_pubkey,
        roster.device_id,
        roster.owner_grant,
        roster.roster_hash()
    )
}

pub fn circle_metadata_semantic_prefix(metadata: &CircleMetadata) -> String {
    format!(
        "circles/{}/metadata/{}/{}/{}",
        metadata.circle_id,
        metadata.author_pubkey,
        metadata.epoch_id,
        metadata.metadata_hash()
    )
}

pub fn circle_access_leaf_semantic_prefix(leaf: &CircleAccessLeaf) -> String {
    format!(
        "circles/{}/access-leaves/{}/{}/{}/{}",
        leaf.circle_id, leaf.owner_pubkey, leaf.epoch_id, leaf.recipient_slot, leaf.leaf_id
    )
}

pub fn circle_access_envelope_semantic_prefix(envelope: &AccessEnvelope) -> String {
    format!(
        "circles/{}/access-envelopes/{}/{}/{}",
        envelope.circle_id, envelope.owner_pubkey, envelope.recipient_slot, envelope.control_hash
    )
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
    let shared = x25519_dalek::x25519(local_identity.to_x25519_secret_key(), peer_x25519);
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
}

/// HMAC identity of one scoped row. It is stable across audience moves and
/// Store-key rotations because it derives from the unique generation-1 key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowRoutingId([u8; 32]);

impl fmt::Debug for RowRoutingId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for RowRoutingId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for RowRoutingId {
    type Err = RowRoutingIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(RowRoutingIdError(value.to_string()));
        }
        let bytes: [u8; 32] = hex::decode(value)
            .map_err(|_| RowRoutingIdError(value.to_string()))?
            .try_into()
            .map_err(|_| RowRoutingIdError(value.to_string()))?;
        Ok(Self(bytes))
    }
}

impl Serialize for RowRoutingId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RowRoutingId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("row routing id must be exactly 64 lowercase hexadecimal characters: {0:?}")]
pub struct RowRoutingIdError(String);

#[derive(Clone)]
pub(crate) struct RowRoutingKey([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowRoutingKeyError {
    #[error("Store keyring has no generation-1 key")]
    MissingGenerationOne,
    #[error("Store keyring has more than one generation-1 key")]
    AmbiguousGenerationOne,
}

pub(crate) fn derive_row_routing_key(
    encryption: &EncryptionService,
    store_root_hash: ObjectHash,
) -> Result<RowRoutingKey, RowRoutingKeyError> {
    let mut generation_one = encryption
        .keyring_entries()
        .into_iter()
        .filter_map(|(generation, key)| (generation == 1).then_some(key));
    let key = generation_one
        .next()
        .ok_or(RowRoutingKeyError::MissingGenerationOne)?;
    if generation_one.next().is_some() {
        return Err(RowRoutingKeyError::AmbiguousGenerationOne);
    }
    let hkdf = Hkdf::<Sha256>::new(Some(ROW_ROUTING_KEY_DOMAIN), &key);
    let mut derived = [0u8; 32];
    hkdf.expand(store_root_hash.as_bytes(), &mut derived)
        .expect("32 bytes is a valid HKDF output length");
    Ok(RowRoutingKey(derived))
}

pub(crate) fn row_routing_id(key: &RowRoutingKey, table: &str, row_id: &str) -> RowRoutingId {
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.0).expect("HMAC accepts a 32-byte key");
    mac.update(ROW_ROUTING_ID_DOMAIN);
    mac.update(&(table.len() as u64).to_be_bytes());
    mac.update(table.as_bytes());
    mac.update(&(row_id.len() as u64).to_be_bytes());
    mac.update(row_id.as_bytes());
    RowRoutingId(mac.finalize().into_bytes().into())
}

fn encode_base32(bytes: &[u8; 16]) -> String {
    let mut output = String::with_capacity(CIRCLE_ID_LENGTH);
    let mut buffer = 0u32;
    let mut bits = 0u8;
    for byte in bytes {
        buffer = (buffer << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            output.push(CIRCLE_ID_ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
            buffer &= (1u32 << bits).wrapping_sub(1);
        }
    }
    if bits != 0 {
        output.push(CIRCLE_ID_ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    output
}

fn decode_base32(value: &str) -> Result<[u8; 16], CircleIdError> {
    if value.len() != CIRCLE_ID_LENGTH {
        return Err(CircleIdError(value.to_string()));
    }
    let mut output = Vec::with_capacity(16);
    let mut buffer = 0u32;
    let mut bits = 0u8;
    for byte in value.bytes() {
        let digit = CIRCLE_ID_ALPHABET
            .iter()
            .position(|candidate| *candidate == byte)
            .ok_or_else(|| CircleIdError(value.to_string()))? as u32;
        buffer = (buffer << 5) | digit;
        bits += 5;
        while bits >= 8 {
            bits -= 8;
            output.push(((buffer >> bits) & 0xff) as u8);
            buffer &= (1u32 << bits).wrapping_sub(1);
        }
    }
    if output.len() != 16 || buffer != 0 {
        return Err(CircleIdError(value.to_string()));
    }
    output
        .try_into()
        .map_err(|_| CircleIdError(value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merkle_proofs_verify_for_every_leaf_in_even_and_odd_layers() {
        for leaf_count in 1..=9 {
            let leaves = (0..leaf_count)
                .map(|index| ObjectHash::digest(format!("leaf-{index}").as_bytes()))
                .collect::<Vec<_>>();
            let (root, proofs) = merkle_root_and_proofs(&leaves);
            assert_eq!(proofs.len(), leaves.len());
            for (index, (leaf, proof)) in leaves.iter().zip(&proofs).enumerate() {
                assert!(
                    verify_merkle_proof(*leaf, proof, root),
                    "leaf {index} of {leaf_count} failed its canonical proof"
                );
            }
        }
    }

    #[test]
    fn founder_payload_is_complete_and_acyclic_for_both_store_policies() {
        let owner = crate::keys::UserKeypair::generate();
        let peer = crate::keys::UserKeypair::generate();
        let owner_pubkey = crate::keys::public_key_hex(&owner);
        let peer_pubkey = crate::keys::public_key_hex(&peer);
        let members = vec![
            (
                owner_pubkey.clone(),
                super::super::membership::MemberRole::Owner,
            ),
            (
                peer_pubkey.clone(),
                super::super::membership::MemberRole::Member,
            ),
        ];

        for membership in [
            StoreMembershipStateRef::merge_concurrent(
                vec![super::super::membership::MembershipCoord {
                    author_pubkey: owner_pubkey.clone(),
                    author_owner_grant: OwnerGrantId(ObjectHash::digest(b"store-owner-grant")),
                    seq: 1,
                    entry_hash: ObjectHash::digest(b"store-founder"),
                }],
                &members,
            ),
            StoreMembershipStateRef::serial(
                Some(super::super::store_commit::CommitPosition {
                    seq: 1,
                    commit_hash: ObjectHash::digest(b"store-commit"),
                }),
                &members,
            ),
        ] {
            let membership_grant = match &membership {
                StoreMembershipStateRef::MergeConcurrent { heads, .. } => Some(heads[0].clone()),
                StoreMembershipStateRef::Serial { .. } => None,
            };
            let ids = crate::id_provider::SequentialIdProvider::new("founder-circle");
            let creation = CircleCreation::founder(
                ObjectHash::digest(b"store-root"),
                "device-a",
                "Household",
                "0000000001000-0000-device-a",
                membership,
                membership_grant,
                members.clone(),
                &ids,
                &owner,
            )
            .expect("construct founder circle");

            assert!(creation.control.verify());
            assert!(creation.metadata.verify());
            assert!(creation.roster.verify());
            assert_eq!(creation.access.len(), 2);
            for access in &creation.access {
                assert!(access.leaf.verify(&creation.control));
                assert!(access.envelope.verify(&creation.control));
                assert!(access
                    .leaf
                    .verify_envelope(&creation.control, &access.envelope));
                assert!(!access.leaf.bytes.windows(64).any(|window| {
                    window == creation.control.coord.control_hash().to_string().as_bytes()
                }));
            }
            assert!(matches!(
                creation
                    .access
                    .iter()
                    .find(|access| access.leaf.value.recipient_pubkey == owner_pubkey)
                    .unwrap()
                    .leaf
                    .value
                    .disposition,
                CircleAccessDisposition::Active { .. }
            ));
            assert!(matches!(
                creation
                    .access
                    .iter()
                    .find(|access| access.leaf.value.recipient_pubkey == peer_pubkey)
                    .unwrap()
                    .leaf
                    .value
                    .disposition,
                CircleAccessDisposition::Inactive
            ));

            if matches!(
                creation.control.value.order,
                CircleControlOrder::MergeConcurrent { .. }
            ) {
                let mut seized = creation.control.value.clone();
                seized.circle_id = CircleId::from_bytes([0x5a; 16]);
                seized.signature = keys::sign_hex(&owner, &seized.canonical_bytes()).1;
                assert!(
                    !seized.verify(),
                    "a founder control must not choose an arbitrary Circle ID"
                );

                let mut discontinuous = creation.control.value.clone();
                let CircleControlOrder::MergeConcurrent { seq, .. } = &mut discontinuous.order
                else {
                    unreachable!()
                };
                *seq = 2;
                discontinuous.signature =
                    keys::sign_hex(&owner, &discontinuous.canonical_bytes()).1;
                assert!(
                    !discontinuous.verify(),
                    "a control without a predecessor must be genesis"
                );
            }
        }
    }

    #[test]
    fn access_verification_rejects_signed_context_and_proof_substitution() {
        let owner = crate::keys::UserKeypair::generate();
        let peer = crate::keys::UserKeypair::generate();
        let owner_pubkey = crate::keys::public_key_hex(&owner);
        let peer_pubkey = crate::keys::public_key_hex(&peer);
        let members = vec![
            (
                owner_pubkey.clone(),
                super::super::membership::MemberRole::Owner,
            ),
            (
                peer_pubkey.clone(),
                super::super::membership::MemberRole::Member,
            ),
        ];
        let membership = StoreMembershipStateRef::merge_concurrent(
            vec![super::super::membership::MembershipCoord {
                author_pubkey: owner_pubkey.clone(),
                author_owner_grant: OwnerGrantId(ObjectHash::digest(b"store-owner-grant")),
                seq: 1,
                entry_hash: ObjectHash::digest(b"store-founder"),
            }],
            &members,
        );
        let grant = match &membership {
            StoreMembershipStateRef::MergeConcurrent { heads, .. } => heads[0].clone(),
            StoreMembershipStateRef::Serial { .. } => unreachable!(),
        };
        let ids = crate::id_provider::SequentialIdProvider::new("access-verification");
        let creation = CircleCreation::founder(
            ObjectHash::digest(b"store-root"),
            "device-a",
            "Household",
            "0000000001000-0000-device-a",
            membership,
            Some(grant),
            members.clone(),
            &ids,
            &owner,
        )
        .expect("construct founder circle");

        let mut wrong_store = creation.access[0].envelope.clone();
        wrong_store.store_root_hash = ObjectHash::digest(b"other-store");
        wrong_store.signature = keys::sign_hex(&owner, &wrong_store.canonical_bytes()).1;
        assert!(!wrong_store.verify(&creation.control));

        let mut non_owner = creation.access[0].envelope.clone();
        non_owner.owner_pubkey = peer_pubkey;
        non_owner.signature = keys::sign_hex(&peer, &non_owner.canonical_bytes()).1;
        assert!(!non_owner.verify(&creation.control));

        let mut substituted_proof = creation.access[0].envelope.clone();
        substituted_proof.proof = creation.access[1].envelope.proof.clone();
        substituted_proof.signature =
            keys::sign_hex(&owner, &substituted_proof.canonical_bytes()).1;
        assert!(!substituted_proof.verify(&creation.control));

        let mut substituted_leaf_id = creation.access[0].envelope.clone();
        substituted_leaf_id.leaf_id = creation.access[1].leaf.value.leaf_id;
        substituted_leaf_id.signature =
            keys::sign_hex(&owner, &substituted_leaf_id.canonical_bytes()).1;
        assert!(substituted_leaf_id.verify(&creation.control));
        assert!(!creation.access[0]
            .leaf
            .verify_envelope(&creation.control, &substituted_leaf_id));

        let mut wrong_membership_leaf = creation.access[0].leaf.value.clone();
        wrong_membership_leaf.store_membership = StoreMembershipStateRef::serial(None, &members);
        wrong_membership_leaf.signature =
            keys::sign_hex(&owner, &wrong_membership_leaf.canonical_bytes()).1;
        let recipient_key =
            keys::ed25519_to_x25519_public_key(&owner.public_key()).expect("convert recipient key");
        let bytes = keys::seal_box_encrypt(
            &serde_json::to_vec(&wrong_membership_leaf).expect("serialize forged leaf"),
            &recipient_key,
        );
        let wrong_membership_leaf = PreparedAccessLeaf {
            leaf_hash: ObjectHash::digest(&bytes),
            bytes,
            value: wrong_membership_leaf,
        };
        assert!(!wrong_membership_leaf.verify(&creation.control));

        let mut wrong_policy_control = creation.control.value.clone();
        wrong_policy_control.store_membership = StoreMembershipStateRef::serial(None, &members);
        wrong_policy_control.membership_grant = None;
        wrong_policy_control.signature =
            keys::sign_hex(&owner, &wrong_policy_control.canonical_bytes()).1;
        assert!(!wrong_policy_control.verify());
    }

    #[test]
    fn circle_id_round_trips_only_its_canonical_lowercase_base32() {
        let id = CircleId::from_bytes([0; 16]);
        let encoded = id.to_string();
        assert_eq!(encoded.len(), CIRCLE_ID_LENGTH);
        assert_eq!(encoded.parse::<CircleId>().unwrap(), id);
        assert!(encoded.to_uppercase().parse::<CircleId>().is_err());
        assert!("local".parse::<CircleId>().is_err());
        assert!(format!("{}b", &encoded[..25]).parse::<CircleId>().is_err());
    }

    #[test]
    fn row_routing_id_is_stable_across_store_key_rotation() {
        let root = ObjectHash::digest(b"store-root");
        let before = EncryptionService::from_key([1u8; 32]);
        let after = before
            .with_appended_generation(2, [2u8; 32])
            .expect("append generation");
        let before_id = row_routing_id(
            &derive_row_routing_key(&before, root).unwrap(),
            "accounts",
            "row-1",
        );
        let after_id = row_routing_id(
            &derive_row_routing_key(&after, root).unwrap(),
            "accounts",
            "row-1",
        );
        assert_eq!(before_id, after_id);
        assert_ne!(
            before_id,
            row_routing_id(
                &derive_row_routing_key(&after, root).unwrap(),
                "accounts",
                "row-2",
            )
        );
    }

    #[test]
    fn row_routing_key_requires_exactly_one_generation_one_key() {
        let root = ObjectHash::digest(b"store-root");
        let missing = EncryptionService::from_key_at_generation(2, [2u8; 32]);
        assert!(matches!(
            derive_row_routing_key(&missing, root),
            Err(RowRoutingKeyError::MissingGenerationOne)
        ));

        let ambiguous = EncryptionService::from_keyring([(1, [1u8; 32]), (1, [2u8; 32])])
            .expect("build forked generation one");
        assert!(matches!(
            derive_row_routing_key(&ambiguous, root),
            Err(RowRoutingKeyError::AmbiguousGenerationOne)
        ));
    }

    #[tokio::test]
    async fn control_history_caches_the_verified_access_owner_and_rejects_second_genesis() {
        let author = crate::keys::UserKeypair::generate();
        let author_pubkey = crate::keys::public_key_hex(&author);
        let earlier_owner = loop {
            let candidate = crate::keys::UserKeypair::generate();
            if crate::keys::public_key_hex(&candidate) < author_pubkey {
                break candidate;
            }
        };
        let earlier_owner_pubkey = crate::keys::public_key_hex(&earlier_owner);
        let members = vec![
            (
                author_pubkey.clone(),
                super::super::membership::MemberRole::Owner,
            ),
            (
                earlier_owner_pubkey.clone(),
                super::super::membership::MemberRole::Owner,
            ),
        ];
        let store_root_hash = ObjectHash::digest(b"multi-owner-store-root");
        let grant = super::super::membership::MembershipCoord {
            author_pubkey: author_pubkey.clone(),
            author_owner_grant: OwnerGrantId(ObjectHash::digest(b"store-owner-grant")),
            seq: 1,
            entry_hash: ObjectHash::digest(b"store-founder"),
        };
        let membership = StoreMembershipStateRef::merge_concurrent(vec![grant.clone()], &members);
        let ids = crate::id_provider::SequentialIdProvider::new("multi-owner-control");
        let creation = CircleCreation::founder(
            store_root_hash,
            "device-a",
            "Household",
            "0000000001000-0000-device-a",
            membership,
            Some(grant.clone()),
            members,
            &ids,
            &author,
        )
        .expect("construct founder circle");
        let mut control = creation.control.value.clone();
        control.owners = vec![earlier_owner_pubkey, author_pubkey.clone()];
        control.owners.sort();
        assert_ne!(control.owners[0], control.author_pubkey);
        control.signature = keys::sign_hex(&author, &control.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control.coord(),
            bytes: serde_json::to_vec(&control).expect("serialize control"),
            value: control,
        };
        let reference = super::super::store_commit::CircleControlRef {
            circle_id: creation.circle_id,
            control: control.coord.clone(),
        };
        let commit = super::super::store_commit::StoreBatchCommit::signed_batch(
            store_root_hash,
            crate::WriteId::from_generated("multi-owner-control-commit".to_string()),
            "device-a".to_string(),
            super::super::store_commit::StoreCommitOrder::MergeConcurrent {
                seq: 1,
                previous_commit_hash: None,
                dependencies: BTreeMap::new(),
            },
            Some(grant),
            None,
            Vec::new(),
            vec![reference],
            None,
            &[],
            &author,
        )
        .expect("sign Store commit");
        let own_access = creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == author_pubkey)
            .expect("author access");
        let verified = super::super::circle_ops::VerifiedCircleReference {
            circle_id: creation.circle_id,
            control: control.clone(),
            local_access: Some(super::super::circle_ops::VerifiedCircleAccess {
                leaf: own_access.leaf.clone(),
                active: Some(super::super::circle_ops::VerifiedCircleActive {
                    roster: creation.roster.clone(),
                    metadata: creation.metadata.clone(),
                }),
            }),
        };
        let db = super::super::test_helpers::open_test_db();
        let first_commit = commit.clone();
        db.call(move |conn| {
            crate::database::Database::record_verified_circle_activations_on(
                conn,
                &first_commit,
                &[verified],
            )
        })
        .await
        .expect("record multi-Owner control");
        let circle_id = creation.circle_id.to_string();
        let cached_owner = db
            .call(move |conn| {
                conn.query_row(
                    "SELECT owner_pubkey FROM circle_access_cache WHERE circle_id = ?1",
                    [circle_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(crate::database::DbError::from)
            })
            .await
            .expect("read cached access owner");
        assert_eq!(cached_owner, author_pubkey);

        let mut second_value = control.value.clone();
        second_value.metadata_hash = ObjectHash::digest(b"different founder metadata");
        second_value.signature = keys::sign_hex(&author, &second_value.canonical_bytes()).1;
        let second_control = PreparedCircleControl {
            coord: second_value.coord(),
            bytes: serde_json::to_vec(&second_value).expect("serialize second founder control"),
            value: second_value,
        };
        let second_commit = super::super::store_commit::StoreBatchCommit::signed_batch(
            store_root_hash,
            crate::WriteId::from_generated("second-founder-control-commit".to_string()),
            "device-a".to_string(),
            super::super::store_commit::StoreCommitOrder::MergeConcurrent {
                seq: 2,
                previous_commit_hash: Some(commit.commit_hash()),
                dependencies: BTreeMap::new(),
            },
            control.value.membership_grant.clone(),
            None,
            Vec::new(),
            vec![super::super::store_commit::CircleControlRef {
                circle_id: creation.circle_id,
                control: second_control.coord.clone(),
            }],
            None,
            &[],
            &author,
        )
        .expect("sign second founder Store commit");
        let error = db
            .call(move |conn| {
                crate::database::Database::record_verified_circle_activations_on(
                    conn,
                    &second_commit,
                    &[super::super::circle_ops::VerifiedCircleReference {
                        circle_id: creation.circle_id,
                        control: second_control,
                        local_access: None,
                    }],
                )
            })
            .await
            .expect_err("a Circle cannot accept a second founder control");
        assert!(
            error.to_string().contains("already has a founder"),
            "{error}"
        );
    }
}
