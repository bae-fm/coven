//! Recipient dispatch and canonical access-leaf commitments.

use std::fmt;
use std::str::FromStr;

use hkdf::Hkdf;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Sha256;

use super::circle::{AccessLeafId, CircleEpochId, CircleId};
use super::store_commit::ObjectHash;
use crate::keys::{self, UserKeypair};

const RECIPIENT_SLOT_DOMAIN: &[u8] = b"coven.circle-recipient-slot.v1";
const ACCESS_MERKLE_LEAF_DOMAIN: &[u8] = b"coven.circle-access-merkle-leaf.v1\0";
const ACCESS_MERKLE_NODE_DOMAIN: &[u8] = b"coven.circle-access-merkle-node.v1\0";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecipientSlot([u8; 32]);

impl RecipientSlot {
    pub fn derive(
        identity: &UserKeypair,
        peer_ed25519_pubkey_hex: &str,
        circle_id: CircleId,
    ) -> Result<Self, CircleAccessError> {
        let shared = keys::x25519_shared_secret(identity, peer_ed25519_pubkey_hex)?;
        let hkdf = Hkdf::<Sha256>::new(Some(RECIPIENT_SLOT_DOMAIN), &shared);
        let mut slot = [0; 32];
        hkdf.expand(circle_id.as_bytes(), &mut slot)
            .expect("32 bytes is a valid HKDF output length");
        Ok(Self(slot))
    }
}

impl fmt::Debug for RecipientSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for RecipientSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for RecipientSlot {
    type Err = CircleAccessError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(CircleAccessError::InvalidRecipientSlot(value.to_string()));
        }
        let bytes: [u8; 32] = hex::decode(value)
            .map_err(|_| CircleAccessError::InvalidRecipientSlot(value.to_string()))?
            .try_into()
            .map_err(|_| CircleAccessError::InvalidRecipientSlot(value.to_string()))?;
        Ok(Self(bytes))
    }
}

impl Serialize for RecipientSlot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RecipientSlot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum AccessMerkleStep {
    Left(ObjectHash),
    Right(ObjectHash),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessMerkleProof {
    steps: Vec<AccessMerkleStep>,
}

impl AccessMerkleProof {
    pub fn verify(&self, leaf_hash: ObjectHash, expected_root: ObjectHash) -> bool {
        let mut node = access_leaf_node(leaf_hash);
        for step in &self.steps {
            node = match step {
                AccessMerkleStep::Left(left) => access_parent_node(*left, node),
                AccessMerkleStep::Right(right) => access_parent_node(node, *right),
            };
        }
        node == expected_root
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessMerkleTree {
    leaf_hashes: Vec<ObjectHash>,
    root: ObjectHash,
}

impl AccessMerkleTree {
    pub fn from_leaf_hashes(
        hashes: impl IntoIterator<Item = ObjectHash>,
    ) -> Result<Self, CircleAccessError> {
        let mut leaf_hashes = hashes.into_iter().collect::<Vec<_>>();
        if leaf_hashes.is_empty() {
            return Err(CircleAccessError::EmptyAccessTree);
        }
        leaf_hashes.sort();
        if leaf_hashes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CircleAccessError::DuplicateLeafHash);
        }
        let root = merkle_root(&leaf_hashes);
        Ok(Self { leaf_hashes, root })
    }

    pub fn root(&self) -> ObjectHash {
        self.root
    }

    pub fn proof(&self, leaf_hash: ObjectHash) -> Result<AccessMerkleProof, CircleAccessError> {
        let mut index = self
            .leaf_hashes
            .binary_search(&leaf_hash)
            .map_err(|_| CircleAccessError::LeafNotInTree)?;
        let mut level = self
            .leaf_hashes
            .iter()
            .copied()
            .map(access_leaf_node)
            .collect::<Vec<_>>();
        let mut steps = Vec::new();
        while level.len() > 1 {
            if index % 2 == 1 {
                steps.push(AccessMerkleStep::Left(level[index - 1]));
            } else if index + 1 < level.len() {
                steps.push(AccessMerkleStep::Right(level[index + 1]));
            }
            level = next_merkle_level(&level);
            index /= 2;
        }
        Ok(AccessMerkleProof { steps })
    }
}

fn merkle_root(leaf_hashes: &[ObjectHash]) -> ObjectHash {
    let mut level = leaf_hashes
        .iter()
        .copied()
        .map(access_leaf_node)
        .collect::<Vec<_>>();
    while level.len() > 1 {
        level = next_merkle_level(&level);
    }
    level[0]
}

fn next_merkle_level(level: &[ObjectHash]) -> Vec<ObjectHash> {
    level
        .chunks(2)
        .map(|pair| {
            if pair.len() == 2 {
                access_parent_node(pair[0], pair[1])
            } else {
                pair[0]
            }
        })
        .collect()
}

fn access_leaf_node(leaf_hash: ObjectHash) -> ObjectHash {
    let mut bytes = Vec::with_capacity(ACCESS_MERKLE_LEAF_DOMAIN.len() + 32);
    bytes.extend_from_slice(ACCESS_MERKLE_LEAF_DOMAIN);
    bytes.extend_from_slice(leaf_hash.as_bytes());
    ObjectHash::digest(&bytes)
}

fn access_parent_node(left: ObjectHash, right: ObjectHash) -> ObjectHash {
    let mut bytes = Vec::with_capacity(ACCESS_MERKLE_NODE_DOMAIN.len() + 64);
    bytes.extend_from_slice(ACCESS_MERKLE_NODE_DOMAIN);
    bytes.extend_from_slice(left.as_bytes());
    bytes.extend_from_slice(right.as_bytes());
    ObjectHash::digest(&bytes)
}

pub fn access_leaf_semantic_prefix(
    circle_id: CircleId,
    owner_pubkey: &str,
    epoch_id: CircleEpochId,
    recipient_slot: RecipientSlot,
    leaf_id: AccessLeafId,
) -> Result<String, CircleAccessError> {
    validate_owner_pubkey(owner_pubkey)?;
    Ok(format!(
        "circles/{circle_id}/access-leaves/{owner_pubkey}/{epoch_id}/{recipient_slot}/{leaf_id}"
    ))
}

pub fn access_envelope_semantic_prefix(
    circle_id: CircleId,
    owner_pubkey: &str,
    recipient_slot: RecipientSlot,
    control_hash: ObjectHash,
) -> Result<String, CircleAccessError> {
    validate_owner_pubkey(owner_pubkey)?;
    Ok(format!(
        "circles/{circle_id}/access-envelopes/{owner_pubkey}/{recipient_slot}/{control_hash}"
    ))
}

fn validate_owner_pubkey(owner_pubkey: &str) -> Result<(), CircleAccessError> {
    let bytes: [u8; keys::SIGN_PUBLICKEYBYTES] = hex::decode(owner_pubkey)
        .map_err(|_| CircleAccessError::InvalidOwnerPubkey)?
        .try_into()
        .map_err(|_| CircleAccessError::InvalidOwnerPubkey)?;
    if hex::encode(bytes) != owner_pubkey {
        return Err(CircleAccessError::InvalidOwnerPubkey);
    }
    keys::ed25519_to_x25519_public_key(&bytes)
        .map(|_| ())
        .map_err(|_| CircleAccessError::InvalidOwnerPubkey)
}

#[derive(Debug, thiserror::Error)]
pub enum CircleAccessError {
    #[error(transparent)]
    Key(#[from] keys::KeyError),
    #[error("recipient slot must be exactly 64 lowercase hexadecimal characters: {0:?}")]
    InvalidRecipientSlot(String),
    #[error("circle access owner is not a canonical Ed25519 public key")]
    InvalidOwnerPubkey,
    #[error("access Merkle tree has no leaves")]
    EmptyAccessTree,
    #[error("access Merkle tree contains a duplicate leaf hash")]
    DuplicateLeafHash,
    #[error("access leaf hash is absent from the Merkle tree")]
    LeafNotInTree,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::{public_key_hex, UserKeypair};
    use crate::sync::circle::{AccessLeafId, CircleEpochId, CircleId};
    use crate::sync::store_commit::ObjectHash;

    #[test]
    fn recipient_slot_is_shared_only_by_the_owner_recipient_circle_pair() {
        let owner = UserKeypair::generate();
        let recipient = UserKeypair::generate();
        let other = UserKeypair::generate();
        let circle = CircleId::from_bytes([1; 16]);

        let owner_slot =
            RecipientSlot::derive(&owner, &public_key_hex(&recipient), circle).unwrap();
        let recipient_slot =
            RecipientSlot::derive(&recipient, &public_key_hex(&owner), circle).unwrap();
        assert_eq!(owner_slot, recipient_slot);
        assert_ne!(
            owner_slot,
            RecipientSlot::derive(&owner, &public_key_hex(&other), circle).unwrap()
        );
        assert_ne!(
            owner_slot,
            RecipientSlot::derive(
                &owner,
                &public_key_hex(&recipient),
                CircleId::from_bytes([2; 16]),
            )
            .unwrap()
        );
    }

    #[test]
    fn access_merkle_tree_is_canonical_and_rejects_substitution() {
        let first = ObjectHash::digest(b"leaf-a");
        let second = ObjectHash::digest(b"leaf-b");
        let third = ObjectHash::digest(b"leaf-c");
        let tree = AccessMerkleTree::from_leaf_hashes([third, first, second]).unwrap();
        let reordered = AccessMerkleTree::from_leaf_hashes([second, third, first]).unwrap();
        assert_eq!(tree.root(), reordered.root());

        let proof = tree.proof(second).unwrap();
        assert!(proof.verify(second, tree.root()));
        assert!(!proof.verify(first, tree.root()));
        assert!(!proof.verify(second, ObjectHash::digest(b"another-control-root")));

        let replacement = AccessMerkleTree::from_leaf_hashes([
            first,
            second,
            ObjectHash::digest(b"replacement-leaf"),
        ])
        .unwrap();
        let replacement_proof = replacement.proof(second).unwrap();
        assert!(!replacement_proof.verify(second, tree.root()));
    }

    #[test]
    fn access_paths_bind_every_dispatch_coordinate() {
        let circle = CircleId::from_bytes([3; 16]);
        let epoch = CircleEpochId::from_bytes([4; 16]);
        let leaf = AccessLeafId::from_bytes([5; 16]);
        let slot: RecipientSlot = "11".repeat(32).parse().unwrap();
        let owner = "22".repeat(32);
        let control_hash = ObjectHash::digest(b"control");

        assert_eq!(
            access_leaf_semantic_prefix(circle, &owner, epoch, slot, leaf).unwrap(),
            format!("circles/{circle}/access-leaves/{owner}/{epoch}/{slot}/{leaf}")
        );
        assert_eq!(
            access_envelope_semantic_prefix(circle, &owner, slot, control_hash).unwrap(),
            format!("circles/{circle}/access-envelopes/{owner}/{slot}/{control_hash}")
        );
    }
}
