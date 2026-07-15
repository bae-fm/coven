//! Circle identities, audience routing, and control coordinates.

use std::fmt;
use std::str::FromStr;

#[cfg(test)]
use hkdf::Hkdf;
#[cfg(test)]
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(test)]
use sha2::Sha256;

use super::membership::OwnerGrantId;
use super::store_commit::ObjectHash;
#[cfg(test)]
use crate::encryption::EncryptionService;

const CIRCLE_ID_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const CIRCLE_ID_LENGTH: usize = 26;
#[cfg(test)]
const ROW_ROUTING_KEY_DOMAIN: &[u8] = b"coven.row-routing.v1";
#[cfg(test)]
const ROW_ROUTING_ID_DOMAIN: &[u8] = b"coven.row-routing-id.v1\0";

/// A random 128-bit circle identity encoded as canonical lowercase base32.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CircleId([u8; 16]);

impl CircleId {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 16];
        rand::rng().fill_bytes(&mut bytes);
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
#[cfg(test)]
struct RowRoutingKey([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowRoutingKeyError {
    #[error("Store keyring has no generation-1 key")]
    MissingGenerationOne,
    #[error("Store keyring has more than one generation-1 key")]
    AmbiguousGenerationOne,
}

#[cfg(test)]
fn derive_row_routing_key(
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

#[cfg(test)]
fn row_routing_id(key: &RowRoutingKey, table: &str, row_id: &str) -> RowRoutingId {
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
}
