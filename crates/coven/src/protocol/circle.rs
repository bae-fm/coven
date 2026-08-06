//! Circle identities, audience routing, and control coordinates.

use std::fmt;
use std::str::FromStr;

use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Sha256;

use super::membership::MembershipGrantId;
use super::store_commit::ObjectHash;
use coven_keys::encryption::EncryptionService;

pub use super::circle_control::*;
pub(crate) use super::circle_roster::*;

const CIRCLE_ID_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const CIRCLE_ID_LENGTH: usize = 26;
const ROW_ROUTING_KEY_DOMAIN: &[u8] = b"coven.row-routing.v1";
const ROW_ROUTING_ID_DOMAIN: &[u8] = b"coven.row-routing-id.v1\0";
const CIRCLE_ID_FOUNDER_DOMAIN: &str = "coven.circle-id-founder.v1";
const CIRCLE_EPOCH_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-epoch-id-generation.v1\0";
const ACCESS_LEAF_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-access-leaf-id-generation.v1\0";

/// A self-certifying 128-bit circle identity encoded as canonical lowercase base32.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CircleId([u8; 16]);

impl CircleId {
    pub(crate) fn founder(
        store_root_hash: ObjectHash,
        author_pubkey: &str,
        owner_grant: &MembershipGrantId,
    ) -> Self {
        #[derive(Serialize)]
        struct Founder<'a> {
            domain: &'static str,
            store_root_hash: ObjectHash,
            author_pubkey: &'a str,
            owner_grant: &'a MembershipGrantId,
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

/// One Circle as the local application sees it. A Circle with a single resolved
/// control is `Active`; a Circle whose control history forked into concurrent
/// valid successors is `Conflicted` and carries no name, role, or key until an
/// Owner resolves it. A conflicted Circle refuses authoring and package
/// publication, so it has no single resolved roster or metadata to report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CircleInfo {
    Active {
        id: CircleId,
        name: String,
        role: CircleRole,
        /// The resolved roster names a Store identity that is no longer an
        /// active Store member. Publishing new Circle content is blocked until
        /// an Owner closes the epoch and activates a successor roster without
        /// that identity.
        rotation_required: bool,
    },
    Conflicted {
        id: CircleId,
        /// Every retained concurrent control successor, in canonical order. The
        /// Owner resolves the conflict by naming this complete set and a chosen
        /// successor state.
        branches: Vec<CircleControlCoord>,
    },
    /// The Circle's control history terminated in an Owner-signed deletion. Its
    /// rows and access are gone locally; only the authority spine remains.
    Deleted { id: CircleId },
}

impl CircleInfo {
    pub fn id(&self) -> CircleId {
        match self {
            Self::Active { id, .. } | Self::Conflicted { id, .. } | Self::Deleted { id } => *id,
        }
    }

    /// The Circle's display name, or `None` while its control is conflicted and
    /// has no single resolved metadata.
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Active { name, .. } => Some(name),
            Self::Conflicted { .. } | Self::Deleted { .. } => None,
        }
    }

    /// Whether publishing new content is blocked because the resolved roster
    /// names a removed Store member. Always `false` for a conflicted Circle,
    /// which blocks all authoring until it is resolved.
    pub fn rotation_required(&self) -> bool {
        matches!(
            self,
            Self::Active {
                rotation_required: true,
                ..
            }
        )
    }
}

/// The public derived state of one Circle. Mapped once from the internal current
/// state; `Circles::list` reports it per Circle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CircleState {
    /// A single resolved active-epoch control whose roster names only current
    /// Store members.
    Active,
    /// The local identity holds no active access — never granted, or revoked by a
    /// removal it has not re-joined past.
    Inactive,
    /// An epoch close is in flight; new-content authoring under the old epoch is
    /// frozen until the successor activates.
    Closing,
    /// The resolved roster names Store identities that are no longer active Store
    /// members. New Circle content is refused until an Owner closes the epoch and
    /// activates a successor roster without them.
    RotationRequired { removed_members: Vec<String> },
    /// The control history forked into concurrent valid successors awaiting Owner
    /// resolution. Carries the complete retained branch set.
    ControlConflict { branches: Vec<CircleControlCoord> },
    /// The control history terminated in an Owner-signed deletion.
    Deleted,
}

/// One Circle as `Circles::list` reports it: its id, display name, the local
/// identity's role when it holds active access, and the derived state. The name
/// is absent for a Circle with no resolved metadata (inactive, conflicted, or
/// deleted); the role is present only when the local identity holds active roster
/// membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Circle {
    pub id: CircleId,
    pub name: Option<String>,
    pub role: Option<CircleRole>,
    pub state: CircleState,
}

/// The settlement of one participant device's create-once epoch-close response
/// slot: it published its own applied frontier, an Owner excluded it, or the slot
/// is still empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircleCloseSettlement {
    Responded,
    Excluded,
    Pending,
}

/// One participant in an in-flight epoch close and its slot settlement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircleCloseParticipant {
    pub device_id: crate::protocol::store_commit::StoreDeviceId,
    pub settlement: CircleCloseSettlement,
}

/// The read-only status of a Circle's in-flight epoch close: which participant
/// slots hold responses, exclusions, or nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircleCloseStatus {
    pub circle_id: CircleId,
    pub close_id: CircleEpochCloseId,
    pub participants: Vec<CircleCloseParticipant>,
}

/// Why publishing new content into a Circle is refused. Carried as the durable
/// typed reason on a blocked host write and on a refused Circle lifecycle
/// operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CirclePublicationBlocked {
    RotationRequired {
        circle_id: CircleId,
        removed_members: Vec<String>,
    },
}

impl std::fmt::Display for CirclePublicationBlocked {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RotationRequired {
                circle_id,
                removed_members,
            } => write!(
                formatter,
                "Circle {circle_id} requires rotation: its roster names removed Store members {removed_members:?}"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircleMemberInfo {
    pub pubkey: String,
    pub role: CircleRole,
    pub is_self: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CircleOperationId(crate::WriteId);

impl CircleOperationId {
    pub(crate) fn from_write_id(write_id: crate::WriteId) -> Self {
        Self(write_id)
    }

    /// A well-formed operation id that names no real operation, for API dispatch
    /// tests that only need a value to send through the command channel.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn placeholder(seed: &str) -> Self {
        Self::from_write_id(crate::WriteId::from_generated(seed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn finalization_write_id(&self) -> crate::WriteId {
        crate::WriteId::from_generated(
            crate::protocol::store_commit::ObjectHash::digest(
                &[
                    b"coven.circle-epoch-close-finalization-write.v1\0".as_slice(),
                    self.as_str().as_bytes(),
                ]
                .concat(),
            )
            .to_string(),
        )
    }

    pub(crate) fn cancellation_write_id(&self) -> crate::WriteId {
        crate::WriteId::from_generated(
            crate::protocol::store_commit::ObjectHash::digest(
                &[
                    b"coven.circle-epoch-close-cancellation-write.v1\0".as_slice(),
                    self.as_str().as_bytes(),
                ]
                .concat(),
            )
            .to_string(),
        )
    }
}

impl fmt::Display for CircleOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The stable identity of one Circle epoch close, derived from the durable
/// operation that opened it. It names the close a `Circles::close_status` inspects
/// and the reserved response and outcome slots that settle it; a close's identity
/// is fixed for its lifetime and survives cancellation and retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CircleEpochCloseId(ObjectHash);

impl CircleEpochCloseId {
    pub(crate) fn from_operation_id(operation_id: &CircleOperationId) -> Self {
        Self(ObjectHash::digest(
            &[
                b"coven.circle-epoch-close-id.v1\0".as_slice(),
                operation_id.as_str().as_bytes(),
            ]
            .concat(),
        ))
    }
}

impl fmt::Display for CircleEpochCloseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircleOperationKind {
    Create,
    Rename,
    AddMember,
    RemoveMember,
    ResolveControl,
    Delete,
}

/// Why a durable Circle operation cannot currently publish. One variant per
/// production block site; each future block site adds its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleOperationBlock {
    /// The author's exact grant no longer holds current Store write authority.
    AuthorityLost {
        grant_id: crate::protocol::membership::MembershipGrantId,
    },
    /// Another writer took this device's stream position between the operation's
    /// composition and its publication. The candidate commit is bound to that
    /// create-once head slot, so it can never activate there and no re-publish
    /// can succeed: the operation is over, and its initiator discards it and
    /// re-issues.
    PositionLost {
        winner_commit: crate::protocol::store_commit::ObjectHash,
    },
}

impl std::fmt::Display for CircleOperationBlock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AuthorityLost { grant_id } => write!(
                formatter,
                "author grant {grant_id} no longer has current Store write authority"
            ),
            Self::PositionLost { winner_commit } => write!(
                formatter,
                "Store commit {winner_commit} took this device's stream position \
                 before the operation published"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleOperationState {
    Pending,
    WaitingForCloseResponses,
    Finalizing,
    Blocked {
        block: CircleOperationBlock,
    },
    /// A verified nonactivation proof was accepted; the candidate's exclusive
    /// objects are being exact-deleted and the durable row cleared. Restart
    /// resumes the same cleanup from this state.
    Discarding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircleOperationInfo {
    pub operation_id: CircleOperationId,
    pub circle_id: CircleId,
    pub kind: CircleOperationKind,
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

macro_rules! generated_hex_id {
    ($name:ident, $domain:ident) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name([u8; 16]);

        impl $name {
            pub(crate) fn generate(ids: &dyn coven_foundation::id_provider::IdProvider) -> Self {
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

pub(crate) fn generated_id_digest(
    ids: &dyn coven_foundation::id_provider::IdProvider,
    domain: &[u8],
) -> ObjectHash {
    let id = ids.new_id();
    let mut material = Vec::with_capacity(domain.len() + id.len());
    material.extend_from_slice(domain);
    material.extend_from_slice(id.as_bytes());
    ObjectHash::digest(&material)
}

fn generated_id_bytes(
    ids: &dyn coven_foundation::id_provider::IdProvider,
    domain: &[u8],
) -> [u8; 16] {
    generated_id_digest(ids, domain).as_bytes()[..16]
        .try_into()
        .expect("SHA-256 digest prefix has fixed length")
}

/// HMAC identity of one scoped row. It is stable across audience moves and
/// Store-key rotations because it derives from the unique generation-1 key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RowRoutingId([u8; 32]);

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
pub(crate) struct RowRoutingIdError(String);

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
#[path = "circle_tests.rs"]
mod tests;
