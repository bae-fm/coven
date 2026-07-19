//! Circle identities, audience routing, and control coordinates.

use std::fmt;
use std::str::FromStr;

use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Sha256;

use super::membership::MembershipGrantId;
use super::store_commit::ObjectHash;
use crate::encryption::EncryptionService;

pub use super::circle_control::*;
pub use super::circle_roster::*;

const CIRCLE_ID_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const CIRCLE_ID_LENGTH: usize = 26;
const ROW_ROUTING_KEY_DOMAIN: &[u8] = b"coven.row-routing.v1";
const ROW_ROUTING_ID_DOMAIN: &[u8] = b"coven.row-routing-id.v1\0";
const CIRCLE_ID_FOUNDER_DOMAIN: &str = "coven.circle-id-founder.v1";
const CIRCLE_EPOCH_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-epoch-id-generation.v1\0";
const ACCESS_LEAF_ID_GENERATION_DOMAIN: &[u8] = b"coven.circle-access-leaf-id-generation.v1\0";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircleInfo {
    pub id: CircleId,
    pub name: String,
    pub role: CircleRole,
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

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for CircleOperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircleOperationKind {
    Create,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleOperationState {
    Pending,
    Blocked { reason: String },
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
            pub(crate) fn generate(ids: &dyn crate::id_provider::IdProvider) -> Self {
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
    ids: &dyn crate::id_provider::IdProvider,
    domain: &[u8],
) -> ObjectHash {
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
    use std::collections::BTreeMap;

    use super::super::circle_control::{merkle_root_and_proofs, verify_merkle_proof};
    use super::*;
    use crate::keys::{self, UserKeypair};

    fn candidate_family(label: &str) -> super::super::store_commit::CandidateFamilyId {
        super::super::store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(
            label.as_bytes(),
        ))
    }

    fn exact_object(label: &str, bytes: &[u8]) -> super::super::storage::ExactObjectRef {
        super::super::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(format!("store-v1/test/{label}.json"))
                .unwrap(),
            bytes.len() as u64,
            ObjectHash::digest(bytes),
        )
    }

    fn exact_logical_object(
        logical_key: String,
        bytes: &[u8],
    ) -> super::super::storage::ExactObjectRef {
        super::super::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(logical_key).unwrap(),
            bytes.len() as u64,
            ObjectHash::digest(bytes),
        )
    }

    fn test_founder_entry(
        label: &str,
        owner: &UserKeypair,
        membership: super::super::store_commit::GrantStreamAnchor,
    ) -> super::super::membership::MembershipEntry {
        super::super::membership::founder_entry(
            label,
            owner,
            crate::sync::test_helpers::test_membership_grant_id(label),
            "founder",
            membership,
            crate::sync::test_helpers::test_founder_provider_admin(label),
        )
    }

    fn merge_membership_ref(
        owner: &UserKeypair,
        members: &[(String, super::super::membership::MemberRole)],
        label: &str,
    ) -> (
        StoreMembershipStateRef,
        super::super::membership::MembershipGrantCreationAuthority,
    ) {
        let founder = test_founder_entry(
            label,
            owner,
            super::super::store_commit::GrantStreamAnchor::StoreMembership {
                first_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                    "store-v1/test/{label}/membership/1.json"
                ))
                .unwrap(),
            },
        );
        let founder_coord = founder.coord();
        let mut chain = super::super::membership::MembershipChain::from_entries(vec![founder])
            .expect("found merge-concurrent membership");
        for (index, (pubkey, role)) in members.iter().enumerate() {
            if pubkey == &keys::public_key_hex(owner) {
                continue;
            }
            let entry = chain
                .signed_set_member_in_stream(
                    owner,
                    founder_coord.stream_id,
                    pubkey.clone(),
                    None,
                    role.clone(),
                    format!("member-{index}"),
                )
                .expect("sign merge-concurrent member");
            chain
                .add_entry(entry)
                .expect("apply merge-concurrent member");
        }
        let resolved = match chain.status() {
            super::super::membership::MembershipStatus::Resolved(resolved) => resolved,
            super::super::membership::MembershipStatus::Conflict(_) => {
                panic!("membership fixture must resolve")
            }
        };
        let tip = chain.entries().last().expect("membership tip").coord();
        let head = super::super::membership::MembershipHeadRef {
            coord: tip,
            head_hash: ObjectHash::digest(format!("{label} head").as_bytes()),
            object: exact_object(&format!("{label}/membership-head"), b"membership head"),
        };
        (
            StoreMembershipStateRef::merge_concurrent(
                vec![head],
                Vec::new(),
                Vec::new(),
                resolved.state_hash,
            )
            .expect("valid merge-concurrent membership reference"),
            super::super::membership::MembershipGrantCreationAuthority::Entry(founder_coord),
        )
    }

    fn serial_membership_ref(
        owner: &UserKeypair,
        members: &[(String, super::super::membership::MemberRole)],
        label: &str,
    ) -> StoreMembershipStateRef {
        let root_bytes = format!("{label} root").into_bytes();
        let root = super::super::store_commit::StoreRootRef {
            store_root_id: ObjectHash::digest(format!("{label} identity").as_bytes()),
            store_root_hash: ObjectHash::digest(&root_bytes),
            object: exact_object(&format!("{label}/root"), &root_bytes),
        };
        let origin = super::super::store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: super::super::store_commit::StoreCreationId::from_nonce(label),
        };
        let founder_registration = super::super::store_commit::StoreDeviceRegistrationRef {
            device_id: super::super::store_commit::StoreDeviceId::derive(&root, &origin),
            registration_hash: ObjectHash::digest(format!("{label} registration").as_bytes()),
            object: exact_object(
                &format!("{label}/registration"),
                format!("{label} registration").as_bytes(),
            ),
        };
        let founder = test_founder_entry(
            label,
            owner,
            super::super::store_commit::GrantStreamAnchor::StoreMembership {
                first_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                    "store-v1/test/{label}/membership/1.json"
                ))
                .unwrap(),
            },
        );
        let mut membership = super::super::membership::SerialMembershipState::from_founder(
            root.store_root_id,
            &founder,
        )
        .expect("found Serial membership");
        for (index, (pubkey, role)) in members.iter().enumerate() {
            if pubkey == &keys::public_key_hex(owner) {
                continue;
            }
            let entry = membership
                .signed_set_member(
                    owner,
                    pubkey.clone(),
                    None,
                    role.clone(),
                    format!("member-{index}"),
                )
                .expect("sign Serial member");
            membership = membership.apply(&entry).expect("apply Serial member");
        }
        let authorization =
            super::super::membership::SerialAuthorizationState::from_test_membership(
                &founder, membership,
            )
            .expect("test Serial authorization");
        StoreMembershipStateRef::serial(
            super::super::store_commit::SerialStorePosition::Genesis {
                root,
                founder_registration,
            },
            Vec::new(),
            &authorization,
        )
        .expect("valid Serial membership reference")
    }

    struct MergeDeviceAuthority {
        registration: super::super::store_commit::StoreDeviceRegistration,
        reference: super::super::store_commit::StoreDeviceRegistrationRef,
        device_signer: UserKeypair,
        stream_id: super::super::membership::AuthorStreamId,
    }

    fn merge_device_authority(
        identity: &UserKeypair,
        store_root_hash: ObjectHash,
        label: &str,
    ) -> MergeDeviceAuthority {
        let root = super::super::store_commit::StoreRootRef {
            store_root_id: ObjectHash::digest(format!("{label} identity").as_bytes()),
            store_root_hash,
            object: exact_object(&format!("{label}/root"), label.as_bytes()),
        };
        let slot = |stream: &str| {
            crate::storage::cloud::ObjectSlot::logical(format!(
                "store-v1/test/{label}/{stream}/1.json"
            ))
            .unwrap()
        };
        let registration = super::super::store_commit::StoreDeviceRegistration::signed(
            root.clone(),
            super::super::store_commit::StoreDeviceRegistrationOrigin::Founder {
                creation_id: super::super::store_commit::StoreCreationId::from_nonce(label),
            },
            super::super::storage::ProviderDeviceBinding {
                principal: super::super::storage::ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(label.as_bytes()),
                },
            },
            super::super::store_commit::StoreCommitAnchor::MergeConcurrent {
                announcements: super::super::store_commit::DeviceStreamAnchor::StoreAnnouncements {
                    first_slot: slot("announcements"),
                },
            },
            super::super::store_commit::DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot("acknowledgements"),
            },
            super::super::store_commit::DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot("snapshots"),
            },
            identity,
        )
        .expect("sign test device registration");
        let bytes = registration.to_bytes();
        let reference = super::super::store_commit::StoreDeviceRegistrationRef::from_registration(
            &registration,
            exact_object(&format!("{label}/registration"), &bytes),
        );
        let device_signer = registration
            .device_signer(identity)
            .expect("derive registered device signer");
        let stream_id =
            super::super::membership::AuthorStreamId::store_announcements(&root, &reference);
        MergeDeviceAuthority {
            registration,
            reference,
            device_signer,
            stream_id,
        }
    }

    fn test_activation_objects(label: &str) -> super::super::store_commit::CircleActivationObjects {
        let bytes = label.as_bytes();
        super::super::store_commit::CircleActivationObjects {
            control: super::super::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(format!(
                    "store-v1/test-circle-controls/{label}.json"
                ))
                .expect("test control slot is valid"),
                bytes.len() as u64,
                ObjectHash::digest(bytes),
            ),
            control_head: None,
            roster_entries: BTreeMap::new(),
            roster_heads: BTreeMap::new(),
            roster_resolutions: BTreeMap::new(),
            metadata_entries: BTreeMap::new(),
            metadata_heads: BTreeMap::new(),
            access: Vec::new(),
        }
    }

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

        let (merge_membership, merge_authority) =
            merge_membership_ref(&owner, &members, "founder-circle-merge");
        for (membership, membership_authority) in [
            (merge_membership, Some(merge_authority)),
            (
                serial_membership_ref(&owner, &members, "founder-circle-serial"),
                None,
            ),
        ] {
            let ids = crate::id_provider::SequentialIdProvider::new("founder-circle");
            let candidate_family = candidate_family("founder-circle");
            let creation = CircleCreation::founder(
                ObjectHash::digest(b"store-root"),
                candidate_family,
                "device-a",
                "Household",
                "0000000001000-0000-device-a",
                membership,
                membership_authority,
                members.clone(),
                &ids,
                &owner,
            )
            .expect("construct founder circle");

            assert!(creation.control.verify());
            assert!(creation.metadata.verify());
            assert!(creation.resolved_roster().verify());
            assert_eq!(creation.access.len(), 2);
            for access in &creation.access {
                assert!(access.leaf.verify(&creation.control, candidate_family));
                assert!(access.envelope.verify(&creation.control, candidate_family));
                assert!(access.leaf.verify_envelope(
                    &creation.control,
                    &access.envelope,
                    candidate_family
                ));
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
                creation.control.value.value,
                CircleControlValue::MergeConcurrent { .. }
            ) {
                let mut seized = creation.control.value.clone();
                seized.circle_id = CircleId::from_bytes([0x5a; 16]);
                seized.signature = keys::sign_hex(&owner, &seized.canonical_bytes()).1;
                assert!(
                    !seized.verify(),
                    "a founder control must not choose an arbitrary Circle ID"
                );

                let mut discontinuous = creation.control.value.clone();
                let CircleControlValue::MergeConcurrent { order, .. } = &mut discontinuous.value
                else {
                    unreachable!()
                };
                order.seq = 2;
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
        let (membership, authority) = merge_membership_ref(&owner, &members, "access-verification");
        let ids = crate::id_provider::SequentialIdProvider::new("access-verification");
        let candidate_family = candidate_family("access-verification");
        let creation = CircleCreation::founder(
            ObjectHash::digest(b"store-root"),
            candidate_family,
            "device-a",
            "Household",
            "0000000001000-0000-device-a",
            membership,
            Some(authority),
            members.clone(),
            &ids,
            &owner,
        )
        .expect("construct founder circle");

        let mut wrong_store = creation.access[0].envelope.clone();
        wrong_store.store_root_hash = ObjectHash::digest(b"other-store");

        wrong_store.signature = keys::sign_hex(&owner, &wrong_store.canonical_bytes()).1;
        assert!(!wrong_store.verify(&creation.control, candidate_family));

        let mut wrong_family_envelope = creation.access[0].envelope.clone();
        wrong_family_envelope.candidate_family =
            super::super::store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(
                b"other access family",
            ));
        wrong_family_envelope.signature =
            keys::sign_hex(&owner, &wrong_family_envelope.canonical_bytes()).1;
        assert!(!wrong_family_envelope.verify(&creation.control, candidate_family));

        let mut wrong_family_leaf = creation.access[0].leaf.clone();
        wrong_family_leaf.value.candidate_family =
            super::super::store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(
                b"other leaf family",
            ));
        wrong_family_leaf.value.signature =
            keys::sign_hex(&owner, &wrong_family_leaf.value.canonical_bytes()).1;
        assert!(!wrong_family_leaf.verify(&creation.control, candidate_family));

        let mut non_owner = creation.access[0].envelope.clone();
        non_owner.owner_pubkey = peer_pubkey;
        non_owner.signature = keys::sign_hex(&peer, &non_owner.canonical_bytes()).1;
        assert!(!non_owner.verify(&creation.control, candidate_family));

        let mut substituted_proof = creation.access[0].envelope.clone();
        substituted_proof.proof = creation.access[1].envelope.proof.clone();
        substituted_proof.signature =
            keys::sign_hex(&owner, &substituted_proof.canonical_bytes()).1;
        assert!(!substituted_proof.verify(&creation.control, candidate_family));

        let mut substituted_leaf_id = creation.access[0].envelope.clone();
        substituted_leaf_id.leaf_id = creation.access[1].leaf.value.leaf_id;
        substituted_leaf_id.signature =
            keys::sign_hex(&owner, &substituted_leaf_id.canonical_bytes()).1;
        assert!(substituted_leaf_id.verify(&creation.control, candidate_family));
        assert!(!creation.access[0].leaf.verify_envelope(
            &creation.control,
            &substituted_leaf_id,
            candidate_family,
        ));

        let mut wrong_membership_leaf = creation.access[0].leaf.value.clone();
        wrong_membership_leaf.store_membership =
            serial_membership_ref(&owner, &members, "wrong-membership-leaf");
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
        assert!(!wrong_membership_leaf.verify(&creation.control, candidate_family));

        let mut wrong_keyring_leaf = creation
            .access
            .iter()
            .find(|access| {
                matches!(
                    &access.leaf.value.disposition,
                    CircleAccessDisposition::Active { .. }
                )
            })
            .expect("founder access")
            .leaf
            .value
            .clone();
        let CircleAccessDisposition::Active { keyring, .. } = &mut wrong_keyring_leaf.disposition
        else {
            panic!("founder access must be active")
        };
        *keyring = crate::encryption::MasterKeyring::generate().to_serialized();
        wrong_keyring_leaf.signature =
            keys::sign_hex(&owner, &wrong_keyring_leaf.canonical_bytes()).1;
        let bytes = keys::seal_box_encrypt(
            &serde_json::to_vec(&wrong_keyring_leaf).expect("serialize wrong-keyring leaf"),
            &recipient_key,
        );
        let wrong_keyring_leaf = PreparedAccessLeaf {
            leaf_hash: ObjectHash::digest(&bytes),
            bytes,
            value: wrong_keyring_leaf,
        };
        assert!(!wrong_keyring_leaf.verify(&creation.control, candidate_family));

        let mut wrong_policy_control =
            serde_json::to_value(&creation.control.value).expect("serialize Merge control");
        let value = wrong_policy_control["value"]
            .as_object_mut()
            .expect("control value is a tagged object");
        let merge = value
            .remove("merge_concurrent")
            .expect("Merge control has Merge value");
        value.insert("serial".to_string(), merge);
        assert!(serde_json::from_value::<CircleControl>(wrong_policy_control).is_err());
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
    fn recipient_slot_rejects_the_ed25519_identity_point() {
        let local = UserKeypair::generate();
        let mut identity = [0; keys::SIGN_PUBLICKEYBYTES];
        identity[0] = 1;
        let recipient = hex::encode(identity);

        assert_eq!(
            recipient_slot_with_peer(&local, &recipient, CircleId::from_bytes([9; 16])),
            Err(CircleCreateError::InvalidRecipient(recipient))
        );
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
        let (membership, membership_authority) =
            merge_membership_ref(&author, &members, "multi-owner-control");
        let device = merge_device_authority(&author, store_root_hash, "multi-owner-device");
        let ids = crate::id_provider::SequentialIdProvider::new("multi-owner-control");
        let operation_id = crate::WriteId::from_generated("multi-owner-control-commit".to_string());
        let order = super::super::store_commit::StoreCommitOrder::MergeConcurrent {
            seq: 1,
            predecessor: None,
            dependencies: BTreeMap::new(),
        };
        let candidate_family = super::super::store_commit::CandidateFamilyId::derive(
            store_root_hash,
            &device.reference,
            &operation_id,
            &order,
        );
        let creation = CircleCreation::founder(
            store_root_hash,
            candidate_family,
            &device.reference.device_id.to_string(),
            "Household",
            "0000000001000-0000-device-a",
            membership.clone(),
            Some(membership_authority.clone()),
            members,
            &ids,
            &author,
        )
        .expect("construct founder circle");
        let mut control = creation.control.value.clone();
        let CircleControlValue::MergeConcurrent { active_epoch, .. } = &mut control.value else {
            panic!("Merge creation must carry Merge control")
        };
        active_epoch.common.owners = vec![earlier_owner_pubkey, author_pubkey.clone()];
        active_epoch.common.owners.sort();
        assert_ne!(active_epoch.common.owners[0], control.author_pubkey);
        control.signature = keys::sign_hex(&author, &control.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control.coord(),
            bytes: serde_json::to_vec(&control).expect("serialize control"),
            value: control,
        };
        let reference = super::super::store_commit::CircleControlRef::MergeConcurrent {
            circle_id: creation.circle_id,
            control: control.coord.clone(),
            head_hash: ObjectHash::digest(b"multi-owner-control-head"),
            objects: test_activation_objects("multi-owner"),
        };
        let first_coord = super::super::store_commit::StoreCommitCoord::MergeConcurrent {
            stream_id: device.stream_id,
            sequence: 1,
        };
        let commit = super::super::store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            operation_id,
            first_coord.clone(),
            device.reference.clone(),
            &device.registration,
            order,
            membership.clone(),
            super::super::store_commit::StoreDeviceStateRef::MergeConcurrent {
                frontier: super::super::store_commit::CommitFrontier::MergeConcurrent(
                    BTreeMap::new(),
                ),
                recovery: Vec::new(),
                state_hash: ObjectHash::digest(b"multi-owner initial device state"),
            },
            Some(membership_authority.clone()),
            super::super::store_commit::StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempts: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_abandonments: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                circle_controls: vec![reference.clone()],
                store_package: None,
                circle_packages: &[],
            },
            &device.device_signer,
        )
        .expect("sign Store commit");
        let first_commit_path = format!(
            "{}.json",
            super::super::store_commit::commit_semantic_prefix(
                commit.candidate_family(),
                &device.stream_id.to_string(),
                1,
                commit.commit_hash(),
            )
        );
        let commit_ref = super::super::store_commit::StoreBatchCommitRef::from_commit(
            &commit,
            first_coord,
            exact_logical_object(first_commit_path, &commit.to_bytes()),
        )
        .expect("reference first Store commit");
        let own_access = creation
            .access
            .iter()
            .find(|access| access.leaf.value.recipient_pubkey == author_pubkey)
            .expect("author access");
        let verified = super::super::circle_ops::VerifiedCircleReference {
            reference,
            circle_id: creation.circle_id,
            control: control.clone(),
            local_access: Some(super::super::circle_ops::VerifiedCircleAccess {
                leaf: own_access.leaf.clone(),
                active: Some(super::super::circle_ops::VerifiedCircleActive {
                    roster: creation.resolved_roster(),
                    metadata: creation.metadata.clone(),
                }),
            }),
        };
        let db = super::super::test_helpers::open_test_db();
        let first_commit = commit.clone();
        let first_commit_ref = commit_ref.clone();
        db.call(move |conn| {
            crate::database::Database::record_verified_circle_activations_on(
                conn,
                &first_commit,
                &first_commit_ref,
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
        db.call(|conn| {
            conn.execute("DELETE FROM circle_metadata_cache", [])
                .map_err(crate::database::DbError::from)?;
            conn.execute("DELETE FROM circle_roster_cache", [])
                .map_err(crate::database::DbError::from)?;
            conn.execute("DELETE FROM circle_access_cache", [])
                .map(|_| ())
                .map_err(crate::database::DbError::from)
        })
        .await
        .expect("remove historical Circle projections");
        let circles = db
            .get_circles(&author_pubkey)
            .await
            .expect("list Circle from its derived current state");
        assert_eq!(
            circles,
            vec![CircleInfo {
                id: creation.circle_id,
                name: creation.metadata.name.clone(),
                role: CircleRole::Owner,
            }]
        );
        let (publication_encryption, publication_fingerprint) = db
            .circle_publication_context(creation.circle_id, control.coord.clone())
            .await
            .expect("load publication authority from derived current state");
        assert_eq!(
            publication_encryption.seal_key_fingerprint(),
            publication_fingerprint
        );
        assert_eq!(publication_fingerprint, control.value.key_fingerprint());

        let mut second_value = control.value.clone();
        let CircleControlValue::MergeConcurrent { active_epoch, .. } = &mut second_value.value
        else {
            panic!("Merge creation must carry Merge control")
        };
        active_epoch.common.access_root = ObjectHash::digest(b"different founder access root");
        second_value.signature = keys::sign_hex(&author, &second_value.canonical_bytes()).1;
        let second_control = PreparedCircleControl {
            coord: second_value.coord(),
            bytes: serde_json::to_vec(&second_value).expect("serialize second founder control"),
            value: second_value,
        };
        let second_reference = super::super::store_commit::CircleControlRef::MergeConcurrent {
            circle_id: creation.circle_id,
            control: second_control.coord.clone(),
            head_hash: ObjectHash::digest(b"second-founder-control-head"),
            objects: test_activation_objects("second-founder"),
        };
        let second_coord = super::super::store_commit::StoreCommitCoord::MergeConcurrent {
            stream_id: device.stream_id,
            sequence: 2,
        };
        let second_commit = super::super::store_commit::StoreBatchCommit::signed_operations(
            store_root_hash,
            crate::WriteId::from_generated("second-founder-control-commit".to_string()),
            second_coord.clone(),
            device.reference,
            &device.registration,
            super::super::store_commit::StoreCommitOrder::MergeConcurrent {
                seq: 2,
                predecessor: Some(commit_ref.clone()),
                dependencies: BTreeMap::new(),
            },
            membership,
            super::super::store_commit::StoreDeviceStateRef::MergeConcurrent {
                frontier: super::super::store_commit::CommitFrontier::MergeConcurrent(
                    BTreeMap::from([(device.stream_id, commit_ref.clone())]),
                ),
                recovery: Vec::new(),
                state_hash: ObjectHash::digest(b"multi-owner second device state"),
            },
            control.value.membership_authority().cloned(),
            super::super::store_commit::StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempts: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_abandonments: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                circle_controls: vec![second_reference.clone()],
                store_package: None,
                circle_packages: &[],
            },
            &device.device_signer,
        )
        .expect("sign second founder Store commit");
        let second_commit_path = format!(
            "{}.json",
            super::super::store_commit::commit_semantic_prefix(
                second_commit.candidate_family(),
                &device.stream_id.to_string(),
                2,
                second_commit.commit_hash(),
            )
        );
        let second_commit_ref = super::super::store_commit::StoreBatchCommitRef::from_commit(
            &second_commit,
            second_coord,
            exact_logical_object(second_commit_path, &second_commit.to_bytes()),
        )
        .expect("reference second Store commit");
        let error = db
            .call(move |conn| {
                crate::database::Database::record_verified_circle_activations_on(
                    conn,
                    &second_commit,
                    &second_commit_ref,
                    &[super::super::circle_ops::VerifiedCircleReference {
                        reference: second_reference,
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
