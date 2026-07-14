//! Signed, hash-addressed Store commit protocol objects.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use super::membership::{
    derive_founder_grant_id, verify_membership_entry, MembershipChange, MembershipCoord,
    MembershipEntry, OwnerGrantId,
};
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::CopyId;

pub const STORE_PROTOCOL_VERSION: u32 = 1;

const GENESIS_DOMAIN: &[u8] = b"coven.protocol-genesis.v1\0";
const COMMIT_DOMAIN: &[u8] = b"coven.store-batch-commit.v1\0";
const HEAD_DOMAIN: &[u8] = b"coven.store-device-head.v1\0";
const REGISTRATION_DOMAIN: &[u8] = b"coven.store-device-registration.v1\0";
const ACK_DOMAIN: &[u8] = b"coven.store-ack.v1\0";
const SNAPSHOT_DOMAIN: &[u8] = b"coven.snapshot-meta.v1\0";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectHash([u8; 32]);

impl ObjectHash {
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ObjectHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for ObjectHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for ObjectHash {
    type Err = StoreProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(StoreProtocolError::InvalidObjectHash(value.to_string()));
        }
        let decoded = hex::decode(value)
            .map_err(|_| StoreProtocolError::InvalidObjectHash(value.to_string()))?;
        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|_| StoreProtocolError::InvalidObjectHash(value.to_string()))?;
        Ok(Self(bytes))
    }
}

impl Serialize for ObjectHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ObjectHash {
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
#[serde(deny_unknown_fields)]
pub struct CommitPosition {
    pub seq: u64,
    pub commit_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePackageRef {
    pub object_key: String,
    pub content_hash: ObjectHash,
    pub schema_version: u32,
    pub changeset_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommit {
    pub version: u32,
    pub genesis_hash: ObjectHash,
    pub device_id: String,
    pub author_pubkey: String,
    pub seq: u64,
    pub previous_commit_hash: Option<ObjectHash>,
    pub dependencies: BTreeMap<String, CommitPosition>,
    pub membership_grant: Option<MembershipCoord>,
    pub package: StorePackageRef,
    pub signature: String,
}

#[derive(Serialize)]
struct CommitSignedFields<'a> {
    version: u32,
    genesis_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    seq: u64,
    previous_commit_hash: Option<ObjectHash>,
    dependencies: &'a BTreeMap<String, CommitPosition>,
    membership_grant: Option<&'a MembershipCoord>,
    package: &'a StorePackageRef,
}

impl StoreBatchCommit {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        genesis_hash: ObjectHash,
        device_id: String,
        seq: u64,
        previous_commit_hash: Option<ObjectHash>,
        dependencies: BTreeMap<String, CommitPosition>,
        membership_grant: Option<MembershipCoord>,
        schema_version: u32,
        package_bytes: &[u8],
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_device_id(&device_id)?;
        if seq == 0 {
            return Err(StoreProtocolError::InvalidSequence(seq));
        }
        match (seq, previous_commit_hash) {
            (1, None) => {}
            (1, Some(_)) => return Err(StoreProtocolError::UnexpectedPredecessor),
            (_, Some(_)) => {}
            (_, None) => return Err(StoreProtocolError::MissingPredecessor),
        }
        if dependencies.contains_key(&device_id) {
            return Err(StoreProtocolError::OwnDependency(device_id));
        }
        validate_frontier(&dependencies)?;
        if let Some(grant) = membership_grant.as_ref() {
            validate_membership_coord(grant)?;
        }
        let changeset_size =
            u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
        let author_pubkey = keys::public_key_hex(signer);
        let content_hash = ObjectHash::digest(package_bytes);
        let package = StorePackageRef {
            object_key: package_semantic_prefix(&device_id, seq, content_hash),
            content_hash,
            schema_version,
            changeset_size,
        };
        let mut commit = Self {
            version: STORE_PROTOCOL_VERSION,
            genesis_hash,
            device_id,
            author_pubkey,
            seq,
            previous_commit_hash,
            dependencies,
            membership_grant,
            package,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &commit.canonical_signed_bytes());
        commit.signature = signature;
        Ok(commit)
    }

    pub fn canonical_signed_bytes(&self) -> Vec<u8> {
        let fields = CommitSignedFields {
            version: self.version,
            genesis_hash: self.genesis_hash,
            device_id: &self.device_id,
            author_pubkey: &self.author_pubkey,
            seq: self.seq,
            previous_commit_hash: self.previous_commit_hash,
            dependencies: &self.dependencies,
            membership_grant: self.membership_grant.as_ref(),
            package: &self.package,
        };
        domain_json(COMMIT_DOMAIN, &fields)
    }

    pub fn commit_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn position(&self) -> CommitPosition {
        CommitPosition {
            seq: self.seq,
            commit_hash: self.commit_hash(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreBatchCommit serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_genesis: ObjectHash,
        expected_device: &str,
        expected_seq: u64,
    ) -> Result<Self, StoreProtocolError> {
        let commit: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        commit.verify_at(expected_genesis, expected_device, expected_seq)?;
        Ok(commit)
    }

    pub fn verify_at(
        &self,
        expected_genesis: ObjectHash,
        expected_device: &str,
        expected_seq: u64,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        if self.genesis_hash != expected_genesis {
            return Err(StoreProtocolError::GenesisMismatch {
                expected: expected_genesis,
                actual: self.genesis_hash,
            });
        }
        if self.device_id != expected_device || self.seq != expected_seq {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: commit_slot_prefix(expected_device, expected_seq),
                actual: commit_slot_prefix(&self.device_id, self.seq),
            });
        }
        validate_device_id(&self.device_id)?;
        if self.package.object_key
            != package_semantic_prefix(&self.device_id, self.seq, self.package.content_hash)
        {
            return Err(StoreProtocolError::RelocatedPackage {
                expected: package_semantic_prefix(
                    &self.device_id,
                    self.seq,
                    self.package.content_hash,
                ),
                actual: self.package.object_key.clone(),
            });
        }
        match (self.seq, self.previous_commit_hash) {
            (0, _) => return Err(StoreProtocolError::InvalidSequence(0)),
            (1, None) => {}
            (1, Some(_)) => return Err(StoreProtocolError::UnexpectedPredecessor),
            (_, None) => return Err(StoreProtocolError::MissingPredecessor),
            (_, Some(_)) => {}
        }
        if self.dependencies.contains_key(&self.device_id) {
            return Err(StoreProtocolError::OwnDependency(self.device_id.clone()));
        }
        validate_frontier(&self.dependencies)?;
        if let Some(grant) = self.membership_grant.as_ref() {
            validate_membership_coord(grant)?;
        }
        if !keys::verify_signature_hex(
            &self.author_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    pub fn verify_package(&self, package_bytes: &[u8]) -> Result<(), StoreProtocolError> {
        let length =
            u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
        if length != self.package.changeset_size {
            return Err(StoreProtocolError::PackageLengthMismatch {
                expected: self.package.changeset_size,
                actual: length,
            });
        }
        let actual = ObjectHash::digest(package_bytes);
        if actual != self.package.content_hash {
            return Err(StoreProtocolError::PackageHashMismatch {
                expected: self.package.content_hash,
                actual,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceHead {
    pub version: u32,
    pub genesis_hash: ObjectHash,
    pub device_id: String,
    pub author_pubkey: String,
    pub position: Option<CommitPosition>,
    pub published_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct HeadSignedFields<'a> {
    version: u32,
    genesis_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    position: Option<&'a CommitPosition>,
    published_at: &'a str,
}

impl StoreDeviceHead {
    pub fn signed(
        genesis_hash: ObjectHash,
        device_id: String,
        position: Option<CommitPosition>,
        published_at: String,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_device_id(&device_id)?;
        let author_pubkey = keys::public_key_hex(signer);
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            genesis_hash,
            device_id,
            author_pubkey,
            position,
            published_at,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &head.canonical_signed_bytes());
        head.signature = signature;
        Ok(head)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            HEAD_DOMAIN,
            &HeadSignedFields {
                version: self.version,
                genesis_hash: self.genesis_hash,
                device_id: &self.device_id,
                author_pubkey: &self.author_pubkey,
                position: self.position.as_ref(),
                published_at: &self.published_at,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreDeviceHead serialization cannot fail")
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn slot_sequence(&self) -> u64 {
        self.position.as_ref().map_or(0, |position| position.seq)
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_genesis: ObjectHash,
        expected_device: &str,
        expected_seq: u64,
    ) -> Result<Self, StoreProtocolError> {
        let head: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(head.version)?;
        if head.genesis_hash != expected_genesis {
            return Err(StoreProtocolError::GenesisMismatch {
                expected: expected_genesis,
                actual: head.genesis_hash,
            });
        }
        if head.device_id != expected_device || head.slot_sequence() != expected_seq {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: head_slot_prefix(expected_device, expected_seq),
                actual: head_slot_prefix(&head.device_id, head.slot_sequence()),
            });
        }
        validate_device_id(&head.device_id)?;
        if head
            .position
            .as_ref()
            .is_some_and(|position| position.seq == 0)
        {
            return Err(StoreProtocolError::InvalidSequence(0));
        }
        if !keys::verify_signature_hex(
            &head.author_pubkey,
            &head.signature,
            &head.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(head)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreDeviceRegistrationState {
    Active,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRegistration {
    pub version: u32,
    pub genesis_hash: ObjectHash,
    pub device_id: String,
    pub author_pubkey: String,
    pub revision: u64,
    pub previous_registration_hash: Option<ObjectHash>,
    pub state: StoreDeviceRegistrationState,
    pub signature: String,
}

#[derive(Serialize)]
struct RegistrationSignedFields<'a> {
    version: u32,
    genesis_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    revision: u64,
    previous_registration_hash: Option<ObjectHash>,
    state: StoreDeviceRegistrationState,
}

impl StoreDeviceRegistration {
    pub fn signed(
        genesis_hash: ObjectHash,
        device_id: String,
        revision: u64,
        previous_registration_hash: Option<ObjectHash>,
        state: StoreDeviceRegistrationState,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_device_id(&device_id)?;
        validate_chained_revision(revision, previous_registration_hash)?;
        let author_pubkey = keys::public_key_hex(signer);
        let mut registration = Self {
            version: STORE_PROTOCOL_VERSION,
            genesis_hash,
            device_id,
            author_pubkey,
            revision,
            previous_registration_hash,
            state,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &registration.canonical_signed_bytes());
        registration.signature = signature;
        Ok(registration)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            REGISTRATION_DOMAIN,
            &RegistrationSignedFields {
                version: self.version,
                genesis_hash: self.genesis_hash,
                device_id: &self.device_id,
                author_pubkey: &self.author_pubkey,
                revision: self.revision,
                previous_registration_hash: self.previous_registration_hash,
                state: self.state,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreDeviceRegistration serialization cannot fail")
    }

    pub fn registration_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_genesis: ObjectHash,
        expected_device: &str,
        expected_revision: u64,
    ) -> Result<Self, StoreProtocolError> {
        let registration: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(registration.version)?;
        if registration.genesis_hash != expected_genesis {
            return Err(StoreProtocolError::GenesisMismatch {
                expected: expected_genesis,
                actual: registration.genesis_hash,
            });
        }
        if registration.device_id != expected_device || registration.revision != expected_revision {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: registration_slot_prefix(expected_device, expected_revision),
                actual: registration_slot_prefix(&registration.device_id, registration.revision),
            });
        }
        validate_device_id(&registration.device_id)?;
        validate_chained_revision(
            registration.revision,
            registration.previous_registration_hash,
        )?;
        if !keys::verify_signature_hex(
            &registration.author_pubkey,
            &registration.signature,
            &registration.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(registration)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAck {
    pub version: u32,
    pub genesis_hash: ObjectHash,
    pub device_id: String,
    pub author_pubkey: String,
    pub revision: u64,
    pub previous_ack_hash: Option<ObjectHash>,
    pub frontier: BTreeMap<String, CommitPosition>,
    pub last_sync: String,
    pub signature: String,
}

#[derive(Serialize)]
struct AckSignedFields<'a> {
    version: u32,
    genesis_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    revision: u64,
    previous_ack_hash: Option<ObjectHash>,
    frontier: &'a BTreeMap<String, CommitPosition>,
    last_sync: &'a str,
}

impl StoreAck {
    pub fn signed(
        genesis_hash: ObjectHash,
        device_id: String,
        revision: u64,
        previous_ack_hash: Option<ObjectHash>,
        frontier: BTreeMap<String, CommitPosition>,
        last_sync: String,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_device_id(&device_id)?;
        validate_chained_revision(revision, previous_ack_hash)?;
        validate_frontier(&frontier)?;
        let author_pubkey = keys::public_key_hex(signer);
        let mut ack = Self {
            version: STORE_PROTOCOL_VERSION,
            genesis_hash,
            device_id,
            author_pubkey,
            revision,
            previous_ack_hash,
            frontier,
            last_sync,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &ack.canonical_signed_bytes());
        ack.signature = signature;
        Ok(ack)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            ACK_DOMAIN,
            &AckSignedFields {
                version: self.version,
                genesis_hash: self.genesis_hash,
                device_id: &self.device_id,
                author_pubkey: &self.author_pubkey,
                revision: self.revision,
                previous_ack_hash: self.previous_ack_hash,
                frontier: &self.frontier,
                last_sync: &self.last_sync,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreAck serialization cannot fail")
    }

    pub fn ack_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_genesis: ObjectHash,
        expected_device: &str,
        expected_revision: u64,
    ) -> Result<Self, StoreProtocolError> {
        let ack: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(ack.version)?;
        if ack.genesis_hash != expected_genesis {
            return Err(StoreProtocolError::GenesisMismatch {
                expected: expected_genesis,
                actual: ack.genesis_hash,
            });
        }
        if ack.device_id != expected_device || ack.revision != expected_revision {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: ack_slot_prefix(expected_device, expected_revision),
                actual: ack_slot_prefix(&ack.device_id, ack.revision),
            });
        }
        validate_device_id(&ack.device_id)?;
        validate_chained_revision(ack.revision, ack.previous_ack_hash)?;
        validate_frontier(&ack.frontier)?;
        if !keys::verify_signature_hex(
            &ack.author_pubkey,
            &ack.signature,
            &ack.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(ack)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotMeta {
    pub version: u32,
    pub genesis_hash: ObjectHash,
    pub author_pubkey: String,
    pub image_hash: ObjectHash,
    pub coverage: BTreeMap<String, CommitPosition>,
    pub schema_version: u32,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct SnapshotSignedFields<'a> {
    version: u32,
    genesis_hash: ObjectHash,
    author_pubkey: &'a str,
    image_hash: ObjectHash,
    coverage: &'a BTreeMap<String, CommitPosition>,
    schema_version: u32,
    created_at: &'a str,
}

impl SnapshotMeta {
    pub fn signed(
        genesis_hash: ObjectHash,
        image_hash: ObjectHash,
        coverage: BTreeMap<String, CommitPosition>,
        schema_version: u32,
        created_at: String,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_frontier(&coverage)?;
        let author_pubkey = keys::public_key_hex(signer);
        let mut meta = Self {
            version: STORE_PROTOCOL_VERSION,
            genesis_hash,
            author_pubkey,
            image_hash,
            coverage,
            schema_version,
            created_at,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &meta.canonical_signed_bytes());
        meta.signature = signature;
        Ok(meta)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            SNAPSHOT_DOMAIN,
            &SnapshotSignedFields {
                version: self.version,
                genesis_hash: self.genesis_hash,
                author_pubkey: &self.author_pubkey,
                image_hash: self.image_hash,
                coverage: &self.coverage,
                schema_version: self.schema_version,
                created_at: &self.created_at,
            },
        )
    }

    pub fn snapshot_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("SnapshotMeta serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_genesis: ObjectHash,
        expected_author: &str,
        expected_hash: ObjectHash,
    ) -> Result<Self, StoreProtocolError> {
        let meta: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(meta.version)?;
        if meta.genesis_hash != expected_genesis {
            return Err(StoreProtocolError::GenesisMismatch {
                expected: expected_genesis,
                actual: meta.genesis_hash,
            });
        }
        if meta.author_pubkey != expected_author {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: snapshot_semantic_prefix(expected_author, expected_hash),
                actual: snapshot_semantic_prefix(&meta.author_pubkey, meta.snapshot_hash()),
            });
        }
        validate_device_id(&meta.author_pubkey)?;
        validate_frontier(&meta.coverage)?;
        if !keys::verify_signature_hex(
            &meta.author_pubkey,
            &meta.signature,
            &meta.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let actual = meta.snapshot_hash();
        if actual != expected_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected_hash,
                actual,
            });
        }
        Ok(meta)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolGenesis {
    pub version: u32,
    pub store_id: String,
    pub founder: MembershipEntry,
    pub schema_version: u32,
    pub author_pubkey: String,
    pub signature: String,
}

#[derive(Serialize)]
struct GenesisSignedFields<'a> {
    version: u32,
    store_id: &'a str,
    founder: &'a MembershipEntry,
    schema_version: u32,
    author_pubkey: &'a str,
}

impl ProtocolGenesis {
    pub fn signed(
        store_id: String,
        founder: MembershipEntry,
        schema_version: u32,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let author_pubkey = keys::public_key_hex(signer);
        let mut genesis = Self {
            version: STORE_PROTOCOL_VERSION,
            store_id,
            founder,
            schema_version,
            author_pubkey,
            signature: String::new(),
        };
        genesis.validate_founder()?;
        let (_, signature) = keys::sign_hex(signer, &genesis.canonical_signed_bytes());
        genesis.signature = signature;
        Ok(genesis)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            GENESIS_DOMAIN,
            &GenesisSignedFields {
                version: self.version,
                store_id: &self.store_id,
                founder: &self.founder,
                schema_version: self.schema_version,
                author_pubkey: &self.author_pubkey,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("ProtocolGenesis serialization cannot fail")
    }

    pub fn object_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.to_bytes())
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, StoreProtocolError> {
        let genesis: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(genesis.version)?;
        genesis.validate_founder()?;
        if !keys::verify_signature_hex(
            &genesis.author_pubkey,
            &genesis.signature,
            &genesis.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(genesis)
    }

    pub fn parse_expected(
        bytes: &[u8],
        expected_hash: ObjectHash,
        expected_store_id: &str,
        expected_founder: &str,
    ) -> Result<Self, StoreProtocolError> {
        let genesis = Self::parse(bytes)?;
        let actual_hash = genesis.object_hash();
        if actual_hash != expected_hash {
            return Err(StoreProtocolError::GenesisMismatch {
                expected: expected_hash,
                actual: actual_hash,
            });
        }
        if genesis.store_id != expected_store_id {
            return Err(StoreProtocolError::StoreMismatch {
                expected: expected_store_id.to_string(),
                actual: genesis.store_id,
            });
        }
        if genesis.author_pubkey != expected_founder {
            return Err(StoreProtocolError::FounderMismatch {
                expected: expected_founder.to_string(),
                actual: genesis.author_pubkey,
            });
        }
        Ok(genesis)
    }

    fn validate_founder(&self) -> Result<(), StoreProtocolError> {
        if self.store_id.is_empty() {
            return Err(StoreProtocolError::EmptyStoreId);
        }
        let founder = &self.founder;
        let expected_grant = derive_founder_grant_id(&self.store_id, &self.author_pubkey);
        if founder.version != STORE_PROTOCOL_VERSION
            || founder.store_id != self.store_id
            || founder.author_pubkey != self.author_pubkey
            || founder.author_owner_grant != expected_grant
            || founder.seq != 1
            || founder.previous_hash.is_some()
            || !founder.dependencies.is_empty()
            || founder.change
                != (MembershipChange::Founder {
                    owner_pubkey: self.author_pubkey.clone(),
                    owner_grant_id: expected_grant,
                })
            || !verify_membership_entry(founder)
        {
            return Err(StoreProtocolError::InvalidFounder);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StoreProtocolError {
    #[error("object hash must be exactly 64 lowercase hexadecimal characters: {0:?}")]
    InvalidObjectHash(String),
    #[error("unsupported Store protocol version {0}")]
    UnsupportedVersion(u32),
    #[error("malformed Store protocol object: {0}")]
    Malformed(String),
    #[error("Store protocol signature is invalid")]
    InvalidSignature,
    #[error("Store protocol object is in slot {actual:?}, expected {expected:?}")]
    RelocatedSlot { expected: String, actual: String },
    #[error("Store package names key {actual:?}, expected {expected:?}")]
    RelocatedPackage { expected: String, actual: String },
    #[error("Store protocol genesis hash is {actual}, expected {expected}")]
    GenesisMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store id is {actual:?}, expected {expected:?}")]
    StoreMismatch { expected: String, actual: String },
    #[error("founder is {actual:?}, expected {expected:?}")]
    FounderMismatch { expected: String, actual: String },
    #[error("protocol genesis has an invalid founder membership entry")]
    InvalidFounder,
    #[error("protocol genesis store id is empty")]
    EmptyStoreId,
    #[error("Store commit sequence must start at 1, got {0}")]
    InvalidSequence(u64),
    #[error("Store commit sequence 1 must not name a predecessor")]
    UnexpectedPredecessor,
    #[error("Store commit after sequence 1 must name its predecessor hash")]
    MissingPredecessor,
    #[error("Store control revision must start at 1, got {0}")]
    InvalidRevision(u64),
    #[error("Store control revision 1 must not name a predecessor")]
    UnexpectedControlPredecessor,
    #[error("Store control revision after 1 must name its predecessor hash")]
    MissingControlPredecessor,
    #[error("Store commit for {0:?} must not name its own device as a dependency")]
    OwnDependency(String),
    #[error("invalid membership coordinate {author}/{grant}/{seq} with entry hash {entry_hash}")]
    InvalidMembershipCoordinate {
        author: String,
        grant: String,
        seq: u64,
        entry_hash: String,
    },
    #[error(
        "membership object coordinate {expected_author}/{expected_grant}/{expected_seq} differs from signed entry {declared_author}/{declared_grant}/{declared_seq}"
    )]
    MembershipCoordinateMismatch {
        expected_author: String,
        expected_grant: String,
        expected_seq: u64,
        declared_author: String,
        declared_grant: String,
        declared_seq: u64,
    },
    #[error("Store package length exceeds the platform address space")]
    PackageTooLarge,
    #[error("Store package length is {actual}, expected {expected}")]
    PackageLengthMismatch { expected: u64, actual: u64 },
    #[error("Store package hash is {actual}, expected {expected}")]
    PackageHashMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store object hash is {actual}, expected {expected}")]
    ObjectHashMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("unsafe Store device id: {0}")]
    UnsafeDeviceId(String),
    #[error("malformed Store protocol path: {0:?}")]
    MalformedPath(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashedCopySlot {
    pub owner: String,
    pub sequence: u64,
    pub semantic_hash: ObjectHash,
    pub copy_id: CopyId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipCopySlot {
    pub author: String,
    pub author_owner_grant: OwnerGrantId,
    pub sequence: u64,
    pub semantic_hash: ObjectHash,
    pub copy_id: CopyId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCopySlot {
    pub author: String,
    pub semantic_hash: ObjectHash,
    pub copy_id: CopyId,
}

pub fn parse_genesis_copy_key(path: &str) -> Result<(ObjectHash, CopyId), StoreProtocolError> {
    let segments: Vec<&str> = path.split('/').collect();
    if segments.len() != 5
        || segments[0] != "store-v1"
        || segments[1] != "genesis"
        || segments[3] != "copies"
    {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    Ok((
        segments[2]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        parse_copy_filename(segments[4], ".json", path)?,
    ))
}

fn parse_decimal(segment: &str, allow_zero: bool, path: &str) -> Result<u64, StoreProtocolError> {
    let value = segment
        .parse::<u64>()
        .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?;
    if value.to_string() != segment || (!allow_zero && value == 0) {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    Ok(value)
}

fn parse_copy_filename(
    filename: &str,
    extension: &str,
    path: &str,
) -> Result<CopyId, StoreProtocolError> {
    filename
        .strip_suffix(extension)
        .ok_or_else(|| StoreProtocolError::MalformedPath(path.to_string()))?
        .parse()
        .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))
}

fn parse_hashed_copy_slot(
    path: &str,
    namespace: &[&str],
    extension: &str,
    allow_zero: bool,
) -> Result<HashedCopySlot, StoreProtocolError> {
    let segments: Vec<&str> = path.split('/').collect();
    let offset = namespace.len();
    if segments.len() != offset + 5
        || segments[..offset] != *namespace
        || segments[offset + 3] != "copies"
    {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    let owner = segments[offset].to_string();
    validate_device_id(&owner)?;
    Ok(HashedCopySlot {
        owner,
        sequence: parse_decimal(segments[offset + 1], allow_zero, path)?,
        semantic_hash: segments[offset + 2]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        copy_id: parse_copy_filename(segments[offset + 4], extension, path)?,
    })
}

pub fn parse_package_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, &["store-v1", "packages"], ".pkg", false)
}

pub fn parse_commit_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, &["store-v1", "commits"], ".json", false)
}

pub fn parse_head_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, &["store-v1", "heads"], ".json", true)
}

pub fn parse_registration_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, &["store-v1", "devices"], ".json", false)
}

pub fn parse_ack_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, &["store-v1", "acks"], ".json", false)
}

fn parse_membership_copy_key(
    path: &str,
    object_kind: &str,
) -> Result<MembershipCopySlot, StoreProtocolError> {
    let segments: Vec<&str> = path.split('/').collect();
    if segments.len() != 9
        || segments[..3] != ["store-v1", "membership", object_kind]
        || segments[7] != "copies"
    {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    validate_device_id(segments[3])?;
    Ok(MembershipCopySlot {
        author: segments[3].to_string(),
        author_owner_grant: OwnerGrantId(
            segments[4]
                .parse()
                .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        ),
        sequence: parse_decimal(segments[5], false, path)?,
        semantic_hash: segments[6]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        copy_id: parse_copy_filename(segments[8], ".json", path)?,
    })
}

pub fn parse_membership_entry_copy_key(
    path: &str,
) -> Result<MembershipCopySlot, StoreProtocolError> {
    parse_membership_copy_key(path, "entries")
}

pub fn parse_membership_head_copy_key(
    path: &str,
) -> Result<MembershipCopySlot, StoreProtocolError> {
    parse_membership_copy_key(path, "heads")
}

fn parse_snapshot_copy_key(
    path: &str,
    namespace: &str,
    extension: &str,
) -> Result<SnapshotCopySlot, StoreProtocolError> {
    let segments: Vec<&str> = path.split('/').collect();
    if segments.len() != 6
        || segments[0] != "store-v1"
        || segments[1] != namespace
        || segments[4] != "copies"
    {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    validate_device_id(segments[2])?;
    Ok(SnapshotCopySlot {
        author: segments[2].to_string(),
        semantic_hash: segments[3]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        copy_id: parse_copy_filename(segments[5], extension, path)?,
    })
}

pub fn parse_snapshot_meta_copy_key(path: &str) -> Result<SnapshotCopySlot, StoreProtocolError> {
    parse_snapshot_copy_key(path, "snapshots", ".json")
}

pub fn parse_snapshot_image_copy_key(path: &str) -> Result<SnapshotCopySlot, StoreProtocolError> {
    parse_snapshot_copy_key(path, "snapshot-images", ".db")
}

pub fn protocol_prefix() -> &'static str {
    "store-v1/"
}

pub fn genesis_semantic_prefix(genesis_hash: ObjectHash) -> String {
    format!("store-v1/genesis/{genesis_hash}")
}

pub fn genesis_copy_key(genesis_hash: ObjectHash, copy_id: CopyId) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        genesis_semantic_prefix(genesis_hash)
    )
}

pub fn package_semantic_prefix(device_id: &str, seq: u64, package_hash: ObjectHash) -> String {
    format!("store-v1/packages/{device_id}/{seq}/{package_hash}")
}

pub fn package_copy_key(
    device_id: &str,
    seq: u64,
    package_hash: ObjectHash,
    copy_id: CopyId,
) -> String {
    format!(
        "{}/copies/{copy_id}.pkg",
        package_semantic_prefix(device_id, seq, package_hash)
    )
}

pub fn commit_slot_prefix(device_id: &str, seq: u64) -> String {
    format!("store-v1/commits/{device_id}/{seq}")
}

pub fn commit_semantic_prefix(device_id: &str, seq: u64, commit_hash: ObjectHash) -> String {
    format!("{}/{commit_hash}", commit_slot_prefix(device_id, seq))
}

pub fn commit_copy_key(
    device_id: &str,
    seq: u64,
    commit_hash: ObjectHash,
    copy_id: CopyId,
) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        commit_semantic_prefix(device_id, seq, commit_hash)
    )
}

pub fn head_slot_prefix(device_id: &str, seq: u64) -> String {
    format!("store-v1/heads/{device_id}/{seq}")
}

pub fn head_semantic_prefix(device_id: &str, seq: u64, head_hash: ObjectHash) -> String {
    format!("{}/{head_hash}", head_slot_prefix(device_id, seq))
}

pub fn head_copy_key(device_id: &str, seq: u64, head_hash: ObjectHash, copy_id: CopyId) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        head_semantic_prefix(device_id, seq, head_hash)
    )
}

pub fn registration_slot_prefix(device_id: &str, revision: u64) -> String {
    format!("store-v1/devices/{device_id}/{revision}")
}

pub fn registration_semantic_prefix(
    device_id: &str,
    revision: u64,
    registration_hash: ObjectHash,
) -> String {
    format!(
        "{}/{registration_hash}",
        registration_slot_prefix(device_id, revision)
    )
}

pub fn registration_copy_key(
    device_id: &str,
    revision: u64,
    registration_hash: ObjectHash,
    copy_id: CopyId,
) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        registration_semantic_prefix(device_id, revision, registration_hash)
    )
}

pub fn ack_slot_prefix(device_id: &str, revision: u64) -> String {
    format!("store-v1/acks/{device_id}/{revision}")
}

pub fn ack_semantic_prefix(device_id: &str, revision: u64, ack_hash: ObjectHash) -> String {
    format!("{}/{ack_hash}", ack_slot_prefix(device_id, revision))
}

pub fn ack_copy_key(
    device_id: &str,
    revision: u64,
    ack_hash: ObjectHash,
    copy_id: CopyId,
) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        ack_semantic_prefix(device_id, revision, ack_hash)
    )
}

pub fn membership_entry_semantic_prefix(
    author: &str,
    author_owner_grant: &OwnerGrantId,
    seq: u64,
    entry_hash: ObjectHash,
) -> String {
    format!("store-v1/membership/entries/{author}/{author_owner_grant}/{seq}/{entry_hash}")
}

pub fn membership_entry_copy_key(
    author: &str,
    author_owner_grant: &OwnerGrantId,
    seq: u64,
    entry_hash: ObjectHash,
    copy_id: CopyId,
) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        membership_entry_semantic_prefix(author, author_owner_grant, seq, entry_hash)
    )
}

pub fn membership_head_semantic_prefix(
    author: &str,
    author_owner_grant: &OwnerGrantId,
    seq: u64,
    head_hash: ObjectHash,
) -> String {
    format!("store-v1/membership/heads/{author}/{author_owner_grant}/{seq}/{head_hash}")
}

pub fn membership_head_copy_key(
    author: &str,
    author_owner_grant: &OwnerGrantId,
    seq: u64,
    head_hash: ObjectHash,
    copy_id: CopyId,
) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        membership_head_semantic_prefix(author, author_owner_grant, seq, head_hash)
    )
}

pub fn snapshot_image_semantic_prefix(author: &str, image_hash: ObjectHash) -> String {
    format!("store-v1/snapshot-images/{author}/{image_hash}")
}

pub fn snapshot_image_copy_key(author: &str, image_hash: ObjectHash, copy_id: CopyId) -> String {
    format!(
        "{}/copies/{copy_id}.db",
        snapshot_image_semantic_prefix(author, image_hash)
    )
}

pub fn snapshot_semantic_prefix(author: &str, snapshot_hash: ObjectHash) -> String {
    format!("store-v1/snapshots/{author}/{snapshot_hash}")
}

pub fn snapshot_copy_key(author: &str, snapshot_hash: ObjectHash, copy_id: CopyId) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        snapshot_semantic_prefix(author, snapshot_hash)
    )
}

fn domain_json(domain: &[u8], value: &impl Serialize) -> Vec<u8> {
    let json = serde_json::to_vec(value).expect("canonical Store fields serialize");
    let mut bytes = Vec::with_capacity(domain.len() + json.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&json);
    bytes
}

fn require_version(version: u32) -> Result<(), StoreProtocolError> {
    if version == STORE_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(StoreProtocolError::UnsupportedVersion(version))
    }
}

fn validate_chained_revision(
    revision: u64,
    previous_hash: Option<ObjectHash>,
) -> Result<(), StoreProtocolError> {
    match (revision, previous_hash) {
        (0, _) => Err(StoreProtocolError::InvalidRevision(0)),
        (1, None) => Ok(()),
        (1, Some(_)) => Err(StoreProtocolError::UnexpectedControlPredecessor),
        (_, Some(_)) => Ok(()),
        (_, None) => Err(StoreProtocolError::MissingControlPredecessor),
    }
}

fn validate_frontier(
    frontier: &BTreeMap<String, CommitPosition>,
) -> Result<(), StoreProtocolError> {
    for (device_id, position) in frontier {
        validate_device_id(device_id)?;
        if position.seq == 0 {
            return Err(StoreProtocolError::InvalidSequence(0));
        }
    }
    Ok(())
}

fn validate_membership_coord(coord: &MembershipCoord) -> Result<(), StoreProtocolError> {
    if coord.seq == 0 || coord.author_pubkey.is_empty() {
        return Err(StoreProtocolError::InvalidMembershipCoordinate {
            author: coord.author_pubkey.clone(),
            grant: coord.author_owner_grant.to_string(),
            seq: coord.seq,
            entry_hash: coord.entry_hash.to_string(),
        });
    }
    Ok(())
}

fn validate_device_id(device_id: &str) -> Result<(), StoreProtocolError> {
    crate::store_dir::validate_path_token(device_id)
        .map_err(|error| StoreProtocolError::UnsafeDeviceId(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::membership::founder_entry;

    fn fixture() -> (UserKeypair, ProtocolGenesis, StoreBatchCommit, Vec<u8>) {
        let signer = UserKeypair::generate();
        let founder = founder_entry("store-a", &signer, "0000000001000-0000-device-a");
        let genesis = ProtocolGenesis::signed("store-a".to_string(), founder, 3, &signer)
            .expect("sign genesis");
        let package = b"package".to_vec();
        let commit = StoreBatchCommit::signed(
            genesis.object_hash(),
            "device-a".to_string(),
            1,
            None,
            BTreeMap::from([(
                "device-b".to_string(),
                CommitPosition {
                    seq: 4,
                    commit_hash: ObjectHash::digest(b"device-b/4"),
                },
            )]),
            Some(MembershipCoord {
                author_pubkey: keys::public_key_hex(&signer),
                author_owner_grant: genesis.founder.author_owner_grant.clone(),
                seq: 1,
                entry_hash: crate::sync::membership::entry_hash(&genesis.founder),
            }),
            3,
            &package,
            &signer,
        )
        .expect("sign commit");
        (signer, genesis, commit, package)
    }

    #[test]
    fn object_hash_is_strict_lowercase_hex() {
        let hash = ObjectHash::digest(b"fixture");
        assert_eq!(hash.to_string().parse::<ObjectHash>().unwrap(), hash);
        assert!(hash
            .to_string()
            .to_uppercase()
            .parse::<ObjectHash>()
            .is_err());
        assert!("0".repeat(63).parse::<ObjectHash>().is_err());
        assert!(format!("{}g", "0".repeat(63))
            .parse::<ObjectHash>()
            .is_err());
    }

    #[test]
    fn canonical_commit_round_trip_and_literal_bytes() {
        let (_, genesis, commit, package) = fixture();
        let bytes = commit.to_bytes();
        let parsed = StoreBatchCommit::parse_at(&bytes, genesis.object_hash(), "device-a", 1)
            .expect("parse commit");
        parsed.verify_package(&package).expect("verify package");
        assert_eq!(parsed, commit);
        assert!(commit.canonical_signed_bytes().starts_with(COMMIT_DOMAIN));
    }

    #[test]
    fn commit_rejects_dependency_package_predecessor_and_slot_tamper() {
        let (_, genesis, commit, package) = fixture();
        let mut tampered = commit.clone();
        tampered.dependencies.get_mut("device-b").unwrap().seq += 1;
        assert!(matches!(
            tampered.verify_at(genesis.object_hash(), "device-a", 1),
            Err(StoreProtocolError::InvalidSignature)
        ));

        let mut tampered = commit.clone();
        tampered.package.content_hash = ObjectHash::digest(b"different");
        assert!(matches!(
            tampered.verify_at(genesis.object_hash(), "device-a", 1),
            Err(StoreProtocolError::RelocatedPackage { .. })
        ));

        assert!(matches!(
            commit.verify_at(genesis.object_hash(), "device-a", 2),
            Err(StoreProtocolError::RelocatedSlot { .. })
        ));
        assert!(matches!(
            commit.verify_package(b"different"),
            Err(StoreProtocolError::PackageLengthMismatch { .. })
                | Err(StoreProtocolError::PackageHashMismatch { .. })
        ));
        commit.verify_package(&package).unwrap();
    }

    #[test]
    fn unknown_fields_and_versions_are_rejected() {
        let (_, genesis, commit, _) = fixture();
        let mut value = serde_json::to_value(&commit).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(StoreBatchCommit::parse_at(
            &serde_json::to_vec(&value).unwrap(),
            genesis.object_hash(),
            "device-a",
            1,
        )
        .is_err());

        let mut value = serde_json::to_value(&commit).unwrap();
        value["version"] = serde_json::json!(2);
        assert!(matches!(
            StoreBatchCommit::parse_at(
                &serde_json::to_vec(&value).unwrap(),
                genesis.object_hash(),
                "device-a",
                1,
            ),
            Err(StoreProtocolError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn genesis_embeds_and_authenticates_the_founder_entry() {
        let (_, genesis, _, _) = fixture();
        let bytes = genesis.to_bytes();
        let parsed = ProtocolGenesis::parse_expected(
            &bytes,
            genesis.object_hash(),
            "store-a",
            &genesis.author_pubkey,
        )
        .expect("parse exact genesis");
        assert_eq!(parsed, genesis);
    }
}
