//! Signed, hash-addressed Store commit protocol objects.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
use crate::sync::circle::{CircleControlCoord, CircleId};
use crate::KeyFingerprint;
use crate::{WriteId, WritePolicy};

pub const STORE_PROTOCOL_VERSION: u32 = 1;

pub(crate) const STORE_PROTOCOL_PREFIX: &str = "store-v1/";
pub(crate) const STORE_PROTOCOL_ROOT_PREFIX: &str = "store-v1/store-protocol-root/";
pub(crate) const STORE_COMMIT_PREFIX: &str = "store-v1/commits/";
pub(crate) const STORE_HEAD_PREFIX: &str = "store-v1/heads/";
pub(crate) const STORE_ACK_PREFIX: &str = "store-v1/acks/";
pub(crate) const STORE_DEVICE_REGISTRATION_PREFIX: &str = "store-v1/devices/";
pub(crate) const STORE_SNAPSHOT_META_PREFIX: &str = "store-v1/snapshots/";
pub(crate) const STORE_SNAPSHOT_IMAGE_PREFIX: &str = "store-v1/snapshot-images/";
pub(crate) const STORE_MEMBERSHIP_ENTRY_PREFIX: &str = "store-v1/membership/entries/";
pub(crate) const STORE_MEMBERSHIP_HEAD_PREFIX: &str = "store-v1/membership/heads/";
pub(crate) const STORE_PACKAGE_PREFIX: &str = "store-v1/packages/";
const STORE_SERIAL_HEAD_KEY: &str = "store-v1/heads/serial.json";

const STORE_PROTOCOL_ROOT_DOMAIN: &[u8] = b"coven.store-protocol-root.v1\0";
const COMMIT_DOMAIN: &[u8] = b"coven.store-batch-commit.v1\0";
const HEAD_DOMAIN: &[u8] = b"coven.store-device-head.v1\0";
const SERIAL_HEAD_DOMAIN: &[u8] = b"coven.store-serial-head.v1\0";
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

/// Exact materialized cut, shaped by the Store's signed write policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitFrontier {
    MergeConcurrent(BTreeMap<String, CommitPosition>),
    Serial(Option<CommitPosition>),
}

impl CommitFrontier {
    pub fn from_positions(
        policy: WritePolicy,
        mut positions: BTreeMap<String, CommitPosition>,
    ) -> Result<Self, StoreProtocolError> {
        match policy {
            WritePolicy::MergeConcurrent => Ok(Self::MergeConcurrent(positions)),
            WritePolicy::Serial => {
                let position = positions.remove(SERIAL_STREAM_ID);
                if !positions.is_empty() {
                    return Err(StoreProtocolError::Malformed(format!(
                        "Serial frontier contains non-serial streams: {:?}",
                        positions.keys().collect::<Vec<_>>()
                    )));
                }
                Ok(Self::Serial(position))
            }
        }
    }

    pub fn into_positions(self) -> BTreeMap<String, CommitPosition> {
        match self {
            Self::MergeConcurrent(positions) => positions,
            Self::Serial(Some(position)) => {
                BTreeMap::from([(SERIAL_STREAM_ID.to_string(), position)])
            }
            Self::Serial(None) => BTreeMap::new(),
        }
    }

    pub fn position_count(&self) -> usize {
        match self {
            Self::MergeConcurrent(positions) => positions.len(),
            Self::Serial(Some(_)) => 1,
            Self::Serial(None) => 0,
        }
    }

    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent(_) => WritePolicy::MergeConcurrent,
            Self::Serial(_) => WritePolicy::Serial,
        }
    }

    pub fn merge_positions(&self) -> Result<&BTreeMap<String, CommitPosition>, StoreProtocolError> {
        match self {
            Self::MergeConcurrent(positions) => Ok(positions),
            Self::Serial(_) => Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            }),
        }
    }

    pub fn serial_position(&self) -> Result<Option<&CommitPosition>, StoreProtocolError> {
        match self {
            Self::Serial(position) => Ok(position.as_ref()),
            Self::MergeConcurrent(_) => Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::Serial,
                actual: WritePolicy::MergeConcurrent,
            }),
        }
    }
}

/// Predecessor and dependency order authenticated by one Store commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitOrder {
    MergeConcurrent {
        seq: u64,
        previous_commit_hash: Option<ObjectHash>,
        dependencies: BTreeMap<String, CommitPosition>,
    },
    Serial {
        seq: u64,
        previous_commit_hash: Option<ObjectHash>,
    },
}

impl StoreCommitOrder {
    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial { .. } => WritePolicy::Serial,
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            Self::MergeConcurrent { seq, .. } | Self::Serial { seq, .. } => *seq,
        }
    }

    pub fn previous_commit_hash(&self) -> Option<ObjectHash> {
        match self {
            Self::MergeConcurrent {
                previous_commit_hash,
                ..
            }
            | Self::Serial {
                previous_commit_hash,
                ..
            } => *previous_commit_hash,
        }
    }

    pub fn dependencies(&self) -> Option<&BTreeMap<String, CommitPosition>> {
        match self {
            Self::MergeConcurrent { dependencies, .. } => Some(dependencies),
            Self::Serial { .. } => None,
        }
    }

    pub fn stream_id<'a>(&self, device_id: &'a str) -> &'a str {
        match self {
            Self::MergeConcurrent { .. } => device_id,
            Self::Serial { .. } => SERIAL_STREAM_ID,
        }
    }
}

pub const SERIAL_STREAM_ID: &str = "serial";

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
pub struct CirclePackageRef {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub package: StorePackageRef,
    pub key_fingerprint: KeyFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlRef {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRegistrationRef {
    pub device_id: String,
    pub revision: u64,
    pub registration_hash: ObjectHash,
}

impl StoreDeviceRegistrationRef {
    pub fn from_registration(registration: &StoreDeviceRegistration) -> Self {
        Self {
            device_id: registration.device_id.clone(),
            revision: registration.revision,
            registration_hash: registration.registration_hash(),
        }
    }

    pub fn verify_registration(
        &self,
        registration: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        if registration.device_id != self.device_id
            || registration.revision != self.revision
            || registration.registration_hash() != self.registration_hash
        {
            return Err(StoreProtocolError::DeviceRegistrationRefMismatch {
                device_id: self.device_id.clone(),
                revision: self.revision,
                expected: self.registration_hash,
                actual: registration.registration_hash(),
            });
        }
        Ok(())
    }
}

pub struct CirclePackageInput<'a> {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub key_fingerprint: KeyFingerprint,
    pub package: StorePackageInput<'a>,
}

pub struct StorePackageInput<'a> {
    pub schema_version: u32,
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreControl {
    SerialMembership {
        entry: super::membership::SerialMembershipEntry,
    },
    SerialMembershipAndKeyRotation {
        entry: super::membership::SerialMembershipEntry,
        generation: u64,
    },
}

impl StoreControl {
    pub fn serial_membership_entry(&self) -> &super::membership::SerialMembershipEntry {
        match self {
            Self::SerialMembership { entry }
            | Self::SerialMembershipAndKeyRotation { entry, .. } => entry,
        }
    }

    pub fn key_generation(&self) -> Option<u64> {
        match self {
            Self::SerialMembership { .. } => None,
            Self::SerialMembershipAndKeyRotation { generation, .. } => Some(*generation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommit {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub device_id: String,
    pub author_pubkey: String,
    pub write_id: WriteId,
    pub order: StoreCommitOrder,
    pub membership_grant: Option<MembershipCoord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<StoreControl>,
    pub device_registrations: Vec<StoreDeviceRegistrationRef>,
    pub circle_controls: Vec<CircleControlRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_package: Option<StorePackageRef>,
    pub circle_packages: Vec<CirclePackageRef>,
    pub signature: String,
}

#[derive(Serialize)]
struct CommitSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    write_id: &'a WriteId,
    order: &'a StoreCommitOrder,
    membership_grant: Option<&'a MembershipCoord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    control: Option<&'a StoreControl>,
    device_registrations: &'a [StoreDeviceRegistrationRef],
    circle_controls: &'a [CircleControlRef],
    #[serde(skip_serializing_if = "Option::is_none")]
    store_package: Option<&'a StorePackageRef>,
    circle_packages: &'a [CirclePackageRef],
}

impl StoreBatchCommit {
    pub fn policy(&self) -> WritePolicy {
        self.order.policy()
    }

    pub fn seq(&self) -> u64 {
        self.order.seq()
    }

    pub fn previous_commit_hash(&self) -> Option<ObjectHash> {
        self.order.previous_commit_hash()
    }

    pub fn merge_dependencies(
        &self,
    ) -> Result<&BTreeMap<String, CommitPosition>, StoreProtocolError> {
        match &self.order {
            StoreCommitOrder::MergeConcurrent { dependencies, .. } => Ok(dependencies),
            StoreCommitOrder::Serial { .. } => Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        device_id: String,
        order: StoreCommitOrder,
        membership_grant: Option<MembershipCoord>,
        schema_version: u32,
        package_bytes: &[u8],
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_batch(
            store_root_hash,
            write_id,
            device_id,
            order,
            membership_grant,
            None,
            Vec::new(),
            Vec::new(),
            Some(StorePackageInput {
                schema_version,
                bytes: package_bytes,
            }),
            &[],
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_control(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        device_id: String,
        order: StoreCommitOrder,
        membership_grant: Option<MembershipCoord>,
        control: Option<StoreControl>,
        schema_version: u32,
        package_bytes: &[u8],
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let store_package = (!package_bytes.is_empty()).then_some(package_bytes);
        Self::signed_batch(
            store_root_hash,
            write_id,
            device_id,
            order,
            membership_grant,
            control,
            Vec::new(),
            Vec::new(),
            store_package.map(|bytes| StorePackageInput {
                schema_version,
                bytes,
            }),
            &[],
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_registrations(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        device_id: String,
        order: StoreCommitOrder,
        membership_grant: Option<MembershipCoord>,
        device_registrations: Vec<StoreDeviceRegistrationRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_batch(
            store_root_hash,
            write_id,
            device_id,
            order,
            membership_grant,
            None,
            device_registrations,
            Vec::new(),
            None,
            &[],
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_batch(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        device_id: String,
        order: StoreCommitOrder,
        membership_grant: Option<MembershipCoord>,
        control: Option<StoreControl>,
        device_registrations: Vec<StoreDeviceRegistrationRef>,
        circle_controls: Vec<CircleControlRef>,
        store_package_input: Option<StorePackageInput<'_>>,
        circle_package_inputs: &[CirclePackageInput<'_>],
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_device_id(&device_id)?;
        let seq = order.seq();
        let previous_commit_hash = order.previous_commit_hash();
        if seq == 0 {
            return Err(StoreProtocolError::InvalidSequence(seq));
        }
        match (seq, previous_commit_hash) {
            (1, None) => {}
            (1, Some(_)) => return Err(StoreProtocolError::UnexpectedPredecessor),
            (_, Some(_)) => {}
            (_, None) => return Err(StoreProtocolError::MissingPredecessor),
        }
        if let Some(dependencies) = order.dependencies() {
            if dependencies.contains_key(&device_id) {
                return Err(StoreProtocolError::OwnDependency(device_id));
            }
            validate_frontier(dependencies)?;
        }
        if let Some(grant) = membership_grant.as_ref() {
            validate_membership_coord(grant)?;
        }
        let author_pubkey = keys::public_key_hex(signer);
        validate_control(
            order.policy(),
            store_root_hash,
            &author_pubkey,
            control.as_ref(),
        )?;
        let stream_id = order.stream_id(&device_id);
        let store_package = store_package_input
            .map(|input| package_ref(stream_id, seq, input.schema_version, input.bytes))
            .transpose()?;
        validate_device_registration_refs(&device_registrations)?;
        let mut seen_circles = BTreeSet::new();
        let circle_packages = circle_package_inputs
            .iter()
            .map(|input| {
                if !seen_circles.insert(input.circle_id) {
                    return Err(StoreProtocolError::DuplicateCirclePackage(input.circle_id));
                }
                validate_circle_control_coord(order.policy(), &input.control)?;
                let package = package_ref(
                    stream_id,
                    seq,
                    input.package.schema_version,
                    input.package.bytes,
                )?;
                Ok(CirclePackageRef {
                    circle_id: input.circle_id,
                    control: input.control.clone(),
                    package: StorePackageRef {
                        object_key: circle_package_semantic_prefix(
                            input.circle_id,
                            stream_id,
                            seq,
                            package.content_hash,
                        ),
                        ..package
                    },
                    key_fingerprint: input.key_fingerprint,
                })
            })
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        for control_ref in &circle_controls {
            validate_circle_control_coord(order.policy(), &control_ref.control)?;
        }
        if control.is_none()
            && device_registrations.is_empty()
            && circle_controls.is_empty()
            && store_package.is_none()
            && circle_packages.is_empty()
        {
            return Err(StoreProtocolError::EmptyBatch);
        }
        let mut commit = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            device_id,
            author_pubkey,
            write_id,
            order,
            membership_grant,
            control,
            device_registrations,
            circle_controls,
            store_package,
            circle_packages,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &commit.canonical_signed_bytes());
        commit.signature = signature;
        Ok(commit)
    }

    pub fn canonical_signed_bytes(&self) -> Vec<u8> {
        let fields = CommitSignedFields {
            version: self.version,
            store_root_hash: self.store_root_hash,
            device_id: &self.device_id,
            author_pubkey: &self.author_pubkey,
            write_id: &self.write_id,
            order: &self.order,
            membership_grant: self.membership_grant.as_ref(),
            control: self.control.as_ref(),
            device_registrations: &self.device_registrations,
            circle_controls: &self.circle_controls,
            store_package: self.store_package.as_ref(),
            circle_packages: &self.circle_packages,
        };
        domain_json(COMMIT_DOMAIN, &fields)
    }

    pub fn commit_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn position(&self) -> CommitPosition {
        CommitPosition {
            seq: self.order.seq(),
            commit_hash: self.commit_hash(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreBatchCommit serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected_policy: WritePolicy,
        expected_stream: &str,
        expected_seq: u64,
    ) -> Result<Self, StoreProtocolError> {
        let commit: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        commit.verify_at(
            expected_store_root_hash,
            expected_policy,
            expected_stream,
            expected_seq,
        )?;
        Ok(commit)
    }

    pub fn verify_at(
        &self,
        expected_store_root_hash: ObjectHash,
        expected_policy: WritePolicy,
        expected_stream: &str,
        expected_seq: u64,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        if self.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: self.store_root_hash,
            });
        }
        if self.order.policy() != expected_policy {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: expected_policy,
                actual: self.order.policy(),
            });
        }
        let stream_id = self.order.stream_id(&self.device_id);
        if stream_id != expected_stream || self.order.seq() != expected_seq {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: commit_slot_prefix(expected_stream, expected_seq),
                actual: commit_slot_prefix(stream_id, self.order.seq()),
            });
        }
        validate_device_id(&self.device_id)?;
        if let Some(package) = &self.store_package {
            let expected =
                package_semantic_prefix(stream_id, self.order.seq(), package.content_hash);
            if package.object_key != expected {
                return Err(StoreProtocolError::RelocatedPackage {
                    expected,
                    actual: package.object_key.clone(),
                });
            }
        }
        let mut seen_circles = BTreeSet::new();
        for circle_package in &self.circle_packages {
            if !seen_circles.insert(circle_package.circle_id) {
                return Err(StoreProtocolError::DuplicateCirclePackage(
                    circle_package.circle_id,
                ));
            }
            validate_circle_control_coord(self.policy(), &circle_package.control)?;
            let expected = circle_package_semantic_prefix(
                circle_package.circle_id,
                stream_id,
                self.seq(),
                circle_package.package.content_hash,
            );
            if circle_package.package.object_key != expected {
                return Err(StoreProtocolError::RelocatedCirclePackage {
                    circle_id: circle_package.circle_id,
                    expected,
                    actual: circle_package.package.object_key.clone(),
                });
            }
        }
        for control_ref in &self.circle_controls {
            validate_circle_control_coord(self.policy(), &control_ref.control)?;
        }
        validate_device_registration_refs(&self.device_registrations)?;
        if self.control.is_none()
            && self.device_registrations.is_empty()
            && self.circle_controls.is_empty()
            && self.store_package.is_none()
            && self.circle_packages.is_empty()
        {
            return Err(StoreProtocolError::EmptyBatch);
        }
        match (self.order.seq(), self.order.previous_commit_hash()) {
            (0, _) => return Err(StoreProtocolError::InvalidSequence(0)),
            (1, None) => {}
            (1, Some(_)) => return Err(StoreProtocolError::UnexpectedPredecessor),
            (_, None) => return Err(StoreProtocolError::MissingPredecessor),
            (_, Some(_)) => {}
        }
        if let Some(dependencies) = self.order.dependencies() {
            if dependencies.contains_key(&self.device_id) {
                return Err(StoreProtocolError::OwnDependency(self.device_id.clone()));
            }
            validate_frontier(dependencies)?;
        }
        if let Some(grant) = self.membership_grant.as_ref() {
            validate_membership_coord(grant)?;
        }
        validate_parsed_control(self)?;
        if !keys::verify_signature_hex(
            &self.author_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    pub fn verify_store_package(&self, package_bytes: &[u8]) -> Result<(), StoreProtocolError> {
        let package = self
            .store_package
            .as_ref()
            .ok_or(StoreProtocolError::MissingStorePackage)?;
        verify_package_ref(package, package_bytes)
    }

    pub fn verify_circle_package(
        &self,
        circle_id: CircleId,
        package_bytes: &[u8],
    ) -> Result<(), StoreProtocolError> {
        let package = self
            .circle_packages
            .iter()
            .find(|package| package.circle_id == circle_id)
            .ok_or(StoreProtocolError::MissingCirclePackage(circle_id))?;
        verify_package_ref(&package.package, package_bytes)
    }
}

fn package_ref(
    stream_id: &str,
    seq: u64,
    schema_version: u32,
    package_bytes: &[u8],
) -> Result<StorePackageRef, StoreProtocolError> {
    let changeset_size =
        u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
    let content_hash = ObjectHash::digest(package_bytes);
    Ok(StorePackageRef {
        object_key: package_semantic_prefix(stream_id, seq, content_hash),
        content_hash,
        schema_version,
        changeset_size,
    })
}

fn verify_package_ref(
    package: &StorePackageRef,
    package_bytes: &[u8],
) -> Result<(), StoreProtocolError> {
    let length =
        u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
    if length != package.changeset_size {
        return Err(StoreProtocolError::PackageLengthMismatch {
            expected: package.changeset_size,
            actual: length,
        });
    }
    let actual = ObjectHash::digest(package_bytes);
    if actual != package.content_hash {
        return Err(StoreProtocolError::PackageHashMismatch {
            expected: package.content_hash,
            actual,
        });
    }
    Ok(())
}

fn validate_control(
    policy: WritePolicy,
    store_root_hash: ObjectHash,
    author_pubkey: &str,
    control: Option<&StoreControl>,
) -> Result<(), StoreProtocolError> {
    let Some(control) = control else {
        return Ok(());
    };
    if policy != WritePolicy::Serial {
        return Err(StoreProtocolError::ControlRequiresSerial);
    }
    let entry = control.serial_membership_entry();
    if entry.store_root_hash != store_root_hash
        || entry.author_pubkey != author_pubkey
        || !entry.verify()
    {
        return Err(StoreProtocolError::InvalidSerialControl);
    }
    if control.key_generation() == Some(0) {
        return Err(StoreProtocolError::InvalidKeyGeneration(0));
    }
    Ok(())
}

fn validate_parsed_control(commit: &StoreBatchCommit) -> Result<(), StoreProtocolError> {
    validate_control(
        commit.policy(),
        commit.store_root_hash,
        &commit.author_pubkey,
        commit.control.as_ref(),
    )
}

fn validate_circle_control_coord(
    policy: WritePolicy,
    coord: &CircleControlCoord,
) -> Result<(), StoreProtocolError> {
    coord
        .validate()
        .map_err(|_| StoreProtocolError::InvalidCircleControlCoord)?;
    let actual = match coord {
        CircleControlCoord::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
        CircleControlCoord::Serial { .. } => WritePolicy::Serial,
    };
    if policy != actual {
        return Err(StoreProtocolError::CircleControlPolicyMismatch {
            expected: policy,
            actual,
        });
    }
    Ok(())
}

fn validate_device_registration_refs(
    registrations: &[StoreDeviceRegistrationRef],
) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for registration in registrations {
        validate_device_id(&registration.device_id)?;
        if registration.revision == 0 {
            return Err(StoreProtocolError::InvalidRevision(0));
        }
        if !seen.insert((registration.device_id.as_str(), registration.revision)) {
            return Err(StoreProtocolError::DuplicateDeviceRegistration {
                device_id: registration.device_id.clone(),
                revision: registration.revision,
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub device_id: String,
    pub author_pubkey: String,
    pub position: Option<CommitPosition>,
    pub published_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct HeadSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    position: Option<&'a CommitPosition>,
    published_at: &'a str,
}

impl StoreDeviceHead {
    pub fn signed(
        store_root_hash: ObjectHash,
        device_id: String,
        position: Option<CommitPosition>,
        published_at: String,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_device_id(&device_id)?;
        let author_pubkey = keys::public_key_hex(signer);
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
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
                store_root_hash: self.store_root_hash,
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
        expected_store_root_hash: ObjectHash,
        expected_device: &str,
        expected_seq: u64,
    ) -> Result<Self, StoreProtocolError> {
        let head: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(head.version)?;
        if head.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: head.store_root_hash,
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

/// Signed global activation point for a Serial Store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSerialHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub commit: Option<CommitPosition>,
    pub tip_write_id: Option<WriteId>,
    pub author_pubkey: String,
    pub signature: String,
}

#[derive(Serialize)]
struct SerialHeadSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    commit: Option<&'a CommitPosition>,
    tip_write_id: Option<&'a WriteId>,
    author_pubkey: &'a str,
}

impl StoreSerialHead {
    pub fn signed(
        store_root_hash: ObjectHash,
        commit: Option<CommitPosition>,
        tip_write_id: Option<WriteId>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        if commit.is_some() != tip_write_id.is_some() {
            return Err(StoreProtocolError::InvalidSerialHead);
        }
        if commit.as_ref().is_some_and(|position| position.seq == 0) {
            return Err(StoreProtocolError::InvalidSequence(0));
        }
        let author_pubkey = keys::public_key_hex(signer);
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            commit,
            tip_write_id,
            author_pubkey,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &head.canonical_signed_bytes());
        head.signature = signature;
        Ok(head)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            SERIAL_HEAD_DOMAIN,
            &SerialHeadSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                commit: self.commit.as_ref(),
                tip_write_id: self.tip_write_id.as_ref(),
                author_pubkey: &self.author_pubkey,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreSerialHead serialization cannot fail")
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn parse(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
    ) -> Result<Self, StoreProtocolError> {
        let head: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(head.version)?;
        if head.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: head.store_root_hash,
            });
        }
        if head.commit.is_some() != head.tip_write_id.is_some() {
            return Err(StoreProtocolError::InvalidSerialHead);
        }
        if head
            .commit
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
    pub store_root_hash: ObjectHash,
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
    store_root_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    revision: u64,
    previous_registration_hash: Option<ObjectHash>,
    state: StoreDeviceRegistrationState,
}

impl StoreDeviceRegistration {
    pub fn signed(
        store_root_hash: ObjectHash,
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
            store_root_hash,
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
                store_root_hash: self.store_root_hash,
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
        expected_store_root_hash: ObjectHash,
        expected_device: &str,
        expected_revision: u64,
    ) -> Result<Self, StoreProtocolError> {
        let registration: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(registration.version)?;
        if registration.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: registration.store_root_hash,
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
    pub store_root_hash: ObjectHash,
    pub device_id: String,
    pub author_pubkey: String,
    pub revision: u64,
    pub previous_ack_hash: Option<ObjectHash>,
    pub frontier: CommitFrontier,
    pub last_sync: String,
    pub signature: String,
}

#[derive(Serialize)]
struct AckSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    device_id: &'a str,
    author_pubkey: &'a str,
    revision: u64,
    previous_ack_hash: Option<ObjectHash>,
    frontier: &'a CommitFrontier,
    last_sync: &'a str,
}

impl StoreAck {
    pub fn signed(
        store_root_hash: ObjectHash,
        device_id: String,
        revision: u64,
        previous_ack_hash: Option<ObjectHash>,
        frontier: CommitFrontier,
        last_sync: String,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_device_id(&device_id)?;
        validate_chained_revision(revision, previous_ack_hash)?;
        validate_commit_frontier(&frontier)?;
        let author_pubkey = keys::public_key_hex(signer);
        let mut ack = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
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
                store_root_hash: self.store_root_hash,
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
        expected_store_root_hash: ObjectHash,
        expected_device: &str,
        expected_revision: u64,
    ) -> Result<Self, StoreProtocolError> {
        let ack: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(ack.version)?;
        if ack.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: ack.store_root_hash,
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
        validate_commit_frontier(&ack.frontier)?;
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
    pub store_root_hash: ObjectHash,
    pub author_pubkey: String,
    pub image_hash: ObjectHash,
    pub coverage: CommitFrontier,
    pub schema_version: u32,
    pub created_at: String,
    pub signature: String,
}

#[derive(Serialize)]
struct SnapshotSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    author_pubkey: &'a str,
    image_hash: ObjectHash,
    coverage: &'a CommitFrontier,
    schema_version: u32,
    created_at: &'a str,
}

impl SnapshotMeta {
    pub fn signed(
        store_root_hash: ObjectHash,
        image_hash: ObjectHash,
        coverage: CommitFrontier,
        schema_version: u32,
        created_at: String,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_frontier(&coverage)?;
        let author_pubkey = keys::public_key_hex(signer);
        let mut meta = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
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
                store_root_hash: self.store_root_hash,
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
        expected_store_root_hash: ObjectHash,
        expected_author: &str,
        expected_hash: ObjectHash,
    ) -> Result<Self, StoreProtocolError> {
        let meta: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(meta.version)?;
        if meta.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: meta.store_root_hash,
            });
        }
        if meta.author_pubkey != expected_author {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: snapshot_semantic_prefix(expected_author, expected_hash),
                actual: snapshot_semantic_prefix(&meta.author_pubkey, meta.snapshot_hash()),
            });
        }
        validate_device_id(&meta.author_pubkey)?;
        validate_commit_frontier(&meta.coverage)?;
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
pub struct StoreProtocolRoot {
    pub version: u32,
    pub store_id: String,
    pub founder: MembershipEntry,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
    pub write_policy: WritePolicy,
    pub author_pubkey: String,
    pub signature: String,
}

#[derive(Serialize)]
struct StoreProtocolRootSignedFields<'a> {
    version: u32,
    store_id: &'a str,
    founder: &'a MembershipEntry,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    write_policy: WritePolicy,
    author_pubkey: &'a str,
}

impl StoreProtocolRoot {
    pub fn signed(
        store_id: String,
        founder: MembershipEntry,
        schema_version: u32,
        sync_routing_hash: ObjectHash,
        write_policy: WritePolicy,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let author_pubkey = keys::public_key_hex(signer);
        let mut store_protocol_root = Self {
            version: STORE_PROTOCOL_VERSION,
            store_id,
            founder,
            schema_version,
            sync_routing_hash,
            write_policy,
            author_pubkey,
            signature: String::new(),
        };
        store_protocol_root.validate_founder()?;
        let (_, signature) = keys::sign_hex(signer, &store_protocol_root.canonical_signed_bytes());
        store_protocol_root.signature = signature;
        Ok(store_protocol_root)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            STORE_PROTOCOL_ROOT_DOMAIN,
            &StoreProtocolRootSignedFields {
                version: self.version,
                store_id: &self.store_id,
                founder: &self.founder,
                schema_version: self.schema_version,
                sync_routing_hash: self.sync_routing_hash,
                write_policy: self.write_policy,
                author_pubkey: &self.author_pubkey,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreProtocolRoot serialization cannot fail")
    }

    pub fn object_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.to_bytes())
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, StoreProtocolError> {
        let store_protocol_root: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(store_protocol_root.version)?;
        store_protocol_root.validate_founder()?;
        if !keys::verify_signature_hex(
            &store_protocol_root.author_pubkey,
            &store_protocol_root.signature,
            &store_protocol_root.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(store_protocol_root)
    }

    pub fn parse_expected(
        bytes: &[u8],
        expected_hash: ObjectHash,
        expected_store_id: &str,
        expected_founder: &str,
        expected_write_policy: WritePolicy,
        expected_sync_routing_hash: ObjectHash,
    ) -> Result<Self, StoreProtocolError> {
        let store_protocol_root =
            Self::parse_pinned(bytes, expected_hash, expected_store_id, expected_founder)?;
        if store_protocol_root.write_policy != expected_write_policy {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: expected_write_policy,
                actual: store_protocol_root.write_policy,
            });
        }
        if store_protocol_root.sync_routing_hash != expected_sync_routing_hash {
            return Err(StoreProtocolError::SyncRoutingMismatch {
                expected: expected_sync_routing_hash,
                actual: store_protocol_root.sync_routing_hash,
            });
        }
        Ok(store_protocol_root)
    }

    pub fn parse_pinned(
        bytes: &[u8],
        expected_hash: ObjectHash,
        expected_store_id: &str,
        expected_founder: &str,
    ) -> Result<Self, StoreProtocolError> {
        let store_protocol_root = Self::parse(bytes)?;
        let actual_hash = store_protocol_root.object_hash();
        if actual_hash != expected_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_hash,
                actual: actual_hash,
            });
        }
        if store_protocol_root.store_id != expected_store_id {
            return Err(StoreProtocolError::StoreMismatch {
                expected: expected_store_id.to_string(),
                actual: store_protocol_root.store_id,
            });
        }
        if store_protocol_root.author_pubkey != expected_founder {
            return Err(StoreProtocolError::FounderMismatch {
                expected: expected_founder.to_string(),
                actual: store_protocol_root.author_pubkey,
            });
        }
        Ok(store_protocol_root)
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
    #[error("Store protocol root hash is {actual}, expected {expected}")]
    StoreRootMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store id is {actual:?}, expected {expected:?}")]
    StoreMismatch { expected: String, actual: String },
    #[error("founder is {actual:?}, expected {expected:?}")]
    FounderMismatch { expected: String, actual: String },
    #[error("store protocol root has an invalid founder membership entry")]
    InvalidFounder,
    #[error("Store write policy is {actual:?}, expected {expected:?}")]
    WritePolicyMismatch {
        expected: WritePolicy,
        actual: WritePolicy,
    },
    #[error("Store sync-routing hash is {actual}, expected {expected}")]
    SyncRoutingMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store controls require the Serial write policy")]
    ControlRequiresSerial,
    #[error("Store Serial control is invalid or signed by a different commit author")]
    InvalidSerialControl,
    #[error("Store batch has no Store package, circle package, or control")]
    EmptyBatch,
    #[error("Store batch has no Store package")]
    MissingStorePackage,
    #[error("Store batch repeats Store device registration {device_id:?} revision {revision}")]
    DuplicateDeviceRegistration { device_id: String, revision: u64 },
    #[error(
        "Store device registration {device_id:?} revision {revision} has hash {actual}, expected {expected}"
    )]
    DeviceRegistrationRefMismatch {
        device_id: String,
        revision: u64,
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store batch has no package for circle {0}")]
    MissingCirclePackage(CircleId),
    #[error("Store batch has more than one package for circle {0}")]
    DuplicateCirclePackage(CircleId),
    #[error("circle control coordinate is invalid")]
    InvalidCircleControlCoord,
    #[error("circle control uses {actual:?}, expected Store policy {expected:?}")]
    CircleControlPolicyMismatch {
        expected: WritePolicy,
        actual: WritePolicy,
    },
    #[error("circle {circle_id} package is at {actual:?}, expected {expected:?}")]
    RelocatedCirclePackage {
        circle_id: CircleId,
        expected: String,
        actual: String,
    },
    #[error("Store key generation must be positive, got {0}")]
    InvalidKeyGeneration(u64),
    #[error("Serial head commit and tip write id must either both be present or both be absent")]
    InvalidSerialHead,
    #[error("store protocol root store id is empty")]
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

pub fn parse_store_protocol_root_copy_key(
    path: &str,
) -> Result<(ObjectHash, CopyId), StoreProtocolError> {
    let Some(relative) = path.strip_prefix(STORE_PROTOCOL_ROOT_PREFIX) else {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    };
    let segments: Vec<&str> = relative.split('/').collect();
    if segments.len() != 3 || segments[1] != "copies" {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    Ok((
        segments[0]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        parse_copy_filename(segments[2], ".json", path)?,
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
    prefix: &str,
    extension: &str,
    allow_zero: bool,
) -> Result<HashedCopySlot, StoreProtocolError> {
    let Some(relative) = path.strip_prefix(prefix) else {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    };
    let segments: Vec<&str> = relative.split('/').collect();
    if segments.len() != 5 || segments[3] != "copies" {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    let owner = segments[0].to_string();
    validate_device_id(&owner)?;
    Ok(HashedCopySlot {
        owner,
        sequence: parse_decimal(segments[1], allow_zero, path)?,
        semantic_hash: segments[2]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        copy_id: parse_copy_filename(segments[4], extension, path)?,
    })
}

pub fn parse_package_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, STORE_PACKAGE_PREFIX, ".pkg", false)
}

pub fn parse_commit_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, STORE_COMMIT_PREFIX, ".json", false)
}

pub fn parse_head_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, STORE_HEAD_PREFIX, ".json", true)
}

pub fn parse_registration_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, STORE_DEVICE_REGISTRATION_PREFIX, ".json", false)
}

pub fn parse_ack_copy_key(path: &str) -> Result<HashedCopySlot, StoreProtocolError> {
    parse_hashed_copy_slot(path, STORE_ACK_PREFIX, ".json", false)
}

fn parse_membership_copy_key(
    path: &str,
    prefix: &str,
) -> Result<MembershipCopySlot, StoreProtocolError> {
    let Some(relative) = path.strip_prefix(prefix) else {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    };
    let segments: Vec<&str> = relative.split('/').collect();
    if segments.len() != 6 || segments[4] != "copies" {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    validate_device_id(segments[0])?;
    Ok(MembershipCopySlot {
        author: segments[0].to_string(),
        author_owner_grant: OwnerGrantId(
            segments[1]
                .parse()
                .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        ),
        sequence: parse_decimal(segments[2], false, path)?,
        semantic_hash: segments[3]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        copy_id: parse_copy_filename(segments[5], ".json", path)?,
    })
}

pub fn parse_membership_entry_copy_key(
    path: &str,
) -> Result<MembershipCopySlot, StoreProtocolError> {
    parse_membership_copy_key(path, STORE_MEMBERSHIP_ENTRY_PREFIX)
}

pub fn parse_membership_head_copy_key(
    path: &str,
) -> Result<MembershipCopySlot, StoreProtocolError> {
    parse_membership_copy_key(path, STORE_MEMBERSHIP_HEAD_PREFIX)
}

fn parse_snapshot_copy_key(
    path: &str,
    prefix: &str,
    extension: &str,
) -> Result<SnapshotCopySlot, StoreProtocolError> {
    let Some(relative) = path.strip_prefix(prefix) else {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    };
    let segments: Vec<&str> = relative.split('/').collect();
    if segments.len() != 4 || segments[2] != "copies" {
        return Err(StoreProtocolError::MalformedPath(path.to_string()));
    }
    validate_device_id(segments[0])?;
    Ok(SnapshotCopySlot {
        author: segments[0].to_string(),
        semantic_hash: segments[1]
            .parse()
            .map_err(|_| StoreProtocolError::MalformedPath(path.to_string()))?,
        copy_id: parse_copy_filename(segments[3], extension, path)?,
    })
}

pub fn parse_snapshot_meta_copy_key(path: &str) -> Result<SnapshotCopySlot, StoreProtocolError> {
    parse_snapshot_copy_key(path, STORE_SNAPSHOT_META_PREFIX, ".json")
}

pub fn parse_snapshot_image_copy_key(path: &str) -> Result<SnapshotCopySlot, StoreProtocolError> {
    parse_snapshot_copy_key(path, STORE_SNAPSHOT_IMAGE_PREFIX, ".db")
}

pub fn protocol_prefix() -> &'static str {
    STORE_PROTOCOL_PREFIX
}

pub fn serial_head_key() -> &'static str {
    STORE_SERIAL_HEAD_KEY
}

pub fn store_protocol_root_semantic_prefix(store_root_hash: ObjectHash) -> String {
    format!("{STORE_PROTOCOL_ROOT_PREFIX}{store_root_hash}")
}

pub fn store_protocol_root_copy_key(store_root_hash: ObjectHash, copy_id: CopyId) -> String {
    format!(
        "{}/copies/{copy_id}.json",
        store_protocol_root_semantic_prefix(store_root_hash)
    )
}

pub fn package_semantic_prefix(device_id: &str, seq: u64, package_hash: ObjectHash) -> String {
    format!("{STORE_PACKAGE_PREFIX}{device_id}/{seq}/{package_hash}")
}

pub fn circle_package_semantic_prefix(
    circle_id: CircleId,
    device_id: &str,
    seq: u64,
    package_hash: ObjectHash,
) -> String {
    format!("circles/{circle_id}/packages/{device_id}/{seq}/{package_hash}")
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
    format!("{STORE_COMMIT_PREFIX}{device_id}/{seq}")
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
    format!("{STORE_HEAD_PREFIX}{device_id}/{seq}")
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
    format!("{STORE_DEVICE_REGISTRATION_PREFIX}{device_id}/{revision}")
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
    format!("{STORE_ACK_PREFIX}{device_id}/{revision}")
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
    format!("{STORE_MEMBERSHIP_ENTRY_PREFIX}{author}/{author_owner_grant}/{seq}/{entry_hash}")
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
    format!("{STORE_MEMBERSHIP_HEAD_PREFIX}{author}/{author_owner_grant}/{seq}/{head_hash}")
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
    format!("{STORE_SNAPSHOT_IMAGE_PREFIX}{author}/{image_hash}")
}

pub fn snapshot_image_copy_key(author: &str, image_hash: ObjectHash, copy_id: CopyId) -> String {
    format!(
        "{}/copies/{copy_id}.db",
        snapshot_image_semantic_prefix(author, image_hash)
    )
}

pub fn snapshot_semantic_prefix(author: &str, snapshot_hash: ObjectHash) -> String {
    format!("{STORE_SNAPSHOT_META_PREFIX}{author}/{snapshot_hash}")
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

fn validate_commit_frontier(frontier: &CommitFrontier) -> Result<(), StoreProtocolError> {
    match frontier {
        CommitFrontier::MergeConcurrent(frontier) => validate_frontier(frontier),
        CommitFrontier::Serial(Some(position)) if position.seq == 0 => {
            Err(StoreProtocolError::InvalidSequence(0))
        }
        CommitFrontier::Serial(_) => Ok(()),
    }
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

    fn routing_hash() -> ObjectHash {
        ObjectHash::digest(b"test-sync-schema")
    }

    fn fixture() -> (UserKeypair, StoreProtocolRoot, StoreBatchCommit, Vec<u8>) {
        let signer = UserKeypair::generate();
        let founder = founder_entry("store-a", &signer, "0000000001000-0000-device-a");
        let store_protocol_root = StoreProtocolRoot::signed(
            "store-a".to_string(),
            founder,
            3,
            routing_hash(),
            WritePolicy::MergeConcurrent,
            &signer,
        )
        .expect("sign Store protocol root");
        let package = b"package".to_vec();
        let commit = StoreBatchCommit::signed(
            store_protocol_root.object_hash(),
            WriteId::from_generated("canonical-write".to_string()),
            "device-a".to_string(),
            StoreCommitOrder::MergeConcurrent {
                seq: 1,
                previous_commit_hash: None,
                dependencies: BTreeMap::from([(
                    "device-b".to_string(),
                    CommitPosition {
                        seq: 4,
                        commit_hash: ObjectHash::digest(b"device-b/4"),
                    },
                )]),
            },
            Some(MembershipCoord {
                author_pubkey: keys::public_key_hex(&signer),
                author_owner_grant: store_protocol_root.founder.author_owner_grant.clone(),
                seq: 1,
                entry_hash: crate::sync::membership::entry_hash(&store_protocol_root.founder),
            }),
            3,
            &package,
            &signer,
        )
        .expect("sign commit");
        (signer, store_protocol_root, commit, package)
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
        let (_, store_protocol_root, commit, package) = fixture();
        let bytes = commit.to_bytes();
        let parsed = StoreBatchCommit::parse_at(
            &bytes,
            store_protocol_root.object_hash(),
            WritePolicy::MergeConcurrent,
            "device-a",
            1,
        )
        .expect("parse commit");
        parsed
            .verify_store_package(&package)
            .expect("verify package");
        assert_eq!(parsed, commit);
        assert!(commit.canonical_signed_bytes().starts_with(COMMIT_DOMAIN));
    }

    #[test]
    fn commit_rejects_dependency_package_predecessor_and_slot_tamper() {
        let (_, store_protocol_root, commit, package) = fixture();
        let mut tampered = commit.clone();
        let StoreCommitOrder::MergeConcurrent { dependencies, .. } = &mut tampered.order else {
            panic!("fixture uses MergeConcurrent order")
        };
        dependencies.get_mut("device-b").unwrap().seq += 1;
        assert!(matches!(
            tampered.verify_at(
                store_protocol_root.object_hash(),
                WritePolicy::MergeConcurrent,
                "device-a",
                1
            ),
            Err(StoreProtocolError::InvalidSignature)
        ));

        let mut tampered = commit.clone();
        tampered
            .store_package
            .as_mut()
            .expect("fixture has Store package")
            .content_hash = ObjectHash::digest(b"different");
        assert!(matches!(
            tampered.verify_at(
                store_protocol_root.object_hash(),
                WritePolicy::MergeConcurrent,
                "device-a",
                1
            ),
            Err(StoreProtocolError::RelocatedPackage { .. })
        ));

        assert!(matches!(
            commit.verify_at(
                store_protocol_root.object_hash(),
                WritePolicy::MergeConcurrent,
                "device-a",
                2
            ),
            Err(StoreProtocolError::RelocatedSlot { .. })
        ));
        assert!(matches!(
            commit.verify_store_package(b"different"),
            Err(StoreProtocolError::PackageLengthMismatch { .. })
                | Err(StoreProtocolError::PackageHashMismatch { .. })
        ));
        commit.verify_store_package(&package).unwrap();
    }

    #[test]
    fn unknown_fields_and_versions_are_rejected() {
        let (_, store_protocol_root, commit, _) = fixture();
        let mut value = serde_json::to_value(&commit).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(StoreBatchCommit::parse_at(
            &serde_json::to_vec(&value).unwrap(),
            store_protocol_root.object_hash(),
            WritePolicy::MergeConcurrent,
            "device-a",
            1,
        )
        .is_err());

        let mut value = serde_json::to_value(&commit).unwrap();
        value["version"] = serde_json::json!(2);
        assert!(matches!(
            StoreBatchCommit::parse_at(
                &serde_json::to_vec(&value).unwrap(),
                store_protocol_root.object_hash(),
                WritePolicy::MergeConcurrent,
                "device-a",
                1,
            ),
            Err(StoreProtocolError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn store_protocol_root_embeds_and_authenticates_the_founder_entry() {
        let (_, store_protocol_root, _, _) = fixture();
        let bytes = store_protocol_root.to_bytes();
        let parsed = StoreProtocolRoot::parse_expected(
            &bytes,
            store_protocol_root.object_hash(),
            "store-a",
            &store_protocol_root.author_pubkey,
            WritePolicy::MergeConcurrent,
            routing_hash(),
        )
        .expect("parse exact Store protocol root");
        assert_eq!(parsed, store_protocol_root);
    }

    #[test]
    fn store_protocol_root_signs_the_required_write_policy() {
        let (_, store_protocol_root, _, _) = fixture();
        let value = serde_json::to_value(store_protocol_root).expect("serialize Store root");

        assert_eq!(
            value.get("write_policy"),
            Some(&serde_json::json!("merge_concurrent"))
        );
        assert!(
            value.get("sync_routing_hash").is_some(),
            "the signed Store root must bind the sync-routing contract"
        );
    }

    #[test]
    fn store_only_commit_uses_the_multi_audience_batch_shape() {
        let (_, _, commit, _) = fixture();
        let value = serde_json::to_value(commit).expect("serialize Store commit");

        assert!(value.get("package").is_none());
        assert!(value.get("store_package").is_some());
        assert_eq!(
            value.get("device_registrations"),
            Some(&serde_json::json!([]))
        );
        assert_eq!(value.get("circle_controls"), Some(&serde_json::json!([])));
        assert_eq!(value.get("circle_packages"), Some(&serde_json::json!([])));
    }

    #[test]
    fn device_registration_activation_is_signed_into_a_control_only_commit() {
        let (signer, root, _, _) = fixture();
        let registration = StoreDeviceRegistration::signed(
            root.object_hash(),
            "device-a".to_string(),
            1,
            None,
            StoreDeviceRegistrationState::Active,
            &signer,
        )
        .unwrap();
        let reference = StoreDeviceRegistrationRef::from_registration(&registration);
        let commit = StoreBatchCommit::signed_with_registrations(
            root.object_hash(),
            WriteId::from_generated("register-device-a".to_string()),
            "device-a".to_string(),
            StoreCommitOrder::MergeConcurrent {
                seq: 1,
                previous_commit_hash: None,
                dependencies: BTreeMap::new(),
            },
            Some(MembershipCoord {
                author_pubkey: keys::public_key_hex(&signer),
                author_owner_grant: root.founder.author_owner_grant.clone(),
                seq: 1,
                entry_hash: crate::sync::membership::entry_hash(&root.founder),
            }),
            vec![reference.clone()],
            &signer,
        )
        .unwrap();

        assert!(commit.store_package.is_none());
        assert_eq!(commit.device_registrations, vec![reference]);
        assert_eq!(
            StoreBatchCommit::parse_at(
                &commit.to_bytes(),
                root.object_hash(),
                WritePolicy::MergeConcurrent,
                "device-a",
                1,
            )
            .unwrap(),
            commit,
        );
    }

    #[test]
    fn serial_membership_and_rotation_are_authenticated_by_the_global_commit() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let founder = founder_entry("serial-control", &owner, "founder");
        let root = StoreProtocolRoot::signed(
            "serial-control".to_string(),
            founder.clone(),
            1,
            routing_hash(),
            WritePolicy::Serial,
            &owner,
        )
        .unwrap();
        let state = crate::sync::membership::SerialMembershipState::from_founder(
            root.object_hash(),
            &founder,
        )
        .unwrap();
        let entry = state
            .signed_set_member(
                &owner,
                keys::public_key_hex(&member),
                None,
                crate::sync::membership::MemberRole::Member,
                "add".to_string(),
            )
            .unwrap();
        let commit = StoreBatchCommit::signed_with_control(
            root.object_hash(),
            WriteId::from_generated("serial-control-write".to_string()),
            "owner-device".to_string(),
            StoreCommitOrder::Serial {
                seq: 1,
                previous_commit_hash: None,
            },
            None,
            Some(StoreControl::SerialMembershipAndKeyRotation {
                entry: entry.clone(),
                generation: 2,
            }),
            1,
            &[],
            &owner,
        )
        .unwrap();
        let parsed = StoreBatchCommit::parse_at(
            &commit.to_bytes(),
            root.object_hash(),
            WritePolicy::Serial,
            SERIAL_STREAM_ID,
            1,
        )
        .unwrap();
        assert_eq!(parsed, commit);
        assert!(state
            .apply(parsed.control.unwrap().serial_membership_entry())
            .is_ok());

        assert!(matches!(
            StoreBatchCommit::signed_with_control(
                root.object_hash(),
                WriteId::from_generated("merge-control-write".to_string()),
                "owner-device".to_string(),
                StoreCommitOrder::MergeConcurrent {
                    seq: 1,
                    previous_commit_hash: None,
                    dependencies: BTreeMap::new(),
                },
                None,
                Some(StoreControl::SerialMembership { entry }),
                1,
                &[],
                &owner,
            ),
            Err(StoreProtocolError::ControlRequiresSerial)
        ));
    }
}
