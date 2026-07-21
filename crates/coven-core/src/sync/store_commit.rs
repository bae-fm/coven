//! Signed, hash-addressed Store commit protocol objects.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use super::membership::{
    verify_membership_entry, AuthorHead, AuthorStreamId, MembershipChange, MembershipCoord,
    MembershipEntry, MembershipEntryRef, MembershipGrantCreationAuthority, MembershipGrantId,
    MembershipHeadRef, StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use super::storage::{ExactObjectRef, ProviderDeviceBinding};
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::ObjectSlot;
use crate::sync::circle::{
    AccessLeafId, CircleControlCoord, CircleEpochId, CircleId, CircleMetadataCoord,
    CircleMetadataHeadRef, CircleRosterConflictResolutionRef, CircleRosterCoord,
    CircleRosterHeadRef,
};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::KeyFingerprint;
use crate::{WriteId, WritePolicy};

mod ordered_map_entries {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<K, V, S>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        K: Ord + Serialize,
        V: Serialize,
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, K, V, D>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: Ord + Deserialize<'de>,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let entries = Vec::<(K, V)>::deserialize(deserializer)?;
        let entry_count = entries.len();
        let map = entries.into_iter().collect::<BTreeMap<_, _>>();
        if map.len() != entry_count {
            return Err(serde::de::Error::custom(
                "ordered map entries contain a duplicate key",
            ));
        }
        Ok(map)
    }
}

pub const STORE_PROTOCOL_VERSION: u32 = 1;

pub(crate) const STORE_PROTOCOL_PREFIX: &str = "store-v1/";
pub const STORE_PROTOCOL_ROOT_SEMANTIC_PATH: &str = "store-v1/store-protocol-root";
pub const STORE_PROTOCOL_ROOT_LOGICAL_KEY: &str = "store-v1/store-protocol-root.json";
pub(crate) const STORE_CANDIDATE_PREFIX: &str = "store-v1/candidates/";
pub(crate) const STORE_HEAD_PREFIX: &str = "store-v1/heads/";
pub(crate) const STORE_ACK_PREFIX: &str = "store-v1/acks/";
pub(crate) const STORE_DEVICE_REGISTRATION_PREFIX: &str = "store-v1/devices/";
pub(crate) const STORE_DEVICE_JOIN_ATTEMPT_PREFIX: &str = "store-v1/device-join-attempts/";
pub(crate) const STORE_DEVICE_JOIN_OUTCOME_PREFIX: &str = "store-v1/device-join-outcomes/";
pub(crate) const STORE_DEVICE_JOIN_CLEANUP_RECEIPT_PREFIX: &str =
    "store-v1/device-join-cleanup-receipts/";
pub(crate) const STORE_DEVICE_EXCLUSION_PROPOSAL_PREFIX: &str =
    "store-v1/device-exclusion-proposals/";
pub(crate) const STORE_DEVICE_EXCLUSION_OUTCOME_PREFIX: &str =
    "store-v1/device-exclusion-outcomes/";
pub(crate) const STORE_PROVIDER_ACCESS_GRANT_PREFIX: &str = "store-v1/provider-access/grants/";
pub(crate) const STORE_PROVIDER_ACCESS_WITHDRAWAL_PREFIX: &str =
    "store-v1/provider-access/withdrawals/";
pub(crate) const STORE_OWNER_RECOVERY_PREFIX: &str = "store-v1/recovery/";
pub(crate) const STORE_SNAPSHOT_META_PREFIX: &str = "store-v1/snapshots/";
pub(crate) const STORE_SNAPSHOT_IMAGE_PREFIX: &str = "store-v1/snapshot-images/";
pub(crate) const STORE_MEMBERSHIP_ENTRY_PREFIX: &str = "store-v1/membership/entries/";
pub(crate) const STORE_MEMBERSHIP_HEAD_PREFIX: &str = "store-v1/membership/heads/";
const STORE_SERIAL_HEAD_KEY: &str = "store-v1/heads/serial.json";

const STORE_PROTOCOL_ROOT_DOMAIN: &[u8] = b"coven.store-protocol-root.v1\0";
const COMMIT_DOMAIN: &[u8] = b"coven.store-batch-commit.v1\0";
const HEAD_DOMAIN: &[u8] = b"coven.store-device-head.v1\0";
const MERGE_HISTORY_SUMMARY_DOMAIN: &[u8] = b"coven.retained-merge-history-summary.v1\0";
const SERIAL_HEAD_DOMAIN: &[u8] = b"coven.store-serial-head.v1\0";
const REGISTRATION_DOMAIN: &[u8] = b"coven.store-device-registration.v1\0";
const SELF_RETIREMENT_DOMAIN: &[u8] = b"coven.store-device-self-retirement.v1\0";
const DEVICE_JOIN_ATTEMPT_DOMAIN: &[u8] = b"coven.device-join-attempt.v1\0";
const DEVICE_READINESS_DOMAIN: &[u8] = b"coven.device-readiness.v1\0";
const DEVICE_JOIN_OUTCOME_DOMAIN: &[u8] = b"coven.device-join-outcome.v1\0";
const DEVICE_EXCLUSION_PROPOSAL_DOMAIN: &[u8] = b"coven.store-device-exclusion-proposal.v1\0";
const DEVICE_EXCLUSION_DOMAIN: &[u8] = b"coven.store-device-exclusion.v1\0";
const DEVICE_EXCLUSION_CANCELLATION_DOMAIN: &[u8] =
    b"coven.store-device-exclusion-cancellation.v1\0";
const OWNER_RECOVERY_NODE_DOMAIN: &[u8] = b"coven.owner-recovery-node.v1\0";
const ACK_DOMAIN: &[u8] = b"coven.store-ack.v1\0";
const SNAPSHOT_DOMAIN: &[u8] = b"coven.snapshot-meta.v1\0";
const CANDIDATE_FAMILY_DOMAIN: &[u8] = b"coven.candidate-family.v1\0";
const STREAM_ACTIVATION_ID_DOMAIN: &[u8] = b"coven.stream-activation-id.v1\0";
const AUTHOR_STREAM_ID_DOMAIN: &[u8] = b"coven.author-stream-id.v1\0";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectHash([u8; 32]);

impl ObjectHash {
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub(crate) fn from_digest(bytes: [u8; 32]) -> Self {
        Self(bytes)
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

/// Closed coordinate of one Store commit under the Store's signed policy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitCoord {
    MergeConcurrent {
        stream_id: AuthorStreamId,
        sequence: u64,
    },
    Serial {
        sequence: u64,
    },
}

impl StoreCommitCoord {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::MergeConcurrent { sequence, .. } | Self::Serial { sequence } => *sequence,
        }
    }

    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial { .. } => WritePolicy::Serial,
        }
    }

    pub fn validate(&self) -> Result<(), StoreProtocolError> {
        if self.sequence() == 0 {
            return Err(StoreProtocolError::Malformed(
                "Store commit coordinate uses sequence zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Domain-separated family shared by replacements at one competition point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CandidateFamilyId(ObjectHash);

impl CandidateFamilyId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }

    pub fn as_hash(self) -> ObjectHash {
        self.0
    }

    pub fn derive(
        store_root_hash: ObjectHash,
        author_registration: &StoreDeviceRegistrationRef,
        write_id: &WriteId,
        order: &StoreCommitOrder,
    ) -> Self {
        #[derive(Serialize)]
        struct Fields<'a> {
            store_root_hash: ObjectHash,
            author_registration: &'a StoreDeviceRegistrationRef,
            write_id: &'a WriteId,
            policy: WritePolicy,
            sequence: u64,
            predecessor: CandidateFamilyPredecessor<'a>,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "snake_case")]
        enum CandidateFamilyPredecessor<'a> {
            Merge(Option<&'a StoreBatchCommitRef>),
            SerialGenesis {
                root: &'a StoreRootRef,
                founder_registration: &'a StoreDeviceRegistrationRef,
            },
            SerialCommit(&'a StoreBatchCommitRef),
        }

        let predecessor = match order {
            StoreCommitOrder::MergeConcurrent { predecessor, .. } => {
                CandidateFamilyPredecessor::Merge(predecessor.as_ref())
            }
            StoreCommitOrder::Serial {
                predecessor:
                    StoreSerialPredecessor::Genesis {
                        root,
                        founder_registration,
                    },
                ..
            } => CandidateFamilyPredecessor::SerialGenesis {
                root,
                founder_registration,
            },
            StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => CandidateFamilyPredecessor::SerialCommit(predecessor),
        };
        let fields = Fields {
            store_root_hash,
            author_registration,
            write_id,
            policy: order.policy(),
            sequence: order.seq(),
            predecessor,
        };
        Self(ObjectHash::digest(&domain_json(
            CANDIDATE_FAMILY_DOMAIN,
            &fields,
        )))
    }
}

/// Exact identity of one signed Store commit candidate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommitRef {
    pub coord: StoreCommitCoord,
    pub commit_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// Exact stored candidate commit retained as cleanup authority after abandonment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommitDeletionTarget {
    pub coord: StoreCommitCoord,
    pub object: ExactObjectRef,
    pub canonical_signed_bytes: Vec<u8>,
}

impl StoreBatchCommitDeletionTarget {
    pub(crate) fn verify_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        let commit = self.verify_exact_candidate(expected_store_root_hash, author)?;
        if matches!(
            &commit.body,
            StoreCommitBody::SerialRecoveryActivation { .. }
                | StoreCommitBody::AbandonCandidates { .. }
        ) {
            return Err(StoreProtocolError::Malformed(
                "retained authority cannot be a candidate cleanup target".to_string(),
            ));
        }
        Ok(commit)
    }

    pub(crate) fn verify_nonactivation_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        self.verify_exact_candidate(expected_store_root_hash, author)
    }

    fn verify_exact_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        self.object
            .verify(&self.canonical_signed_bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let commit: StoreBatchCommit = serde_json::from_slice(&self.canonical_signed_bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if commit.to_bytes() != self.canonical_signed_bytes {
            return Err(StoreProtocolError::Malformed(
                "candidate commit bytes are not canonical".to_string(),
            ));
        }
        commit.verify_at(expected_store_root_hash, &self.coord, author)?;
        StoreBatchCommitRef::from_commit(&commit, self.coord.clone(), self.object.clone())?;
        Ok(commit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateCleanupManifest {
    pub candidate: StoreBatchCommitDeletionTarget,
}

impl StoreBatchCommitRef {
    pub fn from_commit(
        commit: &StoreBatchCommit,
        coord: StoreCommitCoord,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        if coord.policy() != commit.policy() || coord.sequence() != commit.seq() {
            return Err(StoreProtocolError::Malformed(
                "Store commit reference coordinate differs from the signed commit".to_string(),
            ));
        }
        let reference = Self {
            coord,
            commit_hash: commit.commit_hash(),
            object,
        };
        reference.verify_commit(commit)?;
        Ok(reference)
    }

    pub fn verify_commit(&self, commit: &StoreBatchCommit) -> Result<(), StoreProtocolError> {
        if self.coord.policy() != commit.policy()
            || self.coord.sequence() != commit.seq()
            || self.commit_hash != commit.commit_hash()
        {
            return Err(StoreProtocolError::Malformed(
                "exact Store commit reference differs from the signed commit".to_string(),
            ));
        }
        let stream_id = commit_stream_id(&self.coord);
        let expected = format!(
            "{}.json",
            commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id,
                self.coord.sequence(),
                self.commit_hash,
            )
        );
        if self.object.slot().logical_key() != expected {
            return Err(StoreProtocolError::RelocatedSlot {
                expected,
                actual: self.object.slot().logical_key().to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreRootRef {
    pub store_root_id: ObjectHash,
    pub store_root_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamActivationId(ObjectHash);

impl StreamActivationId {
    pub(crate) fn from_digest(hash: ObjectHash) -> Self {
        Self(hash)
    }

    pub fn as_hash(self) -> ObjectHash {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamActivation {
    GrantAuthorized {
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        grant_id: MembershipGrantId,
        anchor: GrantStreamAnchor,
    },
    DeviceAuthorized {
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        anchor: DeviceStreamAnchor,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegisteredStreamActivation {
    activation: StreamActivation,
    activating_commit: StoreBatchCommitRef,
}

impl RegisteredStreamActivation {
    pub(crate) fn from_stored(
        stored_activation_id: StreamActivationId,
        stored_author_stream_id: AuthorStreamId,
        activation: StreamActivation,
        activating_commit: StoreBatchCommitRef,
    ) -> Result<Self, StoreProtocolError> {
        if activation.activation_id() != stored_activation_id {
            return Err(StoreProtocolError::Malformed(
                "stored stream activation id differs from its canonical descriptor".to_string(),
            ));
        }
        if activation.author_stream_id() != stored_author_stream_id {
            return Err(StoreProtocolError::Malformed(
                "stored author stream id differs from its canonical descriptor".to_string(),
            ));
        }
        Ok(Self {
            activation,
            activating_commit,
        })
    }

    pub(crate) fn activation(&self) -> &StreamActivation {
        &self.activation
    }

    pub(crate) fn activating_commit(&self) -> &StoreBatchCommitRef {
        &self.activating_commit
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamAnchorDomain {
    StoreMembership,
    OwnerRecovery,
    CircleControl { circle_id: CircleId },
    CircleRoster { circle_id: CircleId },
    CircleMetadata { circle_id: CircleId },
    StoreAnnouncements,
    StoreAcknowledgements,
    StoreSnapshots,
}

impl GrantStreamAnchor {
    fn domain(&self) -> StreamAnchorDomain {
        match self {
            Self::StoreMembership { .. } => StreamAnchorDomain::StoreMembership,
            Self::OwnerRecovery { .. } => StreamAnchorDomain::OwnerRecovery,
            Self::CircleControl { circle_id, .. } => StreamAnchorDomain::CircleControl {
                circle_id: *circle_id,
            },
            Self::CircleRoster { circle_id, .. } => StreamAnchorDomain::CircleRoster {
                circle_id: *circle_id,
            },
            Self::CircleMetadata { circle_id, .. } => StreamAnchorDomain::CircleMetadata {
                circle_id: *circle_id,
            },
        }
    }
}

impl DeviceStreamAnchor {
    fn domain(&self) -> StreamAnchorDomain {
        match self {
            Self::StoreAnnouncements { .. } => StreamAnchorDomain::StoreAnnouncements,
            Self::StoreAcknowledgements { .. } => StreamAnchorDomain::StoreAcknowledgements,
            Self::StoreSnapshots { .. } => StreamAnchorDomain::StoreSnapshots,
        }
    }
}

impl StreamActivation {
    pub fn grant_authorized(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        grant_id: MembershipGrantId,
        anchor: GrantStreamAnchor,
    ) -> Self {
        Self::GrantAuthorized {
            store_root_hash,
            author_registration,
            grant_id,
            anchor,
        }
    }

    pub fn device_authorized(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        anchor: DeviceStreamAnchor,
    ) -> Self {
        Self::DeviceAuthorized {
            store_root_hash,
            author_registration,
            anchor,
        }
    }

    pub fn activation_id(&self) -> StreamActivationId {
        StreamActivationId(ObjectHash::digest(&domain_json(
            STREAM_ACTIVATION_ID_DOMAIN,
            self,
        )))
    }

    pub fn author_stream_id(&self) -> AuthorStreamId {
        match self {
            Self::GrantAuthorized {
                store_root_hash,
                author_registration,
                grant_id,
                anchor,
            } => derive_grant_author_stream_id(
                *store_root_hash,
                author_registration,
                grant_id,
                anchor.domain(),
            ),
            Self::DeviceAuthorized {
                store_root_hash,
                author_registration,
                anchor,
            } => derive_device_author_stream_id(
                *store_root_hash,
                author_registration,
                anchor.domain(),
            ),
        }
    }

    pub(crate) fn device_authorized_stream_id(
        store_root_hash: ObjectHash,
        author_registration: &StoreDeviceRegistrationRef,
        domain: StreamAnchorDomain,
    ) -> AuthorStreamId {
        derive_device_author_stream_id(store_root_hash, author_registration, domain)
    }

    pub(crate) fn grant_authorized_stream_id(
        store_root_hash: ObjectHash,
        author_registration: &StoreDeviceRegistrationRef,
        grant_id: &MembershipGrantId,
        domain: StreamAnchorDomain,
    ) -> AuthorStreamId {
        derive_grant_author_stream_id(store_root_hash, author_registration, grant_id, domain)
    }

    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::GrantAuthorized { anchor, .. } => anchor.first_slot(),
            Self::DeviceAuthorized { anchor, .. } => anchor.first_slot(),
        }
    }

    pub fn author_registration(&self) -> &StoreDeviceRegistrationRef {
        match self {
            Self::GrantAuthorized {
                author_registration,
                ..
            }
            | Self::DeviceAuthorized {
                author_registration,
                ..
            } => author_registration,
        }
    }

    pub fn store_root_hash(&self) -> ObjectHash {
        match self {
            Self::GrantAuthorized {
                store_root_hash, ..
            }
            | Self::DeviceAuthorized {
                store_root_hash, ..
            } => *store_root_hash,
        }
    }
}

#[derive(Serialize)]
struct GrantAuthorStreamFields<'a> {
    store_root_hash: ObjectHash,
    domain: StreamAnchorDomain,
    author_registration: &'a StoreDeviceRegistrationRef,
    grant_id: &'a MembershipGrantId,
}

#[derive(Serialize)]
struct DeviceAuthorStreamFields<'a> {
    store_root_hash: ObjectHash,
    domain: StreamAnchorDomain,
    author_registration: &'a StoreDeviceRegistrationRef,
}

fn derive_grant_author_stream_id(
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    grant_id: &MembershipGrantId,
    domain: StreamAnchorDomain,
) -> AuthorStreamId {
    derive_author_stream_id(&GrantAuthorStreamFields {
        store_root_hash,
        domain,
        author_registration,
        grant_id,
    })
}

fn derive_device_author_stream_id(
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    domain: StreamAnchorDomain,
) -> AuthorStreamId {
    derive_author_stream_id(&DeviceAuthorStreamFields {
        store_root_hash,
        domain,
        author_registration,
    })
}

fn derive_author_stream_id(fields: &impl Serialize) -> AuthorStreamId {
    AuthorStreamId::from_digest(ObjectHash::digest(&domain_json(
        AUTHOR_STREAM_ID_DOMAIN,
        fields,
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorLink {
    pub activation: StreamActivationId,
    pub predecessor: Option<ExactObjectRef>,
    pub next_slot: ObjectSlot,
}

/// Exact materialized cut, shaped by the Store's signed write policy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitFrontier {
    MergeConcurrent(BTreeMap<AuthorStreamId, StoreBatchCommitRef>),
    Serial(Option<StoreBatchCommitRef>),
}

/// Exact Store history cut, including the signed Serial genesis authority.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreHistoryCut {
    MergeConcurrent(BTreeMap<AuthorStreamId, StoreBatchCommitRef>),
    Serial(StoreSerialPredecessor),
}

impl StoreHistoryCut {
    pub fn merge_concurrent(commits: BTreeMap<AuthorStreamId, StoreBatchCommitRef>) -> Self {
        Self::MergeConcurrent(commits)
    }

    pub fn serial(predecessor: StoreSerialPredecessor) -> Self {
        Self::Serial(predecessor)
    }

    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent(_) => WritePolicy::MergeConcurrent,
            Self::Serial(_) => WritePolicy::Serial,
        }
    }

    pub fn position_count(&self) -> usize {
        match self {
            Self::MergeConcurrent(commits) => commits.len(),
            Self::Serial(_) => 1,
        }
    }

    pub fn serial_predecessor(&self) -> Option<&StoreSerialPredecessor> {
        match self {
            Self::Serial(predecessor) => Some(predecessor),
            Self::MergeConcurrent(_) => None,
        }
    }

    pub fn frontier(&self) -> CommitFrontier {
        match self {
            Self::MergeConcurrent(commits) => CommitFrontier::MergeConcurrent(commits.clone()),
            Self::Serial(StoreSerialPredecessor::Genesis { .. }) => CommitFrontier::Serial(None),
            Self::Serial(StoreSerialPredecessor::Commit(commit)) => {
                CommitFrontier::Serial(Some(commit.clone()))
            }
        }
    }

    pub(crate) fn join(self, other: Self) -> Result<Self, StoreProtocolError> {
        merge_history_cuts(self, other)
    }
}

impl CommitFrontier {
    pub fn from_refs(
        policy: WritePolicy,
        mut commits: BTreeMap<String, StoreBatchCommitRef>,
    ) -> Result<Self, StoreProtocolError> {
        match policy {
            WritePolicy::MergeConcurrent => commits
                .into_iter()
                .map(|(stream_id, commit)| {
                    let stream_id = stream_id.parse().map_err(StoreProtocolError::Malformed)?;
                    Ok((stream_id, commit))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map(Self::MergeConcurrent),
            WritePolicy::Serial => {
                let commit = commits.remove(SERIAL_STREAM_ID);
                if !commits.is_empty() {
                    return Err(StoreProtocolError::Malformed(format!(
                        "Serial frontier contains non-serial streams: {:?}",
                        commits.keys().collect::<Vec<_>>()
                    )));
                }
                if commit.as_ref().is_some_and(|reference| {
                    !matches!(reference.coord, StoreCommitCoord::Serial { .. })
                }) {
                    return Err(StoreProtocolError::Malformed(
                        "Serial frontier contains a Merge commit".to_string(),
                    ));
                }
                Ok(Self::Serial(commit))
            }
        }
    }

    pub fn into_refs(self) -> BTreeMap<String, StoreBatchCommitRef> {
        match self {
            Self::MergeConcurrent(commits) => commits
                .into_iter()
                .map(|(stream_id, commit)| (stream_id.to_string(), commit))
                .collect(),
            Self::Serial(Some(commit)) => BTreeMap::from([(SERIAL_STREAM_ID.to_string(), commit)]),
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

    pub fn covers(&self, covered: &Self) -> bool {
        match (self, covered) {
            (Self::MergeConcurrent(current), Self::MergeConcurrent(covered)) => {
                covered.iter().all(|(stream, covered_ref)| {
                    current.get(stream).is_some_and(|current_ref| {
                        current_ref.coord.sequence() > covered_ref.coord.sequence()
                            || current_ref.coord.sequence() == covered_ref.coord.sequence()
                                && current_ref == covered_ref
                    })
                })
            }
            (Self::Serial(current), Self::Serial(covered)) => match (current, covered) {
                (_, None) => true,
                (Some(current), Some(covered)) => {
                    current.coord.sequence() > covered.coord.sequence()
                        || current.coord.sequence() == covered.coord.sequence()
                            && current == covered
                }
                (None, Some(_)) => false,
            },
            _ => false,
        }
    }

    pub fn merge_commits(
        &self,
    ) -> Result<&BTreeMap<AuthorStreamId, StoreBatchCommitRef>, StoreProtocolError> {
        match self {
            Self::MergeConcurrent(commits) => Ok(commits),
            Self::Serial(_) => Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            }),
        }
    }

    pub fn serial_commit(&self) -> Result<Option<&StoreBatchCommitRef>, StoreProtocolError> {
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitOrder {
    MergeConcurrent {
        seq: u64,
        predecessor: Option<StoreBatchCommitRef>,
        dependencies: BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
    },
    Serial {
        seq: u64,
        predecessor: SerialStorePosition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SerialStorePosition {
    Genesis {
        root: StoreRootRef,
        founder_registration: StoreDeviceRegistrationRef,
    },
    Commit(StoreBatchCommitRef),
}

pub type StoreSerialPredecessor = SerialStorePosition;

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

    pub fn predecessor(&self) -> Option<&StoreBatchCommitRef> {
        match self {
            Self::MergeConcurrent { predecessor, .. } => predecessor.as_ref(),
            Self::Serial {
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => Some(predecessor),
            Self::Serial {
                predecessor: StoreSerialPredecessor::Genesis { .. },
                ..
            } => None,
        }
    }

    pub fn dependencies(&self) -> Option<&BTreeMap<AuthorStreamId, StoreBatchCommitRef>> {
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

    pub fn predecessor_cut(&self) -> Result<StoreHistoryCut, StoreProtocolError> {
        match self {
            Self::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            } => {
                let mut cut = dependencies.clone();
                if let Some(predecessor) = predecessor {
                    let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord
                    else {
                        return Err(StoreProtocolError::JoinAttemptMismatch);
                    };
                    if cut
                        .insert(stream_id, predecessor.clone())
                        .is_some_and(|existing| existing != *predecessor)
                    {
                        return Err(StoreProtocolError::JoinAttemptMismatch);
                    }
                }
                Ok(StoreHistoryCut::MergeConcurrent(cut))
            }
            Self::Serial { predecessor, .. } => Ok(StoreHistoryCut::Serial(predecessor.clone())),
        }
    }
}

pub const SERIAL_STREAM_ID: &str = "serial";

fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    match coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorePackageRef {
    pub candidate_family: CandidateFamilyId,
    pub content_hash: ObjectHash,
    pub schema_version: u32,
    pub changeset_size: u64,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CirclePackageRef {
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub package: StorePackageRef,
    pub key_fingerprint: KeyFingerprint,
}

/// Exact recipient-visible access envelope paired with its sealed leaf.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessEnvelopeObjectRef {
    pub owner_pubkey: String,
    pub recipient_slot: String,
    pub control_hash: ObjectHash,
    pub leaf_id: AccessLeafId,
    pub leaf_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// Exact recipient-sealed access-leaf object named by a Store activation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessLeafObjectRef {
    pub owner_pubkey: String,
    pub epoch_id: CircleEpochId,
    pub recipient_slot: String,
    pub leaf_id: AccessLeafId,
    pub leaf_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAccessObjectRef {
    pub leaf: CircleAccessLeafObjectRef,
    pub envelope: CircleAccessEnvelopeObjectRef,
}

/// Exact Circle-metadata object and the epoch key that must open it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataObjectRef {
    pub key_fingerprint: KeyFingerprint,
    pub object: ExactObjectRef,
}

/// Closed exact object graph needed to verify one Store-activated Circle control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleActivationObjects {
    pub control: ExactObjectRef,
    #[serde(with = "ordered_map_entries")]
    pub roster_entries: BTreeMap<CircleRosterCoord, ExactObjectRef>,
    pub roster_heads: Vec<CircleRosterHeadRef>,
    #[serde(with = "ordered_map_entries")]
    pub roster_resolutions: BTreeMap<CircleRosterConflictResolutionRef, ExactObjectRef>,
    #[serde(with = "ordered_map_entries")]
    pub metadata_entries: BTreeMap<CircleMetadataCoord, CircleMetadataObjectRef>,
    pub metadata_heads: Vec<CircleMetadataHeadRef>,
    pub access: Vec<CircleAccessObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleControlRef {
    MergeConcurrent {
        circle_id: CircleId,
        control: CircleControlCoord,
        head_hash: ObjectHash,
        head_object: ExactObjectRef,
        objects: CircleActivationObjects,
    },
    Serial {
        circle_id: CircleId,
        control: CircleControlCoord,
        objects: CircleActivationObjects,
    },
}

impl CircleControlRef {
    pub fn circle_id(&self) -> CircleId {
        match self {
            Self::MergeConcurrent { circle_id, .. } | Self::Serial { circle_id, .. } => *circle_id,
        }
    }

    pub fn control(&self) -> &CircleControlCoord {
        match self {
            Self::MergeConcurrent { control, .. } | Self::Serial { control, .. } => control,
        }
    }

    pub fn head_hash(&self) -> Option<ObjectHash> {
        match self {
            Self::MergeConcurrent { head_hash, .. } => Some(*head_hash),
            Self::Serial { .. } => None,
        }
    }

    pub fn head_object(&self) -> Option<&ExactObjectRef> {
        match self {
            Self::MergeConcurrent { head_object, .. } => Some(head_object),
            Self::Serial { .. } => None,
        }
    }

    pub fn objects(&self) -> &CircleActivationObjects {
        match self {
            Self::MergeConcurrent { objects, .. } | Self::Serial { objects, .. } => objects,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRegistrationRef {
    pub device_id: StoreDeviceId,
    pub registration_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl StoreDeviceRegistrationRef {
    pub fn from_registration(
        registration: &StoreDeviceRegistration,
        object: ExactObjectRef,
    ) -> Self {
        Self {
            device_id: registration.device_id,
            registration_hash: registration.registration_hash(),
            object,
        }
    }

    pub fn verify_registration(
        &self,
        registration: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        if registration.device_id != self.device_id
            || registration.registration_hash() != self.registration_hash
        {
            return Err(StoreProtocolError::DeviceRegistrationRefMismatch {
                device_id: self.device_id.to_string(),
                revision: 1,
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
    pub candidate_family: CandidateFamilyId,
    pub schema_version: u32,
    pub bytes: &'a [u8],
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreControl {
    MergeMembership {
        transition: super::membership::MergeMembershipHeadTransition,
    },
    SerialMembership {
        entry: super::membership::SerialMembershipEntry,
    },
    SerialMembershipAndKeyRotation {
        entry: super::membership::SerialMembershipEntry,
        generation: u64,
        wrapped_keys: Vec<super::wrapped_store_key::WrappedStoreKeyRef>,
    },
    ProviderAdmin {
        change: super::provider::ProviderAdminChange,
    },
}

/// Exact Recovery activation carried by the first Serial commit authored by
/// the replacement device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerialRecoveryActivation {
    pub registration: ActivatedStoreDeviceRegistrationRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OwnerPromotionId(ObjectHash);

impl OwnerPromotionId {
    pub fn from_generated(value: String) -> Self {
        Self(ObjectHash::digest(
            &[
                b"coven.owner-promotion-id.v1\0".as_slice(),
                value.as_bytes(),
            ]
            .concat(),
        ))
    }
}

impl fmt::Display for OwnerPromotionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionFinalization {
    MergeConcurrent {
        author_stream: AuthorStreamId,
        seq: u64,
        previous_hash: Option<ObjectHash>,
    },
    Serial,
}

impl OwnerPromotionFinalization {
    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial => WritePolicy::Serial,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionRequest {
    pub version: u32,
    pub promotion_id: OwnerPromotionId,
    pub store_root_hash: ObjectHash,
    pub promoter_registration: StoreDeviceRegistrationRef,
    pub promoter_owner_grant: MembershipGrantId,
    pub member_pubkey: String,
    pub member_grant: MembershipGrantId,
    pub member_registration: StoreDeviceRegistrationRef,
    pub intended_owner_grant: MembershipGrantId,
    pub predecessor_membership: StoreMembershipStateRef,
    pub predecessor_devices: StoreDeviceStateRef,
    pub finalization: OwnerPromotionFinalization,
    pub signature: String,
}

impl OwnerPromotionRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        promotion_id: OwnerPromotionId,
        root: &StoreRootRef,
        promoter_registration: StoreDeviceRegistrationRef,
        promoter: &StoreDeviceRegistration,
        promoter_owner_grant: MembershipGrantId,
        member_pubkey: String,
        member_grant: MembershipGrantId,
        member_registration: StoreDeviceRegistrationRef,
        predecessor_membership: StoreMembershipStateRef,
        predecessor_devices: StoreDeviceStateRef,
        finalization: OwnerPromotionFinalization,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let intended_owner_grant =
            derive_owner_promotion_grant(root.store_root_hash, promotion_id, &member_pubkey);
        let mut request = Self {
            version: STORE_PROTOCOL_VERSION,
            promotion_id,
            store_root_hash: root.store_root_hash,
            promoter_registration,
            promoter_owner_grant,
            member_pubkey,
            member_grant,
            member_registration,
            intended_owner_grant,
            predecessor_membership,
            predecessor_devices,
            finalization,
            signature: String::new(),
        };
        request.validate_shape(root, promoter)?;
        let device_signer = promoter.device_signer(signer)?;
        request.signature = keys::sign_hex(&device_signer, &request.canonical_bytes()).1;
        Ok(request)
    }

    pub fn verify(
        &self,
        root: &StoreRootRef,
        promoter: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.validate_shape(root, promoter)?;
        if !keys::verify_signature_hex(
            &promoter.device_signing_pubkey,
            &self.signature,
            &self.canonical_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    fn validate_shape(
        &self,
        root: &StoreRootRef,
        promoter: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        self.promoter_registration.verify_registration(promoter)?;
        if self.store_root_hash != root.store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: root.store_root_hash,
                actual: self.store_root_hash,
            });
        }
        if promoter.store_root != *root
            || promoter.author_pubkey == self.member_pubkey
            || self.member_pubkey.is_empty()
            || self.predecessor_membership.write_policy() != self.finalization.policy()
            || self.predecessor_devices.write_policy() != self.finalization.policy()
            || self.intended_owner_grant
                != derive_owner_promotion_grant(
                    self.store_root_hash,
                    self.promotion_id,
                    &self.member_pubkey,
                )
            || matches!(
                self.finalization,
                OwnerPromotionFinalization::MergeConcurrent { seq: 0, .. }
            )
        {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        Ok(())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            version: u32,
            promotion_id: OwnerPromotionId,
            store_root_hash: ObjectHash,
            promoter_registration: &'a StoreDeviceRegistrationRef,
            promoter_owner_grant: &'a MembershipGrantId,
            member_pubkey: &'a str,
            member_grant: &'a MembershipGrantId,
            member_registration: &'a StoreDeviceRegistrationRef,
            intended_owner_grant: &'a MembershipGrantId,
            predecessor_membership: &'a StoreMembershipStateRef,
            predecessor_devices: &'a StoreDeviceStateRef,
            finalization: &'a OwnerPromotionFinalization,
        }
        domain_json(
            b"coven.owner-promotion-request.v1\0",
            &Signed {
                version: self.version,
                promotion_id: self.promotion_id,
                store_root_hash: self.store_root_hash,
                promoter_registration: &self.promoter_registration,
                promoter_owner_grant: &self.promoter_owner_grant,
                member_pubkey: &self.member_pubkey,
                member_grant: &self.member_grant,
                member_registration: &self.member_registration,
                intended_owner_grant: &self.intended_owner_grant,
                predecessor_membership: &self.predecessor_membership,
                predecessor_devices: &self.predecessor_devices,
                finalization: &self.finalization,
            },
        )
    }
}

pub fn derive_owner_promotion_grant(
    store_root_hash: ObjectHash,
    promotion_id: OwnerPromotionId,
    member_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(&domain_json(
        b"coven.owner-promotion-grant.v1\0",
        &(store_root_hash, promotion_id, member_pubkey),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionRequestActivation {
    MergeConcurrent {
        commit: StoreBatchCommitRef,
        head: StoreDeviceHeadRef,
    },
    Serial {
        commit: StoreBatchCommitRef,
    },
}

impl OwnerPromotionRequestActivation {
    pub fn commit(&self) -> &StoreBatchCommitRef {
        match self {
            Self::MergeConcurrent { commit, .. } | Self::Serial { commit } => commit,
        }
    }

    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial { .. } => WritePolicy::Serial,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionAnchors {
    MergeConcurrent {
        membership: GrantStreamAnchor,
        recovery: GrantStreamAnchor,
    },
    Serial {
        recovery: GrantStreamAnchor,
    },
}

impl OwnerPromotionAnchors {
    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial { .. } => WritePolicy::Serial,
        }
    }

    pub fn recovery(&self) -> &GrantStreamAnchor {
        match self {
            Self::MergeConcurrent { recovery, .. } | Self::Serial { recovery } => recovery,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionAcceptance {
    pub request: Box<OwnerPromotionRequest>,
    pub activation: OwnerPromotionRequestActivation,
    pub anchors: OwnerPromotionAnchors,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionStatus {
    Preparing {
        member_registration: StoreDeviceRegistrationRef,
    },
    RequestPending {
        request: OwnerPromotionRequest,
    },
    AwaitingAcceptance {
        request: OwnerPromotionRequest,
        activation: OwnerPromotionRequestActivation,
    },
    AcceptanceReady {
        acceptance: OwnerPromotionAcceptance,
    },
    FinalizationPending {
        acceptance: OwnerPromotionAcceptance,
    },
    Finalized {
        membership: StoreMembershipStateRef,
    },
    Nonactivated {
        request: OwnerPromotionRequest,
    },
    Stale {
        acceptance: OwnerPromotionAcceptance,
        reason: OwnerPromotionStaleReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionStaleReason {
    MergeFinalizationPointOccupied { winner: MembershipHeadRef },
    MergeActivationRejected,
    SerialHeadAdvanced { current: SerialStorePosition },
}

impl OwnerPromotionAcceptance {
    pub fn signed(
        request: OwnerPromotionRequest,
        activation: OwnerPromotionRequestActivation,
        anchors: OwnerPromotionAnchors,
        candidate: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let mut acceptance = Self {
            request: Box::new(request),
            activation,
            anchors,
            signature: String::new(),
        };
        acceptance.validate_shape(candidate)?;
        let device_signer = candidate.device_signer(signer)?;
        acceptance.signature = keys::sign_hex(&device_signer, &acceptance.canonical_bytes()).1;
        Ok(acceptance)
    }

    pub fn verify(&self, candidate: &StoreDeviceRegistration) -> Result<(), StoreProtocolError> {
        self.validate_shape(candidate)?;
        if !keys::verify_signature_hex(
            &candidate.device_signing_pubkey,
            &self.signature,
            &self.canonical_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    fn validate_shape(
        &self,
        candidate: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.request
            .member_registration
            .verify_registration(candidate)?;
        if candidate.store_root.store_root_hash != self.request.store_root_hash
            || candidate.author_pubkey != self.request.member_pubkey
            || self.activation.policy() != self.request.finalization.policy()
            || self.anchors.policy() != self.request.finalization.policy()
            || self.activation.commit().coord.policy() != self.request.finalization.policy()
            || !matches!(
                self.anchors.recovery(),
                GrantStreamAnchor::OwnerRecovery { .. }
            )
        {
            return Err(StoreProtocolError::OwnerPromotionMismatch);
        }
        match &self.anchors {
            OwnerPromotionAnchors::MergeConcurrent {
                membership,
                recovery,
            } => {
                if !matches!(membership, GrantStreamAnchor::StoreMembership { .. }) {
                    return Err(StoreProtocolError::OwnerPromotionMismatch);
                }
                let membership_stream = StreamActivation::grant_authorized_stream_id(
                    self.request.store_root_hash,
                    &self.request.member_registration,
                    &self.request.intended_owner_grant,
                    StreamAnchorDomain::StoreMembership,
                );
                let membership_key = format!(
                    "{}.json",
                    membership_head_slot_prefix(
                        &self.request.member_pubkey,
                        &self.request.intended_owner_grant,
                        membership_stream,
                        1,
                    )
                );
                let recovery_key = format!(
                    "{}.json",
                    owner_recovery_semantic_prefix(
                        &self.request.member_pubkey,
                        self.request.intended_owner_grant.clone(),
                        1,
                    )
                );
                if membership.first_slot().logical_key() != membership_key
                    || recovery.first_slot().logical_key() != recovery_key
                    || matches!(
                        (membership.first_slot().physical(), recovery.first_slot().physical()),
                        (
                            crate::storage::cloud::PhysicalObjectLocator::Opaque(left),
                            crate::storage::cloud::PhysicalObjectLocator::Opaque(right),
                        ) if left == right
                    )
                {
                    return Err(StoreProtocolError::OwnerPromotionMismatch);
                }
            }
            OwnerPromotionAnchors::Serial { recovery } => {
                let recovery_key = format!(
                    "{}.json",
                    owner_recovery_semantic_prefix(
                        &self.request.member_pubkey,
                        self.request.intended_owner_grant.clone(),
                        1,
                    )
                );
                if recovery.first_slot().logical_key() != recovery_key {
                    return Err(StoreProtocolError::OwnerPromotionMismatch);
                }
            }
        }
        Ok(())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        domain_json(
            b"coven.owner-promotion-acceptance.v1\0",
            &(&self.request, &self.activation, &self.anchors),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerConflictResolutionAcceptance {
    pub store_root_hash: ObjectHash,
    pub owner_grant: MembershipGrantId,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub provider: ProviderDeviceBinding,
    pub membership: GrantStreamAnchor,
    pub recovery: GrantStreamAnchor,
    pub device_state: StoreDeviceStateRef,
    pub signature: String,
}

impl OwnerConflictResolutionAcceptance {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        owner_grant: MembershipGrantId,
        owner_registration: StoreDeviceRegistrationRef,
        membership: GrantStreamAnchor,
        recovery: GrantStreamAnchor,
        device_state: StoreDeviceStateRef,
        registration: &StoreDeviceRegistration,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let mut acceptance = Self {
            store_root_hash,
            owner_grant,
            owner_registration,
            provider: registration.provider.clone(),
            membership,
            recovery,
            device_state,
            signature: String::new(),
        };
        acceptance.validate_shape(registration)?;
        let device_signer = registration.device_signer(signer)?;
        acceptance.signature = keys::sign_hex(&device_signer, &acceptance.canonical_bytes()).1;
        Ok(acceptance)
    }

    pub fn verify(&self, registration: &StoreDeviceRegistration) -> Result<(), StoreProtocolError> {
        self.validate_shape(registration)?;
        if !keys::verify_signature_hex(
            &registration.device_signing_pubkey,
            &self.signature,
            &self.canonical_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    fn validate_shape(
        &self,
        registration: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.owner_registration.verify_registration(registration)?;
        if registration.store_root.store_root_hash != self.store_root_hash
            || registration.provider != self.provider
            || !matches!(
                registration.store_commits,
                StoreCommitAnchor::MergeConcurrent { .. }
            )
            || !matches!(self.membership, GrantStreamAnchor::StoreMembership { .. })
            || !matches!(self.recovery, GrantStreamAnchor::OwnerRecovery { .. })
            || !matches!(
                self.device_state,
                StoreDeviceStateRef::MergeConcurrent { .. }
            )
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        domain_json(
            b"coven.owner-conflict-resolution-acceptance.v1\0",
            &(
                self.store_root_hash,
                &self.owner_grant,
                &self.owner_registration,
                &self.provider,
                &self.membership,
                &self.recovery,
                &self.device_state,
            ),
        )
    }
}

impl StoreControl {
    pub fn serial_membership_entry(&self) -> Option<&super::membership::SerialMembershipEntry> {
        match self {
            Self::SerialMembership { entry }
            | Self::SerialMembershipAndKeyRotation { entry, .. } => Some(entry),
            Self::MergeMembership { .. } | Self::ProviderAdmin { .. } => None,
        }
    }

    pub fn merge_membership_transition(
        &self,
    ) -> Option<&super::membership::MergeMembershipHeadTransition> {
        match self {
            Self::MergeMembership { transition } => Some(transition),
            Self::SerialMembership { .. }
            | Self::SerialMembershipAndKeyRotation { .. }
            | Self::ProviderAdmin { .. } => None,
        }
    }

    pub fn key_generation(&self) -> Option<u64> {
        match self {
            Self::MergeMembership { .. } | Self::SerialMembership { .. } => None,
            Self::SerialMembershipAndKeyRotation { generation, .. } => Some(*generation),
            Self::ProviderAdmin { .. } => None,
        }
    }

    pub(crate) fn introduced_wrapped_keys(
        &self,
    ) -> Vec<&super::wrapped_store_key::WrappedStoreKeyRef> {
        match self {
            Self::MergeMembership { .. } => Vec::new(),
            Self::SerialMembership { entry } => match &entry.change {
                super::membership::SerialMembershipChange::SetMember { wrapped_key, .. } => {
                    vec![wrapped_key]
                }
                super::membership::SerialMembershipChange::RemoveMember { .. } => Vec::new(),
            },
            Self::SerialMembershipAndKeyRotation { wrapped_keys, .. } => {
                wrapped_keys.iter().collect()
            }
            Self::ProviderAdmin { .. } => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateObjectManifest {
    pub family: CandidateFamilyId,
    pub objects: Vec<CandidateExclusiveObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CandidateExclusiveObjectRef {
    StorePackage(StorePackageRef),
    CirclePackage(CirclePackageRef),
    CircleAccess {
        circle_id: CircleId,
        access: CircleAccessObjectRef,
    },
    SelfRetirement(StoreDeviceSelfRetirementRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinAttemptDecisionRef {
    Attempt(DeviceJoinAttemptRef),
    Abandoned(super::device_join::DeviceJoinAbandonmentRef),
}

impl DeviceJoinAttemptDecisionRef {
    pub fn attempt_id(&self) -> DeviceJoinAttemptId {
        match self {
            Self::Attempt(reference) => reference.attempt_id,
            Self::Abandoned(reference) => reference.attempt_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCommitOperations {
    pub acknowledgement: Option<StoreAckRef>,
    pub control: Option<StoreControl>,
    pub device_join_attempt_decisions: Vec<DeviceJoinAttemptDecisionRef>,
    pub device_join_outcomes: Vec<DeviceJoinOutcomeRef>,
    pub device_join_cleanup_receipts: Vec<super::device_join::DeviceJoinCleanupReceiptRef>,
    pub provider_access_grants: Vec<super::provider::StoreMemberProviderAccessGrantRef>,
    pub provider_access_withdrawals:
        Vec<super::provider::StoreMemberProviderAccessWithdrawalReceiptRef>,
    pub device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
    pub device_exclusion_proposals: Vec<StoreDeviceExclusionProposalRef>,
    pub device_exclusion_outcomes: Vec<StoreDeviceExclusionOutcomeRef>,
    pub stream_activations: Vec<StreamActivation>,
    pub circle_controls: Vec<CircleControlRef>,
    pub store_package: Option<StorePackageRef>,
    pub circle_packages: Vec<CirclePackageRef>,
}

impl StoreCommitOperations {
    fn is_empty(&self) -> bool {
        self.acknowledgement.is_none() && self.has_no_other_operations()
    }

    pub(crate) fn is_acknowledgement_only(&self) -> bool {
        self.acknowledgement.is_some() && self.has_no_other_operations()
    }

    pub(crate) fn is_circle_control_activation_only(&self) -> bool {
        self.acknowledgement.is_none()
            && self.control.is_none()
            && self.device_join_attempt_decisions.is_empty()
            && self.device_join_outcomes.is_empty()
            && self.device_join_cleanup_receipts.is_empty()
            && self.provider_access_grants.is_empty()
            && self.provider_access_withdrawals.is_empty()
            && self.device_registrations.is_empty()
            && self.device_exclusion_proposals.is_empty()
            && self.device_exclusion_outcomes.is_empty()
            && self.circle_controls.len() == 1
            && self.store_package.is_none()
            && self.circle_packages.is_empty()
    }

    fn has_no_other_operations(&self) -> bool {
        self.control.is_none()
            && self.device_join_attempt_decisions.is_empty()
            && self.device_join_outcomes.is_empty()
            && self.device_join_cleanup_receipts.is_empty()
            && self.provider_access_grants.is_empty()
            && self.provider_access_withdrawals.is_empty()
            && self.device_registrations.is_empty()
            && self.device_exclusion_proposals.is_empty()
            && self.device_exclusion_outcomes.is_empty()
            && self.stream_activations.is_empty()
            && self.circle_controls.is_empty()
            && self.store_package.is_none()
            && self.circle_packages.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitBody {
    Operations(StoreCommitOperations),
    ReclaimAuthorization {
        authorization: Box<super::store_reclaim::ReclaimAuthorizationRef>,
    },
    ReclaimReceipt {
        receipt: Box<super::store_reclaim::ReclaimReceiptRef>,
    },
    SelfRetirement {
        retirement: StoreDeviceSelfRetirementRef,
    },
    SerialRecoveryActivation {
        activation: SerialRecoveryActivation,
    },
    OwnerPromotionRequest {
        request: Box<OwnerPromotionRequest>,
    },
    AbandonCandidates {
        manifests: Vec<CandidateCleanupManifest>,
    },
}

pub struct StoreCommitOperationsInput<'a> {
    pub acknowledgement: Option<StoreAckRef>,
    pub control: Option<StoreControl>,
    pub device_join_attempt_decisions: Vec<DeviceJoinAttemptDecisionRef>,
    pub device_join_outcomes: Vec<DeviceJoinOutcomeRef>,
    pub device_join_cleanup_receipts: Vec<super::device_join::DeviceJoinCleanupReceiptRef>,
    pub provider_access_grants: Vec<super::provider::StoreMemberProviderAccessGrantRef>,
    pub provider_access_withdrawals:
        Vec<super::provider::StoreMemberProviderAccessWithdrawalReceiptRef>,
    pub device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
    pub device_exclusion_proposals: Vec<StoreDeviceExclusionProposalRef>,
    pub device_exclusion_outcomes: Vec<StoreDeviceExclusionOutcomeRef>,
    pub stream_activations: Vec<StreamActivation>,
    pub circle_controls: Vec<CircleControlRef>,
    pub store_package: Option<StorePackageInput<'a>>,
    pub circle_packages: &'a [CirclePackageInput<'a>],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreOperationMembershipAuthority {
    MergeConcurrent {
        predecessor: MembershipGrantCreationAuthority,
    },
    Serial,
}

impl StoreOperationMembershipAuthority {
    fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial => WritePolicy::Serial,
        }
    }

    fn into_commit_authority(self) -> Option<MembershipGrantCreationAuthority> {
        match self {
            Self::MergeConcurrent { predecessor } => Some(predecessor),
            Self::Serial => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommit {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub write_id: WriteId,
    pub author_registration: StoreDeviceRegistrationRef,
    pub order: StoreCommitOrder,
    pub membership_state: StoreMembershipStateRef,
    pub device_state: StoreDeviceStateRef,
    pub membership_authority: Option<MembershipGrantCreationAuthority>,
    pub candidate_objects: CandidateObjectManifest,
    pub body: StoreCommitBody,
    pub signature: String,
}

#[derive(Serialize)]
struct CommitSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    write_id: &'a WriteId,
    author_registration: &'a StoreDeviceRegistrationRef,
    order: &'a StoreCommitOrder,
    membership_state: &'a StoreMembershipStateRef,
    device_state: &'a StoreDeviceStateRef,
    membership_authority: Option<&'a MembershipGrantCreationAuthority>,
    candidate_objects: &'a CandidateObjectManifest,
    body: &'a StoreCommitBody,
}

impl StoreBatchCommit {
    pub(crate) fn verified_candidate_objects(
        &self,
    ) -> Result<&CandidateObjectManifest, StoreProtocolError> {
        let expected = candidate_manifest(self.candidate_family(), &self.body)?;
        if self.candidate_objects != expected {
            return Err(StoreProtocolError::Malformed(
                "candidate object manifest differs from exact commit body graph".to_string(),
            ));
        }
        Ok(&self.candidate_objects)
    }

    pub fn policy(&self) -> WritePolicy {
        self.order.policy()
    }

    pub fn seq(&self) -> u64 {
        self.order.seq()
    }

    pub fn candidate_family(&self) -> CandidateFamilyId {
        CandidateFamilyId::derive(
            self.store_root_hash,
            &self.author_registration,
            &self.write_id,
            &self.order,
        )
    }

    pub fn operations(&self) -> Option<&StoreCommitOperations> {
        match &self.body {
            StoreCommitBody::Operations(operations) => Some(operations),
            StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SelfRetirement { .. }
            | StoreCommitBody::SerialRecoveryActivation { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn operations_membership_authority(
        &self,
    ) -> Result<StoreOperationMembershipAuthority, StoreProtocolError> {
        if self.operations().is_none() {
            return Err(StoreProtocolError::Malformed(
                "Store commit does not carry operations".to_string(),
            ));
        }
        validate_operation_membership_authority(
            self.order.policy(),
            self.membership_authority.as_ref(),
        )?;
        match self.order.policy() {
            WritePolicy::MergeConcurrent => {
                Ok(StoreOperationMembershipAuthority::MergeConcurrent {
                    predecessor: self
                        .membership_authority
                        .clone()
                        .expect("validated MergeConcurrent operations carry membership authority"),
                })
            }
            WritePolicy::Serial => Ok(StoreOperationMembershipAuthority::Serial),
        }
    }

    pub fn control(&self) -> Option<&StoreControl> {
        self.operations()
            .and_then(|operations| operations.control.as_ref())
    }

    pub fn acknowledgement(&self) -> Option<&StoreAckRef> {
        self.operations()
            .and_then(|operations| operations.acknowledgement.as_ref())
    }

    pub fn serial_recovery_activation(&self) -> Option<&SerialRecoveryActivation> {
        match &self.body {
            StoreCommitBody::SerialRecoveryActivation { activation } => Some(activation),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SelfRetirement { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn self_retirement(&self) -> Option<&StoreDeviceSelfRetirementRef> {
        match &self.body {
            StoreCommitBody::SelfRetirement { retirement } => Some(retirement),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SerialRecoveryActivation { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn abandoned_candidates(&self) -> &[CandidateCleanupManifest] {
        match &self.body {
            StoreCommitBody::AbandonCandidates { manifests } => manifests,
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SelfRetirement { .. }
            | StoreCommitBody::SerialRecoveryActivation { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. } => &[],
        }
    }

    pub fn reclaim_authorization(&self) -> Option<&super::store_reclaim::ReclaimAuthorizationRef> {
        match &self.body {
            StoreCommitBody::ReclaimAuthorization { authorization } => Some(authorization.as_ref()),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SelfRetirement { .. }
            | StoreCommitBody::SerialRecoveryActivation { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn reclaim_receipt(&self) -> Option<&super::store_reclaim::ReclaimReceiptRef> {
        match &self.body {
            StoreCommitBody::ReclaimReceipt { receipt } => Some(receipt.as_ref()),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::SelfRetirement { .. }
            | StoreCommitBody::SerialRecoveryActivation { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn device_join_attempt_decisions(&self) -> &[DeviceJoinAttemptDecisionRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_join_attempt_decisions.as_slice()
        })
    }

    pub fn device_join_outcomes(&self) -> &[DeviceJoinOutcomeRef] {
        self.operations()
            .map_or(&[], |operations| operations.device_join_outcomes.as_slice())
    }

    pub fn device_join_cleanup_receipts(
        &self,
    ) -> &[super::device_join::DeviceJoinCleanupReceiptRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_join_cleanup_receipts.as_slice()
        })
    }

    pub fn provider_access_grants(&self) -> &[super::provider::StoreMemberProviderAccessGrantRef] {
        self.operations().map_or(&[], |operations| {
            operations.provider_access_grants.as_slice()
        })
    }

    pub fn provider_access_withdrawals(
        &self,
    ) -> &[super::provider::StoreMemberProviderAccessWithdrawalReceiptRef] {
        self.operations().map_or(&[], |operations| {
            operations.provider_access_withdrawals.as_slice()
        })
    }

    pub fn device_registrations(&self) -> &[ActivatedStoreDeviceRegistrationRef] {
        match &self.body {
            StoreCommitBody::Operations(operations) => operations.device_registrations.as_slice(),
            StoreCommitBody::SerialRecoveryActivation { activation } => {
                std::slice::from_ref(&activation.registration)
            }
            StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SelfRetirement { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. } => &[],
            StoreCommitBody::AbandonCandidates { .. } => &[],
        }
    }

    pub fn device_exclusion_proposals(&self) -> &[StoreDeviceExclusionProposalRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_exclusion_proposals.as_slice()
        })
    }

    pub fn device_exclusion_outcomes(&self) -> &[StoreDeviceExclusionOutcomeRef] {
        self.operations().map_or(&[], |operations| {
            operations.device_exclusion_outcomes.as_slice()
        })
    }

    pub fn stream_activations(&self) -> &[StreamActivation] {
        self.operations()
            .map_or(&[], |operations| operations.stream_activations.as_slice())
    }

    pub fn device_retirements(&self) -> &[StoreDeviceSelfRetirementRef] {
        match &self.body {
            StoreCommitBody::SelfRetirement { retirement } => std::slice::from_ref(retirement),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SerialRecoveryActivation { .. }
            | StoreCommitBody::OwnerPromotionRequest { .. } => &[],
            StoreCommitBody::AbandonCandidates { .. } => &[],
        }
    }

    pub fn owner_promotion_request(&self) -> Option<&OwnerPromotionRequest> {
        match &self.body {
            StoreCommitBody::OwnerPromotionRequest { request } => Some(request),
            StoreCommitBody::Operations(_)
            | StoreCommitBody::ReclaimAuthorization { .. }
            | StoreCommitBody::ReclaimReceipt { .. }
            | StoreCommitBody::SelfRetirement { .. }
            | StoreCommitBody::SerialRecoveryActivation { .. }
            | StoreCommitBody::AbandonCandidates { .. } => None,
        }
    }

    pub fn circle_controls(&self) -> &[CircleControlRef] {
        self.operations()
            .map_or(&[], |operations| operations.circle_controls.as_slice())
    }

    pub fn store_package(&self) -> Option<&StorePackageRef> {
        self.operations()
            .and_then(|operations| operations.store_package.as_ref())
    }

    pub fn circle_packages(&self) -> &[CirclePackageRef] {
        self.operations()
            .map_or(&[], |operations| operations.circle_packages.as_slice())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_reclaim_authorization(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        authorization: super::store_reclaim::ReclaimAuthorizationRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::ReclaimAuthorization {
                authorization: Box::new(authorization),
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_reclaim_receipt(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        receipt: super::store_reclaim::ReclaimReceiptRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::ReclaimReceipt {
                receipt: Box::new(receipt),
            },
            signer,
        )
    }

    pub fn merge_dependencies(
        &self,
    ) -> Result<&BTreeMap<AuthorStreamId, StoreBatchCommitRef>, StoreProtocolError> {
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
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        package: StorePackageInput<'_>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: Some(package),
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_control(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        control: Option<StoreControl>,
        package: Option<StorePackageInput<'_>>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: package,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_registrations(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations,
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_serial_recovery(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        activation: SerialRecoveryActivation,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        validate_serial_recovery_activation(&order, &activation, &author_registration)?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::SerialRecoveryActivation { activation },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_owner_promotion_request(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        request: OwnerPromotionRequest,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        if membership_authority.policy() != order.policy() {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: order.policy(),
                actual: membership_authority.policy(),
            });
        }
        let membership_authority = membership_authority.into_commit_authority();
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            membership_authority.as_ref(),
            signer,
        )?;
        validate_owner_promotion_request_for_commit(
            &request,
            store_root_hash,
            &author_registration,
            author,
            &membership_state,
            &device_state,
            order.policy(),
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitBody::OwnerPromotionRequest {
                request: Box::new(request),
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_self_retirement(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: Option<MembershipGrantCreationAuthority>,
        retirement: StoreDeviceSelfRetirementRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            membership_authority.as_ref(),
            signer,
        )?;
        validate_device_retirement_refs(
            std::slice::from_ref(&retirement),
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order),
            &author_registration,
            &order,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitBody::SelfRetirement { retirement },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_candidate_abandonment(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        mut manifests: Vec<CandidateCleanupManifest>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        manifests.sort();
        validate_candidate_abandonment(
            &manifests,
            store_root_hash,
            &author_registration,
            &coord,
            &order,
            author,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::AbandonCandidates { manifests },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_attempts(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        attempts: Vec<DeviceJoinAttemptRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: attempts
                    .into_iter()
                    .map(DeviceJoinAttemptDecisionRef::Attempt)
                    .collect(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_outcomes(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        device_join_outcomes: Vec<DeviceJoinOutcomeRef>,
        device_registrations: Vec<ActivatedStoreDeviceRegistrationRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes,
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations,
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_abandonments(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        abandonments: Vec<super::device_join::DeviceJoinAbandonmentRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: abandonments
                    .into_iter()
                    .map(DeviceJoinAttemptDecisionRef::Abandoned)
                    .collect(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_join_cleanup_receipts(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        receipts: Vec<super::device_join::DeviceJoinCleanupReceiptRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: receipts,
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_device_exclusions(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        proposals: Vec<StoreDeviceExclusionProposalRef>,
        outcomes: Vec<StoreDeviceExclusionOutcomeRef>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants: Vec::new(),
                provider_access_withdrawals: Vec::new(),
                device_registrations: Vec::new(),
                device_exclusion_proposals: proposals,
                device_exclusion_outcomes: outcomes,
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_provider_access(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        provider_access_grants: Vec<super::provider::StoreMemberProviderAccessGrantRef>,
        provider_access_withdrawals: Vec<
            super::provider::StoreMemberProviderAccessWithdrawalReceiptRef,
        >,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        Self::signed_operations(
            store_root_hash,
            write_id,
            coord,
            author_registration,
            author,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitOperationsInput {
                acknowledgement: None,
                control: None,
                device_join_attempt_decisions: Vec::new(),
                device_join_outcomes: Vec::new(),
                device_join_cleanup_receipts: Vec::new(),
                provider_access_grants,
                provider_access_withdrawals,
                device_registrations: Vec::new(),
                device_exclusion_proposals: Vec::new(),
                device_exclusion_outcomes: Vec::new(),
                stream_activations: Vec::new(),
                circle_controls: Vec::new(),
                store_package: None,
                circle_packages: &[],
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_operations(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        input: StoreCommitOperationsInput<'_>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        if membership_authority.policy() != order.policy() {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: order.policy(),
                actual: membership_authority.policy(),
            });
        }
        let membership_authority = membership_authority.into_commit_authority();
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            membership_authority.as_ref(),
            signer,
        )?;
        let StoreCommitOperationsInput {
            acknowledgement,
            control,
            device_join_attempt_decisions,
            device_join_outcomes,
            device_join_cleanup_receipts,
            provider_access_grants,
            provider_access_withdrawals,
            device_registrations,
            device_exclusion_proposals,
            device_exclusion_outcomes,
            stream_activations,
            circle_controls,
            store_package,
            circle_packages,
        } = input;
        validate_control(
            order.policy(),
            store_root_hash,
            &author_registration,
            &author.author_pubkey,
            &membership_state,
            control.as_ref(),
        )?;
        validate_commit_acknowledgement(&acknowledgement, &author_registration)?;
        let stream_id = commit_stream_id(&coord);
        let seq = order.seq();
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
        let store_package = store_package
            .map(|input| {
                if input.candidate_family != candidate_family {
                    return Err(StoreProtocolError::Malformed(
                        "Store package candidate family differs from its commit".to_string(),
                    ));
                }
                let semantic_prefix = package_semantic_prefix(
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(input.bytes),
                );
                package_ref(&semantic_prefix, &input)
            })
            .transpose()?;
        validate_device_join_attempt_decision_refs(&device_join_attempt_decisions)?;
        validate_device_join_outcome_refs(&device_join_outcomes)?;
        validate_device_join_cleanup_receipt_refs(&device_join_cleanup_receipts)?;
        validate_provider_access_refs(&provider_access_grants, &provider_access_withdrawals)?;
        validate_device_registration_refs(&device_registrations)?;
        validate_device_exclusion_refs(&device_exclusion_proposals, &device_exclusion_outcomes)?;
        validate_stream_activations(
            store_root_hash,
            &author_registration,
            order.policy(),
            control.as_ref(),
            &stream_activations,
        )?;
        let mut seen_circles = BTreeSet::new();
        let circle_packages = circle_packages
            .iter()
            .map(|input| {
                if !seen_circles.insert(input.circle_id) {
                    return Err(StoreProtocolError::DuplicateCirclePackage(input.circle_id));
                }
                validate_circle_control_coord(order.policy(), &input.control)?;
                if input.package.candidate_family != candidate_family {
                    return Err(StoreProtocolError::Malformed(
                        "Circle package candidate family differs from its commit".to_string(),
                    ));
                }
                let semantic_prefix = circle_package_semantic_prefix(
                    input.circle_id,
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(input.package.bytes),
                );
                let package = package_ref(&semantic_prefix, &input.package)?;
                Ok(CirclePackageRef {
                    circle_id: input.circle_id,
                    control: input.control.clone(),
                    package,
                    key_fingerprint: input.key_fingerprint,
                })
            })
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        validate_circle_control_refs(order.policy(), &circle_controls)?;
        let operations = StoreCommitOperations {
            acknowledgement,
            control,
            device_join_attempt_decisions,
            device_join_outcomes,
            device_join_cleanup_receipts,
            provider_access_grants,
            provider_access_withdrawals,
            device_registrations,
            device_exclusion_proposals,
            device_exclusion_outcomes,
            stream_activations,
            circle_controls,
            store_package,
            circle_packages,
        };
        if operations.is_empty() {
            return Err(StoreProtocolError::EmptyBatch);
        }
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            membership_authority,
            StoreCommitBody::Operations(operations),
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_signed_body(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        author_registration: StoreDeviceRegistrationRef,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: Option<MembershipGrantCreationAuthority>,
        body: StoreCommitBody,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
        validate_commit_body(store_root_hash, &body, family, &author_registration, &order)?;
        let candidate_objects = candidate_manifest(family, &body)?;
        let mut commit = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            membership_authority,
            candidate_objects,
            body,
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
            write_id: &self.write_id,
            author_registration: &self.author_registration,
            order: &self.order,
            membership_state: &self.membership_state,
            device_state: &self.device_state,
            membership_authority: self.membership_authority.as_ref(),
            candidate_objects: &self.candidate_objects,
            body: &self.body,
        };
        domain_json(COMMIT_DOMAIN, &fields)
    }

    pub fn commit_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreBatchCommit serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected_coord: &StoreCommitCoord,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let commit: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        commit.verify_at(expected_store_root_hash, expected_coord, author)?;
        Ok(commit)
    }

    pub fn verify_at(
        &self,
        expected_store_root_hash: ObjectHash,
        expected_coord: &StoreCommitCoord,
        author: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        if self.store_root_hash != expected_store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root_hash,
                actual: self.store_root_hash,
            });
        }
        if self.order.policy() != expected_coord.policy() {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: expected_coord.policy(),
                actual: self.order.policy(),
            });
        }
        let stream_id = commit_stream_id(expected_coord);
        if self.order.seq() != expected_coord.sequence() {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: commit_slot_prefix(&stream_id, expected_coord.sequence()),
                actual: commit_slot_prefix(&stream_id, self.order.seq()),
            });
        }
        self.author_registration.verify_registration(author)?;
        let family = self.candidate_family();
        if let Some(package) = self.store_package() {
            if package.candidate_family != self.candidate_family() {
                return Err(StoreProtocolError::Malformed(
                    "Store package candidate family differs from its commit".to_string(),
                ));
            }
            let expected =
                package_semantic_prefix(family, &stream_id, self.order.seq(), package.content_hash);
            if package.object.slot().logical_key() != format!("{expected}.pkg") {
                return Err(StoreProtocolError::RelocatedPackage {
                    expected,
                    actual: package.object.slot().logical_key().to_string(),
                });
            }
        }
        let mut seen_circles = BTreeSet::new();
        for circle_package in self.circle_packages() {
            if circle_package.package.candidate_family != self.candidate_family() {
                return Err(StoreProtocolError::Malformed(
                    "Circle package candidate family differs from its commit".to_string(),
                ));
            }
            if !seen_circles.insert(circle_package.circle_id) {
                return Err(StoreProtocolError::DuplicateCirclePackage(
                    circle_package.circle_id,
                ));
            }
            validate_circle_control_coord(self.policy(), &circle_package.control)?;
            let expected = circle_package_semantic_prefix(
                circle_package.circle_id,
                family,
                &stream_id,
                self.seq(),
                circle_package.package.content_hash,
            );
            if circle_package.package.object.slot().logical_key() != format!("{expected}.pkg") {
                return Err(StoreProtocolError::RelocatedCirclePackage {
                    circle_id: circle_package.circle_id,
                    expected,
                    actual: circle_package
                        .package
                        .object
                        .slot()
                        .logical_key()
                        .to_string(),
                });
            }
        }
        validate_commit_body(
            self.store_root_hash,
            &self.body,
            family,
            &self.author_registration,
            &self.order,
        )?;
        if matches!(self.body, StoreCommitBody::Operations(_)) {
            validate_operation_membership_authority(
                self.order.policy(),
                self.membership_authority.as_ref(),
            )?;
        }
        if let StoreCommitBody::AbandonCandidates { manifests } = &self.body {
            validate_candidate_abandonment(
                manifests,
                self.store_root_hash,
                &self.author_registration,
                expected_coord,
                &self.order,
                author,
            )?;
        }
        if let StoreCommitBody::OwnerPromotionRequest { request } = &self.body {
            validate_owner_promotion_request_for_commit(
                request,
                self.store_root_hash,
                &self.author_registration,
                author,
                &self.membership_state,
                &self.device_state,
                self.policy(),
            )?;
        }
        self.verified_candidate_objects()?;
        validate_commit_order(&self.order)?;
        validate_commit_predecessor_states(
            &self.order,
            &self.membership_state,
            &self.device_state,
        )?;
        if let Some(authority) = self.membership_authority.as_ref() {
            validate_membership_authority(authority)?;
        }
        validate_parsed_control(self, author)?;
        if !keys::verify_signature_hex(
            &author.device_signing_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }

    pub fn verify_store_package(&self, package_bytes: &[u8]) -> Result<(), StoreProtocolError> {
        let package = self
            .store_package()
            .ok_or(StoreProtocolError::MissingStorePackage)?;
        verify_package_ref(package, package_bytes)
    }

    pub fn verify_circle_package(
        &self,
        circle_id: CircleId,
        package_bytes: &[u8],
    ) -> Result<(), StoreProtocolError> {
        let package = self
            .circle_packages()
            .iter()
            .find(|package| package.circle_id == circle_id)
            .ok_or(StoreProtocolError::MissingCirclePackage(circle_id))?;
        verify_package_ref(&package.package, package_bytes)
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_commit_envelope(
    store_root_hash: ObjectHash,
    coord: &StoreCommitCoord,
    author_registration: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    order: &StoreCommitOrder,
    membership_state: &StoreMembershipStateRef,
    device_state: &StoreDeviceStateRef,
    membership_authority: Option<&MembershipGrantCreationAuthority>,
    signer: &UserKeypair,
) -> Result<(), StoreProtocolError> {
    author_registration.verify_registration(author)?;
    if keys::public_key_hex(signer) != author.device_signing_pubkey {
        return Err(StoreProtocolError::InvalidSignature);
    }
    if order.seq() == 0 {
        return Err(StoreProtocolError::InvalidSequence(0));
    }
    validate_commit_order(order)?;
    validate_commit_predecessor_states(order, membership_state, device_state)?;
    if coord.sequence() != order.seq() || coord.policy() != order.policy() {
        return Err(StoreProtocolError::Malformed(
            "Store commit coordinate disagrees with its order".to_string(),
        ));
    }
    if let Some(authority) = membership_authority {
        validate_membership_authority(authority)?;
    }
    if store_root_hash != author.store_root.store_root_hash {
        return Err(StoreProtocolError::StoreRootMismatch {
            expected: store_root_hash,
            actual: author.store_root.store_root_hash,
        });
    }
    Ok(())
}

fn validate_commit_body(
    store_root_hash: ObjectHash,
    body: &StoreCommitBody,
    family: CandidateFamilyId,
    author: &StoreDeviceRegistrationRef,
    order: &StoreCommitOrder,
) -> Result<(), StoreProtocolError> {
    match body {
        StoreCommitBody::Operations(operations) => {
            if operations.is_empty() {
                return Err(StoreProtocolError::EmptyBatch);
            }
            validate_circle_control_refs(order.policy(), &operations.circle_controls)?;
            validate_commit_acknowledgement(&operations.acknowledgement, author)?;
            validate_device_join_attempt_decision_refs(&operations.device_join_attempt_decisions)?;
            validate_device_join_outcome_refs(&operations.device_join_outcomes)?;
            validate_device_join_cleanup_receipt_refs(&operations.device_join_cleanup_receipts)?;
            validate_provider_access_refs(
                &operations.provider_access_grants,
                &operations.provider_access_withdrawals,
            )?;
            validate_device_registration_refs(&operations.device_registrations)?;
            validate_device_exclusion_refs(
                &operations.device_exclusion_proposals,
                &operations.device_exclusion_outcomes,
            )?;
            validate_stream_activations(
                store_root_hash,
                author,
                order.policy(),
                operations.control.as_ref(),
                &operations.stream_activations,
            )?;
        }
        StoreCommitBody::ReclaimAuthorization { .. } => {}
        StoreCommitBody::ReclaimReceipt { .. } => {}
        StoreCommitBody::SelfRetirement { retirement } => {
            validate_device_retirement_refs(
                std::slice::from_ref(retirement),
                family,
                author,
                order,
            )?;
        }
        StoreCommitBody::SerialRecoveryActivation { activation } => {
            validate_serial_recovery_activation(order, activation, author)?;
        }
        StoreCommitBody::OwnerPromotionRequest { request } => {
            if request.store_root_hash != store_root_hash
                || request.promoter_registration != *author
                || request.finalization.policy() != order.policy()
            {
                return Err(StoreProtocolError::OwnerPromotionMismatch);
            }
        }
        StoreCommitBody::AbandonCandidates { manifests } => {
            if manifests.is_empty() {
                return Err(StoreProtocolError::Malformed(
                    "candidate abandonment has no candidates".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_owner_promotion_request_for_commit(
    request: &OwnerPromotionRequest,
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    membership_state: &StoreMembershipStateRef,
    device_state: &StoreDeviceStateRef,
    policy: WritePolicy,
) -> Result<(), StoreProtocolError> {
    request.verify(&author.store_root, author)?;
    if request.store_root_hash != store_root_hash
        || request.promoter_registration != *author_registration
        || request.predecessor_membership != *membership_state
        || request.predecessor_devices != *device_state
        || request.finalization.policy() != policy
    {
        return Err(StoreProtocolError::OwnerPromotionMismatch);
    }
    Ok(())
}

fn validate_stream_activations(
    store_root_hash: ObjectHash,
    author: &StoreDeviceRegistrationRef,
    policy: WritePolicy,
    control: Option<&StoreControl>,
    activations: &[StreamActivation],
) -> Result<(), StoreProtocolError> {
    if activations.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::Malformed(
            "stream activations are not strictly sorted and unique".to_string(),
        ));
    }
    let mut activation_ids = BTreeSet::new();
    let mut stream_ids = BTreeSet::new();
    let mut first_slots = BTreeSet::new();
    for activation in activations {
        if activation.store_root_hash() != store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: store_root_hash,
                actual: activation.store_root_hash(),
            });
        }
        let owner_promotion = matches!(control, Some(StoreControl::MergeMembership { .. }));
        if activation.author_registration() != author && !owner_promotion {
            return Err(StoreProtocolError::Malformed(
                "stream activation registration differs from its commit author".to_string(),
            ));
        }
        if policy == WritePolicy::Serial {
            return Err(StoreProtocolError::Malformed(
                "Serial Store commit contains a stream activation".to_string(),
            ));
        }
        let allowed_anchor = matches!(
            (control, activation),
            (
                Some(StoreControl::MergeMembership { .. }),
                StreamActivation::GrantAuthorized {
                    anchor: GrantStreamAnchor::StoreMembership { .. }
                        | GrantStreamAnchor::OwnerRecovery { .. },
                    ..
                }
            ) | (
                _,
                StreamActivation::GrantAuthorized {
                    anchor: GrantStreamAnchor::CircleControl { .. }
                        | GrantStreamAnchor::CircleRoster { .. }
                        | GrantStreamAnchor::CircleMetadata { .. },
                    ..
                }
            )
        );
        if !allowed_anchor {
            return Err(StoreProtocolError::Malformed(
                "Store commit contains a root- or registration-authorized stream anchor"
                    .to_string(),
            ));
        }
        if !activation_ids.insert(activation.activation_id())
            || !stream_ids.insert(activation.author_stream_id())
            || !first_slots.insert(activation.first_slot().clone())
        {
            return Err(StoreProtocolError::Malformed(
                "stream activations repeat an activation, author stream, or first slot".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_candidate_abandonment(
    manifests: &[CandidateCleanupManifest],
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    coord: &StoreCommitCoord,
    order: &StoreCommitOrder,
    author: &StoreDeviceRegistration,
) -> Result<(), StoreProtocolError> {
    if manifests.is_empty() {
        return Err(StoreProtocolError::Malformed(
            "candidate abandonment has no candidates".to_string(),
        ));
    }
    if manifests.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::Malformed(
            "candidate abandonment manifests are not strictly sorted and unique".to_string(),
        ));
    }
    for manifest in manifests {
        if &manifest.candidate.coord != coord {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate occupies a different competition point".to_string(),
            ));
        }
        let candidate = manifest
            .candidate
            .verify_candidate(store_root_hash, author)?;
        if &candidate.author_registration != author_registration {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate has a different author registration".to_string(),
            ));
        }
        let shares_predecessor = match (&candidate.order, order) {
            (
                StoreCommitOrder::MergeConcurrent {
                    predecessor: candidate_predecessor,
                    ..
                },
                StoreCommitOrder::MergeConcurrent { predecessor, .. },
            ) => candidate_predecessor == predecessor,
            (
                StoreCommitOrder::Serial {
                    predecessor: candidate_predecessor,
                    ..
                },
                StoreCommitOrder::Serial { predecessor, .. },
            ) => candidate_predecessor == predecessor,
            _ => false,
        };
        if !shares_predecessor {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate has a different predecessor".to_string(),
            ));
        }
    }
    Ok(())
}

fn candidate_manifest(
    family: CandidateFamilyId,
    body: &StoreCommitBody,
) -> Result<CandidateObjectManifest, StoreProtocolError> {
    let mut objects = Vec::new();
    match body {
        StoreCommitBody::Operations(operations) => {
            objects.extend(
                operations
                    .store_package
                    .iter()
                    .cloned()
                    .map(CandidateExclusiveObjectRef::StorePackage),
            );
            objects.extend(
                operations
                    .circle_packages
                    .iter()
                    .cloned()
                    .map(CandidateExclusiveObjectRef::CirclePackage),
            );
            for control in &operations.circle_controls {
                let circle_id = control.circle_id();
                if control
                    .objects()
                    .access
                    .iter()
                    .any(|access| access.envelope.control_hash != control.control().control_hash())
                {
                    return Err(StoreProtocolError::Malformed(
                        "Circle access envelope differs from its activating control".to_string(),
                    ));
                }
                objects.extend(
                    control.objects().access.iter().cloned().map(|access| {
                        CandidateExclusiveObjectRef::CircleAccess { circle_id, access }
                    }),
                );
            }
        }
        StoreCommitBody::ReclaimAuthorization { .. } => {}
        StoreCommitBody::ReclaimReceipt { .. } => {}
        StoreCommitBody::SelfRetirement { retirement } => {
            objects.push(CandidateExclusiveObjectRef::SelfRetirement(
                retirement.clone(),
            ));
        }
        StoreCommitBody::SerialRecoveryActivation { .. }
        | StoreCommitBody::OwnerPromotionRequest { .. }
        | StoreCommitBody::AbandonCandidates { .. } => {}
    }
    objects.sort_by_cached_key(|object| {
        serde_json::to_vec(object).expect("candidate object serialization cannot fail")
    });
    let mut exact_refs = BTreeSet::new();
    let mut access_keys = BTreeSet::new();
    for object in &objects {
        validate_candidate_object_path(family, object)?;
        match object {
            CandidateExclusiveObjectRef::CircleAccess { circle_id, access } => {
                let key = (
                    *circle_id,
                    access.leaf.owner_pubkey.clone(),
                    access.leaf.recipient_slot.clone(),
                    access.envelope.control_hash,
                );
                if !access_keys.insert(key) {
                    return Err(StoreProtocolError::Malformed(
                        "candidate object manifest repeats a Circle access semantic key"
                            .to_string(),
                    ));
                }
                insert_candidate_exact_ref(&mut exact_refs, &access.leaf.object)?;
                insert_candidate_exact_ref(&mut exact_refs, &access.envelope.object)?;
            }
            CandidateExclusiveObjectRef::StorePackage(reference) => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::CirclePackage(reference) => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.package.object)?;
            }
            CandidateExclusiveObjectRef::SelfRetirement(reference) => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
        }
    }
    Ok(CandidateObjectManifest { family, objects })
}

fn insert_candidate_exact_ref<'a>(
    exact_refs: &mut BTreeSet<&'a ExactObjectRef>,
    object: &'a ExactObjectRef,
) -> Result<(), StoreProtocolError> {
    if !exact_refs.insert(object) {
        return Err(StoreProtocolError::Malformed(
            "candidate object manifest repeats an exact object reference".to_string(),
        ));
    }
    Ok(())
}

fn validate_candidate_object_path(
    family: CandidateFamilyId,
    candidate: &CandidateExclusiveObjectRef,
) -> Result<(), StoreProtocolError> {
    let (expected, object) = match candidate {
        CandidateExclusiveObjectRef::StorePackage(reference) => {
            if reference.candidate_family != family {
                return Err(StoreProtocolError::Malformed(
                    "Store package candidate family differs from its manifest".to_string(),
                ));
            }
            return Ok(());
        }
        CandidateExclusiveObjectRef::CirclePackage(reference) => {
            if reference.package.candidate_family != family {
                return Err(StoreProtocolError::Malformed(
                    "Circle package candidate family differs from its manifest".to_string(),
                ));
            }
            return Ok(());
        }
        CandidateExclusiveObjectRef::CircleAccess { circle_id, access } => {
            validate_circle_access_ref(*circle_id, family, access)?;
            return Ok(());
        }
        CandidateExclusiveObjectRef::SelfRetirement(reference) => (
            format!(
                "{}.json",
                device_self_retirement_semantic_prefix(
                    family,
                    &reference.target.device_id,
                    reference.retirement_hash,
                )
            ),
            &reference.object,
        ),
    };
    if object.slot().logical_key() != expected {
        return Err(StoreProtocolError::RelocatedCandidateObject {
            expected,
            actual: object.slot().logical_key().to_string(),
        });
    }
    Ok(())
}

fn validate_circle_access_ref(
    circle_id: CircleId,
    family: CandidateFamilyId,
    access: &CircleAccessObjectRef,
) -> Result<(), StoreProtocolError> {
    if access.leaf.owner_pubkey != access.envelope.owner_pubkey
        || access.leaf.recipient_slot != access.envelope.recipient_slot
        || access.leaf.leaf_id != access.envelope.leaf_id
        || access.leaf.leaf_hash != access.envelope.leaf_hash
        || access.leaf.leaf_hash != access.leaf.object.stored_hash()
    {
        return Err(StoreProtocolError::Malformed(
            "paired Circle access leaf and envelope references differ".to_string(),
        ));
    }
    let leaf_expected = circle_access_leaf_semantic_prefix(
        circle_id,
        family,
        &access.leaf.owner_pubkey,
        access.leaf.epoch_id,
        &access.leaf.recipient_slot,
        access.leaf.leaf_id,
    );
    if access.leaf.object.slot().logical_key() != leaf_expected {
        return Err(StoreProtocolError::RelocatedCandidateObject {
            expected: leaf_expected,
            actual: access.leaf.object.slot().logical_key().to_string(),
        });
    }
    let envelope_expected = format!(
        "{}.json",
        circle_access_envelope_semantic_prefix(
            circle_id,
            family,
            &access.envelope.owner_pubkey,
            &access.envelope.recipient_slot,
            access.envelope.control_hash,
        )
    );
    if access.envelope.object.slot().logical_key() != envelope_expected {
        return Err(StoreProtocolError::RelocatedCandidateObject {
            expected: envelope_expected,
            actual: access.envelope.object.slot().logical_key().to_string(),
        });
    }
    Ok(())
}

fn package_ref(
    semantic_prefix: &str,
    input: &StorePackageInput<'_>,
) -> Result<StorePackageRef, StoreProtocolError> {
    let package_bytes = input.bytes;
    let changeset_size =
        u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
    let content_hash = ObjectHash::digest(package_bytes);
    let expected_key = format!("{semantic_prefix}.pkg");
    if input.object.slot().logical_key() != expected_key {
        return Err(StoreProtocolError::RelocatedPackage {
            expected: expected_key,
            actual: input.object.slot().logical_key().to_string(),
        });
    }
    Ok(StorePackageRef {
        candidate_family: input.candidate_family,
        content_hash,
        schema_version: input.schema_version,
        changeset_size,
        object: input.object.clone(),
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
    author_registration: &StoreDeviceRegistrationRef,
    author_pubkey: &str,
    membership_state: &StoreMembershipStateRef,
    control: Option<&StoreControl>,
) -> Result<(), StoreProtocolError> {
    let Some(control) = control else {
        return Ok(());
    };
    if let StoreControl::MergeMembership { transition } = control {
        let StoreMembershipStateRef::MergeConcurrent(_) = membership_state else {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: membership_state.write_policy(),
            });
        };
        if policy != WritePolicy::MergeConcurrent
            || transition.body.author_registration != *author_registration
            || transition.body.entry.coord.author_pubkey != author_pubkey
            || transition.body.entry.coord.seq == 0
        {
            return Err(StoreProtocolError::InvalidMergeMembershipControl);
        }
        return Ok(());
    }
    if policy != WritePolicy::Serial {
        return Err(StoreProtocolError::ControlRequiresSerial);
    }
    if let Some(entry) = control.serial_membership_entry() {
        if entry.store_root_hash != store_root_hash
            || entry.author_pubkey != author_pubkey
            || !entry.verify()
        {
            return Err(StoreProtocolError::InvalidSerialControl);
        }
    }
    if control.key_generation() == Some(0) {
        return Err(StoreProtocolError::InvalidKeyGeneration(0));
    }
    Ok(())
}

fn validate_serial_recovery_activation(
    order: &StoreCommitOrder,
    activation: &SerialRecoveryActivation,
    author_registration: &StoreDeviceRegistrationRef,
) -> Result<(), StoreProtocolError> {
    if order.policy() != WritePolicy::Serial {
        return Err(StoreProtocolError::WritePolicyMismatch {
            expected: WritePolicy::Serial,
            actual: order.policy(),
        });
    }
    if &activation.registration.registration != author_registration
        || !matches!(
            activation.registration.authority,
            StoreDeviceRegistrationActivationRef::Recovery { .. }
        )
    {
        return Err(StoreProtocolError::Malformed(
            "Serial recovery activation does not bind its Recovery author registration".to_string(),
        ));
    }
    Ok(())
}

fn validate_parsed_control(
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> Result<(), StoreProtocolError> {
    validate_control(
        commit.policy(),
        commit.store_root_hash,
        &commit.author_registration,
        &author.author_pubkey,
        &commit.membership_state,
        commit.control(),
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

fn validate_circle_control_refs(
    policy: WritePolicy,
    controls: &[CircleControlRef],
) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for control_ref in controls {
        if !seen.insert(control_ref.circle_id()) {
            return Err(StoreProtocolError::DuplicateCircleControl(
                control_ref.circle_id(),
            ));
        }
        validate_circle_control_coord(policy, control_ref.control())?;
        if matches!(policy, WritePolicy::MergeConcurrent)
            != matches!(control_ref, CircleControlRef::MergeConcurrent { .. })
        {
            return Err(StoreProtocolError::CircleControlPolicyMismatch {
                expected: policy,
                actual: match control_ref {
                    CircleControlRef::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
                    CircleControlRef::Serial { .. } => WritePolicy::Serial,
                },
            });
        }
    }
    Ok(())
}

fn validate_device_registration_refs(
    registrations: &[ActivatedStoreDeviceRegistrationRef],
) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for activation in registrations {
        if !seen.insert(activation.registration.device_id) {
            return Err(StoreProtocolError::DuplicateDeviceRegistration {
                device_id: activation.registration.device_id.to_string(),
                revision: 1,
            });
        }
    }
    Ok(())
}

fn validate_device_exclusion_refs(
    proposals: &[StoreDeviceExclusionProposalRef],
    outcomes: &[StoreDeviceExclusionOutcomeRef],
) -> Result<(), StoreProtocolError> {
    if proposals.windows(2).any(|pair| pair[0] >= pair[1])
        || outcomes.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    let mut ids = BTreeSet::new();
    for proposal in proposals {
        proposal.validate_path()?;
        if !ids.insert(proposal.proposal_id) {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
    }
    for outcome in outcomes {
        let proposal = outcome.proposal();
        proposal.validate_path()?;
        let expected = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(
                proposal.target.device_id,
                proposal.proposal_id,
            )
        );
        if outcome.object().slot().logical_key() != expected || !ids.insert(proposal.proposal_id) {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
    }
    Ok(())
}

fn validate_commit_acknowledgement(
    acknowledgement: &Option<StoreAckRef>,
    author: &StoreDeviceRegistrationRef,
) -> Result<(), StoreProtocolError> {
    let Some(acknowledgement) = acknowledgement else {
        return Ok(());
    };
    let expected = format!(
        "{}.json",
        ack_slot_prefix(&author.device_id.to_string(), acknowledgement.sequence)
    );
    if acknowledgement.registration != *author
        || acknowledgement.sequence < 2
        || acknowledgement.object.slot().logical_key() != expected
    {
        return Err(StoreProtocolError::Malformed(
            "Store commit acknowledgement is not the author's exact non-initial acknowledgement"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_device_join_attempt_decision_refs(
    decisions: &[DeviceJoinAttemptDecisionRef],
) -> Result<(), StoreProtocolError> {
    if decisions
        .windows(2)
        .any(|pair| pair[0].attempt_id() >= pair[1].attempt_id())
    {
        return Err(StoreProtocolError::JoinAttemptMismatch);
    }
    Ok(())
}

fn validate_device_join_outcome_refs(
    outcomes: &[DeviceJoinOutcomeRef],
) -> Result<(), StoreProtocolError> {
    let mut attempts = BTreeSet::new();
    if outcomes.windows(2).any(|pair| pair[0] >= pair[1])
        || outcomes
            .iter()
            .any(|outcome| !attempts.insert(outcome.attempt().attempt_id))
    {
        return Err(StoreProtocolError::JoinOutcomeMismatch);
    }
    Ok(())
}

fn validate_device_join_cleanup_receipt_refs(
    receipts: &[super::device_join::DeviceJoinCleanupReceiptRef],
) -> Result<(), StoreProtocolError> {
    if receipts.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::JoinOutcomeMismatch);
    }
    Ok(())
}

fn validate_provider_access_refs(
    grants: &[super::provider::StoreMemberProviderAccessGrantRef],
    withdrawals: &[super::provider::StoreMemberProviderAccessWithdrawalReceiptRef],
) -> Result<(), StoreProtocolError> {
    if grants.windows(2).any(|pair| pair[0] >= pair[1])
        || withdrawals.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(StoreProtocolError::ProviderAccessMismatch);
    }
    let granted = grants
        .iter()
        .map(|reference| &reference.grant_id)
        .collect::<BTreeSet<_>>();
    if withdrawals
        .iter()
        .any(|reference| granted.contains(&reference.grant_id))
    {
        return Err(StoreProtocolError::ProviderAccessMismatch);
    }
    Ok(())
}

fn validate_device_retirement_refs(
    retirements: &[StoreDeviceSelfRetirementRef],
    candidate_family: CandidateFamilyId,
    author: &StoreDeviceRegistrationRef,
    order: &StoreCommitOrder,
) -> Result<(), StoreProtocolError> {
    if retirements.len() > 1 {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    let expected_cut = match order {
        StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => {
            let mut cut = dependencies.clone();
            if let Some(predecessor) = predecessor {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord else {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                };
                if cut
                    .insert(stream_id, predecessor.clone())
                    .is_some_and(|existing| existing != *predecessor)
                {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
            }
            StoreHistoryCut::MergeConcurrent(cut)
        }
        StoreCommitOrder::Serial { predecessor, .. } => {
            StoreHistoryCut::Serial(predecessor.clone())
        }
    };
    for retirement in retirements {
        if retirement.candidate_family != candidate_family
            || retirement.target != *author
            || retirement.retiring_cut != expected_cut
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCausalFloor {
    pub effective_coordinates: Vec<MembershipCoord>,
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
}

impl MembershipCausalFloor {
    pub fn from_membership(membership: &super::membership::MembershipChain) -> Self {
        Self {
            effective_coordinates: membership.effective_frontier(),
            resolutions: membership.resolution_refs().to_vec(),
        }
    }

    pub(crate) fn advance(
        &mut self,
        coordinate: super::membership::MembershipCoord,
        resolutions: &[StoreMembershipConflictResolutionRef],
    ) -> Result<(), StoreProtocolError> {
        let stream = coordinate.stream_key();
        self.effective_coordinates
            .retain(|current| current.stream_key() != stream);
        self.effective_coordinates.push(coordinate);
        self.effective_coordinates.sort();
        self.resolutions.extend_from_slice(resolutions);
        self.resolutions.sort();
        self.resolutions.dedup();
        self.validate()
    }

    fn validate(&self) -> Result<(), StoreProtocolError> {
        if self
            .effective_coordinates
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
            || self.resolutions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(StoreProtocolError::Malformed(
                "Merge history membership floor is not canonical".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedRegistration {
    pub reference: StoreDeviceRegistrationRef,
    pub value: StoreDeviceRegistration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedActivatedAck {
    #[serde(with = "ordered_map_entries")]
    pub chain: BTreeMap<u64, (StoreAckRef, StoreAck)>,
    pub activating_commit: StoreBatchCommitRef,
    pub activating_commit_value: StoreBatchCommit,
}

impl RetainedVerifiedActivatedAck {
    pub fn latest(&self) -> Option<&(StoreAckRef, StoreAck)> {
        self.chain
            .last_key_value()
            .map(|(_, acknowledgement)| acknowledgement)
    }

    pub fn exactly_extends(&self, predecessor: &Self) -> bool {
        self.chain.len() > predecessor.chain.len()
            && predecessor.chain.iter().all(|(sequence, acknowledgement)| {
                self.chain.get(sequence) == Some(acknowledgement)
            })
    }

    pub(crate) fn validate_chain(
        &self,
        root: &StoreRootRef,
        registration: &RetainedVerifiedRegistration,
    ) -> Result<(), StoreProtocolError> {
        if self.chain.is_empty() {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let mut predecessor: Option<&StoreAckRef> = None;
        for (expected_sequence, (sequence, (reference, value))) in (1_u64..).zip(self.chain.iter())
        {
            if *sequence != expected_sequence
                || reference.sequence != expected_sequence
                || value.sequence != expected_sequence
                || reference.registration != registration.reference
                || value.registration != registration.reference
                || value.successor.predecessor.as_ref()
                    != predecessor.map(|reference| &reference.object)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            reference
                .object
                .verify(&value.to_bytes())
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            StoreAck::parse_at(&value.to_bytes(), root, reference, &registration.value)?;
            predecessor = Some(reference);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedAcceptedStoreAnnouncement {
    pub reference: StoreDeviceHeadRef,
    pub value: StoreDeviceHead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMergeMembershipProof {
    pub commit: StoreBatchCommitRef,
    pub commit_value: StoreBatchCommit,
    pub announcement: Option<RetainedAcceptedStoreAnnouncement>,
    pub entry: MembershipEntryRef,
    pub entry_value: MembershipEntry,
    pub head: MembershipHeadRef,
    pub head_value: AuthorHead,
    pub resolution: Option<StoreMembershipConflictResolutionRef>,
    pub resolution_value: Option<StoreMembershipConflictResolution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedVerifiedMergeHistorySummary {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub policy: WritePolicy,
    #[serde(with = "ordered_map_entries")]
    pub causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    pub post_state: StoreDeviceStateRef,
    pub membership_floor: MembershipCausalFloor,
    #[serde(with = "ordered_map_entries")]
    pub registrations: BTreeMap<StoreDeviceId, RetainedVerifiedRegistration>,
    #[serde(with = "ordered_map_entries")]
    pub acknowledgements: BTreeMap<StoreDeviceId, RetainedVerifiedActivatedAck>,
    #[serde(with = "ordered_map_entries")]
    pub membership_proofs: BTreeMap<StoreBatchCommitRef, RetainedMergeMembershipProof>,
    #[serde(with = "ordered_map_entries")]
    pub announcement_frontier: BTreeMap<AuthorStreamId, RetainedAcceptedStoreAnnouncement>,
}

#[derive(Debug, Clone)]
pub(crate) struct OpenedRetainedMergeHistorySummary {
    pub(crate) summary: RetainedVerifiedMergeHistorySummary,
    pub(crate) announcement_frontier: BTreeMap<AuthorStreamId, RetainedAcceptedStoreAnnouncement>,
    pub(crate) post_state: ResolvedStoreDeviceState,
}

impl RetainedVerifiedMergeHistorySummary {
    pub fn digest(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(MERGE_HISTORY_SUMMARY_DOMAIN, self))
    }

    pub fn frontier(
        &self,
    ) -> Result<BTreeMap<AuthorStreamId, StoreBatchCommitRef>, StoreProtocolError> {
        let mut frontier = BTreeMap::new();
        for reference in self.causal_cut.values() {
            let StoreCommitCoord::MergeConcurrent {
                stream_id,
                sequence,
            } = reference.coord
            else {
                return Err(StoreProtocolError::WritePolicyMismatch {
                    expected: WritePolicy::MergeConcurrent,
                    actual: WritePolicy::Serial,
                });
            };
            match frontier.entry(stream_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(reference.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if sequence > entry.get().coord.sequence() {
                        entry.insert(reference.clone());
                    }
                }
            }
        }
        Ok(frontier)
    }

    pub fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        if self.policy != WritePolicy::MergeConcurrent
            || self.post_state.write_policy() != WritePolicy::MergeConcurrent
        {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: self.policy,
            });
        }
        self.membership_floor.validate()?;
        for (coord, reference) in &self.causal_cut {
            if coord != &reference.coord
                || !matches!(coord, StoreCommitCoord::MergeConcurrent { .. })
            {
                return Err(StoreProtocolError::Malformed(
                    "Merge history causal cut contains a mismatched coordinate".to_string(),
                ));
            }
        }
        let expected_frontier = CommitFrontier::MergeConcurrent(self.frontier()?);
        let StoreDeviceStateRef::MergeConcurrent { frontier, .. } = &self.post_state else {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            });
        };
        if frontier != &expected_frontier {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        for (device_id, registration) in &self.registrations {
            if device_id != &registration.reference.device_id
                || registration.value.store_root.store_root_hash != self.store_root_hash
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            registration
                .reference
                .verify_registration(&registration.value)?;
            registration
                .reference
                .object
                .verify(&registration.value.to_bytes())
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            StoreDeviceRegistration::parse_at(
                &registration.value.to_bytes(),
                &registration.value.store_root,
                *device_id,
            )?;
        }
        for (device_id, acknowledgement) in &self.acknowledgements {
            let registration = self
                .registrations
                .get(device_id)
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            acknowledgement.validate_chain(&registration.value.store_root, registration)?;
            let (acknowledgement_ref, acknowledgement_value) = acknowledgement
                .latest()
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            acknowledgement
                .activating_commit
                .verify_commit(&acknowledgement.activating_commit_value)?;
            if device_id != &acknowledgement_ref.registration.device_id
                || acknowledgement.activating_commit_value.acknowledgement()
                    != Some(acknowledgement_ref)
                || acknowledgement.activating_commit_value.author_registration
                    != registration.reference
                || self
                    .causal_cut
                    .get(&acknowledgement.activating_commit.coord)
                    != Some(&acknowledgement.activating_commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            let predecessor_cut = acknowledgement
                .activating_commit_value
                .order
                .predecessor_cut()?;
            if acknowledgement_value.store_cut != predecessor_cut
                || acknowledgement_value.device_state
                    != acknowledgement.activating_commit_value.device_state
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        for (reference, proof) in &self.membership_proofs {
            if reference != &proof.commit
                || self.causal_cut.get(&proof.commit.coord) != Some(&proof.commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof.commit.verify_commit(&proof.commit_value)?;
            let Some(StoreControl::MergeMembership { transition }) = proof.commit_value.control()
            else {
                return Err(StoreProtocolError::DeviceStateMismatch);
            };
            if transition.body.entry != proof.entry
                || proof.entry.coord != proof.entry_value.coord()
                || !super::membership::verify_membership_entry(&proof.entry_value)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .entry
                .object
                .verify(
                    &serde_json::to_vec(&proof.entry_value)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?,
                )
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            let head_author = self
                .registrations
                .get(&proof.head_value.body.author_registration.device_id)
                .ok_or(StoreProtocolError::DeviceStateMismatch)?;
            if !transition.matches_head(&proof.head_value, &proof.head)
                || !proof.head_value.verify(&head_author.value)
                || !matches!(
                    &proof.head_value.activation,
                    super::membership::MembershipHeadActivation::StoreCommit { commit }
                        if commit == &proof.commit
                )
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            proof
                .head
                .object
                .verify(
                    &serde_json::to_vec(&proof.head_value)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?,
                )
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
            match (
                &proof.entry_value.change,
                &proof.resolution,
                &proof.resolution_value,
            ) {
                (
                    super::membership::MembershipChange::ResolutionActivation { resolution },
                    Some(reference),
                    Some(value),
                ) if resolution == reference
                    && value.store_root_hash == self.store_root_hash
                    && value.resolution_ref(reference.object.clone()) == *reference
                    && value.verify_signature() =>
                {
                    reference
                        .object
                        .verify(
                            &serde_json::to_vec(value).map_err(|error| {
                                StoreProtocolError::Malformed(error.to_string())
                            })?,
                        )
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                }
                (super::membership::MembershipChange::ResolutionActivation { .. }, _, _)
                | (_, Some(_), _)
                | (_, _, Some(_)) => return Err(StoreProtocolError::DeviceStateMismatch),
                _ => {}
            }
            if let Some(announcement) = &proof.announcement {
                self.validate_announcement(announcement)?;
                if announcement.value.commit != proof.commit {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
            }
        }
        for (stream_id, announcement) in &self.announcement_frontier {
            self.validate_announcement(announcement)?;
            if !matches!(
                announcement.value.commit.coord,
                StoreCommitCoord::MergeConcurrent {
                    stream_id: announcement_stream,
                    ..
                } if announcement_stream == *stream_id
            ) || self.causal_cut.get(&announcement.value.commit.coord)
                != Some(&announcement.value.commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        Ok(())
    }

    pub fn validate_snapshot_baseline(&self) -> Result<(), StoreProtocolError> {
        self.validate_shape()?;
        let frontier = self.frontier()?;
        if self.announcement_frontier.len() != frontier.len()
            || frontier.iter().any(|(stream_id, commit)| {
                self.announcement_frontier
                    .get(stream_id)
                    .is_none_or(|announcement| announcement.value.commit != *commit)
            })
            || self
                .membership_proofs
                .values()
                .any(|proof| proof.announcement.is_none())
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }

    fn validate_announcement(
        &self,
        announcement: &RetainedAcceptedStoreAnnouncement,
    ) -> Result<(), StoreProtocolError> {
        let registration = self
            .registrations
            .get(&announcement.value.author_registration.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if announcement.value.store_root_hash != self.store_root_hash
            || announcement.value.author_registration != registration.reference
            || announcement.reference.head_hash != announcement.value.head_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        announcement
            .reference
            .object
            .verify(&announcement.value.to_bytes())
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        StoreDeviceHead::parse_at(
            &announcement.value.to_bytes(),
            self.store_root_hash,
            &registration.value,
            &announcement.value.commit,
        )?;
        Ok(())
    }

    pub(crate) fn open(
        &self,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        head: &StoreDeviceHead,
        head_ref: &StoreDeviceHeadRef,
        state: &ResolvedStoreDeviceState,
    ) -> Result<OpenedRetainedMergeHistorySummary, StoreProtocolError> {
        self.validate_shape()?;
        state.validate_canonical()?;
        commit_ref.verify_commit(commit)?;
        if self.store_root_hash != commit.store_root_hash
            || self.digest() != head.history_summary
            || head.commit != *commit_ref
            || head.head_hash() != head_ref.head_hash
            || !self.causal_cut.contains_key(&commit_ref.coord)
            || self.causal_cut.get(&commit_ref.coord) != Some(commit_ref)
            || self.post_state.state_hash() != state.state_hash
            || self.post_state.recovery() != state.recovery
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        head_ref
            .object
            .verify(&head.to_bytes())
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let registration = self
            .registrations
            .get(&commit.author_registration.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if registration.reference != commit.author_registration
            || registration.value.store_root.store_root_hash != self.store_root_hash
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        registration
            .reference
            .verify_registration(&registration.value)?;
        StoreDeviceHead::parse_at(
            &head.to_bytes(),
            self.store_root_hash,
            &registration.value,
            commit_ref,
        )?;
        let StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence,
        } = commit_ref.coord
        else {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            });
        };
        let frontier = self.frontier()?;
        for (accepted_stream, accepted_commit) in &frontier {
            if *accepted_stream == stream_id {
                if accepted_commit != commit_ref {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                continue;
            }
            if self
                .announcement_frontier
                .get(accepted_stream)
                .map(|announcement| &announcement.value.commit)
                != Some(accepted_commit)
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        let current_membership_floor_matches = match commit.control() {
            Some(StoreControl::MergeMembership { .. }) => {
                self.membership_proofs.get(commit_ref).is_some_and(|proof| {
                    self.membership_floor
                        .effective_coordinates
                        .contains(&proof.entry.coord)
                        && proof.head_value.body.resolutions.iter().all(|resolution| {
                            self.membership_floor
                                .resolutions
                                .binary_search(resolution)
                                .is_ok()
                        })
                })
            }
            _ => true,
        };
        if !current_membership_floor_matches
            || self
                .membership_proofs
                .iter()
                .any(|(reference, proof)| proof.announcement.is_none() && reference != commit_ref)
            || matches!(commit.control(), Some(StoreControl::MergeMembership { .. }))
                != self.membership_proofs.contains_key(commit_ref)
            || commit.acknowledgement().is_some_and(|reference| {
                self.acknowledgements
                    .get(&reference.registration.device_id)
                    .is_none_or(|acknowledgement| {
                        acknowledgement
                            .latest()
                            .is_none_or(|(retained, _)| retained != reference)
                            || acknowledgement.activating_commit != *commit_ref
                    })
            })
            || commit.device_registrations().iter().any(|activation| {
                self.registrations
                    .get(&activation.registration.device_id)
                    .is_none_or(|registration| registration.reference != activation.registration)
            })
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let predecessor = self.announcement_frontier.get(&stream_id);
        let first_slot = match &registration.value.store_commits {
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { first_slot },
            } => first_slot,
            StoreCommitAnchor::MergeConcurrent { .. } | StoreCommitAnchor::Serial => {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        };
        if predecessor.is_none() && (sequence != 1 || head_ref.object.slot() != first_slot)
            || predecessor
                .as_ref()
                .map(|accepted| accepted.value.commit.coord.sequence())
                .is_some_and(|previous| previous.checked_add(1) != Some(sequence))
            || head.successor.predecessor
                != predecessor.map(|accepted| accepted.reference.object.clone())
            || predecessor.is_some_and(|accepted| {
                accepted.value.successor.next_slot != *head_ref.object.slot()
            })
        {
            return Err(StoreProtocolError::Malformed(
                "Merge history head does not exactly extend its retained announcement frontier"
                    .to_string(),
            ));
        }
        let mut announcement_frontier = self.announcement_frontier.clone();
        announcement_frontier.insert(
            stream_id,
            RetainedAcceptedStoreAnnouncement {
                reference: head_ref.clone(),
                value: head.clone(),
            },
        );
        Ok(OpenedRetainedMergeHistorySummary {
            summary: self.clone(),
            announcement_frontier,
            post_state: state.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub commit: StoreBatchCommitRef,
    pub history_summary: ObjectHash,
    pub successor: SuccessorLink,
    pub signature: String,
}

#[derive(Serialize)]
struct HeadSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    author_registration: &'a StoreDeviceRegistrationRef,
    commit: &'a StoreBatchCommitRef,
    history_summary: ObjectHash,
    successor: &'a SuccessorLink,
}

impl StoreDeviceHead {
    pub fn signed(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        commit: StoreBatchCommitRef,
        history_summary: ObjectHash,
        successor: SuccessorLink,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        if commit.coord.sequence() == 0 {
            return Err(StoreProtocolError::InvalidSequence(0));
        }
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            author_registration,
            commit,
            history_summary,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &head.canonical_signed_bytes());
        head.signature = signature;
        Ok(head)
    }

    pub(crate) fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            HEAD_DOMAIN,
            &HeadSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                author_registration: &self.author_registration,
                commit: &self.commit,
                history_summary: self.history_summary,
                successor: &self.successor,
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
        self.commit.coord.sequence()
    }

    pub(crate) fn signature_is_valid_for(
        &self,
        expected_registration: &StoreDeviceRegistration,
    ) -> bool {
        keys::verify_signature_hex(
            &expected_registration.device_signing_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        )
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected_registration: &StoreDeviceRegistration,
        expected_ref: &StoreBatchCommitRef,
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
        head.author_registration
            .verify_registration(expected_registration)?;
        if &head.commit != expected_ref {
            return Err(StoreProtocolError::Malformed(
                "Store head activates a different exact commit".to_string(),
            ));
        }
        if head.commit.coord.sequence() == 0 {
            return Err(StoreProtocolError::InvalidSequence(0));
        }
        if !head.signature_is_valid_for(expected_registration) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(head)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceHeadRef {
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// Signed global activation point for a Serial Store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSerialHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub state: StoreSerialHeadState,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreSerialHeadState {
    Genesis {
        root: StoreRootRef,
        founder_registration: StoreDeviceRegistrationRef,
    },
    Commit {
        author_registration: StoreDeviceRegistrationRef,
        commit: StoreBatchCommitRef,
    },
}

#[derive(Serialize)]
struct SerialHeadSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    state: &'a StoreSerialHeadState,
}

impl StoreSerialHead {
    pub fn signed(
        store_root_hash: ObjectHash,
        state: StoreSerialHeadState,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_serial_head_state(&state)?;
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            state,
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
                state: &self.state,
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
        executor: &StoreDeviceRegistration,
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
        validate_serial_head_state(&head.state)?;
        match &head.state {
            StoreSerialHeadState::Genesis {
                founder_registration,
                ..
            } => founder_registration.verify_registration(executor)?,
            StoreSerialHeadState::Commit {
                author_registration,
                ..
            } => author_registration.verify_registration(executor)?,
        }
        if !keys::verify_signature_hex(
            &executor.device_signing_pubkey,
            &head.signature,
            &head.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(head)
    }
}

fn validate_serial_head_state(state: &StoreSerialHeadState) -> Result<(), StoreProtocolError> {
    match state {
        StoreSerialHeadState::Genesis { .. } => Ok(()),
        StoreSerialHeadState::Commit { commit, .. } => match commit.coord {
            StoreCommitCoord::Serial { sequence } if sequence > 0 => Ok(()),
            _ => Err(StoreProtocolError::InvalidSerialHead),
        },
    }
}

const STORE_DEVICE_ID_DOMAIN: &[u8] = b"coven.store-device-id.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreDeviceId(ObjectHash);

impl StoreDeviceId {
    pub fn derive(store_root: &StoreRootRef, origin: &StoreDeviceRegistrationOrigin) -> Self {
        let mut material = STORE_DEVICE_ID_DOMAIN.to_vec();
        material.extend(
            serde_json::to_vec(&(store_root, origin.external_id()))
                .expect("Store device identity serialization cannot fail"),
        );
        Self(ObjectHash::digest(&material))
    }
}

impl fmt::Display for StoreDeviceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl FromStr for StoreDeviceId {
    type Err = StoreProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreCreationId(ObjectHash);

impl StoreCreationId {
    pub fn from_random_bytes(bytes: [u8; 32]) -> Self {
        Self(ObjectHash::from_digest(bytes))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn from_nonce(nonce: &str) -> Self {
        Self(ObjectHash::digest(nonce.as_bytes()))
    }
}

impl fmt::Display for StoreCreationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceJoinAttemptId(ObjectHash);

impl DeviceJoinAttemptId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceRecoveryId(ObjectHash);

impl DeviceRecoveryId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAttemptRef {
    pub attempt_id: DeviceJoinAttemptId,
    pub attempt_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinAttempt {
    pub version: u32,
    pub store_root: StoreRootRef,
    pub attempt_id: DeviceJoinAttemptId,
    pub attempt_slot: ObjectSlot,
    pub expected_registration: StoreDeviceRegistration,
    pub registration_slot: ObjectSlot,
    pub outcome_slot: ObjectSlot,
    pub bootstrap_cut: StoreHistoryCut,
    pub membership: StoreMembershipStateRef,
    pub provider_admin_grant: super::provider::ProviderAdminGrantId,
    pub provider_approval: super::device_join::DeviceProviderAdmissionApproval,
    pub provider_response: super::device_join::DeviceProviderResponseReservation,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

pub(crate) struct UnverifiedDeviceJoinAttempt(DeviceJoinAttempt);

impl UnverifiedDeviceJoinAttempt {
    pub(crate) fn verify_at(
        self,
        expected: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<DeviceJoinAttempt, StoreProtocolError> {
        let attempt = self.0;
        require_version(attempt.version)?;
        attempt.validate_shape()?;
        if attempt.attempt_id != expected.attempt_id
            || attempt.attempt_hash() != expected.attempt_hash
            || &attempt.attempt_slot != expected.object.slot()
        {
            return Err(StoreProtocolError::JoinAttemptMismatch);
        }
        attempt.owner_registration.verify_registration(owner)?;
        if !keys::verify_signature_hex(
            &owner.device_signing_pubkey,
            &attempt.signature,
            &attempt.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(attempt)
    }
}

#[derive(Serialize)]
struct DeviceJoinAttemptSignedFields<'a> {
    version: u32,
    store_root: &'a StoreRootRef,
    attempt_id: DeviceJoinAttemptId,
    attempt_slot: &'a ObjectSlot,
    expected_registration: &'a StoreDeviceRegistration,
    registration_slot: &'a ObjectSlot,
    outcome_slot: &'a ObjectSlot,
    bootstrap_cut: &'a StoreHistoryCut,
    membership: &'a StoreMembershipStateRef,
    provider_admin_grant: &'a super::provider::ProviderAdminGrantId,
    provider_approval: &'a super::device_join::DeviceProviderAdmissionApproval,
    provider_response: &'a super::device_join::DeviceProviderResponseReservation,
    owner_registration: &'a StoreDeviceRegistrationRef,
    owner_grant: &'a MembershipGrantId,
}

impl DeviceJoinAttempt {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root: StoreRootRef,
        attempt_id: DeviceJoinAttemptId,
        attempt_slot: ObjectSlot,
        expected_registration: StoreDeviceRegistration,
        registration_slot: ObjectSlot,
        outcome_slot: ObjectSlot,
        bootstrap_cut: StoreHistoryCut,
        membership: StoreMembershipStateRef,
        provider_admin_grant: super::provider::ProviderAdminGrantId,
        provider_approval: super::device_join::DeviceProviderAdmissionApproval,
        provider_response: super::device_join::DeviceProviderResponseReservation,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut attempt = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root,
            attempt_id,
            attempt_slot,
            expected_registration,
            registration_slot,
            outcome_slot,
            bootstrap_cut,
            membership,
            provider_admin_grant,
            provider_approval,
            provider_response,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        attempt.validate_shape()?;
        let (_, signature) = keys::sign_hex(owner_device_signer, &attempt.canonical_signed_bytes());
        attempt.signature = signature;
        Ok(attempt)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_JOIN_ATTEMPT_DOMAIN,
            &DeviceJoinAttemptSignedFields {
                version: self.version,
                store_root: &self.store_root,
                attempt_id: self.attempt_id,
                attempt_slot: &self.attempt_slot,
                expected_registration: &self.expected_registration,
                registration_slot: &self.registration_slot,
                outcome_slot: &self.outcome_slot,
                bootstrap_cut: &self.bootstrap_cut,
                membership: &self.membership,
                provider_admin_grant: &self.provider_admin_grant,
                provider_approval: &self.provider_approval,
                provider_response: &self.provider_response,
                owner_registration: &self.owner_registration,
                owner_grant: &self.owner_grant,
            },
        )
    }

    pub fn attempt_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("DeviceJoinAttempt serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        Self::parse_unverified(bytes)?.verify_at(expected, owner)
    }

    pub(crate) fn parse_unverified(
        bytes: &[u8],
    ) -> Result<UnverifiedDeviceJoinAttempt, StoreProtocolError> {
        serde_json::from_slice(bytes)
            .map(UnverifiedDeviceJoinAttempt)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
    }

    fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        let registration_policy = match &self.expected_registration.store_commits {
            StoreCommitAnchor::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            StoreCommitAnchor::Serial => WritePolicy::Serial,
        };
        validate_store_history_cut(&self.bootstrap_cut)?;
        if !matches!(
            (registration_policy, &self.bootstrap_cut),
            (
                WritePolicy::MergeConcurrent,
                StoreHistoryCut::MergeConcurrent(_)
            ) | (WritePolicy::Serial, StoreHistoryCut::Serial(_))
        ) {
            return Err(StoreProtocolError::JoinAttemptMismatch);
        }
        if self.expected_registration.store_root != self.store_root
            || self.expected_registration.device_id
                != StoreDeviceId::derive(&self.store_root, &self.expected_registration.origin)
            || self.attempt_slot == self.registration_slot
            || self.attempt_slot == self.outcome_slot
            || self.registration_slot == self.outcome_slot
            || self.membership.write_policy() != registration_policy
            || self.provider_admin_grant
                != self.provider_approval.request.offer.provider_admin.grant_id
            || self.provider_approval.request.offer.store_root != self.store_root
            || self.provider_approval.request.offer.attempt_id != self.attempt_id
            || self.provider_approval.request.offer.attempt_slot != self.attempt_slot
            || self.provider_approval.request.offer.outcome_slot != self.outcome_slot
            || self.provider_approval.request.offer.owner_registration != self.owner_registration
            || self.provider_approval.request.offer.owner_grant != self.owner_grant
            || self.provider_approval.request.offer.member_pubkey
                != self.expected_registration.author_pubkey
            || self.provider_approval.request.peer_provider != self.expected_registration.provider
        {
            return Err(StoreProtocolError::JoinAttemptMismatch);
        }
        match (&self.provider_approval.admission, &self.provider_response) {
            (
                super::device_join::DeviceProviderAdmissionChallenge::SamePrincipal,
                super::device_join::DeviceProviderResponseReservation::SamePrincipal,
            )
            | (
                super::device_join::DeviceProviderAdmissionChallenge::CrossPrincipal(_),
                super::device_join::DeviceProviderResponseReservation::CrossPrincipal { .. },
            ) => {}
            _ => return Err(StoreProtocolError::JoinAttemptMismatch),
        }
        match &self.expected_registration.origin {
            StoreDeviceRegistrationOrigin::Join {
                attempt_id,
                attempt_slot,
                outcome_slot,
            } if *attempt_id == self.attempt_id
                && attempt_slot == &self.attempt_slot
                && outcome_slot == &self.outcome_slot =>
            {
                Ok(())
            }
            _ => Err(StoreProtocolError::JoinAttemptMismatch),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceReadinessProof {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub attempt: DeviceJoinAttemptRef,
    pub registration: StoreDeviceRegistrationRef,
    pub initial_ack: StoreAckRef,
    pub bootstrap_cut: StoreHistoryCut,
    pub signature: String,
}

#[derive(Serialize)]
struct DeviceReadinessSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    attempt: &'a DeviceJoinAttemptRef,
    registration: &'a StoreDeviceRegistrationRef,
    initial_ack: &'a StoreAckRef,
    bootstrap_cut: &'a StoreHistoryCut,
}

impl DeviceReadinessProof {
    pub fn signed(
        attempt: DeviceJoinAttemptRef,
        registration: StoreDeviceRegistrationRef,
        initial_ack: StoreAckRef,
        bootstrap_cut: StoreHistoryCut,
        registration_value: &StoreDeviceRegistration,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        registration.verify_registration(registration_value)?;
        if keys::public_key_hex(device_signer) != registration_value.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut proof = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: registration_value.store_root.store_root_hash,
            attempt,
            registration,
            initial_ack,
            bootstrap_cut,
            signature: String::new(),
        };
        validate_store_history_cut(&proof.bootstrap_cut)?;
        let (_, signature) = keys::sign_hex(device_signer, &proof.canonical_signed_bytes());
        proof.signature = signature;
        Ok(proof)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_READINESS_DOMAIN,
            &DeviceReadinessSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                attempt: &self.attempt,
                registration: &self.registration,
                initial_ack: &self.initial_ack,
                bootstrap_cut: &self.bootstrap_cut,
            },
        )
    }

    pub fn verify(
        &self,
        attempt_ref: &DeviceJoinAttemptRef,
        attempt: &DeviceJoinAttempt,
        registration: &StoreDeviceRegistration,
        initial_ack_ref: &StoreAckRef,
        initial_ack: &StoreAck,
    ) -> Result<(), StoreProtocolError> {
        require_version(self.version)?;
        if &self.attempt != attempt_ref
            || attempt_ref.attempt_id != attempt.attempt_id
            || attempt_ref.attempt_hash != attempt.attempt_hash()
            || self.store_root_hash != registration.store_root.store_root_hash
            || self.registration.device_id != registration.device_id
            || self.bootstrap_cut != attempt.bootstrap_cut
        {
            return Err(StoreProtocolError::DeviceReadinessMismatch);
        }
        self.registration.verify_registration(registration)?;
        if initial_ack.registration != self.registration
            || initial_ack.sequence != 1
            || initial_ack.successor.predecessor.is_some()
            || initial_ack_ref != &self.initial_ack
            || initial_ack_ref.registration != self.registration
            || initial_ack_ref.sequence != initial_ack.sequence
            || initial_ack_ref.ack_hash != initial_ack.ack_hash()
            || initial_ack.store_cut != self.bootstrap_cut
        {
            return Err(StoreProtocolError::DeviceReadinessMismatch);
        }
        validate_store_history_cut(&self.bootstrap_cut)?;
        if !keys::verify_signature_hex(
            &registration.device_signing_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinOutcomeBody {
    Activated { readiness: DeviceReadinessProof },
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinOutcome {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub attempt: DeviceJoinAttemptRef,
    pub body: DeviceJoinOutcomeBody,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Serialize)]
struct DeviceJoinOutcomeSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    attempt: &'a DeviceJoinAttemptRef,
    body: &'a DeviceJoinOutcomeBody,
    owner_registration: &'a StoreDeviceRegistrationRef,
    owner_grant: &'a MembershipGrantId,
}

impl DeviceJoinOutcome {
    pub fn signed(
        attempt: DeviceJoinAttemptRef,
        body: DeviceJoinOutcomeBody,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut outcome = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: owner.store_root.store_root_hash,
            attempt,
            body,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(owner_device_signer, &outcome.canonical_signed_bytes());
        outcome.signature = signature;
        Ok(outcome)
    }

    pub(crate) fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_JOIN_OUTCOME_DOMAIN,
            &DeviceJoinOutcomeSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                attempt: &self.attempt,
                body: &self.body,
                owner_registration: &self.owner_registration,
                owner_grant: &self.owner_grant,
            },
        )
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("DeviceJoinOutcome serialization cannot fail")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceJoinOutcomeRef {
    Activated {
        attempt: DeviceJoinAttemptRef,
        outcome_hash: ObjectHash,
        object: ExactObjectRef,
    },
    Cancelled {
        attempt: DeviceJoinAttemptRef,
        outcome_hash: ObjectHash,
        object: ExactObjectRef,
    },
}

impl DeviceJoinOutcomeRef {
    pub fn slot(&self) -> &ObjectSlot {
        self.object().slot()
    }

    pub fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Activated { object, .. } | Self::Cancelled { object, .. } => object,
        }
    }

    pub fn attempt(&self) -> &DeviceJoinAttemptRef {
        match self {
            Self::Activated { attempt, .. } | Self::Cancelled { attempt, .. } => attempt,
        }
    }

    pub fn verify_outcome(&self, outcome: &DeviceJoinOutcome) -> Result<(), StoreProtocolError> {
        let (attempt, expected_hash, expects_activated) = match self {
            Self::Activated {
                attempt,
                outcome_hash,
                ..
            } => (attempt, outcome_hash, true),
            Self::Cancelled {
                attempt,
                outcome_hash,
                ..
            } => (attempt, outcome_hash, false),
        };
        if &outcome.attempt != attempt || outcome.outcome_hash() != *expected_hash {
            return Err(StoreProtocolError::JoinOutcomeMismatch);
        }
        if expects_activated != matches!(outcome.body, DeviceJoinOutcomeBody::Activated { .. }) {
            return Err(StoreProtocolError::JoinOutcomeMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecoveryNodeRef {
    pub owner_pubkey: String,
    pub owner_grant: MembershipGrantId,
    pub sequence: u64,
    pub node_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OwnerRecoveryActivationId(ObjectHash);

impl OwnerRecoveryActivationId {
    pub fn derive(
        root: &StoreRootRef,
        owner_pubkey: &str,
        owner_grant: &MembershipGrantId,
        anchor: &GrantStreamAnchor,
    ) -> Result<Self, StoreProtocolError> {
        if !matches!(anchor, GrantStreamAnchor::OwnerRecovery { .. }) {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(Self(ObjectHash::digest(&domain_json(
            b"coven.owner-recovery-activation.v1\0",
            &(root, owner_pubkey, owner_grant, anchor),
        ))))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerRecoveryPosition {
    BeforeFirst {
        activation: OwnerRecoveryActivationId,
    },
    At {
        node: OwnerRecoveryNodeRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecoveryCursor {
    pub owner_grant: MembershipGrantId,
    pub position: OwnerRecoveryPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceStateRef {
    MergeConcurrent {
        frontier: CommitFrontier,
        recovery: Vec<OwnerRecoveryCursor>,
        state_hash: ObjectHash,
    },
    Serial {
        position: SerialStorePosition,
        recovery: Vec<OwnerRecoveryCursor>,
        state_hash: ObjectHash,
    },
}

impl StoreDeviceStateRef {
    pub fn merge_concurrent(
        frontier: CommitFrontier,
        state: &ResolvedStoreDeviceState,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_frontier(&frontier)?;
        if !matches!(frontier, CommitFrontier::MergeConcurrent(_)) {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            });
        }
        validate_recovery_cursors(&state.recovery)?;
        Ok(Self::MergeConcurrent {
            frontier,
            recovery: state.recovery.clone(),
            state_hash: state.state_hash,
        })
    }

    pub fn serial(
        position: SerialStorePosition,
        state: &ResolvedStoreDeviceState,
    ) -> Result<Self, StoreProtocolError> {
        validate_recovery_cursors(&state.recovery)?;
        Ok(Self::Serial {
            position,
            recovery: state.recovery.clone(),
            state_hash: state.state_hash,
        })
    }

    pub fn state_hash(&self) -> ObjectHash {
        match self {
            Self::MergeConcurrent { state_hash, .. } | Self::Serial { state_hash, .. } => {
                *state_hash
            }
        }
    }

    pub fn recovery(&self) -> &[OwnerRecoveryCursor] {
        match self {
            Self::MergeConcurrent { recovery, .. } | Self::Serial { recovery, .. } => recovery,
        }
    }

    pub fn write_policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial { .. } => WritePolicy::Serial,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceStatus {
    Active,
    Inactive {
        terminals: Vec<StoreDeviceTerminalRef>,
        accepted_cut: StoreHistoryCut,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRecord {
    pub registration: StoreDeviceRegistrationRef,
    pub proposals: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    pub status: StoreDeviceStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreDeviceExclusionProposalId(ObjectHash);

impl StoreDeviceExclusionProposalId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }
}

impl fmt::Display for StoreDeviceExclusionProposalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionProposalRef {
    pub proposal_id: StoreDeviceExclusionProposalId,
    pub target: StoreDeviceRegistrationRef,
    pub proposal_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionRef {
    pub proposal: StoreDeviceExclusionProposalRef,
    pub outcome_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionCancellationRef {
    pub proposal: StoreDeviceExclusionProposalRef,
    pub outcome_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionOutcomeRef {
    Excluded(StoreDeviceExclusionRef),
    Cancelled(StoreDeviceExclusionCancellationRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedStoreDeviceOperations {
    proposals: Vec<(
        RetainedStoreDeviceExclusionProposal,
        StoreDeviceExclusionProposal,
    )>,
    outcomes: Vec<VerifiedStoreDeviceExclusionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VerifiedStoreDeviceExclusionOutcome {
    Excluded {
        source: RetainedStoreDeviceExclusionOutcome,
        accepted_cut: StoreHistoryCut,
    },
    Cancelled(RetainedStoreDeviceExclusionOutcome),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedStoreDeviceRegistrationActivations {
    registrations: Vec<RetainedStoreDeviceRegistrationActivation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedStoreDeviceRegistrationActivation {
    canonical_registration: Vec<u8>,
    authority: StoreDeviceRegistrationActivation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedStoreDeviceOperations {
    proposals: Vec<RetainedStoreDeviceExclusionProposal>,
    outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedStoreDeviceExclusionProposal {
    reference: StoreDeviceExclusionProposalRef,
    canonical_proposal: Vec<u8>,
    canonical_target_registration: Vec<u8>,
    canonical_owner_registration: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedStoreDeviceExclusionOutcome {
    Excluded {
        reference: StoreDeviceExclusionRef,
        canonical_outcome: Vec<u8>,
        proposal: RetainedStoreDeviceExclusionProposal,
        canonical_owner_registration: Vec<u8>,
    },
    Cancelled {
        reference: StoreDeviceExclusionCancellationRef,
        canonical_outcome: Vec<u8>,
        proposal: RetainedStoreDeviceExclusionProposal,
        canonical_owner_registration: Vec<u8>,
    },
}

impl VerifiedStoreDeviceOperations {
    pub(crate) fn proposals(
        &self,
    ) -> impl ExactSizeIterator<
        Item = (
            &StoreDeviceExclusionProposalRef,
            &StoreDeviceExclusionProposal,
        ),
    > {
        self.proposals
            .iter()
            .map(|(source, proposal)| (&source.reference, proposal))
    }

    pub(crate) fn exclusions(
        &self,
    ) -> impl Iterator<Item = (&StoreDeviceExclusionRef, &StoreHistoryCut)> {
        self.outcomes.iter().filter_map(|outcome| match outcome {
            VerifiedStoreDeviceExclusionOutcome::Excluded {
                source,
                accepted_cut,
            } => Some((source.exclusion_reference(), accepted_cut)),
            VerifiedStoreDeviceExclusionOutcome::Cancelled(_) => None,
        })
    }

    pub(crate) fn from_retained_sources(
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
        proposals: Vec<RetainedStoreDeviceExclusionProposal>,
        outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
    ) -> Result<Self, StoreProtocolError> {
        let proposal_refs = proposals
            .iter()
            .map(|source| source.reference.clone())
            .collect::<Vec<_>>();
        let outcome_refs = outcomes
            .iter()
            .map(RetainedStoreDeviceExclusionOutcome::wire_reference)
            .collect::<Vec<_>>();
        if proposal_refs.as_slice() != commit.device_exclusion_proposals()
            || outcome_refs.as_slice() != commit.device_exclusion_outcomes()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let retained = RetainedStoreDeviceOperations {
            proposals: proposals.clone(),
            outcomes: outcomes.clone(),
        };
        let proposals = proposals
            .into_iter()
            .map(|source| {
                let proposal = source.verify(root)?;
                if proposal.frozen_device_state != commit.device_state {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                Ok((source, proposal))
            })
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        let outcomes = outcomes
            .into_iter()
            .map(|source| source.verify(root, commit))
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        let verified = Self {
            proposals,
            outcomes,
        };
        if verified.to_retained() != retained {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(verified)
    }

    pub(crate) fn without_exclusions(
        commit: &StoreBatchCommit,
    ) -> Result<Self, StoreProtocolError> {
        if !commit.device_exclusion_proposals().is_empty()
            || !commit.device_exclusion_outcomes().is_empty()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(Self {
            proposals: Vec::new(),
            outcomes: Vec::new(),
        })
    }

    pub(crate) fn to_retained(&self) -> RetainedStoreDeviceOperations {
        RetainedStoreDeviceOperations {
            proposals: self
                .proposals
                .iter()
                .map(|(source, _)| source.clone())
                .collect(),
            outcomes: self
                .outcomes
                .iter()
                .map(VerifiedStoreDeviceExclusionOutcome::source)
                .cloned()
                .collect(),
        }
    }

    pub(crate) fn apply_to(
        &self,
        predecessor: ResolvedStoreDeviceState,
        predecessor_ref: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, StoreProtocolError> {
        let mut state = predecessor;
        for (source, proposal) in &self.proposals {
            state = state.propose_exclusion(source.reference.clone(), proposal, predecessor_ref)?;
        }
        for outcome in &self.outcomes {
            state = match outcome {
                VerifiedStoreDeviceExclusionOutcome::Excluded {
                    source,
                    accepted_cut,
                } => state.exclude(source.exclusion_reference().clone(), accepted_cut.clone())?,
                VerifiedStoreDeviceExclusionOutcome::Cancelled(source) => {
                    state.cancel_exclusion(source.cancellation_reference().clone())?
                }
            };
        }
        Ok(state)
    }
}

impl RetainedStoreDeviceRegistrationActivations {
    pub(crate) fn from_verified(
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
        registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
    ) -> Result<Self, StoreProtocolError> {
        if registrations.len() != commit.device_registrations().len() {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let retained = Self {
            registrations: registrations
                .iter()
                .map(
                    |(registration, authority)| RetainedStoreDeviceRegistrationActivation {
                        canonical_registration: registration.to_bytes(),
                        authority: authority.clone(),
                    },
                )
                .collect(),
        };
        retained.verify_for(root, commit)?;
        Ok(retained)
    }

    pub(crate) fn verify_for(
        &self,
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
    ) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, StoreProtocolError>
    {
        if self.registrations.len() != commit.device_registrations().len() {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        commit
            .device_registrations()
            .iter()
            .zip(&self.registrations)
            .map(|(activated, retained)| retained.verify(root, activated))
            .collect()
    }
}

impl RetainedStoreDeviceRegistrationActivation {
    fn verify(
        &self,
        root: &StoreRootRef,
        activated: &ActivatedStoreDeviceRegistrationRef,
    ) -> Result<(StoreDeviceRegistration, StoreDeviceRegistrationActivation), StoreProtocolError>
    {
        let registration = verify_retained_registration(
            root,
            &activated.registration,
            &self.canonical_registration,
        )?;
        verify_registration_activation_binding(activated, &registration, &self.authority)?;
        Ok((registration, self.authority.clone()))
    }
}

impl RetainedStoreDeviceOperations {
    pub(crate) fn from_sources(
        proposals: Vec<RetainedStoreDeviceExclusionProposal>,
        outcomes: Vec<RetainedStoreDeviceExclusionOutcome>,
    ) -> Self {
        Self {
            proposals,
            outcomes,
        }
    }

    pub(crate) fn verify_for(
        &self,
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
    ) -> Result<VerifiedStoreDeviceOperations, StoreProtocolError> {
        VerifiedStoreDeviceOperations::from_retained_sources(
            root,
            commit,
            self.proposals.clone(),
            self.outcomes.clone(),
        )
    }
}

impl RetainedStoreDeviceExclusionProposal {
    pub(crate) fn from_exact(
        reference: StoreDeviceExclusionProposalRef,
        proposal: &StoreDeviceExclusionProposal,
        target: &StoreDeviceRegistration,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let retained = Self {
            reference,
            canonical_proposal: proposal.to_bytes(),
            canonical_target_registration: target.to_bytes(),
            canonical_owner_registration: owner.to_bytes(),
        };
        let (opened_proposal, opened_target, opened_owner) =
            retained.verify_with_registrations(&target.store_root)?;
        if opened_proposal != *proposal || opened_target != *target || opened_owner != *owner {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(retained)
    }

    pub(crate) fn from_verified(
        proposal: &super::store_objects::VerifiedDeviceExclusionProposal,
    ) -> Self {
        Self {
            reference: proposal.reference.clone(),
            canonical_proposal: proposal.object.bytes.clone(),
            canonical_target_registration: proposal.target.to_bytes(),
            canonical_owner_registration: proposal.owner.to_bytes(),
        }
    }

    pub(crate) fn reference(&self) -> &StoreDeviceExclusionProposalRef {
        &self.reference
    }

    fn verify(
        &self,
        root: &StoreRootRef,
    ) -> Result<StoreDeviceExclusionProposal, StoreProtocolError> {
        self.verify_with_registrations(root)
            .map(|(proposal, _, _)| proposal)
    }

    fn verify_with_registrations(
        &self,
        root: &StoreRootRef,
    ) -> Result<
        (
            StoreDeviceExclusionProposal,
            StoreDeviceRegistration,
            StoreDeviceRegistration,
        ),
        StoreProtocolError,
    > {
        self.reference
            .object
            .verify(&self.canonical_proposal)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let unverified: StoreDeviceExclusionProposal =
            serde_json::from_slice(&self.canonical_proposal)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if unverified.to_bytes() != self.canonical_proposal {
            return Err(StoreProtocolError::Malformed(
                "retained Store device exclusion proposal is not canonically encoded".to_string(),
            ));
        }
        let target = verify_retained_registration(
            root,
            &unverified.target,
            &self.canonical_target_registration,
        )?;
        let owner = verify_retained_registration(
            root,
            &unverified.owner_registration,
            &self.canonical_owner_registration,
        )?;
        let proposal = StoreDeviceExclusionProposal::parse_at(
            &self.canonical_proposal,
            &self.reference,
            &target,
            &owner,
        )?;
        Ok((proposal, target, owner))
    }
}

impl RetainedStoreDeviceExclusionOutcome {
    pub(crate) fn from_exact(
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: RetainedStoreDeviceExclusionProposal,
        outcome: &StoreDeviceExclusionOutcome,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        if reference.proposal() != outcome.proposal()
            || reference.outcome_hash() != outcome.outcome_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let canonical_outcome = outcome.to_bytes();
        reference
            .object()
            .verify(&canonical_outcome)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        Ok(match (reference, outcome) {
            (
                StoreDeviceExclusionOutcomeRef::Excluded(reference),
                StoreDeviceExclusionOutcome::Excluded(_),
            ) => Self::Excluded {
                reference: reference.clone(),
                canonical_outcome,
                proposal,
                canonical_owner_registration: owner.to_bytes(),
            },
            (
                StoreDeviceExclusionOutcomeRef::Cancelled(reference),
                StoreDeviceExclusionOutcome::Cancelled(_),
            ) => Self::Cancelled {
                reference: reference.clone(),
                canonical_outcome,
                proposal,
                canonical_owner_registration: owner.to_bytes(),
            },
            _ => return Err(StoreProtocolError::DeviceStateMismatch),
        })
    }

    pub(crate) fn from_verified(
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: RetainedStoreDeviceExclusionProposal,
        outcome: &super::store_objects::VerifiedDeviceExclusionOutcome,
    ) -> Result<Self, StoreProtocolError> {
        match (reference, &outcome.object.value) {
            (
                StoreDeviceExclusionOutcomeRef::Excluded(reference),
                StoreDeviceExclusionOutcome::Excluded(_),
            ) => Ok(Self::Excluded {
                reference: reference.clone(),
                canonical_outcome: outcome.object.bytes.clone(),
                proposal,
                canonical_owner_registration: outcome.owner.to_bytes(),
            }),
            (
                StoreDeviceExclusionOutcomeRef::Cancelled(reference),
                StoreDeviceExclusionOutcome::Cancelled(_),
            ) => Ok(Self::Cancelled {
                reference: reference.clone(),
                canonical_outcome: outcome.object.bytes.clone(),
                proposal,
                canonical_owner_registration: outcome.owner.to_bytes(),
            }),
            _ => Err(StoreProtocolError::DeviceStateMismatch),
        }
    }

    pub(crate) fn wire_reference(&self) -> StoreDeviceExclusionOutcomeRef {
        match self {
            Self::Excluded { reference, .. } => {
                StoreDeviceExclusionOutcomeRef::Excluded(reference.clone())
            }
            Self::Cancelled { reference, .. } => {
                StoreDeviceExclusionOutcomeRef::Cancelled(reference.clone())
            }
        }
    }

    fn exclusion_reference(&self) -> &StoreDeviceExclusionRef {
        match self {
            Self::Excluded { reference, .. } => reference,
            Self::Cancelled { .. } => unreachable!("verified exclusion changed variant"),
        }
    }

    fn cancellation_reference(&self) -> &StoreDeviceExclusionCancellationRef {
        match self {
            Self::Cancelled { reference, .. } => reference,
            Self::Excluded { .. } => unreachable!("verified cancellation changed variant"),
        }
    }

    fn verify(
        self,
        root: &StoreRootRef,
        commit: &StoreBatchCommit,
    ) -> Result<VerifiedStoreDeviceExclusionOutcome, StoreProtocolError> {
        let (reference, canonical_outcome, proposal_source, canonical_owner_registration) =
            match &self {
                Self::Excluded {
                    reference,
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                } => (
                    StoreDeviceExclusionOutcomeRef::Excluded(reference.clone()),
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                ),
                Self::Cancelled {
                    reference,
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                } => (
                    StoreDeviceExclusionOutcomeRef::Cancelled(reference.clone()),
                    canonical_outcome,
                    proposal,
                    canonical_owner_registration,
                ),
            };
        reference
            .object()
            .verify(canonical_outcome)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let (proposal, target, _) = proposal_source.verify_with_registrations(root)?;
        let unverified: StoreDeviceExclusionOutcome = serde_json::from_slice(canonical_outcome)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if unverified.to_bytes() != *canonical_outcome {
            return Err(StoreProtocolError::Malformed(
                "retained Store device exclusion outcome is not canonically encoded".to_string(),
            ));
        }
        let owner_reference = match &unverified {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => &exclusion.owner_registration,
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                &cancellation.owner_registration
            }
        };
        let owner =
            verify_retained_registration(root, owner_reference, canonical_owner_registration)?;
        let outcome = StoreDeviceExclusionOutcome::parse_at(
            canonical_outcome,
            &reference,
            &proposal,
            &target,
            &owner,
        )?;
        match (&self, outcome) {
            (Self::Excluded { .. }, StoreDeviceExclusionOutcome::Excluded(exclusion)) => {
                if matches!(
                    &exclusion.proof,
                    StoreDeviceExclusionProof::MergeConcurrent {
                        frozen_device_state,
                        ..
                    } if frozen_device_state != &proposal.frozen_device_state
                ) {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                let proof_policy = match &exclusion.proof {
                    StoreDeviceExclusionProof::MergeConcurrent { .. } => {
                        WritePolicy::MergeConcurrent
                    }
                    StoreDeviceExclusionProof::Serial => WritePolicy::Serial,
                };
                if proof_policy != commit.policy() {
                    return Err(StoreProtocolError::WritePolicyMismatch {
                        expected: commit.policy(),
                        actual: proof_policy,
                    });
                }
                let accepted_cut = match exclusion.proof {
                    StoreDeviceExclusionProof::MergeConcurrent { cutoff, .. } => cutoff,
                    StoreDeviceExclusionProof::Serial => commit.order.predecessor_cut()?,
                };
                Ok(VerifiedStoreDeviceExclusionOutcome::Excluded {
                    source: self,
                    accepted_cut,
                })
            }
            (Self::Cancelled { .. }, StoreDeviceExclusionOutcome::Cancelled(_)) => {
                Ok(VerifiedStoreDeviceExclusionOutcome::Cancelled(self))
            }
            _ => Err(StoreProtocolError::DeviceStateMismatch),
        }
    }
}

fn verify_retained_registration(
    root: &StoreRootRef,
    reference: &StoreDeviceRegistrationRef,
    canonical_registration: &[u8],
) -> Result<StoreDeviceRegistration, StoreProtocolError> {
    reference
        .object
        .verify(canonical_registration)
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    let registration =
        StoreDeviceRegistration::parse_at(canonical_registration, root, reference.device_id)?;
    if registration.to_bytes() != canonical_registration {
        return Err(StoreProtocolError::Malformed(
            "retained Store device registration is not canonically encoded".to_string(),
        ));
    }
    reference.verify_registration(&registration)?;
    Ok(registration)
}

fn verify_registration_activation_binding(
    activated: &ActivatedStoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    authority: &StoreDeviceRegistrationActivation,
) -> Result<(), StoreProtocolError> {
    match (&registration.origin, &activated.authority, authority) {
        (
            StoreDeviceRegistrationOrigin::Join {
                attempt_id: origin_attempt,
                outcome_slot,
                ..
            },
            StoreDeviceRegistrationActivationRef::Join {
                attempt_id,
                outcome,
            },
            StoreDeviceRegistrationActivation::Join {
                attempt_id: retained_attempt,
                outcome: retained_outcome,
            },
        ) if origin_attempt == attempt_id
            && attempt_id == retained_attempt
            && outcome_slot == outcome.slot()
            && outcome == retained_outcome =>
        {
            Ok(())
        }
        (
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id: origin_recovery,
                recovery_slot,
                ..
            },
            StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node },
            StoreDeviceRegistrationActivation::Recovery {
                recovery_id: retained_recovery,
                node: retained_node,
            },
        ) if origin_recovery == recovery_id
            && recovery_id == retained_recovery
            && recovery_slot == node.slot()
            && node == retained_node =>
        {
            Ok(())
        }
        _ => Err(StoreProtocolError::DeviceStateMismatch),
    }
}

impl VerifiedStoreDeviceExclusionOutcome {
    fn source(&self) -> &RetainedStoreDeviceExclusionOutcome {
        match self {
            Self::Excluded { source, .. } | Self::Cancelled(source) => source,
        }
    }
}

impl StoreDeviceExclusionOutcomeRef {
    pub fn proposal(&self) -> &StoreDeviceExclusionProposalRef {
        match self {
            Self::Excluded(reference) => &reference.proposal,
            Self::Cancelled(reference) => &reference.proposal,
        }
    }

    pub fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Excluded(reference) => &reference.object,
            Self::Cancelled(reference) => &reference.object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionProposal {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub proposal_id: StoreDeviceExclusionProposalId,
    pub target: StoreDeviceRegistrationRef,
    pub frozen_device_state: StoreDeviceStateRef,
    pub outcome_slot: ObjectSlot,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionOutcome {
    Excluded(StoreDeviceExclusion),
    Cancelled(StoreDeviceExclusionCancellation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusionCancellation {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub proposal: StoreDeviceExclusionProposalRef,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceExclusion {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub proposal: StoreDeviceExclusionProposalRef,
    pub target: StoreDeviceRegistrationRef,
    pub proof: StoreDeviceExclusionProof,
    pub owner_registration: StoreDeviceRegistrationRef,
    pub owner_grant: MembershipGrantId,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceExclusionProof {
    MergeConcurrent {
        frozen_device_state: StoreDeviceStateRef,
        remaining_device_acks: Vec<StoreAckRef>,
        cutoff: StoreHistoryCut,
    },
    Serial,
}

impl StoreDeviceExclusionProposal {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        proposal_id: StoreDeviceExclusionProposalId,
        target: StoreDeviceRegistrationRef,
        target_registration: &StoreDeviceRegistration,
        frozen_device_state: StoreDeviceStateRef,
        outcome_slot: ObjectSlot,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        target.verify_registration(target_registration)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey
            || owner.store_root.store_root_hash != store_root_hash
            || target_registration.store_root.store_root_hash != store_root_hash
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let expected_outcome = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(target.device_id, proposal_id)
        );
        if outcome_slot.logical_key() != expected_outcome {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: expected_outcome,
                actual: outcome_slot.logical_key().to_string(),
            });
        }
        validate_store_device_state_ref(&frozen_device_state)?;
        let mut proposal = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            proposal_id,
            target,
            frozen_device_state,
            outcome_slot,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(owner_device_signer, &proposal.canonical_signed_bytes());
        proposal.signature = signature;
        Ok(proposal)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_EXCLUSION_PROPOSAL_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                self.proposal_id,
                &self.target,
                &self.frozen_device_state,
                &self.outcome_slot,
                &self.owner_registration,
                &self.owner_grant,
            ),
        )
    }

    pub fn proposal_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Store device exclusion proposal serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &StoreDeviceExclusionProposalRef,
        target: &StoreDeviceRegistration,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let proposal: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(proposal.version)?;
        expected.verify_proposal(&proposal)?;
        proposal.target.verify_registration(target)?;
        proposal.owner_registration.verify_registration(owner)?;
        validate_store_device_state_ref(&proposal.frozen_device_state)?;
        let expected_outcome = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(
                proposal.target.device_id,
                proposal.proposal_id,
            )
        );
        if proposal.outcome_slot.logical_key() != expected_outcome {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: expected_outcome,
                actual: proposal.outcome_slot.logical_key().to_string(),
            });
        }
        if proposal.store_root_hash != owner.store_root.store_root_hash
            || proposal.store_root_hash != target.store_root.store_root_hash
            || !keys::verify_signature_hex(
                &owner.device_signing_pubkey,
                &proposal.signature,
                &proposal.canonical_signed_bytes(),
            )
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(proposal)
    }
}

impl StoreDeviceExclusionProposalRef {
    pub fn from_proposal(
        proposal: &StoreDeviceExclusionProposal,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        let reference = Self {
            proposal_id: proposal.proposal_id,
            target: proposal.target.clone(),
            proposal_hash: proposal.proposal_hash(),
            object,
        };
        reference.validate_path()?;
        Ok(reference)
    }

    pub(crate) fn validate_path(&self) -> Result<(), StoreProtocolError> {
        let expected = format!(
            "{}.json",
            device_exclusion_proposal_semantic_prefix(
                self.target.device_id,
                self.proposal_id,
                self.proposal_hash,
            )
        );
        if self.object.slot().logical_key() != expected {
            return Err(StoreProtocolError::RelocatedSlot {
                expected,
                actual: self.object.slot().logical_key().to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn verify_proposal(
        &self,
        proposal: &StoreDeviceExclusionProposal,
    ) -> Result<(), StoreProtocolError> {
        self.validate_path()?;
        if self.proposal_id != proposal.proposal_id
            || self.target != proposal.target
            || self.proposal_hash != proposal.proposal_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }
}

impl StoreDeviceExclusionCancellation {
    pub fn signed(
        proposal: StoreDeviceExclusionProposalRef,
        proposal_value: &StoreDeviceExclusionProposal,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey
            || proposal.proposal_hash != proposal_value.proposal_hash()
            || proposal.target != proposal_value.target
            || proposal_value.store_root_hash != owner.store_root.store_root_hash
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let mut cancellation = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: owner.store_root.store_root_hash,
            proposal,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(owner_device_signer, &cancellation.canonical_signed_bytes());
        cancellation.signature = signature;
        Ok(cancellation)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_EXCLUSION_CANCELLATION_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                &self.proposal,
                &self.owner_registration,
                &self.owner_grant,
            ),
        )
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }
}

impl StoreDeviceExclusion {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        proposal: StoreDeviceExclusionProposalRef,
        proposal_value: &StoreDeviceExclusionProposal,
        target: StoreDeviceRegistrationRef,
        target_registration: &StoreDeviceRegistration,
        proof: StoreDeviceExclusionProof,
        owner_registration: StoreDeviceRegistrationRef,
        owner_grant: MembershipGrantId,
        owner: &StoreDeviceRegistration,
        owner_device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        owner_registration.verify_registration(owner)?;
        target.verify_registration(target_registration)?;
        if keys::public_key_hex(owner_device_signer) != owner.device_signing_pubkey
            || proposal.target != target
            || proposal.proposal_hash != proposal_value.proposal_hash()
            || proposal.target != proposal_value.target
            || target_registration.store_root.store_root_hash != owner.store_root.store_root_hash
        {
            return Err(StoreProtocolError::InvalidSignature);
        }
        validate_device_exclusion_proof(&proof)?;
        let mut exclusion = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: owner.store_root.store_root_hash,
            proposal,
            target,
            proof,
            owner_registration,
            owner_grant,
            signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(owner_device_signer, &exclusion.canonical_signed_bytes());
        exclusion.signature = signature;
        Ok(exclusion)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            DEVICE_EXCLUSION_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                &self.proposal,
                &self.target,
                &self.proof,
                &self.owner_registration,
                &self.owner_grant,
            ),
        )
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }
}

impl StoreDeviceExclusionOutcome {
    pub fn outcome_hash(&self) -> ObjectHash {
        match self {
            Self::Excluded(exclusion) => exclusion.outcome_hash(),
            Self::Cancelled(cancellation) => cancellation.outcome_hash(),
        }
    }

    pub fn proposal(&self) -> &StoreDeviceExclusionProposalRef {
        match self {
            Self::Excluded(exclusion) => &exclusion.proposal,
            Self::Cancelled(cancellation) => &cancellation.proposal,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Store device exclusion outcome serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &StoreDeviceExclusionOutcomeRef,
        proposal: &StoreDeviceExclusionProposal,
        target: &StoreDeviceRegistration,
        owner: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let outcome: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if outcome.proposal().proposal_id != proposal.proposal_id
            || outcome.proposal().proposal_hash != proposal.proposal_hash()
            || outcome.proposal().target != proposal.target
            || expected.proposal() != outcome.proposal()
            || expected.object().slot() != &proposal.outcome_slot
            || expected.outcome_hash() != outcome.outcome_hash()
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        match &outcome {
            Self::Excluded(exclusion) => {
                require_version(exclusion.version)?;
                exclusion.target.verify_registration(target)?;
                exclusion.owner_registration.verify_registration(owner)?;
                validate_device_exclusion_proof(&exclusion.proof)?;
                if exclusion.store_root_hash != proposal.store_root_hash
                    || exclusion.store_root_hash != target.store_root.store_root_hash
                    || exclusion.target != proposal.target
                    || !keys::verify_signature_hex(
                        &owner.device_signing_pubkey,
                        &exclusion.signature,
                        &exclusion.canonical_signed_bytes(),
                    )
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
            }
            Self::Cancelled(cancellation) => {
                require_version(cancellation.version)?;
                cancellation.owner_registration.verify_registration(owner)?;
                if cancellation.store_root_hash != proposal.store_root_hash
                    || !keys::verify_signature_hex(
                        &owner.device_signing_pubkey,
                        &cancellation.signature,
                        &cancellation.canonical_signed_bytes(),
                    )
                {
                    return Err(StoreProtocolError::InvalidSignature);
                }
            }
        }
        Ok(outcome)
    }
}

impl StoreDeviceExclusionOutcomeRef {
    pub fn from_outcome(
        outcome: &StoreDeviceExclusionOutcome,
        proposal: &StoreDeviceExclusionProposal,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        if object.slot() != &proposal.outcome_slot
            || outcome.proposal().proposal_id != proposal.proposal_id
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(match outcome {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => {
                Self::Excluded(StoreDeviceExclusionRef {
                    proposal: exclusion.proposal.clone(),
                    outcome_hash: exclusion.outcome_hash(),
                    object,
                })
            }
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                Self::Cancelled(StoreDeviceExclusionCancellationRef {
                    proposal: cancellation.proposal.clone(),
                    outcome_hash: cancellation.outcome_hash(),
                    object,
                })
            }
        })
    }

    pub fn outcome_hash(&self) -> ObjectHash {
        match self {
            Self::Excluded(reference) => reference.outcome_hash,
            Self::Cancelled(reference) => reference.outcome_hash,
        }
    }
}

fn validate_device_exclusion_proof(
    proof: &StoreDeviceExclusionProof,
) -> Result<(), StoreProtocolError> {
    match proof {
        StoreDeviceExclusionProof::MergeConcurrent {
            frozen_device_state,
            remaining_device_acks,
            cutoff,
        } => {
            if frozen_device_state.write_policy() != WritePolicy::MergeConcurrent
                || cutoff.policy() != WritePolicy::MergeConcurrent
                || remaining_device_acks
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            validate_store_device_state_ref(frozen_device_state)?;
            validate_store_history_cut(cutoff)
        }
        StoreDeviceExclusionProof::Serial => Ok(()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceSelfRetirementRef {
    pub candidate_family: CandidateFamilyId,
    pub target: StoreDeviceRegistrationRef,
    pub retiring_cut: StoreHistoryCut,
    pub retirement_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceSelfRetirement {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub candidate_family: CandidateFamilyId,
    pub target: StoreDeviceRegistrationRef,
    pub retiring_cut: StoreHistoryCut,
    pub signature: String,
}

#[derive(Serialize)]
struct StoreDeviceSelfRetirementSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    candidate_family: CandidateFamilyId,
    target: &'a StoreDeviceRegistrationRef,
    retiring_cut: &'a StoreHistoryCut,
}

impl StoreDeviceSelfRetirement {
    pub fn signed(
        store_root_hash: ObjectHash,
        candidate_family: CandidateFamilyId,
        target: StoreDeviceRegistrationRef,
        retiring_cut: StoreHistoryCut,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_store_history_cut(&retiring_cut)?;
        let mut retirement = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            candidate_family,
            target,
            retiring_cut,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &retirement.canonical_signed_bytes());
        retirement.signature = signature;
        Ok(retirement)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            SELF_RETIREMENT_DOMAIN,
            &StoreDeviceSelfRetirementSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                candidate_family: self.candidate_family,
                target: &self.target,
                retiring_cut: &self.retiring_cut,
            },
        )
    }

    pub fn retirement_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreDeviceSelfRetirement serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected: &StoreDeviceSelfRetirementRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let retirement: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(retirement.version)?;
        validate_store_history_cut(&retirement.retiring_cut)?;
        if retirement.store_root_hash != registration.store_root.store_root_hash
            || retirement.candidate_family != expected.candidate_family
            || retirement.target != expected.target
            || retirement.retiring_cut != expected.retiring_cut
            || retirement.retirement_hash() != expected.retirement_hash
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let prefix = device_self_retirement_semantic_prefix(
            expected.candidate_family,
            &expected.target.device_id,
            expected.retirement_hash,
        );
        if expected.object.slot().logical_key() != format!("{prefix}.json") {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: prefix,
                actual: expected.object.slot().logical_key().to_string(),
            });
        }
        expected.target.verify_registration(registration)?;
        if !keys::verify_signature_hex(
            &registration.device_signing_pubkey,
            &retirement.signature,
            &retirement.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(retirement)
    }
}

impl StoreDeviceSelfRetirementRef {
    pub fn from_retirement(retirement: &StoreDeviceSelfRetirement, object: ExactObjectRef) -> Self {
        Self {
            candidate_family: retirement.candidate_family,
            target: retirement.target.clone(),
            retiring_cut: retirement.retiring_cut.clone(),
            retirement_hash: retirement.retirement_hash(),
            object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceTerminalRef {
    Excluded(StoreDeviceExclusionRef),
    SelfRetirement(StoreDeviceSelfRetirementRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceProposalState {
    Pending {
        proposal: StoreDeviceExclusionProposalRef,
    },
    Cancelled {
        outcome: StoreDeviceExclusionCancellationRef,
    },
    Superseded {
        proposal: StoreDeviceExclusionProposalRef,
        terminals: Vec<StoreDeviceTerminalRef>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedStoreDeviceState {
    pub devices: BTreeMap<StoreDeviceId, StoreDeviceRecord>,
    pub recovery: Vec<OwnerRecoveryCursor>,
    pub state_hash: ObjectHash,
}

impl ResolvedStoreDeviceState {
    pub(crate) fn validate_canonical(&self) -> Result<(), StoreProtocolError> {
        let canonical = Self::from_parts(self.devices.clone(), self.recovery.clone())?;
        if canonical != *self {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }

    pub fn founder(
        root: &StoreRootRef,
        founder_registration: StoreDeviceRegistrationRef,
        founder_pubkey: &str,
        founder_grant: MembershipGrantId,
        founder_recovery: &GrantStreamAnchor,
    ) -> Result<Self, StoreProtocolError> {
        let cursor = OwnerRecoveryCursor {
            owner_grant: founder_grant.clone(),
            position: OwnerRecoveryPosition::BeforeFirst {
                activation: OwnerRecoveryActivationId::derive(
                    root,
                    founder_pubkey,
                    &founder_grant,
                    founder_recovery,
                )?,
            },
        };
        let devices = BTreeMap::from([(
            founder_registration.device_id,
            StoreDeviceRecord {
                registration: founder_registration,
                proposals: BTreeMap::new(),
                status: StoreDeviceStatus::Active,
            },
        )]);
        Self::from_parts(devices, vec![cursor])
    }

    pub fn activate_registration(
        &self,
        registration: StoreDeviceRegistrationRef,
        recovery: Option<OwnerRecoveryCursor>,
    ) -> Result<Self, StoreProtocolError> {
        if self.devices.contains_key(&registration.device_id) {
            return Err(StoreProtocolError::DuplicateDeviceRegistration {
                device_id: registration.device_id.to_string(),
                revision: 1,
            });
        }
        let mut devices = self.devices.clone();
        devices.insert(
            registration.device_id,
            StoreDeviceRecord {
                registration,
                proposals: BTreeMap::new(),
                status: StoreDeviceStatus::Active,
            },
        );
        let mut cursors = self.recovery.clone();
        if let Some(cursor) = recovery {
            if let Some(existing) = cursors
                .iter_mut()
                .find(|existing| existing.owner_grant == cursor.owner_grant)
            {
                *existing = cursor;
            } else {
                cursors.push(cursor);
            }
        }
        Self::from_parts(devices, cursors)
    }

    pub fn activate_owner_recovery(
        &self,
        owner_grant: MembershipGrantId,
        activation: OwnerRecoveryActivationId,
    ) -> Result<Self, StoreProtocolError> {
        if self
            .recovery
            .iter()
            .any(|cursor| cursor.owner_grant == owner_grant)
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        let mut recovery = self.recovery.clone();
        recovery.push(OwnerRecoveryCursor {
            owner_grant,
            position: OwnerRecoveryPosition::BeforeFirst { activation },
        });
        Self::from_parts(self.devices.clone(), recovery)
    }

    pub fn propose_exclusion(
        &self,
        reference: StoreDeviceExclusionProposalRef,
        proposal: &StoreDeviceExclusionProposal,
        predecessor_ref: &StoreDeviceStateRef,
    ) -> Result<Self, StoreProtocolError> {
        reference.verify_proposal(proposal)?;
        if &proposal.frozen_device_state != predecessor_ref
            || predecessor_ref.state_hash() != self.state_hash
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&reference.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if record.registration != reference.target
            || !matches!(record.status, StoreDeviceStatus::Active)
            || record.proposals.contains_key(&reference.proposal_id)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        record.proposals.insert(
            reference.proposal_id,
            StoreDeviceProposalState::Pending {
                proposal: reference,
            },
        );
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn cancel_exclusion(
        &self,
        cancellation: StoreDeviceExclusionCancellationRef,
    ) -> Result<Self, StoreProtocolError> {
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&cancellation.proposal.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        let state = record
            .proposals
            .get_mut(&cancellation.proposal.proposal_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if !matches!(state, StoreDeviceProposalState::Pending { proposal } if proposal == &cancellation.proposal)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        *state = StoreDeviceProposalState::Cancelled {
            outcome: cancellation,
        };
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn exclude(
        &self,
        exclusion: StoreDeviceExclusionRef,
        accepted_cut: StoreHistoryCut,
    ) -> Result<Self, StoreProtocolError> {
        validate_store_history_cut(&accepted_cut)?;
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&exclusion.proposal.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if record.registration != exclusion.proposal.target
            || !matches!(record.status, StoreDeviceStatus::Active)
            || !matches!(
                record.proposals.get(&exclusion.proposal.proposal_id),
                Some(StoreDeviceProposalState::Pending { proposal }) if proposal == &exclusion.proposal
            )
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let terminals = vec![StoreDeviceTerminalRef::Excluded(exclusion)];
        supersede_pending_proposals(&mut record.proposals, &terminals);
        record.status = StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        };
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn self_retire(
        &self,
        retirement: StoreDeviceSelfRetirementRef,
    ) -> Result<Self, StoreProtocolError> {
        let mut devices = self.devices.clone();
        let record = devices
            .get_mut(&retirement.target.device_id)
            .ok_or(StoreProtocolError::DeviceStateMismatch)?;
        if record.registration != retirement.target
            || !matches!(record.status, StoreDeviceStatus::Active)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        record.status = StoreDeviceStatus::Inactive {
            terminals: vec![StoreDeviceTerminalRef::SelfRetirement(retirement.clone())],
            accepted_cut: retirement.retiring_cut,
        };
        let StoreDeviceStatus::Inactive { terminals, .. } = &record.status else {
            unreachable!("self-retirement writes an inactive device state")
        };
        supersede_pending_proposals(&mut record.proposals, terminals);
        Self::from_parts(devices, self.recovery.clone())
    }

    pub fn merge(states: impl IntoIterator<Item = Self>) -> Result<Self, StoreProtocolError> {
        let mut devices = BTreeMap::new();
        let mut recovery = BTreeMap::<MembershipGrantId, OwnerRecoveryPosition>::new();
        for state in states {
            for (device_id, record) in state.devices {
                match devices.entry(device_id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(record);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        if entry.get().registration != record.registration {
                            return Err(StoreProtocolError::DeviceStateMismatch);
                        }
                        let merged_status =
                            merge_device_status(entry.get().status.clone(), record.status)?;
                        let mut merged_proposals = merge_device_proposals(
                            entry.get().proposals.clone(),
                            record.proposals,
                        )?;
                        if let StoreDeviceStatus::Inactive { terminals, .. } = &merged_status {
                            supersede_pending_proposals(&mut merged_proposals, terminals);
                        }
                        entry.get_mut().status = merged_status;
                        entry.get_mut().proposals = merged_proposals;
                    }
                }
            }
            for cursor in state.recovery {
                match recovery.entry(cursor.owner_grant) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(cursor.position);
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        if entry.get() != &cursor.position {
                            return Err(StoreProtocolError::OwnerRecoveryMismatch);
                        }
                    }
                }
            }
        }
        Self::from_parts(
            devices,
            recovery
                .into_iter()
                .map(|(owner_grant, position)| OwnerRecoveryCursor {
                    owner_grant,
                    position,
                })
                .collect(),
        )
    }

    fn from_parts(
        devices: BTreeMap<StoreDeviceId, StoreDeviceRecord>,
        mut recovery: Vec<OwnerRecoveryCursor>,
    ) -> Result<Self, StoreProtocolError> {
        recovery.sort();
        validate_recovery_cursors(&recovery)?;
        validate_store_device_records(&devices)?;
        let state_hash = ObjectHash::digest(&domain_json(
            b"coven.store-device-state.v1\0",
            &(&devices, &recovery),
        ));
        Ok(Self {
            devices,
            recovery,
            state_hash,
        })
    }
}

fn supersede_pending_proposals(
    proposals: &mut BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    terminals: &[StoreDeviceTerminalRef],
) {
    for state in proposals.values_mut() {
        if let StoreDeviceProposalState::Pending { proposal } = state {
            *state = StoreDeviceProposalState::Superseded {
                proposal: proposal.clone(),
                terminals: terminals.to_vec(),
            };
        }
    }
}

fn merge_device_proposals(
    mut left: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
    right: BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>,
) -> Result<BTreeMap<StoreDeviceExclusionProposalId, StoreDeviceProposalState>, StoreProtocolError>
{
    for (proposal_id, right_state) in right {
        match left.entry(proposal_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(right_state);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let merged = merge_device_proposal_state(entry.get().clone(), right_state)?;
                entry.insert(merged);
            }
        }
    }
    Ok(left)
}

fn merge_device_proposal_state(
    left: StoreDeviceProposalState,
    right: StoreDeviceProposalState,
) -> Result<StoreDeviceProposalState, StoreProtocolError> {
    let left_proposal = match &left {
        StoreDeviceProposalState::Pending { proposal }
        | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
        StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
    };
    let right_proposal = match &right {
        StoreDeviceProposalState::Pending { proposal }
        | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
        StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
    };
    if left_proposal != right_proposal {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    match (left, right) {
        (
            StoreDeviceProposalState::Pending { proposal },
            StoreDeviceProposalState::Pending { .. },
        ) => Ok(StoreDeviceProposalState::Pending { proposal }),
        (
            StoreDeviceProposalState::Cancelled { outcome },
            StoreDeviceProposalState::Cancelled { outcome: other },
        ) => {
            if outcome != other {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            Ok(StoreDeviceProposalState::Cancelled { outcome })
        }
        (StoreDeviceProposalState::Cancelled { outcome }, _)
        | (_, StoreDeviceProposalState::Cancelled { outcome }) => {
            Ok(StoreDeviceProposalState::Cancelled { outcome })
        }
        (
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals: left,
            },
            StoreDeviceProposalState::Superseded {
                terminals: right, ..
            },
        ) => Ok(StoreDeviceProposalState::Superseded {
            proposal,
            terminals: merge_terminal_refs(left, right)?,
        }),
        (
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals,
            },
            _,
        )
        | (
            _,
            StoreDeviceProposalState::Superseded {
                proposal,
                terminals,
            },
        ) => Ok(StoreDeviceProposalState::Superseded {
            proposal,
            terminals,
        }),
    }
}

fn merge_device_status(
    left: StoreDeviceStatus,
    right: StoreDeviceStatus,
) -> Result<StoreDeviceStatus, StoreProtocolError> {
    match (left, right) {
        (StoreDeviceStatus::Active, StoreDeviceStatus::Active) => Ok(StoreDeviceStatus::Active),
        (
            StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            },
            StoreDeviceStatus::Active,
        )
        | (
            StoreDeviceStatus::Active,
            StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            },
        ) => Ok(StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        }),
        (
            StoreDeviceStatus::Inactive {
                terminals: left_terminals,
                accepted_cut: left_cut,
            },
            StoreDeviceStatus::Inactive {
                terminals: right_terminals,
                accepted_cut: right_cut,
            },
        ) => Ok(StoreDeviceStatus::Inactive {
            terminals: merge_terminal_refs(left_terminals, right_terminals)?,
            accepted_cut: intersect_terminal_history_cuts(left_cut, right_cut)?,
        }),
    }
}

fn merge_terminal_refs(
    left: Vec<StoreDeviceTerminalRef>,
    right: Vec<StoreDeviceTerminalRef>,
) -> Result<Vec<StoreDeviceTerminalRef>, StoreProtocolError> {
    let terminals = left
        .into_iter()
        .chain(right)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    validate_terminal_refs(&terminals)?;
    Ok(terminals)
}

fn merge_history_cuts(
    left: StoreHistoryCut,
    right: StoreHistoryCut,
) -> Result<StoreHistoryCut, StoreProtocolError> {
    match (left, right) {
        (StoreHistoryCut::MergeConcurrent(mut left), StoreHistoryCut::MergeConcurrent(right)) => {
            for (stream, reference) in right {
                match left.entry(stream) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(reference);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let current = entry.get();
                        if reference.coord.sequence() > current.coord.sequence() {
                            entry.insert(reference);
                        } else if reference.coord.sequence() == current.coord.sequence()
                            && reference != *current
                        {
                            return Err(StoreProtocolError::DeviceStateMismatch);
                        }
                    }
                }
            }
            Ok(StoreHistoryCut::MergeConcurrent(left))
        }
        (StoreHistoryCut::Serial(left), StoreHistoryCut::Serial(right)) if left == right => {
            Ok(StoreHistoryCut::Serial(left))
        }
        _ => Err(StoreProtocolError::DeviceStateMismatch),
    }
}

fn intersect_terminal_history_cuts(
    left: StoreHistoryCut,
    right: StoreHistoryCut,
) -> Result<StoreHistoryCut, StoreProtocolError> {
    match (left, right) {
        (StoreHistoryCut::MergeConcurrent(left), StoreHistoryCut::MergeConcurrent(right)) => {
            let mut intersection = BTreeMap::new();
            for (stream, left_reference) in left {
                let Some(right_reference) = right.get(&stream) else {
                    continue;
                };
                let left_sequence = left_reference.coord.sequence();
                let right_sequence = right_reference.coord.sequence();
                let reference = if left_sequence < right_sequence {
                    left_reference
                } else if right_sequence < left_sequence {
                    right_reference.clone()
                } else if left_reference == *right_reference {
                    left_reference
                } else {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                };
                intersection.insert(stream, reference);
            }
            Ok(StoreHistoryCut::MergeConcurrent(intersection))
        }
        (StoreHistoryCut::Serial(left), StoreHistoryCut::Serial(right)) if left == right => {
            Ok(StoreHistoryCut::Serial(left))
        }
        _ => Err(StoreProtocolError::DeviceStateMismatch),
    }
}

fn validate_store_device_records(
    devices: &BTreeMap<StoreDeviceId, StoreDeviceRecord>,
) -> Result<(), StoreProtocolError> {
    for (device_id, record) in devices {
        if record.registration.device_id != *device_id {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        for (proposal_id, state) in &record.proposals {
            let proposal = match state {
                StoreDeviceProposalState::Pending { proposal }
                | StoreDeviceProposalState::Superseded { proposal, .. } => proposal,
                StoreDeviceProposalState::Cancelled { outcome } => &outcome.proposal,
            };
            if proposal.proposal_id != *proposal_id {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            if proposal.target != record.registration {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            if let StoreDeviceProposalState::Superseded { terminals, .. } = state {
                validate_terminal_refs(terminals)?;
            }
        }
        if let StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        } = &record.status
        {
            validate_terminal_refs(terminals)?;
            validate_store_history_cut(accepted_cut)?;
            if record
                .proposals
                .values()
                .any(|state| matches!(state, StoreDeviceProposalState::Pending { .. }))
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
    }
    Ok(())
}

fn validate_terminal_refs(terminals: &[StoreDeviceTerminalRef]) -> Result<(), StoreProtocolError> {
    if terminals.is_empty() || terminals.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    Ok(())
}

pub(crate) fn canonical_recovery_cursors(
    mut recovery: Vec<OwnerRecoveryCursor>,
) -> Result<Vec<OwnerRecoveryCursor>, StoreProtocolError> {
    recovery.sort();
    validate_recovery_cursors(&recovery)?;
    Ok(recovery)
}

pub(crate) fn validate_recovery_cursors(
    recovery: &[OwnerRecoveryCursor],
) -> Result<(), StoreProtocolError> {
    if recovery.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::OwnerRecoveryMismatch);
    }
    Ok(())
}

impl OwnerRecoveryNodeRef {
    pub fn slot(&self) -> &ObjectSlot {
        self.object.slot()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceRecoveryReadiness {
    pub registration: StoreDeviceRegistrationRef,
    pub initial_ack: StoreAckRef,
    pub bootstrap_cut: StoreHistoryCut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecoveryNode {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub recovery_id: DeviceRecoveryId,
    pub owner_pubkey: String,
    pub owner_grant: MembershipGrantId,
    pub sequence: u64,
    pub membership: StoreMembershipStateRef,
    pub predecessor: Option<OwnerRecoveryNodeRef>,
    pub readiness: DeviceRecoveryReadiness,
    pub next_slot: ObjectSlot,
    pub signature: String,
}

impl OwnerRecoveryNode {
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        store_root_hash: ObjectHash,
        recovery_id: DeviceRecoveryId,
        owner_grant: MembershipGrantId,
        sequence: u64,
        membership: StoreMembershipStateRef,
        predecessor: Option<OwnerRecoveryNodeRef>,
        readiness: DeviceRecoveryReadiness,
        next_slot: ObjectSlot,
        owner_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let owner_pubkey = keys::public_key_hex(owner_signer);
        let mut node = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            recovery_id,
            owner_pubkey,
            owner_grant,
            sequence,
            membership,
            predecessor,
            readiness,
            next_slot,
            signature: String::new(),
        };
        node.validate_shape()?;
        let (_, signature) = keys::sign_hex(owner_signer, &node.canonical_signed_bytes());
        node.signature = signature;
        Ok(node)
    }

    pub fn parse_at(
        bytes: &[u8],
        store_root: &StoreRootRef,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<Self, StoreProtocolError> {
        let node: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(node.version)?;
        node.validate_shape()?;
        if node.store_root_hash != store_root.store_root_hash
            || node.owner_pubkey != reference.owner_pubkey
            || node.owner_grant != reference.owner_grant
            || node.sequence != reference.sequence
            || node.node_hash() != reference.node_hash
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        if !keys::verify_signature_hex(
            &node.owner_pubkey,
            &node.signature,
            &node.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(node)
    }

    fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        let predecessor_matches = match &self.predecessor {
            None => self.sequence == 1,
            Some(predecessor) => {
                predecessor.owner_pubkey == self.owner_pubkey
                    && predecessor.owner_grant == self.owner_grant
                    && predecessor.sequence.checked_add(1) == Some(self.sequence)
            }
        };
        if !predecessor_matches || self.readiness.initial_ack.sequence != 1 {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(())
    }

    pub(crate) fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            OWNER_RECOVERY_NODE_DOMAIN,
            &(
                self.version,
                self.store_root_hash,
                self.recovery_id,
                &self.owner_pubkey,
                &self.owner_grant,
                self.sequence,
                &self.membership,
                &self.predecessor,
                &self.readiness,
                &self.next_slot,
            ),
        )
    }

    pub fn node_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("OwnerRecoveryNode serialization cannot fail")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationOrigin {
    Founder {
        creation_id: StoreCreationId,
    },
    Join {
        attempt_id: DeviceJoinAttemptId,
        attempt_slot: ObjectSlot,
        outcome_slot: ObjectSlot,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        recovery_slot: ObjectSlot,
        owner_grant: MembershipGrantId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationActivation {
    Founder {
        root: StoreRootRef,
    },
    Join {
        attempt_id: DeviceJoinAttemptId,
        outcome: DeviceJoinOutcomeRef,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        node: OwnerRecoveryNodeRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatedStoreDeviceRegistrationRef {
    pub registration: StoreDeviceRegistrationRef,
    pub authority: StoreDeviceRegistrationActivationRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationActivationRef {
    Join {
        attempt_id: DeviceJoinAttemptId,
        outcome: DeviceJoinOutcomeRef,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        node: OwnerRecoveryNodeRef,
    },
}

impl StoreDeviceRegistrationOrigin {
    fn external_id(&self) -> ObjectHash {
        match self {
            Self::Founder { creation_id } => creation_id.0,
            Self::Join { attempt_id, .. } => attempt_id.0,
            Self::Recovery { recovery_id, .. } => recovery_id.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceStreamAnchor {
    StoreAnnouncements { first_slot: ObjectSlot },
    StoreAcknowledgements { first_slot: ObjectSlot },
    StoreSnapshots { first_slot: ObjectSlot },
}

impl DeviceStreamAnchor {
    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::StoreAnnouncements { first_slot }
            | Self::StoreAcknowledgements { first_slot }
            | Self::StoreSnapshots { first_slot } => first_slot,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum GrantStreamAnchor {
    StoreMembership {
        first_slot: ObjectSlot,
    },
    OwnerRecovery {
        first_slot: ObjectSlot,
    },
    CircleControl {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
    CircleRoster {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
    CircleMetadata {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
}

impl GrantStreamAnchor {
    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::StoreMembership { first_slot }
            | Self::OwnerRecovery { first_slot }
            | Self::CircleControl { first_slot, .. }
            | Self::CircleRoster { first_slot, .. }
            | Self::CircleMetadata { first_slot, .. } => first_slot,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitAnchor {
    MergeConcurrent { announcements: DeviceStreamAnchor },
    Serial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRegistration {
    pub version: u32,
    pub store_root: StoreRootRef,
    pub device_id: StoreDeviceId,
    pub author_pubkey: String,
    pub device_signing_pubkey: String,
    pub origin: StoreDeviceRegistrationOrigin,
    pub provider: ProviderDeviceBinding,
    pub store_commits: StoreCommitAnchor,
    pub acknowledgements: DeviceStreamAnchor,
    pub snapshots: DeviceStreamAnchor,
    pub identity_signature: String,
}

#[derive(Serialize)]
struct RegistrationSignedFields<'a> {
    version: u32,
    store_root: &'a StoreRootRef,
    device_id: StoreDeviceId,
    author_pubkey: &'a str,
    device_signing_pubkey: &'a str,
    origin: &'a StoreDeviceRegistrationOrigin,
    provider: &'a ProviderDeviceBinding,
    store_commits: &'a StoreCommitAnchor,
    acknowledgements: &'a DeviceStreamAnchor,
    snapshots: &'a DeviceStreamAnchor,
}

impl StoreDeviceRegistration {
    fn device_stream_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
        anchor: &DeviceStreamAnchor,
    ) -> Result<StreamActivation, StoreProtocolError> {
        reference.verify_registration(self)?;
        Ok(StreamActivation::device_authorized(
            self.store_root.store_root_hash,
            reference.clone(),
            anchor.clone(),
        ))
    }

    pub fn store_announcement_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        let StoreCommitAnchor::MergeConcurrent { announcements } = &self.store_commits else {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            });
        };
        self.device_stream_activation(reference, announcements)
    }

    pub fn store_acknowledgement_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        self.device_stream_activation(reference, &self.acknowledgements)
    }

    pub fn store_snapshot_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        self.device_stream_activation(reference, &self.snapshots)
    }

    pub fn signed(
        store_root: StoreRootRef,
        origin: StoreDeviceRegistrationOrigin,
        provider: ProviderDeviceBinding,
        store_commits: StoreCommitAnchor,
        acknowledgements: DeviceStreamAnchor,
        snapshots: DeviceStreamAnchor,
        identity_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_registration_anchors(&store_commits, &acknowledgements, &snapshots)?;
        let author_pubkey = keys::public_key_hex(identity_signer);
        let device_signer = derive_device_signer(identity_signer, &store_root, &origin);
        let device_signing_pubkey = keys::public_key_hex(&device_signer);
        let device_id = StoreDeviceId::derive(&store_root, &origin);
        let mut registration = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root,
            device_id,
            author_pubkey,
            device_signing_pubkey,
            origin,
            provider,
            store_commits,
            acknowledgements,
            snapshots,
            identity_signature: String::new(),
        };
        let (_, signature) =
            keys::sign_hex(identity_signer, &registration.canonical_signed_bytes());
        registration.identity_signature = signature;
        Ok(registration)
    }

    pub(crate) fn device_signer(
        &self,
        identity_signer: &UserKeypair,
    ) -> Result<UserKeypair, StoreProtocolError> {
        if keys::public_key_hex(identity_signer) != self.author_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let signer = derive_device_signer(identity_signer, &self.store_root, &self.origin);
        if keys::public_key_hex(&signer) != self.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(signer)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            REGISTRATION_DOMAIN,
            &RegistrationSignedFields {
                version: self.version,
                store_root: &self.store_root,
                device_id: self.device_id,
                author_pubkey: &self.author_pubkey,
                device_signing_pubkey: &self.device_signing_pubkey,
                origin: &self.origin,
                provider: &self.provider,
                store_commits: &self.store_commits,
                acknowledgements: &self.acknowledgements,
                snapshots: &self.snapshots,
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
        expected_store_root: &StoreRootRef,
        expected_device: StoreDeviceId,
    ) -> Result<Self, StoreProtocolError> {
        let registration: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(registration.version)?;
        if &registration.store_root != expected_store_root {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root.store_root_hash,
                actual: registration.store_root.store_root_hash,
            });
        }
        if registration.device_id != expected_device {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: registration_slot_prefix(&expected_device.to_string()),
                actual: registration_slot_prefix(&registration.device_id.to_string()),
            });
        }
        if registration.device_id
            != StoreDeviceId::derive(&registration.store_root, &registration.origin)
        {
            return Err(StoreProtocolError::Malformed(
                "Store device id differs from its root and origin".to_string(),
            ));
        }
        validate_registration_anchors(
            &registration.store_commits,
            &registration.acknowledgements,
            &registration.snapshots,
        )?;
        if !keys::verify_signature_hex(
            &registration.author_pubkey,
            &registration.identity_signature,
            &registration.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(registration)
    }
}

fn derive_device_signer(
    identity_signer: &UserKeypair,
    store_root: &StoreRootRef,
    origin: &StoreDeviceRegistrationOrigin,
) -> UserKeypair {
    const DOMAIN: &[u8] = b"coven.store-device-signing-key.v1\0";
    let context = serde_json::to_vec(&(store_root, origin))
        .expect("Store device signing context serialization cannot fail");
    identity_signer.derive_signing_key(DOMAIN, &context)
}

fn validate_registration_anchors(
    commits: &StoreCommitAnchor,
    acknowledgements: &DeviceStreamAnchor,
    snapshots: &DeviceStreamAnchor,
) -> Result<(), StoreProtocolError> {
    if !matches!(
        acknowledgements,
        DeviceStreamAnchor::StoreAcknowledgements { .. }
    ) || !matches!(snapshots, DeviceStreamAnchor::StoreSnapshots { .. })
        || !matches!(
            commits,
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { .. }
            }
        ) && !matches!(commits, StoreCommitAnchor::Serial)
    {
        return Err(StoreProtocolError::Malformed(
            "Store device registration contains mismatched permanent stream anchors".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAck {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub registration: StoreDeviceRegistrationRef,
    pub sequence: u64,
    pub store_cut: StoreHistoryCut,
    pub device_state: StoreDeviceStateRef,
    pub snapshot: Option<StoreSnapshotLocator>,
    pub exclusions: StoreAckExclusionState,
    pub last_sync: String,
    pub successor: SuccessorLink,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreAckRef {
    pub registration: StoreDeviceRegistrationRef,
    pub sequence: u64,
    pub ack_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshotLocator {
    pub author_registration: StoreDeviceRegistrationRef,
    pub snapshot: StoreSnapshotRef,
}

/// The exact membership and device state represented by one Store snapshot.
/// Both references retain their policy-specific coordinates, while their state
/// hashes identify the resolved state across state-neutral commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshotState {
    pub membership: StoreMembershipStateRef,
    pub devices: StoreDeviceStateRef,
}

impl StoreSnapshotState {
    fn validate(
        &self,
        store_root_hash: ObjectHash,
        coverage: &CommitFrontier,
    ) -> Result<(), StoreProtocolError> {
        self.membership.validate_shape()?;
        validate_store_device_state_ref(&self.devices)?;
        if self.membership.write_policy() != coverage.policy()
            || self.devices.write_policy() != coverage.policy()
        {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: coverage.policy(),
                actual: if self.membership.write_policy() != coverage.policy() {
                    self.membership.write_policy()
                } else {
                    self.devices.write_policy()
                },
            });
        }
        if self.membership.recovery() != self.devices.recovery() {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        match (coverage, &self.membership, &self.devices) {
            (
                CommitFrontier::MergeConcurrent(expected),
                StoreMembershipStateRef::MergeConcurrent(_),
                StoreDeviceStateRef::MergeConcurrent { frontier, .. },
            ) if frontier == &CommitFrontier::MergeConcurrent(expected.clone()) => Ok(()),
            (
                CommitFrontier::Serial(expected),
                StoreMembershipStateRef::Serial(membership),
                StoreDeviceStateRef::Serial { position, .. },
            ) => {
                let expected = match expected {
                    Some(commit) => StoreSerialPredecessor::Commit(commit.clone()),
                    None => match position {
                        StoreSerialPredecessor::Genesis { root, .. }
                            if root.store_root_hash == store_root_hash =>
                        {
                            position.clone()
                        }
                        _ => return Err(StoreProtocolError::DeviceStateMismatch),
                    },
                };
                if membership.position != expected || position != &expected {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                Ok(())
            }
            _ => Err(StoreProtocolError::DeviceStateMismatch),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreAckExclusionState {
    MergeConcurrent {
        proposal_freezes: Vec<StoreDeviceProposalAck>,
    },
    Serial,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceProposalAck {
    pub proposal: StoreDeviceExclusionProposalRef,
    pub target_cut: StoreHistoryCut,
}

#[derive(Serialize)]
struct AckSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    registration: &'a StoreDeviceRegistrationRef,
    sequence: u64,
    store_cut: &'a StoreHistoryCut,
    device_state: &'a StoreDeviceStateRef,
    snapshot: Option<&'a StoreSnapshotLocator>,
    exclusions: &'a StoreAckExclusionState,
    last_sync: &'a str,
    successor: &'a SuccessorLink,
}

impl StoreAck {
    pub fn signed(
        store_root_hash: ObjectHash,
        registration: StoreDeviceRegistrationRef,
        sequence: u64,
        store_cut: StoreHistoryCut,
        device_state: StoreDeviceStateRef,
        snapshot: Option<StoreSnapshotLocator>,
        exclusions: StoreAckExclusionState,
        last_sync: String,
        successor: SuccessorLink,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_successor_sequence(sequence, &successor)?;
        validate_ack_state(
            store_root_hash,
            &registration,
            &store_cut,
            &device_state,
            &exclusions,
        )?;
        let mut ack = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            registration,
            sequence,
            store_cut,
            device_state,
            snapshot,
            exclusions,
            last_sync,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &ack.canonical_signed_bytes());
        ack.signature = signature;
        Ok(ack)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            ACK_DOMAIN,
            &AckSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                registration: &self.registration,
                sequence: self.sequence,
                store_cut: &self.store_cut,
                device_state: &self.device_state,
                snapshot: self.snapshot.as_ref(),
                exclusions: &self.exclusions,
                last_sync: &self.last_sync,
                successor: &self.successor,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreAck serialization cannot fail")
    }

    pub fn ack_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn semantic_hash_from_bytes(bytes: &[u8]) -> Result<ObjectHash, StoreProtocolError> {
        let ack: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        Ok(ack.ack_hash())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root: &StoreRootRef,
        expected: &StoreAckRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let ack: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(ack.version)?;
        if ack.store_root_hash != expected_store_root.store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root.store_root_hash,
                actual: ack.store_root_hash,
            });
        }
        ack.registration.verify_registration(author)?;
        if ack.registration != expected.registration {
            return Err(StoreProtocolError::DeviceRegistrationRefMismatch {
                device_id: expected.registration.device_id.to_string(),
                revision: 1,
                expected: expected.registration.registration_hash,
                actual: ack.registration.registration_hash,
            });
        }
        if ack.sequence != expected.sequence {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: ack_slot_prefix(&author.device_id.to_string(), expected.sequence),
                actual: ack_slot_prefix(&author.device_id.to_string(), ack.sequence),
            });
        }
        validate_successor_sequence(ack.sequence, &ack.successor)?;
        validate_ack_state(
            ack.store_root_hash,
            &ack.registration,
            &ack.store_cut,
            &ack.device_state,
            &ack.exclusions,
        )?;
        let activation = author
            .store_acknowledgement_activation(&ack.registration)?
            .activation_id();
        if ack.successor.activation != activation {
            return Err(StoreProtocolError::Malformed(
                "Store acknowledgement successor uses another stream activation".to_string(),
            ));
        }
        if !keys::verify_signature_hex(
            &author.device_signing_pubkey,
            &ack.signature,
            &ack.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        if ack.ack_hash() != expected.ack_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.ack_hash,
                actual: ack.ack_hash(),
            });
        }
        Ok(ack)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotMeta {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub generation: u64,
    pub predecessor: Option<StoreSnapshotRef>,
    pub image: SnapshotImageRef,
    pub coverage: CommitFrontier,
    pub state: StoreSnapshotState,
    pub history_summary: StoreSnapshotHistorySummary,
    pub schema_version: u32,
    pub created_at: String,
    pub successor: SnapshotSuccessorLink,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreSnapshotHistorySummary {
    MergeConcurrent(RetainedVerifiedMergeHistorySummary),
    Serial,
}

impl StoreSnapshotHistorySummary {
    fn validate(
        &self,
        store_root_hash: ObjectHash,
        coverage: &CommitFrontier,
        state: &StoreSnapshotState,
    ) -> Result<(), StoreProtocolError> {
        match (self, coverage) {
            (Self::MergeConcurrent(summary), CommitFrontier::MergeConcurrent(frontier)) => {
                summary.validate_snapshot_baseline()?;
                if summary.store_root_hash != store_root_hash
                    || summary.frontier()? != *frontier
                    || summary.post_state != state.devices
                {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
                Ok(())
            }
            (Self::Serial, CommitFrontier::Serial(_)) => Ok(()),
            (Self::MergeConcurrent(_), CommitFrontier::Serial(_)) => {
                Err(StoreProtocolError::WritePolicyMismatch {
                    expected: WritePolicy::Serial,
                    actual: WritePolicy::MergeConcurrent,
                })
            }
            (Self::Serial, CommitFrontier::MergeConcurrent(_)) => {
                Err(StoreProtocolError::WritePolicyMismatch {
                    expected: WritePolicy::MergeConcurrent,
                    actual: WritePolicy::Serial,
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotImageRef {
    pub image_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshotRef {
    pub generation: u64,
    pub snapshot_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSuccessorLink {
    pub activation: StreamActivationId,
    pub predecessor: Option<StoreSnapshotRef>,
    pub next_slot: ObjectSlot,
}

#[derive(Serialize)]
struct SnapshotSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    author_registration: &'a StoreDeviceRegistrationRef,
    generation: u64,
    predecessor: Option<&'a StoreSnapshotRef>,
    image: &'a SnapshotImageRef,
    coverage: &'a CommitFrontier,
    state: &'a StoreSnapshotState,
    history_summary: &'a StoreSnapshotHistorySummary,
    schema_version: u32,
    created_at: &'a str,
    successor: &'a SnapshotSuccessorLink,
}

impl SnapshotMeta {
    pub fn signed(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        generation: u64,
        predecessor: Option<StoreSnapshotRef>,
        image: SnapshotImageRef,
        coverage: CommitFrontier,
        state: StoreSnapshotState,
        history_summary: StoreSnapshotHistorySummary,
        schema_version: u32,
        created_at: String,
        successor: SnapshotSuccessorLink,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_snapshot_generation(generation, predecessor.as_ref())?;
        validate_commit_frontier(&coverage)?;
        state.validate(store_root_hash, &coverage)?;
        history_summary.validate(store_root_hash, &coverage, &state)?;
        let mut meta = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            author_registration,
            generation,
            predecessor,
            image,
            coverage,
            state,
            history_summary,
            schema_version,
            created_at,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &meta.canonical_signed_bytes());
        meta.signature = signature;
        Ok(meta)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            SNAPSHOT_DOMAIN,
            &SnapshotSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                author_registration: &self.author_registration,
                generation: self.generation,
                predecessor: self.predecessor.as_ref(),
                image: &self.image,
                coverage: &self.coverage,
                state: &self.state,
                history_summary: &self.history_summary,
                schema_version: self.schema_version,
                created_at: &self.created_at,
                successor: &self.successor,
            },
        )
    }

    pub fn snapshot_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn semantic_hash_from_bytes(bytes: &[u8]) -> Result<ObjectHash, StoreProtocolError> {
        let meta: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        Ok(meta.snapshot_hash())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("SnapshotMeta serialization cannot fail")
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected: &StoreSnapshotRef,
        author: &StoreDeviceRegistration,
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
        meta.author_registration.verify_registration(author)?;
        if meta.generation != expected.generation {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: snapshot_semantic_prefix(
                    &author.device_id.to_string(),
                    expected.snapshot_hash,
                ),
                actual: snapshot_semantic_prefix(
                    &author.device_id.to_string(),
                    meta.snapshot_hash(),
                ),
            });
        }
        validate_snapshot_generation(meta.generation, meta.predecessor.as_ref())?;
        validate_commit_frontier(&meta.coverage)?;
        meta.state
            .validate(expected_store_root_hash, &meta.coverage)?;
        meta.history_summary
            .validate(expected_store_root_hash, &meta.coverage, &meta.state)?;
        if !keys::verify_signature_hex(
            &author.device_signing_pubkey,
            &meta.signature,
            &meta.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let actual = meta.snapshot_hash();
        if actual != expected.snapshot_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.snapshot_hash,
                actual,
            });
        }
        Ok(meta)
    }
}

fn validate_snapshot_generation(
    generation: u64,
    predecessor: Option<&StoreSnapshotRef>,
) -> Result<(), StoreProtocolError> {
    match (generation, predecessor) {
        (0, None) => Ok(()),
        (0, Some(_)) | (_, None) => Err(StoreProtocolError::Malformed(
            "Store snapshot generation and predecessor disagree".to_string(),
        )),
        (generation, Some(predecessor)) => {
            let expected = predecessor.generation.checked_add(1).ok_or_else(|| {
                StoreProtocolError::Malformed("Store snapshot generation overflow".to_string())
            })?;
            if generation != expected {
                return Err(StoreProtocolError::Malformed(
                    "Store snapshot generation does not follow its predecessor".to_string(),
                ));
            }
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCreationDescriptor {
    pub version: u32,
    pub creation_id: StoreCreationId,
    pub provider: super::storage::StoreProviderBinding,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
    pub write_policy: WritePolicy,
    pub founder_pubkey: String,
    pub founder_grant: MembershipGrantId,
    pub root_slot: ObjectSlot,
    pub founder_registration: ObjectSlot,
    pub founder_provider_admin: super::provider::FounderProviderAdminGrant,
    pub membership: StoreMembershipGenesis,
    pub founder_recovery: GrantStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreMembershipGenesis {
    MergeConcurrent {
        founder_membership: GrantStreamAnchor,
    },
    Serial,
}

impl StoreCreationDescriptor {
    pub fn store_root_id(&self) -> ObjectHash {
        ObjectHash::digest(&domain_json(b"coven.store-creation-descriptor.v1\0", self))
    }

    pub fn validate_merge_founder_entry(
        &self,
        founder: &MembershipEntry,
    ) -> Result<(), StoreProtocolError> {
        let StoreMembershipGenesis::MergeConcurrent { founder_membership } = &self.membership
        else {
            return Err(StoreProtocolError::InvalidFounder);
        };
        let MembershipChange::Founder {
            creation_id,
            owner_pubkey,
            owner_grant_id,
            membership,
            provider_admin,
        } = &founder.change
        else {
            return Err(StoreProtocolError::InvalidFounder);
        };
        if founder.store_id != self.store_root_id().to_string()
            || creation_id != &self.creation_id
            || founder.author_pubkey != self.founder_pubkey
            || founder.author_owner_grant != self.founder_grant
            || owner_pubkey != &self.founder_pubkey
            || owner_grant_id != &self.founder_grant
            || membership != founder_membership
            || provider_admin != &self.founder_provider_admin
            || founder.seq != 1
            || founder.previous_hash.is_some()
            || !founder.dependencies.is_empty()
            || !founder.resolution_dependencies.is_empty()
            || founder.provider_admin.is_some()
            || !verify_membership_entry(founder)
        {
            return Err(StoreProtocolError::InvalidFounder);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreProtocolRoot {
    pub descriptor: StoreCreationDescriptor,
    pub signature: String,
}

impl StoreProtocolRoot {
    pub fn signed(
        descriptor: StoreCreationDescriptor,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let mut store_protocol_root = Self {
            descriptor,
            signature: String::new(),
        };
        store_protocol_root.validate_descriptor()?;
        if keys::public_key_hex(signer) != store_protocol_root.descriptor.founder_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let (_, signature) = keys::sign_hex(signer, &store_protocol_root.canonical_signed_bytes());
        store_protocol_root.signature = signature;
        Ok(store_protocol_root)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(STORE_PROTOCOL_ROOT_DOMAIN, &self.descriptor)
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
        require_version(store_protocol_root.descriptor.version)?;
        store_protocol_root.validate_descriptor()?;
        if !keys::verify_signature_hex(
            &store_protocol_root.descriptor.founder_pubkey,
            &store_protocol_root.signature,
            &store_protocol_root.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(store_protocol_root)
    }

    pub fn parse_expected(
        bytes: &[u8],
        expected: &StoreRootRef,
        expected_write_policy: WritePolicy,
        expected_sync_routing_hash: ObjectHash,
    ) -> Result<Self, StoreProtocolError> {
        let store_protocol_root = Self::parse_pinned(bytes, expected)?;
        if store_protocol_root.descriptor.write_policy != expected_write_policy {
            return Err(StoreProtocolError::WritePolicyMismatch {
                expected: expected_write_policy,
                actual: store_protocol_root.descriptor.write_policy,
            });
        }
        if store_protocol_root.descriptor.sync_routing_hash != expected_sync_routing_hash {
            return Err(StoreProtocolError::SyncRoutingMismatch {
                expected: expected_sync_routing_hash,
                actual: store_protocol_root.descriptor.sync_routing_hash,
            });
        }
        Ok(store_protocol_root)
    }

    pub fn parse_pinned(bytes: &[u8], expected: &StoreRootRef) -> Result<Self, StoreProtocolError> {
        let store_protocol_root = Self::parse(bytes)?;
        let actual_hash = store_protocol_root.object_hash();
        if actual_hash != expected.store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected.store_root_hash,
                actual: actual_hash,
            });
        }
        let actual_root_id = store_protocol_root.descriptor.store_root_id();
        if actual_root_id != expected.store_root_id {
            return Err(StoreProtocolError::StoreRootIdMismatch {
                expected: expected.store_root_id,
                actual: actual_root_id,
            });
        }
        if expected.object.slot() != &store_protocol_root.descriptor.root_slot {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: serde_json::to_string(&store_protocol_root.descriptor.root_slot)
                    .expect("Store root slot serialization cannot fail"),
                actual: serde_json::to_string(expected.object.slot())
                    .expect("Store root slot serialization cannot fail"),
            });
        }
        Ok(store_protocol_root)
    }

    fn validate_descriptor(&self) -> Result<(), StoreProtocolError> {
        let descriptor = &self.descriptor;
        descriptor
            .provider
            .validate()
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        descriptor
            .founder_provider_admin
            .provider
            .validate_for(&descriptor.provider)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        descriptor
            .founder_provider_admin
            .capability
            .verify(
                &descriptor.provider,
                &descriptor.founder_provider_admin.provider,
                descriptor.write_policy == WritePolicy::Serial,
            )
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if !matches!(
            descriptor.founder_recovery,
            GrantStreamAnchor::OwnerRecovery { .. }
        ) {
            return Err(StoreProtocolError::InvalidFounder);
        }
        if descriptor.version != STORE_PROTOCOL_VERSION
            || descriptor.founder_pubkey.is_empty()
            || descriptor.root_slot.logical_key() != "store-v1/store-protocol-root.json"
            || !matches!(
                (&descriptor.write_policy, &descriptor.membership),
                (
                    WritePolicy::MergeConcurrent,
                    StoreMembershipGenesis::MergeConcurrent {
                        founder_membership: GrantStreamAnchor::StoreMembership { .. }
                    }
                ) | (WritePolicy::Serial, StoreMembershipGenesis::Serial)
            )
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
    #[error("Owner promotion evidence does not match its exact Store authority")]
    OwnerPromotionMismatch,
    #[error("Store protocol object is in slot {actual:?}, expected {expected:?}")]
    RelocatedSlot { expected: String, actual: String },
    #[error("Store package names key {actual:?}, expected {expected:?}")]
    RelocatedPackage { expected: String, actual: String },
    #[error("candidate object names key {actual:?}, expected {expected:?}")]
    RelocatedCandidateObject { expected: String, actual: String },
    #[error("Store protocol root hash is {actual}, expected {expected}")]
    StoreRootMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store protocol root id is {actual}, expected {expected}")]
    StoreRootIdMismatch {
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
    #[error("Store Merge membership control is invalid or signed by a different device")]
    InvalidMergeMembershipControl,
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
    #[error("device join attempt fields do not name one exact registration lifecycle")]
    JoinAttemptMismatch,
    #[error("device readiness proof differs from its exact attempt, registration, or initial acknowledgement")]
    DeviceReadinessMismatch,
    #[error("device join outcome differs from its exact attempt or closed outcome variant")]
    JoinOutcomeMismatch,
    #[error("provider access activation contains duplicate or contradictory exact authority")]
    ProviderAccessMismatch,
    #[error("Owner recovery node differs from its exact registration lifecycle")]
    OwnerRecoveryMismatch,
    #[error("Store device state differs from its signed predecessor state")]
    DeviceStateMismatch,
    #[error("Store batch has no package for circle {0}")]
    MissingCirclePackage(CircleId),
    #[error("Store batch has more than one package for circle {0}")]
    DuplicateCirclePackage(CircleId),
    #[error("Store batch has more than one control for circle {0}")]
    DuplicateCircleControl(CircleId),
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
    #[error("Store acknowledgement sequence must start at 1, got {0}")]
    InvalidAckSequence(u64),
    #[error("Store acknowledgement sequence 1 must not name a predecessor object")]
    UnexpectedAckPredecessor,
    #[error("Store acknowledgement after sequence 1 must name its predecessor object")]
    MissingAckPredecessor,
    #[error("Store commit for {0:?} must not name its own device as a dependency")]
    OwnDependency(String),
    #[error(
        "invalid membership coordinate {author}/{grant}/{stream_id}/{seq} with entry hash {entry_hash}"
    )]
    InvalidMembershipCoordinate {
        author: String,
        grant: String,
        stream_id: String,
        seq: u64,
        entry_hash: String,
    },
    #[error("invalid Store membership resolution authority for resolver {0:?}")]
    InvalidMembershipResolutionAuthority(String),
    #[error("membership object coordinate {expected:?} differs from signed entry {declared:?}")]
    MembershipCoordinateMismatch {
        expected: Box<MembershipCoord>,
        declared: Box<MembershipCoord>,
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
}

pub fn protocol_prefix() -> &'static str {
    STORE_PROTOCOL_PREFIX
}

pub fn serial_head_key() -> &'static str {
    STORE_SERIAL_HEAD_KEY
}

pub fn store_protocol_root_logical_key() -> &'static str {
    STORE_PROTOCOL_ROOT_SEMANTIC_PATH
}

pub fn device_join_attempt_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{STORE_DEVICE_JOIN_ATTEMPT_PREFIX}{}", attempt_id.0)
}

pub fn device_self_retirement_semantic_prefix(
    family: CandidateFamilyId,
    device_id: &StoreDeviceId,
    retirement_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_CANDIDATE_PREFIX}{}/device-self-retirements/{device_id}/{retirement_hash}",
        family.as_hash()
    )
}

pub fn circle_access_leaf_semantic_prefix(
    circle_id: CircleId,
    family: CandidateFamilyId,
    owner_pubkey: &str,
    epoch_id: CircleEpochId,
    recipient_slot: &str,
    leaf_id: AccessLeafId,
) -> String {
    format!(
        "circles/{circle_id}/candidates/{}/access-leaves/{owner_pubkey}/{epoch_id}/{recipient_slot}/{leaf_id}",
        family.as_hash(),
    )
}

pub fn circle_access_envelope_semantic_prefix(
    circle_id: CircleId,
    family: CandidateFamilyId,
    owner_pubkey: &str,
    recipient_slot: &str,
    control_hash: ObjectHash,
) -> String {
    format!(
        "circles/{circle_id}/candidates/{}/access-envelopes/{owner_pubkey}/{recipient_slot}/{control_hash}",
        family.as_hash(),
    )
}

pub fn device_join_outcome_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{STORE_DEVICE_JOIN_OUTCOME_PREFIX}{}", attempt_id.0)
}

pub fn device_join_abandonment_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    device_join_attempt_semantic_prefix(attempt_id)
}

pub fn device_join_cleanup_receipt_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{STORE_DEVICE_JOIN_CLEANUP_RECEIPT_PREFIX}{}", attempt_id.0)
}

pub fn device_exclusion_proposal_semantic_prefix(
    target: StoreDeviceId,
    proposal_id: StoreDeviceExclusionProposalId,
    proposal_hash: ObjectHash,
) -> String {
    format!("{STORE_DEVICE_EXCLUSION_PROPOSAL_PREFIX}{target}/{proposal_id}/{proposal_hash}")
}

pub fn device_exclusion_outcome_semantic_prefix(
    target: StoreDeviceId,
    proposal_id: StoreDeviceExclusionProposalId,
) -> String {
    format!("{STORE_DEVICE_EXCLUSION_OUTCOME_PREFIX}{target}/{proposal_id}")
}

pub fn provider_access_grant_semantic_prefix(
    grant_id: &super::provider::ProviderAccessGrantId,
) -> String {
    format!("{STORE_PROVIDER_ACCESS_GRANT_PREFIX}{}", grant_id.0)
}

pub fn provider_access_withdrawal_semantic_prefix(
    grant_id: &super::provider::ProviderAccessGrantId,
) -> String {
    format!("{STORE_PROVIDER_ACCESS_WITHDRAWAL_PREFIX}{}", grant_id.0)
}

pub fn owner_recovery_semantic_prefix(
    owner_pubkey: &str,
    owner_grant: MembershipGrantId,
    sequence: u64,
) -> String {
    format!("{STORE_OWNER_RECOVERY_PREFIX}{owner_pubkey}/{owner_grant}/{sequence}")
}

pub fn package_semantic_prefix(
    family: CandidateFamilyId,
    device_id: &str,
    seq: u64,
    package_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_CANDIDATE_PREFIX}{}/packages/{device_id}/{seq}/{package_hash}",
        family.as_hash()
    )
}

pub fn circle_package_semantic_prefix(
    circle_id: CircleId,
    family: CandidateFamilyId,
    device_id: &str,
    seq: u64,
    package_hash: ObjectHash,
) -> String {
    format!(
        "circles/{circle_id}/candidates/{}/packages/{device_id}/{seq}/{package_hash}",
        family.as_hash()
    )
}

pub fn commit_slot_prefix(device_id: &str, seq: u64) -> String {
    format!("{STORE_CANDIDATE_PREFIX}*/commits/{device_id}/{seq}")
}

pub fn commit_semantic_prefix(
    family: CandidateFamilyId,
    device_id: &str,
    seq: u64,
    commit_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_CANDIDATE_PREFIX}{}/commits/{device_id}/{seq}/{commit_hash}",
        family.as_hash()
    )
}

pub fn semantic_prefix_from_exact_object(
    object: &ExactObjectRef,
    extension: &str,
) -> Result<String, StoreProtocolError> {
    object
        .slot()
        .logical_key()
        .strip_suffix(extension)
        .map(str::to_string)
        .ok_or_else(|| StoreProtocolError::RelocatedSlot {
            expected: format!("candidate object ending in {extension}"),
            actual: object.slot().logical_key().to_string(),
        })
}

pub fn head_slot_prefix(device_id: &str, seq: u64) -> String {
    format!("{STORE_HEAD_PREFIX}{device_id}/{seq}")
}

pub fn head_semantic_prefix(device_id: &str, seq: u64, head_hash: ObjectHash) -> String {
    format!("{}/{head_hash}", head_slot_prefix(device_id, seq))
}

pub fn registration_slot_prefix(device_id: &str) -> String {
    format!("{STORE_DEVICE_REGISTRATION_PREFIX}{device_id}")
}

pub fn registration_semantic_prefix(device_id: &str) -> String {
    registration_slot_prefix(device_id)
}

pub fn founder_registration_semantic_prefix(creation_id: StoreCreationId) -> String {
    format!("store-v1/devices/founder/{creation_id}/registration")
}

pub fn founder_membership_head_semantic_prefix(creation_id: StoreCreationId) -> String {
    format!("{STORE_MEMBERSHIP_HEAD_PREFIX}founder/{creation_id}/1")
}

pub fn ack_slot_prefix(device_id: &str, revision: u64) -> String {
    format!("{STORE_ACK_PREFIX}{device_id}/{revision}")
}

pub fn ack_semantic_prefix(device_id: &str, revision: u64, ack_hash: ObjectHash) -> String {
    format!("{}/{ack_hash}", ack_slot_prefix(device_id, revision))
}

pub fn snapshot_slot_prefix(device_id: &str, generation: u64) -> String {
    format!("{STORE_SNAPSHOT_META_PREFIX}{device_id}/{generation}")
}

pub fn membership_entry_semantic_prefix(
    author: &str,
    author_owner_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
    entry_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_MEMBERSHIP_ENTRY_PREFIX}{author}/{author_owner_grant}/{stream_id}/{seq}/{entry_hash}"
    )
}

pub fn membership_head_semantic_prefix(
    author: &str,
    author_owner_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
    head_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_MEMBERSHIP_HEAD_PREFIX}{author}/{author_owner_grant}/{stream_id}/{seq}/{head_hash}"
    )
}

pub fn membership_head_slot_prefix(
    author: &str,
    author_owner_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
) -> String {
    format!("{STORE_MEMBERSHIP_HEAD_PREFIX}{author}/{author_owner_grant}/{stream_id}/{seq}")
}

pub fn membership_resolution_semantic_prefix(
    conflict_hash: ObjectHash,
    resolver: &str,
    resolution_hash: ObjectHash,
) -> String {
    format!("store-v1/membership/resolutions/{conflict_hash}/{resolver}/{resolution_hash}")
}

pub fn snapshot_image_semantic_prefix(author: &str, image_hash: ObjectHash) -> String {
    format!("{STORE_SNAPSHOT_IMAGE_PREFIX}{author}/{image_hash}")
}

pub fn snapshot_semantic_prefix(author: &str, snapshot_hash: ObjectHash) -> String {
    format!("{STORE_SNAPSHOT_META_PREFIX}{author}/{snapshot_hash}")
}

pub(crate) fn domain_json(domain: &[u8], value: &impl Serialize) -> Vec<u8> {
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

fn validate_commit_order(order: &StoreCommitOrder) -> Result<(), StoreProtocolError> {
    let seq = order.seq();
    if seq == 0 {
        return Err(StoreProtocolError::InvalidSequence(0));
    }
    match order {
        StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => {
            match (seq, predecessor) {
                (1, None) => {}
                (1, Some(_)) => return Err(StoreProtocolError::UnexpectedPredecessor),
                (_, None) => return Err(StoreProtocolError::MissingPredecessor),
                (_, Some(reference)) => match reference.coord {
                    StoreCommitCoord::MergeConcurrent { sequence, .. }
                        if sequence.checked_add(1) == Some(seq) => {}
                    _ => {
                        return Err(StoreProtocolError::Malformed(
                            "Merge predecessor is not the preceding Merge commit".to_string(),
                        ));
                    }
                },
            }
            for (stream_id, reference) in dependencies {
                match reference.coord {
                    StoreCommitCoord::MergeConcurrent {
                        stream_id: declared,
                        sequence,
                    } if declared == *stream_id && sequence > 0 => {}
                    _ => {
                        return Err(StoreProtocolError::Malformed(format!(
                            "Merge dependency {stream_id} has a different exact coordinate"
                        )));
                    }
                }
            }
        }
        StoreCommitOrder::Serial { predecessor, .. } => match (seq, predecessor) {
            (1, StoreSerialPredecessor::Genesis { .. }) => {}
            (1, StoreSerialPredecessor::Commit(_)) => {
                return Err(StoreProtocolError::UnexpectedPredecessor);
            }
            (_, StoreSerialPredecessor::Genesis { .. }) => {
                return Err(StoreProtocolError::MissingPredecessor);
            }
            (_, StoreSerialPredecessor::Commit(reference)) => match reference.coord {
                StoreCommitCoord::Serial { sequence } if sequence.checked_add(1) == Some(seq) => {}
                _ => {
                    return Err(StoreProtocolError::Malformed(
                        "Serial predecessor is not the preceding Serial commit".to_string(),
                    ));
                }
            },
        },
    }
    Ok(())
}

fn validate_commit_predecessor_states(
    order: &StoreCommitOrder,
    membership: &StoreMembershipStateRef,
    devices: &StoreDeviceStateRef,
) -> Result<(), StoreProtocolError> {
    membership.validate_shape()?;
    if membership.write_policy() != order.policy() || devices.write_policy() != order.policy() {
        return Err(StoreProtocolError::WritePolicyMismatch {
            expected: order.policy(),
            actual: if membership.write_policy() != order.policy() {
                membership.write_policy()
            } else {
                devices.write_policy()
            },
        });
    }
    if membership.recovery() != devices.recovery() {
        return Err(StoreProtocolError::OwnerRecoveryMismatch);
    }
    validate_recovery_cursors(membership.recovery())?;
    validate_recovery_cursors(devices.recovery())?;
    match (order, membership, devices) {
        (
            StoreCommitOrder::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            },
            StoreMembershipStateRef::MergeConcurrent { .. },
            StoreDeviceStateRef::MergeConcurrent { frontier, .. },
        ) => {
            let mut expected = dependencies.clone();
            if let Some(predecessor) = predecessor {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord else {
                    return Err(StoreProtocolError::Malformed(
                        "Merge predecessor has a Serial coordinate".to_string(),
                    ));
                };
                if expected
                    .insert(stream_id, predecessor.clone())
                    .is_some_and(|dependency| dependency != *predecessor)
                {
                    return Err(StoreProtocolError::Malformed(
                        "Merge predecessor disagrees with the same-stream dependency".to_string(),
                    ));
                }
            }
            if frontier != &CommitFrontier::MergeConcurrent(expected) {
                return Err(StoreProtocolError::Malformed(
                    "Store device state names a different Merge predecessor cut".to_string(),
                ));
            }
            Ok(())
        }
        (
            StoreCommitOrder::Serial { predecessor, .. },
            StoreMembershipStateRef::Serial(state),
            StoreDeviceStateRef::Serial {
                position: device_position,
                ..
            },
        ) => {
            if &state.position != predecessor || device_position != predecessor {
                return Err(StoreProtocolError::Malformed(
                    "Store predecessor state differs from the exact Serial predecessor".to_string(),
                ));
            }
            Ok(())
        }
        _ => Err(StoreProtocolError::WritePolicyMismatch {
            expected: order.policy(),
            actual: membership.write_policy(),
        }),
    }
}

fn validate_commit_frontier(frontier: &CommitFrontier) -> Result<(), StoreProtocolError> {
    match frontier {
        CommitFrontier::MergeConcurrent(frontier) => {
            for (stream_id, reference) in frontier {
                match reference.coord {
                    StoreCommitCoord::MergeConcurrent {
                        stream_id: declared,
                        sequence,
                    } if declared == *stream_id && sequence > 0 => {}
                    _ => {
                        return Err(StoreProtocolError::Malformed(format!(
                            "Merge frontier entry {stream_id} has a different exact coordinate"
                        )));
                    }
                }
            }
            Ok(())
        }
        CommitFrontier::Serial(Some(reference)) if !matches!(reference.coord, StoreCommitCoord::Serial { sequence } if sequence > 0) => {
            Err(StoreProtocolError::Malformed(
                "Serial frontier contains an invalid exact coordinate".to_string(),
            ))
        }
        CommitFrontier::Serial(_) => Ok(()),
    }
}

pub(crate) fn validate_store_history_cut(
    frontier: &StoreHistoryCut,
) -> Result<(), StoreProtocolError> {
    match frontier {
        StoreHistoryCut::MergeConcurrent(commits) => {
            validate_commit_frontier(&CommitFrontier::MergeConcurrent(commits.clone()))
        }
        StoreHistoryCut::Serial(StoreSerialPredecessor::Genesis { .. }) => Ok(()),
        StoreHistoryCut::Serial(StoreSerialPredecessor::Commit(reference)) => {
            validate_commit_frontier(&CommitFrontier::Serial(Some(reference.clone())))
        }
    }
}

fn validate_store_device_state_ref(state: &StoreDeviceStateRef) -> Result<(), StoreProtocolError> {
    validate_recovery_cursors(state.recovery())?;
    match state {
        StoreDeviceStateRef::MergeConcurrent { frontier, .. } => {
            if !matches!(frontier, CommitFrontier::MergeConcurrent(_)) {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            validate_commit_frontier(frontier)
        }
        StoreDeviceStateRef::Serial { position, .. } => {
            validate_store_history_cut(&StoreHistoryCut::Serial(position.clone()))
        }
    }
}

fn validate_ack_history_cut(
    store_root_hash: ObjectHash,
    _author: &StoreDeviceRegistrationRef,
    cut: &StoreHistoryCut,
) -> Result<(), StoreProtocolError> {
    if let StoreHistoryCut::Serial(StoreSerialPredecessor::Genesis {
        root,
        founder_registration: _,
    }) = cut
    {
        if root.store_root_hash != store_root_hash {
            return Err(StoreProtocolError::Malformed(
                "Serial genesis acknowledgement belongs to another exact Store root".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_successor_sequence(
    sequence: u64,
    successor: &SuccessorLink,
) -> Result<(), StoreProtocolError> {
    match (sequence, successor.predecessor.is_some()) {
        (0, _) => Err(StoreProtocolError::InvalidAckSequence(0)),
        (1, false) => Ok(()),
        (1, true) => Err(StoreProtocolError::UnexpectedAckPredecessor),
        (_, true) => Ok(()),
        (_, false) => Err(StoreProtocolError::MissingAckPredecessor),
    }
}

fn validate_ack_state(
    store_root_hash: ObjectHash,
    registration: &StoreDeviceRegistrationRef,
    store_cut: &StoreHistoryCut,
    device_state: &StoreDeviceStateRef,
    exclusions: &StoreAckExclusionState,
) -> Result<(), StoreProtocolError> {
    validate_store_history_cut(store_cut)?;
    validate_ack_history_cut(store_root_hash, registration, store_cut)?;
    let state_matches = match (store_cut, device_state) {
        (
            StoreHistoryCut::MergeConcurrent(commits),
            StoreDeviceStateRef::MergeConcurrent { frontier, .. },
        ) => frontier == &CommitFrontier::MergeConcurrent(commits.clone()),
        (
            StoreHistoryCut::Serial(position),
            StoreDeviceStateRef::Serial {
                position: device_position,
                ..
            },
        ) => position == device_position,
        _ => false,
    };
    if !state_matches {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    match (store_cut, exclusions) {
        (
            StoreHistoryCut::MergeConcurrent(_),
            StoreAckExclusionState::MergeConcurrent { proposal_freezes },
        ) => {
            if proposal_freezes
                .windows(2)
                .any(|pair| pair[0].proposal.proposal_id >= pair[1].proposal.proposal_id)
                || proposal_freezes
                    .iter()
                    .any(|freeze| !matches!(freeze.target_cut, StoreHistoryCut::MergeConcurrent(_)))
            {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
            for freeze in proposal_freezes {
                validate_store_history_cut(&freeze.target_cut)?;
                freeze.proposal.validate_path()?;
                if !store_cut.frontier().covers(&freeze.target_cut.frontier()) {
                    return Err(StoreProtocolError::DeviceStateMismatch);
                }
            }
            Ok(())
        }
        (StoreHistoryCut::Serial(_), StoreAckExclusionState::Serial) => Ok(()),
        _ => Err(StoreProtocolError::WritePolicyMismatch {
            expected: device_state.write_policy(),
            actual: match exclusions {
                StoreAckExclusionState::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
                StoreAckExclusionState::Serial => WritePolicy::Serial,
            },
        }),
    }
}

fn validate_membership_coord(coord: &MembershipCoord) -> Result<(), StoreProtocolError> {
    if coord.seq == 0 || coord.author_pubkey.is_empty() {
        return Err(StoreProtocolError::InvalidMembershipCoordinate {
            author: coord.author_pubkey.clone(),
            grant: coord.author_owner_grant.to_string(),
            stream_id: coord.stream_id.to_string(),
            seq: coord.seq,
            entry_hash: coord.entry_hash.to_string(),
        });
    }
    Ok(())
}

fn validate_membership_authority(
    authority: &MembershipGrantCreationAuthority,
) -> Result<(), StoreProtocolError> {
    match authority {
        MembershipGrantCreationAuthority::Entry(coord) => validate_membership_coord(coord),
        MembershipGrantCreationAuthority::ConflictResolution(reference) => {
            let resolver = hex::decode(&reference.resolver_pubkey).map_err(|_| {
                StoreProtocolError::InvalidMembershipResolutionAuthority(
                    reference.resolver_pubkey.clone(),
                )
            })?;
            if resolver.len() != crate::keys::SIGN_PUBLICKEYBYTES {
                return Err(StoreProtocolError::InvalidMembershipResolutionAuthority(
                    reference.resolver_pubkey.clone(),
                ));
            }
            Ok(())
        }
    }
}

fn validate_operation_membership_authority(
    policy: WritePolicy,
    authority: Option<&MembershipGrantCreationAuthority>,
) -> Result<(), StoreProtocolError> {
    match (policy, authority) {
        (WritePolicy::MergeConcurrent, Some(authority)) => validate_membership_authority(authority),
        (WritePolicy::Serial, None) => Ok(()),
        (WritePolicy::MergeConcurrent, None) => Err(StoreProtocolError::Malformed(
            "MergeConcurrent operations commit omits its predecessor membership grant authority"
                .to_string(),
        )),
        (WritePolicy::Serial, Some(_)) => Err(StoreProtocolError::Malformed(
            "Serial operations commit carries a MergeConcurrent membership grant authority"
                .to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::membership::founder_entry;

    fn routing_hash() -> ObjectHash {
        ObjectHash::digest(b"test-sync-schema")
    }

    struct Fixture {
        signer: UserKeypair,
        root: StoreProtocolRoot,
        root_ref: StoreRootRef,
        registration: StoreDeviceRegistration,
        registration_ref: StoreDeviceRegistrationRef,
        commit: StoreBatchCommit,
        commit_ref: StoreBatchCommitRef,
        package: Vec<u8>,
    }

    fn slot(key: String) -> ObjectSlot {
        ObjectSlot::logical(key).expect("valid test object slot")
    }

    fn exact(key: String, bytes: &[u8]) -> ExactObjectRef {
        ExactObjectRef::new(slot(key), bytes.len() as u64, ObjectHash::digest(bytes))
    }

    fn circle_activation(
        fixture: &Fixture,
        circle_id: CircleId,
        grant_id: MembershipGrantId,
        anchor: fn(CircleId, ObjectSlot) -> GrantStreamAnchor,
        first_slot: ObjectSlot,
    ) -> StreamActivation {
        StreamActivation::grant_authorized(
            fixture.root_ref.store_root_hash,
            fixture.registration_ref.clone(),
            grant_id,
            anchor(circle_id, first_slot),
        )
    }

    fn joined_registration(
        fixture: &Fixture,
        identity: &UserKeypair,
        label: &str,
    ) -> (StoreDeviceRegistration, StoreDeviceRegistrationRef) {
        let registration = StoreDeviceRegistration::signed(
            fixture.root_ref.clone(),
            StoreDeviceRegistrationOrigin::Join {
                attempt_id: DeviceJoinAttemptId::from_hash(ObjectHash::digest(label.as_bytes())),
                attempt_slot: slot(format!("store-v1/tests/{label}/join-attempt.json")),
                outcome_slot: slot(format!("store-v1/tests/{label}/join-outcome.json")),
            },
            crate::sync::storage::ProviderDeviceBinding {
                principal: crate::sync::storage::ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(label.as_bytes()),
                },
            },
            StoreCommitAnchor::Serial,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot(format!("store-v1/tests/{label}/acks/1.json")),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot(format!("store-v1/tests/{label}/snapshots/1.json")),
            },
            identity,
        )
        .expect("sign joined registration");
        let bytes = registration.to_bytes();
        let reference = StoreDeviceRegistrationRef::from_registration(
            &registration,
            exact(
                format!(
                    "{}.json",
                    registration_semantic_prefix(&registration.device_id.to_string())
                ),
                &bytes,
            ),
        );
        (registration, reference)
    }

    #[test]
    fn owner_promotion_request_and_acceptance_bind_both_exact_devices() {
        let fixture = fixture();
        let candidate_identity = UserKeypair::generate();
        let candidate_pubkey = keys::public_key_hex(&candidate_identity);
        let (candidate, candidate_ref) =
            joined_registration(&fixture, &candidate_identity, "promotion-candidate");
        let request = OwnerPromotionRequest::signed(
            OwnerPromotionId::from_generated("promotion-1".to_string()),
            &fixture.root_ref,
            fixture.registration_ref.clone(),
            &fixture.registration,
            fixture.root.descriptor.founder_grant.clone(),
            candidate_pubkey,
            MembershipGrantId(ObjectHash::digest(b"candidate Member grant")),
            candidate_ref,
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            OwnerPromotionFinalization::Serial,
            &fixture.signer,
        )
        .expect("sign promotion request");
        request
            .verify(&fixture.root_ref, &fixture.registration)
            .expect("verify promotion request");
        let acceptance = OwnerPromotionAcceptance::signed(
            request.clone(),
            OwnerPromotionRequestActivation::Serial {
                commit: fixture.commit_ref.clone(),
            },
            OwnerPromotionAnchors::Serial {
                recovery: GrantStreamAnchor::OwnerRecovery {
                    first_slot: slot(format!(
                        "{}.json",
                        owner_recovery_semantic_prefix(
                            &request.member_pubkey,
                            request.intended_owner_grant.clone(),
                            1,
                        )
                    )),
                },
            },
            &candidate,
            &candidate_identity,
        )
        .expect("sign promotion acceptance");
        acceptance
            .verify(&candidate)
            .expect("verify promotion acceptance");
        assert!(OwnerPromotionAcceptance::signed(
            request.clone(),
            OwnerPromotionRequestActivation::Serial {
                commit: fixture.commit_ref.clone(),
            },
            OwnerPromotionAnchors::Serial {
                recovery: GrantStreamAnchor::OwnerRecovery {
                    first_slot: slot(
                        "store-v1/recovery/another-owner/another-grant/1.json".to_string(),
                    ),
                },
            },
            &candidate,
            &candidate_identity,
        )
        .is_err());

        let mut substituted = request;
        substituted.member_grant = MembershipGrantId(ObjectHash::digest(b"other Member grant"));
        assert!(matches!(
            substituted.verify(&fixture.root_ref, &fixture.registration),
            Err(StoreProtocolError::InvalidSignature)
        ));
    }

    #[test]
    fn stream_activation_descriptor_and_locator_derivations_are_identical() {
        let fixture = fixture();
        let circle_id = CircleId::from_bytes([4; 16]);
        let other_circle = CircleId::from_bytes([5; 16]);
        let grant = MembershipGrantId(ObjectHash::digest(b"Circle activation grant"));
        let other_grant = MembershipGrantId(ObjectHash::digest(b"other Circle activation grant"));
        let first_slot = slot("store-v1/circles/stream/first.json".to_string());
        let activation = circle_activation(
            &fixture,
            circle_id,
            grant.clone(),
            |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                circle_id,
                first_slot,
            },
            first_slot.clone(),
        );
        let locator = StreamActivation::grant_authorized_stream_id(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            &grant,
            StreamAnchorDomain::CircleRoster { circle_id },
        );
        assert_eq!(activation.author_stream_id(), locator);
        let locator_text = locator.to_string();
        assert_eq!(locator_text.len(), 64);
        assert!(locator_text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

        let other_slot = circle_activation(
            &fixture,
            circle_id,
            grant.clone(),
            |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                circle_id,
                first_slot,
            },
            slot("store-v1/circles/stream/other-first.json".to_string()),
        );
        assert_eq!(activation.author_stream_id(), other_slot.author_stream_id());
        assert_ne!(activation.activation_id(), other_slot.activation_id());

        let other_domain = circle_activation(
            &fixture,
            circle_id,
            grant.clone(),
            |circle_id, first_slot| GrantStreamAnchor::CircleMetadata {
                circle_id,
                first_slot,
            },
            first_slot.clone(),
        );
        let other_circle = circle_activation(
            &fixture,
            other_circle,
            grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                circle_id,
                first_slot,
            },
            first_slot.clone(),
        );
        let other_grant = circle_activation(
            &fixture,
            circle_id,
            other_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                circle_id,
                first_slot,
            },
            first_slot,
        );
        assert_ne!(
            activation.author_stream_id(),
            other_domain.author_stream_id()
        );
        assert_ne!(
            activation.author_stream_id(),
            other_circle.author_stream_id()
        );
        assert_ne!(
            activation.author_stream_id(),
            other_grant.author_stream_id()
        );
    }

    #[test]
    fn commit_stream_activation_validation_rejects_wrong_authority_order_and_identity_collisions() {
        let (fixture, other_fixture) = (fixture(), fixture());
        let circle_id = CircleId::from_bytes([6; 16]);
        let grant = MembershipGrantId(ObjectHash::digest(b"validation Circle grant"));
        let control = circle_activation(
            &fixture,
            circle_id,
            grant.clone(),
            |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                circle_id,
                first_slot,
            },
            slot("store-v1/circles/validation/control.json".to_string()),
        );
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::Serial,
            None,
            std::slice::from_ref(&control),
        )
        .is_err());

        let mut wrong_root = control.clone();
        let StreamActivation::GrantAuthorized {
            store_root_hash, ..
        } = &mut wrong_root
        else {
            unreachable!()
        };
        *store_root_hash = ObjectHash::digest(b"wrong Store root");
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::MergeConcurrent,
            None,
            &[wrong_root],
        )
        .is_err());

        let mut wrong_registration = control.clone();
        let StreamActivation::GrantAuthorized {
            author_registration,
            ..
        } = &mut wrong_registration
        else {
            unreachable!()
        };
        *author_registration = other_fixture.registration_ref;
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::MergeConcurrent,
            None,
            &[wrong_registration],
        )
        .is_err());

        let non_circle = StreamActivation::grant_authorized(
            fixture.root_ref.store_root_hash,
            fixture.registration_ref.clone(),
            grant.clone(),
            GrantStreamAnchor::StoreMembership {
                first_slot: slot("store-v1/membership/non-circle.json".to_string()),
            },
        );
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::MergeConcurrent,
            None,
            &[non_circle],
        )
        .is_err());

        let roster = circle_activation(
            &fixture,
            circle_id,
            grant.clone(),
            |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                circle_id,
                first_slot,
            },
            slot("store-v1/circles/validation/roster.json".to_string()),
        );
        let mut unsorted = vec![control.clone(), roster.clone()];
        unsorted.sort();
        unsorted.reverse();
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::MergeConcurrent,
            None,
            &unsorted,
        )
        .is_err());
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::MergeConcurrent,
            None,
            &[control.clone(), control.clone()],
        )
        .is_err());

        let same_stream = circle_activation(
            &fixture,
            circle_id,
            grant.clone(),
            |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                circle_id,
                first_slot,
            },
            slot("store-v1/circles/validation/control-other.json".to_string()),
        );
        let mut duplicate_stream = vec![control.clone(), same_stream];
        duplicate_stream.sort();
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::MergeConcurrent,
            None,
            &duplicate_stream,
        )
        .is_err());

        let shared_slot = slot("store-v1/circles/validation/shared.json".to_string());
        let mut duplicate_slot = vec![
            circle_activation(
                &fixture,
                circle_id,
                grant.clone(),
                |circle_id, first_slot| GrantStreamAnchor::CircleRoster {
                    circle_id,
                    first_slot,
                },
                shared_slot.clone(),
            ),
            circle_activation(
                &fixture,
                circle_id,
                grant,
                |circle_id, first_slot| GrantStreamAnchor::CircleMetadata {
                    circle_id,
                    first_slot,
                },
                shared_slot,
            ),
        ];
        duplicate_slot.sort();
        assert!(validate_stream_activations(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            WritePolicy::MergeConcurrent,
            None,
            &duplicate_slot,
        )
        .is_err());
    }

    fn fixture() -> Fixture {
        let signer = UserKeypair::generate();
        let founder_grant =
            crate::sync::test_helpers::test_membership_grant_id("store-a founder grant");
        let provider_admin =
            crate::sync::test_helpers::test_serial_founder_provider_admin("store-a");
        let founder_recovery = GrantStreamAnchor::OwnerRecovery {
            first_slot: slot("store-v1/recovery/founder/1.json".to_string()),
        };
        let store_protocol_root = StoreProtocolRoot::signed(
            StoreCreationDescriptor {
                version: STORE_PROTOCOL_VERSION,
                creation_id: StoreCreationId::from_nonce("store-a"),
                provider: crate::sync::storage::StoreProviderBinding::S3 {
                    endpoint: crate::sync::storage::S3EndpointBinding::Custom {
                        origin: "https://test.invalid".to_string(),
                    },
                    region: "test-region".to_string(),
                    bucket: "store-a-bucket".to_string(),
                    key_prefix: None,
                },
                schema_version: 3,
                sync_routing_hash: routing_hash(),
                write_policy: WritePolicy::Serial,
                founder_pubkey: keys::public_key_hex(&signer),
                founder_grant: founder_grant.clone(),
                root_slot: slot(format!("{}.json", store_protocol_root_logical_key())),
                founder_registration: slot(
                    "store-v1/device-registrations/founder.json".to_string(),
                ),
                founder_provider_admin: provider_admin.clone(),
                membership: StoreMembershipGenesis::Serial,
                founder_recovery: founder_recovery.clone(),
            },
            &signer,
        )
        .expect("sign Store protocol root");
        let store_root_id = store_protocol_root.descriptor.store_root_id();
        let founder = founder_entry(
            &store_root_id.to_string(),
            &signer,
            founder_grant,
            "0000000001000-0000-device-a",
            GrantStreamAnchor::StoreMembership {
                first_slot: slot("store-v1/membership/founder/1.json".to_string()),
            },
            provider_admin,
        );
        let root_bytes = store_protocol_root.to_bytes();
        let root_ref = StoreRootRef {
            store_root_id,
            store_root_hash: store_protocol_root.object_hash(),
            object: exact(
                format!("{}.json", store_protocol_root_logical_key()),
                &root_bytes,
            ),
        };
        let registration = StoreDeviceRegistration::signed(
            root_ref.clone(),
            StoreDeviceRegistrationOrigin::Founder {
                creation_id: StoreCreationId::from_nonce("store-a"),
            },
            crate::sync::storage::ProviderDeviceBinding {
                principal: crate::sync::storage::ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(b"test access key"),
                },
            },
            StoreCommitAnchor::Serial,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot("store-v1/acks/founder/1.json".to_string()),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot("store-v1/snapshots/founder/1.json".to_string()),
            },
            &signer,
        )
        .expect("sign founder registration");
        let registration_bytes = registration.to_bytes();
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            exact(
                format!(
                    "{}.json",
                    registration_semantic_prefix(&registration.device_id.to_string())
                ),
                &registration_bytes,
            ),
        );
        let predecessor = StoreSerialPredecessor::Genesis {
            root: root_ref.clone(),
            founder_registration: registration_ref.clone(),
        };
        let resolved_devices = ResolvedStoreDeviceState::founder(
            &root_ref,
            registration_ref.clone(),
            &store_protocol_root.descriptor.founder_pubkey,
            founder.author_owner_grant.clone(),
            &founder_recovery,
        )
        .expect("founder device state");
        let device_state = StoreDeviceStateRef::serial(predecessor.clone(), &resolved_devices)
            .expect("founder device state ref");
        let membership = crate::sync::membership::SerialMembershipState::from_founder(
            root_ref.store_root_id,
            &founder,
        )
        .expect("founder membership");
        let authorization =
            crate::sync::membership::SerialAuthorizationState::from_test_membership(
                &founder, membership,
            )
            .expect("founder authorization");
        let membership_state = StoreMembershipStateRef::serial(
            predecessor.clone(),
            resolved_devices.recovery.clone(),
            &authorization,
        )
        .expect("founder membership ref");
        let package = b"package".to_vec();
        let write_id = WriteId::from_generated("canonical-write".to_string());
        let order = StoreCommitOrder::Serial {
            seq: 1,
            predecessor,
        };
        let candidate_family = CandidateFamilyId::derive(
            root_ref.store_root_hash,
            &registration_ref,
            &write_id,
            &order,
        );
        let package_object = exact(
            format!(
                "{}.pkg",
                package_semantic_prefix(
                    candidate_family,
                    SERIAL_STREAM_ID,
                    1,
                    ObjectHash::digest(&package),
                )
            ),
            &package,
        );
        let device_signer = registration.device_signer(&signer).unwrap();
        let commit = StoreBatchCommit::signed(
            root_ref.store_root_hash,
            write_id,
            StoreCommitCoord::Serial { sequence: 1 },
            registration_ref.clone(),
            &registration,
            order,
            membership_state,
            device_state,
            StoreOperationMembershipAuthority::Serial,
            StorePackageInput {
                candidate_family,
                schema_version: 3,
                bytes: &package,
                object: package_object,
            },
            &device_signer,
        )
        .expect("sign commit");
        let commit_bytes = commit.to_bytes();
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            StoreCommitCoord::Serial { sequence: 1 },
            exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        commit.candidate_family(),
                        SERIAL_STREAM_ID,
                        1,
                        commit.commit_hash(),
                    )
                ),
                &commit_bytes,
            ),
        )
        .expect("exact commit ref");
        Fixture {
            signer,
            root: store_protocol_root,
            root_ref,
            registration,
            registration_ref,
            commit,
            commit_ref,
            package,
        }
    }

    #[test]
    fn operations_authority_validation_requires_merge_and_forbids_serial_authority() {
        let fixture = fixture();
        let authority = MembershipGrantCreationAuthority::Entry(MembershipCoord {
            author_pubkey: keys::public_key_hex(&fixture.signer),
            author_owner_grant: fixture.root.descriptor.founder_grant,
            stream_id: AuthorStreamId::from_bytes([7; 32]),
            seq: 1,
            entry_hash: ObjectHash::digest(b"operations authority"),
        });

        assert!(
            validate_operation_membership_authority(WritePolicy::MergeConcurrent, None).is_err()
        );
        assert!(
            validate_operation_membership_authority(WritePolicy::Serial, Some(&authority)).is_err()
        );
    }

    #[test]
    fn serial_operations_reject_a_merge_membership_authority_at_parse() {
        let fixture = fixture();
        let mut commit = fixture.commit;
        commit.membership_authority =
            Some(MembershipGrantCreationAuthority::Entry(MembershipCoord {
                author_pubkey: keys::public_key_hex(&fixture.signer),
                author_owner_grant: fixture.root.descriptor.founder_grant,
                stream_id: AuthorStreamId::from_bytes([7; 32]),
                seq: 1,
                entry_hash: ObjectHash::digest(b"Serial commit Merge authority"),
            }));
        let device_signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        commit.signature = keys::sign_hex(&device_signer, &commit.canonical_signed_bytes()).1;

        assert!(StoreBatchCommit::parse_at(
            &commit.to_bytes(),
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        )
        .is_err());
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
        let fixture = fixture();
        let bytes = fixture.commit.to_bytes();
        let parsed = StoreBatchCommit::parse_at(
            &bytes,
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        )
        .expect("parse commit");
        parsed
            .verify_store_package(&fixture.package)
            .expect("verify package");
        assert_eq!(parsed, fixture.commit);
        assert!(fixture
            .commit
            .canonical_signed_bytes()
            .starts_with(COMMIT_DOMAIN));
    }

    #[test]
    fn commit_rejects_package_signature_and_coordinate_tamper() {
        let fixture = fixture();
        let mut tampered = fixture.commit.clone();
        tampered.signature.push('0');
        assert!(matches!(
            tampered.verify_at(
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            ),
            Err(StoreProtocolError::InvalidSignature)
        ));

        let mut tampered = fixture.commit.clone();
        let StoreCommitBody::Operations(operations) = &mut tampered.body else {
            panic!("fixture commit carries operations")
        };
        operations
            .store_package
            .as_mut()
            .expect("fixture has Store package")
            .content_hash = ObjectHash::digest(b"different");
        assert!(matches!(
            tampered.verify_at(
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            ),
            Err(StoreProtocolError::RelocatedPackage { .. })
        ));

        assert!(matches!(
            fixture.commit.verify_at(
                fixture.root_ref.store_root_hash,
                &StoreCommitCoord::Serial { sequence: 2 },
                &fixture.registration,
            ),
            Err(StoreProtocolError::RelocatedSlot { .. })
        ));
        assert!(matches!(
            fixture.commit.verify_store_package(b"different"),
            Err(StoreProtocolError::PackageLengthMismatch { .. })
                | Err(StoreProtocolError::PackageHashMismatch { .. })
        ));
        fixture
            .commit
            .verify_store_package(&fixture.package)
            .unwrap();
    }

    #[test]
    fn unknown_fields_and_versions_are_rejected() {
        let fixture = fixture();
        let mut value = serde_json::to_value(&fixture.commit).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(StoreBatchCommit::parse_at(
            &serde_json::to_vec(&value).unwrap(),
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        )
        .is_err());

        let mut value = serde_json::to_value(&fixture.commit).unwrap();
        value["version"] = serde_json::json!(2);
        assert!(matches!(
            StoreBatchCommit::parse_at(
                &serde_json::to_vec(&value).unwrap(),
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            ),
            Err(StoreProtocolError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn readiness_rejects_a_bootstrap_cut_other_than_the_signed_attempt_cut() {
        let fixture = fixture();
        let joiner = UserKeypair::generate();
        let attempt_id = DeviceJoinAttemptId::from_hash(ObjectHash::digest(b"join attempt"));
        let attempt_slot = slot("store-v1/device-join-attempts/test.json".to_string());
        let outcome_slot = slot("store-v1/device-join-outcomes/test.json".to_string());
        let registration_slot = slot("store-v1/device-registrations/joiner.json".to_string());
        let provider_admin = crate::sync::provider::ProviderAdminState::founder_from_root(
            fixture.root_ref.clone(),
            fixture.registration_ref.clone(),
            &fixture.root.descriptor.founder_provider_admin,
        )
        .records()
        .values()
        .next()
        .expect("founder provider administrator exists")
        .clone();
        let registration = StoreDeviceRegistration::signed(
            fixture.root_ref.clone(),
            StoreDeviceRegistrationOrigin::Join {
                attempt_id,
                attempt_slot: attempt_slot.clone(),
                outcome_slot: outcome_slot.clone(),
            },
            provider_admin.provider.clone(),
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements {
                    first_slot: slot("store-v1/heads/joiner/1.json".to_string()),
                },
            },
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot("store-v1/acks/joiner/1.json".to_string()),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot("store-v1/snapshots/joiner/1.json".to_string()),
            },
            &joiner,
        )
        .unwrap();
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            ExactObjectRef::new(
                registration_slot.clone(),
                registration.to_bytes().len() as u64,
                ObjectHash::digest(&registration.to_bytes()),
            ),
        );
        let membership = StoreMembershipStateRef::merge_concurrent(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            ObjectHash::digest(b"membership"),
        )
        .unwrap();
        let attempt_cut = StoreHistoryCut::MergeConcurrent(BTreeMap::new());
        let owner_device_signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        let offer = crate::sync::device_join::DeviceJoinOffer::signed(
            attempt_id,
            keys::public_key_hex(&joiner),
            fixture.root_ref.clone(),
            fixture.root.descriptor.provider.clone(),
            attempt_slot.clone(),
            outcome_slot.clone(),
            fixture.registration_ref.clone(),
            fixture.root.descriptor.founder_grant.clone(),
            provider_admin.clone(),
            &fixture.registration,
            &owner_device_signer,
        )
        .unwrap();
        let access_request = crate::sync::device_join::DeviceProviderAccessRequest::signed(
            offer,
            provider_admin.provider.clone(),
            &joiner,
        )
        .unwrap();
        let access_grant_id = crate::sync::provider::ProviderAccessGrantId::from_random_bytes(
            *ObjectHash::digest(b"join provider access grant").as_bytes(),
        );
        let access_grant = crate::sync::provider::StoreMemberProviderAccessGrant::signed(
            access_grant_id,
            keys::public_key_hex(&joiner),
            provider_admin.provider.clone(),
            provider_admin.access.clone(),
            provider_admin.grant_id.clone(),
            fixture.registration_ref.clone(),
            &fixture.root.descriptor.provider,
            &fixture.registration,
            &owner_device_signer,
        )
        .unwrap();
        let access_grant_ref = crate::sync::provider::StoreMemberProviderAccessGrantRef::from_grant(
            &access_grant,
            exact(
                provider_access_grant_semantic_prefix(&access_grant.grant_id) + ".json",
                &access_grant.to_bytes(),
            ),
        );
        let verified_root = crate::sync::store_objects::VerifiedObject {
            value: fixture.root.clone(),
            bytes: fixture.root.to_bytes(),
            semantic_hash: fixture.root_ref.store_root_hash,
            object: fixture.root_ref.object.clone(),
        };
        let approval = crate::sync::device_join::DeviceProviderAdmissionApproval::signed(
            access_request,
            crate::sync::provider::ActivatedStoreMemberProviderAccessGrant {
                grant: access_grant,
                grant_ref: access_grant_ref,
                activation: fixture.commit_ref.clone(),
            },
            crate::sync::device_join::DeviceProviderAdmissionChallenge::SamePrincipal,
            &verified_root,
            &fixture.registration,
            &owner_device_signer,
        )
        .unwrap();
        let attempt = DeviceJoinAttempt::signed(
            fixture.root_ref.clone(),
            attempt_id,
            attempt_slot.clone(),
            registration.clone(),
            registration_slot,
            outcome_slot,
            attempt_cut,
            membership,
            provider_admin.grant_id,
            approval,
            crate::sync::device_join::DeviceProviderResponseReservation::SamePrincipal,
            fixture.registration_ref.clone(),
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.registration,
            &owner_device_signer,
        )
        .unwrap();
        let attempt_ref = DeviceJoinAttemptRef {
            attempt_id,
            attempt_hash: attempt.attempt_hash(),
            object: ExactObjectRef::new(
                attempt_slot,
                attempt.to_bytes().len() as u64,
                ObjectHash::digest(&attempt.to_bytes()),
            ),
        };
        let stream_id = AuthorStreamId::from_digest(ObjectHash::digest(b"other stream"));
        let other_commit_hash = ObjectHash::digest(b"other commit");
        let other_commit = StoreBatchCommitRef {
            coord: StoreCommitCoord::MergeConcurrent {
                stream_id,
                sequence: 1,
            },
            commit_hash: other_commit_hash,
            object: exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        CandidateFamilyId::from_hash(ObjectHash::digest(
                            b"other commit candidate family",
                        )),
                        &stream_id.to_string(),
                        1,
                        other_commit_hash,
                    )
                ),
                b"other commit",
            ),
        };
        let other_frontier = BTreeMap::from([(stream_id, other_commit)]);
        let other_cut = StoreHistoryCut::MergeConcurrent(other_frontier.clone());
        let other_device_state = StoreDeviceStateRef::MergeConcurrent {
            frontier: CommitFrontier::MergeConcurrent(other_frontier),
            recovery: Vec::new(),
            state_hash: ObjectHash::digest(b"other device state"),
        };
        let device_signer = registration.device_signer(&joiner).unwrap();
        let ack = StoreAck::signed(
            fixture.root_ref.store_root_hash,
            registration_ref.clone(),
            1,
            other_cut.clone(),
            other_device_state,
            None,
            StoreAckExclusionState::MergeConcurrent {
                proposal_freezes: Vec::new(),
            },
            "2026-07-16T00:00:00Z".to_string(),
            SuccessorLink {
                activation: registration
                    .store_acknowledgement_activation(&registration_ref)
                    .expect("derive exact Store acknowledgement activation")
                    .activation_id(),
                predecessor: None,
                next_slot: slot("store-v1/acks/joiner/2.json".to_string()),
            },
            &device_signer,
        )
        .unwrap();
        let ack_ref = StoreAckRef {
            registration: registration_ref.clone(),
            sequence: 1,
            ack_hash: ack.ack_hash(),
            object: exact("store-v1/acks/joiner/1.json".to_string(), &ack.to_bytes()),
        };
        let proof = DeviceReadinessProof::signed(
            attempt_ref.clone(),
            registration_ref,
            ack_ref.clone(),
            other_cut,
            &registration,
            &device_signer,
        )
        .unwrap();

        assert!(matches!(
            proof.verify(&attempt_ref, &attempt, &registration, &ack_ref, &ack),
            Err(StoreProtocolError::DeviceReadinessMismatch)
        ));
    }

    #[test]
    fn store_ack_semantic_hash_is_distinct_from_its_stored_json_hash() {
        let signer = UserKeypair::generate();
        let store_root_hash = ObjectHash::digest(b"ack semantic hash Store root");
        let root_ref = StoreRootRef {
            store_root_id: ObjectHash::digest(b"ack semantic hash Store id"),
            store_root_hash,
            object: exact(store_protocol_root_logical_key().to_string(), b"Store root"),
        };
        let origin = StoreDeviceRegistrationOrigin::Founder {
            creation_id: StoreCreationId::from_nonce("ack semantic hash founder"),
        };
        let registration_ref = StoreDeviceRegistrationRef {
            device_id: StoreDeviceId::derive(&root_ref, &origin),
            registration_hash: ObjectHash::digest(b"ack semantic hash registration"),
            object: exact(
                "store-v1/device-registrations/founder.json".to_string(),
                b"registration",
            ),
        };
        let store_cut = StoreHistoryCut::Serial(StoreSerialPredecessor::Genesis {
            root: root_ref.clone(),
            founder_registration: registration_ref.clone(),
        });
        let device_state = StoreDeviceStateRef::Serial {
            position: store_cut.serial_predecessor().unwrap().clone(),
            recovery: Vec::new(),
            state_hash: ObjectHash::digest(b"ack semantic hash device state"),
        };
        let ack = StoreAck::signed(
            store_root_hash,
            registration_ref.clone(),
            1,
            store_cut,
            device_state,
            None,
            StoreAckExclusionState::Serial,
            "2026-07-16T00:00:00Z".to_string(),
            SuccessorLink {
                activation: StreamActivation::device_authorized(
                    store_root_hash,
                    registration_ref.clone(),
                    DeviceStreamAnchor::StoreAcknowledgements {
                        first_slot: slot("store-v1/acks/founder/1.json".to_string()),
                    },
                )
                .activation_id(),
                predecessor: None,
                next_slot: slot("store-v1/acks/founder/2.json".to_string()),
            },
            &signer,
        )
        .unwrap();
        let bytes = ack.to_bytes();
        let semantic_hash = StoreAck::semantic_hash_from_bytes(&bytes).unwrap();

        assert_eq!(semantic_hash, ack.ack_hash());
        assert_ne!(semantic_hash, ObjectHash::digest(&bytes));
    }

    #[test]
    fn store_ack_wire_shape_binds_activation_state_without_a_parallel_predecessor_ref() {
        let signer = UserKeypair::generate();
        let store_root_hash = ObjectHash::digest(b"ack wire shape Store root");
        let root_ref = StoreRootRef {
            store_root_id: ObjectHash::digest(b"ack wire shape Store id"),
            store_root_hash,
            object: exact(store_protocol_root_logical_key().to_string(), b"Store root"),
        };
        let origin = StoreDeviceRegistrationOrigin::Founder {
            creation_id: StoreCreationId::from_nonce("ack wire shape founder"),
        };
        let registration_ref = StoreDeviceRegistrationRef {
            device_id: StoreDeviceId::derive(&root_ref, &origin),
            registration_hash: ObjectHash::digest(b"ack wire shape registration"),
            object: exact(
                "store-v1/device-registrations/founder.json".to_string(),
                b"registration",
            ),
        };
        let store_cut = StoreHistoryCut::Serial(StoreSerialPredecessor::Genesis {
            root: root_ref.clone(),
            founder_registration: registration_ref.clone(),
        });
        let device_state = StoreDeviceStateRef::Serial {
            position: store_cut.serial_predecessor().unwrap().clone(),
            recovery: Vec::new(),
            state_hash: ObjectHash::digest(b"ack wire shape device state"),
        };
        let ack = StoreAck::signed(
            store_root_hash,
            registration_ref.clone(),
            1,
            store_cut,
            device_state,
            None,
            StoreAckExclusionState::Serial,
            "2026-07-18T00:00:00Z".to_string(),
            SuccessorLink {
                activation: StreamActivation::device_authorized(
                    store_root_hash,
                    registration_ref,
                    DeviceStreamAnchor::StoreAcknowledgements {
                        first_slot: slot("store-v1/acks/founder/1.json".to_string()),
                    },
                )
                .activation_id(),
                predecessor: None,
                next_slot: slot("store-v1/acks/founder/2.json".to_string()),
            },
            &signer,
        )
        .unwrap();
        let value = serde_json::to_value(ack).unwrap();

        assert!(value.get("registration").is_some());
        assert!(value.get("sequence").is_some());
        assert!(value.get("device_state").is_some());
        assert!(value.get("snapshot").is_some());
        assert!(value.get("exclusions").is_some());
        assert!(value.get("author_registration").is_none());
        assert!(value.get("revision").is_none());
        assert!(value.get("predecessor").is_none());
    }

    #[test]
    fn store_protocol_root_authenticates_the_creation_descriptor() {
        let fixture = fixture();
        let bytes = fixture.root.to_bytes();
        let parsed = StoreProtocolRoot::parse_expected(
            &bytes,
            &fixture.root_ref,
            WritePolicy::Serial,
            routing_hash(),
        )
        .expect("parse exact Store protocol root");
        assert_eq!(parsed, fixture.root);
    }

    #[test]
    fn store_protocol_root_signs_the_required_write_policy() {
        let fixture = fixture();
        let value = serde_json::to_value(fixture.root).expect("serialize Store root");

        let descriptor = value
            .get("descriptor")
            .expect("Store root carries its creation descriptor");
        assert_eq!(
            descriptor.get("write_policy"),
            Some(&serde_json::json!("serial"))
        );
        assert!(
            descriptor.get("sync_routing_hash").is_some(),
            "the signed Store root must bind the sync-routing contract"
        );
    }

    #[test]
    fn operations_commit_uses_the_closed_body_and_signed_manifest_shape() {
        let fixture = fixture();
        let value = serde_json::to_value(&fixture.commit).expect("serialize Store commit");

        assert!(value.get("package").is_none());
        assert!(value.get("store_package").is_none());
        assert!(value.get("device_registrations").is_none());
        assert!(value.get("device_retirements").is_none());
        assert!(value.get("circle_controls").is_none());
        assert!(value.get("circle_packages").is_none());
        let operations = value
            .get("body")
            .and_then(|body| body.get("operations"))
            .expect("Store commit carries one closed operations body");
        assert!(operations.get("store_package").is_some());
        assert_eq!(
            operations.get("device_registrations"),
            Some(&serde_json::json!([]))
        );
        assert_eq!(
            operations.get("device_join_attempt_decisions"),
            Some(&serde_json::json!([]))
        );
        assert_eq!(
            operations.get("circle_controls"),
            Some(&serde_json::json!([]))
        );
        assert_eq!(
            operations.get("circle_packages"),
            Some(&serde_json::json!([]))
        );
        assert_eq!(
            value
                .get("candidate_objects")
                .and_then(|manifest| manifest.get("family")),
            Some(&serde_json::to_value(fixture.commit.candidate_family()).unwrap())
        );
    }

    #[test]
    fn one_join_attempt_cannot_be_activated_and_abandoned_in_the_same_commit() {
        let attempt_id = DeviceJoinAttemptId::from_hash(ObjectHash::digest(b"join attempt"));
        let attempt = DeviceJoinAttemptRef {
            attempt_id,
            attempt_hash: ObjectHash::digest(b"attempt body"),
            object: exact(
                "store-v1/device-join-attempts/attempt.json".to_string(),
                b"attempt body",
            ),
        };
        let abandonment = super::super::device_join::DeviceJoinAbandonmentRef {
            attempt_id,
            abandonment_hash: ObjectHash::digest(b"abandonment body"),
            object: exact(
                "store-v1/device-join-abandonments/attempt.json".to_string(),
                b"abandonment body",
            ),
        };

        assert_eq!(
            validate_device_join_attempt_decision_refs(&[
                DeviceJoinAttemptDecisionRef::Attempt(attempt),
                DeviceJoinAttemptDecisionRef::Abandoned(abandonment),
            ]),
            Err(StoreProtocolError::JoinAttemptMismatch)
        );
    }

    fn resign_commit(commit: &mut StoreBatchCommit, fixture: &Fixture) {
        let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        commit.signature = keys::sign_hex(&signer, &commit.canonical_signed_bytes()).1;
    }

    fn candidate_cleanup_manifest(fixture: &Fixture, label: &str) -> CandidateCleanupManifest {
        let package = label.as_bytes();
        let write_id = WriteId::from_generated(format!("{label}-write"));
        let order = fixture.commit.order.clone();
        let sequence = order.seq();
        let family = CandidateFamilyId::derive(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            &write_id,
            &order,
        );
        let package_object = exact(
            format!(
                "{}.pkg",
                package_semantic_prefix(
                    family,
                    SERIAL_STREAM_ID,
                    sequence,
                    ObjectHash::digest(package),
                )
            ),
            package,
        );
        let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        let commit = StoreBatchCommit::signed(
            fixture.root_ref.store_root_hash,
            write_id,
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref.clone(),
            &fixture.registration,
            order,
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            StoreOperationMembershipAuthority::Serial,
            StorePackageInput {
                candidate_family: family,
                schema_version: 3,
                bytes: package,
                object: package_object,
            },
            &signer,
        )
        .expect("sign candidate commit");
        let bytes = commit.to_bytes();
        CandidateCleanupManifest {
            candidate: StoreBatchCommitDeletionTarget {
                coord: fixture.commit_ref.coord.clone(),
                object: exact(
                    format!(
                        "{}.json",
                        commit_semantic_prefix(
                            commit.candidate_family(),
                            SERIAL_STREAM_ID,
                            commit.seq(),
                            commit.commit_hash(),
                        )
                    ),
                    &bytes,
                ),
                canonical_signed_bytes: bytes,
            },
        }
    }

    fn sign_candidate_abandonment(
        fixture: &Fixture,
        manifests: Vec<CandidateCleanupManifest>,
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        StoreBatchCommit::signed_with_candidate_abandonment(
            fixture.root_ref.store_root_hash,
            WriteId::from_generated("abandon-candidates".to_string()),
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref.clone(),
            &fixture.registration,
            fixture.commit.order.clone(),
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            manifests,
            &signer,
        )
    }

    #[test]
    fn candidate_abandonment_is_signed_canonical_cleanup_authority() {
        let fixture = fixture();
        let first = candidate_cleanup_manifest(&fixture, "first candidate");
        let second = candidate_cleanup_manifest(&fixture, "second candidate");
        let commit = sign_candidate_abandonment(&fixture, vec![second.clone(), first.clone()])
            .expect("sign candidate abandonment");

        assert!(commit.candidate_objects.objects.is_empty());
        assert_eq!(
            commit.abandoned_candidates(),
            [first, second]
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        );
        commit
            .verify_at(
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            )
            .expect("verify candidate abandonment");
    }

    #[test]
    fn candidate_abandonment_rejects_duplicate_and_inexact_targets() {
        let fixture = fixture();
        let manifest = candidate_cleanup_manifest(&fixture, "candidate");
        assert!(matches!(
            sign_candidate_abandonment(&fixture, vec![manifest.clone(), manifest.clone()]),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("strictly sorted and unique")
        ));

        let mut inexact = manifest;
        inexact.candidate.object = exact(
            inexact.candidate.object.slot().logical_key().to_string(),
            b"different stored bytes",
        );
        assert!(matches!(
            sign_candidate_abandonment(&fixture, vec![inexact]),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("does not match stored size/hash")
        ));
    }

    #[test]
    fn candidate_abandonment_rejects_noncanonical_or_unsigned_candidate_bytes() {
        let fixture = fixture();
        let manifest = candidate_cleanup_manifest(&fixture, "candidate");
        let candidate: StoreBatchCommit =
            serde_json::from_slice(&manifest.candidate.canonical_signed_bytes).unwrap();

        let mut noncanonical = manifest.clone();
        noncanonical.candidate.canonical_signed_bytes =
            serde_json::to_vec_pretty(&candidate).expect("serialize noncanonical candidate");
        noncanonical.candidate.object = exact(
            noncanonical
                .candidate
                .object
                .slot()
                .logical_key()
                .to_string(),
            &noncanonical.candidate.canonical_signed_bytes,
        );
        assert!(matches!(
            sign_candidate_abandonment(&fixture, vec![noncanonical]),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("not canonical")
        ));

        let mut unsigned_candidate = candidate;
        unsigned_candidate.signature.push('0');
        let mut unsigned = manifest;
        unsigned.candidate.canonical_signed_bytes = unsigned_candidate.to_bytes();
        unsigned.candidate.object = exact(
            unsigned.candidate.object.slot().logical_key().to_string(),
            &unsigned.candidate.canonical_signed_bytes,
        );
        assert!(matches!(
            sign_candidate_abandonment(&fixture, vec![unsigned]),
            Err(StoreProtocolError::InvalidSignature)
        ));
    }

    #[test]
    fn candidate_abandonment_rejects_retained_authority_target() {
        let fixture = fixture();
        let inner = sign_candidate_abandonment(
            &fixture,
            vec![candidate_cleanup_manifest(&fixture, "candidate")],
        )
        .expect("sign inner candidate abandonment");
        let bytes = inner.to_bytes();
        let retained = CandidateCleanupManifest {
            candidate: StoreBatchCommitDeletionTarget {
                coord: fixture.commit_ref.coord.clone(),
                object: exact(
                    format!(
                        "{}.json",
                        commit_semantic_prefix(
                            inner.candidate_family(),
                            SERIAL_STREAM_ID,
                            inner.seq(),
                            inner.commit_hash(),
                        )
                    ),
                    &bytes,
                ),
                canonical_signed_bytes: bytes,
            },
        };

        assert!(matches!(
            sign_candidate_abandonment(&fixture, vec![retained]),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("retained authority")
        ));
    }

    #[test]
    fn parsed_candidate_abandonment_rejects_noncanonical_manifest_order() {
        let fixture = fixture();
        let first = candidate_cleanup_manifest(&fixture, "first candidate");
        let second = candidate_cleanup_manifest(&fixture, "second candidate");
        let mut commit = sign_candidate_abandonment(&fixture, vec![first, second])
            .expect("sign candidate abandonment");
        let StoreCommitBody::AbandonCandidates { manifests } = &mut commit.body else {
            panic!("commit carries candidate abandonment")
        };
        manifests.reverse();
        resign_commit(&mut commit, &fixture);

        assert!(matches!(
            commit.verify_at(
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            ),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("strictly sorted and unique")
        ));
    }

    #[test]
    fn commit_rejects_manifest_omission_invention_and_family_substitution() {
        let fixture = fixture();

        let mut omitted = fixture.commit.clone();
        omitted.candidate_objects.objects.clear();
        resign_commit(&mut omitted, &fixture);
        assert!(matches!(
            omitted.verify_at(
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            ),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("manifest differs")
        ));

        let mut invented = fixture.commit.clone();
        invented
            .candidate_objects
            .objects
            .push(invented.candidate_objects.objects[0].clone());
        resign_commit(&mut invented, &fixture);
        assert!(matches!(
            invented.verify_at(
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            ),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("manifest differs")
        ));

        let mut substituted = fixture.commit.clone();
        substituted.candidate_objects.family =
            CandidateFamilyId::from_hash(ObjectHash::digest(b"substituted candidate family"));
        resign_commit(&mut substituted, &fixture);
        assert!(matches!(
            substituted.verify_at(
                fixture.root_ref.store_root_hash,
                &fixture.commit_ref.coord,
                &fixture.registration,
            ),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("manifest differs")
        ));
    }

    fn closed_store_package_fixture(
        fixture: &Fixture,
    ) -> (StoreBatchCommit, StoreBatchCommitRef, Vec<u8>, Vec<u8>) {
        let write_id = WriteId::from_generated("closed-package-graph".to_string());
        let order = fixture.commit.order.clone();
        let sequence = order.seq();
        let family = CandidateFamilyId::derive(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            &write_id,
            &order,
        );
        let package = super::super::audience_package::AudiencePackage::store(
            fixture.root_ref.store_root_hash,
            family,
            write_id.clone(),
            fixture.commit_ref.coord.clone(),
            3,
            b"closed graph changeset".to_vec(),
            Vec::new(),
        )
        .unwrap();
        let semantic = package.to_bytes();
        let stored = b"encrypted closed graph package".to_vec();
        let package_object = exact(
            format!(
                "{}.pkg",
                package_semantic_prefix(
                    family,
                    SERIAL_STREAM_ID,
                    sequence,
                    ObjectHash::digest(&semantic),
                )
            ),
            &stored,
        );
        let signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        let commit = StoreBatchCommit::signed(
            fixture.root_ref.store_root_hash,
            write_id,
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref.clone(),
            &fixture.registration,
            order,
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            StoreOperationMembershipAuthority::Serial,
            StorePackageInput {
                candidate_family: family,
                schema_version: 3,
                bytes: &semantic,
                object: package_object,
            },
            &signer,
        )
        .unwrap();
        let commit_bytes = commit.to_bytes();
        let reference = StoreBatchCommitRef::from_commit(
            &commit,
            fixture.commit_ref.coord.clone(),
            exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        family,
                        SERIAL_STREAM_ID,
                        sequence,
                        commit.commit_hash(),
                    )
                ),
                &commit_bytes,
            ),
        )
        .unwrap();
        (commit, reference, semantic, stored)
    }

    #[test]
    fn closed_candidate_graph_rejects_omitted_invented_and_substituted_package_material() {
        let fixture = fixture();
        let (commit, owner, semantic, stored) = closed_store_package_fixture(&fixture);
        let package = commit.store_package().cloned().unwrap();
        let graph =
            super::super::remote_object::CandidateObjectGraph::from_commit(&commit).unwrap();
        assert!(matches!(
            graph.clone().close(&commit, &owner, Vec::new()),
            Err(super::super::remote_object::RemoteObjectRecordError::CandidateObjectMissing)
        ));
        let exact_material = super::super::remote_object::CandidateObjectMaterial {
            object: package.object.clone(),
            canonical_semantic_bytes: semantic.clone(),
            stored_bytes: stored.clone(),
        };
        let invented_material = super::super::remote_object::CandidateObjectMaterial {
            object: exact("store-v1/candidates/invented.pkg".to_string(), b"invented"),
            canonical_semantic_bytes: b"invented".to_vec(),
            stored_bytes: b"invented".to_vec(),
        };
        assert!(matches!(
            graph.clone().close(
                &commit,
                &owner,
                vec![exact_material.clone(), invented_material]
            ),
            Err(super::super::remote_object::RemoteObjectRecordError::CandidateObjectInvented)
        ));
        let mut records = graph.close(&commit, &owner, vec![exact_material]).unwrap();
        let super::super::remote_object::RemoteObjectRecord::CandidateExclusive(record) =
            &mut records[0]
        else {
            panic!("package graph must close as candidate-exclusive")
        };
        record.identity.domain =
            super::super::remote_object::CandidateExclusiveObjectDomain::CirclePackage {
                reference: CirclePackageRef {
                    circle_id: CircleId::from_bytes([9; 16]),
                    control: CircleControlCoord::Serial {
                        author_pubkey: keys::public_key_hex(&fixture.signer),
                        generation: 1,
                        control_hash: ObjectHash::digest(b"substituted control"),
                    },
                    package,
                    key_fingerprint: KeyFingerprint::from_bytes([7; 8]),
                },
            };
        assert!(matches!(
            records[0].validate(),
            Err(super::super::remote_object::RemoteObjectRecordError::DomainMismatch)
        ));
    }

    #[test]
    fn candidate_manifest_rejects_one_exact_object_reached_twice() {
        let fixture = fixture();
        let mut operations = fixture
            .commit
            .operations()
            .expect("fixture commit carries operations")
            .clone();
        let package = operations
            .store_package
            .clone()
            .expect("fixture commit carries a Store package");
        operations.circle_packages.push(CirclePackageRef {
            circle_id: CircleId::from_bytes([8; 16]),
            control: CircleControlCoord::Serial {
                author_pubkey: keys::public_key_hex(&fixture.signer),
                generation: 1,
                control_hash: ObjectHash::digest(b"duplicate exact object control"),
            },
            package,
            key_fingerprint: KeyFingerprint::from_bytes([9; 8]),
        });

        assert!(matches!(
            candidate_manifest(
                fixture.commit.candidate_family(),
                &StoreCommitBody::Operations(operations),
            ),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("repeats an exact object reference")
        ));
    }

    #[test]
    fn candidate_manifest_rejects_duplicate_circle_access_with_distinct_provider_ids() {
        let fixture = fixture();
        let family = fixture.commit.candidate_family();
        let circle_id = CircleId::from_bytes([7; 16]);
        let owner_pubkey = keys::public_key_hex(&fixture.signer);
        let recipient_slot = "recipient-slot".to_string();
        let ids = crate::id_provider::SequentialIdProvider::new("duplicate Circle access");
        let epoch_id = CircleEpochId::generate(&ids);
        let leaf_id = AccessLeafId::generate(&ids);
        let leaf_hash = ObjectHash::digest(b"sealed access leaf");
        let control_hash = ObjectHash::digest(b"Circle access control");
        let leaf_key = circle_access_leaf_semantic_prefix(
            circle_id,
            family,
            &owner_pubkey,
            epoch_id,
            &recipient_slot,
            leaf_id,
        );
        let envelope_key = format!(
            "{}.json",
            circle_access_envelope_semantic_prefix(
                circle_id,
                family,
                &owner_pubkey,
                &recipient_slot,
                control_hash,
            )
        );
        let access = |provider_id: &str| CircleAccessObjectRef {
            leaf: CircleAccessLeafObjectRef {
                owner_pubkey: owner_pubkey.clone(),
                epoch_id,
                recipient_slot: recipient_slot.clone(),
                leaf_id,
                leaf_hash,
                object: ExactObjectRef::new(
                    ObjectSlot::opaque(leaf_key.clone(), format!("{provider_id}-leaf")).unwrap(),
                    18,
                    leaf_hash,
                ),
            },
            envelope: CircleAccessEnvelopeObjectRef {
                owner_pubkey: owner_pubkey.clone(),
                recipient_slot: recipient_slot.clone(),
                control_hash,
                leaf_id,
                leaf_hash,
                object: ExactObjectRef::new(
                    ObjectSlot::opaque(envelope_key.clone(), format!("{provider_id}-envelope"))
                        .unwrap(),
                    20,
                    ObjectHash::digest(provider_id.as_bytes()),
                ),
            },
        };
        let control = CircleControlCoord::Serial {
            author_pubkey: owner_pubkey.clone(),
            generation: 1,
            control_hash,
        };
        let mut operations = fixture
            .commit
            .operations()
            .expect("fixture commit carries operations")
            .clone();
        operations.circle_controls.push(CircleControlRef::Serial {
            circle_id,
            control,
            objects: CircleActivationObjects {
                control: exact("circle-control.json".to_string(), b"control"),
                roster_entries: BTreeMap::new(),
                roster_heads: Vec::new(),
                roster_resolutions: BTreeMap::new(),
                metadata_entries: BTreeMap::new(),
                metadata_heads: Vec::new(),
                access: vec![access("drive-file-a"), access("drive-file-b")],
            },
        });

        assert!(matches!(
            candidate_manifest(family, &StoreCommitBody::Operations(operations)),
            Err(StoreProtocolError::Malformed(reason))
                if reason.contains("repeats a Circle access semantic key")
        ));
    }

    #[test]
    fn commit_reference_constructor_rejects_relocated_exact_object() {
        let fixture = fixture();
        let bytes = fixture.commit.to_bytes();
        let relocated = exact(
            format!(
                "store-v1/candidates/{}/packages/relocated.json",
                fixture.commit.candidate_family().as_hash()
            ),
            &bytes,
        );

        assert!(matches!(
            StoreBatchCommitRef::from_commit(
                &fixture.commit,
                fixture.commit_ref.coord.clone(),
                relocated,
            ),
            Err(StoreProtocolError::RelocatedSlot { .. })
        ));
    }

    #[test]
    fn self_retirement_is_exact_commit_state_and_dominates_active_merge_state() {
        let fixture = fixture();
        let write_id = WriteId::from_generated("retire-founder".to_string());
        let order = fixture.commit.order.clone();
        let candidate_family = CandidateFamilyId::derive(
            fixture.root_ref.store_root_hash,
            &fixture.registration_ref,
            &write_id,
            &order,
        );
        let retiring_cut = match &order {
            StoreCommitOrder::Serial { predecessor, .. } => {
                StoreHistoryCut::Serial(predecessor.clone())
            }
            StoreCommitOrder::MergeConcurrent { .. } => unreachable!(),
        };
        let device_signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        let retirement = StoreDeviceSelfRetirement::signed(
            fixture.root_ref.store_root_hash,
            candidate_family,
            fixture.registration_ref.clone(),
            retiring_cut,
            &device_signer,
        )
        .unwrap();
        let retirement_bytes = retirement.to_bytes();
        let retirement_ref = StoreDeviceSelfRetirementRef::from_retirement(
            &retirement,
            exact(
                format!(
                    "{}.json",
                    device_self_retirement_semantic_prefix(
                        candidate_family,
                        &fixture.registration_ref.device_id,
                        retirement.retirement_hash(),
                    )
                ),
                &retirement_bytes,
            ),
        );
        assert_eq!(
            StoreDeviceSelfRetirement::parse_at(
                &retirement_bytes,
                &retirement_ref,
                &fixture.registration,
            )
            .unwrap(),
            retirement
        );
        let commit = StoreBatchCommit::signed_with_self_retirement(
            fixture.root_ref.store_root_hash,
            write_id,
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref.clone(),
            &fixture.registration,
            order,
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            None,
            retirement_ref.clone(),
            &device_signer,
        )
        .unwrap();
        assert_eq!(
            commit.device_retirements(),
            std::slice::from_ref(&retirement_ref)
        );
        let commit_bytes = commit.to_bytes();
        let commit_ref = StoreBatchCommitRef::from_commit(
            &commit,
            fixture.commit_ref.coord.clone(),
            exact(
                format!(
                    "{}.json",
                    commit_semantic_prefix(
                        commit.candidate_family(),
                        SERIAL_STREAM_ID,
                        commit.seq(),
                        commit.commit_hash(),
                    )
                ),
                &commit_bytes,
            ),
        )
        .unwrap();
        let mut remotes = super::super::remote_object::CandidateObjectGraph::from_commit(&commit)
            .unwrap()
            .close(
                &commit,
                &commit_ref,
                vec![super::super::remote_object::CandidateObjectMaterial {
                    object: retirement_ref.object.clone(),
                    canonical_semantic_bytes: retirement_bytes.clone(),
                    stored_bytes: retirement_bytes.clone(),
                }],
            )
            .unwrap();
        assert_eq!(remotes.len(), 1);
        remotes[0].mark_uploaded_verified().unwrap();
        let activated = remotes.pop().unwrap().into_activated(&commit_ref).unwrap();
        assert!(matches!(
            activated,
            super::super::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                if matches!(
                    &record.identity.domain,
                    super::super::remote_object::RetainedAuthorityObjectDomain::SelfRetirement { reference }
                        if reference == &retirement_ref
                )
        ));

        let active = ResolvedStoreDeviceState::founder(
            &fixture.root_ref,
            fixture.registration_ref,
            &fixture.root.descriptor.founder_pubkey,
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.root.descriptor.founder_recovery,
        )
        .unwrap();
        let retired = active.clone().self_retire(retirement_ref).unwrap();
        let merged = ResolvedStoreDeviceState::merge([active, retired]).unwrap();
        assert!(matches!(
            merged
                .devices
                .values()
                .next()
                .expect("founder device")
                .status,
            StoreDeviceStatus::Inactive { .. }
        ));
    }

    #[test]
    fn serial_membership_and_rotation_are_authenticated_by_the_global_commit() {
        let fixture = fixture();
        let member = UserKeypair::generate();
        let state = crate::sync::membership::SerialMembershipState::from_founder(
            fixture.root_ref.store_root_hash,
            &founder_entry(
                &fixture.root_ref.store_root_id.to_string(),
                &fixture.signer,
                fixture.root.descriptor.founder_grant.clone(),
                "0000000001000-0000-device-a",
                GrantStreamAnchor::StoreMembership {
                    first_slot: slot("store-v1/membership/founder/1.json".to_string()),
                },
                fixture.root.descriptor.founder_provider_admin.clone(),
            ),
        )
        .unwrap();
        let add = state
            .signed_set_member(
                &fixture.signer,
                keys::public_key_hex(&member),
                None,
                crate::sync::membership::MemberRole::Member,
                "add".to_string(),
            )
            .unwrap();
        let state = state.apply(&add).unwrap();
        let entry = state
            .signed_remove_member(
                &fixture.signer,
                keys::public_key_hex(&member),
                "remove".to_string(),
            )
            .unwrap();
        let wrapped_keys = vec![crate::sync::membership::test_wrapped_key_ref(
            &keys::public_key_hex(&fixture.signer),
            &keys::public_key_hex(&fixture.signer),
            2,
            b"Store commit Serial rotation wrap",
        )];
        let device_signer = fixture.registration.device_signer(&fixture.signer).unwrap();
        let commit = StoreBatchCommit::signed_with_control(
            fixture.root_ref.store_root_hash,
            WriteId::from_generated("serial-control-write".to_string()),
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref,
            &fixture.registration,
            fixture.commit.order,
            fixture.commit.membership_state,
            fixture.commit.device_state,
            StoreOperationMembershipAuthority::Serial,
            Some(StoreControl::SerialMembershipAndKeyRotation {
                entry: entry.clone(),
                generation: 2,
                wrapped_keys,
            }),
            None,
            &device_signer,
        )
        .unwrap();
        let parsed = StoreBatchCommit::parse_at(
            &commit.to_bytes(),
            fixture.root_ref.store_root_hash,
            &fixture.commit_ref.coord,
            &fixture.registration,
        )
        .unwrap();
        assert_eq!(parsed, commit);
        assert!(state
            .apply(
                parsed
                    .control()
                    .unwrap()
                    .serial_membership_entry()
                    .expect("membership control")
            )
            .is_ok());
    }

    fn merge_cut_reference(
        stream_byte: u8,
        sequence: u64,
        identity_byte: u8,
    ) -> (AuthorStreamId, StoreBatchCommitRef) {
        let stream = AuthorStreamId::from_bytes([stream_byte; 32]);
        (
            stream,
            StoreBatchCommitRef {
                coord: StoreCommitCoord::MergeConcurrent {
                    stream_id: stream,
                    sequence,
                },
                commit_hash: ObjectHash::digest(&[identity_byte]),
                object: exact(
                    format!("test/terminal-cut/{stream_byte}/{sequence}/{identity_byte}.json"),
                    &[identity_byte],
                ),
            },
        )
    }

    fn terminal_ref(fixture: &Fixture, identity_byte: u8) -> StoreDeviceTerminalRef {
        StoreDeviceTerminalRef::SelfRetirement(StoreDeviceSelfRetirementRef {
            candidate_family: fixture.commit.candidate_family(),
            target: fixture.registration_ref.clone(),
            retiring_cut: StoreHistoryCut::MergeConcurrent(BTreeMap::new()),
            retirement_hash: ObjectHash::digest(&[identity_byte]),
            object: exact(
                format!("test/terminal/{identity_byte}.json"),
                &[identity_byte],
            ),
        })
    }

    fn inactive_status(
        terminals: Vec<StoreDeviceTerminalRef>,
        cut: impl IntoIterator<Item = (AuthorStreamId, StoreBatchCommitRef)>,
    ) -> StoreDeviceStatus {
        StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut: StoreHistoryCut::MergeConcurrent(cut.into_iter().collect()),
        }
    }

    #[test]
    fn concurrent_terminal_states_union_terminals_and_intersect_cuts_in_both_orders() {
        let fixture = fixture();
        let (stream_a, a3) = merge_cut_reference(1, 3, 31);
        let (_, a5) = merge_cut_reference(1, 5, 51);
        let (stream_b, b4) = merge_cut_reference(2, 4, 42);
        let left_terminal = terminal_ref(&fixture, 1);
        let right_terminal = terminal_ref(&fixture, 2);
        let left = inactive_status(
            vec![left_terminal.clone()],
            [(stream_a, a5), (stream_b, b4.clone())],
        );
        let right = inactive_status(vec![right_terminal.clone()], [(stream_a, a3.clone())]);
        let expected = inactive_status(vec![left_terminal, right_terminal], [(stream_a, a3)]);

        assert_eq!(
            merge_device_status(left.clone(), right.clone()).unwrap(),
            expected
        );
        assert_eq!(merge_device_status(right, left).unwrap(), expected);
    }

    #[test]
    fn concurrent_terminal_cut_rejects_different_refs_at_the_same_coordinate() {
        let fixture = fixture();
        let (stream, left) = merge_cut_reference(1, 3, 31);
        let (_, right) = merge_cut_reference(1, 3, 32);
        let terminal = terminal_ref(&fixture, 1);

        assert_eq!(
            merge_device_status(
                inactive_status(vec![terminal.clone()], [(stream, left)]),
                inactive_status(vec![terminal], [(stream, right)]),
            ),
            Err(StoreProtocolError::DeviceStateMismatch)
        );
    }

    #[test]
    fn concurrent_terminal_cut_intersection_is_associative_and_idempotent() {
        let fixture = fixture();
        let terminal = terminal_ref(&fixture, 1);
        let (stream_a, a2) = merge_cut_reference(1, 2, 21);
        let (_, a3) = merge_cut_reference(1, 3, 31);
        let (_, a4) = merge_cut_reference(1, 4, 41);
        let (stream_b, b1) = merge_cut_reference(2, 1, 12);
        let (_, b2) = merge_cut_reference(2, 2, 22);
        let left = inactive_status(
            vec![terminal.clone()],
            [(stream_a, a4), (stream_b, b2.clone())],
        );
        let middle = inactive_status(
            vec![terminal.clone()],
            [(stream_a, a3), (stream_b, b1.clone())],
        );
        let right = inactive_status(vec![terminal], [(stream_a, a2)]);

        assert_eq!(
            merge_device_status(
                merge_device_status(left.clone(), middle.clone()).unwrap(),
                right.clone(),
            )
            .unwrap(),
            merge_device_status(left.clone(), merge_device_status(middle, right).unwrap()).unwrap()
        );
        assert_eq!(
            merge_device_status(left.clone(), left.clone()).unwrap(),
            left
        );
    }

    #[test]
    fn acknowledgement_cut_join_remains_componentwise_maximum() {
        let (stream_a, a2) = merge_cut_reference(1, 2, 21);
        let (_, a4) = merge_cut_reference(1, 4, 41);
        let (stream_b, b1) = merge_cut_reference(2, 1, 12);
        let joined = StoreHistoryCut::MergeConcurrent(BTreeMap::from([(stream_a, a2)]))
            .join(StoreHistoryCut::MergeConcurrent(BTreeMap::from([
                (stream_a, a4.clone()),
                (stream_b, b1.clone()),
            ])))
            .unwrap();

        assert_eq!(
            joined,
            StoreHistoryCut::MergeConcurrent(BTreeMap::from([(stream_a, a4), (stream_b, b1),]))
        );
    }

    #[test]
    fn device_exclusion_objects_drive_the_exact_pending_and_terminal_states() {
        let fixture = fixture();
        let resolved = ResolvedStoreDeviceState::founder(
            &fixture.root_ref,
            fixture.registration_ref.clone(),
            &fixture.root.descriptor.founder_pubkey,
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.root.descriptor.founder_recovery,
        )
        .expect("founder device state");
        let predecessor = fixture.commit.device_state.clone();
        let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
            b"device exclusion proposal",
        ));
        let outcome_key = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(
                fixture.registration_ref.device_id,
                proposal_id,
            )
        );
        let device_signer = fixture
            .registration
            .device_signer(&fixture.signer)
            .expect("founder device signer");
        let proposal = StoreDeviceExclusionProposal::signed(
            fixture.root_ref.store_root_hash,
            proposal_id,
            fixture.registration_ref.clone(),
            &fixture.registration,
            predecessor.clone(),
            slot(outcome_key.clone()),
            fixture.registration_ref.clone(),
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.registration,
            &device_signer,
        )
        .expect("sign exclusion proposal");
        let proposal_bytes = proposal.to_bytes();
        let proposal_ref = StoreDeviceExclusionProposalRef::from_proposal(
            &proposal,
            exact(
                format!(
                    "{}.json",
                    device_exclusion_proposal_semantic_prefix(
                        fixture.registration_ref.device_id,
                        proposal_id,
                        proposal.proposal_hash(),
                    )
                ),
                &proposal_bytes,
            ),
        )
        .expect("exact exclusion proposal ref");
        let parsed = StoreDeviceExclusionProposal::parse_at(
            &proposal_bytes,
            &proposal_ref,
            &fixture.registration,
            &fixture.registration,
        )
        .expect("parse exclusion proposal");
        assert_eq!(parsed, proposal);

        let pending = resolved
            .propose_exclusion(proposal_ref.clone(), &proposal, &predecessor)
            .expect("activate exclusion proposal");
        assert!(device_state_has_exact_pending_proposal(
            &pending,
            &proposal_ref
        ));

        let cancellation = StoreDeviceExclusionCancellation::signed(
            proposal_ref.clone(),
            &proposal,
            fixture.registration_ref.clone(),
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.registration,
            &device_signer,
        )
        .expect("sign exclusion cancellation");
        let cancellation_value = StoreDeviceExclusionOutcome::Cancelled(cancellation);
        let cancellation_bytes = cancellation_value.to_bytes();
        let cancellation_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
            &cancellation_value,
            &proposal,
            exact(outcome_key.clone(), &cancellation_bytes),
        )
        .expect("exact exclusion cancellation ref");
        let parsed = StoreDeviceExclusionOutcome::parse_at(
            &cancellation_bytes,
            &cancellation_ref,
            &proposal,
            &fixture.registration,
            &fixture.registration,
        )
        .expect("parse exclusion cancellation");
        assert_eq!(parsed, cancellation_value);
        let StoreDeviceExclusionOutcomeRef::Cancelled(cancellation_ref) = cancellation_ref else {
            panic!("cancellation ref changed variant")
        };
        let cancelled = pending
            .cancel_exclusion(cancellation_ref.clone())
            .expect("activate exclusion cancellation");
        assert!(matches!(
            cancelled
                .devices
                .get(&fixture.registration_ref.device_id)
                .and_then(|record| record.proposals.get(&proposal_id)),
            Some(StoreDeviceProposalState::Cancelled { outcome }) if outcome == &cancellation_ref
        ));

        let exclusion = StoreDeviceExclusion::signed(
            proposal_ref.clone(),
            &proposal,
            fixture.registration_ref.clone(),
            &fixture.registration,
            StoreDeviceExclusionProof::Serial,
            fixture.registration_ref.clone(),
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.registration,
            &device_signer,
        )
        .expect("sign device exclusion");
        let exclusion_value = StoreDeviceExclusionOutcome::Excluded(exclusion);
        let exclusion_bytes = exclusion_value.to_bytes();
        let exclusion_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
            &exclusion_value,
            &proposal,
            exact(outcome_key, &exclusion_bytes),
        )
        .expect("exact exclusion ref");
        let StoreDeviceExclusionOutcomeRef::Excluded(exclusion_ref) = exclusion_ref else {
            panic!("exclusion ref changed variant")
        };
        let accepted_cut = fixture
            .commit
            .order
            .predecessor_cut()
            .expect("Serial predecessor cut");
        let excluded = pending
            .exclude(exclusion_ref.clone(), accepted_cut.clone())
            .expect("activate device exclusion");
        assert!(matches!(
            &excluded
                .devices
                .get(&fixture.registration_ref.device_id)
                .expect("excluded record")
                .status,
            StoreDeviceStatus::Inactive { terminals, accepted_cut: cut }
                if terminals == &vec![StoreDeviceTerminalRef::Excluded(exclusion_ref)]
                    && cut == &accepted_cut
        ));
    }

    #[test]
    fn retained_registration_activations_reopen_exact_canonical_inputs() {
        let fixture = fixture();
        let replacement = UserKeypair::generate();
        let recovery_id = DeviceRecoveryId::from_hash(ObjectHash::digest(b"retained recovery"));
        let recovery_slot = slot("store-v1/recovery/retained/1.json".to_string());
        let replacement_registration = StoreDeviceRegistration::signed(
            fixture.root_ref.clone(),
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id,
                recovery_slot: recovery_slot.clone(),
                owner_grant: fixture.root.descriptor.founder_grant.clone(),
            },
            fixture.registration.provider.clone(),
            StoreCommitAnchor::Serial,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot("store-v1/acks/retained/1.json".to_string()),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot("store-v1/snapshots/retained/1.json".to_string()),
            },
            &replacement,
        )
        .expect("sign replacement registration");
        let replacement_bytes = replacement_registration.to_bytes();
        let replacement_ref = StoreDeviceRegistrationRef::from_registration(
            &replacement_registration,
            exact(
                format!(
                    "{}.json",
                    registration_semantic_prefix(&replacement_registration.device_id.to_string())
                ),
                &replacement_bytes,
            ),
        );
        let recovery_node = OwnerRecoveryNodeRef {
            owner_pubkey: fixture.registration.author_pubkey.clone(),
            owner_grant: fixture.root.descriptor.founder_grant.clone(),
            sequence: 1,
            node_hash: ObjectHash::digest(b"retained recovery node"),
            object: exact(
                recovery_slot.logical_key().to_string(),
                b"retained recovery node",
            ),
        };
        let activated = ActivatedStoreDeviceRegistrationRef {
            registration: replacement_ref,
            authority: StoreDeviceRegistrationActivationRef::Recovery {
                recovery_id,
                node: recovery_node.clone(),
            },
        };
        let authority = StoreDeviceRegistrationActivation::Recovery {
            recovery_id,
            node: recovery_node,
        };
        let device_signer = fixture
            .registration
            .device_signer(&fixture.signer)
            .expect("founder device signer");
        let commit = StoreBatchCommit::signed_with_registrations(
            fixture.root_ref.store_root_hash,
            WriteId::from_generated("retained-registration".to_string()),
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref.clone(),
            &fixture.registration,
            fixture.commit.order.clone(),
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            StoreOperationMembershipAuthority::Serial,
            vec![activated],
            &device_signer,
        )
        .expect("sign registration activation commit");
        let input = vec![(replacement_registration.clone(), authority.clone())];
        let retained = RetainedStoreDeviceRegistrationActivations::from_verified(
            &fixture.root_ref,
            &commit,
            &input,
        )
        .expect("retain registration activation");
        let encoded = serde_json::to_vec(&retained).expect("encode retained registration");
        let decoded: RetainedStoreDeviceRegistrationActivations =
            serde_json::from_slice(&encoded).expect("decode retained registration");
        assert_eq!(
            decoded
                .verify_for(&fixture.root_ref, &commit)
                .expect("verify retained registration"),
            input
        );

        let mut tampered = retained.clone();
        tampered.registrations[0].canonical_registration.push(b' ');
        assert!(tampered.verify_for(&fixture.root_ref, &commit).is_err());

        let missing = RetainedStoreDeviceRegistrationActivations {
            registrations: Vec::new(),
        };
        assert!(missing.verify_for(&fixture.root_ref, &commit).is_err());

        let mut substituted = retained;
        substituted.registrations[0].canonical_registration = fixture.registration.to_bytes();
        assert!(substituted.verify_for(&fixture.root_ref, &commit).is_err());
    }

    #[test]
    fn retained_device_operations_reopen_sources_and_derive_the_accepted_cut() {
        let fixture = fixture();
        let proposal_id = StoreDeviceExclusionProposalId::from_hash(ObjectHash::digest(
            b"retained exclusion proposal",
        ));
        let outcome_key = format!(
            "{}.json",
            device_exclusion_outcome_semantic_prefix(
                fixture.registration_ref.device_id,
                proposal_id,
            )
        );
        let device_signer = fixture
            .registration
            .device_signer(&fixture.signer)
            .expect("founder device signer");
        let proposal = StoreDeviceExclusionProposal::signed(
            fixture.root_ref.store_root_hash,
            proposal_id,
            fixture.registration_ref.clone(),
            &fixture.registration,
            fixture.commit.device_state.clone(),
            slot(outcome_key.clone()),
            fixture.registration_ref.clone(),
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.registration,
            &device_signer,
        )
        .expect("sign retained exclusion proposal");
        let proposal_bytes = proposal.to_bytes();
        let proposal_ref = StoreDeviceExclusionProposalRef::from_proposal(
            &proposal,
            exact(
                format!(
                    "{}.json",
                    device_exclusion_proposal_semantic_prefix(
                        fixture.registration_ref.device_id,
                        proposal_id,
                        proposal.proposal_hash(),
                    )
                ),
                &proposal_bytes,
            ),
        )
        .expect("exact retained exclusion proposal");
        let proposal_source = RetainedStoreDeviceExclusionProposal {
            reference: proposal_ref.clone(),
            canonical_proposal: proposal_bytes,
            canonical_target_registration: fixture.registration.to_bytes(),
            canonical_owner_registration: fixture.registration.to_bytes(),
        };
        let proposal_commit = StoreBatchCommit::signed_with_device_exclusions(
            fixture.root_ref.store_root_hash,
            WriteId::from_generated("retained-proposal".to_string()),
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref.clone(),
            &fixture.registration,
            fixture.commit.order.clone(),
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            StoreOperationMembershipAuthority::Serial,
            vec![proposal_ref.clone()],
            Vec::new(),
            &device_signer,
        )
        .expect("sign retained proposal commit");
        let retained_proposal =
            RetainedStoreDeviceOperations::from_sources(vec![proposal_source.clone()], Vec::new());
        let verified_proposal = retained_proposal
            .verify_for(&fixture.root_ref, &proposal_commit)
            .expect("verify retained proposal input");
        assert_eq!(
            verified_proposal
                .proposals()
                .next()
                .map(|(reference, value)| (reference.clone(), value.clone())),
            Some((proposal_ref.clone(), proposal.clone()))
        );
        let mut tampered_proposal = retained_proposal;
        tampered_proposal.proposals[0].canonical_proposal.push(b' ');
        assert!(tampered_proposal
            .verify_for(&fixture.root_ref, &proposal_commit)
            .is_err());
        let exclusion = StoreDeviceExclusion::signed(
            proposal_ref,
            &proposal,
            fixture.registration_ref.clone(),
            &fixture.registration,
            StoreDeviceExclusionProof::Serial,
            fixture.registration_ref.clone(),
            fixture.root.descriptor.founder_grant.clone(),
            &fixture.registration,
            &device_signer,
        )
        .expect("sign retained exclusion outcome");
        let outcome = StoreDeviceExclusionOutcome::Excluded(exclusion);
        let outcome_bytes = outcome.to_bytes();
        let outcome_ref = StoreDeviceExclusionOutcomeRef::from_outcome(
            &outcome,
            &proposal,
            exact(outcome_key, &outcome_bytes),
        )
        .expect("exact retained exclusion outcome");
        let StoreDeviceExclusionOutcomeRef::Excluded(exclusion_ref) = outcome_ref.clone() else {
            panic!("retained exclusion reference changed variant")
        };
        let outcome_source = RetainedStoreDeviceExclusionOutcome::Excluded {
            reference: exclusion_ref,
            canonical_outcome: outcome_bytes,
            proposal: proposal_source,
            canonical_owner_registration: fixture.registration.to_bytes(),
        };
        let commit = StoreBatchCommit::signed_with_device_exclusions(
            fixture.root_ref.store_root_hash,
            WriteId::from_generated("retained-exclusion".to_string()),
            fixture.commit_ref.coord.clone(),
            fixture.registration_ref.clone(),
            &fixture.registration,
            fixture.commit.order.clone(),
            fixture.commit.membership_state.clone(),
            fixture.commit.device_state.clone(),
            StoreOperationMembershipAuthority::Serial,
            Vec::new(),
            vec![outcome_ref],
            &device_signer,
        )
        .expect("sign retained exclusion commit");
        let retained =
            RetainedStoreDeviceOperations::from_sources(Vec::new(), vec![outcome_source]);
        let encoded = serde_json::to_vec(&retained).expect("encode retained device operations");
        let decoded: RetainedStoreDeviceOperations =
            serde_json::from_slice(&encoded).expect("decode retained device operations");
        let verified = decoded
            .verify_for(&fixture.root_ref, &commit)
            .expect("verify retained device operations");
        assert_eq!(verified.to_retained(), retained);
        assert_eq!(
            verified.exclusions().next().map(|(_, cut)| cut.clone()),
            Some(
                commit
                    .order
                    .predecessor_cut()
                    .expect("derive accepted predecessor cut")
            )
        );

        let mut tampered = retained.clone();
        let RetainedStoreDeviceExclusionOutcome::Excluded {
            canonical_outcome, ..
        } = &mut tampered.outcomes[0]
        else {
            unreachable!()
        };
        canonical_outcome.push(b' ');
        assert!(tampered.verify_for(&fixture.root_ref, &commit).is_err());

        let missing = RetainedStoreDeviceOperations::from_sources(Vec::new(), Vec::new());
        assert!(missing.verify_for(&fixture.root_ref, &commit).is_err());

        let mut substituted = retained;
        let RetainedStoreDeviceExclusionOutcome::Excluded {
            canonical_owner_registration,
            ..
        } = &mut substituted.outcomes[0]
        else {
            unreachable!()
        };
        let mut other_registration = fixture.registration.clone();
        other_registration.author_pubkey.push('0');
        *canonical_owner_registration = other_registration.to_bytes();
        assert!(substituted.verify_for(&fixture.root_ref, &commit).is_err());
    }

    fn device_state_has_exact_pending_proposal(
        state: &ResolvedStoreDeviceState,
        expected: &StoreDeviceExclusionProposalRef,
    ) -> bool {
        state
            .devices
            .get(&expected.target.device_id)
            .and_then(|record| record.proposals.get(&expected.proposal_id))
            .is_some_and(|state| {
                matches!(state, StoreDeviceProposalState::Pending { proposal } if proposal == expected)
            })
    }
}
