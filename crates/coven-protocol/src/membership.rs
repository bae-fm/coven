//! Store-bound causal membership protocol.
//!
//! Every causal author stream is identified by its author, the Owner grant that
//! authorizes it, and an independently generated stream id. Entries carry the
//! complete observed stream frontier; authorization is derived from that causal
//! past, never from `created_at`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::causal_grants::{
    self, CausalAssignment, CausalChange, CausalCoordinate, CausalEntry, CausalGrantConflict,
    CausalGrantError, CausalGrantStatus, GrantRetirements, GrantState, OwnerGrantBarrier,
};
pub use super::causal_grants::{AuthorStreamId, MembershipGrantId};
use super::store_commit::{
    GrantStreamAnchor, ObjectHash, OwnerConflictResolutionAcceptance, OwnerPromotionAcceptance,
    OwnerPromotionFinalization, OwnerRecoveryCursor, Signed, SignedBody, StoreBatchCommitRef,
    StoreCreationId, StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    StoreRootRef, SuccessorLink,
};
#[cfg(any(test, feature = "test-utils"))]
use super::store_commit::{
    OwnerPromotionAcceptanceBody, OwnerPromotionAnchors, OwnerPromotionId, OwnerPromotionRequest,
    OwnerPromotionRequestActivation, OwnerPromotionRequestBody, OwnerRecoveryNodeRef,
    OwnerRecoveryPosition,
};
// Only this module's own tests build a conflict-resolution acceptance body; the
// `test-utils` fixtures above do not.
#[cfg(test)]
use super::store_commit::OwnerConflictResolutionAcceptanceBody;
use super::wrapped_store_key::WrappedStoreKeyRef;
use crate::objects::ExactObjectRef;
use coven_keys::keys::{self, UserKeypair};

mod authoring;
mod authority;
mod chain;
mod conflict;
mod entry;
mod reduction;

pub use conflict::{
    derive_store_resolution_grant, resolve_store_membership_conflict, MembershipConflict,
    MembershipConflictSelection, MembershipStatus, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionBody, StoreMembershipConflictResolutionRef,
};
pub use conflict::{MembershipConflictChoice, MembershipConflictInfo};
pub use entry::{
    derive_founder_stream_id, derive_grant_id, founder_entry_for_creation, verify_membership_entry,
};
#[cfg(any(test, feature = "test-utils"))]
pub use entry::{founder_entry, test_wrapped_key_ref};
use reduction::*;

const MEMBERSHIP_ENTRY_DOMAIN: &[u8] = b"coven.store-membership-entry.v1\0";
const MEMBERSHIP_HEAD_DOMAIN: &[u8] = b"coven.store-membership-head.v1\0";
const MEMBERSHIP_RESOLUTION_DOMAIN: &[u8] = b"coven.store-membership-conflict-resolution.v1\0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemberRole {
    Owner,
    Member,
    Follower,
}

impl MemberRole {
    pub fn can_write(&self) -> bool {
        matches!(self, Self::Owner | Self::Member)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerRecoveryAnchorRef {
    Founder {
        creation_id: StoreCreationId,
    },
    Promotion {
        acceptance: Box<OwnerPromotionAcceptance>,
    },
    ConflictResolution {
        acceptance: Box<OwnerConflictResolutionAcceptance>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreMembershipRoleGrant {
    Owner { recovery: OwnerRecoveryAnchorRef },
    Member,
    Follower,
}

impl StoreMembershipRoleGrant {
    pub fn role(&self) -> MemberRole {
        match self {
            Self::Owner { .. } => MemberRole::Owner,
            Self::Member => MemberRole::Member,
            Self::Follower => MemberRole::Follower,
        }
    }

    fn from_direct_assignment(role: MemberRole) -> Result<Self, MembershipError> {
        match role {
            MemberRole::Owner => Err(MembershipError::OwnerPromotionRequired),
            MemberRole::Member => Ok(Self::Member),
            MemberRole::Follower => Ok(Self::Follower),
        }
    }

    fn can_write(&self) -> bool {
        matches!(self, Self::Owner { .. } | Self::Member)
    }

    fn is_owner(&self) -> bool {
        matches!(self, Self::Owner { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberInfo {
    pub pubkey: String,
    pub role: MemberRole,
    pub is_self: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum MembershipChange {
    Founder {
        creation_id: StoreCreationId,
        owner_pubkey: String,
        owner_grant_id: MembershipGrantId,
        membership: GrantStreamAnchor,
        provider_admin: super::provider::FounderProviderAdminGrant,
    },
    SetMember {
        user_pubkey: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_account_email: Option<String>,
        role: StoreMembershipRoleGrant,
        grant_id: MembershipGrantId,
        membership: Option<GrantStreamAnchor>,
        replaces: BTreeSet<MembershipGrantId>,
        retirement_barriers: BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>,
        #[serde(skip_serializing_if = "Option::is_none")]
        retirement_device_state: Option<StoreDeviceStateRef>,
        wrapped_key: WrappedStoreKeyRef,
    },
    RemoveMember {
        user_pubkey: String,
        removes: BTreeSet<MembershipGrantId>,
        retirement_barriers: BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>,
        #[serde(skip_serializing_if = "Option::is_none")]
        retirement_device_state: Option<StoreDeviceStateRef>,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
    },
    ProviderAdmin,
    ResolutionActivation {
        resolution: StoreMembershipConflictResolutionRef,
    },
}

impl MembershipChange {
    pub fn membership_anchor(&self) -> Option<GrantStreamAnchor> {
        match self {
            Self::Founder { membership, .. } => Some(membership.clone()),
            Self::SetMember { membership, .. } => membership.clone(),
            Self::RemoveMember { .. } | Self::ProviderAdmin | Self::ResolutionActivation { .. } => {
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipCoord {
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
    pub seq: u64,
    pub entry_hash: ObjectHash,
}

impl MembershipCoord {
    pub fn stream_key(&self) -> MembershipStreamKey {
        MembershipStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            stream_id: self.stream_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MembershipStreamKey {
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
}

impl CausalCoordinate for MembershipCoord {
    type StreamKey = MembershipStreamKey;

    fn stream_key(&self) -> Self::StreamKey {
        MembershipCoord::stream_key(self)
    }

    fn author_pubkey(&self) -> &str {
        &self.author_pubkey
    }

    fn author_owner_grant(&self) -> &MembershipGrantId {
        &self.author_owner_grant
    }

    fn seq(&self) -> u64 {
        self.seq
    }

    fn entry_hash(&self) -> ObjectHash {
        self.entry_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreAssignment {
    role: StoreMembershipRoleGrant,
    provider_account_email: Option<String>,
}

impl causal_grants::CausalHistoryEntry for MembershipEntry {
    type Coord = MembershipCoord;

    fn coord(&self) -> Self::Coord {
        self.coord()
    }

    fn dependencies(&self) -> &[Self::Coord] {
        &self.dependencies
    }
}

impl CausalAssignment for StoreAssignment {
    fn is_owner(&self) -> bool {
        self.role.is_owner()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct OwnerStreamBarrier {
    pub observed_streams: Vec<MembershipCoord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct StoreGrantStreamBarrier {
    pub observed_streams: Vec<MembershipCoord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct MergeStoreOwnerGrantBarrier {
    pub author_streams: StoreGrantStreamBarrier,
    pub recovery: OwnerRecoveryCursor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MergeMembershipGrantRetirementBarrier {
    NonOwner {
        author_streams: StoreGrantStreamBarrier,
    },
    Owner {
        barrier: MergeStoreOwnerGrantBarrier,
    },
}

impl MergeMembershipGrantRetirementBarrier {
    fn author_streams(&self) -> &StoreGrantStreamBarrier {
        match self {
            Self::NonOwner { author_streams } => author_streams,
            Self::Owner { barrier } => &barrier.author_streams,
        }
    }

    fn owner_stream_barrier(&self) -> Option<OwnerGrantBarrier<MembershipCoord>> {
        match self {
            Self::NonOwner { .. } => None,
            Self::Owner { barrier } => Some(shared_store_barrier(&barrier.author_streams)),
        }
    }
}

/// The wire body of one membership entry. Every field here is signed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MembershipEntryBody {
    pub store_id: String,
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
    pub seq: u64,
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<MembershipCoord>,
    pub resolution_dependencies: Vec<StoreMembershipConflictResolutionRef>,
    pub created_at: String,
    pub change: MembershipChange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_admin: Option<super::provider::ProviderAdminMembershipChange>,
}

impl SignedBody for MembershipEntryBody {
    const DOMAIN: &'static [u8] = MEMBERSHIP_ENTRY_DOMAIN;
}

pub type MembershipEntry = Signed<MembershipEntryBody>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipEntryRef {
    pub coord: MembershipCoord,
    pub object: ExactObjectRef,
}

impl MembershipEntry {
    pub fn coord(&self) -> MembershipCoord {
        MembershipCoord {
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            stream_id: self.stream_id,
            seq: self.seq,
            entry_hash: self.hash(),
        }
    }
}

/// The wire body of one membership author head. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorHeadBody {
    pub store_id: String,
    pub body: MembershipHeadBody,
    pub activation: MembershipHeadActivation,
}

impl SignedBody for AuthorHeadBody {
    const DOMAIN: &'static [u8] = MEMBERSHIP_HEAD_DOMAIN;
}

pub type AuthorHead = Signed<AuthorHeadBody>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipHeadBody {
    pub author_registration: StoreDeviceRegistrationRef,
    pub entry: MembershipEntryRef,
    pub predecessor: Option<MembershipHeadRef>,
    pub resolutions: Vec<StoreMembershipConflictResolutionRef>,
    pub successor: SuccessorLink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipHeadActivation {
    Direct,
    StoreCommit { commit: StoreBatchCommitRef },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeMembershipHeadTransition {
    pub body: MembershipHeadBody,
    pub head_slot: crate::objects::ObjectSlot,
}

impl MergeMembershipHeadTransition {
    pub fn matches_head(&self, head: &AuthorHead, reference: &MembershipHeadRef) -> bool {
        self.body == head.body
            && self.head_slot == *reference.object.slot()
            && self.body.entry.coord == reference.coord
            && head.entry_coord() == reference.coord
            && head.head_hash() == reference.head_hash
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipHeadRef {
    pub coord: MembershipCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MembershipFloorError {
    #[error("membership floor is empty")]
    Empty,
    #[error("membership floor contains sequence zero")]
    SequenceZero,
    #[error("membership floor is not strictly ordered by author stream")]
    NotStrictlyOrdered,
}

pub fn validate_membership_floor(floor: &[MembershipHeadRef]) -> Result<(), MembershipFloorError> {
    if floor.is_empty() {
        return Err(MembershipFloorError::Empty);
    }
    for (index, reference) in floor.iter().enumerate() {
        if reference.coord.seq == 0 {
            return Err(MembershipFloorError::SequenceZero);
        }
        if index > 0 && floor[index - 1].coord.stream_key() >= reference.coord.stream_key() {
            return Err(MembershipFloorError::NotStrictlyOrdered);
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MembershipError {
    #[error("membership chain is empty")]
    EmptyChain,
    #[error("membership entry {0} has unsupported version")]
    UnsupportedVersion(usize),
    #[error("membership entry {index} belongs to store {actual:?}, expected {expected:?}")]
    StoreMismatch {
        index: usize,
        expected: String,
        actual: String,
    },
    #[error("membership entry {0} has an invalid signature")]
    InvalidSignature(usize),
    #[error("membership entry {index} is in coordinate {actual:?}, expected {expected:?}")]
    CoordinateMismatch {
        index: usize,
        expected: Box<MembershipCoord>,
        actual: Box<MembershipCoord>,
    },
    #[error("membership stream {author}/{grant} is missing sequence {seq}")]
    MissingSequence {
        author: String,
        grant: MembershipGrantId,
        seq: u64,
    },
    #[error("membership stream {author}/{grant} has conflicting entries at sequence {seq}")]
    ConflictingSequence {
        author: String,
        grant: MembershipGrantId,
        seq: u64,
    },
    #[error("membership entry at {coord:?} differs from the expected exact entry")]
    ExactEntryMismatch { coord: Box<MembershipCoord> },
    #[error("membership entry {index} has predecessor {actual:?}, expected {expected:?}")]
    BrokenStreamLink {
        index: usize,
        expected: Option<ObjectHash>,
        actual: Option<ObjectHash>,
    },
    #[error("membership entry {index} does not carry its complete own-stream dependency")]
    MissingOwnDependency { index: usize },
    #[error("membership entry {index} depends on missing coordinate {dependency:?}")]
    MissingDependency {
        index: usize,
        dependency: Box<MembershipCoord>,
    },
    #[error(
        "membership entry {index} dependency frontier is not strictly ordered by author stream"
    )]
    NonCanonicalDependencyFrontier { index: usize },
    #[error("membership dependency graph contains a cycle")]
    DependencyCycle,
    #[error("membership founder entry is invalid")]
    InvalidFounder,
    #[error("membership entry {index} author is not active under Owner grant {grant}")]
    AuthorGrantInactive {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("membership entry {index} creates an already-defined grant {grant}")]
    DuplicateGrant {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("membership entry {index} replaces or removes grant {grant} owned by another member")]
    GrantOwnerMismatch {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("membership entry {index} does not name the exact active grants for member {pubkey}")]
    GrantSetMismatch { index: usize, pubkey: String },
    #[error("membership entry {index} removes no exact grants")]
    EmptyRemoval { index: usize },
    #[error("membership entry {index} removes Owner grant {grant} without its exact observed-through coordinate")]
    MissingOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error(
        "membership entry {index} carries an invalid revocation barrier for Owner grant {grant}"
    )]
    InvalidOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("membership change retiring Owner grants lacks the exact Merge device state")]
    MissingOwnerRecoveryState,
    #[error("membership change without an Owner retirement carries a device state")]
    UnexpectedOwnerRecoveryState,
    #[error("membership entry {0} carries an invalid Owner membership stream anchor")]
    InvalidOwnerMembershipAnchor(usize),
    #[error("membership entry {0} carries invalid wrapped Store-key authority")]
    InvalidWrappedKeys(usize),
    #[error("checkpoint lacks the exact record for membership grant {grant}")]
    MissingCheckpointGrant { grant: MembershipGrantId },
    #[error("checkpoint lacks retirement evidence for membership grant {grant}")]
    MissingCheckpointRetirementEvidence { grant: MembershipGrantId },
    #[error("membership grant {grant} retirement at {authority:?} lacks its exact signed barrier")]
    MissingRetirementBarrier {
        grant: MembershipGrantId,
        authority: Box<MembershipCoord>,
    },
    #[error(
        "current member {recipient_pubkey} lacks wrapped Store-key coverage for rotation {rotation:?}"
    )]
    MissingWrappedKeyCoverage {
        recipient_pubkey: String,
        rotation: Box<MembershipCoord>,
    },
    #[error("membership history leaves no active Owner")]
    NoActiveOwner,
    #[error(
        "membership revocation cycle has {sources} sources, exceeding the protocol limit of {maximum}"
    )]
    RevocationCycleTooWide { sources: usize, maximum: usize },
    #[error("signer {0} has no active Owner grant")]
    SignerIsNotOwner(String),
    #[error("member {0} has no active grants")]
    NotAMember(String),
    #[error("non-founder Owner grants require an accepted Owner promotion")]
    OwnerPromotionRequired,
    #[error("Owner promotion does not match the exact current membership state")]
    InvalidOwnerPromotion,
    #[error("membership author stream contains a pruned suffix and cannot be extended")]
    PrunedAuthorStream,
    #[error("membership author stream exhausted its sequence space")]
    SequenceExhausted,
    #[error("membership resolution activation entry {0} is invalid")]
    InvalidResolutionActivation(usize),
    #[error("membership resolution activation requires a fresh persisted author stream")]
    ResolutionActivationRequiresFreshStream,
    #[error("provider administrator control entry {0} is invalid")]
    InvalidProviderAdminChange(usize),
    #[error("membership has an unresolved semantic conflict")]
    Conflict,
    #[error("membership conflict is missing its exact signed raw heads")]
    MissingConflictHeads,
    #[error("membership conflict resolution does not name exact validated conflict evidence")]
    InvalidConflictResolution,
    #[error("provider administrator history is invalid: {0}")]
    ProviderAdmin(#[from] super::provider::ProviderAdminReducerError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipGrantRecord {
    pub member_pubkey: String,
    pub role: StoreMembershipRoleGrant,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_account_email: Option<String>,
    pub creation_authority: MembershipGrantCreationAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipGrantCreationAuthority {
    Entry(MembershipCoord),
    ConflictResolution(StoreMembershipConflictResolutionRef),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipGrantRetirement {
    Entry {
        authority: MembershipCoord,
        barrier: MergeMembershipGrantRetirementBarrier,
    },
    ConflictResolution {
        authority: StoreMembershipConflictResolutionRef,
        barrier: MergeMembershipGrantRetirementBarrier,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedStoreMembership {
    pub grants:
        BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    pub provider_admin: super::provider::ProviderAdminResolution,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipBranch {
    pub heads: Vec<MembershipHeadRef>,
    pub effective_frontier: Vec<MembershipCoord>,
    pub grants:
        BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    pub provider_admin: super::provider::ProviderAdminResolution,
    pub state_hash: ObjectHash,
}

impl ResolvedStoreMembership {
    pub fn active_grants(
        &self,
    ) -> impl Iterator<Item = (&MembershipGrantId, &MembershipGrantRecord)> {
        causal_grants::active_grants(&self.grants)
    }

    pub fn active_grant(&self, grant: &MembershipGrantId) -> Option<&MembershipGrantRecord> {
        self.grants.get(grant).and_then(GrantState::active)
    }
}

impl StoreMembershipBranch {
    pub fn active_grants(
        &self,
    ) -> impl Iterator<Item = (&MembershipGrantId, &MembershipGrantRecord)> {
        causal_grants::active_grants(&self.grants)
    }
}

#[derive(Debug, Clone, Default)]
struct CausalState {
    grants:
        BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
}

#[derive(Debug, Clone)]
pub struct MembershipChain {
    entries: Vec<MembershipEntry>,
    coords: Vec<MembershipCoord>,
    state: CausalState,
    included: BTreeSet<MembershipCoord>,
    status: Option<MembershipStatus>,
    head_refs: Vec<MembershipHeadRef>,
    resolution_checkpoint: Option<MembershipResolutionCheckpoint>,
    provider_admin_genesis: super::provider::ProviderAdminState,
}

#[derive(Debug, Clone)]
struct MembershipResolutionCheckpoint {
    raw_heads: Vec<MembershipCoord>,
    effective_frontier: Vec<MembershipCoord>,
    grants:
        BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    grant_anchors: BTreeMap<MembershipGrantId, GrantStreamAnchor>,
    included: BTreeSet<MembershipCoord>,
    resolutions: Vec<StoreMembershipConflictResolutionRef>,
    provider_admin: super::provider::ProviderAdminState,
}

#[cfg(any(test, feature = "test-utils"))]
fn test_provider_admin_genesis(
    entries: &[MembershipEntry],
) -> Result<super::provider::ProviderAdminState, MembershipError> {
    let founder = entries
        .iter()
        .find_map(|entry| match &entry.change {
            MembershipChange::Founder { provider_admin, .. } => Some((entry, provider_admin)),
            _ => None,
        })
        .ok_or(MembershipError::InvalidFounder)?;
    let root_bytes = founder.0.store_id.as_bytes();
    let root = StoreRootRef {
        store_root_id: ObjectHash::digest(
            format!("{} test root id", founder.0.store_id).as_bytes(),
        ),
        store_root_hash: ObjectHash::digest(root_bytes),
        object: ExactObjectRef::new(
            crate::objects::ObjectSlot::logical(format!(
                "store-v1/test/{}/root.json",
                founder.0.store_id
            ))
            .expect("valid test root slot"),
            root_bytes.len() as u64,
            ObjectHash::digest(root_bytes),
        ),
    };
    let registration: StoreDeviceRegistrationRef =
        serde_json::from_value(serde_json::json!({
            "device_id": ObjectHash::digest(format!("{} founder device", founder.0.store_id).as_bytes()),
            "registration_hash": ObjectHash::digest(format!("{} founder registration", founder.0.store_id).as_bytes()),
            "object": {
                "slot": {"logical_key": format!("store-v1/test/{}/registration.json", founder.0.store_id), "physical": {"kind": "logical_key"}},
                "stored_size": 1,
                "stored_hash": ObjectHash::digest(format!("{} founder registration object", founder.0.store_id).as_bytes()),
            }
        }))
        .expect("valid test founder registration reference");
    Ok(super::provider::ProviderAdminState::founder_from_root(
        root,
        registration,
        founder.1,
    ))
}

impl MembershipChain {}

impl AuthorHead {
    pub fn signed(
        store_id: String,
        mut body: MembershipHeadBody,
        activation: MembershipHeadActivation,
        device_signer: &UserKeypair,
    ) -> Self {
        body.resolutions.sort();
        body.resolutions.dedup();
        Signed::sign(
            AuthorHeadBody {
                store_id,
                body,
                activation,
            },
            device_signer,
        )
    }

    pub fn verify(&self, registration: &StoreDeviceRegistration) -> bool {
        self.body
            .resolutions
            .windows(2)
            .all(|pair| pair[0] < pair[1])
            && self
                .body
                .author_registration
                .verify_registration(registration)
                .is_ok()
            && registration.author_pubkey == self.body.entry.coord.author_pubkey
            && self.body.successor.predecessor
                == self
                    .body
                    .predecessor
                    .as_ref()
                    .map(|reference| reference.object.clone())
            && self.verify_by(&registration.device_signing_pubkey).is_ok()
    }

    pub fn entry_coord(&self) -> MembershipCoord {
        self.body.entry.coord.clone()
    }

    pub fn head_hash(&self) -> ObjectHash {
        self.hash()
    }
}

#[cfg(test)]
mod tests;

pub const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalStoreMembership {
    Current,
    NotYetMember,
    Removed,
    IdentityNotSupplied,
}

impl LocalStoreMembership {
    pub fn from_membership(
        membership: &MembershipChain,
        identity: Option<&coven_keys::keys::UserKeypair>,
    ) -> Result<Self, crate::membership::MembershipError> {
        membership.ensure_resolved()?;
        let Some(identity) = identity else {
            return Ok(Self::IdentityNotSupplied);
        };
        let identity = coven_keys::keys::public_key_hex(identity);
        if membership
            .current_members()
            .iter()
            .any(|(member, _)| member == &identity)
        {
            Ok(Self::Current)
        } else if membership.contains_member_history(&identity) {
            Ok(Self::Removed)
        } else {
            Ok(Self::NotYetMember)
        }
    }

    pub fn allows_circle_access(self) -> bool {
        matches!(self, Self::Current)
    }

    pub fn retains_circle_rows(self) -> bool {
        !matches!(self, Self::Removed)
    }
}

pub enum ApplyOutcome<R> {
    Applied(Vec<coven_foundation::changeset::RowChange>),
    Held(R),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct MembershipFloor(pub Vec<MembershipHeadRef>);

impl MembershipFloor {
    pub fn validate(&self) -> Result<(), MembershipFloorError> {
        crate::membership::validate_membership_floor(&self.0)
    }
}
