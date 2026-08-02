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
pub(crate) use super::causal_grants::{AuthorStreamId, MembershipGrantId};
use super::store_commit::{
    GrantStreamAnchor, ObjectHash, OwnerConflictResolutionAcceptance, OwnerPromotionAcceptance,
    OwnerPromotionFinalization, OwnerRecoveryCursor, StoreBatchCommitRef, StoreCreationId,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreRootRef,
    SuccessorLink, STORE_PROTOCOL_VERSION,
};
#[cfg(test)]
use super::store_commit::{
    OwnerPromotionAnchors, OwnerPromotionId, OwnerPromotionRequest,
    OwnerPromotionRequestActivation, OwnerRecoveryNodeRef, OwnerRecoveryPosition,
};
use super::wrapped_store_key::WrappedStoreKeyRef;
use crate::keys::{self, UserKeypair};
use crate::storage::ExactObjectRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MemberRole {
    Owner,
    Member,
    Follower,
}

impl MemberRole {
    pub(crate) fn can_write(&self) -> bool {
        matches!(self, Self::Owner | Self::Member)
    }
}

#[cfg(test)]
pub(crate) fn test_wrapped_key_ref(
    owner_pubkey: &str,
    recipient_pubkey: &str,
    generation: u64,
    label: &[u8],
) -> WrappedStoreKeyRef {
    let wrap_hash = ObjectHash::digest(
        &[
            label,
            owner_pubkey.as_bytes(),
            recipient_pubkey.as_bytes(),
            &generation.to_le_bytes(),
        ]
        .concat(),
    );
    let logical_key =
        format!("keys/{owner_pubkey}/{recipient_pubkey}/{generation}/{wrap_hash}.json");
    WrappedStoreKeyRef {
        owner_pubkey: owner_pubkey.to_string(),
        recipient_pubkey: recipient_pubkey.to_string(),
        generation,
        wrap_hash,
        object: ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(logical_key)
                .expect("test wrapped-key slot is valid"),
            label.len() as u64,
            ObjectHash::digest(label),
        ),
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

#[derive(Clone, PartialEq, Eq)]
pub struct MembershipConflictChoice {
    pub id: String,
    pub members: Vec<MemberInfo>,
    conflict_hash: ObjectHash,
    selection: MembershipConflictSelection,
}

impl std::fmt::Debug for MembershipConflictChoice {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MembershipConflictChoice")
            .field("id", &self.id)
            .field("members", &self.members)
            .finish()
    }
}

impl MembershipConflictChoice {
    pub(crate) fn new(
        id: String,
        members: Vec<MemberInfo>,
        conflict_hash: ObjectHash,
        selection: MembershipConflictSelection,
    ) -> Self {
        Self {
            id,
            members,
            conflict_hash,
            selection,
        }
    }

    pub(crate) fn conflict_hash(&self) -> ObjectHash {
        self.conflict_hash
    }

    pub(crate) fn selection(&self) -> &MembershipConflictSelection {
        &self.selection
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipConflictInfo {
    ConcurrentMemberAssignments {
        id: String,
        member_pubkey: String,
        choices: Vec<MembershipConflictChoice>,
    },
    RevocationCycle {
        id: String,
        choices: Vec<MembershipConflictChoice>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) enum MembershipChange {
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
    pub(crate) fn membership_anchor(&self) -> Option<GrantStreamAnchor> {
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
    pub(crate) fn stream_key(&self) -> MembershipStreamKey {
        MembershipStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            stream_id: self.stream_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MembershipStreamKey {
    pub(crate) author_pubkey: String,
    pub(crate) author_owner_grant: MembershipGrantId,
    pub(crate) stream_id: AuthorStreamId,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipEntry {
    pub version: u32,
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
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipEntryRef {
    pub coord: MembershipCoord,
    pub object: ExactObjectRef,
}

impl MembershipEntry {
    pub(crate) fn coord(&self) -> MembershipCoord {
        MembershipCoord {
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            stream_id: self.stream_id,
            seq: self.seq,
            entry_hash: entry_hash(self),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorHead {
    pub version: u32,
    pub store_id: String,
    pub body: MembershipHeadBody,
    pub activation: MembershipHeadActivation,
    pub signature: String,
}

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
    pub head_slot: crate::storage::cloud::ObjectSlot,
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

pub(crate) fn validate_membership_floor(floor: &[MembershipHeadRef]) -> Result<(), String> {
    if floor.is_empty() {
        return Err("membership floor is empty".to_string());
    }
    for (index, reference) in floor.iter().enumerate() {
        if reference.coord.seq == 0 {
            return Err("membership floor contains sequence zero".to_string());
        }
        if index > 0 && floor[index - 1].coord.stream_key() >= reference.coord.stream_key() {
            return Err("membership floor is not strictly ordered by author stream".to_string());
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
pub(crate) struct ResolvedStoreMembership {
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

fn active_membership_grants(
    grants: &BTreeMap<
        MembershipGrantId,
        GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
    >,
) -> impl Iterator<Item = (&MembershipGrantId, &MembershipGrantRecord)> {
    grants
        .iter()
        .filter_map(|(grant, state)| state.active().map(|record| (grant, record)))
}

impl ResolvedStoreMembership {
    pub(crate) fn active_grants(
        &self,
    ) -> impl Iterator<Item = (&MembershipGrantId, &MembershipGrantRecord)> {
        active_membership_grants(&self.grants)
    }

    pub(crate) fn active_grant(&self, grant: &MembershipGrantId) -> Option<&MembershipGrantRecord> {
        self.grants.get(grant).and_then(GrantState::active)
    }
}

impl StoreMembershipBranch {
    pub fn active_grants(
        &self,
    ) -> impl Iterator<Item = (&MembershipGrantId, &MembershipGrantRecord)> {
        active_membership_grants(&self.grants)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MembershipConflict {
    ConcurrentMemberAssignments {
        conflict_hash: ObjectHash,
        heads: Vec<MembershipHeadRef>,
        effective_frontier: Vec<MembershipCoord>,
        member_pubkey: String,
        conflicting_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
        uncontested_grants: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
        grants: BTreeMap<
            MembershipGrantId,
            GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
        >,
        provider_admin: super::provider::ProviderAdminResolution,
    },
    RevocationCycle {
        conflict_hash: ObjectHash,
        heads: Vec<MembershipHeadRef>,
        cyclic_sources: Vec<MembershipCoord>,
        involved_owner_grants: BTreeSet<MembershipGrantId>,
        maximal_valid_branches: Vec<StoreMembershipBranch>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MembershipStatus {
    Resolved(ResolvedStoreMembership),
    Conflict(MembershipConflict),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMembershipConflictResolutionRef {
    pub conflict_hash: ObjectHash,
    pub resolver_pubkey: String,
    pub resolution_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum MembershipConflictSelection {
    MemberAssignment { grant: MembershipGrantId },
    RevocationBranch { heads: Vec<MembershipHeadRef> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreMembershipConflictResolution {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub conflict_hash: ObjectHash,
    pub conflicting_heads: Vec<MembershipHeadRef>,
    pub retired_owner_grants: BTreeSet<MembershipGrantId>,
    pub retirement_barriers: BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>,
    pub resolver_pubkey: String,
    pub selection: MembershipConflictSelection,
    pub replacement_grant: MembershipGrantId,
    pub replacement_membership: GrantStreamAnchor,
    pub replacement_acceptance: OwnerConflictResolutionAcceptance,
    pub signature: String,
}

impl StoreMembershipConflictResolution {
    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            conflict_hash: ObjectHash,
            conflicting_heads: &'a [MembershipHeadRef],
            retired_owner_grants: &'a BTreeSet<MembershipGrantId>,
            retirement_barriers:
                &'a BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>,
            resolver_pubkey: &'a str,
            selection: &'a MembershipConflictSelection,
            replacement_grant: &'a MembershipGrantId,
            replacement_membership: &'a GrantStreamAnchor,
            replacement_acceptance: &'a OwnerConflictResolutionAcceptance,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.store-membership-conflict-resolution.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            conflict_hash: self.conflict_hash,
            conflicting_heads: &self.conflicting_heads,
            retired_owner_grants: &self.retired_owner_grants,
            retirement_barriers: &self.retirement_barriers,
            resolver_pubkey: &self.resolver_pubkey,
            selection: &self.selection,
            replacement_grant: &self.replacement_grant,
            replacement_membership: &self.replacement_membership,
            replacement_acceptance: &self.replacement_acceptance,
        })
        .expect("Store membership resolution serialization cannot fail")
    }

    pub(crate) fn resolution_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self)
                .expect("Store membership resolution serialization cannot fail"),
        )
    }

    pub(crate) fn resolution_ref(
        &self,
        object: ExactObjectRef,
    ) -> StoreMembershipConflictResolutionRef {
        StoreMembershipConflictResolutionRef {
            conflict_hash: self.conflict_hash,
            resolver_pubkey: self.resolver_pubkey.clone(),
            resolution_hash: self.resolution_hash(),
            object,
        }
    }

    pub(crate) fn verify_signature(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.replacement_grant
                == derive_store_resolution_grant(&self.conflict_hash, &self.resolver_pubkey)
            && self.replacement_acceptance.store_root_hash == self.store_root_hash
            && self.replacement_acceptance.owner_grant == self.replacement_grant
            && self.replacement_acceptance.membership == self.replacement_membership
            && keys::verify_signature_hex(
                &self.resolver_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub(crate) fn verify_against(
        &self,
        store_root_hash: ObjectHash,
        conflict: &MembershipConflict,
    ) -> bool {
        let (conflict_hash, heads, expected_retired, known_grants, resolver_is_owner) =
            match (conflict, &self.selection) {
                (
                    MembershipConflict::ConcurrentMemberAssignments {
                        conflict_hash,
                        heads,
                        conflicting_grants,
                        uncontested_grants,
                        grants,
                        ..
                    },
                    MembershipConflictSelection::MemberAssignment { grant },
                ) => (
                    conflict_hash,
                    heads,
                    uncontested_grants
                        .iter()
                        .filter_map(|(grant, record)| {
                            (record.member_pubkey == self.resolver_pubkey && record.role.is_owner())
                                .then_some(grant.clone())
                        })
                        .collect(),
                    grants.keys().cloned().collect::<BTreeSet<_>>(),
                    conflicting_grants.contains_key(grant)
                        && uncontested_grants.values().any(|record| {
                            record.member_pubkey == self.resolver_pubkey && record.role.is_owner()
                        }),
                ),
                (
                    MembershipConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        involved_owner_grants,
                        maximal_valid_branches,
                        ..
                    },
                    MembershipConflictSelection::RevocationBranch {
                        heads: selected_heads,
                    },
                ) => {
                    let Some(branch) = maximal_valid_branches
                        .iter()
                        .find(|branch| branch.heads == *selected_heads)
                    else {
                        return false;
                    };
                    let mut retired = involved_owner_grants.clone();
                    retired.extend(branch.active_grants().filter_map(|(grant, record)| {
                        (record.member_pubkey == self.resolver_pubkey && record.role.is_owner())
                            .then_some(grant.clone())
                    }));
                    (
                        conflict_hash,
                        heads,
                        retired,
                        maximal_valid_branches
                            .iter()
                            .flat_map(|branch| branch.grants.keys().cloned())
                            .collect(),
                        branch.active_grants().any(|(_, record)| {
                            record.member_pubkey == self.resolver_pubkey && record.role.is_owner()
                        }),
                    )
                }
                _ => return false,
            };
        self.version == STORE_PROTOCOL_VERSION
            && self.store_root_hash == store_root_hash
            && self.conflict_hash == *conflict_hash
            && self.conflicting_heads == *heads
            && self.retired_owner_grants == expected_retired
            && self.retirement_barriers.len() == known_grants.len()
            && self
                .retirement_barriers
                .keys()
                .all(|grant| known_grants.contains(grant))
            && self.replacement_grant
                == derive_store_resolution_grant(conflict_hash, &self.resolver_pubkey)
            && resolver_is_owner
            && self.verify_signature()
    }
}

pub(crate) fn derive_store_resolution_grant(
    conflict_hash: &ObjectHash,
    resolver_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!("coven.store-membership-resolution-grant.v1\0{conflict_hash}\0{resolver_pubkey}")
            .as_bytes(),
    ))
}

fn conflict_retirement_barriers(
    records: BTreeMap<MembershipGrantId, MembershipGrantRecord>,
    effective_frontier: Vec<MembershipCoord>,
    device_state: &StoreDeviceStateRef,
) -> Result<BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>, MembershipError> {
    let recovery = device_state.recovery();
    records
        .into_iter()
        .map(|(grant, record)| {
            let mut observed_streams = effective_frontier
                .iter()
                .filter(|coord| coord.author_owner_grant == grant)
                .cloned()
                .collect::<Vec<_>>();
            observed_streams.sort_by_key(MembershipCoord::stream_key);
            observed_streams.dedup_by_key(|coord| coord.stream_key());
            let author_streams = StoreGrantStreamBarrier { observed_streams };
            let barrier = if record.role.is_owner() {
                let cursor = recovery
                    .iter()
                    .find(|cursor| cursor.owner_grant == grant)
                    .cloned()
                    .ok_or(MembershipError::MissingOwnerRecoveryState)?;
                MergeMembershipGrantRetirementBarrier::Owner {
                    barrier: MergeStoreOwnerGrantBarrier {
                        author_streams,
                        recovery: cursor,
                    },
                }
            } else {
                MergeMembershipGrantRetirementBarrier::NonOwner { author_streams }
            };
            Ok((grant, barrier))
        })
        .collect()
}

pub(crate) fn resolve_store_membership_conflict(
    store_root_hash: ObjectHash,
    conflict: &MembershipConflict,
    resolutions: &[(
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    )],
) -> Result<ResolvedStoreMembership, MembershipError> {
    if resolutions.is_empty() {
        return Err(MembershipError::InvalidConflictResolution);
    }
    let mut by_resolver = BTreeMap::new();
    let mut retired_owner_grants = BTreeSet::new();
    for (_, resolution) in resolutions {
        if !resolution.verify_against(store_root_hash, conflict) {
            return Err(MembershipError::InvalidConflictResolution);
        }
        if let Some(existing) = by_resolver.insert(
            resolution.resolver_pubkey.clone(),
            resolution.resolution_hash(),
        ) {
            if existing != resolution.resolution_hash() {
                return Err(MembershipError::InvalidConflictResolution);
            }
            continue;
        }
        retired_owner_grants.extend(resolution.retired_owner_grants.iter().cloned());
    }
    let (mut grants, known_records, provider_admin) = match conflict {
        MembershipConflict::ConcurrentMemberAssignments {
            conflicting_grants,
            grants,
            provider_admin,
            ..
        } => {
            let selected = resolutions
                .iter()
                .filter_map(|(_, resolution)| match &resolution.selection {
                    MembershipConflictSelection::MemberAssignment { grant } => Some(grant.clone()),
                    MembershipConflictSelection::RevocationBranch { .. } => None,
                })
                .collect::<BTreeSet<_>>();
            let retained = (selected.len() == 1)
                .then(|| selected.first().cloned())
                .flatten();
            let mut resolved = grants.clone();
            for (grant, record) in conflicting_grants {
                if retained.as_ref() == Some(grant) {
                    continue;
                }
                let retirements = assignment_conflict_retirements(resolutions, grant)?;
                resolved.insert(
                    grant.clone(),
                    GrantState::Tombstoned {
                        record: record.clone(),
                        retirements,
                    },
                );
            }
            (
                resolved,
                grants
                    .iter()
                    .map(|(grant, state)| (grant.clone(), state.record().clone()))
                    .collect::<BTreeMap<_, _>>(),
                provider_admin.clone(),
            )
        }
        MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } => {
            let mut selected_branches = Vec::new();
            for (_, resolution) in resolutions {
                let MembershipConflictSelection::RevocationBranch {
                    heads: selected_heads,
                } = &resolution.selection
                else {
                    return Err(MembershipError::InvalidConflictResolution);
                };
                let branch = maximal_valid_branches
                    .iter()
                    .find(|branch| branch.heads == *selected_heads)
                    .ok_or(MembershipError::InvalidConflictResolution)?;
                if !selected_branches
                    .iter()
                    .any(|selected: &&StoreMembershipBranch| selected.heads == branch.heads)
                {
                    selected_branches.push(branch);
                }
            }
            let (first_branch, other_branches) = selected_branches
                .split_first()
                .ok_or(MembershipError::InvalidConflictResolution)?;
            let mut resolved = first_branch
                .active_grants()
                .filter(|(grant, _)| !retired_owner_grants.contains(*grant))
                .filter(|(grant, record)| {
                    other_branches.iter().all(|branch| {
                        branch.grants.get(*grant).and_then(GrantState::active) == Some(*record)
                    })
                })
                .map(|(grant, record)| {
                    (
                        grant.clone(),
                        GrantState::Active {
                            record: record.clone(),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let known_records = maximal_valid_branches
                .iter()
                .flat_map(|branch| branch.grants.iter())
                .map(|(grant, state)| (grant.clone(), state.record().clone()))
                .collect::<BTreeMap<_, _>>();
            for branch in maximal_valid_branches {
                for (grant, state) in &branch.grants {
                    if state.retirements().is_some()
                        && causal_grants::merge_conflict_grant_state(
                            &mut resolved,
                            grant.clone(),
                            state,
                        )
                        .is_err()
                    {
                        return Err(MembershipError::InvalidConflictResolution);
                    }
                }
            }
            for branch in maximal_valid_branches {
                for (grant, record) in branch.active_grants() {
                    if resolved.get(grant).and_then(GrantState::active).is_some() {
                        continue;
                    }
                    let resolution_retirements =
                        conflict_resolution_retirements(resolutions, grant)?;
                    match resolved.entry(grant.clone()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(GrantState::Tombstoned {
                                record: record.clone(),
                                retirements: resolution_retirements.clone(),
                            });
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            if entry.get().record() != record {
                                return Err(MembershipError::InvalidConflictResolution);
                            }
                            let GrantState::Tombstoned { retirements, .. } = entry.get_mut() else {
                                unreachable!("active conflict grant was handled above")
                            };
                            retirements.extend(resolution_retirements.iter().cloned());
                        }
                    }
                }
            }
            let provider_admin = super::provider::ProviderAdminResolution::Resolved(
                super::provider::ProviderAdminState::merge(
                    selected_branches
                        .iter()
                        .map(|branch| branch.provider_admin.combined_state().clone()),
                )?,
            );
            (resolved, known_records, provider_admin)
        }
    };
    for (reference, resolution) in resolutions {
        for retired in &resolution.retired_owner_grants {
            let record = known_records
                .get(retired)
                .ok_or(MembershipError::InvalidConflictResolution)?
                .clone();
            let barrier = resolution
                .retirement_barriers
                .get(retired)
                .cloned()
                .ok_or(MembershipError::InvalidConflictResolution)?;
            let retirement = MembershipGrantRetirement::ConflictResolution {
                authority: reference.clone(),
                barrier,
            };
            match grants.entry(retired.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(GrantState::Tombstoned {
                        record,
                        retirements: GrantRetirements::new(retirement),
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().record() != &record {
                        return Err(MembershipError::InvalidConflictResolution);
                    }
                    let mut retirements = match entry.get().retirements() {
                        Some(retirements) => retirements.clone(),
                        None => GrantRetirements::new(retirement.clone()),
                    };
                    retirements.insert(retirement);
                    *entry.get_mut() = GrantState::Tombstoned {
                        record,
                        retirements,
                    };
                }
            }
        }
    }
    for (reference, resolution) in resolutions {
        let record = MembershipGrantRecord {
            member_pubkey: resolution.resolver_pubkey.clone(),
            role: StoreMembershipRoleGrant::Owner {
                recovery: OwnerRecoveryAnchorRef::ConflictResolution {
                    acceptance: Box::new(resolution.replacement_acceptance.clone()),
                },
            },
            provider_account_email: None,
            creation_authority: MembershipGrantCreationAuthority::ConflictResolution(
                reference.clone(),
            ),
        };
        if grants
            .insert(
                resolution.replacement_grant.clone(),
                GrantState::Active {
                    record: record.clone(),
                },
            )
            .is_some_and(|current| current.active() != Some(&record))
        {
            return Err(MembershipError::InvalidConflictResolution);
        }
    }
    let mut members = BTreeSet::new();
    if !active_membership_grants(&grants).any(|(_, record)| record.role.is_owner())
        || active_membership_grants(&grants)
            .any(|(_, record)| !members.insert(record.member_pubkey.clone()))
    {
        return Err(MembershipError::InvalidConflictResolution);
    }
    Ok(ResolvedStoreMembership {
        state_hash: store_membership_state_hash(&grants, &provider_admin),
        grants,
        provider_admin,
    })
}

fn conflict_resolution_retirements(
    resolutions: &[(
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    )],
    grant: &MembershipGrantId,
) -> Result<GrantRetirements<MembershipGrantRetirement>, MembershipError> {
    let mut retirements = resolutions.iter().map(|(reference, resolution)| {
        resolution
            .retirement_barriers
            .get(grant)
            .cloned()
            .map(|barrier| MembershipGrantRetirement::ConflictResolution {
                authority: reference.clone(),
                barrier,
            })
            .ok_or(MembershipError::InvalidConflictResolution)
    });
    let first = retirements
        .next()
        .ok_or(MembershipError::InvalidConflictResolution)??;
    let mut result = GrantRetirements::new(first);
    for retirement in retirements {
        result.insert(retirement?);
    }
    Ok(result)
}

fn assignment_conflict_retirements(
    resolutions: &[(
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    )],
    grant: &MembershipGrantId,
) -> Result<GrantRetirements<MembershipGrantRetirement>, MembershipError> {
    let mut retirements = resolutions
        .iter()
        .filter(|(_, resolution)| {
            !matches!(
                &resolution.selection,
                MembershipConflictSelection::MemberAssignment { grant: selected }
                    if selected == grant
            )
        })
        .map(|(reference, resolution)| {
            resolution
                .retirement_barriers
                .get(grant)
                .cloned()
                .map(|barrier| MembershipGrantRetirement::ConflictResolution {
                    authority: reference.clone(),
                    barrier,
                })
                .ok_or(MembershipError::InvalidConflictResolution)
        });
    let first = retirements
        .next()
        .ok_or(MembershipError::InvalidConflictResolution)??;
    let mut result = GrantRetirements::new(first);
    for retirement in retirements {
        result.insert(retirement?);
    }
    Ok(result)
}

#[derive(Debug, Clone, Default)]
struct CausalState {
    grants:
        BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
}

#[derive(Debug, Clone)]
pub(crate) struct MembershipChain {
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

#[cfg(test)]
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
            crate::storage::cloud::ObjectSlot::logical(format!(
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

impl MembershipChain {
    #[cfg(test)]
    pub(crate) fn from_entries(entries: Vec<MembershipEntry>) -> Result<Self, MembershipError> {
        let provider_admin = test_provider_admin_genesis(&entries)?;
        Self::from_entries_with_coords_and_provider_admin(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            provider_admin,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_entries_with_coords_and_heads(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<(MembershipHeadRef, AuthorHead)>,
    ) -> Result<Self, MembershipError> {
        let values = entries
            .iter()
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        let provider_admin = test_provider_admin_genesis(&values)?;
        Self::from_entries_with_coords_and_heads_and_provider_admin(entries, heads, provider_admin)
    }

    #[cfg(test)]
    pub(crate) fn from_entries_with_coords_and_provider_admin(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        provider_admin: super::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        Self::from_entries_with_coords_and_head_refs(entries, Vec::new(), provider_admin)
    }

    pub(crate) fn from_entries_with_coords_and_heads_and_provider_admin(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<(MembershipHeadRef, AuthorHead)>,
        provider_admin: super::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        let expected_store = entries
            .first()
            .map(|(_, entry)| entry.store_id.as_str())
            .ok_or(MembershipError::EmptyChain)?;
        if heads.iter().any(|(reference, head)| {
            reference.head_hash != head.head_hash()
                || head.store_id != expected_store
                || entries
                    .iter()
                    .find(|(coord, _)| *coord == head.entry_coord())
                    .is_none_or(|(_, entry)| head.body.resolutions != entry.resolution_dependencies)
        }) {
            return Err(MembershipError::MissingConflictHeads);
        }
        Self::from_entries_with_coords_and_head_refs(
            entries,
            heads.into_iter().map(|(reference, _)| reference).collect(),
            provider_admin,
        )
    }

    fn from_entries_with_coords_and_head_refs(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        head_refs: Vec<MembershipHeadRef>,
        provider_admin_genesis: super::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        if entries.is_empty() {
            return Err(MembershipError::EmptyChain);
        }
        let (coords, entries): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let mut chain = Self {
            entries,
            coords,
            state: CausalState::default(),
            included: BTreeSet::new(),
            status: None,
            head_refs,
            resolution_checkpoint: None,
            provider_admin_genesis,
        };
        chain.rebuild()?;
        Ok(chain)
    }

    pub(crate) fn entries(&self) -> &[MembershipEntry] {
        &self.entries
    }

    pub(crate) fn status(&self) -> &MembershipStatus {
        self.status
            .as_ref()
            .expect("a loaded membership chain always has status")
    }

    pub(crate) fn head_refs(&self) -> &[MembershipHeadRef] {
        &self.head_refs
    }

    pub(crate) fn head_ref_for_stream(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<&MembershipHeadRef> {
        self.head_refs.iter().find(|reference| {
            reference.coord.author_pubkey == author
                && reference.coord.author_owner_grant == *grant
                && reference.coord.stream_id == stream_id
        })
    }

    pub(crate) fn membership_anchor(
        &self,
        grant: &MembershipGrantId,
    ) -> Option<&GrantStreamAnchor> {
        self.entries
            .iter()
            .find_map(|entry| match &entry.change {
                MembershipChange::Founder {
                    owner_grant_id,
                    membership,
                    ..
                } if owner_grant_id == grant => Some(membership),
                MembershipChange::SetMember {
                    grant_id,
                    membership: Some(membership),
                    ..
                } if grant_id == grant => Some(membership),
                _ => None,
            })
            .or_else(|| {
                self.resolution_checkpoint
                    .as_ref()?
                    .grant_anchors
                    .get(grant)
            })
    }

    pub(crate) fn membership_stream_id(&self, grant: &MembershipGrantId) -> Option<AuthorStreamId> {
        let record = self.state.grants.get(grant)?.record();
        store_membership_anchor_stream(&record.member_pubkey, grant, self.membership_anchor(grant)?)
    }

    pub(crate) fn activated_membership_streams(
        &self,
    ) -> Vec<(MembershipStreamKey, GrantStreamAnchor)> {
        let mut streams = self
            .state
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                let record = state.record();
                let anchor = self.membership_anchor(grant)?.clone();
                let stream_id = self.membership_stream_id(grant)?;
                Some((
                    MembershipStreamKey {
                        author_pubkey: record.member_pubkey.clone(),
                        author_owner_grant: grant.clone(),
                        stream_id,
                    },
                    anchor,
                ))
            })
            .collect::<BTreeMap<_, _>>();
        let mut included = self.included.clone();
        if let MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) = self.status()
        {
            for branch in maximal_valid_branches {
                included.extend(causal_grants::history_closure(
                    &self.entries,
                    &branch.effective_frontier,
                ));
            }
        }
        for (coord, entry) in self.entries_with_coords() {
            if !included.contains(coord) {
                continue;
            }
            let (owner_pubkey, grant, anchor) = match &entry.change {
                MembershipChange::SetMember {
                    user_pubkey,
                    role: StoreMembershipRoleGrant::Owner { .. },
                    grant_id,
                    membership: Some(membership),
                    ..
                } => (user_pubkey, grant_id, membership),
                _ => continue,
            };
            let stream_id = store_membership_anchor_stream(owner_pubkey, grant, anchor)
                .expect("validated Owner grant has a Store membership stream anchor");
            streams.insert(
                MembershipStreamKey {
                    author_pubkey: owner_pubkey.clone(),
                    author_owner_grant: grant.clone(),
                    stream_id,
                },
                anchor.clone(),
            );
        }
        streams.into_iter().collect()
    }

    pub(crate) fn activate_head_ref(
        &mut self,
        reference: MembershipHeadRef,
    ) -> Result<(), MembershipError> {
        if !self.coords.contains(&reference.coord) {
            return Err(MembershipError::MissingConflictHeads);
        }
        let stream = reference.coord.stream_key();
        self.head_refs
            .retain(|current| current.coord.stream_key() != stream);
        self.head_refs.push(reference);
        self.head_refs.sort();
        self.rebuild()
    }

    pub(crate) fn resolution_refs(&self) -> &[StoreMembershipConflictResolutionRef] {
        self.resolution_checkpoint
            .as_ref()
            .map_or(&[], |checkpoint| checkpoint.resolutions.as_slice())
    }

    pub(crate) fn conflict(&self) -> Option<&MembershipConflict> {
        match self.status() {
            MembershipStatus::Resolved(_) => None,
            MembershipStatus::Conflict(conflict) => Some(conflict),
        }
    }

    pub(crate) fn ensure_resolved(&self) -> Result<(), MembershipError> {
        match self.status() {
            MembershipStatus::Resolved(_) => Ok(()),
            MembershipStatus::Conflict(_) => Err(MembershipError::Conflict),
        }
    }

    pub(crate) fn resolved_with(
        &self,
        store_root_hash: ObjectHash,
        resolutions: &[(
            StoreMembershipConflictResolutionRef,
            StoreMembershipConflictResolution,
        )],
    ) -> Result<ResolvedStoreMembership, MembershipError> {
        match self.status() {
            MembershipStatus::Resolved(resolved) if resolutions.is_empty() => Ok(resolved.clone()),
            MembershipStatus::Conflict(conflict) => {
                resolve_store_membership_conflict(store_root_hash, conflict, resolutions)
            }
            MembershipStatus::Resolved(_) => Err(MembershipError::InvalidConflictResolution),
        }
    }

    pub(crate) fn signed_conflict_resolution(
        &self,
        store_root_hash: ObjectHash,
        selection: MembershipConflictSelection,
        replacement_membership: GrantStreamAnchor,
        replacement_acceptance: OwnerConflictResolutionAcceptance,
        signer: &UserKeypair,
    ) -> Result<StoreMembershipConflictResolution, MembershipError> {
        let MembershipStatus::Conflict(conflict) = self.status() else {
            return Err(MembershipError::Conflict);
        };
        let resolver_pubkey = keys::public_key_hex(signer);
        let (conflict_hash, heads, retired_owner_grants, records, effective_frontier) =
            match (conflict, &selection) {
                (
                    MembershipConflict::ConcurrentMemberAssignments {
                        conflict_hash,
                        heads,
                        effective_frontier,
                        conflicting_grants,
                        uncontested_grants,
                        grants,
                        ..
                    },
                    MembershipConflictSelection::MemberAssignment { grant },
                ) => {
                    if !conflicting_grants.contains_key(grant) {
                        return Err(MembershipError::InvalidConflictResolution);
                    }
                    let retired = uncontested_grants
                        .iter()
                        .filter_map(|(grant, record)| {
                            (record.member_pubkey == resolver_pubkey && record.role.is_owner())
                                .then_some(grant.clone())
                        })
                        .collect::<BTreeSet<_>>();
                    if retired.is_empty() {
                        return Err(MembershipError::SignerIsNotOwner(resolver_pubkey));
                    }
                    (
                        conflict_hash,
                        heads,
                        retired,
                        grants
                            .iter()
                            .map(|(grant, state)| (grant.clone(), state.record().clone()))
                            .collect(),
                        effective_frontier.clone(),
                    )
                }
                (
                    MembershipConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        involved_owner_grants,
                        maximal_valid_branches,
                        ..
                    },
                    MembershipConflictSelection::RevocationBranch {
                        heads: selected_heads,
                    },
                ) => {
                    let branch = maximal_valid_branches
                        .iter()
                        .find(|branch| branch.heads == *selected_heads)
                        .ok_or(MembershipError::InvalidConflictResolution)?;
                    let resolver_grants = branch
                        .active_grants()
                        .filter_map(|(grant, record)| {
                            (record.member_pubkey == resolver_pubkey && record.role.is_owner())
                                .then_some(grant.clone())
                        })
                        .collect::<BTreeSet<_>>();
                    if resolver_grants.is_empty() {
                        return Err(MembershipError::SignerIsNotOwner(resolver_pubkey));
                    }
                    let mut retired = involved_owner_grants.clone();
                    retired.extend(resolver_grants);
                    let records = maximal_valid_branches
                        .iter()
                        .flat_map(|branch| branch.grants.iter())
                        .map(|(grant, state)| (grant.clone(), state.record().clone()))
                        .collect();
                    let mut frontier = maximal_valid_branches
                        .iter()
                        .flat_map(|branch| branch.effective_frontier.iter().cloned())
                        .collect::<Vec<_>>();
                    frontier.sort();
                    frontier.dedup();
                    (conflict_hash, heads, retired, records, frontier)
                }
                _ => return Err(MembershipError::InvalidConflictResolution),
            };
        let replacement_grant = derive_store_resolution_grant(conflict_hash, &resolver_pubkey);
        let retirement_barriers = conflict_retirement_barriers(
            records,
            effective_frontier,
            &replacement_acceptance.device_state,
        )?;
        let mut resolution = StoreMembershipConflictResolution {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            conflict_hash: *conflict_hash,
            conflicting_heads: heads.clone(),
            retired_owner_grants,
            retirement_barriers,
            resolver_pubkey,
            selection,
            replacement_grant,
            replacement_membership,
            replacement_acceptance,
            signature: String::new(),
        };
        resolution.signature = keys::sign_hex(signer, &resolution.canonical_bytes()).1;
        Ok(resolution)
    }

    pub(crate) fn entries_with_coords(
        &self,
    ) -> impl Iterator<Item = (&MembershipCoord, &MembershipEntry)> {
        self.coords.iter().zip(self.entries.iter())
    }

    pub(crate) fn store_id(&self) -> Option<&str> {
        self.entries.first().map(|entry| entry.store_id.as_str())
    }

    pub(crate) fn founder_coord(&self) -> Option<&MembershipCoord> {
        self.entries_with_coords().find_map(|(coord, entry)| {
            matches!(entry.change, MembershipChange::Founder { .. }).then_some(coord)
        })
    }

    pub(crate) fn founder_entry(&self) -> Option<&MembershipEntry> {
        self.entries
            .iter()
            .find(|entry| matches!(entry.change, MembershipChange::Founder { .. }))
    }

    pub(crate) fn founder_pubkey(&self) -> Option<&str> {
        self.founder_entry().and_then(|entry| match &entry.change {
            MembershipChange::Founder { owner_pubkey, .. } => Some(owner_pubkey.as_str()),
            MembershipChange::SetMember { .. }
            | MembershipChange::RemoveMember { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => None,
        })
    }

    pub(crate) fn is_founded_by(&self, owner_pubkey: &str) -> bool {
        self.founder_pubkey() == Some(owner_pubkey)
    }

    pub(crate) fn add_entry(&mut self, entry: MembershipEntry) -> Result<(), MembershipError> {
        self.add_entry_at(entry.coord(), entry)
    }

    pub(crate) fn add_entry_at(
        &mut self,
        coord: MembershipCoord,
        entry: MembershipEntry,
    ) -> Result<(), MembershipError> {
        self.entries.push(entry);
        self.coords.push(coord);
        if let Err(error) = self.rebuild() {
            self.entries.pop();
            self.coords.pop();
            self.rebuild().expect("previous membership chain validated");
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn can_write_now(&self, pubkey: &str) -> bool {
        if self.conflict().is_some() {
            return false;
        }
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.can_write())
    }

    pub(crate) fn is_owner_now(&self, pubkey: &str) -> bool {
        if self.conflict().is_some() {
            return false;
        }
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.is_owner())
    }

    pub(crate) fn authorizes_write_authority(
        &self,
        authority: &MembershipGrantCreationAuthority,
        pubkey: &str,
    ) -> bool {
        let MembershipStatus::Resolved(resolved) = self.status() else {
            return false;
        };
        resolved.active_grants().any(|(_, record)| {
            record.member_pubkey == pubkey
                && record.role.can_write()
                && &record.creation_authority == authority
        })
    }

    pub(crate) fn active_grant(
        &self,
        grant_id: &MembershipGrantId,
    ) -> Option<&MembershipGrantRecord> {
        let MembershipStatus::Resolved(resolved) = self.status() else {
            return None;
        };
        resolved.active_grant(grant_id)
    }

    pub(crate) fn contains_coord(&self, expected: &MembershipCoord) -> bool {
        self.coords.iter().any(|coord| coord == expected)
    }

    pub(crate) fn effectively_contains_coord(&self, expected: &MembershipCoord) -> bool {
        self.included.contains(expected)
    }

    pub(crate) fn current_members(&self) -> Vec<(String, MemberRole)> {
        let mut members = BTreeMap::new();
        for state in self.state.grants.values() {
            let Some(record) = state.active() else {
                continue;
            };
            members.insert(record.member_pubkey.clone(), record.role.role());
        }
        members.into_iter().collect()
    }

    pub(crate) fn contains_member_history(&self, pubkey: &str) -> bool {
        self.state
            .grants
            .values()
            .any(|state| state.record().member_pubkey == pubkey)
    }

    pub(crate) fn active_wrapped_keys_for(
        &self,
        recipient_pubkey: &str,
    ) -> Vec<WrappedStoreKeyRef> {
        let active_grants = self.active_grant_ids(recipient_pubkey);
        self.entries_with_coords()
            .filter(|(coord, _)| self.included.contains(*coord))
            .flat_map(|(_, entry)| match &entry.change {
                MembershipChange::SetMember {
                    grant_id,
                    wrapped_key,
                    ..
                } if active_grants.contains(grant_id) => std::slice::from_ref(wrapped_key),
                MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.as_slice(),
                MembershipChange::Founder { .. }
                | MembershipChange::SetMember { .. }
                | MembershipChange::ProviderAdmin
                | MembershipChange::ResolutionActivation { .. } => &[],
            })
            .filter(|reference| reference.recipient_pubkey == recipient_pubkey)
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(crate) fn wrapped_key_authority_for(
        &self,
        recipient_pubkey: &str,
    ) -> Result<Vec<WrappedStoreKeyRef>, MembershipError> {
        let active_grants = self.active_grants_for(recipient_pubkey);
        for (index, (rotation_coord, entry)) in self
            .entries_with_coords()
            .enumerate()
            .filter(|(_, (coord, _))| self.included.contains(*coord))
        {
            let MembershipChange::RemoveMember { wrapped_keys, .. } = &entry.change else {
                continue;
            };
            if wrapped_keys
                .iter()
                .any(|reference| reference.recipient_pubkey == recipient_pubkey)
            {
                continue;
            }
            let rotation_generation = wrapped_keys
                .first()
                .ok_or(MembershipError::InvalidWrappedKeys(index))?
                .generation;
            let covered_by_later_grant = !active_grants.is_empty()
                && active_grants.iter().all(|(active_grant, _)| {
                    let Some((_, creation)) = self.entries_with_coords().find(|(_, entry)| {
                        matches!(
                            &entry.change,
                            MembershipChange::SetMember { grant_id, .. }
                                if grant_id == *active_grant
                        )
                    }) else {
                        return false;
                    };
                    let MembershipChange::SetMember { wrapped_key, .. } = &creation.change else {
                        return false;
                    };
                    wrapped_key.generation >= rotation_generation
                        && causal_grants::history_closure(&self.entries, &creation.dependencies)
                            .contains(rotation_coord)
                });
            if !covered_by_later_grant {
                return Err(MembershipError::MissingWrappedKeyCoverage {
                    recipient_pubkey: recipient_pubkey.to_string(),
                    rotation: Box::new(rotation_coord.clone()),
                });
            }
        }
        Ok(self.active_wrapped_keys_for(recipient_pubkey))
    }

    pub(crate) fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        self.active_grants_for(pubkey)
            .into_iter()
            .next()
            .and_then(|(_, record)| record.provider_account_email.as_deref())
    }

    pub(crate) fn write_grant_authority(
        &self,
        pubkey: &str,
    ) -> Option<MembershipGrantCreationAuthority> {
        self.active_grants_for(pubkey)
            .into_iter()
            .find(|(_, record)| record.role.can_write())
            .map(|(_, record)| record.creation_authority.clone())
    }

    pub(crate) fn active_grant_ids(&self, pubkey: &str) -> BTreeSet<MembershipGrantId> {
        self.active_grants_for(pubkey)
            .into_iter()
            .map(|(grant, _)| grant.clone())
            .collect()
    }

    pub(crate) fn active_owner_grant(&self, pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants_for(pubkey)
            .into_iter()
            .find(|(_, record)| record.role.is_owner())
            .map(|(grant, _)| grant.clone())
    }

    pub(crate) fn reusable_author_streams(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
    ) -> BTreeSet<AuthorStreamId> {
        self.effective_frontier()
            .into_iter()
            .filter(|coord| {
                coord.author_pubkey == author_pubkey
                    && coord.author_owner_grant == *grant
                    && self.raw_stream_tip(author_pubkey, grant, coord.stream_id)
                        == Some(coord.clone())
            })
            .map(|coord| coord.stream_id)
            .collect()
    }

    /// Raw signed coverage: the greatest loaded coordinate in every stream,
    /// including suffixes removed by causal pruning.
    #[cfg(test)]
    pub(crate) fn author_heads(&self) -> Vec<MembershipCoord> {
        causal_grants::stream_frontier(self.coords.iter().cloned())
    }

    /// Effective authoring frontier after causal pruning.
    pub(crate) fn effective_frontier(&self) -> Vec<MembershipCoord> {
        causal_grants::stream_frontier(
            self.coords
                .iter()
                .filter(|coord| self.included.contains(*coord))
                .cloned(),
        )
    }

    pub(crate) fn causally_includes(&self, predecessor: &MembershipChain) -> bool {
        predecessor.included.is_subset(&self.included)
            && predecessor
                .resolution_refs()
                .iter()
                .all(|reference| self.resolution_refs().binary_search(reference).is_ok())
    }

    pub(crate) fn stream_tip(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<MembershipCoord> {
        self.effective_frontier().into_iter().find(|coord| {
            coord.author_pubkey == author_pubkey
                && coord.author_owner_grant == *grant
                && coord.stream_id == stream_id
        })
    }

    pub(crate) fn raw_stream_tip(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<MembershipCoord> {
        self.coords
            .iter()
            .filter(|coord| {
                coord.author_pubkey == author_pubkey
                    && coord.author_owner_grant == *grant
                    && coord.stream_id == stream_id
            })
            .max_by_key(|coord| coord.seq)
            .cloned()
    }

    pub(crate) fn next_member_grant_id_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: &str,
    ) -> Result<MembershipGrantId, MembershipError> {
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, _) = self.next_stream_position(&author, &author_grant, stream_id)?;
        Ok(derive_grant_id(
            self.store_id().expect("validated chain has a store id"),
            &author,
            &author_grant,
            stream_id,
            seq,
            user_pubkey,
        ))
    }

    pub(crate) fn signed_set_member_with_anchor_and_wrapped_key_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        membership: Option<GrantStreamAnchor>,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let role = StoreMembershipRoleGrant::from_direct_assignment(role)?;
        let grant_id = self.next_member_grant_id_in_stream(signer, stream_id, &user_pubkey)?;
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            stream_id,
            user_pubkey,
            provider_account_email,
            role,
            grant_id,
            membership,
            wrapped_key,
            created_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: StoreMembershipRoleGrant,
        grant_id: MembershipGrantId,
        membership: Option<GrantStreamAnchor>,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, previous_hash) = self.next_stream_position(&author, &author_grant, stream_id)?;
        let replaces = self.active_grant_ids(&user_pubkey);
        let retirement_barriers = self.membership_retirement_barriers(&replaces, None)?;
        if role.is_owner() != membership.is_some() {
            return Err(MembershipError::InvalidOwnerMembershipAnchor(
                self.entries.len(),
            ));
        }
        let mut entry = MembershipEntry {
            version: STORE_PROTOCOL_VERSION,
            store_id: self
                .store_id()
                .expect("validated chain has a store id")
                .to_string(),
            author_pubkey: author,
            author_owner_grant: author_grant,
            stream_id,
            seq,
            previous_hash,
            dependencies: self.effective_frontier(),
            resolution_dependencies: self.resolution_refs().to_vec(),
            created_at,
            change: MembershipChange::SetMember {
                user_pubkey: user_pubkey.clone(),
                provider_account_email,
                role,
                grant_id,
                membership,
                replaces,
                retirement_barriers,
                retirement_device_state: None,
                wrapped_key,
            },
            provider_admin: None,
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn signed_finalize_owner_promotion_in_stream(
        &self,
        root: &StoreRootRef,
        promoter: &StoreDeviceRegistration,
        candidate: &StoreDeviceRegistration,
        acceptance: OwnerPromotionAcceptance,
        signer: &UserKeypair,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        acceptance
            .request
            .verify(root, promoter)
            .map_err(|_| MembershipError::InvalidOwnerPromotion)?;
        acceptance
            .verify(candidate)
            .map_err(|_| MembershipError::InvalidOwnerPromotion)?;
        let request = &acceptance.request;
        let author = keys::public_key_hex(signer);
        let OwnerPromotionFinalization {
            author_stream,
            seq: requested_seq,
            previous_hash: requested_previous_hash,
        } = request.finalization;
        let (expected_seq, expected_previous_hash) =
            self.next_stream_position(&author, &request.promoter_owner_grant, author_stream)?;
        let Some(member) = self.active_grant(&request.member_grant) else {
            return Err(MembershipError::InvalidOwnerPromotion);
        };
        let membership = &acceptance.anchors.membership;
        let root_id = root.store_root_id.to_string();
        if author != promoter.author_pubkey
            || self.store_id() != Some(root_id.as_str())
            || self.active_owner_grant(&author) != Some(request.promoter_owner_grant.clone())
            || member.member_pubkey != request.member_pubkey
            || member.role != StoreMembershipRoleGrant::Member
            || self.active_grant_ids(&request.member_pubkey)
                != BTreeSet::from([request.member_grant.clone()])
            || expected_seq != requested_seq
            || expected_previous_hash != requested_previous_hash
            || self
                .state
                .grants
                .contains_key(&request.intended_owner_grant)
        {
            return Err(MembershipError::InvalidOwnerPromotion);
        }
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            author_stream,
            request.member_pubkey.clone(),
            member.provider_account_email.clone(),
            StoreMembershipRoleGrant::Owner {
                recovery: OwnerRecoveryAnchorRef::Promotion {
                    acceptance: Box::new(acceptance.clone()),
                },
            },
            request.intended_owner_grant.clone(),
            Some(membership.clone()),
            wrapped_key,
            created_at,
        )
    }

    #[cfg(test)]
    pub(crate) fn signed_set_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let role = StoreMembershipRoleGrant::from_direct_assignment(role)?;
        let grant_id = self.next_member_grant_id_in_stream(signer, stream_id, &user_pubkey)?;
        let dependencies = self.effective_frontier();
        let wrapped_key = test_wrapped_key_ref(
            &keys::public_key_hex(signer),
            &user_pubkey,
            membership_causal_generation(&self.entries, &dependencies),
            b"Merge membership test wrap",
        );
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            stream_id,
            user_pubkey,
            provider_account_email,
            role,
            grant_id,
            None,
            wrapped_key,
            created_at,
        )
    }

    #[cfg(test)]
    pub(crate) fn signed_promote_member_in_stream_for_test(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author_pubkey = keys::public_key_hex(signer);
        let dependencies = self.effective_frontier();
        let wrapped_key = test_wrapped_key_ref(
            &author_pubkey,
            &user_pubkey,
            membership_causal_generation(&self.entries, &dependencies),
            b"Merge Owner-promotion test wrap",
        );
        self.signed_promote_member_in_stream_with_wrapped_key_for_test(
            signer,
            stream_id,
            user_pubkey,
            wrapped_key,
            created_at,
        )
    }

    #[cfg(test)]
    pub(crate) fn signed_promote_member_in_stream_with_wrapped_key_for_test(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author_pubkey = keys::public_key_hex(signer);
        let promoter_owner_grant = self
            .active_owner_grant(&author_pubkey)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author_pubkey.clone()))?;
        let member_grants = self.active_grant_ids(&user_pubkey);
        let Some(member_grant) = member_grants.iter().next().cloned() else {
            return Err(MembershipError::InvalidOwnerPromotion);
        };
        if member_grants.len() != 1
            || self
                .active_grant(&member_grant)
                .is_none_or(|record| record.role != StoreMembershipRoleGrant::Member)
        {
            return Err(MembershipError::InvalidOwnerPromotion);
        }
        let (seq, previous_hash) =
            self.next_stream_position(&author_pubkey, &promoter_owner_grant, stream_id)?;
        let promotion_id = OwnerPromotionId::from_generated(format!(
            "test promotion {author_pubkey} {user_pubkey} {stream_id:?} {seq}"
        ));
        let store_root_hash = ObjectHash::digest(
            self.store_id()
                .expect("validated membership chain has a Store id")
                .as_bytes(),
        );
        let intended_owner_grant = super::store_commit::derive_owner_promotion_grant(
            store_root_hash,
            promotion_id,
            &user_pubkey,
        );
        let membership_state_hash = match self.status() {
            MembershipStatus::Resolved(state) => state.state_hash,
            MembershipStatus::Conflict(_) => return Err(MembershipError::InvalidOwnerPromotion),
        };
        let object = |name: &str| {
            let slot = crate::storage::cloud::ObjectSlot::logical(format!(
                "test/owner-promotion/{promotion_id:?}/{name}.json"
            ))
            .expect("test Owner-promotion slot is valid");
            ExactObjectRef::new(slot, 1, ObjectHash::digest(name.as_bytes()))
        };
        let registration = |name: &str| StoreDeviceRegistrationRef {
            device_id: ObjectHash::digest(name.as_bytes())
                .to_string()
                .parse()
                .expect("digest is a valid Store device id"),
            registration_hash: ObjectHash::digest(format!("{name} registration").as_bytes()),
            object: object(&format!("{name}-registration")),
        };
        let candidate_stream = AuthorStreamId::from_bytes([0xA5; 32]);
        let activation_commit = super::store_commit::StoreBatchCommitRef {
            coord: super::store_commit::StoreCommitCoord {
                stream_id: candidate_stream,
                sequence: 1,
            },
            commit_hash: ObjectHash::digest(b"test Owner-promotion activation commit"),
            object: object("activation-commit"),
        };
        let membership = GrantStreamAnchor::StoreMembership {
            first_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                "{}.json",
                super::store_commit::membership_head_slot_prefix(
                    &user_pubkey,
                    &intended_owner_grant,
                    stream_id,
                    1,
                )
            ))
            .expect("test membership head slot is valid"),
        };
        let request = OwnerPromotionRequest {
            version: STORE_PROTOCOL_VERSION,
            promotion_id,
            store_root_hash,
            promoter_registration: registration("promoter"),
            promoter_owner_grant: promoter_owner_grant.clone(),
            member_pubkey: user_pubkey.clone(),
            member_grant,
            member_registration: registration("member"),
            intended_owner_grant: intended_owner_grant.clone(),
            predecessor_membership: super::circle_control::StoreMembershipStateRef::from_parts(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                membership_state_hash,
            )
            .expect("construct test predecessor membership"),
            predecessor_devices: StoreDeviceStateRef::from_resolved(
                super::store_commit::CommitFrontier(BTreeMap::new()),
                &super::store_commit::ResolvedStoreDeviceState {
                    devices: BTreeMap::new(),
                    recovery: Vec::new(),
                    state_hash: ObjectHash::digest(b"test Owner-promotion device state"),
                },
            )
            .expect("construct test predecessor device state"),
            finalization: OwnerPromotionFinalization {
                author_stream: stream_id,
                seq,
                previous_hash,
            },
            signature: String::new(),
        };
        let acceptance = OwnerPromotionAcceptance {
            request: Box::new(request),
            activation: OwnerPromotionRequestActivation {
                commit: activation_commit,
                head: super::store_commit::StoreDeviceHeadRef {
                    head_hash: ObjectHash::digest(b"test Owner-promotion activation head"),
                    object: object("activation-head"),
                },
            },
            anchors: OwnerPromotionAnchors {
                membership: membership.clone(),
                recovery: GrantStreamAnchor::OwnerRecovery {
                    first_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                        "test/owner-promotion/{promotion_id:?}/recovery/1.json"
                    ))
                    .expect("test recovery slot is valid"),
                },
            },
            signature: String::new(),
        };
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            stream_id,
            user_pubkey,
            None,
            StoreMembershipRoleGrant::Owner {
                recovery: OwnerRecoveryAnchorRef::Promotion {
                    acceptance: Box::new(acceptance),
                },
            },
            intended_owner_grant,
            Some(membership),
            wrapped_key,
            created_at,
        )
    }

    #[cfg(test)]
    pub(crate) fn add_owner_for_test(
        &mut self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<(), MembershipError> {
        let member = self.signed_set_member_in_stream(
            signer,
            stream_id,
            user_pubkey.clone(),
            None,
            MemberRole::Member,
            format!("{created_at}: Member grant"),
        )?;
        self.add_entry(member)?;
        let promotion = self.signed_promote_member_in_stream_for_test(
            signer,
            stream_id,
            user_pubkey,
            created_at,
        )?;
        self.add_entry(promotion)
    }

    pub(crate) fn signed_remove_member_with_wrapped_keys_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.signed_remove_member_with_barrier_state(
            signer,
            stream_id,
            user_pubkey,
            wrapped_keys,
            None,
            created_at,
        )
    }

    pub(crate) fn signed_remove_member_with_owner_barrier_state(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        device_state: StoreDeviceStateRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.signed_remove_member_with_barrier_state(
            signer,
            stream_id,
            user_pubkey,
            wrapped_keys,
            Some(device_state),
            created_at,
        )
    }

    fn signed_remove_member_with_barrier_state(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        retirement_device_state: Option<StoreDeviceStateRef>,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let removes = self.active_grant_ids(&user_pubkey);
        if removes.is_empty() {
            return Err(MembershipError::NotAMember(user_pubkey));
        }
        let retains_owner = self.state.grants.iter().any(|(grant, state)| {
            !removes.contains(grant) && state.active().is_some_and(|record| record.role.is_owner())
        });
        if !retains_owner {
            return Err(MembershipError::NoActiveOwner);
        }
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, previous_hash) = self.next_stream_position(&author, &author_grant, stream_id)?;
        let retirement_barriers =
            self.membership_retirement_barriers(&removes, retirement_device_state.as_ref())?;
        let mut entry = MembershipEntry {
            version: STORE_PROTOCOL_VERSION,
            store_id: self
                .store_id()
                .expect("validated chain has a store id")
                .to_string(),
            author_pubkey: author,
            author_owner_grant: author_grant,
            stream_id,
            seq,
            previous_hash,
            dependencies: self.effective_frontier(),
            resolution_dependencies: self.resolution_refs().to_vec(),
            created_at,
            change: MembershipChange::RemoveMember {
                user_pubkey,
                removes,
                retirement_barriers,
                retirement_device_state,
                wrapped_keys,
            },
            provider_admin: None,
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    #[cfg(test)]
    pub(crate) fn signed_remove_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let owner = keys::public_key_hex(signer);
        let dependencies = self.effective_frontier();
        let generation = membership_causal_generation(&self.entries, &dependencies)
            .checked_add(1)
            .ok_or(MembershipError::InvalidWrappedKeys(self.entries.len()))?;
        let wrapped_keys = self
            .current_members()
            .into_iter()
            .filter(|(member, _)| member != &user_pubkey)
            .map(|(member, _)| {
                test_wrapped_key_ref(&owner, &member, generation, b"Merge removal test wrap")
            })
            .collect();
        let removes = self.active_grant_ids(&user_pubkey);
        let mut recovery = removes
            .iter()
            .filter_map(|grant| {
                self.state
                    .grants
                    .get(grant)
                    .and_then(GrantState::active)
                    .filter(|record| record.role.is_owner())
                    .map(|record| OwnerRecoveryCursor {
                        owner_grant: grant.clone(),
                        position: OwnerRecoveryPosition::At {
                            node: OwnerRecoveryNodeRef {
                                owner_pubkey: record.member_pubkey.clone(),
                                owner_grant: grant.clone(),
                                sequence: 1,
                                node_hash: ObjectHash::digest(
                                    format!("test recovery node {grant}").as_bytes(),
                                ),
                                object: ExactObjectRef::new(
                                    crate::storage::cloud::ObjectSlot::logical(format!(
                                        "test/recovery/{grant}/1.json"
                                    ))
                                    .expect("test recovery node slot is valid"),
                                    1,
                                    ObjectHash::digest(format!("test recovery {grant}").as_bytes()),
                                ),
                            },
                        },
                    })
            })
            .collect::<Vec<_>>();
        recovery.sort();
        let device_state = (!recovery.is_empty()).then(|| {
            StoreDeviceStateRef::from_resolved(
                super::store_commit::CommitFrontier(BTreeMap::new()),
                &super::store_commit::ResolvedStoreDeviceState {
                    devices: BTreeMap::new(),
                    recovery,
                    state_hash: ObjectHash::digest(b"test membership retirement device state"),
                },
            )
            .expect("construct test membership retirement device state")
        });
        self.signed_remove_member_with_barrier_state(
            signer,
            stream_id,
            user_pubkey,
            wrapped_keys,
            device_state,
            created_at,
        )
    }

    pub(crate) fn signed_resolution_activation_in_stream(
        &self,
        store_root_hash: ObjectHash,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        reference: StoreMembershipConflictResolutionRef,
        resolution: &StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.ensure_resolved()?;
        let MembershipStatus::Resolved(resolved_before) = self.status() else {
            unreachable!("ensure_resolved accepted a conflict")
        };
        let author = keys::public_key_hex(signer);
        if !resolution.verify_signature()
            || resolution.store_root_hash != store_root_hash
            || reference.resolver_pubkey != author
            || !self.resolution_refs().contains(&reference)
            || self.active_owner_grant(&author) != Some(resolution.replacement_grant.clone())
        {
            return Err(MembershipError::InvalidConflictResolution);
        }
        let author_grant = resolution.replacement_grant.clone();
        if self
            .raw_stream_tip(&author, &author_grant, stream_id)
            .is_some()
        {
            return Err(MembershipError::ResolutionActivationRequiresFreshStream);
        }
        let mut entry = MembershipEntry {
            version: STORE_PROTOCOL_VERSION,
            store_id: self
                .store_id()
                .expect("validated chain has a store id")
                .to_string(),
            author_pubkey: author,
            author_owner_grant: author_grant,
            stream_id,
            seq: 1,
            previous_hash: None,
            dependencies: self.effective_frontier(),
            resolution_dependencies: self.resolution_refs().to_vec(),
            created_at,
            change: MembershipChange::ResolutionActivation {
                resolution: reference,
            },
            provider_admin: None,
            signature: String::new(),
        };
        sign_membership_entry(&mut entry, signer);
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        let MembershipStatus::Resolved(resolved_after) = candidate.status() else {
            return Err(MembershipError::InvalidConflictResolution);
        };
        if resolved_after.state_hash != resolved_before.state_hash {
            return Err(MembershipError::InvalidConflictResolution);
        }
        Ok(entry)
    }

    pub(crate) fn next_stream_position(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Result<(u64, Option<ObjectHash>), MembershipError> {
        let raw_tip = self.raw_stream_tip(author, grant, stream_id);
        let effective_tip = self.stream_tip(author, grant, stream_id);
        if raw_tip != effective_tip {
            return Err(MembershipError::PrunedAuthorStream);
        }
        effective_tip.map_or(Ok((1, None)), |tip| {
            tip.seq
                .checked_add(1)
                .map(|seq| (seq, Some(tip.entry_hash)))
                .ok_or(MembershipError::SequenceExhausted)
        })
    }

    fn membership_retirement_barriers(
        &self,
        grants: &BTreeSet<MembershipGrantId>,
        device_state: Option<&StoreDeviceStateRef>,
    ) -> Result<BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>, MembershipError>
    {
        let retires_owner = grants.iter().any(|grant| {
            self.state
                .grants
                .get(grant)
                .and_then(GrantState::active)
                .is_some_and(|record| record.role.is_owner())
        });
        if retires_owner && device_state.is_none() {
            return Err(MembershipError::MissingOwnerRecoveryState);
        }
        if !retires_owner && device_state.is_some() {
            return Err(MembershipError::UnexpectedOwnerRecoveryState);
        }
        let recovery = match device_state {
            Some(state) => state.recovery(),
            None => &[],
        };
        grants
            .iter()
            .map(|grant| {
                let record = self
                    .state
                    .grants
                    .get(grant)
                    .and_then(GrantState::active)
                    .ok_or_else(|| MembershipError::NotAMember(grant.to_string()))?;
                let author_streams = StoreGrantStreamBarrier {
                    observed_streams: self
                        .effective_frontier()
                        .into_iter()
                        .filter(|coord| coord.author_owner_grant == *grant)
                        .collect(),
                };
                let barrier = if record.role.is_owner() {
                    let cursor = recovery
                        .iter()
                        .find(|cursor| cursor.owner_grant == *grant)
                        .cloned()
                        .ok_or(MembershipError::MissingOwnerRecoveryState)?;
                    MergeMembershipGrantRetirementBarrier::Owner {
                        barrier: MergeStoreOwnerGrantBarrier {
                            author_streams,
                            recovery: cursor,
                        },
                    }
                } else {
                    MergeMembershipGrantRetirementBarrier::NonOwner { author_streams }
                };
                Ok((grant.clone(), barrier))
            })
            .collect()
    }

    fn active_grants_for(&self, pubkey: &str) -> Vec<(&MembershipGrantId, &MembershipGrantRecord)> {
        self.state
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                state
                    .active()
                    .filter(|record| record.member_pubkey == pubkey)
                    .map(|record| (grant, record))
            })
            .collect()
    }

    fn rebuild(&mut self) -> Result<(), MembershipError> {
        let expected_store = self
            .entries
            .first()
            .ok_or(MembershipError::EmptyChain)?
            .store_id
            .clone();
        if expected_store.is_empty() {
            return Err(MembershipError::InvalidFounder);
        }

        for (index, (coord, entry)) in self.entries_with_coords().enumerate() {
            if entry.version != STORE_PROTOCOL_VERSION {
                return Err(MembershipError::UnsupportedVersion(index));
            }
            if entry.store_id != expected_store {
                return Err(MembershipError::StoreMismatch {
                    index,
                    expected: expected_store.clone(),
                    actual: entry.store_id.clone(),
                });
            }
            if !verify_membership_entry(entry) {
                return Err(MembershipError::InvalidSignature(index));
            }
            let actual = entry.coord();
            if *coord != actual {
                return Err(MembershipError::CoordinateMismatch {
                    index,
                    expected: Box::new(coord.clone()),
                    actual: Box::new(actual),
                });
            }
            if !entry
                .dependencies
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            {
                return Err(MembershipError::NonCanonicalDependencyFrontier { index });
            }
            let (barriers, retirement_device_state) = match &entry.change {
                MembershipChange::SetMember {
                    user_pubkey,
                    role,
                    grant_id,
                    replaces,
                    membership,
                    retirement_barriers,
                    retirement_device_state,
                    ..
                } => {
                    let valid_owner_origin = match role {
                        StoreMembershipRoleGrant::Owner {
                            recovery: OwnerRecoveryAnchorRef::Promotion { acceptance },
                        } => {
                            let request = &acceptance.request;
                            let anchors_match =
                                Some(&acceptance.anchors.membership) == membership.as_ref();
                            let finalization_matches = request.finalization.author_stream
                                == entry.stream_id
                                && request.finalization.seq == entry.seq
                                && request.finalization.previous_hash == entry.previous_hash;
                            request.member_pubkey == *user_pubkey
                                && replaces.len() == 1
                                && replaces.contains(&request.member_grant)
                                && request.intended_owner_grant == *grant_id
                                && request.promoter_owner_grant == entry.author_owner_grant
                                && anchors_match
                                && finalization_matches
                        }
                        StoreMembershipRoleGrant::Owner { .. } => false,
                        StoreMembershipRoleGrant::Member | StoreMembershipRoleGrant::Follower => {
                            membership.is_none()
                        }
                    };
                    if role.is_owner()
                        != membership.as_ref().is_some_and(|anchor| {
                            store_membership_anchor_stream(user_pubkey, grant_id, anchor).is_some()
                        })
                        || !valid_owner_origin
                    {
                        return Err(MembershipError::InvalidOwnerMembershipAnchor(index));
                    }
                    (retirement_barriers, retirement_device_state)
                }
                MembershipChange::RemoveMember {
                    retirement_barriers,
                    retirement_device_state,
                    ..
                } => (retirement_barriers, retirement_device_state),
                MembershipChange::ResolutionActivation { resolution } => {
                    if resolution.resolver_pubkey != entry.author_pubkey
                        || entry.seq != 1
                        || entry.previous_hash.is_some()
                        || entry
                            .dependencies
                            .iter()
                            .any(|dependency| dependency.stream_key() == entry.coord().stream_key())
                        || entry.author_owner_grant
                            != derive_store_resolution_grant(
                                &resolution.conflict_hash,
                                &resolution.resolver_pubkey,
                            )
                        || entry
                            .resolution_dependencies
                            .binary_search(resolution)
                            .is_err()
                        || self
                            .resolution_checkpoint
                            .as_ref()
                            .is_none_or(|checkpoint| {
                                let already_checkpointed =
                                    checkpoint.included.contains(&entry.coord())
                                        || checkpoint.raw_heads.contains(&entry.coord());
                                !already_checkpointed
                                    && (entry.dependencies != checkpoint.effective_frontier
                                        || entry.resolution_dependencies != checkpoint.resolutions)
                            })
                    {
                        return Err(MembershipError::InvalidResolutionActivation(index));
                    }
                    continue;
                }
                MembershipChange::ProviderAdmin => {
                    let Some(super::provider::ProviderAdminMembershipChange {
                        owner_barriers, ..
                    }) = &entry.provider_admin
                    else {
                        return Err(MembershipError::InvalidProviderAdminChange(index));
                    };
                    if !entry.resolution_dependencies.is_empty()
                        || owner_barriers.values().any(|barrier| {
                            !barrier
                                .observed_streams
                                .windows(2)
                                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
                        })
                    {
                        return Err(MembershipError::InvalidProviderAdminChange(index));
                    }
                    continue;
                }
                MembershipChange::Founder { .. } => continue,
            };
            if entry.provider_admin.is_some() {
                return Err(MembershipError::InvalidProviderAdminChange(index));
            }
            let owner_recoveries = barriers
                .values()
                .filter_map(|barrier| match barrier {
                    MergeMembershipGrantRetirementBarrier::Owner { barrier } => {
                        Some(&barrier.recovery)
                    }
                    MergeMembershipGrantRetirementBarrier::NonOwner { .. } => None,
                })
                .collect::<Vec<_>>();
            match (owner_recoveries.is_empty(), retirement_device_state) {
                (true, None) => {}
                (false, Some(state))
                    if owner_recoveries
                        .iter()
                        .all(|cursor| state.recovery().binary_search(cursor).is_ok()) => {}
                (true, Some(_)) => return Err(MembershipError::UnexpectedOwnerRecoveryState),
                (false, None | Some(_)) => return Err(MembershipError::MissingOwnerRecoveryState),
            }
            if let Some((grant, _)) = barriers.iter().find(|(_, barrier)| {
                !barrier
                    .author_streams()
                    .observed_streams
                    .windows(2)
                    .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            }) {
                return Err(MembershipError::InvalidOwnerRevocationBarrier {
                    index,
                    grant: grant.clone(),
                });
            }
        }

        let founders = self
            .entries
            .iter()
            .filter_map(|entry| {
                let MembershipChange::Founder {
                    owner_pubkey,
                    owner_grant_id,
                    ..
                } = &entry.change
                else {
                    return None;
                };
                Some((entry, owner_pubkey, owner_grant_id))
            })
            .collect::<Vec<_>>();
        let [(founder, owner_pubkey, owner_grant_id)] = founders.as_slice() else {
            return Err(MembershipError::InvalidFounder);
        };
        if founder.author_pubkey != **owner_pubkey
            || founder.author_owner_grant != **owner_grant_id
            || founder.stream_id != derive_founder_stream_id(&founder.store_id, owner_pubkey)
            || founder.provider_admin.is_some()
        {
            return Err(MembershipError::InvalidFounder);
        }

        validate_provider_admin_controls(&self.entries, self.resolution_checkpoint.as_ref())?;
        validate_membership_retirement_barriers(
            &self.entries,
            self.resolution_checkpoint.as_ref(),
        )?;
        validate_membership_wrapped_keys(&self.entries, self.resolution_checkpoint.as_ref())?;

        let reduced = match &self.resolution_checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&self.entries, checkpoint)?,
            None => reduce_store_membership(&self.entries)?,
        };
        let checkpoint_grants = self
            .resolution_checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.grants);
        let provider_admin_seed = self
            .resolution_checkpoint
            .as_ref()
            .map_or(&self.provider_admin_genesis, |checkpoint| {
                &checkpoint.provider_admin
            });
        let (state_source, status) = match reduced {
            CausalGrantStatus::Resolved(reduced) => {
                let provider_admin = super::provider::ProviderAdminState::reduce_merge(
                    provider_admin_seed,
                    &self.entries,
                    &reduced.included,
                )?;
                let resolved = resolved_store_membership(
                    &reduced,
                    checkpoint_grants,
                    provider_admin,
                    &self.entries,
                )?;
                (Some(reduced), MembershipStatus::Resolved(resolved))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads,
                effective_frontier,
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            }) => {
                let heads = self.exact_head_refs(&raw_heads)?;
                let provider_admin = super::provider::ProviderAdminState::reduce_merge(
                    provider_admin_seed,
                    &self.entries,
                    &reduced.included,
                )?;
                let grants = reduced
                    .grants
                    .iter()
                    .map(|(grant, state)| {
                        Ok((
                            grant.clone(),
                            map_store_grant_state(grant, state, checkpoint_grants, &self.entries)?,
                        ))
                    })
                    .collect::<Result<_, MembershipError>>()?;
                let conflict = MembershipConflict::ConcurrentMemberAssignments {
                    conflict_hash: membership_assignment_conflict_hash(
                        &heads,
                        &member_pubkey,
                        &conflicting_grants,
                    ),
                    heads,
                    effective_frontier,
                    member_pubkey,
                    conflicting_grants: map_store_grants(conflicting_grants, checkpoint_grants)?,
                    uncontested_grants: map_store_grants(uncontested_grants, checkpoint_grants)?,
                    grants,
                    provider_admin,
                };
                (Some(reduced), MembershipStatus::Conflict(conflict))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::RevocationCycle {
                raw_heads,
                cyclic_sources,
                involved_owner_grants,
                maximal_valid_branches,
            }) => {
                let heads = self.exact_head_refs(&raw_heads)?;
                let branches = maximal_valid_branches
                    .into_iter()
                    .map(|branch| -> Result<StoreMembershipBranch, MembershipError> {
                        let resolved = resolved_store_membership(
                            &branch.reduced,
                            checkpoint_grants,
                            super::provider::ProviderAdminState::reduce_merge(
                                provider_admin_seed,
                                &self.entries,
                                &branch.reduced.included,
                            )?,
                            &self.entries,
                        )?;
                        Ok(StoreMembershipBranch {
                            heads: self.branch_head_refs(&branch.raw_heads)?,
                            effective_frontier: branch.effective_frontier,
                            grants: resolved.grants,
                            provider_admin: resolved.provider_admin,
                            state_hash: resolved.state_hash,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let conflict_hash = membership_revocation_conflict_hash(
                    &heads,
                    &cyclic_sources,
                    &involved_owner_grants,
                );
                (
                    None,
                    MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        cyclic_sources,
                        involved_owner_grants,
                        maximal_valid_branches: branches,
                    }),
                )
            }
        };
        if let Some(reduced) = state_source {
            self.state = CausalState {
                grants: reduced
                    .grants
                    .iter()
                    .map(|(grant, state)| {
                        Ok((
                            grant.clone(),
                            map_store_grant_state(grant, state, checkpoint_grants, &self.entries)?,
                        ))
                    })
                    .collect::<Result<_, MembershipError>>()?,
            };
            self.included = reduced.included;
        } else {
            self.state = CausalState::default();
            self.included.clear();
        }
        self.status = Some(status);
        Ok(())
    }

    pub(crate) fn apply_resolutions(
        &mut self,
        store_root_hash: ObjectHash,
        resolutions: &[(
            StoreMembershipConflictResolutionRef,
            StoreMembershipConflictResolution,
        )],
    ) -> Result<(), MembershipError> {
        let (raw_heads, effective_frontier) = match self.conflict() {
            Some(MembershipConflict::ConcurrentMemberAssignments {
                heads,
                effective_frontier,
                ..
            }) => (
                heads
                    .iter()
                    .map(|reference| reference.coord.clone())
                    .collect(),
                effective_frontier.clone(),
            ),
            Some(MembershipConflict::RevocationCycle {
                heads,
                maximal_valid_branches,
                ..
            }) => {
                let selected = resolutions
                    .iter()
                    .map(|(_, resolution)| {
                        let MembershipConflictSelection::RevocationBranch {
                            heads: selected_heads,
                        } = &resolution.selection
                        else {
                            return Err(MembershipError::InvalidConflictResolution);
                        };
                        maximal_valid_branches
                            .iter()
                            .find(|branch| branch.heads == *selected_heads)
                            .map(|branch| branch.effective_frontier.as_slice())
                            .ok_or(MembershipError::InvalidConflictResolution)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    heads
                        .iter()
                        .map(|reference| reference.coord.clone())
                        .collect(),
                    causal_grants::common_frontier(&selected),
                )
            }
            _ => return Err(MembershipError::InvalidConflictResolution),
        };
        let resolved = self.resolved_with(store_root_hash, resolutions)?;
        let grants = resolved.grants.clone();
        let mut grant_anchors = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(BTreeMap::new, |checkpoint| checkpoint.grant_anchors.clone());
        for entry in &self.entries {
            match &entry.change {
                MembershipChange::Founder {
                    owner_grant_id,
                    membership,
                    ..
                } => {
                    grant_anchors.insert(owner_grant_id.clone(), membership.clone());
                }
                MembershipChange::SetMember {
                    grant_id,
                    membership: Some(membership),
                    ..
                } => {
                    grant_anchors.insert(grant_id.clone(), membership.clone());
                }
                _ => {}
            }
        }
        for (_, resolution) in resolutions {
            grant_anchors.insert(
                resolution.replacement_grant.clone(),
                resolution.replacement_membership.clone(),
            );
        }
        let included = causal_grants::history_closure(&self.entries, &effective_frontier);
        let mut resolution_refs = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.resolutions.clone());
        resolution_refs.extend(resolutions.iter().map(|(reference, _)| reference.clone()));
        resolution_refs.sort();
        resolution_refs.dedup();
        self.resolution_checkpoint = Some(MembershipResolutionCheckpoint {
            raw_heads,
            effective_frontier: effective_frontier.clone(),
            grants: grants.clone(),
            grant_anchors,
            included: included.clone(),
            resolutions: resolution_refs,
            provider_admin: resolved.provider_admin.combined_state().clone(),
        });
        self.state = CausalState { grants };
        self.included = included;
        self.status = Some(MembershipStatus::Resolved(resolved));
        Ok(())
    }

    fn exact_head_refs(
        &self,
        raw_heads: &[MembershipCoord],
    ) -> Result<Vec<MembershipHeadRef>, MembershipError> {
        let expected = raw_heads.iter().cloned().collect::<BTreeSet<_>>();
        let mut references = self
            .head_refs
            .iter()
            .filter(|reference| expected.contains(&reference.coord))
            .cloned()
            .collect::<Vec<_>>();
        let actual = references
            .iter()
            .map(|reference| reference.coord.clone())
            .collect::<BTreeSet<_>>();
        if expected != actual || references.len() != expected.len() {
            return Err(MembershipError::MissingConflictHeads);
        }
        references.sort();
        Ok(references)
    }

    fn branch_head_refs(
        &self,
        branch_heads: &[MembershipCoord],
    ) -> Result<Vec<MembershipHeadRef>, MembershipError> {
        let by_coord = self
            .head_refs
            .iter()
            .map(|reference| (reference.coord.clone(), reference.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut references = branch_heads
            .iter()
            .map(|coord| {
                by_coord
                    .get(coord)
                    .cloned()
                    .ok_or(MembershipError::MissingConflictHeads)
            })
            .collect::<Result<Vec<_>, _>>()?;
        references.sort();
        Ok(references)
    }
}

fn validate_membership_retirement_barriers(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        if checkpoint.is_some_and(|checkpoint| {
            checkpoint.raw_heads.iter().any(|head| {
                head.stream_key() == entry.coord().stream_key() && entry.seq <= head.seq
            })
        }) {
            continue;
        }
        let (retired, barriers) = match &entry.change {
            MembershipChange::SetMember {
                replaces,
                retirement_barriers,
                ..
            } => (replaces, retirement_barriers),
            MembershipChange::RemoveMember {
                removes,
                retirement_barriers,
                ..
            } => (removes, retirement_barriers),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => continue,
        };
        if retired != &barriers.keys().cloned().collect::<BTreeSet<_>>() {
            let barrier_grants = barriers.keys().cloned().collect::<BTreeSet<_>>();
            let grant = retired
                .symmetric_difference(&barrier_grants)
                .next()
                .cloned()
                .expect("unequal retirement and barrier grant sets have a difference");
            return Err(MembershipError::InvalidOwnerRevocationBarrier { index, grant });
        }
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = match checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?,
            None => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::Conflict);
        };
        for (grant, barrier) in barriers {
            let Some(record) = reduced.grants.get(grant).and_then(GrantState::active) else {
                return Err(MembershipError::InvalidOwnerRevocationBarrier {
                    index,
                    grant: grant.clone(),
                });
            };
            let expected_streams = entry
                .dependencies
                .iter()
                .filter(|coord| coord.author_owner_grant == *grant)
                .cloned()
                .collect::<Vec<_>>();
            let shape_matches = matches!(
                (record.assignment.is_owner(), barrier),
                (true, MergeMembershipGrantRetirementBarrier::Owner { .. })
                    | (
                        false,
                        MergeMembershipGrantRetirementBarrier::NonOwner { .. }
                    )
            );
            if !shape_matches || barrier.author_streams().observed_streams != expected_streams {
                return Err(MembershipError::InvalidOwnerRevocationBarrier {
                    index,
                    grant: grant.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_membership_wrapped_keys(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_generation = membership_causal_generation(entries, &entry.dependencies);
        let references = match &entry.change {
            MembershipChange::SetMember {
                user_pubkey,
                wrapped_key,
                ..
            } => {
                if wrapped_key.owner_pubkey != entry.author_pubkey
                    || wrapped_key.recipient_pubkey != *user_pubkey
                    || wrapped_key.generation != causal_generation
                    || wrapped_key.validate_identity().is_err()
                {
                    return Err(MembershipError::InvalidWrappedKeys(index));
                }
                continue;
            }
            MembershipChange::RemoveMember {
                user_pubkey,
                wrapped_keys,
                ..
            } => (user_pubkey, wrapped_keys),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => continue,
        };
        let (removed_pubkey, wrapped_keys) = references;
        let rotation_generation = wrapped_keys.first().map(|reference| reference.generation);
        if causal_generation.checked_add(1) != rotation_generation
            || !wrapped_keys.windows(2).all(|pair| pair[0] < pair[1])
            || wrapped_keys.iter().any(|reference| {
                reference.owner_pubkey != entry.author_pubkey
                    || reference.recipient_pubkey == *removed_pubkey
                    || Some(reference.generation) != rotation_generation
                    || reference.validate_identity().is_err()
            })
        {
            return Err(MembershipError::InvalidWrappedKeys(index));
        }
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let precedes_checkpoint = checkpoint.is_some_and(|checkpoint| {
            checkpoint.raw_heads.iter().any(|head| {
                head.stream_key() == entry.coord().stream_key() && entry.seq <= head.seq
            })
        });
        let reduced = match (checkpoint, precedes_checkpoint) {
            (Some(checkpoint), false) => {
                reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?
            }
            (None, _) | (Some(_), true) => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::InvalidWrappedKeys(index));
        };
        let expected_recipients = reduced
            .grants
            .values()
            .filter_map(GrantState::active)
            .filter(|record| record.member_pubkey != *removed_pubkey)
            .map(|record| record.member_pubkey.clone())
            .collect::<BTreeSet<_>>();
        let actual_recipients = wrapped_keys
            .iter()
            .map(|reference| reference.recipient_pubkey.clone())
            .collect::<BTreeSet<_>>();
        if expected_recipients != actual_recipients || actual_recipients.len() != wrapped_keys.len()
        {
            return Err(MembershipError::InvalidWrappedKeys(index));
        }
    }
    Ok(())
}

fn membership_causal_generation(
    entries: &[MembershipEntry],
    dependencies: &[MembershipCoord],
) -> u64 {
    let included = causal_grants::history_closure(entries, dependencies);
    entries
        .iter()
        .filter(|candidate| included.contains(&candidate.coord()))
        .flat_map(|candidate| match &candidate.change {
            MembershipChange::SetMember { wrapped_key, .. } => std::slice::from_ref(wrapped_key),
            MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.as_slice(),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => &[],
        })
        .map(|reference| reference.generation)
        .max()
        .unwrap_or(crate::encryption::INITIAL_KEY_GENERATION)
}

fn reduce_store_membership(
    entries: &[MembershipEntry],
) -> Result<CausalGrantStatus<MembershipCoord, StoreAssignment>, MembershipError> {
    let normalized = normalize_store_membership(entries);
    causal_grants::reduce(&normalized).map_err(map_store_causal_error)
}

fn reduce_store_membership_from_checkpoint(
    entries: &[MembershipEntry],
    checkpoint: &MembershipResolutionCheckpoint,
) -> Result<CausalGrantStatus<MembershipCoord, StoreAssignment>, MembershipError> {
    let suffix = causal_grants::entries_beyond_checkpoint(entries, &checkpoint.raw_heads)
        .cloned()
        .collect::<Vec<_>>();
    let normalized = normalize_store_membership(&suffix);
    let seeds = causal_grants::checkpoint_seed_grants(&checkpoint.grants, |record| {
        causal_grants::CausalSeedGrant {
            member_pubkey: record.member_pubkey.clone(),
            assignment: StoreAssignment {
                role: record.role.clone(),
                provider_account_email: record.provider_account_email.clone(),
            },
        }
    });
    causal_grants::reduce_from_checkpoint(
        &normalized,
        &checkpoint.raw_heads,
        &checkpoint.effective_frontier,
        &seeds,
        &checkpoint.included,
    )
    .map_err(map_store_causal_error)
}

fn validate_provider_admin_controls(
    entries: &[MembershipEntry],
    checkpoint: Option<&MembershipResolutionCheckpoint>,
) -> Result<(), MembershipError> {
    for (index, entry) in entries.iter().enumerate() {
        let Some(super::provider::ProviderAdminMembershipChange { owner_barriers, .. }) =
            &entry.provider_admin
        else {
            continue;
        };
        let included = causal_grants::history_closure(entries, &entry.dependencies);
        let causal_past = entries
            .iter()
            .filter(|candidate| included.contains(&candidate.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = match checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?,
            None => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::InvalidProviderAdminChange(index));
        };
        let expected = reduced
            .grants
            .iter()
            .filter(|(_, state)| {
                state
                    .active()
                    .is_some_and(|record| record.assignment.is_owner())
            })
            .map(|(grant_id, _)| {
                let observed_streams = entry
                    .dependencies
                    .iter()
                    .filter(|coord| coord.author_owner_grant == *grant_id)
                    .cloned()
                    .collect();
                (grant_id.clone(), OwnerStreamBarrier { observed_streams })
            })
            .collect::<BTreeMap<_, _>>();
        if *owner_barriers != expected {
            return Err(MembershipError::InvalidProviderAdminChange(index));
        }
    }
    Ok(())
}

fn normalize_store_membership(
    entries: &[MembershipEntry],
) -> Vec<CausalEntry<MembershipCoord, StoreAssignment>> {
    entries
        .iter()
        .map(|entry| {
            let dependencies = entry
                .dependencies
                .iter()
                .cloned()
                .map(|coord| (coord.stream_key(), coord))
                .collect();
            let change = match &entry.change {
                MembershipChange::Founder {
                    creation_id,
                    owner_pubkey,
                    owner_grant_id,
                    ..
                } => CausalChange::Founder {
                    member_pubkey: owner_pubkey.clone(),
                    grant_id: owner_grant_id.clone(),
                    assignment: StoreAssignment {
                        role: StoreMembershipRoleGrant::Owner {
                            recovery: OwnerRecoveryAnchorRef::Founder {
                                creation_id: *creation_id,
                            },
                        },
                        provider_account_email: None,
                    },
                },
                MembershipChange::SetMember {
                    user_pubkey,
                    provider_account_email,
                    role,
                    grant_id,
                    membership: _,
                    replaces,
                    retirement_barriers,
                    ..
                } => CausalChange::SetMember {
                    member_pubkey: user_pubkey.clone(),
                    assignment: StoreAssignment {
                        role: role.clone(),
                        provider_account_email: provider_account_email.clone(),
                    },
                    grant_id: grant_id.clone(),
                    replaces: replaces.clone(),
                    owner_barriers: retirement_barriers
                        .iter()
                        .filter_map(|(grant, barrier)| {
                            barrier
                                .owner_stream_barrier()
                                .map(|barrier| (grant.clone(), barrier))
                        })
                        .collect(),
                },
                MembershipChange::RemoveMember {
                    user_pubkey,
                    removes,
                    retirement_barriers,
                    ..
                } => CausalChange::RemoveMember {
                    member_pubkey: user_pubkey.clone(),
                    removes: removes.clone(),
                    owner_barriers: retirement_barriers
                        .iter()
                        .filter_map(|(grant, barrier)| {
                            barrier
                                .owner_stream_barrier()
                                .map(|barrier| (grant.clone(), barrier))
                        })
                        .collect(),
                },
                MembershipChange::ProviderAdmin => CausalChange::Control,
                MembershipChange::ResolutionActivation { .. } => CausalChange::ResolutionActivation,
            };
            CausalEntry {
                coord: entry.coord(),
                previous_hash: entry.previous_hash,
                dependencies,
                change,
            }
        })
        .collect()
}

fn map_store_grants(
    grants: BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
    >,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
) -> Result<BTreeMap<MembershipGrantId, MembershipGrantRecord>, MembershipError> {
    grants
        .into_iter()
        .map(|(grant, record)| -> Result<_, MembershipError> {
            let creation_authority =
                membership_creation_authority(&grant, record.creation, checkpoint)?;
            Ok((
                grant,
                MembershipGrantRecord {
                    member_pubkey: record.member_pubkey,
                    role: record.assignment.role,
                    provider_account_email: record.assignment.provider_account_email,
                    creation_authority,
                },
            ))
        })
        .collect()
}

fn resolved_store_membership(
    reduced: &causal_grants::ReducedGrants<MembershipCoord, StoreAssignment>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
    provider_admin: super::provider::ProviderAdminResolution,
    entries: &[MembershipEntry],
) -> Result<ResolvedStoreMembership, MembershipError> {
    let grants = reduced
        .grants
        .iter()
        .map(|(grant, state)| -> Result<_, MembershipError> {
            Ok((
                grant.clone(),
                map_store_grant_state(grant, state, checkpoint, entries)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let state_hash = store_membership_state_hash(&grants, &provider_admin);
    Ok(ResolvedStoreMembership {
        grants,
        provider_admin,
        state_hash,
    })
}

fn map_store_grant_state(
    grant: &MembershipGrantId,
    state: &GrantState<
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
        causal_grants::CausalGrantRetirement<MembershipCoord>,
    >,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
    entries: &[MembershipEntry],
) -> Result<GrantState<MembershipGrantRecord, MembershipGrantRetirement>, MembershipError> {
    let causal_record = state.record();
    let record = MembershipGrantRecord {
        member_pubkey: causal_record.member_pubkey.clone(),
        role: causal_record.assignment.role.clone(),
        provider_account_email: causal_record.assignment.provider_account_email.clone(),
        creation_authority: membership_creation_authority(
            grant,
            causal_record.creation.clone(),
            checkpoint,
        )?,
    };
    causal_grants::try_map_grant_state(
        state,
        record,
        checkpoint
            .and_then(|grants| grants.get(grant))
            .and_then(GrantState::retirements),
        || MembershipError::MissingCheckpointRetirementEvidence {
            grant: grant.clone(),
        },
        |coord, _owner_barrier| {
            Ok(MembershipGrantRetirement::Entry {
                authority: coord.clone(),
                barrier: membership_retirement_barrier(entries, coord, grant).ok_or_else(|| {
                    MembershipError::MissingRetirementBarrier {
                        grant: grant.clone(),
                        authority: Box::new(coord.clone()),
                    }
                })?,
            })
        },
    )
}

fn membership_retirement_barrier(
    entries: &[MembershipEntry],
    authority: &MembershipCoord,
    grant: &MembershipGrantId,
) -> Option<MergeMembershipGrantRetirementBarrier> {
    let entry = entries.iter().find(|entry| entry.coord() == *authority)?;
    let barriers = match &entry.change {
        MembershipChange::SetMember {
            retirement_barriers,
            ..
        }
        | MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } => retirement_barriers,
        MembershipChange::Founder { .. }
        | MembershipChange::ProviderAdmin
        | MembershipChange::ResolutionActivation { .. } => return None,
    };
    barriers.get(grant).cloned()
}

fn membership_creation_authority(
    grant: &MembershipGrantId,
    creation: causal_grants::CausalGrantCreation<MembershipCoord>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<MembershipGrantRecord, MembershipGrantRetirement>>,
    >,
) -> Result<MembershipGrantCreationAuthority, MembershipError> {
    match creation {
        causal_grants::CausalGrantCreation::Entry(coord) => {
            Ok(MembershipGrantCreationAuthority::Entry(coord))
        }
        causal_grants::CausalGrantCreation::Checkpoint => checkpoint
            .and_then(|grants| grants.get(grant))
            .ok_or_else(|| MembershipError::MissingCheckpointGrant {
                grant: grant.clone(),
            })
            .map(|state| state.record().creation_authority.clone()),
    }
}

fn store_membership_state_hash(
    grants: &BTreeMap<
        MembershipGrantId,
        GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
    >,
    provider_admin: &super::provider::ProviderAdminResolution,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        grants: &'a BTreeMap<
            MembershipGrantId,
            GrantState<MembershipGrantRecord, MembershipGrantRetirement>,
        >,
        provider_admin: &'a super::provider::ProviderAdminResolution,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.store-membership-state.v2",
            grants,
            provider_admin,
        })
        .expect("Store membership state serialization cannot fail"),
    )
}

fn membership_assignment_conflict_hash(
    heads: &[MembershipHeadRef],
    member_pubkey: &str,
    conflicting_grants: &BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<MembershipCoord, StoreAssignment>,
    >,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        heads: &'a [MembershipHeadRef],
        member_pubkey: &'a str,
        conflicting_grant_ids: Vec<&'a MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.store-membership-assignment-conflict.v1",
            heads,
            member_pubkey,
            conflicting_grant_ids: conflicting_grants.keys().collect(),
        })
        .expect("Store membership conflict serialization cannot fail"),
    )
}

fn membership_revocation_conflict_hash(
    heads: &[MembershipHeadRef],
    cyclic_sources: &[MembershipCoord],
    involved_owner_grants: &BTreeSet<MembershipGrantId>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        heads: &'a [MembershipHeadRef],
        cyclic_sources: &'a [MembershipCoord],
        involved_owner_grants: &'a BTreeSet<MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.store-membership-revocation-conflict.v1",
            heads,
            cyclic_sources,
            involved_owner_grants,
        })
        .expect("Store membership revocation conflict serialization cannot fail"),
    )
}

fn shared_store_barrier(barrier: &StoreGrantStreamBarrier) -> OwnerGrantBarrier<MembershipCoord> {
    let observed_streams = barrier
        .observed_streams
        .iter()
        .cloned()
        .map(|coord| (coord.stream_key(), coord))
        .collect();
    OwnerGrantBarrier { observed_streams }
}

fn map_store_causal_error(error: CausalGrantError<MembershipCoord>) -> MembershipError {
    match error {
        CausalGrantError::Empty => MembershipError::EmptyChain,
        CausalGrantError::ConflictingSequence { stream, seq } => {
            MembershipError::ConflictingSequence {
                author: stream.author_pubkey,
                grant: stream.author_owner_grant,
                seq,
            }
        }
        CausalGrantError::MissingSequence { stream, seq } => MembershipError::MissingSequence {
            author: stream.author_pubkey,
            grant: stream.author_owner_grant,
            seq,
        },
        CausalGrantError::BrokenStreamLink {
            index,
            expected,
            actual,
        } => MembershipError::BrokenStreamLink {
            index,
            expected,
            actual,
        },
        CausalGrantError::MissingOwnDependency { index } => {
            MembershipError::MissingOwnDependency { index }
        }
        CausalGrantError::DependencyStreamMismatch { .. } => {
            unreachable!("Store dependencies are normalized from their signed coordinates")
        }
        CausalGrantError::MissingDependency { index, dependency } => {
            MembershipError::MissingDependency {
                index,
                dependency: Box::new(dependency),
            }
        }
        CausalGrantError::DependencyCycle => MembershipError::DependencyCycle,
        CausalGrantError::InvalidFounder => MembershipError::InvalidFounder,
        CausalGrantError::AuthorGrantInactive { index, grant } => {
            MembershipError::AuthorGrantInactive { index, grant }
        }
        CausalGrantError::DuplicateGrant { index, grant } => {
            MembershipError::DuplicateGrant { index, grant }
        }
        CausalGrantError::GrantOwnerMismatch { index, grant } => {
            MembershipError::GrantOwnerMismatch { index, grant }
        }
        CausalGrantError::GrantSetMismatch {
            index,
            member_pubkey,
        } => MembershipError::GrantSetMismatch {
            index,
            pubkey: member_pubkey,
        },
        CausalGrantError::EmptyRemoval { index } => MembershipError::EmptyRemoval { index },
        CausalGrantError::MissingOwnerRevocationBarrier { index, grant } => {
            MembershipError::MissingOwnerRevocationBarrier { index, grant }
        }
        CausalGrantError::InvalidOwnerRevocationBarrier { index, grant } => {
            MembershipError::InvalidOwnerRevocationBarrier { index, grant }
        }
        CausalGrantError::NoActiveOwner => MembershipError::NoActiveOwner,
        CausalGrantError::RevocationCycleTooWide { sources, maximum } => {
            MembershipError::RevocationCycleTooWide { sources, maximum }
        }
    }
}

pub(crate) fn derive_founder_stream_id(store_id: &str, owner_pubkey: &str) -> AuthorStreamId {
    AuthorStreamId::from_digest(ObjectHash::digest(
        format!("coven.membership-founder-stream.v1\0{store_id}\0{owner_pubkey}").as_bytes(),
    ))
}

fn store_membership_anchor_stream(
    owner_pubkey: &str,
    owner_grant: &MembershipGrantId,
    anchor: &GrantStreamAnchor,
) -> Option<AuthorStreamId> {
    let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
        return None;
    };
    let prefix = format!(
        "{}{owner_pubkey}/{owner_grant}/",
        super::store_commit::STORE_MEMBERSHIP_HEAD_PREFIX,
    );
    first_slot
        .logical_key()
        .strip_prefix(&prefix)?
        .strip_suffix("/1.json")?
        .parse()
        .ok()
}

pub(crate) fn derive_grant_id(
    store_id: &str,
    author_pubkey: &str,
    author_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
    user_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!(
            "coven.membership-grant.v1\0{store_id}\0{author_pubkey}\0{author_grant}\0{stream_id}\0{seq}\0{user_pubkey}"
        )
        .as_bytes(),
    ))
}

pub(crate) fn founder_entry_for_creation(
    store_id: &str,
    creation_id: StoreCreationId,
    owner: &UserKeypair,
    owner_grant_id: MembershipGrantId,
    created_at: &str,
    membership: GrantStreamAnchor,
    provider_admin: super::provider::FounderProviderAdminGrant,
) -> MembershipEntry {
    let owner_pubkey = keys::public_key_hex(owner);
    let stream_id = derive_founder_stream_id(store_id, &owner_pubkey);
    let mut entry = MembershipEntry {
        version: STORE_PROTOCOL_VERSION,
        store_id: store_id.to_string(),
        author_pubkey: owner_pubkey.clone(),
        author_owner_grant: owner_grant_id.clone(),
        stream_id,
        seq: 1,
        previous_hash: None,
        dependencies: Vec::new(),
        resolution_dependencies: Vec::new(),
        created_at: created_at.to_string(),
        change: MembershipChange::Founder {
            creation_id,
            owner_pubkey,
            owner_grant_id,
            membership,
            provider_admin,
        },
        provider_admin: None,
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner);
    entry
}

#[cfg(test)]
pub(crate) fn founder_entry(
    store_id: &str,
    owner: &UserKeypair,
    owner_grant_id: MembershipGrantId,
    created_at: &str,
    membership: GrantStreamAnchor,
    provider_admin: super::provider::FounderProviderAdminGrant,
) -> MembershipEntry {
    founder_entry_for_creation(
        store_id,
        StoreCreationId::from_nonce(store_id),
        owner,
        owner_grant_id,
        created_at,
        membership,
        provider_admin,
    )
}

pub(crate) fn canonical_bytes(entry: &MembershipEntry) -> Vec<u8> {
    #[derive(Serialize)]
    struct Signed<'a> {
        version: u32,
        store_id: &'a str,
        author_pubkey: &'a str,
        author_owner_grant: &'a MembershipGrantId,
        stream_id: AuthorStreamId,
        seq: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        previous_hash: Option<ObjectHash>,
        dependencies: &'a [MembershipCoord],
        resolution_dependencies: &'a [StoreMembershipConflictResolutionRef],
        created_at: &'a str,
        change: &'a MembershipChange,
        #[serde(skip_serializing_if = "Option::is_none")]
        provider_admin: Option<&'a super::provider::ProviderAdminMembershipChange>,
    }
    serde_json::to_vec(&Signed {
        version: entry.version,
        store_id: &entry.store_id,
        author_pubkey: &entry.author_pubkey,
        author_owner_grant: &entry.author_owner_grant,
        stream_id: entry.stream_id,
        seq: entry.seq,
        previous_hash: entry.previous_hash,
        dependencies: &entry.dependencies,
        resolution_dependencies: &entry.resolution_dependencies,
        created_at: &entry.created_at,
        change: &entry.change,
        provider_admin: entry.provider_admin.as_ref(),
    })
    .expect("membership signed fields serialize")
}

pub(crate) fn entry_hash(entry: &MembershipEntry) -> ObjectHash {
    ObjectHash::digest(
        &serde_json::to_vec(entry).expect("membership entry serialization cannot fail"),
    )
}

pub(crate) fn sign_membership_entry(entry: &mut MembershipEntry, keypair: &UserKeypair) {
    entry.author_pubkey = keys::public_key_hex(keypair);
    let (_, signature) = keys::sign_hex(keypair, &canonical_bytes(entry));
    entry.signature = signature;
}

pub(crate) fn verify_membership_entry(entry: &MembershipEntry) -> bool {
    let activation_position_is_valid = match &entry.change {
        MembershipChange::ResolutionActivation { .. } => {
            entry.seq == 1
                && entry.previous_hash.is_none()
                && entry
                    .dependencies
                    .iter()
                    .all(|dependency| dependency.stream_key() != entry.coord().stream_key())
        }
        _ => true,
    };
    activation_position_is_valid
        && entry
            .resolution_dependencies
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        && keys::verify_signature_hex(
            &entry.author_pubkey,
            &entry.signature,
            &canonical_bytes(entry),
        )
}

impl AuthorHead {
    pub fn signed(
        store_id: String,
        mut body: MembershipHeadBody,
        activation: MembershipHeadActivation,
        device_signer: &UserKeypair,
    ) -> Self {
        body.resolutions.sort();
        body.resolutions.dedup();
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_id,
            body,
            activation,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &head.canonical_bytes());
        head.signature = signature;
        head
    }

    pub fn verify(&self, registration: &StoreDeviceRegistration) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self
                .body
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
            && keys::verify_signature_hex(
                &registration.device_signing_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub fn entry_coord(&self) -> MembershipCoord {
        self.body.entry.coord.clone()
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("membership head serialization cannot fail"),
        )
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            version: u32,
            store_id: &'a str,
            body: &'a MembershipHeadBody,
            activation: &'a MembershipHeadActivation,
        }
        serde_json::to_vec(&Signed {
            version: self.version,
            store_id: &self.store_id,
            body: &self.body,
            activation: &self.activation,
        })
        .expect("membership head signed fields serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::store_commit::{
        membership_entry_semantic_prefix, membership_head_semantic_prefix,
        membership_resolution_semantic_prefix, registration_semantic_prefix, CommitFrontier,
        DeviceStreamAnchor, GrantStreamAnchor, ResolvedStoreDeviceState, StoreCreationId,
        StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceStateRef,
        StoreRootRef, StreamActivation,
    };
    use crate::storage::cloud::ObjectSlot;
    use crate::storage::{ProviderDeviceBinding, ProviderPrincipalId};

    fn key() -> UserKeypair {
        UserKeypair::generate()
    }

    fn stream(byte: u8) -> AuthorStreamId {
        AuthorStreamId::from_bytes([byte; 32])
    }

    fn slot(key: impl Into<String>) -> ObjectSlot {
        ObjectSlot::logical(key.into()).expect("valid test object slot")
    }

    fn exact(key: impl Into<String>, bytes: &[u8]) -> ExactObjectRef {
        ExactObjectRef::new(slot(key), bytes.len() as u64, ObjectHash::digest(bytes))
    }

    fn membership_anchor(store_id: &str) -> GrantStreamAnchor {
        GrantStreamAnchor::StoreMembership {
            first_slot: slot(format!("test/{store_id}/membership/1.json")),
        }
    }

    fn recovery_anchor(store_id: &str) -> GrantStreamAnchor {
        GrantStreamAnchor::OwnerRecovery {
            first_slot: slot(format!("test/{store_id}/recovery/1.json")),
        }
    }

    fn test_founder_entry(
        store_id: &str,
        owner: &UserKeypair,
        created_at: &str,
        membership: GrantStreamAnchor,
    ) -> MembershipEntry {
        founder_entry(
            store_id,
            owner,
            crate::protocol::causal_grants::MembershipGrantId::from_test_label(store_id),
            created_at,
            membership,
            crate::protocol::provider::FounderProviderAdminGrant::from_test_label(store_id),
        )
    }

    fn test_root(store_id: &str) -> StoreRootRef {
        let bytes = store_id.as_bytes();
        StoreRootRef {
            store_root_id: ObjectHash::digest(format!("{store_id} identity").as_bytes()),
            store_root_hash: ObjectHash::digest(bytes),
            object: exact(format!("test/{store_id}/root.json"), bytes),
        }
    }

    fn registration(
        root: &StoreRootRef,
        label: &str,
        signer: &UserKeypair,
    ) -> (StoreDeviceRegistration, StoreDeviceRegistrationRef) {
        let registration = StoreDeviceRegistration::signed(
            root.clone(),
            StoreDeviceRegistrationOrigin::Founder {
                creation_id: StoreCreationId::from_nonce(label),
            },
            ProviderDeviceBinding {
                principal: ProviderPrincipalId::CustomS3Credential {
                    access_key_id_hash: ObjectHash::digest(label.as_bytes()),
                },
            },
            DeviceStreamAnchor::StoreAnnouncements {
                first_slot: slot(format!("test/{label}/announcements/1.json")),
            },
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: slot(format!("test/{label}/acks/1.json")),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: slot(format!("test/{label}/snapshots/1.json")),
            },
            signer,
        )
        .expect("sign test registration");
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

    fn conflict_acceptance(
        chain: &MembershipChain,
        store_root_hash: ObjectHash,
        membership: GrantStreamAnchor,
        signer: &UserKeypair,
    ) -> OwnerConflictResolutionAcceptance {
        let (conflict_hash, owner_grants) = match chain
            .conflict()
            .expect("test chain has a membership conflict")
        {
            MembershipConflict::ConcurrentMemberAssignments {
                conflict_hash,
                grants,
                ..
            } => (
                conflict_hash,
                grants
                    .iter()
                    .filter_map(|(grant, state)| {
                        state
                            .active()
                            .filter(|record| record.role.is_owner())
                            .map(|record| (grant.clone(), record.clone()))
                    })
                    .collect::<Vec<_>>(),
            ),
            MembershipConflict::RevocationCycle {
                conflict_hash,
                maximal_valid_branches,
                ..
            } => (
                conflict_hash,
                maximal_valid_branches
                    .iter()
                    .flat_map(StoreMembershipBranch::active_grants)
                    .filter(|(_, record)| record.role.is_owner())
                    .map(|(grant, record)| (grant.clone(), record.clone()))
                    .collect::<Vec<_>>(),
            ),
        };
        let resolver_pubkey = keys::public_key_hex(signer);
        let root = StoreRootRef {
            store_root_id: ObjectHash::digest(b"test conflict-resolution root id"),
            store_root_hash,
            object: exact(
                "test/conflict-resolution/root.json",
                b"conflict resolution root",
            ),
        };
        let (registration, owner_registration) = registration(
            &root,
            &format!("conflict-resolution-{resolver_pubkey}"),
            signer,
        );
        let mut recovery = owner_grants
            .into_iter()
            .map(|(grant, record)| OwnerRecoveryCursor {
                owner_grant: grant.clone(),
                position: OwnerRecoveryPosition::At {
                    node: OwnerRecoveryNodeRef {
                        owner_pubkey: record.member_pubkey,
                        owner_grant: grant.clone(),
                        sequence: 1,
                        node_hash: ObjectHash::digest(
                            format!("conflict recovery {grant}").as_bytes(),
                        ),
                        object: exact(
                            format!("test/conflict-recovery/{grant}/1.json"),
                            format!("conflict recovery {grant}").as_bytes(),
                        ),
                    },
                },
            })
            .collect::<Vec<_>>();
        recovery.sort();
        recovery.dedup_by(|left, right| left.owner_grant == right.owner_grant);
        OwnerConflictResolutionAcceptance {
            store_root_hash,
            owner_grant: derive_store_resolution_grant(conflict_hash, &resolver_pubkey),
            owner_registration,
            provider: registration.provider,
            membership,
            recovery: recovery_anchor(&format!("conflict-resolution-{resolver_pubkey}")),
            device_state: StoreDeviceStateRef::from_resolved(
                CommitFrontier(BTreeMap::new()),
                &ResolvedStoreDeviceState {
                    devices: BTreeMap::new(),
                    recovery,
                    state_hash: ObjectHash::digest(b"test conflict-resolution device state"),
                },
            )
            .expect("construct conflict-resolution device state"),
            signature: String::new(),
        }
    }

    fn exact_head(
        entry: &MembershipEntry,
        signer: &UserKeypair,
    ) -> (MembershipHeadRef, AuthorHead) {
        exact_head_with_resolutions(entry, signer, entry.resolution_dependencies.clone())
    }

    fn exact_head_with_resolutions(
        entry: &MembershipEntry,
        signer: &UserKeypair,
        resolutions: Vec<StoreMembershipConflictResolutionRef>,
    ) -> (MembershipHeadRef, AuthorHead) {
        let root = test_root(&entry.store_id);
        let (registration, registration_ref) = registration(
            &root,
            &format!("{}-{}", entry.store_id, entry.author_pubkey),
            signer,
        );
        let entry_bytes = serde_json::to_vec(entry).expect("serialize membership entry");
        let coord = entry.coord();
        let entry_ref = MembershipEntryRef {
            coord: coord.clone(),
            object: exact(
                format!(
                    "{}.json",
                    membership_entry_semantic_prefix(
                        &coord.author_pubkey,
                        &coord.author_owner_grant,
                        coord.stream_id,
                        coord.seq,
                        coord.entry_hash,
                    )
                ),
                &entry_bytes,
            ),
        };
        let anchor = membership_anchor(&entry.store_id);
        let successor = SuccessorLink {
            activation: StreamActivation::grant_authorized(
                root.store_root_hash,
                registration_ref.clone(),
                entry.author_owner_grant.clone(),
                anchor,
            )
            .activation_id(),
            predecessor: None,
            next_slot: slot(format!(
                "test/{}/membership-heads/{}/next.json",
                entry.store_id, coord.entry_hash
            )),
        };
        let device_signer = registration.device_signer(signer).unwrap();
        let head = AuthorHead::signed(
            entry.store_id.clone(),
            MembershipHeadBody {
                author_registration: registration_ref,
                entry: entry_ref,
                predecessor: None,
                resolutions,
                successor,
            },
            MembershipHeadActivation::Direct,
            &device_signer,
        );
        let head_bytes = serde_json::to_vec(&head).expect("serialize membership head");
        let reference = MembershipHeadRef {
            coord: coord.clone(),
            head_hash: head.head_hash(),
            object: exact(
                format!(
                    "{}.json",
                    membership_head_semantic_prefix(
                        &coord.author_pubkey,
                        &coord.author_owner_grant,
                        coord.stream_id,
                        coord.seq,
                        head.head_hash(),
                    )
                ),
                &head_bytes,
            ),
        };
        (reference, head)
    }

    fn exact_resolution(
        resolution: StoreMembershipConflictResolution,
    ) -> (
        StoreMembershipConflictResolutionRef,
        StoreMembershipConflictResolution,
    ) {
        let bytes = serde_json::to_vec(&resolution).expect("serialize membership resolution");
        let reference = resolution.resolution_ref(exact(
            format!(
                "{}.json",
                membership_resolution_semantic_prefix(
                    resolution.conflict_hash,
                    &resolution.resolver_pubkey,
                    resolution.resolution_hash(),
                )
            ),
            &bytes,
        ));
        (reference, resolution)
    }

    fn founded(store_id: &str, owner: &UserKeypair) -> MembershipChain {
        MembershipChain::from_entries(vec![test_founder_entry(
            store_id,
            owner,
            "founder",
            membership_anchor(store_id),
        )])
        .unwrap()
    }

    #[test]
    fn membership_head_requires_an_explicit_activation_rule() {
        let owner = key();
        let entry = test_founder_entry(
            "required-head-activation",
            &owner,
            "founder",
            membership_anchor("required-head-activation"),
        );
        let (_, head) = exact_head(&entry, &owner);
        let mut encoded = serde_json::to_value(head).expect("serialize membership head");
        encoded
            .as_object_mut()
            .expect("membership head object")
            .remove("activation");
        assert!(serde_json::from_value::<AuthorHead>(encoded).is_err());
    }

    #[test]
    fn reserved_membership_transition_and_published_head_share_one_body() {
        let owner = key();
        let entry = test_founder_entry(
            "shared-head-body",
            &owner,
            "founder",
            membership_anchor("shared-head-body"),
        );
        let (reference, head) = exact_head(&entry, &owner);
        let transition = MergeMembershipHeadTransition {
            body: head.body.clone(),
            head_slot: reference.object.slot().clone(),
        };
        let encoded = serde_json::to_vec(&transition).expect("serialize reserved transition");
        let decoded: MergeMembershipHeadTransition =
            serde_json::from_slice(&encoded).expect("parse reserved transition");
        assert_eq!(decoded, transition);
        assert!(decoded.matches_head(&head, &reference));

        let mut mismatched = decoded;
        mismatched.body.successor.next_slot = slot("test/shared-head-body/another-next.json");
        assert!(!mismatched.matches_head(&head, &reference));
    }

    #[test]
    fn merge_active_grant_lookup_returns_only_the_exact_live_record() {
        let owner = key();
        let member = key();
        let member_pubkey = keys::public_key_hex(&member);
        let mut chain = founded("exact-live-merge-grant", &owner);
        let addition = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                member_pubkey.clone(),
                None,
                MemberRole::Member,
                "add member".to_string(),
            )
            .unwrap();
        let MembershipChange::SetMember { grant_id, .. } = &addition.change else {
            unreachable!()
        };
        let grant_id = grant_id.clone();
        chain.add_entry(addition).unwrap();
        let MembershipStatus::Resolved(resolved) = chain.status() else {
            panic!("membership must resolve")
        };
        assert_eq!(
            chain.active_grant(&grant_id),
            resolved.active_grant(&grant_id)
        );
        assert!(chain
            .active_grant(&MembershipGrantId(ObjectHash::digest(b"absent grant")))
            .is_none());

        let removal = chain
            .signed_remove_member_in_stream(
                &owner,
                stream(1),
                member_pubkey.clone(),
                "remove member".to_string(),
            )
            .unwrap();
        let retirement_authority = removal.coord();
        chain.add_entry(removal).unwrap();
        assert!(chain.active_grant(&grant_id).is_none());
        let MembershipStatus::Resolved(resolved) = chain.status() else {
            panic!("membership must resolve")
        };
        assert!(matches!(
            &resolved.grants[&grant_id],
            GrantState::Tombstoned { record, retirements }
                if record.member_pubkey == member_pubkey
                    && retirements.as_set() == &BTreeSet::from([MembershipGrantRetirement::Entry {
                        authority: retirement_authority.clone(),
                        barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
                            author_streams: StoreGrantStreamBarrier {
                                observed_streams: Vec::new(),
                            },
                        },
                    }])
        ));
        let mut altered = resolved.grants.clone();
        let GrantState::Tombstoned { retirements, .. } = altered
            .get_mut(&grant_id)
            .expect("retired Merge grant remains present")
        else {
            unreachable!()
        };
        retirements.insert(MembershipGrantRetirement::Entry {
            authority: MembershipCoord {
                entry_hash: ObjectHash::digest(b"different retirement entry"),
                ..retirement_authority.clone()
            },
            barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
                author_streams: StoreGrantStreamBarrier {
                    observed_streams: Vec::new(),
                },
            },
        });
        assert_ne!(
            resolved.state_hash,
            store_membership_state_hash(&altered, &resolved.provider_admin)
        );

        let mut reuse = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                member_pubkey,
                None,
                MemberRole::Member,
                "reuse retired grant".to_string(),
            )
            .unwrap();
        let MembershipChange::SetMember {
            grant_id: candidate,
            ..
        } = &mut reuse.change
        else {
            unreachable!()
        };
        *candidate = grant_id.clone();
        sign_membership_entry(&mut reuse, &owner);
        assert!(matches!(
            chain.add_entry(reuse),
            Err(MembershipError::DuplicateGrant {
                grant,
                ..
            }) if grant == grant_id
        ));
    }

    #[test]
    fn grant_mapping_returns_an_error_when_signed_retirement_evidence_is_absent() {
        let owner = key();
        let founder = test_founder_entry(
            "missing-retirement-evidence",
            &owner,
            "founder",
            membership_anchor("missing-retirement-evidence"),
        );
        let MembershipChange::Founder { owner_grant_id, .. } = &founder.change else {
            panic!("test entry is the founder")
        };
        let owner_grant_id = owner_grant_id.clone();
        let authority = MembershipCoord {
            author_pubkey: keys::public_key_hex(&owner),
            author_owner_grant: owner_grant_id.clone(),
            stream_id: stream(77),
            seq: 1,
            entry_hash: ObjectHash::digest(b"missing retirement authority"),
        };
        let state = GrantState::Tombstoned {
            record: causal_grants::GrantRecord {
                member_pubkey: keys::public_key_hex(&owner),
                assignment: StoreAssignment {
                    role: StoreMembershipRoleGrant::Member,
                    provider_account_email: None,
                },
                creation: causal_grants::CausalGrantCreation::Entry(founder.coord()),
            },
            retirements: GrantRetirements::new(causal_grants::CausalGrantRetirement::Entry {
                coord: authority.clone(),
                owner_barrier: None,
            }),
        };

        assert!(matches!(
            map_store_grant_state(&owner_grant_id, &state, None, &[founder]),
            Err(MembershipError::MissingRetirementBarrier {
                grant,
                authority: missing,
            }) if grant == owner_grant_id && *missing == authority
        ));
    }

    #[test]
    fn concurrent_effective_removals_union_exact_retirement_entries() {
        let first_owner = key();
        let second_owner = key();
        let member = key();
        let member_pubkey = keys::public_key_hex(&member);
        let mut base = founded("concurrent-retirement-evidence", &first_owner);
        base.add_owner_for_test(
            &first_owner,
            stream(1),
            keys::public_key_hex(&second_owner),
            "add second Owner".to_string(),
        )
        .unwrap();
        let add_member = base
            .signed_set_member_in_stream(
                &first_owner,
                stream(1),
                member_pubkey.clone(),
                None,
                MemberRole::Member,
                "add member".to_string(),
            )
            .unwrap();
        let member_grant = match &add_member.change {
            MembershipChange::SetMember { grant_id, .. } => grant_id.clone(),
            _ => unreachable!(),
        };
        base.add_entry(add_member).unwrap();

        let first_removal = base
            .signed_remove_member_in_stream(
                &first_owner,
                stream(1),
                member_pubkey.clone(),
                "first removal".to_string(),
            )
            .unwrap();
        let second_removal = base
            .signed_remove_member_in_stream(
                &second_owner,
                stream(2),
                member_pubkey,
                "second removal".to_string(),
            )
            .unwrap();
        let expected = GrantRetirements::new(MembershipGrantRetirement::Entry {
            authority: first_removal.coord(),
            barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
                author_streams: StoreGrantStreamBarrier {
                    observed_streams: Vec::new(),
                },
            },
        });
        let mut expected = expected;
        expected.insert(MembershipGrantRetirement::Entry {
            authority: second_removal.coord(),
            barrier: MergeMembershipGrantRetirementBarrier::NonOwner {
                author_streams: StoreGrantStreamBarrier {
                    observed_streams: Vec::new(),
                },
            },
        });
        let mut entries = base.entries().to_vec();
        entries.extend([first_removal, second_removal]);
        let chain = MembershipChain::from_entries(entries).unwrap();
        let MembershipStatus::Resolved(resolved) = chain.status() else {
            panic!("concurrent non-Owner removals must resolve")
        };

        assert!(matches!(
            &resolved.grants[&member_grant],
            GrantState::Tombstoned { retirements, .. }
                if retirements.as_set() == expected.as_set()
        ));
    }

    fn three_owner_store_cycle() -> (UserKeypair, UserKeypair, UserKeypair, MembershipChain) {
        let first = key();
        let second = key();
        let third = key();
        let first_pubkey = keys::public_key_hex(&first);
        let second_pubkey = keys::public_key_hex(&second);
        let third_pubkey = keys::public_key_hex(&third);
        let mut base = founded("three-owner-store", &first);
        base.add_owner_for_test(
            &first,
            stream(1),
            second_pubkey.clone(),
            "add second Owner".to_string(),
        )
        .expect("add second Owner");
        base.add_owner_for_test(
            &first,
            stream(1),
            third_pubkey,
            "add third Owner".to_string(),
        )
        .expect("add third Owner");
        let remove_second = base
            .signed_remove_member_in_stream(
                &first,
                stream(1),
                second_pubkey,
                "first branch".to_string(),
            )
            .expect("first branch");
        let remove_first = base
            .signed_remove_member_in_stream(
                &second,
                stream(92),
                first_pubkey,
                "second branch".to_string(),
            )
            .expect("second branch");
        let mut entries = base.entries().to_vec();
        entries.extend([remove_second.clone(), remove_first.clone()]);
        let heads = vec![
            exact_head(
                base.entries().first().expect("founder membership entry"),
                &first,
            ),
            exact_head(&remove_second, &first),
            exact_head(&remove_first, &second),
        ];
        let conflict = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        )
        .expect("three-Owner Store conflict");
        (first, second, third, conflict)
    }

    #[test]
    fn unaffected_store_owner_resolution_retires_its_selected_branch_grant() {
        let (_first, _second, third, conflicted) = three_owner_store_cycle();
        let third_pubkey = keys::public_key_hex(&third);
        let (branch, old_grant) = match conflicted.conflict().expect("conflict") {
            MembershipConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            } => {
                let branch = maximal_valid_branches
                    .iter()
                    .find(|branch| {
                        branch.active_grants().any(|(_, record)| {
                            record.member_pubkey == third_pubkey && record.role.is_owner()
                        })
                    })
                    .expect("unaffected Owner branch");
                let old_grant = branch
                    .active_grants()
                    .find_map(|(grant, record)| {
                        (record.member_pubkey == third_pubkey).then_some(grant.clone())
                    })
                    .expect("unaffected Owner grant");
                (branch.heads.clone(), old_grant)
            }
            _ => panic!("expected revocation conflict"),
        };
        let store_root_hash = ObjectHash::digest(b"unaffected Store resolver root");
        let replacement_membership = membership_anchor("unaffected-store-resolver");
        let acceptance = conflict_acceptance(
            &conflicted,
            store_root_hash,
            replacement_membership.clone(),
            &third,
        );
        let resolution = conflicted
            .signed_conflict_resolution(
                store_root_hash,
                MembershipConflictSelection::RevocationBranch { heads: branch },
                replacement_membership,
                acceptance,
                &third,
            )
            .expect("unaffected Owner resolution");
        let resolution = exact_resolution(resolution);
        let resolved = conflicted
            .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
            .expect("unaffected Owner resolution is valid");

        assert!(resolution.1.retired_owner_grants.contains(&old_grant));
        assert!(resolved.grants[&old_grant].active().is_none());
        assert!(resolved
            .grants
            .get(&resolution.1.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
        assert!(matches!(
            &resolved.grants[&old_grant],
            GrantState::Tombstoned { retirements, .. }
                if retirements.iter().any(|retirement| matches!(
                    retirement,
                    MembershipGrantRetirement::ConflictResolution { authority, .. }
                        if authority == &resolution.0
                ))
        ));
    }

    #[test]
    fn store_revocation_cycle_over_protocol_bound_is_typed() {
        let owners = (0..13).map(|_| key()).collect::<Vec<_>>();
        let pubkeys = owners.iter().map(keys::public_key_hex).collect::<Vec<_>>();
        let mut base = founded("bounded-store-cycle", &owners[0]);
        for pubkey in pubkeys.iter().skip(1) {
            base.add_owner_for_test(
                &owners[0],
                stream(1),
                pubkey.clone(),
                format!("add {pubkey}"),
            )
            .expect("add ring Owner");
        }
        let removals = owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                base.signed_remove_member_in_stream(
                    owner,
                    stream(index as u8 + 101),
                    pubkeys[(index + 1) % pubkeys.len()].clone(),
                    format!("remove ring successor {index}"),
                )
                .expect("sign ring removal")
            })
            .collect::<Vec<_>>();
        let mut entries = base.entries().to_vec();
        entries.extend(removals.iter().cloned());
        let heads = removals
            .iter()
            .zip(&owners)
            .map(|(entry, owner)| exact_head(entry, owner))
            .collect();

        assert!(matches!(
            MembershipChain::from_entries_with_coords_and_heads(
                entries
                    .into_iter()
                    .map(|entry| (entry.coord(), entry))
                    .collect(),
                heads,
            ),
            Err(MembershipError::RevocationCycleTooWide {
                sources: 13,
                maximum: 12,
            })
        ));
    }

    #[test]
    fn timestamp_does_not_change_causal_authorization() {
        let owner = key();
        let member = key();
        let mut chain = founded("store", &owner);
        let add = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&member),
                None,
                MemberRole::Member,
                "9999".to_string(),
            )
            .unwrap();
        chain.add_entry(add).unwrap();
        let remove = chain
            .signed_remove_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&member),
                "0000".to_string(),
            )
            .unwrap();
        chain.add_entry(remove).unwrap();
        assert!(!chain.can_write_now(&keys::public_key_hex(&member)));
    }

    #[test]
    fn signed_candidate_is_validated_before_it_is_returned() {
        let owner = key();
        let chain = founded("store", &owner);

        assert!(matches!(
            chain.signed_remove_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&owner),
                "remove last owner".to_string(),
            ),
            Err(MembershipError::NoActiveOwner)
        ));
    }

    #[test]
    fn direct_owner_assignment_is_rejected() {
        let founder = key();
        let candidate = key();
        let chain = founded("owner-promotion-required", &founder);
        let candidate_pubkey = keys::public_key_hex(&candidate);

        assert!(matches!(
            chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
                &founder,
                stream(1),
                candidate_pubkey.clone(),
                None,
                MemberRole::Owner,
                Some(membership_anchor("direct-owner-assignment")),
                test_wrapped_key_ref(
                    &keys::public_key_hex(&founder),
                    &candidate_pubkey,
                    crate::encryption::INITIAL_KEY_GENERATION,
                    b"direct Owner assignment",
                ),
                "direct Owner assignment".to_string(),
            ),
            Err(MembershipError::OwnerPromotionRequired)
        ));
    }

    #[test]
    fn membership_candidates_require_exact_wrapped_key_recipient_coverage() {
        let owner = key();
        let member = key();
        let owner_pubkey = keys::public_key_hex(&owner);
        let member_pubkey = keys::public_key_hex(&member);
        let mut chain = founded("store", &owner);
        let wrong_recipient = test_wrapped_key_ref(
            &owner_pubkey,
            &owner_pubkey,
            crate::encryption::INITIAL_KEY_GENERATION,
            b"wrong invitation recipient",
        );
        assert!(matches!(
            chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
                &owner,
                stream(1),
                member_pubkey.clone(),
                None,
                MemberRole::Member,
                None,
                wrong_recipient,
                "invalid invitation".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));

        let add = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                member_pubkey.clone(),
                None,
                MemberRole::Member,
                "add member".to_string(),
            )
            .unwrap();
        chain.add_entry(add).unwrap();
        assert!(matches!(
            chain.signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(1),
                member_pubkey,
                Vec::new(),
                "missing owner wrap".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));
    }

    #[test]
    fn wrapped_key_generations_follow_the_causal_membership_history() {
        let owner = key();
        let first_member = key();
        let second_member = key();
        let later_member = key();
        let owner_pubkey = keys::public_key_hex(&owner);
        let first_pubkey = keys::public_key_hex(&first_member);
        let second_pubkey = keys::public_key_hex(&second_member);
        let later_pubkey = keys::public_key_hex(&later_member);
        let mut chain = founded("wrapped-generation-history", &owner);
        for member in [&first_pubkey, &second_pubkey] {
            let add = chain
                .signed_set_member_in_stream(
                    &owner,
                    stream(1),
                    member.clone(),
                    None,
                    MemberRole::Member,
                    format!("add {member}"),
                )
                .unwrap();
            chain.add_entry(add).unwrap();
        }
        let mut first_rotation_wraps = vec![
            test_wrapped_key_ref(&owner_pubkey, &owner_pubkey, 2, b"first owner rotation"),
            test_wrapped_key_ref(&owner_pubkey, &second_pubkey, 2, b"first member rotation"),
        ];
        first_rotation_wraps.sort();
        let first_rotation = chain
            .signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(1),
                first_pubkey,
                first_rotation_wraps,
                "first rotation".to_string(),
            )
            .unwrap();
        chain.add_entry(first_rotation).unwrap();

        assert!(matches!(
            chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
                &owner,
                stream(1),
                later_pubkey.clone(),
                None,
                MemberRole::Member,
                None,
                test_wrapped_key_ref(&owner_pubkey, &later_pubkey, 1, b"stale later invitation",),
                "stale later invitation".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));
        assert!(matches!(
            chain.signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(1),
                second_pubkey,
                vec![test_wrapped_key_ref(
                    &owner_pubkey,
                    &owner_pubkey,
                    2,
                    b"reused rotation generation",
                )],
                "reused rotation generation".to_string(),
            ),
            Err(MembershipError::InvalidWrappedKeys(_))
        ));
    }

    #[test]
    fn concurrent_add_and_rotation_has_incomplete_wrapped_key_authority() {
        let owner = key();
        let removed = key();
        let concurrent_member = key();
        let owner_pubkey = keys::public_key_hex(&owner);
        let removed_pubkey = keys::public_key_hex(&removed);
        let concurrent_pubkey = keys::public_key_hex(&concurrent_member);
        let mut chain = founded("concurrent-add-rotation", &owner);
        let add_removed = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                removed_pubkey.clone(),
                None,
                MemberRole::Member,
                "add member that will be removed".to_string(),
            )
            .unwrap();
        chain.add_entry(add_removed).unwrap();

        let add_concurrent = chain
            .signed_set_member_in_stream(
                &owner,
                stream(2),
                concurrent_pubkey.clone(),
                None,
                MemberRole::Member,
                "concurrent add".to_string(),
            )
            .unwrap();
        let owner_rotation = test_wrapped_key_ref(
            &owner_pubkey,
            &owner_pubkey,
            2,
            b"rotation missing concurrent member",
        );
        let remove = chain
            .signed_remove_member_with_wrapped_keys_in_stream(
                &owner,
                stream(3),
                removed_pubkey,
                vec![owner_rotation],
                "concurrent removal".to_string(),
            )
            .unwrap();
        chain.add_entry(add_concurrent).unwrap();
        chain.add_entry(remove).unwrap();

        assert!(matches!(
            chain.wrapped_key_authority_for(&concurrent_pubkey),
            Err(MembershipError::MissingWrappedKeyCoverage { .. })
        ));

        let replacement_wrap = test_wrapped_key_ref(
            &owner_pubkey,
            &concurrent_pubkey,
            2,
            b"post-rotation replacement invitation",
        );
        let replacement = chain
            .signed_set_member_with_anchor_and_wrapped_key_in_stream(
                &owner,
                stream(4),
                concurrent_pubkey.clone(),
                None,
                MemberRole::Member,
                None,
                replacement_wrap.clone(),
                "replace concurrent invitation after rotation".to_string(),
            )
            .unwrap();
        chain.add_entry(replacement).unwrap();
        assert_eq!(
            chain.wrapped_key_authority_for(&concurrent_pubkey).unwrap(),
            vec![replacement_wrap],
        );
    }

    #[test]
    fn concurrent_member_assignments_are_validated_conflict_state() {
        let owner = key();
        let target = key();
        let target_pubkey = keys::public_key_hex(&target);
        let mut chain = founded("store", &owner);
        let member = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                target_pubkey.clone(),
                None,
                MemberRole::Member,
                "initial Member".to_string(),
            )
            .unwrap();
        chain.add_entry(member).unwrap();
        let first = chain
            .signed_set_member_in_stream(
                &owner,
                stream(21),
                target_pubkey.clone(),
                None,
                MemberRole::Follower,
                "first".to_string(),
            )
            .unwrap();
        let second = chain
            .signed_promote_member_in_stream_for_test(
                &owner,
                stream(22),
                target_pubkey.clone(),
                "second".to_string(),
            )
            .unwrap();
        let mut entries = chain.entries().to_vec();
        entries.extend([first.clone(), second.clone()]);
        let heads = entries
            .iter()
            .filter(|entry| {
                !entries.iter().any(|candidate| {
                    candidate
                        .dependencies
                        .iter()
                        .any(|dependency| dependency == &entry.coord())
                        && candidate.stream_id == entry.stream_id
                })
            })
            .map(|entry| exact_head(entry, &owner))
            .collect();

        let conflicted = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        )
        .expect("well-formed conflict");
        let MembershipConflict::ConcurrentMemberAssignments {
            member_pubkey,
            conflicting_grants,
            ..
        } = conflicted.conflict().expect("assignment conflict")
        else {
            panic!("concurrent assignments must produce an assignment conflict")
        };
        assert_eq!(member_pubkey, &target_pubkey);
        assert_eq!(conflicting_grants.len(), 2);

        let selected_grant = conflicting_grants
            .iter()
            .find_map(|(grant, record)| {
                (record.role.role() == MemberRole::Follower).then(|| grant.clone())
            })
            .expect("Follower assignment");
        let retired_grant = conflicting_grants
            .keys()
            .find(|grant| **grant != selected_grant)
            .expect("other assignment")
            .clone();
        let opaque_choice = MembershipConflictChoice::new(
            "opaque-choice".to_string(),
            Vec::new(),
            ObjectHash::digest(b"hidden conflict"),
            MembershipConflictSelection::MemberAssignment {
                grant: selected_grant.clone(),
            },
        );
        assert_eq!(
            format!("{opaque_choice:?}"),
            "MembershipConflictChoice { id: \"opaque-choice\", members: [] }",
        );
        let store_root_hash = ObjectHash::digest(b"assignment-resolution Store root");
        let replacement_membership = membership_anchor("assignment-resolution");
        let acceptance = conflict_acceptance(
            &conflicted,
            store_root_hash,
            replacement_membership.clone(),
            &owner,
        );
        let resolution_value = conflicted
            .signed_conflict_resolution(
                store_root_hash,
                MembershipConflictSelection::MemberAssignment {
                    grant: selected_grant.clone(),
                },
                replacement_membership,
                acceptance,
                &owner,
            )
            .expect("Owner selects an assignment");
        let mut incomplete_resolution = resolution_value.clone();
        incomplete_resolution
            .retirement_barriers
            .remove(&retired_grant);
        incomplete_resolution.signature =
            keys::sign_hex(&owner, &incomplete_resolution.canonical_bytes()).1;
        assert!(!incomplete_resolution.verify_against(
            store_root_hash,
            conflicted.conflict().expect("assignment conflict"),
        ));
        let resolution = exact_resolution(resolution_value);
        let resolved_once = conflicted
            .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
            .expect("assignment resolution applies");
        let resolved_retry = conflicted
            .resolved_with(store_root_hash, &[resolution.clone(), resolution.clone()])
            .expect("exact assignment resolution retry is idempotent");

        assert_eq!(resolved_once, resolved_retry);
        assert_eq!(
            resolved_once
                .grants
                .get(&selected_grant)
                .and_then(GrantState::active)
                .map(|record| record.role.role()),
            Some(MemberRole::Follower),
        );
        assert!(matches!(
            resolved_once.grants.get(&retired_grant),
            Some(GrantState::Tombstoned { .. })
        ));
        assert!(resolution
            .1
            .retired_owner_grants
            .iter()
            .all(|grant| resolved_once
                .grants
                .get(grant)
                .and_then(GrantState::active)
                .is_none()));
        assert!(resolved_once
            .grants
            .get(&resolution.1.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
    }

    #[test]
    fn assignment_resolvers_keep_only_a_choice_they_all_selected() {
        let first_owner = key();
        let second_owner = key();
        let target = key();
        let first_owner_pubkey = keys::public_key_hex(&first_owner);
        let second_owner_pubkey = keys::public_key_hex(&second_owner);
        let target_pubkey = keys::public_key_hex(&target);
        let mut base = founded("assignment-consensus", &first_owner);
        base.add_owner_for_test(
            &first_owner,
            stream(1),
            second_owner_pubkey.clone(),
            "add second Owner".to_string(),
        )
        .unwrap();
        let initial = base
            .signed_set_member_in_stream(
                &first_owner,
                stream(1),
                target_pubkey.clone(),
                None,
                MemberRole::Member,
                "initial target assignment".to_string(),
            )
            .unwrap();
        base.add_entry(initial).unwrap();
        let follower_assignment = base
            .signed_set_member_in_stream(
                &first_owner,
                stream(21),
                target_pubkey.clone(),
                None,
                MemberRole::Follower,
                "Follower assignment".to_string(),
            )
            .unwrap();
        let member_assignment = base
            .signed_set_member_in_stream(
                &second_owner,
                stream(22),
                target_pubkey.clone(),
                None,
                MemberRole::Member,
                "Member assignment".to_string(),
            )
            .unwrap();
        let mut entries = base.entries().to_vec();
        entries.extend([follower_assignment, member_assignment]);
        let heads = entries
            .iter()
            .filter(|entry| {
                !entries.iter().any(|candidate| {
                    candidate
                        .dependencies
                        .iter()
                        .any(|dependency| dependency == &entry.coord())
                        && candidate.stream_id == entry.stream_id
                })
            })
            .map(|entry| {
                let signer = if entry.author_pubkey == first_owner_pubkey {
                    &first_owner
                } else {
                    assert_eq!(entry.author_pubkey, second_owner_pubkey);
                    &second_owner
                };
                exact_head(entry, signer)
            })
            .collect();
        let conflicted = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        )
        .expect("well-formed assignment conflict");
        let MembershipConflict::ConcurrentMemberAssignments {
            conflicting_grants, ..
        } = conflicted.conflict().expect("assignment conflict")
        else {
            panic!("concurrent assignments must produce an assignment conflict")
        };
        let follower_grant = conflicting_grants
            .iter()
            .find_map(|(grant, record)| {
                (record.role.role() == MemberRole::Follower).then(|| grant.clone())
            })
            .expect("Follower assignment");
        let member_grant = conflicting_grants
            .iter()
            .find_map(|(grant, record)| {
                (record.role.role() == MemberRole::Member).then(|| grant.clone())
            })
            .expect("Member assignment");
        let store_root_hash = ObjectHash::digest(b"assignment consensus Store root");

        let first_membership = membership_anchor("first-assignment-resolution");
        let first_acceptance = conflict_acceptance(
            &conflicted,
            store_root_hash,
            first_membership.clone(),
            &first_owner,
        );
        let first_resolution = exact_resolution(
            conflicted
                .signed_conflict_resolution(
                    store_root_hash,
                    MembershipConflictSelection::MemberAssignment {
                        grant: follower_grant.clone(),
                    },
                    first_membership,
                    first_acceptance,
                    &first_owner,
                )
                .expect("first Owner selects the Follower assignment"),
        );
        let second_membership = membership_anchor("second-assignment-resolution");
        let second_acceptance = conflict_acceptance(
            &conflicted,
            store_root_hash,
            second_membership.clone(),
            &second_owner,
        );
        let second_resolution = exact_resolution(
            conflicted
                .signed_conflict_resolution(
                    store_root_hash,
                    MembershipConflictSelection::MemberAssignment {
                        grant: member_grant.clone(),
                    },
                    second_membership,
                    second_acceptance,
                    &second_owner,
                )
                .expect("second Owner selects the Member assignment"),
        );

        let resolved = conflicted
            .resolved_with(
                store_root_hash,
                &[first_resolution.clone(), second_resolution.clone()],
            )
            .expect("disagreeing assignment resolutions converge");

        assert!(matches!(
            resolved.grants.get(&follower_grant),
            Some(GrantState::Tombstoned { .. })
        ));
        assert!(matches!(
            resolved.grants.get(&member_grant),
            Some(GrantState::Tombstoned { .. })
        ));
        assert!(!resolved
            .grants
            .values()
            .filter_map(GrantState::active)
            .any(|record| record.member_pubkey == target_pubkey));
        for resolution in [&first_resolution, &second_resolution] {
            assert!(resolved
                .grants
                .get(&resolution.1.replacement_grant)
                .and_then(GrantState::active)
                .is_some());
            assert!(resolution
                .1
                .retired_owner_grants
                .iter()
                .all(|grant| resolved
                    .grants
                    .get(grant)
                    .and_then(GrantState::active)
                    .is_none()));
        }
    }

    #[test]
    fn concurrent_cross_revocation_is_a_validated_cycle_conflict() {
        let first_owner = key();
        let second_owner = key();
        let first_pubkey = keys::public_key_hex(&first_owner);
        let second_pubkey = keys::public_key_hex(&second_owner);
        let mut base = founded("store", &first_owner);
        base.add_owner_for_test(
            &first_owner,
            stream(1),
            second_pubkey.clone(),
            "add second".to_string(),
        )
        .unwrap();
        let remove_second = base
            .signed_remove_member_in_stream(
                &first_owner,
                stream(1),
                second_pubkey.clone(),
                "remove second".to_string(),
            )
            .unwrap();
        let remove_first = base
            .signed_remove_member_in_stream(
                &second_owner,
                stream(23),
                first_pubkey.clone(),
                "remove first".to_string(),
            )
            .unwrap();
        let mut entries = base.entries().to_vec();
        entries.extend([remove_second.clone(), remove_first.clone()]);
        let heads = vec![
            exact_head(
                base.entries().first().expect("founder membership entry"),
                &first_owner,
            ),
            exact_head(&remove_second, &first_owner),
            exact_head(&remove_first, &second_owner),
        ];

        let conflicted = MembershipChain::from_entries_with_coords_and_heads(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            heads,
        )
        .expect("well-formed conflict");
        assert!(matches!(
            conflicted.status(),
            MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
                cyclic_sources,
                involved_owner_grants,
                maximal_valid_branches,
                ..

            }) if cyclic_sources.len() == 2
                && involved_owner_grants.len() == 2
                && maximal_valid_branches.len() == 2
        ));

        let MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = conflicted.conflict().expect("cycle conflict")
        else {
            unreachable!();
        };
        let resolver_branch_state = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == first_pubkey && record.role.is_owner()
                })
            })
            .expect("first Owner branch")
            .clone();
        let resolver_branch = resolver_branch_state.heads.clone();
        let second_resolver_branch = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == second_pubkey && record.role.is_owner()
                })
            })
            .expect("second Owner branch")
            .heads
            .clone();
        let store_root_hash = ObjectHash::digest(b"resolution Store root");
        let first_membership = membership_anchor("first-cycle-resolution");
        let first_acceptance = conflict_acceptance(
            &conflicted,
            store_root_hash,
            first_membership.clone(),
            &first_owner,
        );
        let resolution_value = conflicted
            .signed_conflict_resolution(
                store_root_hash,
                MembershipConflictSelection::RevocationBranch {
                    heads: resolver_branch.clone(),
                },
                first_membership.clone(),
                first_acceptance.clone(),
                &first_owner,
            )
            .expect("branch Owner resolves the conflict");
        let mut forged_resolution = resolution_value.clone();
        forged_resolution.signature =
            keys::sign_hex(&second_owner, &forged_resolution.canonical_bytes()).1;
        assert!(!forged_resolution.verify_signature());
        assert!(!forged_resolution.verify_against(
            store_root_hash,
            conflicted.conflict().expect("cycle conflict"),
        ));
        let second_membership = membership_anchor("second-cycle-resolution");
        let second_acceptance = conflict_acceptance(
            &conflicted,
            store_root_hash,
            second_membership.clone(),
            &second_owner,
        );
        let second_resolution_value = conflicted
            .signed_conflict_resolution(
                store_root_hash,
                MembershipConflictSelection::RevocationBranch {
                    heads: second_resolver_branch,
                },
                second_membership,
                second_acceptance,
                &second_owner,
            )
            .expect("other branch Owner resolves the conflict");
        let retried = conflicted
            .signed_conflict_resolution(
                store_root_hash,
                MembershipConflictSelection::RevocationBranch {
                    heads: resolver_branch,
                },
                first_membership,
                first_acceptance,
                &first_owner,
            )
            .expect("same resolver retry");
        assert_eq!(resolution_value, retried);
        assert!(resolution_value.verify_against(
            store_root_hash,
            conflicted.conflict().expect("cycle conflict"),
        ));
        let resolution = exact_resolution(resolution_value);
        let second_resolution = exact_resolution(second_resolution_value);
        let resolved_once = conflicted
            .resolved_with(store_root_hash, std::slice::from_ref(&resolution))
            .expect("one resolution applies");
        let resolved_duplicate = conflicted
            .resolved_with(store_root_hash, &[resolution.clone(), resolution.clone()])
            .expect("an exact retry is idempotent");
        assert_eq!(resolved_once, resolved_duplicate);
        assert!(resolved_once
            .grants
            .get(&resolution.1.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
        assert!(resolution
            .1
            .retired_owner_grants
            .iter()
            .all(|grant| resolved_once
                .grants
                .get(grant)
                .and_then(GrantState::active)
                .is_none()));

        let resolved_union = conflicted
            .resolved_with(
                store_root_hash,
                &[resolution.clone(), second_resolution.clone()],
            )
            .expect("distinct resolvers are unioned");
        assert!(resolved_union
            .grants
            .get(&resolution.1.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
        assert!(resolved_union
            .grants
            .get(&second_resolution.1.replacement_grant)
            .and_then(GrantState::active)
            .is_some());

        let mut branch_specific = conflicted.conflict().expect("cycle conflict").clone();
        let MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = &mut branch_specific
        else {
            unreachable!()
        };
        let branch_only_grant = MembershipGrantId(ObjectHash::digest(b"branch-only grant"));
        let branch_only_creation = maximal_valid_branches[0].effective_frontier[0].clone();
        maximal_valid_branches[0].grants.insert(
            branch_only_grant.clone(),
            GrantState::Active {
                record: MembershipGrantRecord {
                    member_pubkey: keys::public_key_hex(&key()),
                    role: StoreMembershipRoleGrant::Member,
                    provider_account_email: None,
                    creation_authority: MembershipGrantCreationAuthority::Entry(
                        branch_only_creation,
                    ),
                },
            },
        );
        let branch_barrier = MergeMembershipGrantRetirementBarrier::NonOwner {
            author_streams: StoreGrantStreamBarrier {
                observed_streams: Vec::new(),
            },
        };
        let mut branch_resolution_value = resolution.1.clone();
        branch_resolution_value
            .retirement_barriers
            .insert(branch_only_grant.clone(), branch_barrier.clone());
        branch_resolution_value.signature =
            keys::sign_hex(&first_owner, &branch_resolution_value.canonical_bytes()).1;
        let branch_resolution = exact_resolution(branch_resolution_value);
        let mut branch_second_resolution_value = second_resolution.1.clone();
        branch_second_resolution_value
            .retirement_barriers
            .insert(branch_only_grant.clone(), branch_barrier);
        branch_second_resolution_value.signature = keys::sign_hex(
            &second_owner,
            &branch_second_resolution_value.canonical_bytes(),
        )
        .1;
        let branch_second_resolution = exact_resolution(branch_second_resolution_value);
        let composed = resolve_store_membership_conflict(
            store_root_hash,
            &branch_specific,
            &[branch_resolution.clone(), branch_second_resolution.clone()],
        )
        .expect("retire grants not agreed by every valid branch");
        let branch_only_retirements = composed
            .grants
            .get(&branch_only_grant)
            .and_then(GrantState::retirements)
            .expect("branch-only grant is retained as retired");
        assert!(branch_only_retirements.iter().any(|retirement| matches!(
            retirement,
            MembershipGrantRetirement::ConflictResolution { authority, .. }
                if authority == &branch_resolution.0
        )));
        assert!(branch_only_retirements.iter().any(|retirement| matches!(
            retirement,
            MembershipGrantRetirement::ConflictResolution { authority, .. }
                if authority == &branch_second_resolution.0
        )));

        let mut duplicate_member = branch_specific;
        let MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = &mut duplicate_member
        else {
            unreachable!()
        };
        let duplicate_pubkey = keys::public_key_hex(&key());
        let duplicate_creation = resolution.1.conflicting_heads[0].coord.clone();
        for branch in maximal_valid_branches {
            for suffix in [b'a', b'b'] {
                branch.grants.insert(
                    MembershipGrantId(ObjectHash::digest(&[suffix])),
                    GrantState::Active {
                        record: MembershipGrantRecord {
                            member_pubkey: duplicate_pubkey.clone(),
                            role: StoreMembershipRoleGrant::Member,
                            provider_account_email: None,
                            creation_authority: MembershipGrantCreationAuthority::Entry(
                                duplicate_creation.clone(),
                            ),
                        },
                    },
                );
            }
        }
        assert!(matches!(
            resolve_store_membership_conflict(
                store_root_hash,
                &duplicate_member,
                &[resolution.clone(), second_resolution.clone()],
            ),
            Err(MembershipError::InvalidConflictResolution)
        ));

        let mut resumed = conflicted.clone();
        let raw_heads = resumed.author_heads();
        resumed
            .apply_resolutions(store_root_hash, std::slice::from_ref(&resolution))
            .expect("resolution activates replacement Owner grant");
        assert_eq!(resumed.author_heads(), raw_heads);
        let accepted_controls = [remove_second.coord(), remove_first.coord()];
        assert!(accepted_controls
            .iter()
            .all(|coord| resumed.contains_coord(coord)));
        assert!(accepted_controls
            .iter()
            .any(|coord| !resumed.included.contains(coord)));
        let raw_losing_control = accepted_controls
            .iter()
            .find(|coord| !resumed.included.contains(*coord))
            .expect("resolved history retains one raw losing control")
            .clone();
        let checkpoint_floor = crate::protocol::store_commit::MembershipCausalFloor {
            effective_coordinates: vec![raw_losing_control],
            resolutions: resumed.resolution_refs().to_vec(),
        };
        assert!(
            !checkpoint_floor.is_included_in(&resumed),
            "a coordinate present only in the raw losing branch cannot satisfy a retained effective checkpoint floor",
        );
        assert_eq!(
            resumed.effective_frontier(),
            resolver_branch_state.effective_frontier
        );
        assert_eq!(
            resumed.resolution_refs(),
            std::slice::from_ref(&resolution.0)
        );
        let after_resolution = resumed
            .signed_set_member_in_stream(
                &first_owner,
                stream(37),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "write after resolution".to_string(),
            )
            .expect("replacement Owner can author from a fresh stream");
        assert_eq!(
            after_resolution.author_owner_grant,
            resolution.1.replacement_grant
        );
        let activated_head = exact_head(&after_resolution, &first_owner).1;
        resumed
            .add_entry(after_resolution)
            .expect("future authoring validates from the resolved checkpoint");
        assert_eq!(activated_head.body.resolutions, vec![resolution.0.clone()]);
        let authority = MembershipGrantCreationAuthority::ConflictResolution(resolution.0.clone());
        assert!(resumed.authorizes_write_authority(&authority, &first_pubkey));
        let outsider = key();
        let outsider_membership = membership_anchor("non-owner-cycle-resolution");
        let outsider_acceptance = conflict_acceptance(
            &conflicted,
            store_root_hash,
            outsider_membership.clone(),
            &outsider,
        );
        assert!(matches!(
            conflicted.signed_conflict_resolution(
                store_root_hash,
                resolution.1.selection.clone(),
                outsider_membership,
                outsider_acceptance,
                &outsider,
            ),
            Err(MembershipError::SignerIsNotOwner(_))
        ));
    }

    #[test]
    fn dependency_frontier_must_be_strictly_ordered_by_author_stream() {
        let founder = key();
        let second_owner = key();
        let mut chain = founded("store", &founder);
        chain
            .add_owner_for_test(
                &founder,
                stream(1),
                keys::public_key_hex(&second_owner),
                "add owner".to_string(),
            )
            .unwrap();
        let second_stream = chain
            .signed_set_member_in_stream(
                &second_owner,
                stream(31),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "second stream".to_string(),
            )
            .unwrap();
        chain.add_entry(second_stream).unwrap();
        let mut unsorted = chain
            .signed_set_member_in_stream(
                &founder,
                stream(1),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "candidate".to_string(),
            )
            .unwrap();
        assert!(unsorted.dependencies.len() > 1);
        unsorted.dependencies.reverse();
        sign_membership_entry(&mut unsorted, &founder);

        assert!(matches!(
            chain.add_entry(unsorted),
            Err(MembershipError::NonCanonicalDependencyFrontier { .. })
        ));
    }

    #[test]
    fn owner_barrier_must_be_strictly_ordered_by_author_stream() {
        let founder = key();
        let second_owner = key();
        let second_owner_pubkey = keys::public_key_hex(&second_owner);
        let mut chain = founded("store", &founder);
        chain
            .add_owner_for_test(
                &founder,
                stream(1),
                second_owner_pubkey.clone(),
                "add owner".to_string(),
            )
            .unwrap();
        for (stream_id, timestamp) in [(stream(41), "first stream"), (stream(42), "second stream")]
        {
            let authored = chain
                .signed_set_member_in_stream(
                    &second_owner,
                    stream_id,
                    keys::public_key_hex(&key()),
                    None,
                    MemberRole::Member,
                    timestamp.to_string(),
                )
                .unwrap();
            chain.add_entry(authored).unwrap();
        }
        let mut removal = chain
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
                second_owner_pubkey,
                "remove owner".to_string(),
            )
            .unwrap();
        let MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } = &mut removal.change
        else {
            unreachable!();
        };
        let observed = &mut retirement_barriers
            .values_mut()
            .next()
            .expect("owner removal barrier")
            .author_streams()
            .observed_streams
            .clone();
        assert!(observed.len() > 1);
        let barrier = retirement_barriers
            .values_mut()
            .next()
            .expect("owner removal barrier");
        match barrier {
            MergeMembershipGrantRetirementBarrier::Owner { barrier } => {
                barrier.author_streams.observed_streams.reverse();
            }
            MergeMembershipGrantRetirementBarrier::NonOwner { .. } => {
                panic!("Owner removal carries non-Owner barrier")
            }
        }
        sign_membership_entry(&mut removal, &founder);

        assert!(matches!(
            chain.add_entry(removal),
            Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
        ));
    }

    #[test]
    fn owner_readd_uses_a_new_sequence_one_stream() {
        let owner = key();
        let second = key();
        let mut chain = founded("store", &owner);
        chain
            .add_owner_for_test(
                &owner,
                stream(1),
                keys::public_key_hex(&second),
                "add".to_string(),
            )
            .unwrap();
        let old_grant = chain
            .active_owner_grant(&keys::public_key_hex(&second))
            .unwrap();
        let remove = chain
            .signed_remove_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&second),
                "remove".to_string(),
            )
            .unwrap();
        chain.add_entry(remove).unwrap();
        chain
            .add_owner_for_test(
                &owner,
                stream(1),
                keys::public_key_hex(&second),
                "readd".to_string(),
            )
            .unwrap();
        let new_grant = chain
            .active_owner_grant(&keys::public_key_hex(&second))
            .unwrap();
        assert_ne!(old_grant, new_grant);
        let authored = chain
            .signed_set_member_in_stream(
                &second,
                stream(32),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "authored".to_string(),
            )
            .unwrap();
        assert_eq!(authored.seq, 1);
        assert_eq!(authored.author_owner_grant, new_grant);
    }

    #[test]
    fn owner_self_removal_remains_effective_when_its_grant_is_capped_before_first() {
        let founder = key();
        let departing_owner = key();
        let departing_pubkey = keys::public_key_hex(&departing_owner);
        let mut chain = founded("store", &founder);
        chain
            .add_owner_for_test(
                &founder,
                stream(1),
                departing_pubkey.clone(),
                "add owner".to_string(),
            )
            .unwrap();

        let self_removal = chain
            .signed_remove_member_in_stream(
                &departing_owner,
                stream(33),
                departing_pubkey.clone(),
                "self removal".to_string(),
            )
            .unwrap();
        assert!(matches!(
            &self_removal.change,
            MembershipChange::RemoveMember { retirement_barriers, .. }
                if retirement_barriers.values().all(|barrier| barrier.author_streams().observed_streams.is_empty())
        ));
        chain.add_entry(self_removal).unwrap();

        assert!(!chain.is_owner_now(&departing_pubkey));
    }

    #[test]
    fn before_first_barrier_excludes_every_entry_from_the_revoked_owner_stream() {
        let founder = key();
        let second_owner = key();
        let target = key();
        let mut observed = founded("store", &founder);
        observed
            .add_owner_for_test(
                &founder,
                stream(1),
                keys::public_key_hex(&second_owner),
                "add owner".to_string(),
            )
            .unwrap();

        let stale_entry = observed
            .signed_set_member_in_stream(
                &second_owner,
                stream(34),
                keys::public_key_hex(&target),
                None,
                MemberRole::Member,
                "stale entry".to_string(),
            )
            .unwrap();
        let removal = observed
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
                keys::public_key_hex(&second_owner),
                "remove owner".to_string(),
            )
            .unwrap();
        assert!(matches!(
            &removal.change,
            MembershipChange::RemoveMember { retirement_barriers, .. }
                if retirement_barriers.values().all(|barrier| barrier.author_streams().observed_streams.is_empty())
        ));

        let mut entries = observed.entries().to_vec();
        entries.extend([removal, stale_entry]);
        let chain = MembershipChain::from_entries(entries).unwrap();
        assert!(!chain.can_write_now(&keys::public_key_hex(&target)));
        assert!(chain
            .author_heads()
            .iter()
            .any(|coord| coord.author_pubkey == keys::public_key_hex(&second_owner)));
        assert!(chain
            .effective_frontier()
            .iter()
            .all(|coord| coord.author_pubkey != keys::public_key_hex(&second_owner)));
    }

    #[test]
    fn through_barrier_keeps_its_exact_prefix_and_prunes_the_stale_suffix() {
        let founder = key();
        let second_owner = key();
        let first_target = key();
        let second_target = key();
        let third_target = key();
        let mut observed = founded("store", &founder);
        observed
            .add_owner_for_test(
                &founder,
                stream(1),
                keys::public_key_hex(&second_owner),
                "add owner".to_string(),
            )
            .unwrap();
        let first = observed
            .signed_set_member_in_stream(
                &second_owner,
                stream(35),
                keys::public_key_hex(&first_target),
                None,
                MemberRole::Member,
                "first".to_string(),
            )
            .unwrap();
        observed.add_entry(first.clone()).unwrap();

        let removal = observed
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
                keys::public_key_hex(&second_owner),
                "remove owner".to_string(),
            )
            .unwrap();
        assert!(matches!(
            &removal.change,
            MembershipChange::RemoveMember { retirement_barriers, .. }
                if retirement_barriers.values().any(|barrier| barrier.author_streams().observed_streams == vec![first.coord()])
        ));

        let second = observed
            .signed_set_member_in_stream(
                &second_owner,
                stream(35),
                keys::public_key_hex(&second_target),
                None,
                MemberRole::Member,
                "second".to_string(),
            )
            .unwrap();
        let mut exact_entries = observed.entries().to_vec();
        exact_entries.extend([removal.clone(), second.clone()]);
        let exact = MembershipChain::from_entries(exact_entries).unwrap();
        assert!(exact.can_write_now(&keys::public_key_hex(&first_target)));
        assert!(!exact.can_write_now(&keys::public_key_hex(&second_target)));

        let mut stale = observed;
        stale.add_entry(second).unwrap();
        let third = stale
            .signed_set_member_in_stream(
                &second_owner,
                stream(35),
                keys::public_key_hex(&third_target),
                None,
                MemberRole::Member,
                "third".to_string(),
            )
            .unwrap();
        stale.add_entry(third.clone()).unwrap();
        let mut beyond_entries = stale.entries().to_vec();
        beyond_entries.push(removal);
        let pruned = MembershipChain::from_entries(beyond_entries).unwrap();
        assert!(pruned.can_write_now(&keys::public_key_hex(&first_target)));
        assert!(!pruned.can_write_now(&keys::public_key_hex(&second_target)));
        assert!(!pruned.can_write_now(&keys::public_key_hex(&third_target)));
    }

    #[test]
    fn through_barrier_rejects_a_coordinate_hash_that_is_not_its_dependency() {
        let founder = key();
        let second_owner = key();
        let mut chain = founded("store", &founder);
        chain
            .add_owner_for_test(
                &founder,
                stream(1),
                keys::public_key_hex(&second_owner),
                "add owner".to_string(),
            )
            .unwrap();
        let authored = chain
            .signed_set_member_in_stream(
                &second_owner,
                stream(36),
                keys::public_key_hex(&key()),
                None,
                MemberRole::Member,
                "authored".to_string(),
            )
            .unwrap();
        chain.add_entry(authored).unwrap();
        let mut removal = chain
            .signed_remove_member_in_stream(
                &founder,
                stream(1),
                keys::public_key_hex(&second_owner),
                "remove owner".to_string(),
            )
            .unwrap();
        let MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } = &mut removal.change
        else {
            unreachable!();
        };
        let barrier = retirement_barriers
            .values_mut()
            .next()
            .expect("owner removal barrier");
        let MergeMembershipGrantRetirementBarrier::Owner { barrier } = barrier else {
            panic!("Owner removal carries non-Owner barrier")
        };
        let barrier = barrier
            .author_streams
            .observed_streams
            .first_mut()
            .expect("observed owner stream");
        barrier.entry_hash = ObjectHash::digest(b"wrong barrier hash");
        sign_membership_entry(&mut removal, &founder);
        assert!(matches!(
            chain.add_entry(removal),
            Err(MembershipError::InvalidOwnerRevocationBarrier { .. })
        ));
    }

    #[test]
    fn cross_store_replay_fails_even_with_the_same_founder_key() {
        let owner = key();
        let from_a = test_founder_entry("store-a", &owner, "founder", membership_anchor("store-a"));
        let mut replayed = from_a.clone();
        replayed.store_id = "store-b".to_string();
        assert!(!verify_membership_entry(&replayed));
        assert!(MembershipChain::from_entries(vec![from_a])
            .unwrap()
            .is_founded_by(&keys::public_key_hex(&owner)));
    }

    #[test]
    fn created_at_is_signed_but_never_orders_entries() {
        let owner = key();
        let entry = test_founder_entry("store", &owner, "display-time", membership_anchor("store"));
        let mut tampered = entry.clone();
        tampered.created_at = "other".to_string();
        assert!(!verify_membership_entry(&tampered));
    }

    #[test]
    fn membership_head_resolution_cut_must_equal_its_tip_entry_cut() {
        let owner = UserKeypair::generate();
        let entry = test_founder_entry(
            "head-tip-resolution-cut",
            &owner,
            "founder",
            membership_anchor("head-tip-resolution-cut"),
        );
        let fake = StoreMembershipConflictResolutionRef {
            conflict_hash: ObjectHash::digest(b"head-tip conflict"),
            resolver_pubkey: keys::public_key_hex(&owner),
            resolution_hash: ObjectHash::digest(b"head-tip resolution"),
            object: exact(
                "test/head-tip-resolution-cut/resolution.json",
                b"head-tip resolution",
            ),
        };
        let head = exact_head_with_resolutions(&entry, &owner, vec![fake]);

        assert!(matches!(
            MembershipChain::from_entries_with_coords_and_heads(
                vec![(entry.coord(), entry)],
                vec![head],
            ),
            Err(MembershipError::MissingConflictHeads)
        ));
    }

    #[test]
    fn membership_entry_rejects_unsorted_or_duplicate_resolution_dependencies() {
        let owner = UserKeypair::generate();
        let founder = test_founder_entry(
            "entry-resolution-cut",
            &owner,
            "founder",
            membership_anchor("entry-resolution-cut"),
        );
        let chain = MembershipChain::from_entries(vec![founder]).unwrap();
        let entry = chain
            .signed_set_member_in_stream(
                &owner,
                stream(1),
                keys::public_key_hex(&UserKeypair::generate()),
                None,
                MemberRole::Member,
                "member".to_string(),
            )
            .unwrap();
        let mut refs = [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|label| StoreMembershipConflictResolutionRef {
                conflict_hash: ObjectHash::digest(label),
                resolver_pubkey: keys::public_key_hex(&owner),
                resolution_hash: ObjectHash::digest(&[label, b" resolution"].concat()),
                object: exact(
                    format!(
                        "test/entry-resolution-cut/{}.json",
                        String::from_utf8_lossy(label)
                    ),
                    label,
                ),
            })
            .collect::<Vec<_>>();
        refs.sort();

        let mut unsorted = entry.clone();
        unsorted.resolution_dependencies = refs.iter().rev().cloned().collect();
        sign_membership_entry(&mut unsorted, &owner);
        assert!(!verify_membership_entry(&unsorted));

        let mut duplicate = entry;
        duplicate.resolution_dependencies = vec![refs[0].clone(), refs[0].clone()];
        sign_membership_entry(&mut duplicate, &owner);
        assert!(!verify_membership_entry(&duplicate));
    }
}
