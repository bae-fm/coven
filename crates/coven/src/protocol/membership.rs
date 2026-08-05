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
use crate::protocol::objects::ExactObjectRef;

mod authoring;
mod authority;
mod chain;
mod conflict;
mod entry;
mod reduction;

pub(crate) use conflict::{
    derive_store_resolution_grant, resolve_store_membership_conflict, MembershipConflict,
    MembershipConflictSelection, MembershipStatus, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionRef,
};
pub use conflict::{MembershipConflictChoice, MembershipConflictInfo};
pub(crate) use entry::{
    derive_founder_stream_id, derive_grant_id, entry_hash, founder_entry_for_creation,
    sign_membership_entry, verify_membership_entry,
};
#[cfg(test)]
pub(crate) use entry::{founder_entry, test_wrapped_key_ref};
use reduction::*;

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
    pub head_slot: crate::protocol::objects::ObjectSlot,
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
            crate::protocol::objects::ObjectSlot::logical(format!(
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
mod tests;

pub(crate) const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalStoreMembership {
    Current,
    NotYetMember,
    Removed,
    IdentityNotSupplied,
}

impl LocalStoreMembership {
    pub(crate) fn from_membership(
        membership: &MembershipChain,
        identity: Option<&crate::keys::UserKeypair>,
    ) -> Result<Self, crate::protocol::membership::MembershipError> {
        membership.ensure_resolved()?;
        let Some(identity) = identity else {
            return Ok(Self::IdentityNotSupplied);
        };
        let identity = crate::keys::public_key_hex(identity);
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

    pub(crate) fn allows_circle_access(self) -> bool {
        matches!(self, Self::Current)
    }

    pub(crate) fn retains_circle_rows(self) -> bool {
        !matches!(self, Self::Removed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStorePositionReason {
    MissingCommit,
    MissingPackage,
    MissingDeviceRegistration {
        device_id: String,
        revision: u64,
        registration_hash: ObjectHash,
    },
    MissingPredecessor(StoreBatchCommitRef),
    MissingDependency {
        device_id: String,
        commit: StoreBatchCommitRef,
    },
    NewerSchema {
        local: u32,
        required: u32,
    },
    Unauthorized,
    DeviceExclusionFreeze {
        proposal: super::store_commit::StoreDeviceExclusionProposalRef,
        target_cut: crate::protocol::store_commit::StoreHistoryCut,
    },
    InactiveDevice {
        terminals: Vec<super::store_commit::StoreDeviceExclusionRef>,
        accepted_cut: crate::protocol::store_commit::StoreHistoryCut,
    },
    InvalidChangeset(String),
    InvalidRowIdentity {
        table: String,
        reason: String,
    },
    BlobDownloadFailed,
    ForeignKeyDependency,
    ConstraintConflict(Vec<String>),
    HashMismatch {
        referenced_device_id: String,
        referenced_commit: StoreBatchCommitRef,
        materialized_hash: ObjectHash,
    },
    InvalidSignature,
    WrongSlot(String),
    ObjectCollision(String),
    ObjectUnreadable {
        key: String,
        detail: String,
    },
    InvalidObject(String),
}

pub(crate) enum ApplyOutcome {
    Applied(Vec<crate::changeset::RowChange>),
    Held(HeldStorePositionReason),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct MembershipFloor(pub Vec<MembershipHeadRef>);

impl MembershipFloor {
    pub(crate) fn validate(&self) -> Result<(), String> {
        crate::protocol::membership::validate_membership_floor(&self.0)
    }
}
