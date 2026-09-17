//! Signed Circle roster streams and causal assignment reduction.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::causal_grants::{
    self, CausalAssignment, CausalChange, CausalCoordinate, CausalEntry, CausalGrantConflict,
    CausalGrantError, CausalGrantStatus, GrantState, OwnerGrantBarrier,
};
use super::circle::{CircleId, CircleRole};
use super::membership::MembershipGrantId;
use super::store_commit::{ObjectHash, Signed, SignedBody};
use coven_keys::keys;

mod chain;
mod conflict;
mod reduction;

pub use chain::CircleRosterChain;
pub use conflict::{
    CircleMaterializedRoster, CircleRosterConflict, CircleRosterStatus, ResolvedCircleRoster,
};

const ROSTER_DOMAIN: &[u8] = b"coven.circle-roster.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterCoord {
    pub author_pubkey: String,
    pub device_id: String,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub entry_hash: ObjectHash,
}

impl CircleRosterCoord {
    pub fn stream_key(&self) -> CircleAuthorStreamKey {
        CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
        }
    }
}

impl CausalCoordinate for CircleRosterCoord {
    type StreamKey = CircleAuthorStreamKey;

    fn stream_key(&self) -> Self::StreamKey {
        self.stream_key()
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

impl CausalAssignment for CircleRole {
    fn is_owner(&self) -> bool {
        *self == CircleRole::Owner
    }
}

impl causal_grants::CausalHistoryEntry for CircleRosterEntry {
    type Coord = CircleRosterCoord;

    fn coord(&self) -> Self::Coord {
        self.coord()
    }

    fn dependencies(&self) -> &[Self::Coord] {
        &self.dependencies
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAuthorStreamKey {
    pub author_pubkey: String,
    pub device_id: String,
    pub author_owner_grant: MembershipGrantId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleOwnerGrantBarrier {
    pub observed_streams: Vec<CircleRosterCoord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleRosterChange {
    Founder {
        member_pubkey: String,
        grant_id: MembershipGrantId,
    },
    SetMember {
        member_pubkey: String,
        role: CircleRole,
        grant_id: MembershipGrantId,
        replaces: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, CircleOwnerGrantBarrier>,
    },
    RemoveMember {
        member_pubkey: String,
        removes: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, CircleOwnerGrantBarrier>,
    },
}

/// The wire body of one Circle roster entry. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterEntryBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub author_pubkey: String,
    pub device_id: String,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleRosterCoord>,
    pub change: CircleRosterChange,
}

impl SignedBody for CircleRosterEntryBody {
    const DOMAIN: &'static [u8] = ROSTER_DOMAIN;
}

pub type CircleRosterEntry = Signed<CircleRosterEntryBody>;

impl CircleRosterEntry {
    pub fn founder(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        device_id: &str,
        owner_grant: MembershipGrantId,
        signer: &dyn coven_keys::keys::IdentityKeyAuthority,
    ) -> Self {
        let author_pubkey = keys::public_key_hex(signer);
        Signed::sign(
            CircleRosterEntryBody {
                store_root_hash,
                circle_id,
                author_pubkey: author_pubkey.clone(),
                device_id: device_id.to_string(),
                author_owner_grant: owner_grant.clone(),
                seq: 1,
                previous_hash: None,
                dependencies: Vec::new(),
                change: CircleRosterChange::Founder {
                    member_pubkey: author_pubkey,
                    grant_id: owner_grant,
                },
            },
            signer,
        )
    }

    pub(crate) fn entry_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn coord(&self) -> CircleRosterCoord {
        CircleRosterCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            entry_hash: self.entry_hash(),
        }
    }

    pub fn verify(&self) -> bool {
        let own_stream = self.coord().stream_key();
        let dependency_streams = || self.dependencies.iter().map(CircleRosterCoord::stream_key);
        let position_is_valid = match &self.change {
            CircleRosterChange::Founder {
                member_pubkey,
                grant_id,
                ..
            } => {
                self.seq == 1
                    && self.previous_hash.is_none()
                    && self.dependencies.is_empty()
                    && member_pubkey == &self.author_pubkey
                    && grant_id == &self.author_owner_grant
            }
            CircleRosterChange::SetMember { .. } | CircleRosterChange::RemoveMember { .. } => {
                causal_grants::author_stream_position_is_valid(
                    self.seq,
                    self.previous_hash,
                    &own_stream,
                    dependency_streams(),
                )
            }
        };
        !self.author_pubkey.is_empty()
            && !self.device_id.is_empty()
            && position_is_valid
            && self
                .dependencies
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            && match &self.change {
                CircleRosterChange::SetMember { owner_barriers, .. }
                | CircleRosterChange::RemoveMember { owner_barriers, .. } => {
                    owner_barriers.values().all(|barrier| {
                        barrier
                            .observed_streams
                            .windows(2)
                            .all(|pair| pair[0].stream_key() < pair[1].stream_key())
                    })
                }
                CircleRosterChange::Founder { .. } => true,
            }
            && self.verify_by(&self.author_pubkey).is_ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCircleRosterStateRef {
    /// The raw author-stream frontier of the roster chain this control names:
    /// one coordinate per author stream, canonical by stream key.
    pub frontier: Vec<CircleRosterCoord>,
    pub state_hash: ObjectHash,
}

pub(crate) type CircleRosterStateRef = MergeCircleRosterStateRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleGrantRecord {
    pub member_pubkey: String,
    pub role: CircleRole,
    pub creation_authority: CircleRosterCoord,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleGrantRetirement {
    pub authority: CircleRosterCoord,
    pub owner_barrier: Option<CircleOwnerGrantBarrier>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CircleRosterError {
    #[error("Circle roster is empty")]
    Empty,
    #[error("Circle roster entry {0} has an invalid signature or position")]
    InvalidEntry(usize),
    #[error("Circle roster entry {index} belongs to another Store or Circle")]
    ContextMismatch { index: usize },
    #[error("Circle roster founder does not derive its Circle identity")]
    InvalidFounderIdentity,
    #[error("Circle roster signer {0} has no active Owner assignment")]
    SignerIsNotOwner(String),
    #[error("Circle roster member {0} has no active assignment")]
    NotAMember(String),
    #[error("Circle roster sequence {current} has no representable successor")]
    SequenceExhausted { current: u64 },
    #[error("Circle roster has an unresolved semantic conflict")]
    Conflict,
    #[error("Circle roster causal history is empty")]
    CausalEmpty,
    #[error("Circle roster stream {stream:?} has conflicting entries at sequence {seq}")]
    CausalConflictingSequence {
        stream: CircleAuthorStreamKey,
        seq: u64,
    },
    #[error("Circle roster stream {stream:?} is missing sequence {seq}")]
    CausalMissingSequence {
        stream: CircleAuthorStreamKey,
        seq: u64,
    },
    #[error("Circle roster entry {index} has predecessor {actual:?}, expected {expected:?}")]
    CausalBrokenStreamLink {
        index: usize,
        expected: Option<ObjectHash>,
        actual: Option<ObjectHash>,
    },
    #[error("Circle roster entry {index} does not carry its exact own-stream dependency")]
    CausalMissingOwnDependency { index: usize },
    #[error("Circle roster entry {index} has a dependency under the wrong stream key")]
    CausalDependencyStreamMismatch { index: usize },
    #[error("Circle roster entry {index} depends on missing coordinate {dependency:?}")]
    CausalMissingDependency {
        index: usize,
        dependency: CircleRosterCoord,
    },
    #[error("Circle roster dependency graph contains a cycle")]
    CausalDependencyCycle,
    #[error("Circle roster causal founder is invalid")]
    CausalInvalidFounder,
    #[error("Circle roster entry {index} author is not active under Owner grant {grant}")]
    CausalAuthorGrantInactive {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("Circle roster entry {index} creates already-defined grant {grant}")]
    CausalDuplicateGrant {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error(
        "Circle roster entry {index} replaces or removes grant {grant} owned by another member"
    )]
    CausalGrantOwnerMismatch {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("Circle roster entry {index} does not name the exact active grants for member {member_pubkey}")]
    CausalGrantSetMismatch { index: usize, member_pubkey: String },
    #[error("Circle roster entry {index} removes no exact grants")]
    CausalEmptyRemoval { index: usize },
    #[error("Circle roster entry {index} removes Owner grant {grant} without its exact observed frontier")]
    CausalMissingOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("Circle roster entry {index} carries an invalid frontier for Owner grant {grant}")]
    CausalInvalidOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("Circle roster causal history leaves no active Owner")]
    CausalNoActiveOwner,
}

impl From<CausalGrantError<CircleRosterCoord>> for CircleRosterError {
    fn from(error: CausalGrantError<CircleRosterCoord>) -> Self {
        match error {
            CausalGrantError::Empty => Self::CausalEmpty,
            CausalGrantError::ConflictingSequence { stream, seq } => {
                Self::CausalConflictingSequence { stream, seq }
            }
            CausalGrantError::MissingSequence { stream, seq } => {
                Self::CausalMissingSequence { stream, seq }
            }
            CausalGrantError::BrokenStreamLink {
                index,
                expected,
                actual,
            } => Self::CausalBrokenStreamLink {
                index,
                expected,
                actual,
            },
            CausalGrantError::MissingOwnDependency { index } => {
                Self::CausalMissingOwnDependency { index }
            }
            CausalGrantError::DependencyStreamMismatch { index } => {
                Self::CausalDependencyStreamMismatch { index }
            }
            CausalGrantError::MissingDependency { index, dependency } => {
                Self::CausalMissingDependency { index, dependency }
            }
            CausalGrantError::DependencyCycle => Self::CausalDependencyCycle,
            CausalGrantError::InvalidFounder => Self::CausalInvalidFounder,
            CausalGrantError::AuthorGrantInactive { index, grant } => {
                Self::CausalAuthorGrantInactive { index, grant }
            }
            CausalGrantError::DuplicateGrant { index, grant } => {
                Self::CausalDuplicateGrant { index, grant }
            }
            CausalGrantError::GrantOwnerMismatch { index, grant } => {
                Self::CausalGrantOwnerMismatch { index, grant }
            }
            CausalGrantError::GrantSetMismatch {
                index,
                member_pubkey,
            } => Self::CausalGrantSetMismatch {
                index,
                member_pubkey,
            },
            CausalGrantError::EmptyRemoval { index } => Self::CausalEmptyRemoval { index },
            CausalGrantError::MissingOwnerRevocationBarrier { index, grant } => {
                Self::CausalMissingOwnerRevocationBarrier { index, grant }
            }
            CausalGrantError::InvalidOwnerRevocationBarrier { index, grant } => {
                Self::CausalInvalidOwnerRevocationBarrier { index, grant }
            }
            CausalGrantError::NoActiveOwner => Self::CausalNoActiveOwner,
        }
    }
}

#[cfg(test)]
mod authority_tests;
