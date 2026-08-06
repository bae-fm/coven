//! Signed Circle roster streams and causal assignment reduction.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::causal_grants::{
    self, AuthorStreamId, CausalAssignment, CausalChange, CausalCoordinate, CausalEntry,
    CausalGrantConflict, CausalGrantError, CausalGrantStatus, GrantRetirements, GrantState,
    OwnerGrantBarrier,
};
use super::circle::{CircleId, CircleRole};
use super::membership::MembershipGrantId;
use super::store_commit::{ObjectHash, Signed, SignedBody, StoreDeviceRegistration, SuccessorLink};
use crate::protocol::objects::ExactObjectRef;
use coven_keys::keys::{self, UserKeypair};

mod chain;
mod conflict;
mod reduction;

pub(crate) use chain::CircleRosterChain;
#[cfg(test)]
pub(crate) use conflict::CircleRosterConflictResolutionBody;
pub(crate) use conflict::CircleRosterConflictResolutionRef;
pub(crate) use conflict::{
    derive_circle_resolution_grant, resolve_circle_roster_conflict, CircleMaterializedRoster,
    CircleRosterBranch, CircleRosterConflict, CircleRosterConflictResolution, CircleRosterStatus,
    ResolvedCircleRoster,
};

const ROSTER_DOMAIN: &[u8] = b"coven.circle-roster.v1\0";
const ROSTER_HEAD_DOMAIN: &[u8] = b"coven.circle-roster-head.v1\0";
const ROSTER_RESOLUTION_DOMAIN: &[u8] = b"coven.circle-roster-conflict-resolution.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterCoord {
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub entry_hash: ObjectHash,
}

impl CircleRosterCoord {
    pub fn stream_key(&self) -> CircleAuthorStreamKey {
        CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
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
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleOwnerGrantBarrier {
    pub observed_streams: Vec<CircleRosterCoord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleRosterChange {
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
    ResolutionActivation {
        resolution: CircleRosterConflictResolutionRef,
    },
}

/// The wire body of one Circle roster entry. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleRosterEntryBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleRosterCoord>,
    pub resolution_dependencies: Vec<CircleRosterConflictResolutionRef>,
    pub change: CircleRosterChange,
}

impl SignedBody for CircleRosterEntryBody {
    const DOMAIN: &'static [u8] = ROSTER_DOMAIN;
}

pub(crate) type CircleRosterEntry = Signed<CircleRosterEntryBody>;

impl CircleRosterEntry {
    pub(crate) fn founder(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        device_id: &str,
        stream_id: AuthorStreamId,
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
                stream_id,
                author_owner_grant: owner_grant.clone(),
                seq: 1,
                previous_hash: None,
                dependencies: Vec::new(),
                resolution_dependencies: Vec::new(),
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

    pub(crate) fn coord(&self) -> CircleRosterCoord {
        CircleRosterCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            entry_hash: self.entry_hash(),
        }
    }

    pub(crate) fn verify(&self) -> bool {
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
                    && self.resolution_dependencies.is_empty()
                    && member_pubkey == &self.author_pubkey
                    && grant_id == &self.author_owner_grant
            }
            CircleRosterChange::ResolutionActivation { .. } => causal_grants::starts_author_stream(
                self.seq,
                self.previous_hash,
                &own_stream,
                dependency_streams(),
            ),
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
            && self
                .resolution_dependencies
                .windows(2)
                .all(|pair| pair[0] < pair[1])
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
                CircleRosterChange::ResolutionActivation { resolution } => {
                    resolution.resolver_pubkey == self.author_pubkey
                        && self.author_owner_grant
                            == derive_circle_resolution_grant(
                                &resolution.conflict_hash,
                                &resolution.resolver_pubkey,
                            )
                        && self
                            .resolution_dependencies
                            .binary_search(resolution)
                            .is_ok()
                }
            }
            && self.verify_by(&self.author_pubkey).is_ok()
    }
}

/// The wire body of one Circle roster head. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleRosterHeadBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub tip_hash: ObjectHash,
    pub tip: ExactObjectRef,
    pub successor: SuccessorLink,
    pub resolutions: Vec<CircleRosterConflictResolutionRef>,
}

impl SignedBody for CircleRosterHeadBody {
    const DOMAIN: &'static [u8] = ROSTER_HEAD_DOMAIN;
}

pub(crate) type CircleRosterHead = Signed<CircleRosterHeadBody>;

impl CircleRosterHead {
    pub(crate) fn signed(
        entry: &CircleRosterEntry,
        tip: ExactObjectRef,
        successor: SuccessorLink,
        signer: &UserKeypair,
    ) -> Self {
        Self::signed_with_resolutions(
            entry,
            tip,
            successor,
            entry.resolution_dependencies.clone(),
            signer,
        )
    }

    pub(crate) fn signed_with_resolutions(
        entry: &CircleRosterEntry,
        tip: ExactObjectRef,
        successor: SuccessorLink,
        resolutions: Vec<CircleRosterConflictResolutionRef>,
        signer: &UserKeypair,
    ) -> Self {
        Signed::sign(
            CircleRosterHeadBody {
                store_root_hash: entry.store_root_hash,
                circle_id: entry.circle_id,
                author_pubkey: entry.author_pubkey.clone(),
                device_id: entry.device_id.clone(),
                stream_id: entry.stream_id,
                author_owner_grant: entry.author_owner_grant.clone(),
                seq: entry.seq,
                tip_hash: entry.entry_hash(),
                tip,
                successor,
                resolutions,
            },
            signer,
        )
    }

    pub(crate) fn head_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub(crate) fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        self.seq > 0
            && !self.device_id.is_empty()
            && self.device_id == registration.device_id.to_string()
            && self.resolutions.windows(2).all(|pair| pair[0] < pair[1])
            && self.verify_by(&registration.device_signing_pubkey).is_ok()
    }
    pub(crate) fn entry_coord(&self) -> CircleRosterCoord {
        CircleRosterCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            entry_hash: self.tip_hash,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterHeadRef {
    pub coord: CircleRosterCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleRosterHeadRef {
    pub(crate) fn from_stored_head(head: &CircleRosterHead, object: ExactObjectRef) -> Self {
        Self {
            coord: head.entry_coord(),
            head_hash: head.head_hash(),
            object,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExactCircleRosterHead {
    head: CircleRosterHead,
    reference: CircleRosterHeadRef,
}

impl ExactCircleRosterHead {
    pub(crate) fn bind(
        head: CircleRosterHead,
        reference: CircleRosterHeadRef,
    ) -> Result<Self, CircleRosterError> {
        if CircleRosterHeadRef::from_stored_head(&head, reference.object.clone()) != reference {
            return Err(CircleRosterError::MissingConflictHeads);
        }
        Ok(Self { head, reference })
    }

    pub(crate) fn head(&self) -> &CircleRosterHead {
        &self.head
    }

    pub(crate) fn reference(&self) -> &CircleRosterHeadRef {
        &self.reference
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeCircleRosterStateRef {
    pub heads: Vec<CircleRosterHeadRef>,
    pub resolutions: Vec<CircleRosterConflictResolutionRef>,
    pub state_hash: ObjectHash,
}

pub(crate) type CircleRosterStateRef = MergeCircleRosterStateRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleGrantRecord {
    pub member_pubkey: String,
    pub role: CircleRole,
    pub creation_authority: CircleGrantCreationAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleGrantCreationAuthority {
    Entry(CircleRosterCoord),
    ConflictResolution(CircleRosterConflictResolutionRef),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CircleGrantRetirement {
    Entry {
        authority: CircleRosterCoord,
        owner_barrier: Option<CircleOwnerGrantBarrier>,
    },
    ConflictResolution(CircleRosterConflictResolutionRef),
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
    #[error("Circle roster author stream contains a pruned suffix and cannot be extended")]
    PrunedAuthorStream,
    #[error("Circle roster sequence {current} has no representable successor")]
    SequenceExhausted { current: u64 },
    #[error("Circle roster has an unresolved semantic conflict")]
    Conflict,
    #[error("Circle roster conflict is missing its exact signed raw heads")]
    MissingConflictHeads,
    #[error("Circle roster conflict resolution does not name exact validated conflict evidence")]
    InvalidConflictResolution,
    #[error("checkpoint lacks the exact record for Circle grant {grant}")]
    MissingCheckpointGrant { grant: MembershipGrantId },
    #[error("checkpoint lacks retirement evidence for Circle grant {grant}")]
    MissingCheckpointRetirementEvidence { grant: MembershipGrantId },
    #[error("Circle roster causal history: {0}")]
    Causal(String),
    #[error(
        "Circle roster revocation cycle has {sources} sources, exceeding the protocol limit of {maximum}"
    )]
    RevocationCycleTooWide { sources: usize, maximum: usize },
}

impl From<CausalGrantError<CircleRosterCoord>> for CircleRosterError {
    fn from(error: CausalGrantError<CircleRosterCoord>) -> Self {
        match error {
            CausalGrantError::RevocationCycleTooWide { sources, maximum } => {
                Self::RevocationCycleTooWide { sources, maximum }
            }
            error => Self::Causal(error.to_string()),
        }
    }
}

#[cfg(test)]
mod authority_tests;
