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
use super::storage::ExactObjectRef;
use super::store_commit::{
    ObjectHash, StoreDeviceRegistration, SuccessorLink, STORE_PROTOCOL_VERSION,
};
use crate::keys::{self, UserKeypair};

const ROSTER_DOMAIN: &str = "coven.circle-roster.v1";
const ROSTER_HEAD_DOMAIN: &str = "coven.circle-roster-head.v1";

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
    ResolutionActivation {
        resolution: CircleRosterConflictResolutionRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterEntry {
    pub version: u32,
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
    pub signature: String,
}

impl CircleRosterEntry {
    pub(crate) fn founder(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        device_id: &str,
        stream_id: AuthorStreamId,
        owner_grant: MembershipGrantId,
        signer: &UserKeypair,
    ) -> Self {
        let author_pubkey = keys::public_key_hex(signer);
        let mut entry = Self {
            version: STORE_PROTOCOL_VERSION,
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
            signature: String::new(),
        };
        entry.signature = keys::sign_hex(signer, &entry.canonical_bytes()).1;
        entry
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            author_pubkey: &'a str,
            device_id: &'a str,
            stream_id: AuthorStreamId,
            author_owner_grant: &'a MembershipGrantId,
            seq: u64,
            previous_hash: Option<ObjectHash>,
            dependencies: &'a [CircleRosterCoord],
            resolution_dependencies: &'a [CircleRosterConflictResolutionRef],
            change: &'a CircleRosterChange,
        }
        serde_json::to_vec(&Signed {
            domain: ROSTER_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
            stream_id: self.stream_id,
            author_owner_grant: &self.author_owner_grant,
            seq: self.seq,
            previous_hash: self.previous_hash,
            dependencies: &self.dependencies,
            resolution_dependencies: &self.resolution_dependencies,
            change: &self.change,
        })
        .expect("circle roster entry serialization cannot fail")
    }

    pub fn entry_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle roster entry serialization cannot fail"),
        )
    }

    pub fn coord(&self) -> CircleRosterCoord {
        CircleRosterCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            entry_hash: self.entry_hash(),
        }
    }

    pub fn verify(&self) -> bool {
        let position_is_valid = match (&self.change, self.seq, self.previous_hash) {
            (
                CircleRosterChange::Founder {
                    member_pubkey,
                    grant_id,
                    ..
                },
                1,
                None,
            ) => {
                self.dependencies.is_empty()
                    && self.resolution_dependencies.is_empty()
                    && member_pubkey == &self.author_pubkey
                    && grant_id == &self.author_owner_grant
            }
            (CircleRosterChange::Founder { .. }, _, _) => false,
            (CircleRosterChange::ResolutionActivation { .. }, 1, None) => self
                .dependencies
                .iter()
                .all(|dependency| dependency.stream_key() != self.coord().stream_key()),
            (CircleRosterChange::ResolutionActivation { .. }, _, _) => false,
            (_, 1, None) => self
                .dependencies
                .iter()
                .all(|dependency| dependency.stream_key() != self.coord().stream_key()),
            (_, seq, Some(_)) => seq > 1,
            (_, _, None) => false,
        };
        self.version == STORE_PROTOCOL_VERSION
            && !self.author_pubkey.is_empty()
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
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterHead {
    pub version: u32,
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
    pub signature: String,
}

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
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
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
            signature: String::new(),
        };
        head.signature = keys::sign_hex(signer, &head.canonical_bytes()).1;
        head
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            author_pubkey: &'a str,
            device_id: &'a str,
            stream_id: AuthorStreamId,
            author_owner_grant: &'a MembershipGrantId,
            seq: u64,
            tip_hash: ObjectHash,
            tip: &'a ExactObjectRef,
            successor: &'a SuccessorLink,
            resolutions: &'a [CircleRosterConflictResolutionRef],
        }
        serde_json::to_vec(&Signed {
            domain: ROSTER_HEAD_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
            stream_id: self.stream_id,
            author_owner_grant: &self.author_owner_grant,
            seq: self.seq,
            tip_hash: self.tip_hash,
            tip: &self.tip,
            successor: &self.successor,
            resolutions: &self.resolutions,
        })
        .expect("circle roster head serialization cannot fail")
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle roster head serialization cannot fail"),
        )
    }

    pub fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.seq > 0
            && !self.device_id.is_empty()
            && self.device_id == registration.device_id.to_string()
            && self.resolutions.windows(2).all(|pair| pair[0] < pair[1])
            && keys::verify_signature_hex(
                &registration.device_signing_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
    pub fn entry_coord(&self) -> CircleRosterCoord {
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
pub struct MergeCircleRosterStateRef {
    pub heads: Vec<CircleRosterHeadRef>,
    pub resolutions: Vec<CircleRosterConflictResolutionRef>,
    pub state_hash: ObjectHash,
}

pub type CircleRosterStateRef = MergeCircleRosterStateRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleGrantRecord {
    pub member_pubkey: String,
    pub role: CircleRole,
    pub creation_authority: CircleGrantCreationAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleGrantCreationAuthority {
    Entry(CircleRosterCoord),
    ConflictResolution(CircleRosterConflictResolutionRef),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleGrantRetirement {
    Entry {
        authority: CircleRosterCoord,
        owner_barrier: Option<CircleOwnerGrantBarrier>,
    },
    ConflictResolution(CircleRosterConflictResolutionRef),
}

fn active_circle_grants(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
    grants
        .iter()
        .filter_map(|(grant, state)| state.active().map(|record| (grant, record)))
}

fn roster_members(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> BTreeMap<String, CircleRole> {
    active_circle_grants(grants)
        .map(|(_, record)| record)
        .map(|record| (record.member_pubkey.clone(), record.role))
        .collect()
}

fn roster_owners(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> Vec<String> {
    let mut owners = active_circle_grants(grants)
        .map(|(_, record)| record)
        .filter(|record| record.role == CircleRole::Owner)
        .map(|record| record.member_pubkey.clone())
        .collect::<Vec<_>>();
    owners.sort();
    owners
}

fn roster_authorizes_owner_grant(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    author_pubkey: &str,
    grant_id: &MembershipGrantId,
) -> bool {
    grants
        .get(grant_id)
        .and_then(GrantState::active)
        .is_some_and(|record| {
            record.member_pubkey == author_pubkey && record.role == CircleRole::Owner
        })
}

fn roster_grants_are_valid(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> bool {
    active_circle_grants(grants)
        .map(|(_, record)| record)
        .any(|record| record.role == CircleRole::Owner)
        && roster_members(grants).len() == active_circle_grants(grants).count()
}

fn circle_roster_state_hash(
    grants: &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        grants:
            &'a BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.circle-roster-state.v2",
            grants,
        })
        .expect("circle roster state serialization cannot fail"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedCircleRoster {
    pub grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterBranch {
    pub heads: Vec<CircleRosterHeadRef>,
    pub effective_frontier: Vec<CircleRosterCoord>,
    pub grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    pub state_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleRosterConflict {
    ConcurrentMemberAssignments {
        conflict_hash: ObjectHash,
        heads: Vec<CircleRosterHeadRef>,
        effective_frontier: Vec<CircleRosterCoord>,
        member_pubkey: String,
        conflicting_grants: BTreeMap<MembershipGrantId, CircleGrantRecord>,
        uncontested_grants: BTreeMap<MembershipGrantId, CircleGrantRecord>,
    },
    RevocationCycle {
        conflict_hash: ObjectHash,
        heads: Vec<CircleRosterHeadRef>,
        cyclic_sources: Vec<CircleRosterCoord>,
        involved_owner_grants: BTreeSet<MembershipGrantId>,
        maximal_valid_branches: Vec<CircleRosterBranch>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleRosterStatus {
    Resolved(ResolvedCircleRoster),
    Conflict(CircleRosterConflict),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterConflictResolutionRef {
    pub conflict_hash: ObjectHash,
    pub resolver_pubkey: String,
    pub resolution_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleRosterConflictResolution {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub conflict_hash: ObjectHash,
    pub conflicting_heads: Vec<CircleRosterHeadRef>,
    pub retired_owner_grants: BTreeSet<MembershipGrantId>,
    pub resolver_pubkey: String,
    pub resolver_branch_heads: Vec<CircleRosterHeadRef>,
    pub replacement_grant: MembershipGrantId,
    pub signature: String,
}

impl CircleRosterConflictResolution {
    fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            conflict_hash: ObjectHash,
            conflicting_heads: &'a [CircleRosterHeadRef],
            retired_owner_grants: &'a BTreeSet<MembershipGrantId>,
            resolver_pubkey: &'a str,
            resolver_branch_heads: &'a [CircleRosterHeadRef],
            replacement_grant: &'a MembershipGrantId,
        }
        serde_json::to_vec(&Signed {
            domain: "coven.circle-roster-conflict-resolution.v1",
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            conflict_hash: self.conflict_hash,
            conflicting_heads: &self.conflicting_heads,
            retired_owner_grants: &self.retired_owner_grants,
            resolver_pubkey: &self.resolver_pubkey,
            resolver_branch_heads: &self.resolver_branch_heads,
            replacement_grant: &self.replacement_grant,
        })
        .expect("Circle roster resolution serialization cannot fail")
    }

    pub fn resolution_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("Circle roster resolution serialization cannot fail"),
        )
    }

    pub fn resolution_ref(&self) -> CircleRosterConflictResolutionRef {
        CircleRosterConflictResolutionRef {
            conflict_hash: self.conflict_hash,
            resolver_pubkey: self.resolver_pubkey.clone(),
            resolution_hash: self.resolution_hash(),
        }
    }

    pub fn verify_signature(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.replacement_grant
                == derive_circle_resolution_grant(&self.conflict_hash, &self.resolver_pubkey)
            && keys::verify_signature_hex(
                &self.resolver_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }

    pub fn verify_against(
        &self,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        conflict: &CircleRosterConflict,
    ) -> bool {
        let CircleRosterConflict::RevocationCycle {
            conflict_hash,
            heads,
            involved_owner_grants,
            maximal_valid_branches,
            ..
        } = conflict
        else {
            return false;
        };
        let Some(branch) = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == self.resolver_branch_heads)
        else {
            return false;
        };
        let mut expected_retired = involved_owner_grants.clone();
        expected_retired.extend(active_circle_grants(&branch.grants).filter_map(
            |(grant, record)| {
                (record.member_pubkey == self.resolver_pubkey && record.role == CircleRole::Owner)
                    .then_some(grant.clone())
            },
        ));
        self.version == STORE_PROTOCOL_VERSION
            && self.store_root_hash == store_root_hash
            && self.circle_id == circle_id
            && self.conflict_hash == *conflict_hash
            && self.conflicting_heads == *heads
            && self.retired_owner_grants == expected_retired
            && self.replacement_grant
                == derive_circle_resolution_grant(conflict_hash, &self.resolver_pubkey)
            && active_circle_grants(&branch.grants).any(|(_, record)| {
                record.member_pubkey == self.resolver_pubkey && record.role == CircleRole::Owner
            })
            && self.verify_signature()
    }
}

pub fn derive_circle_resolution_grant(
    conflict_hash: &ObjectHash,
    resolver_pubkey: &str,
) -> MembershipGrantId {
    MembershipGrantId(ObjectHash::digest(
        format!("coven.circle-roster-resolution-grant.v1\0{conflict_hash}\0{resolver_pubkey}")
            .as_bytes(),
    ))
}

pub fn resolve_circle_roster_conflict(
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    conflict: &CircleRosterConflict,
    resolutions: &[CircleRosterConflictResolution],
) -> Result<ResolvedCircleRoster, CircleRosterError> {
    let CircleRosterConflict::RevocationCycle {
        maximal_valid_branches,
        ..
    } = conflict
    else {
        return Err(CircleRosterError::InvalidConflictResolution);
    };
    if resolutions.is_empty() {
        return Err(CircleRosterError::InvalidConflictResolution);
    }
    let mut by_resolver = BTreeMap::new();
    let mut selected_branches = Vec::new();
    let mut retired_owner_grants = BTreeSet::new();
    for resolution in resolutions {
        if !resolution.verify_against(store_root_hash, circle_id, conflict) {
            return Err(CircleRosterError::InvalidConflictResolution);
        }
        let resolution_hash = resolution.resolution_hash();
        if let Some(existing) =
            by_resolver.insert(resolution.resolver_pubkey.clone(), resolution_hash)
        {
            if existing != resolution_hash {
                return Err(CircleRosterError::InvalidConflictResolution);
            }
            continue;
        }
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == resolution.resolver_branch_heads)
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        if !selected_branches
            .iter()
            .any(|selected: &&CircleRosterBranch| selected.heads == branch.heads)
        {
            selected_branches.push(branch);
        }
        retired_owner_grants.extend(resolution.retired_owner_grants.iter().cloned());
    }
    let (first_branch, other_branches) = selected_branches
        .split_first()
        .ok_or(CircleRosterError::InvalidConflictResolution)?;
    let mut grants = active_circle_grants(&first_branch.grants)
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
    for branch in maximal_valid_branches {
        for (grant, state) in &branch.grants {
            let GrantState::Tombstoned {
                record,
                retirements,
            } = state
            else {
                continue;
            };
            match grants.entry(grant.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(state.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().record() != record {
                        return Err(CircleRosterError::InvalidConflictResolution);
                    }
                    let mut merged = retirements.clone();
                    if let Some(current_retirements) = entry.get().retirements() {
                        merged.extend(current_retirements.iter().cloned());
                    }
                    *entry.get_mut() = GrantState::Tombstoned {
                        record: record.clone(),
                        retirements: merged,
                    };
                }
            }
        }
    }
    let mut resolution_retirements = resolutions
        .iter()
        .map(|resolution| CircleGrantRetirement::ConflictResolution(resolution.resolution_ref()));
    let mut resolution_retirements = GrantRetirements::new(
        resolution_retirements
            .next()
            .expect("validated conflict has a resolution"),
    );
    resolution_retirements.extend(
        resolutions.iter().skip(1).map(|resolution| {
            CircleGrantRetirement::ConflictResolution(resolution.resolution_ref())
        }),
    );
    for branch in maximal_valid_branches {
        for (grant, record) in branch.active_grants() {
            if grants.get(grant).and_then(GrantState::active).is_some() {
                continue;
            }
            match grants.entry(grant.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(GrantState::Tombstoned {
                        record: record.clone(),
                        retirements: resolution_retirements.clone(),
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().record() != record {
                        return Err(CircleRosterError::InvalidConflictResolution);
                    }
                    let GrantState::Tombstoned { retirements, .. } = entry.get_mut() else {
                        unreachable!("active conflict grant was handled above")
                    };
                    retirements.extend(resolution_retirements.iter().cloned());
                }
            }
        }
    }
    for resolution in resolutions {
        let reference = resolution.resolution_ref();
        for retired in &resolution.retired_owner_grants {
            let record = selected_branches
                .iter()
                .find_map(|branch| branch.grants.get(retired).map(GrantState::record))
                .ok_or(CircleRosterError::InvalidConflictResolution)?
                .clone();
            let retirement = CircleGrantRetirement::ConflictResolution(reference.clone());
            match grants.entry(retired.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(GrantState::Tombstoned {
                        record,
                        retirements: GrantRetirements::new(retirement),
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if entry.get().record() != &record {
                        return Err(CircleRosterError::InvalidConflictResolution);
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
    for resolution in resolutions {
        let record = CircleGrantRecord {
            member_pubkey: resolution.resolver_pubkey.clone(),
            role: CircleRole::Owner,
            creation_authority: CircleGrantCreationAuthority::ConflictResolution(
                resolution.resolution_ref(),
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
            return Err(CircleRosterError::InvalidConflictResolution);
        }
    }
    if !roster_grants_are_valid(&grants) {
        return Err(CircleRosterError::InvalidConflictResolution);
    }
    Ok(ResolvedCircleRoster {
        state_hash: circle_roster_state_hash(&grants),
        grants,
    })
}

impl ResolvedCircleRoster {
    pub fn state_hash(&self) -> ObjectHash {
        self.state_hash
    }

    pub fn members(&self) -> BTreeMap<String, CircleRole> {
        roster_members(&self.grants)
    }

    pub fn owners(&self) -> Vec<String> {
        roster_owners(&self.grants)
    }

    pub fn authorizes_owner_grant(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
        created_at: &CircleRosterCoord,
    ) -> bool {
        self.authorizes_owner_grant_id(author_pubkey, grant_id)
            && self
                .grants
                .get(grant_id)
                .and_then(GrantState::active)
                .is_some_and(|record| {
                    record.creation_authority
                        == CircleGrantCreationAuthority::Entry(created_at.clone())
                })
    }

    pub fn authorizes_resolution_grant(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
        resolution: &CircleRosterConflictResolutionRef,
    ) -> bool {
        self.authorizes_owner_grant_id(author_pubkey, grant_id)
            && self
                .grants
                .get(grant_id)
                .and_then(GrantState::active)
                .is_some_and(|record| {
                    record.creation_authority
                        == CircleGrantCreationAuthority::ConflictResolution(resolution.clone())
                })
    }

    pub fn authorizes_owner_grant_id(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
    ) -> bool {
        roster_authorizes_owner_grant(&self.grants, author_pubkey, grant_id)
    }

    pub fn verify(&self) -> bool {
        self.state_hash == circle_roster_state_hash(&self.grants)
            && roster_grants_are_valid(&self.grants)
    }

    pub fn active_grants(&self) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
        active_circle_grants(&self.grants)
    }
}

impl CircleRosterBranch {
    pub fn active_grants(&self) -> impl Iterator<Item = (&MembershipGrantId, &CircleGrantRecord)> {
        active_circle_grants(&self.grants)
    }
}

pub type CircleMaterializedRoster = ResolvedCircleRoster;

#[derive(Debug, Clone)]
pub struct CircleRosterChain {
    entries: Vec<CircleRosterEntry>,
    reduced: Option<causal_grants::ReducedGrants<CircleRosterCoord, CircleRole>>,
    status: CircleRosterStatus,
    head_refs: Vec<CircleRosterHeadRef>,
    resolution_checkpoint: Option<CircleRosterResolutionCheckpoint>,
}

#[derive(Debug, Clone)]
struct CircleRosterResolutionCheckpoint {
    raw_heads: Vec<CircleRosterCoord>,
    effective_frontier: Vec<CircleRosterCoord>,
    grants: BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    included: BTreeSet<CircleRosterCoord>,
    resolutions: Vec<CircleRosterConflictResolutionRef>,
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
    #[error("Circle roster resolution activation requires a fresh persisted author stream")]
    ResolutionActivationRequiresFreshStream,
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

impl CircleRosterChain {
    pub fn from_entries(entries: Vec<CircleRosterEntry>) -> Result<Self, CircleRosterError> {
        Self::from_entries_and_head_refs(entries, Vec::new())
    }

    pub(crate) fn from_entries_with_heads(
        entries: Vec<CircleRosterEntry>,
        heads: Vec<ExactCircleRosterHead>,
    ) -> Result<Self, CircleRosterError> {
        let head_refs = Self::validate_exact_heads(&entries, &heads)?;
        Self::from_entries_and_head_refs(entries, head_refs)
    }

    pub(crate) fn validate_exact_heads(
        entries: &[CircleRosterEntry],
        heads: &[ExactCircleRosterHead],
    ) -> Result<Vec<CircleRosterHeadRef>, CircleRosterError> {
        let founder = entries.first().ok_or(CircleRosterError::Empty)?;
        if heads.iter().any(|bound| {
            let head = bound.head();
            let reference = bound.reference();
            head.store_root_hash != founder.store_root_hash
                || head.circle_id != founder.circle_id
                || head.entry_coord() != reference.coord
                || entries
                    .iter()
                    .find(|entry| entry.coord() == reference.coord)
                    .is_none_or(|entry| head.resolutions != entry.resolution_dependencies)
        }) {
            return Err(CircleRosterError::MissingConflictHeads);
        }
        Ok(heads.iter().map(|head| head.reference().clone()).collect())
    }

    fn from_entries_and_head_refs(
        entries: Vec<CircleRosterEntry>,
        head_refs: Vec<CircleRosterHeadRef>,
    ) -> Result<Self, CircleRosterError> {
        Self::from_entries_head_refs_and_checkpoint(entries, head_refs, None)
    }

    fn from_entries_head_refs_and_checkpoint(
        entries: Vec<CircleRosterEntry>,
        head_refs: Vec<CircleRosterHeadRef>,
        resolution_checkpoint: Option<CircleRosterResolutionCheckpoint>,
    ) -> Result<Self, CircleRosterError> {
        let founder = entries.first().ok_or(CircleRosterError::Empty)?;
        let expected_store = founder.store_root_hash;
        let expected_circle = founder.circle_id;
        for (index, entry) in entries.iter().enumerate() {
            if !entry.verify() {
                return Err(CircleRosterError::InvalidEntry(index));
            }
            if entry.store_root_hash != expected_store || entry.circle_id != expected_circle {
                return Err(CircleRosterError::ContextMismatch { index });
            }
            if matches!(
                entry.change,
                CircleRosterChange::ResolutionActivation { .. }
            ) && resolution_checkpoint.as_ref().is_none_or(|checkpoint| {
                let already_checkpointed = checkpoint.included.contains(&entry.coord())
                    || checkpoint.raw_heads.contains(&entry.coord());
                !already_checkpointed
                    && (entry.dependencies != checkpoint.effective_frontier
                        || entry.resolution_dependencies != checkpoint.resolutions)
            }) {
                return Err(CircleRosterError::InvalidEntry(index));
            }
        }
        let checkpoint_by_stream = resolution_checkpoint.as_ref().map(|checkpoint| {
            checkpoint
                .raw_heads
                .iter()
                .map(|coord| (coord.stream_key(), coord))
                .collect::<BTreeMap<_, _>>()
        });
        let reduction_entries = entries.iter().filter(|entry| {
            checkpoint_by_stream.as_ref().is_none_or(|heads| {
                heads
                    .get(&entry.coord().stream_key())
                    .is_none_or(|head| entry.seq > head.seq)
            })
        });
        let normalized = reduction_entries
            .map(|entry| CausalEntry {
                coord: entry.coord(),
                previous_hash: entry.previous_hash,
                dependencies: entry
                    .dependencies
                    .iter()
                    .cloned()
                    .map(|coord| (coord.stream_key(), coord))
                    .collect(),
                change: match &entry.change {
                    CircleRosterChange::Founder {
                        member_pubkey,
                        grant_id,
                    } => CausalChange::Founder {
                        member_pubkey: member_pubkey.clone(),
                        grant_id: grant_id.clone(),
                        assignment: CircleRole::Owner,
                    },
                    CircleRosterChange::SetMember {
                        member_pubkey,
                        role,
                        grant_id,
                        replaces,
                        owner_barriers,
                    } => CausalChange::SetMember {
                        member_pubkey: member_pubkey.clone(),
                        assignment: *role,
                        grant_id: grant_id.clone(),
                        replaces: replaces.clone(),
                        owner_barriers: owner_barriers
                            .iter()
                            .map(|(grant, barrier)| {
                                (
                                    grant.clone(),
                                    OwnerGrantBarrier {
                                        observed_streams: barrier
                                            .observed_streams
                                            .iter()
                                            .cloned()
                                            .map(|coord| (coord.stream_key(), coord))
                                            .collect(),
                                    },
                                )
                            })
                            .collect(),
                    },
                    CircleRosterChange::RemoveMember {
                        member_pubkey,
                        removes,
                        owner_barriers,
                    } => CausalChange::RemoveMember {
                        member_pubkey: member_pubkey.clone(),
                        removes: removes.clone(),
                        owner_barriers: owner_barriers
                            .iter()
                            .map(|(grant, barrier)| {
                                (
                                    grant.clone(),
                                    OwnerGrantBarrier {
                                        observed_streams: barrier
                                            .observed_streams
                                            .iter()
                                            .cloned()
                                            .map(|coord| (coord.stream_key(), coord))
                                            .collect(),
                                    },
                                )
                            })
                            .collect(),
                    },
                    CircleRosterChange::ResolutionActivation { .. } => {
                        CausalChange::ResolutionActivation
                    }
                },
            })
            .collect::<Vec<_>>();
        let reduction = match &resolution_checkpoint {
            Some(checkpoint) => {
                let seeds = checkpoint
                    .grants
                    .iter()
                    .map(|(grant, state)| {
                        let record = state.record();
                        let record = causal_grants::CausalSeedGrant {
                            member_pubkey: record.member_pubkey.clone(),
                            assignment: record.role,
                        };
                        (
                            grant.clone(),
                            match state {
                                GrantState::Active { .. } => GrantState::Active { record },
                                GrantState::Tombstoned { .. } => GrantState::Tombstoned {
                                    record,
                                    retirements: GrantRetirements::new(()),
                                },
                            },
                        )
                    })
                    .collect();
                causal_grants::reduce_from_checkpoint(
                    &normalized,
                    &checkpoint.raw_heads,
                    &checkpoint.effective_frontier,
                    &seeds,
                    &checkpoint.included,
                )?
            }
            None => causal_grants::reduce(&normalized)?,
        };
        let founder_entry = entries
            .iter()
            .find(|entry| matches!(entry.change, CircleRosterChange::Founder { .. }))
            .expect("shared reducer requires one founder");
        if founder_entry.circle_id
            != CircleId::founder(
                founder_entry.store_root_hash,
                &founder_entry.author_pubkey,
                &founder_entry.author_owner_grant,
            )
        {
            return Err(CircleRosterError::InvalidFounderIdentity);
        }
        let (reduced, status) = match reduction {
            CausalGrantStatus::Resolved(reduced) => {
                let resolved = resolved_circle_roster(
                    &reduced,
                    resolution_checkpoint
                        .as_ref()
                        .map(|checkpoint| &checkpoint.grants),
                )?;
                (Some(reduced), CircleRosterStatus::Resolved(resolved))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads,
                effective_frontier,
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            }) => {
                let heads = exact_circle_head_refs(&head_refs, &raw_heads)?;
                let conflict_hash = circle_assignment_conflict_hash(
                    expected_store,
                    expected_circle,
                    &heads,
                    &member_pubkey,
                    &conflicting_grants,
                );
                (
                    Some(reduced),
                    CircleRosterStatus::Conflict(
                        CircleRosterConflict::ConcurrentMemberAssignments {
                            conflict_hash,
                            heads,
                            effective_frontier,
                            member_pubkey,
                            conflicting_grants: map_circle_grants(
                                conflicting_grants,
                                resolution_checkpoint
                                    .as_ref()
                                    .map(|checkpoint| &checkpoint.grants),
                            )?,
                            uncontested_grants: map_circle_grants(
                                uncontested_grants,
                                resolution_checkpoint
                                    .as_ref()
                                    .map(|checkpoint| &checkpoint.grants),
                            )?,
                        },
                    ),
                )
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::RevocationCycle {
                raw_heads,
                cyclic_sources,
                involved_owner_grants,
                maximal_valid_branches,
            }) => {
                let heads = exact_circle_head_refs(&head_refs, &raw_heads)?;
                let branches = maximal_valid_branches
                    .into_iter()
                    .map(|branch| -> Result<CircleRosterBranch, CircleRosterError> {
                        let resolved = resolved_circle_roster(
                            &branch.reduced,
                            resolution_checkpoint
                                .as_ref()
                                .map(|checkpoint| &checkpoint.grants),
                        )?;
                        Ok(CircleRosterBranch {
                            heads: exact_circle_head_refs(&head_refs, &branch.raw_heads)?,
                            effective_frontier: branch.effective_frontier,
                            grants: resolved.grants,
                            state_hash: resolved.state_hash,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let conflict_hash = circle_revocation_conflict_hash(
                    expected_store,
                    expected_circle,
                    &heads,
                    &cyclic_sources,
                    &involved_owner_grants,
                );
                (
                    None,
                    CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        cyclic_sources,
                        involved_owner_grants,
                        maximal_valid_branches: branches,
                    }),
                )
            }
        };
        Ok(Self {
            entries,
            reduced,
            status,
            head_refs,
            resolution_checkpoint,
        })
    }

    pub fn entries(&self) -> &[CircleRosterEntry] {
        &self.entries
    }

    pub fn status(&self) -> &CircleRosterStatus {
        &self.status
    }

    pub fn resolution_refs(&self) -> &[CircleRosterConflictResolutionRef] {
        self.resolution_checkpoint
            .as_ref()
            .map_or(&[], |checkpoint| checkpoint.resolutions.as_slice())
    }

    pub(crate) fn resolution_checkpoint_covers(&self, coord: &CircleRosterCoord) -> bool {
        self.resolution_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| {
                checkpoint.included.contains(coord) || checkpoint.raw_heads.contains(coord)
            })
    }

    pub(crate) fn replay_resolved_history_to_heads(
        &self,
        entries: Vec<CircleRosterEntry>,
        heads: Vec<CircleRosterHeadRef>,
    ) -> Result<Self, CircleRosterError> {
        let checkpoint = self
            .resolution_checkpoint
            .clone()
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        if heads.iter().any(|head| {
            entries
                .iter()
                .find(|entry| entry.coord() == head.coord)
                .is_none()
        }) {
            return Err(CircleRosterError::MissingConflictHeads);
        }
        Self::from_entries_head_refs_and_checkpoint(entries, heads, Some(checkpoint))
    }

    pub(crate) fn replay_merged_resolved_histories_to_heads(
        chains: &[&CircleRosterChain],
        entries: Vec<CircleRosterEntry>,
        heads: Vec<CircleRosterHeadRef>,
    ) -> Result<Self, CircleRosterError> {
        let mut raw_by_stream = BTreeMap::new();
        let mut effective_by_stream = BTreeMap::new();
        let mut grants = BTreeMap::new();
        let mut included = BTreeSet::new();
        let mut resolutions = BTreeSet::new();
        for chain in chains {
            let checkpoint = chain
                .resolution_checkpoint
                .as_ref()
                .ok_or(CircleRosterError::InvalidConflictResolution)?;
            if !causal_grants::merge_checkpoint_frontier(&mut raw_by_stream, &checkpoint.raw_heads)
                || !causal_grants::merge_checkpoint_frontier(
                    &mut effective_by_stream,
                    &checkpoint.effective_frontier,
                )
                || !causal_grants::merge_checkpoint_evidence(
                    &mut grants,
                    &mut included,
                    &checkpoint.grants,
                    &checkpoint.included,
                )
            {
                return Err(CircleRosterError::InvalidConflictResolution);
            }
            resolutions.extend(checkpoint.resolutions.iter().cloned());
        }
        let checkpoint = CircleRosterResolutionCheckpoint {
            raw_heads: raw_by_stream.into_values().collect(),
            effective_frontier: effective_by_stream.into_values().collect(),
            grants,
            included,
            resolutions: resolutions.into_iter().collect(),
        };
        let base = chains
            .first()
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        let mut merged = (*base).clone();
        merged.resolution_checkpoint = Some(checkpoint);
        merged.replay_resolved_history_to_heads(entries, heads)
    }

    pub(crate) fn checkpoint_current_resolved_state(&mut self) -> Result<(), CircleRosterError> {
        self.try_resolved()?;
        let resolutions = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.resolutions.clone());
        let checkpoint_grants = self
            .resolution_checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.grants);
        let reduced = self
            .reduced
            .as_ref()
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        let grants = reduced
            .grants
            .iter()
            .map(|(grant, state)| -> Result<_, CircleRosterError> {
                Ok((
                    grant.clone(),
                    map_circle_grant_state(grant, state, checkpoint_grants)?,
                ))
            })
            .collect::<Result<_, _>>()?;
        self.resolution_checkpoint = Some(CircleRosterResolutionCheckpoint {
            raw_heads: self.author_heads(),
            effective_frontier: self.effective_frontier(),
            grants,
            included: reduced.included.clone(),
            resolutions,
        });
        Ok(())
    }

    pub fn signed_cycle_resolution(
        &self,
        resolver_branch_heads: Vec<CircleRosterHeadRef>,
        signer: &UserKeypair,
    ) -> Result<CircleRosterConflictResolution, CircleRosterError> {
        let CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
            conflict_hash,
            heads,
            involved_owner_grants,
            maximal_valid_branches,
            ..
        }) = self.status()
        else {
            return Err(CircleRosterError::Conflict);
        };
        let resolver_pubkey = keys::public_key_hex(signer);
        let branch = maximal_valid_branches
            .iter()
            .find(|branch| branch.heads == resolver_branch_heads)
            .ok_or(CircleRosterError::InvalidConflictResolution)?;
        if !active_circle_grants(&branch.grants).any(|(_, record)| {
            record.member_pubkey == resolver_pubkey && record.role == CircleRole::Owner
        }) {
            return Err(CircleRosterError::SignerIsNotOwner(resolver_pubkey));
        }
        let replacement_grant = derive_circle_resolution_grant(conflict_hash, &resolver_pubkey);
        let mut retired_owner_grants = involved_owner_grants.clone();
        retired_owner_grants.extend(active_circle_grants(&branch.grants).filter_map(
            |(grant, record)| {
                (record.member_pubkey == resolver_pubkey && record.role == CircleRole::Owner)
                    .then_some(grant.clone())
            },
        ));
        let mut resolution = CircleRosterConflictResolution {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: self.entries[0].store_root_hash,
            circle_id: self.entries[0].circle_id,
            conflict_hash: *conflict_hash,
            conflicting_heads: heads.clone(),
            retired_owner_grants,
            resolver_pubkey,
            resolver_branch_heads,
            replacement_grant,
            signature: String::new(),
        };
        resolution.signature = keys::sign_hex(signer, &resolution.canonical_bytes()).1;
        Ok(resolution)
    }

    pub fn resolved(&self) -> ResolvedCircleRoster {
        self.try_resolved()
            .expect("caller must inspect Circle roster status before consuming resolved state")
    }

    pub fn try_resolved(&self) -> Result<ResolvedCircleRoster, CircleRosterError> {
        match &self.status {
            CircleRosterStatus::Resolved(resolved) => Ok(resolved.clone()),
            CircleRosterStatus::Conflict(_) => Err(CircleRosterError::Conflict),
        }
    }

    pub fn resolved_with(
        &self,
        resolutions: &[CircleRosterConflictResolution],
    ) -> Result<ResolvedCircleRoster, CircleRosterError> {
        match &self.status {
            CircleRosterStatus::Resolved(resolved) if resolutions.is_empty() => {
                Ok(resolved.clone())
            }
            CircleRosterStatus::Conflict(conflict) => resolve_circle_roster_conflict(
                self.entries[0].store_root_hash,
                self.entries[0].circle_id,
                conflict,
                resolutions,
            ),
            CircleRosterStatus::Resolved(_) => Err(CircleRosterError::InvalidConflictResolution),
        }
    }

    pub fn apply_resolutions(
        &mut self,
        resolutions: &[CircleRosterConflictResolution],
    ) -> Result<(), CircleRosterError> {
        let (raw_heads, effective_frontier) = match self.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                heads,
                maximal_valid_branches,
                ..
            }) => {
                let selected = resolutions
                    .iter()
                    .map(|resolution| {
                        maximal_valid_branches
                            .iter()
                            .find(|branch| branch.heads == resolution.resolver_branch_heads)
                            .map(|branch| branch.effective_frontier.as_slice())
                            .ok_or(CircleRosterError::InvalidConflictResolution)
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
            _ => return Err(CircleRosterError::InvalidConflictResolution),
        };
        let resolved = self.resolved_with(resolutions)?;
        let grants = resolved.grants.clone();
        let included = circle_history_closure(&self.entries, &effective_frontier);
        let mut resolution_refs = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(Vec::new, |checkpoint| checkpoint.resolutions.clone());
        resolution_refs.extend(
            resolutions
                .iter()
                .map(CircleRosterConflictResolution::resolution_ref),
        );
        resolution_refs.sort();
        resolution_refs.dedup();
        let checkpoint = CircleRosterResolutionCheckpoint {
            raw_heads,
            effective_frontier: effective_frontier.clone(),
            grants: grants.clone(),
            included: included.clone(),
            resolutions: resolution_refs,
        };
        self.reduced = Some(causal_grants::ReducedGrants {
            grants: grants
                .iter()
                .map(|(grant, state)| {
                    let concrete = state.record();
                    let record = causal_grants::GrantRecord {
                        member_pubkey: concrete.member_pubkey.clone(),
                        assignment: concrete.role,
                        creation: causal_grants::CausalGrantCreation::Checkpoint,
                    };
                    (
                        grant.clone(),
                        match state {
                            GrantState::Active { .. } => GrantState::Active { record },
                            GrantState::Tombstoned { .. } => GrantState::Tombstoned {
                                record,
                                retirements: GrantRetirements::new(
                                    causal_grants::CausalGrantRetirement::Checkpoint,
                                ),
                            },
                        },
                    )
                })
                .collect(),
            included: included.clone(),
        });
        self.status = CircleRosterStatus::Resolved(resolved);
        self.resolution_checkpoint = Some(checkpoint);
        Ok(())
    }

    pub fn author_heads(&self) -> Vec<CircleRosterCoord> {
        self.frontier_from_entries(self.entries.iter())
    }

    pub fn effective_frontier(&self) -> Vec<CircleRosterCoord> {
        let Some(reduced) = &self.reduced else {
            return Vec::new();
        };
        self.frontier_from_entries(
            self.entries
                .iter()
                .filter(|entry| reduced.includes_coord(&entry.coord())),
        )
    }

    fn frontier_from_entries<'a>(
        &self,
        entries: impl Iterator<Item = &'a CircleRosterEntry>,
    ) -> Vec<CircleRosterCoord> {
        let mut heads = BTreeMap::<CircleAuthorStreamKey, CircleRosterCoord>::new();
        for entry in entries {
            let coord = entry.coord();
            heads
                .entry(coord.stream_key())
                .and_modify(|current| {
                    if coord.seq > current.seq {
                        *current = coord.clone();
                    }
                })
                .or_insert(coord);
        }
        heads.into_values().collect()
    }

    fn active_grants(&self, member_pubkey: &str) -> BTreeSet<MembershipGrantId> {
        let reduced = self
            .reduced
            .as_ref()
            .expect("resolved roster has reduced grants");
        reduced
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                state
                    .active()
                    .is_some_and(|record| record.member_pubkey == member_pubkey)
                    .then_some(grant.clone())
            })
            .collect()
    }

    fn active_owner_grant(&self, member_pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants(member_pubkey).into_iter().find(|grant| {
            self.reduced
                .as_ref()
                .expect("resolved roster has reduced grants")
                .active_grant(grant)
                .is_some_and(|record| record.assignment == CircleRole::Owner)
        })
    }

    pub(crate) fn reusable_author_streams(
        &self,
        author_pubkey: &str,
        device_id: &str,
        grant: &MembershipGrantId,
    ) -> BTreeSet<AuthorStreamId> {
        self.effective_frontier()
            .into_iter()
            .filter(|effective_tip| {
                effective_tip.author_pubkey == author_pubkey
                    && effective_tip.device_id == device_id
                    && effective_tip.author_owner_grant == *grant
                    && self
                        .entries
                        .iter()
                        .map(CircleRosterEntry::coord)
                        .filter(|coord| coord.stream_key() == effective_tip.stream_key())
                        .max_by_key(|coord| coord.seq)
                        .as_ref()
                        == Some(effective_tip)
            })
            .map(|coord| coord.stream_id)
            .collect()
    }

    fn frontier(&self) -> Vec<CircleRosterCoord> {
        self.effective_frontier()
    }

    fn owner_barriers(
        &self,
        grants: &BTreeSet<MembershipGrantId>,
        dependencies: &[CircleRosterCoord],
    ) -> BTreeMap<MembershipGrantId, CircleOwnerGrantBarrier> {
        grants
            .iter()
            .filter(|grant| {
                self.reduced
                    .as_ref()
                    .expect("resolved roster has reduced grants")
                    .active_grant(grant)
                    .is_some_and(|record| record.assignment == CircleRole::Owner)
            })
            .map(|grant| {
                let observed_streams = dependencies
                    .iter()
                    .filter(|coord| coord.author_owner_grant == *grant)
                    .cloned()
                    .collect();
                (grant.clone(), CircleOwnerGrantBarrier { observed_streams })
            })
            .collect()
    }

    fn next_position(
        &self,
        stream: &CircleAuthorStreamKey,
    ) -> Result<(u64, Option<ObjectHash>), CircleRosterError> {
        let raw_tip = self
            .entries
            .iter()
            .map(CircleRosterEntry::coord)
            .filter(|coord| coord.stream_key() == *stream)
            .max_by_key(|coord| coord.seq);
        let effective_tip = self
            .effective_frontier()
            .into_iter()
            .find(|coord| coord.stream_key() == *stream);
        if raw_tip.is_some()
            && !self
                .reusable_author_streams(
                    &stream.author_pubkey,
                    &stream.device_id,
                    &stream.author_owner_grant,
                )
                .contains(&stream.stream_id)
        {
            return Err(CircleRosterError::PrunedAuthorStream);
        }
        match effective_tip {
            Some(tip) => Ok((
                tip.seq
                    .checked_add(1)
                    .ok_or(CircleRosterError::SequenceExhausted { current: tip.seq })?,
                Some(tip.entry_hash),
            )),
            None => Ok((1, None)),
        }
    }

    pub fn signed_set_member(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        role: CircleRole,
        signer: &UserKeypair,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        self.signed_change(device_id, stream_id, member_pubkey, Some(role), signer)
    }

    pub fn signed_remove_member(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        signer: &UserKeypair,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        if self.active_grants(&member_pubkey).is_empty() {
            return Err(CircleRosterError::NotAMember(member_pubkey));
        }
        self.signed_change(device_id, stream_id, member_pubkey, None, signer)
    }

    pub fn signed_resolution_activation(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        resolution: &CircleRosterConflictResolution,
        signer: &UserKeypair,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        let resolved_before = self.try_resolved()?;
        let reference = resolution.resolution_ref();
        let author_pubkey = keys::public_key_hex(signer);
        if !resolution.verify_signature()
            || resolution.store_root_hash != self.entries[0].store_root_hash
            || resolution.circle_id != self.entries[0].circle_id
            || reference.resolver_pubkey != author_pubkey
            || !self.resolution_refs().contains(&reference)
            || self.active_owner_grant(&author_pubkey) != Some(resolution.replacement_grant.clone())
        {
            return Err(CircleRosterError::InvalidConflictResolution);
        }
        let author_owner_grant = resolution.replacement_grant.clone();
        let stream = CircleAuthorStreamKey {
            author_pubkey: author_pubkey.clone(),
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant: author_owner_grant.clone(),
        };
        if self
            .entries
            .iter()
            .any(|entry| entry.coord().stream_key() == stream)
        {
            return Err(CircleRosterError::ResolutionActivationRequiresFreshStream);
        }
        let mut entry = CircleRosterEntry {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: self.entries[0].store_root_hash,
            circle_id: self.entries[0].circle_id,
            author_pubkey,
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant,
            seq: 1,
            previous_hash: None,
            dependencies: self.effective_frontier(),
            resolution_dependencies: self.resolution_refs().to_vec(),
            change: CircleRosterChange::ResolutionActivation {
                resolution: reference,
            },
            signature: String::new(),
        };
        entry.signature = keys::sign_hex(signer, &entry.canonical_bytes()).1;
        let mut candidate_history = self.entries.clone();
        candidate_history.push(entry.clone());
        let candidate = Self::from_entries_head_refs_and_checkpoint(
            candidate_history,
            self.head_refs.clone(),
            self.resolution_checkpoint.clone(),
        )?;
        if candidate.try_resolved()?.state_hash != resolved_before.state_hash {
            return Err(CircleRosterError::InvalidConflictResolution);
        }
        Ok(entry)
    }

    fn signed_change(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        role: Option<CircleRole>,
        signer: &UserKeypair,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
        if matches!(self.status, CircleRosterStatus::Conflict(_)) {
            return Err(CircleRosterError::Conflict);
        }
        let author_pubkey = keys::public_key_hex(signer);
        let author_owner_grant = self
            .active_owner_grant(&author_pubkey)
            .ok_or_else(|| CircleRosterError::SignerIsNotOwner(author_pubkey.clone()))?;
        let stream = CircleAuthorStreamKey {
            author_pubkey: author_pubkey.clone(),
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant: author_owner_grant.clone(),
        };
        let (seq, previous_hash) = self.next_position(&stream)?;
        let dependencies = self.frontier();
        let replaced = self.active_grants(&member_pubkey);
        let owner_barriers = self.owner_barriers(&replaced, &dependencies);
        let change = match role {
            Some(role) => CircleRosterChange::SetMember {
                member_pubkey: member_pubkey.clone(),
                role,
                grant_id: MembershipGrantId(ObjectHash::digest(
                    format!(
                        "coven.circle-roster-grant.v1\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
                        self.entries[0].circle_id,
                        author_pubkey,
                        device_id,
                        stream_id,
                        author_owner_grant,
                        seq,
                        member_pubkey
                    )
                    .as_bytes(),
                )),
                replaces: replaced,
                owner_barriers,
            },
            None => CircleRosterChange::RemoveMember {
                member_pubkey,
                removes: replaced,
                owner_barriers,
            },
        };
        let mut entry = CircleRosterEntry {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: self.entries[0].store_root_hash,
            circle_id: self.entries[0].circle_id,
            author_pubkey,
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant,
            seq,
            previous_hash,
            dependencies,
            resolution_dependencies: self.resolution_refs().to_vec(),
            change,
            signature: String::new(),
        };
        entry.signature = keys::sign_hex(signer, &entry.canonical_bytes()).1;
        let mut candidate_history = self.entries.clone();
        candidate_history.push(entry.clone());
        Self::from_entries_head_refs_and_checkpoint(
            candidate_history,
            self.head_refs.clone(),
            self.resolution_checkpoint.clone(),
        )?;
        Ok(entry)
    }
}

fn circle_history_closure(
    entries: &[CircleRosterEntry],
    frontier: &[CircleRosterCoord],
) -> BTreeSet<CircleRosterCoord> {
    let by_coord = entries
        .iter()
        .map(|entry| (entry.coord(), entry))
        .collect::<BTreeMap<_, _>>();
    let mut pending = frontier.iter().cloned().collect::<BTreeSet<_>>();
    let mut included = BTreeSet::new();
    while let Some(coord) = pending.pop_first() {
        if !included.insert(coord.clone()) {
            continue;
        }
        if let Some(entry) = by_coord.get(&coord) {
            pending.extend(entry.dependencies.iter().cloned());
        }
    }
    included
}

fn exact_circle_head_refs(
    head_refs: &[CircleRosterHeadRef],
    coords: &[CircleRosterCoord],
) -> Result<Vec<CircleRosterHeadRef>, CircleRosterError> {
    let expected = coords.iter().cloned().collect::<BTreeSet<_>>();
    let mut references = head_refs
        .iter()
        .filter(|reference| expected.contains(&reference.coord))
        .cloned()
        .collect::<Vec<_>>();
    let actual = references
        .iter()
        .map(|reference| reference.coord.clone())
        .collect::<BTreeSet<_>>();
    if expected != actual || references.len() != expected.len() {
        return Err(CircleRosterError::MissingConflictHeads);
    }
    references.sort();
    Ok(references)
}

fn map_circle_grants(
    grants: BTreeMap<MembershipGrantId, causal_grants::GrantRecord<CircleRosterCoord, CircleRole>>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<BTreeMap<MembershipGrantId, CircleGrantRecord>, CircleRosterError> {
    grants
        .into_iter()
        .map(|(grant, record)| -> Result<_, CircleRosterError> {
            let creation_authority =
                circle_creation_authority(&grant, record.creation, checkpoint)?;
            Ok((
                grant,
                CircleGrantRecord {
                    member_pubkey: record.member_pubkey,
                    role: record.assignment,
                    creation_authority,
                },
            ))
        })
        .collect()
}

fn resolved_circle_roster(
    reduced: &causal_grants::ReducedGrants<CircleRosterCoord, CircleRole>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<ResolvedCircleRoster, CircleRosterError> {
    let grants = reduced
        .grants
        .iter()
        .map(|(grant, state)| -> Result<_, CircleRosterError> {
            Ok((
                grant.clone(),
                map_circle_grant_state(grant, state, checkpoint)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Ok(ResolvedCircleRoster {
        state_hash: circle_roster_state_hash(&grants),
        grants,
    })
}

fn map_circle_grant_state(
    grant: &MembershipGrantId,
    state: &GrantState<
        causal_grants::GrantRecord<CircleRosterCoord, CircleRole>,
        causal_grants::CausalGrantRetirement<CircleRosterCoord>,
    >,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<GrantState<CircleGrantRecord, CircleGrantRetirement>, CircleRosterError> {
    let causal_record = state.record();
    let record = CircleGrantRecord {
        member_pubkey: causal_record.member_pubkey.clone(),
        role: causal_record.assignment,
        creation_authority: circle_creation_authority(
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
        || CircleRosterError::MissingCheckpointRetirementEvidence {
            grant: grant.clone(),
        },
        |coord, owner_barrier| {
            Ok(CircleGrantRetirement::Entry {
                authority: coord.clone(),
                owner_barrier: owner_barrier.map(|barrier| CircleOwnerGrantBarrier {
                    observed_streams: barrier.observed_streams.values().cloned().collect(),
                }),
            })
        },
    )
}

fn circle_creation_authority(
    grant: &MembershipGrantId,
    creation: causal_grants::CausalGrantCreation<CircleRosterCoord>,
    checkpoint: Option<
        &BTreeMap<MembershipGrantId, GrantState<CircleGrantRecord, CircleGrantRetirement>>,
    >,
) -> Result<CircleGrantCreationAuthority, CircleRosterError> {
    match creation {
        causal_grants::CausalGrantCreation::Entry(coord) => {
            Ok(CircleGrantCreationAuthority::Entry(coord))
        }
        causal_grants::CausalGrantCreation::Checkpoint => checkpoint
            .and_then(|grants| grants.get(grant))
            .ok_or_else(|| CircleRosterError::MissingCheckpointGrant {
                grant: grant.clone(),
            })
            .map(|state| state.record().creation_authority.clone()),
    }
}

fn circle_assignment_conflict_hash(
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    heads: &[CircleRosterHeadRef],
    member_pubkey: &str,
    conflicting_grants: &BTreeMap<
        MembershipGrantId,
        causal_grants::GrantRecord<CircleRosterCoord, CircleRole>,
    >,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        heads: &'a [CircleRosterHeadRef],
        member_pubkey: &'a str,
        conflicting_grant_ids: Vec<&'a MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.circle-roster-assignment-conflict.v1",
            store_root_hash,
            circle_id,
            heads,
            member_pubkey,
            conflicting_grant_ids: conflicting_grants.keys().collect(),
        })
        .expect("Circle roster conflict serialization cannot fail"),
    )
}

fn circle_revocation_conflict_hash(
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    heads: &[CircleRosterHeadRef],
    cyclic_sources: &[CircleRosterCoord],
    involved_owner_grants: &BTreeSet<MembershipGrantId>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct Conflict<'a> {
        domain: &'static str,
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        heads: &'a [CircleRosterHeadRef],
        cyclic_sources: &'a [CircleRosterCoord],
        involved_owner_grants: &'a BTreeSet<MembershipGrantId>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&Conflict {
            domain: "coven.circle-roster-revocation-conflict.v1",
            store_root_hash,
            circle_id,
            heads,
            cyclic_sources,
            involved_owner_grants,
        })
        .expect("Circle roster revocation conflict serialization cannot fail"),
    )
}

#[cfg(test)]
mod authority_tests {
    use super::*;

    fn grant(label: &[u8]) -> MembershipGrantId {
        MembershipGrantId(ObjectHash::digest(label))
    }

    #[test]
    fn circle_grant_mapping_rejects_missing_checkpoint_evidence() {
        let grant = grant(b"missing Circle checkpoint evidence");
        let coord = CircleRosterCoord {
            author_pubkey: "owner".to_string(),
            device_id: "owner-device".to_string(),
            stream_id: AuthorStreamId::from_bytes([91; 32]),
            author_owner_grant: grant.clone(),
            seq: 1,
            entry_hash: ObjectHash::digest(b"Circle grant creation"),
        };
        let checkpoint_creation = GrantState::Active {
            record: causal_grants::GrantRecord {
                member_pubkey: "owner".to_string(),
                assignment: CircleRole::Owner,
                creation: causal_grants::CausalGrantCreation::Checkpoint,
            },
        };
        assert!(matches!(
            map_circle_grant_state(&grant, &checkpoint_creation, None),
            Err(CircleRosterError::MissingCheckpointGrant { grant: missing })
                if missing == grant
        ));

        let checkpoint_retirement = GrantState::Tombstoned {
            record: causal_grants::GrantRecord {
                member_pubkey: "owner".to_string(),
                assignment: CircleRole::Owner,
                creation: causal_grants::CausalGrantCreation::Entry(coord),
            },
            retirements: GrantRetirements::new(causal_grants::CausalGrantRetirement::Checkpoint),
        };
        assert!(matches!(
            map_circle_grant_state(&grant, &checkpoint_retirement, None),
            Err(CircleRosterError::MissingCheckpointRetirementEvidence { grant: missing })
                if missing == grant
        ));
    }

    fn exact_object(logical_key: String, bytes: &[u8]) -> super::super::storage::ExactObjectRef {
        super::super::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(logical_key)
                .expect("valid test Circle roster slot"),
            bytes.len() as u64,
            ObjectHash::digest(bytes),
        )
    }

    fn signed_head(entry: &CircleRosterEntry, device_signer: &UserKeypair) -> CircleRosterHeadRef {
        signed_head_with_resolutions(entry, entry.resolution_dependencies.clone(), device_signer).1
    }

    fn signed_exact_head(
        entry: &CircleRosterEntry,
        device_signer: &UserKeypair,
    ) -> ExactCircleRosterHead {
        let (head, reference) = signed_head_with_resolutions(
            entry,
            entry.resolution_dependencies.clone(),
            device_signer,
        );
        ExactCircleRosterHead::bind(head, reference).expect("bind test Circle roster head")
    }

    fn signed_head_with_resolutions(
        entry: &CircleRosterEntry,
        resolutions: Vec<CircleRosterConflictResolutionRef>,
        device_signer: &UserKeypair,
    ) -> (CircleRosterHead, CircleRosterHeadRef) {
        let entry_bytes = serde_json::to_vec(entry).expect("serialize test Circle roster entry");
        let tip = exact_object(
            format!(
                "store-v1/test/circle-roster/{}/entry.json",
                entry.entry_hash()
            ),
            &entry_bytes,
        );
        let head_slot = crate::storage::cloud::ObjectSlot::logical(format!(
            "store-v1/test/circle-roster/{}/{}/head.json",
            entry.stream_id, entry.seq
        ))
        .expect("valid test Circle roster-head slot");
        let registration_bytes = format!("{} registration", entry.device_id);
        let registration = super::super::store_commit::StoreDeviceRegistrationRef {
            device_id: ObjectHash::digest(entry.device_id.as_bytes())
                .to_string()
                .parse()
                .expect("valid test Circle device id"),
            registration_hash: ObjectHash::digest(registration_bytes.as_bytes()),
            object: exact_object(
                format!(
                    "store-v1/test/circle-roster/{}/registration.json",
                    entry.device_id
                ),
                registration_bytes.as_bytes(),
            ),
        };
        let activation = super::super::store_commit::StreamActivation::grant_authorized(
            entry.store_root_hash,
            registration,
            entry.author_owner_grant.clone(),
            super::super::store_commit::GrantStreamAnchor::CircleRoster {
                circle_id: entry.circle_id,
                first_slot: head_slot.clone(),
            },
        );
        let head = CircleRosterHead::signed_with_resolutions(
            entry,
            tip,
            SuccessorLink {
                activation: activation.activation_id(),
                predecessor: None,
                next_slot: crate::storage::cloud::ObjectSlot::logical(format!(
                    "store-v1/test/circle-roster/{}/{}/next-head.json",
                    entry.stream_id,
                    entry
                        .seq
                        .checked_add(1)
                        .expect("test Circle roster sequence remains representable")
                ))
                .expect("valid next test Circle roster-head slot"),
            },
            resolutions,
            device_signer,
        );
        let head_bytes = serde_json::to_vec(&head).expect("serialize test Circle roster head");
        let object = super::super::storage::ExactObjectRef::new(
            head_slot,
            head_bytes.len() as u64,
            ObjectHash::digest(&head_bytes),
        );
        let reference = CircleRosterHeadRef::from_stored_head(&head, object);
        (head, reference)
    }

    #[test]
    fn roster_sequence_exhaustion_fails_instead_of_reusing_the_last_sequence() {
        let owner = UserKeypair::generate();
        let owner_pubkey = keys::public_key_hex(&owner);
        let owner_grant = grant(b"sequence-exhaustion-owner-grant");
        let store_root_hash = ObjectHash::digest(b"sequence-exhaustion-store");
        let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
        let stream_id = AuthorStreamId::from_bytes([122; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "owner-device",
            stream_id,
            owner_grant,
            &owner,
        );
        let mut terminal = founder.clone();
        terminal.seq = u64::MAX;
        terminal.previous_hash = Some(founder.entry_hash());
        terminal.signature = keys::sign_hex(&owner, &terminal.canonical_bytes()).1;
        let terminal_coord = terminal.coord();
        let stream = terminal_coord.stream_key();
        let mut chain = CircleRosterChain::from_entries(vec![founder]).expect("founder roster");
        chain.entries.push(terminal);
        chain
            .reduced
            .as_mut()
            .expect("resolved founder roster")
            .included
            .insert(terminal_coord);

        assert!(matches!(
            chain.next_position(&stream),
            Err(CircleRosterError::SequenceExhausted { current: u64::MAX })
        ));
    }

    fn three_owner_cycle() -> (
        ObjectHash,
        CircleId,
        UserKeypair,
        UserKeypair,
        UserKeypair,
        CircleRosterChain,
        Vec<CircleRosterHeadRef>,
    ) {
        let first = UserKeypair::generate();
        let second = UserKeypair::generate();
        let third = UserKeypair::generate();
        let first_pubkey = keys::public_key_hex(&first);
        let second_pubkey = keys::public_key_hex(&second);
        let third_pubkey = keys::public_key_hex(&third);
        let store_root_hash = ObjectHash::digest(b"three-owner Circle conflict Store");
        let founder_grant = grant(b"three-owner Circle founder grant");
        let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &founder_grant);
        let first_stream = AuthorStreamId::from_bytes([81; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            first_stream,
            founder_grant,
            &first,
        );
        let mut base = vec![founder];
        let add_second = CircleRosterChain::from_entries(base.clone())
            .expect("founder roster")
            .signed_set_member(
                "first-device",
                first_stream,
                second_pubkey.clone(),
                CircleRole::Owner,
                &first,
            )
            .expect("add second Owner");
        base.push(add_second);
        let add_third = CircleRosterChain::from_entries(base.clone())
            .expect("two-Owner roster")
            .signed_set_member(
                "first-device",
                first_stream,
                third_pubkey,
                CircleRole::Owner,
                &first,
            )
            .expect("add third Owner");
        base.push(add_third);
        let remove_second = CircleRosterChain::from_entries(base.clone())
            .expect("three-Owner roster")
            .signed_remove_member("first-device", first_stream, second_pubkey, &first)
            .expect("first branch");
        let remove_first = CircleRosterChain::from_entries(base.clone())
            .expect("three-Owner roster")
            .signed_remove_member(
                "second-device",
                AuthorStreamId::from_bytes([82; 32]),
                first_pubkey,
                &second,
            )
            .expect("second branch");
        base.extend([remove_second.clone(), remove_first.clone()]);
        let exact_heads = vec![
            signed_exact_head(&remove_second, &first),
            signed_exact_head(&remove_first, &second),
        ];
        let heads = exact_heads
            .iter()
            .map(|head| head.reference().clone())
            .collect::<Vec<_>>();
        let conflict = CircleRosterChain::from_entries_with_heads(base, exact_heads)
            .expect("three-Owner revocation conflict");
        (
            store_root_hash,
            circle_id,
            first,
            second,
            third,
            conflict,
            heads,
        )
    }

    #[test]
    fn sequential_circle_resolution_checkpoints_retain_every_exact_reference() {
        let (_store_root_hash, _circle_id, first, _second, third, conflicted, first_heads) =
            three_owner_cycle();
        let first_pubkey = keys::public_key_hex(&first);
        let third_pubkey = keys::public_key_hex(&third);
        let first_branch = match conflicted.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants().any(|(_, record)| {
                        record.member_pubkey == first_pubkey && record.role == CircleRole::Owner
                    })
                })
                .expect("first Owner branch")
                .heads
                .clone(),
            _ => panic!("expected first revocation conflict"),
        };
        let first_resolution = conflicted
            .signed_cycle_resolution(first_branch, &first)
            .expect("first resolution");
        let mut resumed = conflicted;
        resumed
            .apply_resolutions(std::slice::from_ref(&first_resolution))
            .expect("apply first resolution");

        let remove_third = resumed
            .signed_remove_member(
                "first-device",
                AuthorStreamId::from_bytes([83; 32]),
                third_pubkey,
                &first,
            )
            .expect("replacement Owner removes third Owner");
        let remove_first = resumed
            .signed_remove_member(
                "third-device",
                AuthorStreamId::from_bytes([84; 32]),
                first_pubkey.clone(),
                &third,
            )
            .expect("third Owner removes replacement Owner");
        let mut entries = resumed.entries().to_vec();
        entries.extend([remove_third.clone(), remove_first.clone()]);
        let mut heads = first_heads;
        heads.extend([
            signed_head(&remove_third, &first),
            signed_head(&remove_first, &third),
        ]);
        heads.sort_by_key(|head| head.coord.clone());
        let mut second_conflict = resumed
            .replay_resolved_history_to_heads(entries, heads)
            .expect("load second conflict from first checkpoint");
        let second_branch = match second_conflict.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => maximal_valid_branches
                .iter()
                .find(|branch| {
                    branch.active_grants().any(|(_, record)| {
                        record.member_pubkey == first_pubkey && record.role == CircleRole::Owner
                    })
                })
                .expect("replacement Owner branch")
                .heads
                .clone(),
            _ => panic!("expected second revocation conflict"),
        };
        let second_resolution = second_conflict
            .signed_cycle_resolution(second_branch, &first)
            .expect("second resolution");
        second_conflict
            .apply_resolutions(std::slice::from_ref(&second_resolution))
            .expect("apply second resolution");

        let mut expected = vec![
            first_resolution.resolution_ref(),
            second_resolution.resolution_ref(),
        ];
        expected.sort();
        assert_eq!(second_conflict.resolution_refs(), expected);
    }

    #[test]
    fn unaffected_circle_owner_resolution_retires_its_selected_branch_grant() {
        let (store_root_hash, circle_id, _first, _second, third, conflicted, _) =
            three_owner_cycle();
        let third_pubkey = keys::public_key_hex(&third);
        let (branch, old_grant) = match conflicted.status() {
            CircleRosterStatus::Conflict(CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            }) => {
                let branch = maximal_valid_branches
                    .iter()
                    .find(|branch| {
                        branch.active_grants().any(|(_, record)| {
                            record.member_pubkey == third_pubkey && record.role == CircleRole::Owner
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
        let resolution = conflicted
            .signed_cycle_resolution(branch, &third)
            .expect("unaffected Owner resolution");
        let resolved = conflicted
            .resolved_with(std::slice::from_ref(&resolution))
            .expect("unaffected Owner resolution is valid");

        assert!(resolution.retired_owner_grants.contains(&old_grant));
        assert!(resolved.grants[&old_grant].active().is_none());
        assert!(resolved
            .grants
            .get(&resolution.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
        assert!(matches!(
            &resolved.grants[&old_grant],
            GrantState::Tombstoned { retirements, .. }
                if retirements.contains(&CircleGrantRetirement::ConflictResolution(
                    resolution.resolution_ref()
                ))
        ));
        assert!(resolution.verify_against(
            store_root_hash,
            circle_id,
            match conflicted.status() {
                CircleRosterStatus::Conflict(conflict) => conflict,
                _ => unreachable!(),
            }
        ));
    }

    #[test]
    fn circle_revocation_cycle_over_protocol_bound_is_typed() {
        let owners = (0..13).map(|_| UserKeypair::generate()).collect::<Vec<_>>();
        let pubkeys = owners.iter().map(keys::public_key_hex).collect::<Vec<_>>();
        let store_root_hash = ObjectHash::digest(b"bounded Circle cycle Store");
        let founder_grant = grant(b"bounded Circle cycle founder");
        let circle_id = CircleId::founder(store_root_hash, &pubkeys[0], &founder_grant);
        let founder_stream = AuthorStreamId::from_bytes([121; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "owner-0-device",
            founder_stream,
            founder_grant,
            &owners[0],
        );
        let mut base = vec![founder];
        for (index, pubkey) in pubkeys.iter().enumerate().skip(1) {
            let add = CircleRosterChain::from_entries(base.clone())
                .expect("load ring roster")
                .signed_set_member(
                    "owner-0-device",
                    founder_stream,
                    pubkey.clone(),
                    CircleRole::Owner,
                    &owners[0],
                )
                .expect("add ring Owner");
            assert_eq!(index as u64 + 1, add.seq);
            base.push(add);
        }
        let base_chain = CircleRosterChain::from_entries(base.clone()).expect("13-Owner roster");
        let removals = owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                let stream = if index == 0 {
                    founder_stream
                } else {
                    AuthorStreamId::from_bytes([index as u8; 32])
                };
                base_chain
                    .signed_remove_member(
                        &format!("owner-{index}-device"),
                        stream,
                        pubkeys[(index + 1) % pubkeys.len()].clone(),
                        owner,
                    )
                    .expect("sign ring removal")
            })
            .collect::<Vec<_>>();
        base.extend(removals.iter().cloned());
        let heads = removals
            .iter()
            .zip(&owners)
            .map(|(entry, owner)| signed_exact_head(entry, owner))
            .collect();

        assert!(matches!(
            CircleRosterChain::from_entries_with_heads(base, heads),
            Err(CircleRosterError::RevocationCycleTooWide {
                sources: 13,
                maximum: 12,
            })
        ));
    }

    #[test]
    fn historical_roster_authorizes_the_exact_grant_at_its_creation_coordinate() {
        let owner = UserKeypair::generate();
        let owner_pubkey = keys::public_key_hex(&owner);
        let owner_grant = grant(b"historical-owner-grant");
        let founder = CircleRosterEntry::founder(
            ObjectHash::digest(b"historical-authority-store"),
            CircleId::founder(
                ObjectHash::digest(b"historical-authority-store"),
                &owner_pubkey,
                &owner_grant,
            ),
            "owner-device",
            AuthorStreamId::from_bytes([1; 32]),
            owner_grant.clone(),
            &owner,
        );
        let created_at = founder.coord();
        let roster = CircleRosterChain::from_entries(vec![founder])
            .expect("load founder roster")
            .resolved();

        assert!(roster.authorizes_owner_grant(&owner_pubkey, &owner_grant, &created_at,));
    }

    #[test]
    fn removed_owner_grant_stays_unauthorized_after_the_identity_is_readded() {
        let first_owner = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let first_pubkey = keys::public_key_hex(&first_owner);
        let second_pubkey = keys::public_key_hex(&second_owner);
        let first_grant = grant(b"first-owner-grant");
        let store_root_hash = ObjectHash::digest(b"remove-readd-store");
        let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &first_grant);
        let first_stream = AuthorStreamId::from_bytes([2; 32]);
        let second_stream = AuthorStreamId::from_bytes([3; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            first_stream,
            first_grant.clone(),
            &first_owner,
        );
        let first_created_at = founder.coord();
        let mut entries = vec![founder];
        let add_second = CircleRosterChain::from_entries(entries.clone())
            .expect("load founder roster")
            .signed_set_member(
                "first-device",
                first_stream,
                second_pubkey.clone(),
                CircleRole::Owner,
                &first_owner,
            )
            .expect("add second Owner");
        entries.push(add_second);
        let remove_first = CircleRosterChain::from_entries(entries.clone())
            .expect("load two-Owner roster")
            .signed_remove_member(
                "second-device",
                second_stream,
                first_pubkey.clone(),
                &second_owner,
            )
            .expect("remove first Owner");
        let retirement_authority = remove_first.coord();
        let CircleRosterChange::RemoveMember { owner_barriers, .. } = &remove_first.change else {
            unreachable!()
        };
        let owner_barrier = owner_barriers[&first_grant].clone();
        entries.push(remove_first);
        let readd_first = CircleRosterChain::from_entries(entries.clone())
            .expect("load removed-Owner roster")
            .signed_set_member(
                "second-device",
                second_stream,
                first_pubkey.clone(),
                CircleRole::Owner,
                &second_owner,
            )
            .expect("re-add first Owner");
        let replacement_grant = match &readd_first.change {
            CircleRosterChange::SetMember { grant_id, .. } => grant_id.clone(),
            _ => panic!("re-add must create a grant"),
        };
        let replacement_created_at = readd_first.coord();
        entries.push(readd_first);
        let roster = CircleRosterChain::from_entries(entries)
            .expect("load re-added roster")
            .resolved();

        assert!(!roster.authorizes_owner_grant(&first_pubkey, &first_grant, &first_created_at,));
        assert!(roster.authorizes_owner_grant(
            &first_pubkey,
            &replacement_grant,
            &replacement_created_at,
        ));
        assert!(matches!(
            &roster.grants[&first_grant],
            GrantState::Tombstoned { record, retirements }
                if record.member_pubkey == first_pubkey
                    && retirements.as_set() == &BTreeSet::from([CircleGrantRetirement::Entry {
                        authority: retirement_authority.clone(),
                        owner_barrier: Some(owner_barrier.clone()),
                    }])
        ));
        let mut altered = roster.grants.clone();
        let GrantState::Tombstoned { retirements, .. } = altered
            .get_mut(&first_grant)
            .expect("retired Circle grant remains present")
        else {
            unreachable!()
        };
        retirements.insert(CircleGrantRetirement::Entry {
            authority: CircleRosterCoord {
                entry_hash: ObjectHash::digest(b"different Circle retirement entry"),
                ..retirement_authority
            },
            owner_barrier: Some(owner_barrier),
        });
        assert_ne!(roster.state_hash, circle_roster_state_hash(&altered));
    }

    #[test]
    fn roster_state_hash_changes_when_only_the_active_grant_identity_changes() {
        let owner = UserKeypair::generate();
        let owner_pubkey = keys::public_key_hex(&owner);
        let store_root_hash = ObjectHash::digest(b"grant-hash-store");
        let build = |grant_id: MembershipGrantId, stream_byte| {
            let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &grant_id);
            CircleRosterChain::from_entries(vec![CircleRosterEntry::founder(
                store_root_hash,
                circle_id,
                "owner-device",
                AuthorStreamId::from_bytes([stream_byte; 32]),
                grant_id,
                &owner,
            )])
            .expect("load founder roster")
            .resolved()
        };

        let first = build(grant(b"state-hash-grant-a"), 4);
        let second = build(grant(b"state-hash-grant-b"), 5);

        assert_ne!(first.state_hash, second.state_hash);
    }

    #[tokio::test]
    async fn cross_revocation_cycle_is_signed_from_an_exact_maximal_branch() {
        let first_owner = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let outsider = UserKeypair::generate();
        let first_pubkey = keys::public_key_hex(&first_owner);
        let second_pubkey = keys::public_key_hex(&second_owner);
        let store_root_hash = ObjectHash::digest(b"Circle conflict Store root");
        let first_grant = grant(b"Circle conflict founder grant");
        let circle_id = CircleId::founder(store_root_hash, &first_pubkey, &first_grant);
        let first_stream = AuthorStreamId::from_bytes([31; 32]);
        let second_stream = AuthorStreamId::from_bytes([32; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "first-device",
            first_stream,
            first_grant,
            &first_owner,
        );
        let mut base = vec![founder];
        let add_second = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_set_member(
                "first-device",
                first_stream,
                second_pubkey.clone(),
                CircleRole::Owner,
                &first_owner,
            )
            .unwrap();
        base.push(add_second);
        let remove_second = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_remove_member(
                "first-device",
                first_stream,
                second_pubkey.clone(),
                &first_owner,
            )
            .unwrap();
        let remove_first = CircleRosterChain::from_entries(base.clone())
            .unwrap()
            .signed_remove_member(
                "second-device",
                second_stream,
                first_pubkey.clone(),
                &second_owner,
            )
            .unwrap();
        base.extend([remove_second.clone(), remove_first.clone()]);
        let conflicted = CircleRosterChain::from_entries_with_heads(
            base,
            vec![
                signed_exact_head(&remove_second, &first_owner),
                signed_exact_head(&remove_first, &second_owner),
            ],
        )
        .expect("well-formed Circle roster conflict");
        let CircleRosterStatus::Conflict(
            conflict @ CircleRosterConflict::RevocationCycle {
                maximal_valid_branches,
                ..
            },
        ) = conflicted.status()
        else {
            panic!("expected revocation cycle");
        };
        assert_eq!(maximal_valid_branches.len(), 2);
        let branch_state = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == first_pubkey && record.role == CircleRole::Owner
                })
            })
            .expect("first Owner branch")
            .clone();
        let branch = branch_state.heads.clone();
        let second_branch = maximal_valid_branches
            .iter()
            .find(|branch| {
                branch.active_grants().any(|(_, record)| {
                    record.member_pubkey == second_pubkey && record.role == CircleRole::Owner
                })
            })
            .expect("second Owner branch")
            .heads
            .clone();
        let resolution = conflicted
            .signed_cycle_resolution(branch.clone(), &first_owner)
            .expect("branch Owner resolution");
        let second_resolution = conflicted
            .signed_cycle_resolution(second_branch, &second_owner)
            .expect("other branch Owner resolution");
        assert_eq!(
            resolution,
            conflicted
                .signed_cycle_resolution(branch, &first_owner)
                .expect("idempotent retry")
        );
        assert!(resolution.verify_against(store_root_hash, circle_id, conflict));
        let resolved_once = conflicted
            .resolved_with(std::slice::from_ref(&resolution))
            .expect("one resolution applies");
        let resolved_duplicate = conflicted
            .resolved_with(&[resolution.clone(), resolution.clone()])
            .expect("an exact retry is idempotent");
        assert_eq!(resolved_once, resolved_duplicate);
        assert!(resolved_once
            .grants
            .get(&resolution.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
        assert!(resolution
            .retired_owner_grants
            .iter()
            .all(|grant| resolved_once
                .grants
                .get(grant)
                .and_then(GrantState::active)
                .is_none()));

        let resolved_union = conflicted
            .resolved_with(&[resolution.clone(), second_resolution.clone()])
            .expect("distinct resolvers are unioned");
        assert!(resolved_union
            .grants
            .get(&resolution.replacement_grant)
            .and_then(GrantState::active)
            .is_some());
        assert!(resolved_union
            .grants
            .get(&second_resolution.replacement_grant)
            .and_then(GrantState::active)
            .is_some());

        let mut branch_specific = conflict.clone();
        let CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = &mut branch_specific
        else {
            unreachable!()
        };
        let branch_only_grant = grant(b"Circle branch-only grant");
        let branch_only_creation = maximal_valid_branches[0].effective_frontier[0].clone();
        maximal_valid_branches[0].grants.insert(
            branch_only_grant.clone(),
            GrantState::Active {
                record: CircleGrantRecord {
                    member_pubkey: keys::public_key_hex(&UserKeypair::generate()),
                    role: CircleRole::Member,
                    creation_authority: CircleGrantCreationAuthority::Entry(branch_only_creation),
                },
            },
        );
        let composed = resolve_circle_roster_conflict(
            store_root_hash,
            circle_id,
            &branch_specific,
            &[resolution.clone(), second_resolution.clone()],
        )
        .expect("retire grants not agreed by every valid branch");
        let branch_only_retirements = composed
            .grants
            .get(&branch_only_grant)
            .and_then(GrantState::retirements)
            .expect("branch-only grant is retained as retired");
        assert!(
            branch_only_retirements.contains(&CircleGrantRetirement::ConflictResolution(
                resolution.resolution_ref()
            ))
        );
        assert!(
            branch_only_retirements.contains(&CircleGrantRetirement::ConflictResolution(
                second_resolution.resolution_ref()
            ))
        );

        let mut duplicate_member = branch_specific;
        let CircleRosterConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        } = &mut duplicate_member
        else {
            unreachable!()
        };
        let duplicate_pubkey = keys::public_key_hex(&UserKeypair::generate());
        let duplicate_creation = resolution.conflicting_heads[0].coord.clone();
        for branch in maximal_valid_branches {
            for suffix in [b'a', b'b'] {
                branch.grants.insert(
                    grant(&[suffix]),
                    GrantState::Active {
                        record: CircleGrantRecord {
                            member_pubkey: duplicate_pubkey.clone(),
                            role: CircleRole::Member,
                            creation_authority: CircleGrantCreationAuthority::Entry(
                                duplicate_creation.clone(),
                            ),
                        },
                    },
                );
            }
        }
        assert!(matches!(
            resolve_circle_roster_conflict(
                store_root_hash,
                circle_id,
                &duplicate_member,
                &[resolution.clone(), second_resolution.clone()],
            ),
            Err(CircleRosterError::InvalidConflictResolution)
        ));

        let mut resumed = conflicted.clone();
        let raw_heads = resumed.author_heads();
        resumed
            .apply_resolutions(std::slice::from_ref(&resolution))
            .expect("resolution activates replacement Owner grant");
        assert_eq!(resumed.author_heads(), raw_heads);
        assert_eq!(
            resumed.effective_frontier(),
            branch_state.effective_frontier
        );
        assert_eq!(resumed.resolution_refs(), &[resolution.resolution_ref()]);
        let reusable = resumed.reusable_author_streams(
            &first_pubkey,
            "first-device",
            &resolution.replacement_grant,
        );
        assert!(reusable.is_empty());
        let db = crate::sync::test_helpers::open_test_db();
        let stream_state_key = format!(
            "circle_roster_author_stream/{circle_id}/{first_pubkey}/{}",
            resolution.replacement_grant
        );
        let selected_stream = crate::sync::store::StoreDatabase::new(&db)
            .select_causal_author_stream(stream_state_key.clone(), reusable)
            .await
            .unwrap();
        assert_eq!(
            crate::sync::store::StoreDatabase::new(&db)
                .select_causal_author_stream(stream_state_key, BTreeSet::from([selected_stream]),)
                .await
                .unwrap(),
            selected_stream
        );
        let after_resolution = resumed
            .signed_set_member(
                "first-device",
                selected_stream,
                keys::public_key_hex(&UserKeypair::generate()),
                CircleRole::Member,
                &first_owner,
            )
            .expect("replacement Owner can author from a fresh stream");
        assert_eq!(
            after_resolution.author_owner_grant,
            resolution.replacement_grant
        );
        assert!(matches!(
            conflicted
                .signed_cycle_resolution(resolution.resolver_branch_heads.clone(), &outsider,),
            Err(CircleRosterError::SignerIsNotOwner(_))
        ));
    }

    #[test]
    fn circle_head_resolution_cut_must_equal_its_tip_entry_cut() {
        let owner = UserKeypair::generate();
        let owner_pubkey = keys::public_key_hex(&owner);
        let store_root_hash = ObjectHash::digest(b"Circle head-tip Store");
        let owner_grant = grant(b"Circle head-tip grant");
        let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
        let entry = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "owner-device",
            AuthorStreamId::from_bytes([99; 32]),
            owner_grant,
            &owner,
        );
        let fake = CircleRosterConflictResolutionRef {
            conflict_hash: ObjectHash::digest(b"Circle head-tip conflict"),
            resolver_pubkey: owner_pubkey,
            resolution_hash: ObjectHash::digest(b"Circle head-tip resolution"),
        };
        let (head, reference) = signed_head_with_resolutions(&entry, vec![fake], &owner);
        let head = ExactCircleRosterHead::bind(head, reference).unwrap();

        assert!(matches!(
            CircleRosterChain::from_entries_with_heads(vec![entry], vec![head]),
            Err(CircleRosterError::MissingConflictHeads)
        ));
    }

    #[test]
    fn circle_entry_rejects_unsorted_or_duplicate_resolution_dependencies() {
        let owner = UserKeypair::generate();
        let owner_pubkey = keys::public_key_hex(&owner);
        let store_root_hash = ObjectHash::digest(b"Circle entry resolution cut Store");
        let owner_grant = grant(b"Circle entry resolution cut grant");
        let circle_id = CircleId::founder(store_root_hash, &owner_pubkey, &owner_grant);
        let stream_id = AuthorStreamId::from_bytes([100; 32]);
        let founder = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            "owner-device",
            stream_id,
            owner_grant,
            &owner,
        );
        let entry = CircleRosterChain::from_entries(vec![founder])
            .unwrap()
            .signed_set_member(
                "owner-device",
                stream_id,
                keys::public_key_hex(&UserKeypair::generate()),
                CircleRole::Member,
                &owner,
            )
            .unwrap();
        let mut refs = [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .map(|label| CircleRosterConflictResolutionRef {
                conflict_hash: ObjectHash::digest(label),
                resolver_pubkey: owner_pubkey.clone(),
                resolution_hash: ObjectHash::digest(&[label, b" resolution"].concat()),
            })
            .collect::<Vec<_>>();
        refs.sort();

        let mut unsorted = entry.clone();
        unsorted.resolution_dependencies = refs.iter().rev().cloned().collect();
        unsorted.signature = keys::sign_hex(&owner, &unsorted.canonical_bytes()).1;
        assert!(!unsorted.verify());

        let mut duplicate = entry;
        duplicate.resolution_dependencies = vec![refs[0].clone(), refs[0].clone()];
        duplicate.signature = keys::sign_hex(&owner, &duplicate.canonical_bytes()).1;
        assert!(!duplicate.verify());
    }
}
