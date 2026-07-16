//! Signed Circle roster streams and causal assignment reduction.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::causal_grants::{
    self, AuthorStreamId, CausalAssignment, CausalChange, CausalCoordinate, CausalEntry,
    CausalGrantError, OwnerGrantBarrier,
};
use super::circle::{CircleId, CircleRole};
use super::membership::MembershipGrantId;
use super::store_commit::{ObjectHash, STORE_PROTOCOL_VERSION};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
            change: CircleRosterChange::Founder {
                member_pubkey: author_pubkey,
                grant_id: owner_grant,
            },
            signature: String::new(),
        };
        entry.signature = keys::sign_hex(signer, &entry.canonical_bytes()).1;
        entry
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
            previous_hash: Option<ObjectHash>,
            dependencies: &'a [CircleRosterCoord],
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
                    && member_pubkey == &self.author_pubkey
                    && grant_id == &self.author_owner_grant
            }
            (CircleRosterChange::Founder { .. }, _, _) => false,
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
    pub signature: String,
}

impl CircleRosterHead {
    pub(crate) fn signed(entry: &CircleRosterEntry, signer: &UserKeypair) -> Self {
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
        })
        .expect("circle roster head serialization cannot fail")
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle roster head serialization cannot fail"),
        )
    }

    pub fn verify(&self) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.seq > 0
            && !self.device_id.is_empty()
            && keys::verify_signature_hex(
                &self.author_pubkey,
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
}

impl CircleRosterHeadRef {
    pub(crate) fn from_head(head: &CircleRosterHead) -> Self {
        Self {
            coord: head.entry_coord(),
            head_hash: head.head_hash(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleRosterStateRef {
    MergeConcurrent {
        heads: Vec<CircleRosterHeadRef>,
        state_hash: ObjectHash,
    },
    Serial {
        state_hash: ObjectHash,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleGrantRecord {
    pub member_pubkey: String,
    pub role: CircleRole,
    pub created_at: CircleRosterCoord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerialCircleGrantRecord {
    pub member_pubkey: String,
    pub role: CircleRole,
    pub created_at_generation: u64,
}

fn circle_roster_state_hash(
    active_grants: &BTreeMap<MembershipGrantId, CircleGrantRecord>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        active_grants: &'a BTreeMap<MembershipGrantId, CircleGrantRecord>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.circle-roster-state.v1",
            active_grants,
        })
        .expect("circle roster state serialization cannot fail"),
    )
}

fn serial_circle_roster_state_hash(
    active_grants: &BTreeMap<MembershipGrantId, SerialCircleGrantRecord>,
) -> ObjectHash {
    #[derive(Serialize)]
    struct State<'a> {
        domain: &'static str,
        active_grants: &'a BTreeMap<MembershipGrantId, SerialCircleGrantRecord>,
    }
    ObjectHash::digest(
        &serde_json::to_vec(&State {
            domain: "coven.serial-circle-roster-state.v1",
            active_grants,
        })
        .expect("Serial circle roster state serialization cannot fail"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedCircleRoster {
    pub active_grants: BTreeMap<MembershipGrantId, CircleGrantRecord>,
    pub state_hash: ObjectHash,
}

impl ResolvedCircleRoster {
    pub fn members(&self) -> BTreeMap<String, CircleRole> {
        self.active_grants
            .values()
            .map(|record| (record.member_pubkey.clone(), record.role))
            .collect()
    }

    pub fn owners(&self) -> Vec<String> {
        let mut owners = self
            .active_grants
            .values()
            .filter_map(|record| {
                (record.role == CircleRole::Owner).then_some(record.member_pubkey.clone())
            })
            .collect::<Vec<_>>();
        owners.sort();
        owners
    }

    pub fn authorizes_owner_grant(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
        created_at: &CircleRosterCoord,
    ) -> bool {
        self.authorizes_owner_grant_id(author_pubkey, grant_id)
            && self
                .active_grants
                .get(grant_id)
                .is_some_and(|record| &record.created_at == created_at)
    }

    pub fn authorizes_owner_grant_id(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
    ) -> bool {
        self.active_grants.get(grant_id).is_some_and(|record| {
            record.member_pubkey == author_pubkey && record.role == CircleRole::Owner
        })
    }

    pub fn verify(&self) -> bool {
        self.state_hash == circle_roster_state_hash(&self.active_grants)
            && self
                .active_grants
                .values()
                .any(|record| record.role == CircleRole::Owner)
            && {
                let members = self.members();
                members.len() == self.active_grants.len()
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SerialCircleRoster {
    pub active_grants: BTreeMap<MembershipGrantId, SerialCircleGrantRecord>,
    pub state_hash: ObjectHash,
}

impl SerialCircleRoster {
    pub(crate) fn founder(
        author_pubkey: String,
        grant_id: MembershipGrantId,
        generation: u64,
    ) -> Self {
        let active_grants = BTreeMap::from([(
            grant_id,
            SerialCircleGrantRecord {
                member_pubkey: author_pubkey,
                role: CircleRole::Owner,
                created_at_generation: generation,
            },
        )]);
        let state_hash = serial_circle_roster_state_hash(&active_grants);
        Self {
            active_grants,
            state_hash,
        }
    }

    pub fn members(&self) -> BTreeMap<String, CircleRole> {
        self.active_grants
            .values()
            .map(|record| (record.member_pubkey.clone(), record.role))
            .collect()
    }

    pub fn owners(&self) -> Vec<String> {
        let mut owners = self
            .active_grants
            .values()
            .filter_map(|record| {
                (record.role == CircleRole::Owner).then_some(record.member_pubkey.clone())
            })
            .collect::<Vec<_>>();
        owners.sort();
        owners
    }

    pub fn authorizes_owner_grant(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
        created_at_generation: u64,
    ) -> bool {
        self.authorizes_owner_grant_id(author_pubkey, grant_id)
            && self
                .active_grants
                .get(grant_id)
                .is_some_and(|record| record.created_at_generation == created_at_generation)
    }

    pub fn authorizes_owner_grant_id(
        &self,
        author_pubkey: &str,
        grant_id: &MembershipGrantId,
    ) -> bool {
        self.active_grants.get(grant_id).is_some_and(|record| {
            record.member_pubkey == author_pubkey && record.role == CircleRole::Owner
        })
    }

    pub fn verify(&self) -> bool {
        self.state_hash == serial_circle_roster_state_hash(&self.active_grants)
            && self
                .active_grants
                .values()
                .any(|record| record.role == CircleRole::Owner)
            && {
                let members = self.members();
                members.len() == self.active_grants.len()
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleMaterializedRoster {
    MergeConcurrent(ResolvedCircleRoster),
    Serial(SerialCircleRoster),
}

impl CircleMaterializedRoster {
    pub fn verify(&self) -> bool {
        match self {
            Self::MergeConcurrent(roster) => roster.verify(),
            Self::Serial(roster) => roster.verify(),
        }
    }

    pub fn state_hash(&self) -> ObjectHash {
        match self {
            Self::MergeConcurrent(roster) => roster.state_hash,
            Self::Serial(roster) => roster.state_hash,
        }
    }

    pub fn members(&self) -> BTreeMap<String, CircleRole> {
        match self {
            Self::MergeConcurrent(roster) => roster.members(),
            Self::Serial(roster) => roster.members(),
        }
    }

    pub fn owners(&self) -> Vec<String> {
        match self {
            Self::MergeConcurrent(roster) => roster.owners(),
            Self::Serial(roster) => roster.owners(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CircleRosterChain {
    entries: Vec<CircleRosterEntry>,
    reduced: causal_grants::ReducedGrants<CircleRosterCoord, CircleRole>,
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
    #[error("Circle roster causal history: {0}")]
    Causal(String),
}

impl From<CausalGrantError<CircleRosterCoord>> for CircleRosterError {
    fn from(error: CausalGrantError<CircleRosterCoord>) -> Self {
        Self::Causal(error.to_string())
    }
}

impl CircleRosterChain {
    pub fn from_entries(entries: Vec<CircleRosterEntry>) -> Result<Self, CircleRosterError> {
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
        }
        let normalized = entries
            .iter()
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
                },
            })
            .collect::<Vec<_>>();
        let reduced = causal_grants::reduce(&normalized)?;
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
        Ok(Self { entries, reduced })
    }

    pub fn entries(&self) -> &[CircleRosterEntry] {
        &self.entries
    }

    pub fn resolved(&self) -> ResolvedCircleRoster {
        let active_grants = self
            .reduced
            .grants
            .iter()
            .filter_map(|(grant, record)| {
                (!self.reduced.removed.contains(grant)).then_some((
                    grant.clone(),
                    CircleGrantRecord {
                        member_pubkey: record.member_pubkey.clone(),
                        role: record.assignment,
                        created_at: record.created_at.clone(),
                    },
                ))
            })
            .collect::<BTreeMap<_, _>>();
        ResolvedCircleRoster {
            state_hash: circle_roster_state_hash(&active_grants),
            active_grants,
        }
    }

    pub fn author_heads(&self) -> Vec<CircleRosterCoord> {
        let mut heads = BTreeMap::<CircleAuthorStreamKey, CircleRosterCoord>::new();
        for entry in &self.entries {
            let coord = entry.coord();
            if !self.reduced.includes_coord(&coord) {
                continue;
            }
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
        self.reduced
            .grants
            .iter()
            .filter_map(|(grant, record)| {
                (record.member_pubkey == member_pubkey && !self.reduced.removed.contains(grant))
                    .then_some(grant.clone())
            })
            .collect()
    }

    fn active_owner_grant(&self, member_pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants(member_pubkey).into_iter().find(|grant| {
            self.reduced
                .active_grant(grant)
                .is_some_and(|record| record.assignment == CircleRole::Owner)
        })
    }

    fn frontier(&self) -> Vec<CircleRosterCoord> {
        self.author_heads()
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
            .author_heads()
            .into_iter()
            .find(|coord| coord.stream_key() == *stream);
        if raw_tip.as_ref() != effective_tip.as_ref() {
            return Err(CircleRosterError::PrunedAuthorStream);
        }
        Ok(effective_tip.map_or((1, None), |tip| (tip.seq + 1, Some(tip.entry_hash))))
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

    fn signed_change(
        &self,
        device_id: &str,
        stream_id: AuthorStreamId,
        member_pubkey: String,
        role: Option<CircleRole>,
        signer: &UserKeypair,
    ) -> Result<CircleRosterEntry, CircleRosterError> {
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
            change,
            signature: String::new(),
        };
        entry.signature = keys::sign_hex(signer, &entry.canonical_bytes()).1;
        let mut candidate_history = self.entries.clone();
        candidate_history.push(entry.clone());
        Self::from_entries(candidate_history)?;
        Ok(entry)
    }
}

#[cfg(test)]
mod authority_tests {
    use super::*;

    fn grant(label: &[u8]) -> MembershipGrantId {
        MembershipGrantId(ObjectHash::digest(label))
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
            AuthorStreamId::from_bytes([1; 16]),
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
        let first_stream = AuthorStreamId::from_bytes([2; 16]);
        let second_stream = AuthorStreamId::from_bytes([3; 16]);
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
                AuthorStreamId::from_bytes([stream_byte; 16]),
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
}
