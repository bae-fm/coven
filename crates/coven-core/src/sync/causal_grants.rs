//! Shared causal assignment reducer for Store membership and Circle rosters.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::store_commit::ObjectHash;

const AUTHOR_STREAM_ID_DOMAIN: &[u8] = b"coven.author-stream-id.v1\0";

/// Generated identity of one causal author stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorStreamId([u8; 16]);

impl AuthorStreamId {
    pub(crate) fn generate(ids: &dyn crate::id_provider::IdProvider) -> Self {
        let id = ids.new_id();
        let mut material = Vec::with_capacity(AUTHOR_STREAM_ID_DOMAIN.len() + id.len());
        material.extend_from_slice(AUTHOR_STREAM_ID_DOMAIN);
        material.extend_from_slice(id.as_bytes());
        Self::from_digest(ObjectHash::digest(&material))
    }

    pub(crate) fn from_digest(digest: ObjectHash) -> Self {
        Self(
            digest.as_bytes()[..16]
                .try_into()
                .expect("SHA-256 prefix has fixed length"),
        )
    }

    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for AuthorStreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for AuthorStreamId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 32
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "author stream id must be exactly 32 lowercase hexadecimal characters: {value:?}"
            ));
        }
        let bytes = hex::decode(value)
            .map_err(|error| format!("decode author stream id: {error}"))?
            .try_into()
            .map_err(|_| "author stream id has the wrong byte length".to_string())?;
        Ok(Self(bytes))
    }
}

impl Serialize for AuthorStreamId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for AuthorStreamId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct MembershipGrantId(pub ObjectHash);

impl fmt::Display for MembershipGrantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

pub(crate) trait CausalCoordinate: Clone + Debug + Eq + Ord {
    type StreamKey: Clone + Debug + Eq + Ord;

    fn stream_key(&self) -> Self::StreamKey;
    fn author_pubkey(&self) -> &str;
    fn author_owner_grant(&self) -> &MembershipGrantId;
    fn seq(&self) -> u64;
    fn entry_hash(&self) -> ObjectHash;
}

pub(crate) trait CausalAssignment: Clone + Debug + Eq {
    fn is_owner(&self) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnerGrantBarrier<C: CausalCoordinate> {
    pub observed_streams: BTreeMap<C::StreamKey, C>,
}

impl<C: CausalCoordinate> OwnerGrantBarrier<C> {
    fn includes(&self, coord: &C) -> bool {
        self.observed_streams
            .get(&coord.stream_key())
            .is_some_and(|barrier| coord.seq() <= barrier.seq())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CausalChange<C: CausalCoordinate, A: CausalAssignment> {
    Founder {
        member_pubkey: String,
        grant_id: MembershipGrantId,
        assignment: A,
    },
    SetMember {
        member_pubkey: String,
        assignment: A,
        grant_id: MembershipGrantId,
        replaces: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, OwnerGrantBarrier<C>>,
    },
    RemoveMember {
        member_pubkey: String,
        removes: BTreeSet<MembershipGrantId>,
        owner_barriers: BTreeMap<MembershipGrantId, OwnerGrantBarrier<C>>,
    },
}

type RemovedGrants<'a, C> = (
    &'a BTreeSet<MembershipGrantId>,
    &'a BTreeMap<MembershipGrantId, OwnerGrantBarrier<C>>,
);

impl<C: CausalCoordinate, A: CausalAssignment> CausalChange<C, A> {
    fn removed(&self) -> Option<RemovedGrants<'_, C>> {
        match self {
            Self::SetMember {
                replaces,
                owner_barriers,
                ..
            } => Some((replaces, owner_barriers)),
            Self::RemoveMember {
                removes,
                owner_barriers,
                ..
            } => Some((removes, owner_barriers)),
            Self::Founder { .. } => None,
        }
    }

    fn removes_grant(&self, grant: &MembershipGrantId) -> bool {
        self.removed()
            .is_some_and(|(removed, _)| removed.contains(grant))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CausalEntry<C: CausalCoordinate, A: CausalAssignment> {
    pub coord: C,
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: BTreeMap<C::StreamKey, C>,
    pub change: CausalChange<C, A>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrantRecord<C: CausalCoordinate, A: CausalAssignment> {
    pub member_pubkey: String,
    pub assignment: A,
    pub created_at: C,
}

#[derive(Debug, Clone)]
pub(crate) struct ReducedGrants<C: CausalCoordinate, A: CausalAssignment> {
    pub grants: BTreeMap<MembershipGrantId, GrantRecord<C, A>>,
    pub removed: BTreeSet<MembershipGrantId>,
    pub included: BTreeSet<C>,
}

impl<C: CausalCoordinate, A: CausalAssignment> ReducedGrants<C, A> {
    pub(crate) fn active_grant(&self, grant: &MembershipGrantId) -> Option<&GrantRecord<C, A>> {
        (!self.removed.contains(grant))
            .then(|| self.grants.get(grant))
            .flatten()
    }

    pub(crate) fn includes_coord(&self, coord: &C) -> bool {
        self.included.contains(coord)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CausalGrantError<C: CausalCoordinate> {
    #[error("causal assignment history is empty")]
    Empty,
    #[error("stream {stream:?} has conflicting entries at sequence {seq}")]
    ConflictingSequence { stream: C::StreamKey, seq: u64 },
    #[error("stream {stream:?} is missing sequence {seq}")]
    MissingSequence { stream: C::StreamKey, seq: u64 },
    #[error("entry {index} has predecessor {actual:?}, expected {expected:?}")]
    BrokenStreamLink {
        index: usize,
        expected: Option<ObjectHash>,
        actual: Option<ObjectHash>,
    },
    #[error("entry {index} does not carry its exact own-stream dependency")]
    MissingOwnDependency { index: usize },
    #[error("entry {index} has a dependency under the wrong stream key")]
    DependencyStreamMismatch { index: usize },
    #[error("entry {index} depends on missing coordinate {dependency:?}")]
    MissingDependency { index: usize, dependency: C },
    #[error("causal assignment dependency graph contains a cycle")]
    DependencyCycle,
    #[error("causal assignment founder is invalid")]
    InvalidFounder,
    #[error("entry {index} author is not active under Owner grant {grant}")]
    AuthorGrantInactive {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("entry {index} creates already-defined grant {grant}")]
    DuplicateGrant {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("entry {index} replaces or removes grant {grant} owned by another member")]
    GrantOwnerMismatch {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("entry {index} does not name the exact active grants for member {member_pubkey}")]
    GrantSetMismatch { index: usize, member_pubkey: String },
    #[error("entry {index} removes no exact grants")]
    EmptyRemoval { index: usize },
    #[error("entry {index} removes Owner grant {grant} without its exact observed frontier")]
    MissingOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("entry {index} carries an invalid frontier for Owner grant {grant}")]
    InvalidOwnerRevocationBarrier {
        index: usize,
        grant: MembershipGrantId,
    },
    #[error("concurrent Owner revocations leave no active Owner")]
    ConcurrentOwnerRevocationConflict,
    #[error("member {member_pubkey} has concurrent active grants {grants:?}")]
    ConcurrentMemberGrantConflict {
        member_pubkey: String,
        grants: Vec<MembershipGrantId>,
    },
}

#[derive(Debug, Clone)]
struct CausalState<C: CausalCoordinate, A: CausalAssignment> {
    grants: BTreeMap<MembershipGrantId, GrantRecord<C, A>>,
    removed: BTreeSet<MembershipGrantId>,
}

impl<C: CausalCoordinate, A: CausalAssignment> Default for CausalState<C, A> {
    fn default() -> Self {
        Self {
            grants: BTreeMap::new(),
            removed: BTreeSet::new(),
        }
    }
}

impl<C: CausalCoordinate, A: CausalAssignment> CausalState<C, A> {
    fn merge(&mut self, other: &Self, index: usize) -> Result<(), CausalGrantError<C>> {
        for (grant, record) in &other.grants {
            if self
                .grants
                .get(grant)
                .is_some_and(|current| current != record)
            {
                return Err(CausalGrantError::DuplicateGrant {
                    index,
                    grant: grant.clone(),
                });
            }
            self.grants
                .entry(grant.clone())
                .or_insert_with(|| record.clone());
        }
        self.removed.extend(other.removed.iter().cloned());
        Ok(())
    }

    fn active_owner(&self, grant: &MembershipGrantId, pubkey: &str) -> bool {
        !self.removed.contains(grant)
            && self.grants.get(grant).is_some_and(|record| {
                record.member_pubkey == pubkey && record.assignment.is_owner()
            })
    }

    fn apply(
        &mut self,
        coord: &C,
        change: &CausalChange<C, A>,
        index: usize,
        validate_exact_grants: bool,
    ) -> Result<(), CausalGrantError<C>> {
        match change {
            CausalChange::Founder {
                member_pubkey,
                grant_id,
                assignment,
            } => self.insert(index, coord, member_pubkey, grant_id, assignment.clone())?,
            CausalChange::SetMember {
                member_pubkey,
                assignment,
                grant_id,
                replaces,
                ..
            } => {
                self.remove(index, member_pubkey, replaces, false, validate_exact_grants)?;
                self.insert(index, coord, member_pubkey, grant_id, assignment.clone())?;
            }
            CausalChange::RemoveMember {
                member_pubkey,
                removes,
                ..
            } => self.remove(index, member_pubkey, removes, true, validate_exact_grants)?,
        }
        Ok(())
    }

    fn insert(
        &mut self,
        index: usize,
        coord: &C,
        member_pubkey: &str,
        grant: &MembershipGrantId,
        assignment: A,
    ) -> Result<(), CausalGrantError<C>> {
        if self.grants.contains_key(grant) {
            return Err(CausalGrantError::DuplicateGrant {
                index,
                grant: grant.clone(),
            });
        }
        self.grants.insert(
            grant.clone(),
            GrantRecord {
                member_pubkey: member_pubkey.to_string(),
                assignment,
                created_at: coord.clone(),
            },
        );
        Ok(())
    }

    fn remove(
        &mut self,
        index: usize,
        member_pubkey: &str,
        grants: &BTreeSet<MembershipGrantId>,
        require_nonempty: bool,
        validate_exact_grants: bool,
    ) -> Result<(), CausalGrantError<C>> {
        if require_nonempty && grants.is_empty() {
            return Err(CausalGrantError::EmptyRemoval { index });
        }
        let active = self
            .grants
            .iter()
            .filter_map(|(grant, record)| {
                (record.member_pubkey == member_pubkey && !self.removed.contains(grant))
                    .then_some(grant.clone())
            })
            .collect::<BTreeSet<_>>();
        if validate_exact_grants && active != *grants {
            return Err(CausalGrantError::GrantSetMismatch {
                index,
                member_pubkey: member_pubkey.to_string(),
            });
        }
        for grant in grants {
            if self
                .grants
                .get(grant)
                .is_none_or(|record| record.member_pubkey != member_pubkey)
            {
                return Err(CausalGrantError::GrantOwnerMismatch {
                    index,
                    grant: grant.clone(),
                });
            }
            self.removed.insert(grant.clone());
        }
        Ok(())
    }
}

pub(crate) fn reduce<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
) -> Result<ReducedGrants<C, A>, CausalGrantError<C>> {
    if entries.is_empty() {
        return Err(CausalGrantError::Empty);
    }
    let mut index_by_coord = BTreeMap::new();
    let mut streams = BTreeMap::<C::StreamKey, BTreeMap<u64, usize>>::new();
    for (index, entry) in entries.iter().enumerate() {
        if index_by_coord.insert(entry.coord.clone(), index).is_some() {
            return Err(CausalGrantError::ConflictingSequence {
                stream: entry.coord.stream_key(),
                seq: entry.coord.seq(),
            });
        }
        if streams
            .entry(entry.coord.stream_key())
            .or_default()
            .insert(entry.coord.seq(), index)
            .is_some()
        {
            return Err(CausalGrantError::ConflictingSequence {
                stream: entry.coord.stream_key(),
                seq: entry.coord.seq(),
            });
        }
        if entry
            .dependencies
            .iter()
            .any(|(stream, coord)| stream != &coord.stream_key())
        {
            return Err(CausalGrantError::DependencyStreamMismatch { index });
        }
    }

    for (stream, positions) in &streams {
        let max_seq = *positions.keys().next_back().expect("stream is non-empty");
        let mut previous_hash = None;
        for seq in 1..=max_seq {
            let Some(index) = positions.get(&seq).copied() else {
                return Err(CausalGrantError::MissingSequence {
                    stream: stream.clone(),
                    seq,
                });
            };
            let entry = &entries[index];
            if entry.previous_hash != previous_hash {
                return Err(CausalGrantError::BrokenStreamLink {
                    index,
                    expected: previous_hash,
                    actual: entry.previous_hash,
                });
            }
            let own_dependency_is_exact = if seq == 1 {
                !entry.dependencies.contains_key(stream)
            } else {
                let predecessor = &entries[positions[&(seq - 1)]].coord;
                entry.dependencies.get(stream) == Some(predecessor)
            };
            if !own_dependency_is_exact {
                return Err(CausalGrantError::MissingOwnDependency { index });
            }
            previous_hash = Some(entry.coord.entry_hash());
        }
    }

    for (index, entry) in entries.iter().enumerate() {
        for dependency in entry.dependencies.values() {
            if !index_by_coord.contains_key(dependency) {
                return Err(CausalGrantError::MissingDependency {
                    index,
                    dependency: dependency.clone(),
                });
            }
        }
    }

    let founders = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            matches!(entry.change, CausalChange::Founder { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    if founders.len() != 1 {
        return Err(CausalGrantError::InvalidFounder);
    }
    let founder_index = founders[0];
    let founder = &entries[founder_index];
    let CausalChange::Founder {
        member_pubkey,
        grant_id,
        assignment,
    } = &founder.change
    else {
        unreachable!()
    };
    if founder.coord.seq() != 1
        || founder.coord.author_pubkey() != member_pubkey
        || founder.coord.author_owner_grant() != grant_id
        || founder.previous_hash.is_some()
        || !founder.dependencies.is_empty()
        || !assignment.is_owner()
    {
        return Err(CausalGrantError::InvalidFounder);
    }

    let mut remaining = (0..entries.len()).collect::<BTreeSet<_>>();
    let mut states = BTreeMap::<C, CausalState<C, A>>::new();
    let mut causal_order = Vec::with_capacity(entries.len());
    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .copied()
            .filter(|index| {
                entries[*index]
                    .dependencies
                    .values()
                    .all(|dependency| states.contains_key(dependency))
            })
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(CausalGrantError::DependencyCycle);
        }
        for index in ready {
            remaining.remove(&index);
            causal_order.push(index);
            let entry = &entries[index];
            let mut state = CausalState::default();
            for dependency in entry.dependencies.values() {
                state.merge(&states[dependency], index)?;
            }
            if index != founder_index
                && !state.active_owner(
                    entry.coord.author_owner_grant(),
                    entry.coord.author_pubkey(),
                )
            {
                return Err(CausalGrantError::AuthorGrantInactive {
                    index,
                    grant: entry.coord.author_owner_grant().clone(),
                });
            }
            validate_barriers(index, entry, &state)?;
            state.apply(&entry.coord, &entry.change, index, true)?;
            states.insert(entry.coord.clone(), state);
        }
    }

    let mut raw = CausalState::default();
    for state in states.values() {
        raw.merge(state, 0)?;
    }
    let mut all_sources = BTreeSet::new();
    for (index, entry) in entries.iter().enumerate() {
        if let Some((removed, _)) = entry.change.removed() {
            for grant in removed {
                if raw
                    .grants
                    .get(grant)
                    .is_some_and(|record| record.assignment.is_owner())
                {
                    all_sources.insert(index);
                }
            }
        }
    }

    let mut sources = all_sources.clone();
    let mut seen_source_sets = BTreeSet::new();
    let included = loop {
        if !seen_source_sets.insert(sources.clone()) {
            return Err(CausalGrantError::ConcurrentOwnerRevocationConflict);
        }
        let cap_sources = cap_sources(entries, &raw, &sources);
        let included = included_entries(entries, &index_by_coord, &cap_sources);
        let next_sources = all_sources
            .intersection(&included)
            .copied()
            .collect::<BTreeSet<_>>();
        if next_sources == sources {
            break included;
        }
        sources = next_sources;
    };

    let mut effective = CausalState::default();
    for index in causal_order {
        let entry = &entries[index];
        if !included.contains(&index) {
            continue;
        }
        effective.apply(&entry.coord, &entry.change, index, false)?;
    }

    let mut active_by_member = BTreeMap::<String, Vec<MembershipGrantId>>::new();
    for (grant, record) in &effective.grants {
        if !effective.removed.contains(grant) {
            active_by_member
                .entry(record.member_pubkey.clone())
                .or_default()
                .push(grant.clone());
        }
    }
    if let Some((member_pubkey, grants)) = active_by_member
        .into_iter()
        .find(|(_, grants)| grants.len() > 1)
    {
        return Err(CausalGrantError::ConcurrentMemberGrantConflict {
            member_pubkey,
            grants,
        });
    }
    require_owner(&effective)?;

    Ok(ReducedGrants {
        grants: effective.grants,
        removed: effective.removed,
        included: included
            .into_iter()
            .map(|index| entries[index].coord.clone())
            .collect(),
    })
}

fn cap_sources<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    raw: &CausalState<C, A>,
    source_indices: &BTreeSet<usize>,
) -> BTreeMap<MembershipGrantId, Vec<(usize, OwnerGrantBarrier<C>)>> {
    let mut sources = BTreeMap::<MembershipGrantId, Vec<(usize, OwnerGrantBarrier<C>)>>::new();
    for index in source_indices {
        let entry = &entries[*index];
        let Some((removed, barriers)) = entry.change.removed() else {
            continue;
        };
        for grant in removed {
            if raw
                .grants
                .get(grant)
                .is_some_and(|record| record.assignment.is_owner())
            {
                sources.entry(grant.clone()).or_default().push((
                    *index,
                    barriers
                        .get(grant)
                        .expect("Owner barriers were validated")
                        .clone(),
                ));
            }
        }
    }
    sources
}

fn included_entries<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    index_by_coord: &BTreeMap<C, usize>,
    cap_sources: &BTreeMap<MembershipGrantId, Vec<(usize, OwnerGrantBarrier<C>)>>,
) -> BTreeSet<usize> {
    let mut included = (0..entries.len())
        .filter(|index| {
            let entry = &entries[*index];
            !cap_sources
                .get(entry.coord.author_owner_grant())
                .is_some_and(|sources| {
                    sources.iter().any(|(source_index, barrier)| {
                        let own_self_revocation = *source_index == *index
                            && entry.change.removes_grant(entry.coord.author_owner_grant());
                        !own_self_revocation && !barrier.includes(&entry.coord)
                    })
                })
        })
        .collect::<BTreeSet<_>>();
    loop {
        let descendants = included
            .iter()
            .copied()
            .filter(|index| {
                entries[*index]
                    .dependencies
                    .values()
                    .any(|dependency| !included.contains(&index_by_coord[dependency]))
            })
            .collect::<Vec<_>>();
        if descendants.is_empty() {
            return included;
        }
        for index in descendants {
            included.remove(&index);
        }
    }
}

fn validate_barriers<C: CausalCoordinate, A: CausalAssignment>(
    index: usize,
    entry: &CausalEntry<C, A>,
    state: &CausalState<C, A>,
) -> Result<(), CausalGrantError<C>> {
    let Some((removed, barriers)) = entry.change.removed() else {
        return Ok(());
    };
    for grant in barriers.keys() {
        if !removed.contains(grant)
            || !state
                .grants
                .get(grant)
                .is_some_and(|record| record.assignment.is_owner())
        {
            return Err(CausalGrantError::InvalidOwnerRevocationBarrier {
                index,
                grant: grant.clone(),
            });
        }
    }
    for grant in removed {
        if !state
            .grants
            .get(grant)
            .is_some_and(|record| record.assignment.is_owner())
        {
            continue;
        }
        let Some(barrier) = barriers.get(grant) else {
            return Err(CausalGrantError::MissingOwnerRevocationBarrier {
                index,
                grant: grant.clone(),
            });
        };
        let observed = entry
            .dependencies
            .iter()
            .filter(|(_, coord)| coord.author_owner_grant() == grant)
            .map(|(stream, coord)| (stream.clone(), coord.clone()))
            .collect::<BTreeMap<_, _>>();
        if barrier.observed_streams != observed {
            return Err(CausalGrantError::InvalidOwnerRevocationBarrier {
                index,
                grant: grant.clone(),
            });
        }
    }
    Ok(())
}

fn require_owner<C: CausalCoordinate, A: CausalAssignment>(
    state: &CausalState<C, A>,
) -> Result<(), CausalGrantError<C>> {
    if state
        .grants
        .iter()
        .any(|(grant, record)| !state.removed.contains(grant) && record.assignment.is_owner())
    {
        Ok(())
    } else {
        Err(CausalGrantError::ConcurrentOwnerRevocationConflict)
    }
}
