//! Shared causal assignment reducer for Store membership and Circle rosters.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::store_commit::ObjectHash;

const MAX_CYCLIC_REVOCATION_SOURCES: usize = 12;

pub(crate) fn canonical_ready_checkpoint<'a, K: Clone + Ord + 'a>(
    mut dependencies: impl Iterator<Item = (&'a K, &'a BTreeSet<K>)>,
    applied: &BTreeSet<K>,
) -> Option<K> {
    dependencies
        .find(|(_, required)| required.is_subset(applied))
        .map(|(checkpoint, _)| checkpoint.clone())
}

pub(crate) fn merge_checkpoint_evidence<K, V, T, C>(
    merged_grants: &mut BTreeMap<K, GrantState<V, T>>,
    merged_included: &mut BTreeSet<C>,
    grants: &BTreeMap<K, GrantState<V, T>>,
    included: &BTreeSet<C>,
) -> bool
where
    K: Clone + Ord,
    V: Clone + Eq,
    T: Clone + Ord,
    C: Clone + Ord,
{
    for (grant, state) in grants {
        match merged_grants.entry(grant.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(state.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if !entry.get_mut().merge(state) {
                    return false;
                }
            }
        }
    }
    merged_included.extend(included.iter().cloned());
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct GrantRetirements<T: Ord>(BTreeSet<T>);

impl<T: Ord> GrantRetirements<T> {
    pub fn new(retirement: T) -> Self {
        Self(BTreeSet::from([retirement]))
    }

    pub fn insert(&mut self, retirement: T) -> bool {
        self.0.insert(retirement)
    }

    pub fn extend(&mut self, retirements: impl IntoIterator<Item = T>) {
        self.0.extend(retirements);
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter()
    }

    pub fn contains(&self, retirement: &T) -> bool {
        self.0.contains(retirement)
    }

    pub fn as_set(&self) -> &BTreeSet<T> {
        &self.0
    }
}

impl<'de, T> Deserialize<'de> for GrantRetirements<T>
where
    T: Deserialize<'de> + Ord,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let retirements = BTreeSet::deserialize(deserializer)?;
        if retirements.is_empty() {
            return Err(serde::de::Error::custom(
                "grant retirement set cannot be empty",
            ));
        }
        Ok(Self(retirements))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum GrantState<R, T: Ord> {
    Active {
        record: R,
    },
    Tombstoned {
        record: R,
        retirements: GrantRetirements<T>,
    },
}

impl<R, T: Ord> GrantState<R, T> {
    pub fn record(&self) -> &R {
        match self {
            Self::Active { record } | Self::Tombstoned { record, .. } => record,
        }
    }

    pub fn active(&self) -> Option<&R> {
        match self {
            Self::Active { record } => Some(record),
            Self::Tombstoned { .. } => None,
        }
    }

    pub fn retirements(&self) -> Option<&GrantRetirements<T>> {
        match self {
            Self::Active { .. } => None,
            Self::Tombstoned { retirements, .. } => Some(retirements),
        }
    }
}

impl<R: Clone + Eq, T: Clone + Ord> GrantState<R, T> {
    fn merge(&mut self, other: &Self) -> bool {
        if self.record() != other.record() {
            return false;
        }
        let retirements = match (&*self, other) {
            (Self::Active { .. }, Self::Active { .. }) => return true,
            (Self::Tombstoned { retirements, .. }, Self::Active { .. }) => retirements.clone(),
            (Self::Active { .. }, Self::Tombstoned { retirements, .. }) => retirements.clone(),
            (
                Self::Tombstoned {
                    retirements: current,
                    ..
                },
                Self::Tombstoned { retirements, .. },
            ) => {
                let mut merged = current.clone();
                merged.extend(retirements.iter().cloned());
                merged
            }
        };
        *self = Self::Tombstoned {
            record: self.record().clone(),
            retirements,
        };
        true
    }
}

pub(crate) fn try_map_grant_state<C, A, R, T, E>(
    state: &GrantState<GrantRecord<C, A>, CausalGrantRetirement<C>>,
    record: R,
    checkpoint_retirements: Option<&GrantRetirements<T>>,
    missing_checkpoint_retirements: impl Fn() -> E,
    map_entry: impl Fn(&C, Option<&OwnerGrantBarrier<C>>) -> Result<T, E>,
) -> Result<GrantState<R, T>, E>
where
    C: CausalCoordinate,
    A: CausalAssignment,
    T: Clone + Ord,
{
    let GrantState::Tombstoned { retirements, .. } = state else {
        return Ok(GrantState::Active { record });
    };
    let mut mapped: Option<GrantRetirements<T>> = None;
    let mut add = |retirement| match &mut mapped {
        Some(mapped) => {
            mapped.insert(retirement);
        }
        None => mapped = Some(GrantRetirements::new(retirement)),
    };
    for retirement in retirements.iter() {
        match retirement {
            CausalGrantRetirement::Entry {
                coord,
                owner_barrier,
            } => add(map_entry(coord, owner_barrier.as_ref())?),
            CausalGrantRetirement::Checkpoint => {
                let checkpoint_retirements =
                    checkpoint_retirements.ok_or_else(&missing_checkpoint_retirements)?;
                for retirement in checkpoint_retirements.iter().cloned() {
                    add(retirement);
                }
            }
        }
    }
    Ok(GrantState::Tombstoned {
        record,
        retirements: mapped.expect("causal tombstone has retirement evidence"),
    })
}

pub(crate) fn merge_checkpoint_frontier<C: CausalCoordinate>(
    merged: &mut BTreeMap<C::StreamKey, C>,
    frontier: &[C],
) -> bool {
    for coord in frontier {
        let stream = coord.stream_key();
        match merged.get(&stream) {
            Some(existing) if existing.seq() == coord.seq() && existing != coord => return false,
            Some(existing) if existing.seq() >= coord.seq() => {}
            _ => {
                merged.insert(stream, coord.clone());
            }
        }
    }
    true
}

/// Derived identity of one causal author stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorStreamId([u8; 32]);

impl AuthorStreamId {
    pub(crate) fn from_digest(digest: ObjectHash) -> Self {
        Self(*digest.as_bytes())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
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
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "author stream id must be exactly 64 lowercase hexadecimal characters: {value:?}"
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

/// The greatest coordinate each author stream reaches across `coords`.
///
/// Streams are keyed by [`CausalCoordinate::stream_key`]; within a stream the
/// highest sequence wins, and the first of an equal pair is kept.
pub(crate) fn stream_frontier<C: CausalCoordinate>(coords: impl IntoIterator<Item = C>) -> Vec<C> {
    let mut heads = BTreeMap::<C::StreamKey, C>::new();
    for coord in coords {
        match heads.entry(coord.stream_key()) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(coord);
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                if coord.seq() > slot.get().seq() {
                    slot.insert(coord);
                }
            }
        }
    }
    heads.into_values().collect()
}

pub(crate) fn common_frontier<C: CausalCoordinate>(frontiers: &[&[C]]) -> Vec<C> {
    let Some(first) = frontiers.first() else {
        return Vec::new();
    };
    let others = frontiers[1..]
        .iter()
        .map(|frontier| {
            frontier
                .iter()
                .map(|coord| (coord.stream_key(), coord))
                .collect::<BTreeMap<_, _>>()
        })
        .collect::<Vec<_>>();
    first
        .iter()
        .filter_map(|coord| {
            let stream = coord.stream_key();
            let mut common = coord.clone();
            for frontier in &others {
                let candidate = frontier.get(&stream)?;
                if candidate.seq() < common.seq() {
                    common = (*candidate).clone();
                }
            }
            Some(common)
        })
        .collect()
}

pub(crate) trait CausalAssignment: Clone + Debug + Eq {
    fn is_owner(&self) -> bool;
}

/// One signed entry of a causal history, viewed as a node of the dependency
/// graph: where it sits, and which coordinates it names as its predecessors.
pub(crate) trait CausalHistoryEntry {
    type Coord: CausalCoordinate;

    fn coord(&self) -> Self::Coord;
    fn dependencies(&self) -> &[Self::Coord];
}

/// The entries a checkpoint has not already absorbed: those standing beyond
/// their stream's checkpointed head, plus any stream the checkpoint never saw.
///
/// A resumed chain replays only this suffix, because the checkpoint already
/// carries the reduced state of everything at or below its heads.
pub(crate) fn entries_beyond_checkpoint<'a, E: CausalHistoryEntry>(
    entries: &'a [E],
    raw_heads: &[E::Coord],
) -> impl Iterator<Item = &'a E> {
    let heads = raw_heads
        .iter()
        .map(|coord| (coord.stream_key(), coord.seq()))
        .collect::<BTreeMap<_, _>>();
    entries.iter().filter(move |entry| {
        let coord = entry.coord();
        heads
            .get(&coord.stream_key())
            .is_none_or(|head| coord.seq() > *head)
    })
}

/// Turn a checkpoint's own grant records into reducer seeds.
///
/// `seed` states how one domain's record names its member and assignment; the
/// rest is the rule both callers must agree on — a grant the checkpoint had
/// already retired seeds as retired, carrying the checkpoint itself as its
/// evidence, so replaying a suffix can never resurrect it.
pub(crate) fn checkpoint_seed_grants<R, T: Ord, A: CausalAssignment>(
    grants: &BTreeMap<MembershipGrantId, GrantState<R, T>>,
    seed: impl Fn(&R) -> CausalSeedGrant<A>,
) -> BTreeMap<MembershipGrantId, GrantState<CausalSeedGrant<A>, ()>> {
    grants
        .iter()
        .map(|(grant, state)| {
            let record = seed(state.record());
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
        .collect()
}

/// Every coordinate reachable from `frontier` by walking dependencies.
///
/// A coordinate with no entry in `entries` is included but not walked through —
/// a frontier may name coordinates a checkpoint has already absorbed.
pub(crate) fn history_closure<E: CausalHistoryEntry>(
    entries: &[E],
    frontier: &[E::Coord],
) -> BTreeSet<E::Coord> {
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
            pending.extend(entry.dependencies().iter().cloned());
        }
    }
    included
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
    Control,
    ResolutionActivation,
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
            Self::Founder { .. } | Self::Control | Self::ResolutionActivation => None,
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
    pub creation: CausalGrantCreation<C>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CausalGrantCreation<C: CausalCoordinate> {
    Entry(C),
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CausalGrantRetirement<C: CausalCoordinate> {
    Entry {
        coord: C,
        owner_barrier: Option<OwnerGrantBarrier<C>>,
    },
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CausalSeedGrant<A: CausalAssignment> {
    pub member_pubkey: String,
    pub assignment: A,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReducedGrants<C: CausalCoordinate, A: CausalAssignment> {
    pub grants:
        BTreeMap<MembershipGrantId, GrantState<GrantRecord<C, A>, CausalGrantRetirement<C>>>,
    pub included: BTreeSet<C>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CausalGrantBranch<C: CausalCoordinate, A: CausalAssignment> {
    pub raw_heads: Vec<C>,
    pub effective_frontier: Vec<C>,
    pub reduced: ReducedGrants<C, A>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CausalGrantConflict<C: CausalCoordinate, A: CausalAssignment> {
    ConcurrentMemberAssignments {
        raw_heads: Vec<C>,
        effective_frontier: Vec<C>,
        member_pubkey: String,
        conflicting_grants: BTreeMap<MembershipGrantId, GrantRecord<C, A>>,
        uncontested_grants: BTreeMap<MembershipGrantId, GrantRecord<C, A>>,
        reduced: ReducedGrants<C, A>,
    },
    RevocationCycle {
        raw_heads: Vec<C>,
        cyclic_sources: Vec<C>,
        involved_owner_grants: BTreeSet<MembershipGrantId>,
        maximal_valid_branches: Vec<CausalGrantBranch<C, A>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CausalGrantStatus<C: CausalCoordinate, A: CausalAssignment> {
    Resolved(ReducedGrants<C, A>),
    Conflict(CausalGrantConflict<C, A>),
}

impl<C: CausalCoordinate, A: CausalAssignment> ReducedGrants<C, A> {
    pub(crate) fn active_grant(&self, grant: &MembershipGrantId) -> Option<&GrantRecord<C, A>> {
        self.grants.get(grant).and_then(GrantState::active)
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
    #[error("causal assignment history leaves no active Owner")]
    NoActiveOwner,
    #[error(
        "causal assignment revocation cycle has {sources} sources, exceeding the protocol limit of {maximum}"
    )]
    RevocationCycleTooWide { sources: usize, maximum: usize },
}

#[derive(Debug, Clone)]
struct CausalState<C: CausalCoordinate, A: CausalAssignment> {
    grants: BTreeMap<MembershipGrantId, GrantState<GrantRecord<C, A>, CausalGrantRetirement<C>>>,
}

impl<C: CausalCoordinate, A: CausalAssignment> Default for CausalState<C, A> {
    fn default() -> Self {
        Self {
            grants: BTreeMap::new(),
        }
    }
}

impl<C: CausalCoordinate, A: CausalAssignment> CausalState<C, A> {
    fn merge(&mut self, other: &Self, index: usize) -> Result<(), CausalGrantError<C>> {
        for (grant, state) in &other.grants {
            match self.grants.entry(grant.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(state.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if !entry.get_mut().merge(state) {
                        return Err(CausalGrantError::DuplicateGrant {
                            index,
                            grant: grant.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn active_owner(&self, grant: &MembershipGrantId, pubkey: &str) -> bool {
        self.grants
            .get(grant)
            .and_then(GrantState::active)
            .is_some_and(|record| record.member_pubkey == pubkey && record.assignment.is_owner())
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
                owner_barriers,
            } => {
                self.remove(
                    index,
                    coord,
                    member_pubkey,
                    replaces,
                    owner_barriers,
                    false,
                    validate_exact_grants,
                )?;
                self.insert(index, coord, member_pubkey, grant_id, assignment.clone())?;
            }
            CausalChange::RemoveMember {
                member_pubkey,
                removes,
                owner_barriers,
            } => self.remove(
                index,
                coord,
                member_pubkey,
                removes,
                owner_barriers,
                true,
                validate_exact_grants,
            )?,
            CausalChange::Control | CausalChange::ResolutionActivation => {}
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
            GrantState::Active {
                record: GrantRecord {
                    member_pubkey: member_pubkey.to_string(),
                    assignment,
                    creation: CausalGrantCreation::Entry(coord.clone()),
                },
            },
        );
        Ok(())
    }

    fn remove(
        &mut self,
        index: usize,
        coord: &C,
        member_pubkey: &str,
        grants: &BTreeSet<MembershipGrantId>,
        owner_barriers: &BTreeMap<MembershipGrantId, OwnerGrantBarrier<C>>,
        require_nonempty: bool,
        validate_exact_grants: bool,
    ) -> Result<(), CausalGrantError<C>> {
        if require_nonempty && grants.is_empty() {
            return Err(CausalGrantError::EmptyRemoval { index });
        }
        let active = self
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                state
                    .active()
                    .is_some_and(|record| record.member_pubkey == member_pubkey)
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
            let state =
                self.grants
                    .get_mut(grant)
                    .ok_or_else(|| CausalGrantError::GrantOwnerMismatch {
                        index,
                        grant: grant.clone(),
                    })?;
            if state.record().member_pubkey != member_pubkey {
                return Err(CausalGrantError::GrantOwnerMismatch {
                    index,
                    grant: grant.clone(),
                });
            }
            let retirement = CausalGrantRetirement::Entry {
                coord: coord.clone(),
                owner_barrier: owner_barriers.get(grant).cloned(),
            };
            match state {
                GrantState::Active { record } => {
                    *state = GrantState::Tombstoned {
                        record: record.clone(),
                        retirements: GrantRetirements::new(retirement),
                    };
                }
                GrantState::Tombstoned { retirements, .. } => {
                    retirements.insert(retirement);
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn reduce<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
) -> Result<CausalGrantStatus<C, A>, CausalGrantError<C>> {
    reduce_internal(entries, &[], &[], &BTreeMap::new(), &BTreeSet::new(), true)
}

pub(crate) fn reduce_from_checkpoint<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    raw_checkpoint_heads: &[C],
    effective_checkpoint_frontier: &[C],
    seed_grants: &BTreeMap<MembershipGrantId, GrantState<CausalSeedGrant<A>, ()>>,
    seed_included: &BTreeSet<C>,
) -> Result<CausalGrantStatus<C, A>, CausalGrantError<C>> {
    reduce_internal(
        entries,
        raw_checkpoint_heads,
        effective_checkpoint_frontier,
        seed_grants,
        seed_included,
        false,
    )
}

fn reduce_internal<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    raw_checkpoint_heads: &[C],
    effective_checkpoint_frontier: &[C],
    seed_grants: &BTreeMap<MembershipGrantId, GrantState<CausalSeedGrant<A>, ()>>,
    seed_included: &BTreeSet<C>,
    require_founder: bool,
) -> Result<CausalGrantStatus<C, A>, CausalGrantError<C>> {
    if entries.is_empty() && raw_checkpoint_heads.is_empty() {
        return Err(CausalGrantError::Empty);
    }
    let checkpoint_by_stream = raw_checkpoint_heads
        .iter()
        .map(|coord| (coord.stream_key(), coord.clone()))
        .collect::<BTreeMap<_, _>>();
    if checkpoint_by_stream.len() != raw_checkpoint_heads.len() {
        return Err(CausalGrantError::ConflictingSequence {
            stream: raw_checkpoint_heads[0].stream_key(),
            seq: raw_checkpoint_heads[0].seq(),
        });
    }
    let checkpoint_set = seed_included.iter().cloned().collect::<BTreeSet<_>>();
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
        let checkpoint = checkpoint_by_stream.get(stream);
        let first_seq = checkpoint.map_or(1, |coord| coord.seq() + 1);
        if positions.keys().next().copied() != Some(first_seq) {
            return Err(CausalGrantError::MissingSequence {
                stream: stream.clone(),
                seq: first_seq,
            });
        }
        let mut previous_hash = checkpoint.map(|coord| coord.entry_hash());
        for seq in first_seq..=max_seq {
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
                let predecessor = if seq == first_seq {
                    checkpoint.expect("a suffix above sequence one has a checkpoint predecessor")
                } else {
                    &entries[positions[&(seq - 1)]].coord
                };
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
            if !index_by_coord.contains_key(dependency) && !checkpoint_set.contains(dependency) {
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
    if (require_founder && founders.len() != 1) || (!require_founder && !founders.is_empty()) {
        return Err(CausalGrantError::InvalidFounder);
    }
    let founder_index = founders.first().copied();
    if let Some(founder_index) = founder_index {
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
    }

    let mut remaining = (0..entries.len()).collect::<BTreeSet<_>>();
    let mut states = BTreeMap::<C, CausalState<C, A>>::new();
    let seed_state = CausalState {
        grants: seed_grants
            .iter()
            .map(|(grant, state)| {
                let record = state.record();
                let record = GrantRecord {
                    member_pubkey: record.member_pubkey.clone(),
                    assignment: record.assignment.clone(),
                    creation: CausalGrantCreation::Checkpoint,
                };
                (
                    grant.clone(),
                    match state {
                        GrantState::Active { .. } => GrantState::Active { record },
                        GrantState::Tombstoned { .. } => GrantState::Tombstoned {
                            record,
                            retirements: GrantRetirements::new(CausalGrantRetirement::Checkpoint),
                        },
                    },
                )
            })
            .collect(),
    };
    for head in effective_checkpoint_frontier {
        states.insert(head.clone(), seed_state.clone());
    }
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
            if Some(index) != founder_index
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
                    .is_some_and(|state| state.record().assignment.is_owner())
                {
                    all_sources.insert(index);
                }
            }
        }
    }

    let mut sources = all_sources.clone();
    let mut source_sets = Vec::new();
    let mut source_set_positions = BTreeMap::new();
    let included = loop {
        if let Some(cycle_start) = source_set_positions.get(&sources).copied() {
            let cyclic_source_indices = source_sets[cycle_start..]
                .iter()
                .flat_map(BTreeSet::iter)
                .copied()
                .chain(sources.iter().copied())
                .collect::<BTreeSet<_>>();
            return Ok(CausalGrantStatus::Conflict(revocation_cycle_conflict(
                entries,
                &index_by_coord,
                &raw,
                &all_sources,
                &cyclic_source_indices,
                &causal_order,
                raw_checkpoint_heads,
                effective_checkpoint_frontier,
                seed_included,
                &seed_state,
            )?));
        }
        source_set_positions.insert(sources.clone(), source_sets.len());
        source_sets.push(sources.clone());
        let cap_sources = cap_sources(entries, &raw, &sources);
        let included = included_entries(entries, &index_by_coord, &cap_sources, &checkpoint_set);
        let next_sources = all_sources
            .intersection(&included)
            .copied()
            .collect::<BTreeSet<_>>();
        if next_sources == sources {
            break included;
        }
        sources = next_sources;
    };

    let effective = effective_state(entries, &causal_order, &included, &seed_state)?;

    let mut active_by_member = BTreeMap::<String, Vec<MembershipGrantId>>::new();
    for (grant, state) in &effective.grants {
        let Some(record) = state.active() else {
            continue;
        };
        active_by_member
            .entry(record.member_pubkey.clone())
            .or_default()
            .push(grant.clone());
    }
    if let Some((member_pubkey, grants)) = active_by_member
        .into_iter()
        .find(|(_, grants)| grants.len() > 1)
    {
        let conflicting_grants = grants
            .iter()
            .map(|grant| {
                (
                    grant.clone(),
                    effective.grants[grant]
                        .active()
                        .expect("conflicting grant is active")
                        .clone(),
                )
            })
            .collect();
        let uncontested_grants = effective
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                (!grants.contains(grant))
                    .then(|| state.active().map(|record| (grant.clone(), record.clone())))
                    .flatten()
            })
            .collect();
        let seed_included = seed_included.iter().cloned().collect::<Vec<_>>();
        let reduced = reduced_from_state(entries, effective, included, &seed_included);
        return Ok(CausalGrantStatus::Conflict(
            CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads: frontier_with_checkpoint(
                    entries,
                    0..entries.len(),
                    raw_checkpoint_heads,
                ),
                effective_frontier: frontier_with_checkpoint(
                    entries,
                    reduced
                        .included
                        .iter()
                        .filter_map(|coord| index_by_coord.get(coord).copied()),
                    effective_checkpoint_frontier,
                ),
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            },
        ));
    }
    require_owner(&effective)?;

    let seed_included = seed_included.iter().cloned().collect::<Vec<_>>();
    Ok(CausalGrantStatus::Resolved(reduced_from_state(
        entries,
        effective,
        included,
        &seed_included,
    )))
}

fn effective_state<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    causal_order: &[usize],
    included: &BTreeSet<usize>,
    seed: &CausalState<C, A>,
) -> Result<CausalState<C, A>, CausalGrantError<C>> {
    let mut effective = seed.clone();
    for index in causal_order {
        if included.contains(index) {
            effective.apply(
                &entries[*index].coord,
                &entries[*index].change,
                *index,
                false,
            )?;
        }
    }
    Ok(effective)
}

fn reduced_from_state<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    state: CausalState<C, A>,
    included: BTreeSet<usize>,
    checkpoint_heads: &[C],
) -> ReducedGrants<C, A> {
    ReducedGrants {
        grants: state.grants,
        included: checkpoint_heads
            .iter()
            .cloned()
            .chain(
                included
                    .into_iter()
                    .map(|index| entries[index].coord.clone()),
            )
            .collect(),
    }
}

fn frontier_with_checkpoint<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    indices: impl IntoIterator<Item = usize>,
    checkpoint_heads: &[C],
) -> Vec<C> {
    stream_frontier(
        checkpoint_heads.iter().cloned().chain(
            indices
                .into_iter()
                .map(|index| entries[index].coord.clone()),
        ),
    )
}

fn revocation_cycle_conflict<C: CausalCoordinate, A: CausalAssignment>(
    entries: &[CausalEntry<C, A>],
    index_by_coord: &BTreeMap<C, usize>,
    raw: &CausalState<C, A>,
    all_sources: &BTreeSet<usize>,
    cyclic_source_indices: &BTreeSet<usize>,
    causal_order: &[usize],
    raw_checkpoint_heads: &[C],
    effective_checkpoint_frontier: &[C],
    seed_included: &BTreeSet<C>,
    seed: &CausalState<C, A>,
) -> Result<CausalGrantConflict<C, A>, CausalGrantError<C>> {
    if cyclic_source_indices.len() > MAX_CYCLIC_REVOCATION_SOURCES {
        return Err(CausalGrantError::RevocationCycleTooWide {
            sources: cyclic_source_indices.len(),
            maximum: MAX_CYCLIC_REVOCATION_SOURCES,
        });
    }
    let checkpoint_set = seed_included.clone();
    let mandatory_sources = all_sources
        .difference(cyclic_source_indices)
        .copied()
        .collect::<BTreeSet<_>>();
    let attacks = all_sources
        .iter()
        .map(|source| {
            let selected = BTreeSet::from([*source]);
            let included = included_entries(
                entries,
                index_by_coord,
                &cap_sources(entries, raw, &selected),
                &checkpoint_set,
            );
            (
                *source,
                all_sources.difference(&included).copied().collect(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let (fixed_source_sets, _) =
        fixed_sets_from_attack_graph(&attacks, &mandatory_sources, cyclic_source_indices.len())
            .map_err(|sources| CausalGrantError::RevocationCycleTooWide {
                sources,
                maximum: MAX_CYCLIC_REVOCATION_SOURCES,
            })?;
    let mut branches = Vec::new();
    let raw_heads = frontier_with_checkpoint(entries, 0..entries.len(), raw_checkpoint_heads);
    for sources in fixed_source_sets {
        let included = included_entries(
            entries,
            index_by_coord,
            &cap_sources(entries, raw, &sources),
            &checkpoint_set,
        );
        let next_sources = all_sources
            .intersection(&included)
            .copied()
            .collect::<BTreeSet<_>>();
        if next_sources != sources {
            continue;
        }
        let effective = effective_state(entries, causal_order, &included, seed)?;
        if !has_owner(&effective) || has_concurrent_assignments(&effective) {
            continue;
        }
        let reduced = reduced_from_state(
            entries,
            effective,
            included.clone(),
            &seed_included.iter().cloned().collect::<Vec<_>>(),
        );
        branches.push(CausalGrantBranch {
            raw_heads: raw_heads
                .iter()
                .filter(|coord| {
                    seed_included.contains(*coord)
                        || index_by_coord
                            .get(*coord)
                            .is_some_and(|index| included.contains(index))
                })
                .cloned()
                .collect(),
            effective_frontier: frontier_with_checkpoint(
                entries,
                included.iter().copied(),
                effective_checkpoint_frontier,
            ),
            reduced,
        });
    }
    let branch_inclusions = branches
        .iter()
        .map(|branch| branch.reduced.included.clone())
        .collect::<Vec<_>>();
    branches.retain(|branch| {
        !branch_inclusions.iter().any(|other| {
            branch.reduced.included != *other && branch.reduced.included.is_subset(other)
        })
    });
    branches.sort_by(|left, right| left.effective_frontier.cmp(&right.effective_frontier));

    let involved_owner_grants = cyclic_source_indices
        .iter()
        .flat_map(|index| {
            entries[*index]
                .change
                .removed()
                .into_iter()
                .flat_map(|(removed, _)| removed.iter())
        })
        .filter(|grant| {
            raw.grants
                .get(*grant)
                .is_some_and(|state| state.record().assignment.is_owner())
        })
        .cloned()
        .collect();
    let mut cyclic_sources = cyclic_source_indices
        .iter()
        .map(|index| entries[*index].coord.clone())
        .collect::<Vec<_>>();
    cyclic_sources.sort();
    Ok(CausalGrantConflict::RevocationCycle {
        raw_heads,
        cyclic_sources,
        involved_owner_grants,
        maximal_valid_branches: branches,
    })
}

fn fixed_sets_from_attack_graph(
    attacks: &BTreeMap<usize, BTreeSet<usize>>,
    mandatory: &BTreeSet<usize>,
    cyclic_source_count: usize,
) -> Result<(Vec<BTreeSet<usize>>, usize), usize> {
    if cyclic_source_count > MAX_CYCLIC_REVOCATION_SOURCES {
        return Err(cyclic_source_count);
    }
    let components = attack_graph_components(attacks);
    let mut memo = BTreeSet::new();
    let mut fixed = BTreeSet::new();
    let mut explored = 0;
    search_fixed_sets(
        attacks,
        &components,
        mandatory.clone(),
        &mut memo,
        &mut fixed,
        &mut explored,
    );
    Ok((fixed.into_iter().collect(), explored))
}

fn search_fixed_sets(
    attacks: &BTreeMap<usize, BTreeSet<usize>>,
    components: &[Vec<usize>],
    mut selected: BTreeSet<usize>,
    memo: &mut BTreeSet<BTreeSet<usize>>,
    fixed: &mut BTreeSet<BTreeSet<usize>>,
    explored: &mut usize,
) {
    if !memo.insert(selected.clone()) {
        return;
    }
    *explored += 1;
    loop {
        let attacked = selected
            .iter()
            .flat_map(|source| attacks[source].iter().copied())
            .collect::<BTreeSet<_>>();
        if !selected.is_disjoint(&attacked) {
            return;
        }
        let undecided = attacks
            .keys()
            .copied()
            .filter(|node| !selected.contains(node) && !attacked.contains(node))
            .collect::<BTreeSet<_>>();
        if undecided.is_empty() {
            fixed.insert(selected);
            return;
        }
        let forced = undecided.iter().copied().find(|node| {
            !undecided
                .iter()
                .any(|source| attacks[source].contains(node))
        });
        let Some(forced) = forced else {
            let pivot = components
                .iter()
                .flat_map(|component| component.iter())
                .find(|node| undecided.contains(node))
                .copied()
                .expect("an undecided attack-graph node exists");
            let alternatives = std::iter::once(pivot)
                .chain(
                    undecided
                        .iter()
                        .copied()
                        .filter(|source| attacks[source].contains(&pivot)),
                )
                .collect::<BTreeSet<_>>();
            for source in alternatives {
                let mut branch = selected.clone();
                branch.insert(source);
                search_fixed_sets(attacks, components, branch, memo, fixed, explored);
            }
            return;
        };
        selected.insert(forced);
    }
}

fn attack_graph_components(attacks: &BTreeMap<usize, BTreeSet<usize>>) -> Vec<Vec<usize>> {
    fn visit(
        node: usize,
        edges: &BTreeMap<usize, BTreeSet<usize>>,
        seen: &mut BTreeSet<usize>,
        order: &mut Vec<usize>,
    ) {
        if !seen.insert(node) {
            return;
        }
        for target in &edges[&node] {
            if edges.contains_key(target) {
                visit(*target, edges, seen, order);
            }
        }
        order.push(node);
    }

    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    for node in attacks.keys().copied() {
        visit(node, attacks, &mut seen, &mut order);
    }
    let mut reverse = attacks
        .keys()
        .copied()
        .map(|node| (node, BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (source, targets) in attacks {
        for target in targets {
            if let Some(incoming) = reverse.get_mut(target) {
                incoming.insert(*source);
            }
        }
    }
    let mut components = Vec::new();
    seen.clear();
    for node in order.into_iter().rev() {
        if seen.contains(&node) {
            continue;
        }
        let mut component_order = Vec::new();
        visit(node, &reverse, &mut seen, &mut component_order);
        component_order.sort_unstable();
        components.push(component_order);
    }
    components
}

fn has_concurrent_assignments<C: CausalCoordinate, A: CausalAssignment>(
    state: &CausalState<C, A>,
) -> bool {
    let mut members = BTreeSet::new();
    state.grants.values().any(|state| {
        state
            .active()
            .is_some_and(|record| !members.insert(record.member_pubkey.clone()))
    })
}

fn has_owner<C: CausalCoordinate, A: CausalAssignment>(state: &CausalState<C, A>) -> bool {
    state.grants.values().any(|state| {
        state
            .active()
            .is_some_and(|record| record.assignment.is_owner())
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
                .is_some_and(|state| state.record().assignment.is_owner())
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
    checkpoint: &BTreeSet<C>,
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
                entries[*index].dependencies.values().any(|dependency| {
                    !checkpoint.contains(dependency)
                        && !index_by_coord
                            .get(dependency)
                            .is_some_and(|index| included.contains(index))
                })
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
                .and_then(GrantState::active)
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
            .and_then(GrantState::active)
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
    if has_owner(state) {
        Ok(())
    } else {
        Err(CausalGrantError::NoActiveOwner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_set_search_propagates_an_unattacked_graph_without_subset_enumeration() {
        let attacks = (0..64)
            .map(|index| (index, BTreeSet::new()))
            .collect::<BTreeMap<_, _>>();
        let (sets, explored_states) =
            fixed_sets_from_attack_graph(&attacks, &BTreeSet::new(), 0).unwrap();

        assert_eq!(sets, vec![(0..64).collect()]);
        assert!(
            explored_states <= 2,
            "constraint propagation explored {explored_states} states"
        );
    }

    #[test]
    fn fixed_set_search_rejects_disjoint_cycles_beyond_the_signed_protocol_bound() {
        let attacks = (0..14)
            .map(|index| (index, BTreeSet::from([index ^ 1])))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            fixed_sets_from_attack_graph(&attacks, &BTreeSet::new(), attacks.len()),
            Err(14)
        );
    }

    #[test]
    fn independent_resolution_checkpoints_replay_in_canonical_order() {
        let first = ObjectHash::digest(b"first independent checkpoint");
        let second = ObjectHash::digest(b"second independent checkpoint");
        let dependencies = BTreeMap::from([(first, BTreeSet::new()), (second, BTreeSet::new())]);
        let expected = *dependencies.keys().next().expect("two checkpoints");

        assert_eq!(
            canonical_ready_checkpoint(dependencies.iter(), &BTreeSet::new()),
            Some(expected)
        );
    }

    #[test]
    fn canonical_checkpoint_order_skips_a_lower_key_until_its_dependency_is_applied() {
        let dependencies = BTreeMap::from([(1u8, BTreeSet::from([2u8])), (2u8, BTreeSet::new())]);
        let mut applied = BTreeSet::new();

        let first = canonical_ready_checkpoint(dependencies.iter(), &applied)
            .expect("dependency checkpoint is ready");
        assert_eq!(first, 2);
        applied.insert(first);
        let second = canonical_ready_checkpoint(dependencies.iter(), &applied)
            .expect("dependent checkpoint becomes ready");
        assert_eq!(second, 1);
    }

    #[test]
    fn cyclic_resolution_checkpoints_have_no_ready_cut() {
        let first = ObjectHash::digest(b"first cyclic checkpoint");
        let second = ObjectHash::digest(b"second cyclic checkpoint");
        let dependencies = BTreeMap::from([
            (first, BTreeSet::from([second])),
            (second, BTreeSet::from([first])),
        ]);

        assert_eq!(
            canonical_ready_checkpoint(dependencies.iter(), &BTreeSet::new()),
            None
        );
    }

    #[test]
    fn full_checkpoint_merge_preserves_a_removal_seen_by_only_one_branch() {
        let grant = MembershipGrantId(ObjectHash::digest(b"removed checkpoint grant"));
        let retired = BTreeMap::from([(
            grant.clone(),
            GrantState::Tombstoned {
                record: "member",
                retirements: GrantRetirements::new("first retirement"),
            },
        )]);
        let active = BTreeMap::from([(grant.clone(), GrantState::Active { record: "member" })]);
        let mut merged_grants = BTreeMap::new();
        let mut merged_included = BTreeSet::new();

        assert!(merge_checkpoint_evidence(
            &mut merged_grants,
            &mut merged_included,
            &retired,
            &BTreeSet::from([1]),
        ));
        assert!(merge_checkpoint_evidence(
            &mut merged_grants,
            &mut merged_included,
            &active,
            &BTreeSet::from([2]),
        ));

        assert_eq!(merged_grants, retired);
        assert_eq!(merged_included, BTreeSet::from([1, 2]));
    }

    #[test]
    fn tombstoned_grant_rejects_an_empty_retirement_set() {
        let encoded = serde_json::json!({
            "tombstoned": {
                "record": "member",
                "retirements": [],
            }
        });

        assert!(serde_json::from_value::<GrantState<String, String>>(encoded).is_err());
    }

    #[test]
    fn checkpoint_merge_unions_concurrent_retirement_evidence() {
        let grant = MembershipGrantId(ObjectHash::digest(b"concurrently retired grant"));
        let branch = |retirement| {
            BTreeMap::from([(
                grant.clone(),
                GrantState::Tombstoned {
                    record: "member",
                    retirements: GrantRetirements::new(retirement),
                },
            )])
        };
        let mut merged = BTreeMap::new();
        let mut included = BTreeSet::new();

        assert!(merge_checkpoint_evidence(
            &mut merged,
            &mut included,
            &branch("first retirement"),
            &BTreeSet::from([1]),
        ));
        assert!(merge_checkpoint_evidence(
            &mut merged,
            &mut included,
            &branch("second retirement"),
            &BTreeSet::from([2]),
        ));

        let GrantState::Tombstoned { retirements, .. } = &merged[&grant] else {
            panic!("retirement must dominate an active branch")
        };
        assert_eq!(
            retirements.as_set(),
            &BTreeSet::from(["first retirement", "second retirement"])
        );
    }
}
