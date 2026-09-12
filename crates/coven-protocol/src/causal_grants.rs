//! Shared causal assignment reducer for Store membership and Circle rosters.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::store_commit::ObjectHash;

mod reduction;

pub(crate) use reduction::reduce;

/// The first key whose dependencies are all applied, in canonical key order.
pub fn canonical_ready_node<'a, K: Clone + Ord + 'a>(
    mut dependencies: impl Iterator<Item = (&'a K, &'a BTreeSet<K>)>,
    applied: &BTreeSet<K>,
) -> Option<K> {
    dependencies
        .find(|(_, required)| required.is_subset(applied))
        .map(|(node, _)| node.clone())
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

/// One domain's grants, keyed by grant id.
pub(crate) type CausalGrants<R, T> = BTreeMap<MembershipGrantId, GrantState<R, T>>;

/// The grants that currently hold their assignment, with their records.
pub fn active_grants<R, T: Ord>(
    grants: &CausalGrants<R, T>,
) -> impl Iterator<Item = (&MembershipGrantId, &R)> {
    grants
        .iter()
        .filter_map(|(grant, state)| state.active().map(|record| (grant, record)))
}

/// Some member holds an active Owner grant. Grant state that leaves no Owner
/// can never be extended, so no reduction may settle on it.
pub(crate) fn has_active_owner<R, T: Ord>(
    grants: &CausalGrants<R, T>,
    is_owner: impl Fn(&R) -> bool,
) -> bool {
    active_grants(grants).any(|(_, record)| is_owner(record))
}

/// Some member holds two active grants at once — the assignment conflict a
/// reduction reports instead of picking one.
pub(crate) fn has_concurrent_assignments<R, T: Ord>(
    grants: &CausalGrants<R, T>,
    member_pubkey: impl Fn(&R) -> &str,
) -> bool {
    let mut members = BTreeSet::new();
    active_grants(grants).any(|(_, record)| !members.insert(member_pubkey(record).to_string()))
}

pub(crate) fn try_map_grant_state<C, A, R, T, E>(
    state: &GrantState<GrantRecord<C, A>, CausalGrantRetirement<C>>,
    record: R,
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
    for retirement in retirements.iter() {
        let mapped_retirement = map_entry(&retirement.coord, retirement.owner_barrier.as_ref())?;
        match &mut mapped {
            Some(mapped) => {
                mapped.insert(mapped_retirement);
            }
            None => mapped = Some(GrantRetirements::new(mapped_retirement)),
        }
    }
    Ok(GrantState::Tombstoned {
        record,
        retirements: mapped.expect("causal tombstone has retirement evidence"),
    })
}

/// Derived identity of one causal author stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorStreamId([u8; 32]);

#[derive(Debug, thiserror::Error)]
pub enum AuthorStreamIdParseError {
    #[error("author stream id must be exactly 64 lowercase hexadecimal characters: {value:?}")]
    InvalidFormat { value: String },
    #[error("decode author stream id: {0}")]
    Hex(#[source] hex::FromHexError),
    #[error("author stream id has the wrong byte length")]
    WrongLength,
}

impl PartialEq for AuthorStreamIdParseError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InvalidFormat { value: left }, Self::InvalidFormat { value: right }) => {
                left == right
            }
            (Self::Hex(left), Self::Hex(right)) => left.to_string() == right.to_string(),
            (Self::WrongLength, Self::WrongLength) => true,
            _ => false,
        }
    }
}

impl Eq for AuthorStreamIdParseError {}

impl AuthorStreamId {
    pub fn from_digest(digest: ObjectHash) -> Self {
        Self(*digest.as_bytes())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for AuthorStreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for AuthorStreamId {
    type Err = AuthorStreamIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(AuthorStreamIdParseError::InvalidFormat {
                value: value.to_string(),
            });
        }
        let bytes = hex::decode(value)
            .map_err(AuthorStreamIdParseError::Hex)?
            .try_into()
            .map_err(|_| AuthorStreamIdParseError::WrongLength)?;
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

impl MembershipGrantId {
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_test_label(label: &str) -> Self {
        Self(ObjectHash::digest(label.as_bytes()))
    }
}

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

/// An entry sits where its sequence says it does: sequence one begins the
/// author stream — no predecessor, and no dependency on the very stream it
/// opens — and every later sequence continues it from a predecessor.
pub(crate) fn author_stream_position_is_valid<K: Eq>(
    seq: u64,
    previous_hash: Option<ObjectHash>,
    own_stream: &K,
    dependency_streams: impl IntoIterator<Item = K>,
) -> bool {
    match (seq, previous_hash) {
        (1, None) => dependency_streams
            .into_iter()
            .all(|stream| stream != *own_stream),
        (1, Some(_)) | (0, _) | (_, None) => false,
        (_, Some(_)) => true,
    }
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

/// Every coordinate reachable from `frontier` by walking dependencies.
///
/// A coordinate with no entry in `entries` is included but not walked through.
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
    /// Index observed stream coordinates by their stream key.
    pub(crate) fn from_observed(coords: impl IntoIterator<Item = C>) -> Self {
        Self {
            observed_streams: coords
                .into_iter()
                .map(|coord| (coord.stream_key(), coord))
                .collect(),
        }
    }

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
            Self::Founder { .. } | Self::Control => None,
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
    pub creation: C,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CausalGrantRetirement<C: CausalCoordinate> {
    pub coord: C,
    pub owner_barrier: Option<OwnerGrantBarrier<C>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReducedGrants<C: CausalCoordinate, A: CausalAssignment> {
    pub grants:
        BTreeMap<MembershipGrantId, GrantState<GrantRecord<C, A>, CausalGrantRetirement<C>>>,
    pub included: BTreeSet<C>,
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
}

#[cfg(test)]
mod tests;

/// The head references matching `coords` exactly — every coordinate has one
/// reference and nothing else is included — in canonical order. `None` when a
/// coordinate is missing or an extra reference remains.
pub(crate) fn exact_head_refs<H, C>(
    head_refs: &[H],
    coords: &[C],
    coord_of: impl Fn(&H) -> &C,
) -> Option<Vec<H>>
where
    H: Clone + Ord,
    C: Clone + Ord,
{
    let expected = coords
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut references = head_refs
        .iter()
        .filter(|reference| expected.contains(coord_of(reference)))
        .cloned()
        .collect::<Vec<_>>();
    let actual = references
        .iter()
        .map(|reference| coord_of(reference).clone())
        .collect::<std::collections::BTreeSet<_>>();
    if expected != actual || references.len() != expected.len() {
        return None;
    }
    references.sort();
    Some(references)
}
