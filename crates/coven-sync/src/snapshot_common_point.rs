//! The common point a reload commits its audiences at (§15). Selections stay
//! fixed, and the target is the frozen requirement and the selected positions.
//! Each round reads every write some audience lacks by exact number, then
//! establishes each missing one from a catalog listed after those reads:
//! counting coverage in any audience shows it was published, so its absence
//! is a deletion. Then an audience whose current newest snapshot reaches it
//! is replaced by that snapshot, and the round starts again at the new target.
//! Otherwise the write has no part in that audience (io `CommonReload.round`,
//! `FixedReload.scan`).

use super::catalog::Verified;
use crate::SyncError;
use coven_database::StoreLog;
use coven_format::value::WritePositions;
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use coven_storage::ObjectPath;
use std::collections::{BTreeMap, BTreeSet};

/// Where one audience's reloaded state starts.
pub(crate) struct Start<B> {
    /// The writes that start already holds.
    pub(crate) positions: WritePositions,
    /// The catalog it was chosen from: 0 for the reload's first, `r + 1` for
    /// the one listed after round `r`'s reads.
    pub(crate) selected_at: usize,
    pub(crate) origin: Origin<B>,
}

pub(crate) enum Origin<B> {
    /// Its existing state, at this device's positions.
    Kept,
    /// No snapshot: every log from its start.
    Empty,
    /// The selected snapshot.
    Selected(B),
}

/// What keeps an audience's current newest snapshot unknown.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Unknown {
    /// A listed snapshot in a newer format may be the newest (§15, §17.2).
    Update(crate::RequiredUpdate),
    /// A listed prefix this device has not verified may be the newest; its
    /// own record says why (§19.1).
    Unverified(ObjectPath),
}

/// One readable audience in a catalog listed after a round's reads.
pub(crate) struct FreshAudience {
    /// Its verified counting snapshots, newest first.
    pub(crate) verified: Vec<Verified>,
    pub(crate) unknown: Option<Unknown>,
}

/// A complete catalog of every audience's snapshots, listed after a round's
/// reads.
pub(crate) struct Fresh {
    pub(crate) readable: BTreeMap<Audience, FreshAudience>,
    /// The verified counting prefixes of audiences this device cannot read.
    pub(crate) others: Vec<WritePositions>,
}

impl Fresh {
    /// Counting coverage in any audience, readable or not, shows the write
    /// was published (§15; io `covered_miss_published`).
    fn published(&self, id: WriteId) -> bool {
        self.readable
            .values()
            .flat_map(|audience| audience.verified.iter().map(|s| &s.prefix.writes))
            .chain(&self.others)
            .any(|positions| positions.covers(id))
    }

    /// The audience's current newest counting snapshot: the newest verified
    /// prefix that follows the replay's boundary.
    pub(crate) fn current(&self, audience: &Audience, log: &StoreLog) -> Option<&Verified> {
        self.readable
            .get(audience)?
            .verified
            .iter()
            .find(|snapshot| snapshot.follows_boundary(log))
    }
}

/// The reads one reload makes.
pub(crate) trait Reads {
    /// What loading a selected snapshot needs.
    type Body;
    /// Whether the write's object exists, at its exact path.
    async fn present(&mut self, id: WriteId) -> Result<bool, SyncError>;
    /// A complete catalog listed after the reads before it.
    async fn refresh(&mut self) -> Result<Fresh, SyncError>;
    /// Select the audience's current newest snapshot in the latest catalog.
    async fn select(&mut self, audience: &Audience) -> Result<Self::Body, SyncError>;
}

/// A completed round's inputs to commit together.
pub(crate) struct Plan<B> {
    /// The final starts; the common point is their `target`.
    pub(crate) starts: BTreeMap<Audience, Start<B>>,
    /// Writes some audience lacks, present in the committing round.
    pub(crate) present: BTreeSet<WriteId>,
    /// Deleted writes some audience lacks. Each such audience's current
    /// newest snapshot did not reach it, so it has no part there.
    pub(crate) deleted: BTreeSet<WriteId>,
}

pub(crate) enum Outcome<B> {
    Complete(Plan<B>),
    /// A missing write no counting snapshot shows published: the history
    /// stays unresolved until it lands or a snapshot covers it.
    Unpublished(WriteId),
    /// An audience lacking a deleted write cannot tell whether it has a part
    /// there, since its current newest snapshot is unknown.
    Unknown(Unknown),
}

/// Each writer's common target: the frozen requirement and every start's
/// positions (io `CommonReload.target`).
pub(crate) fn target<B>(
    required: &WritePositions,
    starts: &BTreeMap<Audience, Start<B>>,
) -> BTreeMap<DeviceId, u64> {
    let mut target = BTreeMap::new();
    super::include_positions(
        &mut target,
        required
            .0
            .iter()
            .chain(starts.values().flat_map(|start| &start.positions.0)),
    );
    target
}

/// Whether a write still needs reading for this start: it does not hold it,
/// and no deletion established by the catalog it was chosen from or earlier
/// shows it has no part there (io `FixedReload.scan`'s `knownDeleted`).
fn lacks<B>(start: &Start<B>, id: WriteId, certified: &BTreeMap<WriteId, usize>) -> bool {
    !start.positions.covers(id) && certified.get(&id).is_none_or(|at| *at > start.selected_at)
}

/// Run rounds until one commits; `starts` holds every readable audience.
/// `waiting` are this device's queued writes, which the reload applies from
/// its queue. A selection changes only when a deletion forces it.
pub(crate) async fn common_point<R: Reads>(
    reads: &mut R,
    log: &StoreLog,
    mut starts: BTreeMap<Audience, Start<R::Body>>,
    required: &WritePositions,
    waiting: &BTreeSet<WriteId>,
) -> Result<Outcome<R::Body>, SyncError> {
    // The catalog after which each deletion was established.
    let mut certified = BTreeMap::new();
    for catalog in 1.. {
        let target = target(required, &starts);
        // Read to the fixed target by exact number, never a listing of the
        // moving log tail (`committed_prefix`).
        let mut inventory = Vec::new();
        for (device, top) in &target {
            let lowest = starts
                .values()
                .map(|start| super::point(&start.positions, *device))
                .min()
                .unwrap_or(0);
            for number in lowest + 1..=*top {
                let id = WriteId {
                    device: *device,
                    number,
                };
                if !waiting.contains(&id)
                    && starts.values().any(|start| lacks(start, id, &certified))
                {
                    inventory.push(id);
                }
            }
        }
        let mut present = BTreeSet::new();
        let mut absent = BTreeSet::new();
        for id in &inventory {
            if reads.present(*id).await? {
                present.insert(*id);
            } else {
                absent.insert(*id);
            }
        }
        let fresh = match absent.is_empty() {
            true => None,
            false => Some(reads.refresh().await?),
        };
        for id in &absent {
            if fresh.as_ref().is_some_and(|fresh| fresh.published(*id)) {
                certified.entry(*id).or_insert(catalog);
            }
        }
        // Each audience in turn, over its missing writes in order: the first
        // whose current newest snapshot reaches one is replaced by it.
        let mut replace = None;
        'audiences: for (audience, start) in &starts {
            for id in &absent {
                // This round's deletions were established after every
                // current selection, so they leave `lacks` unchanged.
                if !lacks(start, *id, &certified) {
                    continue;
                }
                let Some(fresh) = &fresh else {
                    unreachable!("a missing write refreshed the catalog")
                };
                if !certified.contains_key(id) {
                    return Ok(Outcome::Unpublished(*id));
                }
                let listed = fresh
                    .readable
                    .get(audience)
                    .expect("every reloaded audience is readable");
                if let Some(Unknown::Update(update)) = &listed.unknown {
                    return Ok(Outcome::Unknown(Unknown::Update(*update)));
                }
                match fresh.current(audience, log) {
                    Some(current) if current.prefix.writes.covers(*id) => {
                        replace = Some((audience.clone(), current.prefix.writes.clone()));
                        break 'audiences;
                    }
                    // Its current newest does not reach the write, so the
                    // write has no part there (`skipped_has_no_part`), unless
                    // an unverified prefix is the newest.
                    _ => {
                        if let Some(unknown) = &listed.unknown {
                            return Ok(Outcome::Unknown(unknown.clone()));
                        }
                    }
                }
            }
        }
        let Some((audience, positions)) = replace else {
            let deleted = inventory
                .iter()
                .filter(|id| !present.contains(id))
                .copied()
                .collect();
            return Ok(Outcome::Complete(Plan {
                starts,
                present,
                deleted,
            }));
        };
        let body = reads.select(&audience).await?;
        starts.insert(
            audience.clone(),
            Start {
                positions,
                selected_at: catalog,
                origin: Origin::Selected(body),
            },
        );
    }
    unreachable!("rounds continue until one commits")
}
