//! §15's retention hold: which snapshot each boundary replay could still
//! select holds a write's part until it covers it (io `RaiseHistory.Held`).

use coven_database::{Boundary, StoreLog};
use coven_format::{
    sealed_snapshot::SnapshotObjectPrefix,
    store_log::{SnapshotId, StoreChange},
    value::EntryId,
};
use coven_merge::Audience;

/// A boundary replay could still select for an audience: the snapshot it
/// names and, when this device holds an entry naming it, that entry, whose
/// readers it admits. A listed prefix whose view holds an entry this device
/// lacks does not verify here, so an unknown entry admits no reader.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PossibleBoundary {
    pub(crate) snapshot: SnapshotId,
    pub(crate) entry: Option<EntryId>,
}

impl PossibleBoundary {
    fn admits(&self, id: &SnapshotId, prefix: &SnapshotObjectPrefix) -> bool {
        self.snapshot == *id
            || self
                .entry
                .is_some_and(|entry| prefix.store_log.covers(entry))
    }
}

/// §15's retention hold (io `RaiseHistory.Held`): the positions of the
/// newest usable counting snapshot under every boundary replay could still
/// select for this audience, which must all cover a write before its part
/// here is deleted. Those boundaries are the current one, the one the final
/// entries alone select, every non-final raise entry above the final version,
/// and every listed raise snapshot above it that no held entry names, which a
/// raise entry published late could still name. None when one of them
/// selects nothing.
pub(super) fn held_coverage(
    log: &StoreLog,
    audience: &Audience,
    verified: &[super::catalog::Verified],
) -> Option<Vec<coven_format::value::WritePositions>> {
    let here = |boundary: &Boundary| boundary.snapshot().audience == *audience;
    let named = |boundary: Boundary| PossibleBoundary {
        snapshot: boundary.snapshot().clone(),
        entry: Some(boundary.entry()),
    };
    let finals: Vec<_> = log.final_boundaries().filter(here).collect();
    let final_version = finals
        .iter()
        .filter_map(|boundary| match boundary {
            Boundary::SchemaChange(version) => Some(version.number),
            Boundary::Reset { .. } => None,
        })
        .max()
        .unwrap_or(0);
    let mut possible = vec![
        log.boundaries().filter(here).last().map(named),
        finals.last().cloned().map(named),
    ];
    for applied in &log.entries {
        let position = applied.entry.position;
        let decided = log.is_final(position)
            && log.replay.entries.get(&position) != Some(&coven_database::EntryOutcome::Kept);
        if let Some(Boundary::SchemaChange(version)) = Boundary::from_entry(&applied.entry) {
            if version.snapshot.audience == *audience && version.number > final_version && !decided
            {
                possible.push(Some(named(Boundary::SchemaChange(version))));
            }
        }
    }
    // A listed raise snapshot no held entry names yet: an entry published
    // later could name it, and no verified prefix can have read that entry.
    for snapshot in verified {
        let id = snapshot
            .object
            .path
            .snapshot_id()
            .expect("checked snapshot path");
        if snapshot
            .prefix
            .raise
            .is_some_and(|version| version.get() > final_version)
            && !log.entries.iter().any(|applied| {
                matches!(&applied.entry.change, StoreChange::RaiseSchema { snapshot, .. } if *snapshot == id)
            })
        {
            possible.push(Some(PossibleBoundary {
                snapshot: id,
                entry: None,
            }));
        }
    }
    held_selections(&possible, verified)
}

/// The newest counting snapshot each possible boundary admits, from the
/// audience's verified snapshots, newest first; none when one admits none.
/// io `RaiseHistory.selected` under each boundary of `Held`.
pub(crate) fn held_selections(
    possible: &[Option<PossibleBoundary>],
    verified: &[super::catalog::Verified],
) -> Option<Vec<coven_format::value::WritePositions>> {
    let mut selections = Vec::new();
    for (index, boundary) in possible.iter().enumerate() {
        if possible[..index].contains(boundary) {
            continue;
        }
        let selected = verified.iter().find(|snapshot| {
            boundary.as_ref().is_none_or(|boundary| {
                boundary.admits(
                    &snapshot
                        .object
                        .path
                        .snapshot_id()
                        .expect("checked snapshot path"),
                    &snapshot.prefix,
                )
            })
        })?;
        selections.push(selected.prefix.writes.clone());
    }
    Some(selections)
}
