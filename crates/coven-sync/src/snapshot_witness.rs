//! What a log write's holders establish toward deleting it (§15):
//! current coverage under every boundary replay could still select, and a
//! historical witness strictly more than one storage day old. Also the
//! storage time at which a retention age test could next change, for the
//! pass's first phase to observe (SP step 1).

use super::waits::{Facts, RETENTION_AGE};
use coven_format::value::{StorageTime, WritePositions};
use coven_merge::{Audience, WriteId};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// The earliest storage time at which one of a retention evaluation's age
/// tests could change: a waiting write's thirty-day age, or the strict
/// one-day maturity of its covering witnesses. SP step 1 observes storage
/// time once the device clock passes it; the device clock decides nothing.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Threshold(pub(crate) Option<StorageTime>);

impl Threshold {
    /// A write still waiting on a condition its thirty-day age would change.
    pub(crate) fn age(&mut self, facts: &Facts, stored_at: StorageTime) {
        if !facts.aged && facts.first_unmet().is_some() {
            self.include(stored_at.checked_add(RETENTION_AGE));
        }
    }

    /// A covered write whose latest-landing part witness has not matured.
    pub(crate) fn maturity(&mut self, landed: StorageTime) {
        self.include(landed.checked_add(Duration::from_secs(StorageTime::DAY_SECONDS)));
    }

    fn include(&mut self, time: Option<StorageTime>) {
        if let Some(time) = time {
            self.0 = Some(self.0.map_or(time, |known| known.min(time)));
        }
    }
}

/// The audiences whose snapshots must cover a write before it goes: its part
/// audiences and the store. Every member reads the store, whose snapshots
/// carry every consumed write's metadata, so no reader meets a deleted
/// number the store's newest snapshot does not reach (§15; io `Write.holders`).
pub(crate) fn holders<'a>(parts: impl IntoIterator<Item = &'a Audience>) -> BTreeSet<&'a Audience> {
    parts.into_iter().chain([&Audience::Store]).collect()
}

/// What a write's holders establish toward deleting it (§15; io `Eligible`):
/// each one's newest counting snapshot covers it under every boundary replay
/// could still select (`held`), and a checked counting snapshot of each
/// covering it landed strictly more than one storage day before `now`, a
/// witness kept past that snapshot's supersession or deletion
/// (`MatureCoverage`, `maturity_survives_new_snapshots`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Coverage {
    /// Some holder lacks current coverage, or this device never checked a
    /// counting snapshot of it covering the write: it waits for one to be
    /// known and mature.
    Missing,
    /// Covered, with every holder witnessed; the latest of their earliest
    /// witnesses landed at this time, one storage day ago or less.
    Maturing(StorageTime),
    /// Covered, with every holder's witness mature.
    Mature,
}

pub(crate) fn coverage<'a>(
    audiences: impl IntoIterator<Item = &'a Audience>,
    write: WriteId,
    held: &BTreeMap<Audience, Vec<WritePositions>>,
    witnesses: &coven_database::SnapshotWitnesses,
    now: Option<StorageTime>,
) -> Coverage {
    let mut latest: Option<StorageTime> = None;
    for audience in audiences {
        let current = held
            .get(audience)
            .is_some_and(|selections| selections.iter().all(|p| p.covers(write)));
        let Some(landed) = witnesses.landed(audience, write).filter(|_| current) else {
            return Coverage::Missing;
        };
        latest = Some(latest.map_or(landed, |known| known.max(landed)));
    }
    match latest {
        Some(landed) if now.is_none_or(|now| landed.within_day(now)) => Coverage::Maturing(landed),
        _ => Coverage::Mature,
    }
}
