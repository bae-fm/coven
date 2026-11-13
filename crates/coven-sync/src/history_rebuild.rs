//! What a rebuild of a circle below the store's version reads from storage
//! (§16.6, §18.3; `Retention.RebuildKept`, `CircleRaise.rebuilt_body_from_kept`).

use crate::history::History;
use coven_format::{
    admin::SnapshotId,
    sequence::{Purpose, SnapshotAnnouncement},
};
use coven_merge::{Audience, Slot};
use std::collections::BTreeSet;

/// The store history one circle's rebuild starts from and replays.
pub(crate) struct RebuildRead {
    /// The store's newest ordinary announcement at a version of at most the
    /// circle's: its slot and coverage. None when history holds none, when
    /// the rebuild starts empty and `first` stays at the first slot.
    pub(crate) snapshot: Option<(Slot, u64)>,
    /// The bodies of the store's raises and resets after the snapshot's
    /// coverage, through the slot before the store's raise past the
    /// circle's version.
    pub(crate) replay: Vec<SnapshotId>,
}

impl RebuildRead {
    /// The first kept slot may be no later than this: the slot after the
    /// snapshot's coverage, or the first slot with none.
    pub(crate) fn cap(&self) -> Slot {
        match self.snapshot {
            Some((_, covers)) => Slot::new(covers + 1).expect("a slot follows a coverage"),
            None => Slot::FIRST,
        }
    }
}

/// The snapshots one circle's rebuild loads, and how far it reads the store.
pub(crate) struct RebuildStarts {
    /// The circle's newest snapshot covering at most the input slot; None
    /// when the circle starts empty.
    pub(crate) circle: Option<SnapshotAnnouncement>,
    /// The store's newest snapshot, ordinary or the replayed body of a reset,
    /// at a version of at most the circle's, covering at most `store_through`;
    /// None when the store starts empty.
    pub(crate) store: Option<SnapshotAnnouncement>,
    /// The last slot whose store parts the rebuild reads, if it reads to
    /// `input`: the input slot, or the slot before the store's raise past the
    /// circle's version when that is earlier (§16.6, §18.3).
    pub(crate) store_through: u64,
    /// The slot before the store's raise past the circle's version, if
    /// history holds one: the last slot whose store parts any rebuild of the
    /// circle reads.
    pub(crate) store_limit: u64,
}

impl RebuildStarts {
    /// The last slot whose store parts the rebuild reads when it has read
    /// the slots through `target`.
    pub(crate) fn store_through_at(&self, target: u64) -> u64 {
        self.store_limit.min(target)
    }
}

impl History {
    /// What the rebuild of `circle`, at `version`, loads to reach `input`, the
    /// slot its migration marker read through.
    pub(crate) fn rebuild_starts(
        &self,
        circle: coven_foundation::id_source::CircleId,
        version: u32,
        input: u64,
    ) -> RebuildStarts {
        let audience = Audience::Circle(circle);
        let store_limit = self
            .boundaries
            .iter()
            .filter_map(|coverage| match &coverage.boundary {
                coven_database::Boundary::SchemaChange(raise)
                    if raise.snapshot.audience == Audience::Store && raise.number > version =>
                {
                    Some(raise.entry.get() - 1)
                }
                _ => None,
            })
            .min()
            .map_or(u64::MAX, |before| before);
        let store_through = store_limit.min(input);
        let newest = |audience: &Audience, through: u64| {
            self.announcements
                .iter()
                .filter(|(_, announcement)| {
                    announcement.snapshot.audience == *audience
                        && announcement.covers <= through
                        && announcement.breaking_version <= version
                        && self.usable_at(announcement, through)
                })
                .max_by_key(|(slot, announcement)| (announcement.covers, **slot))
                .map(|(_, announcement)| announcement.clone())
        };
        RebuildStarts {
            circle: newest(&audience, input),
            store: newest(&Audience::Store, store_through),
            store_through,
            store_limit,
        }
    }

    /// Each circle that isn't deleted and is below the store's version, with
    /// what a member who starts reading it rebuilds it from.
    pub(crate) fn rebuild_reads(&self) -> Vec<RebuildRead> {
        let store = self.state.reached(&Audience::Store);
        let mut reads = Vec::new();
        for circle in self.state.circles.keys().copied() {
            let audience = Audience::Circle(circle);
            let version = self.state.reached(&audience);
            if self.state.circle_deletions.contains_key(&circle) || version >= store {
                continue;
            }
            // The slot of the store's raise past the circle's version ends
            // the replay before it.
            let limit = self
                .boundaries
                .iter()
                .filter_map(|coverage| match &coverage.boundary {
                    coven_database::Boundary::SchemaChange(raise)
                        if raise.snapshot.audience == Audience::Store && raise.number > version =>
                    {
                        Some(raise.entry.get())
                    }
                    _ => None,
                })
                .min()
                .map_or(u64::MAX, |entry| entry - 1);
            let snapshot = self
                .announcements
                .iter()
                .rev()
                .filter(|(_, announcement)| {
                    announcement.snapshot.audience == Audience::Store
                        && announcement.purpose == Purpose::Ordinary
                        && announcement.breaking_version <= version
                })
                .map(|(slot, announcement)| (*slot, announcement.covers))
                .next();
            let after = snapshot.map_or(0, |(_, covers)| covers);
            let replay = self
                .boundaries
                .iter()
                .filter(|coverage| {
                    let entry = coverage.boundary.entry().get();
                    coverage.boundary.snapshot().audience == Audience::Store
                        && entry > after
                        && entry <= limit
                })
                .map(|coverage| coverage.boundary.snapshot().clone())
                .collect();
            reads.push(RebuildRead { snapshot, replay });
        }
        reads
    }

    /// The announcements rebuilds start from, which are never forgotten.
    pub(crate) fn rebuild_announcements(&self) -> BTreeSet<Slot> {
        self.rebuild_reads().into_iter().filter_map(|read| read.snapshot.map(|s| s.0)).collect()
    }
}

#[cfg(test)]
#[path = "history_rebuild_tests.rs"]
mod tests;
