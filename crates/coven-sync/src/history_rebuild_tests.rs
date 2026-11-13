use super::*;
use coven_database::{Boundary, BoundaryCoverage, StoreCircle, StoreVersion};
use coven_format::sequence::SnapshotAnnouncement;
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::slot;

fn announced(audience: Audience, number: u64, covers: u64, version: u32) -> SnapshotAnnouncement {
    SnapshotAnnouncement {
        snapshot: SnapshotId { audience, device: DeviceId(1), number },
        covers,
        breaking_version: version,
        purpose: Purpose::Ordinary,
        digest: [number as u8; 32],
    }
}

fn raise(number: u32, snapshot: &SnapshotAnnouncement, entry: u64) -> BoundaryCoverage {
    BoundaryCoverage {
        boundary: Boundary::SchemaChange(StoreVersion {
            number,
            snapshot: snapshot.snapshot.clone(),
            entry: slot(entry),
        }),
        included: snapshot.covers,
    }
}

/// Garden is at version 1 and the store was raised to 2 at slot 300; the
/// store's version 1 snapshots were announced at slots 241 and 200.
fn garden() -> (History, CircleId) {
    let circle = CircleId(uuid::Uuid::from_u128(7));
    let mut history = History::default();
    history.state.circles.insert(
        circle,
        StoreCircle { name: "Garden".into(), members: Default::default(), base: 1 },
    );
    let old = announced(Audience::Store, 1, 199, 1);
    let newest = announced(Audience::Store, 2, 240, 1);
    let raised = announced(Audience::Store, 3, 299, 2);
    history.announcements.insert(slot(200), old);
    history.announcements.insert(slot(241), newest);
    history.announcements.insert(slot(301), raised.clone());
    let coverage = raise(2, &raised, 300);
    history.state.schema.insert(
        Audience::Store,
        match &coverage.boundary {
            Boundary::SchemaChange(version) => version.clone(),
            _ => unreachable!(),
        },
    );
    history.boundaries.push(coverage);
    (history, circle)
}

#[test]
fn a_circle_below_the_store_starts_from_the_stores_newest_snapshot_at_its_version() {
    let (history, _) = garden();
    let reads = history.rebuild_reads();
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].snapshot, Some((slot(241), 240)));
    assert_eq!(reads[0].cap(), slot(241));
    assert!(reads[0].replay.is_empty());
    assert_eq!(history.rebuild_announcements(), [slot(241)].into());
}

#[test]
fn the_raises_and_resets_after_the_snapshot_are_replayed_but_not_the_raise_past_the_circle() {
    let (mut history, _) = garden();
    let reset = announced(Audience::Store, 4, 244, 1);
    history.boundaries.push(BoundaryCoverage {
        boundary: Boundary::Reset { entry: slot(250), snapshot: reset.snapshot.clone() },
        included: 244,
    });
    let before = announced(Audience::Store, 5, 100, 1);
    history.boundaries.push(BoundaryCoverage {
        boundary: Boundary::Reset { entry: slot(150), snapshot: before.snapshot.clone() },
        included: 100,
    });
    let reads = history.rebuild_reads();
    assert_eq!(reads[0].replay, [reset.snapshot]);
}

#[test]
fn a_circle_with_no_store_snapshot_at_its_version_holds_first_at_the_first_slot() {
    let (mut history, _) = garden();
    history.announcements.retain(|_, announcement| announcement.breaking_version > 1);
    let reads = history.rebuild_reads();
    assert_eq!(reads[0].snapshot, None);
    assert_eq!(reads[0].cap(), Slot::FIRST);
}

#[test]
fn a_raised_or_deleted_circle_needs_no_rebuild() {
    let (mut history, circle) = garden();
    let mut raised = history.clone();
    let garden = announced(Audience::Circle(circle), 9, 305, 2);
    raised.state.schema.insert(
        Audience::Circle(circle),
        StoreVersion { number: 2, snapshot: garden.snapshot, entry: slot(310) },
    );
    assert!(raised.rebuild_reads().is_empty());
    history.state.circle_deletions.insert(circle, slot(320));
    assert!(history.rebuild_reads().is_empty());
}

#[test]
fn the_announcement_a_rebuild_starts_from_is_never_forgotten() {
    let (history, _) = garden();
    // The version 1 announcement at 200 is older than 241 and covers less;
    // 241 is the newest ordinary one at that version and is newer than it.
    let forgettable = history.forgettable(slot(300), &Default::default());
    assert!(forgettable.contains(&slot(200)));
    assert!(!forgettable.contains(&slot(241)));
}
