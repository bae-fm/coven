use super::*;
use crate::sequence_sync::tests::*;
use std::time::UNIX_EPOCH;

/// A snapshot's signed prefix states the coverage and purpose its announcement
/// names; one that states others is refused as the wrong identity before its
/// body loads (§16.2, format.md D9).
#[tokio::test]
async fn a_snapshot_whose_prefix_differs_from_its_announcement_is_refused() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let slot = ana.sync.snapshot(Audience::Store).await.unwrap();
    let announced = ana.sync.session.lock().await.history.announcements[&slot].clone();
    ana.sync.snapshot_prefix(&announced).await.unwrap();

    let altered = [
        SnapshotAnnouncement { covers: announced.covers - 1, ..announced.clone() },
        SnapshotAnnouncement { purpose: Purpose::Reset, ..announced.clone() },
    ];
    for altered in altered {
        let result = ana.sync.snapshot_prefix(&altered).await.map(drop);
        assert!(
            matches!(
                &result,
                Err(SyncError::Damaged(damaged))
                    if matches!(damaged.failure, Refusal::WrongIdentity { .. })
            ),
            "{result:?}"
        );
    }
    ana.sync.database.close().await.unwrap();
}

/// A floor names the store's newest snapshot first and then each circle's in
/// audience order, and every snapshot it names covers at least the slot before
/// its first kept one (format.md D8).
#[tokio::test]
async fn a_floor_naming_snapshots_out_of_order_or_below_its_first_kept_slot_is_damaged() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    insert(&ana.sync.database, "a", "in the store").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    group.clock.set(
        UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    assert_eq!(floor.announcements.len(), 2, "the store's snapshot and Gifts'");

    let history = ana.sync.session.lock().await.history.clone();
    let authenticate = |floor: FloorObject| {
        let (ana, history) = (&ana, &history);
        async move {
            let mut ring = ana.sync.keys.read().unwrap().unwrap();
            ana.sync.authenticate_floor(&floor, history, &mut ring, &ana.identity).await
        }
    };
    authenticate(floor.clone()).await.unwrap();

    let mut reordered = floor.clone();
    reordered.announcements.reverse();
    let mut below = floor;
    below.header.first = Slot(below.header.at + 2);
    for damaged in [reordered, below] {
        let result = authenticate(damaged).await;
        assert!(
            matches!(
                &result,
                Err(SyncError::Damaged(damaged))
                    if matches!(damaged.failure, Refusal::WrongIdentity { .. })
            ),
            "{result:?}"
        );
    }
    ana.sync.database.close().await.unwrap();
}
