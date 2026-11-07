//! Which audiences a floor names and which history it deletes (§16.4, §16.6).
use crate::sequence_sync::sequence_reload::tests::stored_floor;
use crate::sequence_sync::sequence_retention::GRACE;
use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_merge::Audience;
use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};

/// The floor names the store and every introduced circle, including one the
/// writer isn't in and one that was deleted, and waits while any lacks a
/// snapshot (§16.4, §16.6).
#[tokio::test]
async fn a_floor_names_deleted_circles_and_ones_its_writer_cannot_open() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    // Gifts and Old are Ben's: Ana is in neither, so she cannot open them.
    let gifts = ben.sync.create_circle("Gifts".into()).await.unwrap();
    let old = ben.sync.create_circle("Old".into()).await.unwrap();
    ben.sync.snapshot(Audience::Circle(old)).await.unwrap();
    ben.sync.administer(StoreChange::DeleteCircle { circle: old }).await.unwrap();
    insert(ana.database(), "a", "store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor_path = ObjectPath::floor(group.store);
    assert!(
        group.storage.status(&floor_path).await.unwrap().is_none(),
        "no floor while Gifts has no snapshot"
    );

    ben.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + 2 * GRACE + std::time::Duration::from_secs(2));
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    let session = ana.sync.session.lock().await;
    let named: std::collections::BTreeSet<_> = floor
        .header
        .snapshots
        .iter()
        .map(|slot| session.history.announcements[slot].snapshot.audience.clone())
        .collect();
    assert_eq!(
        named,
        [Audience::Store, Audience::Circle(gifts), Audience::Circle(old)].into_iter().collect()
    );
    assert_eq!(floor.header.snapshots.len(), 3);
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A slot deletion the provider refuses stays journaled and reported, and
/// the next floor is written and its deletions run without waiting for it
/// (§16.6).
#[tokio::test]
async fn a_later_floor_advances_while_a_slot_deletion_is_still_failing() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let grace = |periods: u32| {
        std::time::UNIX_EPOCH + GRACE * periods + std::time::Duration::from_secs(u64::from(periods))
    };
    insert(ana.database(), "first", "under the first floor").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    let refused = ObjectPath::slot(group.store, 1.try_into().unwrap());
    ana.storage
        .set_faults(Faults {
            delete_failures: [(refused.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    group.clock.set(grace(1));
    ana.sync.pass().await.unwrap();
    let first_floor = stored_floor(&ana).await.header.first.get();
    assert!(first_floor > 1);
    let failing = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "sequence-retention")
        .expect("the refused deletion stays journaled")
        .id;
    let pending =
        ana.database().operation_pending::<crate::PendingRecord>(failing).await.unwrap().unwrap();
    assert!(matches!(
        pending.reason,
        crate::PendingReason::Storage { failure: StorageFailure::PermissionDenied, .. }
    ));
    assert!(ana.storage.status(&refused).await.unwrap().is_some());

    insert(ana.database(), "second", "under the second floor").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(grace(3));
    ana.sync.pass().await.unwrap();
    let second_floor = stored_floor(&ana).await.header.first.get();
    assert!(second_floor > first_floor, "{second_floor} {first_floor}");
    // The new floor's own deletions ran past the refused one.
    let between = ObjectPath::slot(group.store, first_floor.try_into().unwrap());
    assert!(ana.storage.status(&between).await.unwrap().is_none());
    assert!(ana.storage.status(&refused).await.unwrap().is_some());
    assert!(ana.database().operation(failing).await.unwrap().is_some());
    ana.database().close().await.unwrap();
}

/// Gifts' only member snapshots it and goes away. Once the snapshot passes
/// grace, Ana's floor deletes the slots it covers without waiting for him,
/// and keeps the slots above it (§16.6).
#[tokio::test]
async fn history_a_circle_already_covers_is_deleted_while_its_members_are_away() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ben.sync.create_circle("Gifts".into()).await.unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000001", gifts, "for Ben alone").await;
    ben.sync.pass().await.unwrap();
    let covered = ben.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    let gifts_covers = ben.sync.session.lock().await.history.announcements[&covered].covers;
    // Ben is away from here on: only Ana passes.
    insert(ana.database(), "a", "written after Gifts' snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert_eq!(floor.header.first.get(), gifts_covers + 1);
    let slot = |number: u64| ObjectPath::slot(group.store, number.try_into().unwrap());
    for deleted in 1..=gifts_covers {
        assert!(group.storage.status(&slot(deleted)).await.unwrap().is_none(), "{deleted}");
    }
    assert!(group.storage.status(&slot(gifts_covers + 1)).await.unwrap().is_some());
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}
