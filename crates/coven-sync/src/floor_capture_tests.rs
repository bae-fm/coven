//! Which audiences a floor names and which history it deletes (§16.4, §16.6).
use crate::sequence_sync::sequence_reload::tests::stored_floor;
use crate::sequence_sync::sequence_retention::GRACE;
use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_merge::Audience;
use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};

/// The floor names the store and every introduced circle that has a snapshot
/// or a part, including one the writer isn't in and one that was deleted, and
/// waits while one with a part lacks a snapshot. A circle with neither is not
/// named (§16.4, §16.6).
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
    let idle = ben.sync.create_circle("Idle".into()).await.unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000001", gifts, "for Ben alone").await;
    ben.sync.pass().await.unwrap();
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
        "no floor while Gifts, which holds a part, has no snapshot"
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
    assert!(!named.contains(&Audience::Circle(idle)));
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

/// A join approved and never opened leaves a circle only its joiner could
/// snapshot, and a removal leaves nobody who can. The circle has no part and
/// no snapshot, so floors go on advancing without it (§16.4).
#[tokio::test]
async fn a_circle_nobody_can_snapshot_does_not_hold_floors_back() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let never_opened = ben.sync.create_circle("Never opened".into()).await.unwrap();
    ana.sync.pass().await.unwrap();
    ana.sync.administer(StoreChange::RemoveMember { member: ben.identity.member() }).await.unwrap();
    insert(ana.database(), "a", "store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert!(floor.header.first.get() > 1);
    let session = ana.sync.session.lock().await;
    let named: Vec<_> = floor
        .header
        .snapshots
        .iter()
        .map(|slot| session.history.announcements[slot].snapshot.audience.clone())
        .collect();
    assert_eq!(named, [Audience::Store]);
    assert!(session.history.introduced_audiences().contains(&Audience::Circle(never_opened)));
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A circle that gains a part is named again from then on: the floor already
/// written stays, and no later floor moves `first` past the part until its
/// snapshot exists (§16.4, §16.6).
#[tokio::test]
async fn a_circle_that_gains_a_part_holds_the_floor_until_it_has_a_snapshot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ben.sync.create_circle("Gifts".into()).await.unwrap();
    insert(ana.database(), "a", "store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let before = stored_floor(&ana).await.header.first.get();
    assert!(before > 1);

    // Ben was behind the floor, so his first pass reloads from it. His clock
    // is early, so his own snapshot of Gifts (§16.2's age rule) doesn't start.
    group.clock.set(std::time::UNIX_EPOCH);
    ben.sync.pass().await.unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000001", gifts, "for Ben alone").await;
    ben.sync.pass().await.unwrap();
    assert!(ben.database().audience_holds_parts(Audience::Circle(gifts)).await.unwrap());
    let part = ben.database().applied_slot().await.unwrap();
    insert(ana.database(), "b", "later store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + 3 * GRACE + std::time::Duration::from_secs(3));
    ana.sync.pass().await.unwrap();
    assert_eq!(
        stored_floor(&ana).await.header.first.get(),
        before,
        "Gifts holds a part, unsnapshotted"
    );
    let slot = ObjectPath::slot(group.store, part.try_into().unwrap());
    assert!(group.storage.status(&slot).await.unwrap().is_some());

    ben.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + 5 * GRACE + std::time::Duration::from_secs(5));
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert!(floor.header.first.get() > part);
    let session = ana.sync.session.lock().await;
    assert!(floor.header.snapshots.iter().any(|slot| {
        session.history.announcements[slot].snapshot.audience == Audience::Circle(gifts)
    }));
    drop(session);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A member away while a floor passes their circle, which has no part and no
/// snapshot, reloads from a floor that leaves it out and goes on writing into
/// it (§16.4, §16.5).
#[tokio::test]
async fn a_returning_member_loads_a_floor_that_does_not_name_their_bare_circle() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let mine = ben.sync.create_circle("Mine".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    insert(ana.database(), "a", "store data").await;
    insert(ana.database(), "b", "more store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert!(floor.header.first.get() > ben.database().applied_slot().await.unwrap() + 1);
    assert_eq!(floor.header.snapshots.len(), 1, "the floor leaves Mine out");
    group.clock.set(std::time::UNIX_EPOCH);
    ben.sync.pass().await.unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000002", mine, "after the reload").await;
    ben.sync.pass().await.unwrap();
    assert_eq!(
        pins(ben.database()).await,
        vec![("00000000-0000-4000-8000-000000000002".into(), "after the reload".into())]
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Carol is added to Ben's circle after a floor that leaves the circle out:
/// the circle has no snapshot to start from, and loads from the floor's
/// first kept slot instead (§16.4, §16.5).
#[tokio::test]
async fn a_circle_the_floor_leaves_out_loads_for_a_member_added_after_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    let mine = ben.sync.create_circle("Mine".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    insert(ana.database(), "a", "store data").await;
    insert(ana.database(), "b", "more store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert!(floor.header.first.get() > carol.database().applied_slot().await.unwrap() + 1);
    assert_eq!(floor.header.snapshots.len(), 1, "the floor leaves Mine out");

    // Ben's clock is early, so he starts no snapshot of Mine.
    group.clock.set(std::time::UNIX_EPOCH);
    ben.sync.pass().await.unwrap();
    ben.sync
        .administer(StoreChange::AddCircleMember { circle: mine, member: carol.identity.member() })
        .await
        .unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000002", mine, "for Carol too").await;
    ben.sync.pass().await.unwrap();
    for _ in 0..3 {
        carol.sync.pass().await.unwrap();
    }
    assert_eq!(
        pins(carol.database()).await,
        vec![("00000000-0000-4000-8000-000000000002".into(), "for Carol too".into())]
    );
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

/// A write a member queued before a floor that doesn't name their bare circle
/// reloads them is still sent afterwards (§16.5).
#[tokio::test]
async fn a_write_queued_before_a_reload_from_a_floor_without_the_circle_is_still_sent() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let mine = ben.sync.create_circle("Mine".into()).await.unwrap();
    pin(ben.database(), "00000000-0000-4000-8000-000000000003", mine, "queued first").await;
    insert(ana.database(), "a", "store data").await;
    insert(ana.database(), "b", "more store data").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    assert!(
        stored_floor(&ana).await.header.first.get()
            > ben.database().applied_slot().await.unwrap() + 1
    );
    group.clock.set(std::time::UNIX_EPOCH);
    for _ in 0..3 {
        ben.sync.pass().await.unwrap();
    }
    assert!(ben.database().audience_holds_parts(Audience::Circle(mine)).await.unwrap());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
