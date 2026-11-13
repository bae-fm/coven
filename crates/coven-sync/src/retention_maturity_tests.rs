use crate::sequence_sync::tests::*;
use coven_foundation::id_source::IdSource;
use coven_merge::slot;
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage};
use std::{sync::Arc, time::UNIX_EPOCH};

/// Ben, whose clock runs a grace period behind the provider's, announces a
/// snapshot and goes quiet. Ana announces two more, and once the newer has
/// passed grace her floor's first kept slot passes all of Ben's slots and
/// both his announcement's and her older one's. Ben reloads from her floor;
/// by his clock her newer snapshot hasn't passed grace. His retention never
/// reads the slot of her older announcement, which retention may have
/// deleted: it forgets both announcements below `first`, and his listing
/// deletes his old body at once, since no reader loads it again (§16.6,
/// §16.8, `Retention.kept_candidates_same_first`,
/// `Announcements.listing_finds_superseded`).
#[tokio::test]
async fn retention_never_reads_an_announcement_slot_below_the_floor() {
    let group = Group::new();
    let ana = group.device(1).await;
    let clock = Arc::new(coven_foundation::clock::FixedClock::new(UNIX_EPOCH));
    let identity = coven_crypto::MemberIdentity::new(
        coven_foundation::id_source::MemberId(group.ids.new_id()),
        coven_format::test_utils::keys_from_seed(2),
    );
    let (temp, _) = group.directory();
    let ben = group
        .open_with_clock(
            temp,
            identity,
            Some(group.ring.clone()),
            group.storage.for_device(),
            clock.clone(),
        )
        .await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    insert(&ben.sync.database, "ben", "before his snapshot").await;
    ben.sync.pass().await.unwrap();
    ben.sync.snapshot(Audience::Store).await.unwrap();
    let his = ben.sync.database.applied_slot().await.unwrap();
    let id = ben.sync.session.lock().await.history.announcements[&slot(his)].snapshot.clone();
    let body = crate::snapshot_object::path(group.store, &id).unwrap();

    ana.sync.pass().await.unwrap();
    insert(&ana.sync.database, "a", "before her older snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    let older = ana.sync.database.applied_slot().await.unwrap();
    insert(&ana.sync.database, "b", "before her newer snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(UNIX_EPOCH + super::GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    assert!(older < floor.header.first.get(), "{:?}", floor.header);
    for slot in [his, older] {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        assert!(group.storage.status(&path).await.unwrap().is_none(), "slot {slot}");
    }

    let mut ring = ben.sync.keys.read().unwrap().unwrap();
    let mut session = ben.sync.session.lock().await;
    ben.sync.read_forward(&mut session, &mut ring, &ben.identity).await.unwrap();
    assert!(session.history.announcements.contains_key(&slot(older)));
    ben.sync.retain_snapshots(&mut session, &mut ring, &ben.identity).await.unwrap();
    for forgotten in [his, older] {
        assert!(!session.history.announcements.contains_key(&slot(forgotten)), "{forgotten}");
    }
    drop(session);
    assert!(
        group.storage.status(&body).await.unwrap().is_none(),
        "his body below `first` goes though no replacement has passed grace by his clock"
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben, on the first app version, holds Garden while Ana's migration raises
/// the store. A member who starts reading Garden rebuilds it from the store's
/// snapshot at Garden's version, so `first` stays at or before the slot after
/// that snapshot's coverage while Garden is below the store, and moves past it
/// once Garden is raised (§16.6).
#[tokio::test]
async fn a_circle_below_the_store_keeps_the_stores_snapshot_at_its_version() {
    use coven_database::StoreVersion;
    use coven_merge::Audience;
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    // Ben, who stays on the old app, makes Garden; Ana is not in it.
    let garden = ben.sync.create_circle("Garden".into()).await.unwrap();
    ana.sync.pass().await.unwrap();
    let old = ana.sync.snapshot(Audience::Store).await.unwrap();
    let covers = ana.sync.session.lock().await.history.announcements[&old].covers;
    // Garden's own snapshot covers more than the old store snapshot, so
    // without the rebuild's hold `first` would pass the store snapshot.
    ben.sync.pass().await.unwrap();
    ben.sync.snapshot(Audience::Circle(garden)).await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(ana.sync.database.store_state().await.unwrap().reached(&Audience::Store), 2);
    assert_eq!(
        ana.sync.database.store_state().await.unwrap().reached(&Audience::Circle(garden)),
        0
    );
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE * 2
            + std::time::Duration::from_secs(1),
    );
    let first = async |ana: &Device| {
        let session = ana.sync.session.lock().await;
        match ana.sync.propose_floor(&session, &group.ring).await.unwrap() {
            Some(crate::sequence_sync::sequence_retention::Retention::Capture(capture)) => {
                capture.first.get()
            }
            other => panic!("expected a floor, found {}", other.is_some()),
        }
    };
    assert!(first(&ana).await <= covers + 1, "first keeps the snapshot Garden rebuilds from");
    // Garden raised: nothing rebuilds from the old snapshot.
    {
        let mut session = ana.sync.session.lock().await;
        let announced = session.history.announcements.values().last().unwrap().snapshot.clone();
        let entry = coven_merge::slot(session.through);
        session.history.state.schema.insert(
            Audience::Circle(garden),
            StoreVersion {
                number: 2,
                snapshot: coven_format::admin::SnapshotId {
                    audience: Audience::Circle(garden),
                    ..announced
                },
                entry,
            },
        );
    }
    assert!(first(&ana).await > covers + 1, "first moves on once Garden is raised");
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// The same scenario, run through the deletion walk. Ana's own old store
/// snapshot and her store reset are superseded by a newer mature snapshot, and
/// the floor passes both, so the walk would delete their bodies; Garden is
/// rebuilt from that snapshot with that reset replayed over it, so both
/// bodies survive (§16.6). Cy, added to Garden before the walk and applying
/// the admission only after it, rebuilds Garden from those bodies, and holds
/// what Ben, who held it and migrated in place, holds.
#[tokio::test]
async fn the_bodies_a_rebuild_reads_survive_the_deletion_walk() {
    use coven_merge::Audience;
    const EARLY: &str = "00000000-0000-4000-8000-000000000001";
    const LATE: &str = "00000000-0000-4000-8000-000000000002";
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let cy = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &cy).await;
    cy.sync.pass().await.unwrap();
    let garden = ben.sync.create_circle("Garden".into()).await.unwrap();
    pin(ben.database(), EARLY, garden, "early").await;
    ana.sync.pass().await.unwrap();
    let old = ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.reset(Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.snapshot(Audience::Circle(garden)).await.unwrap();
    pin(ben.database(), LATE, garden, "late").await;
    ben.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: cy.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let ben_keys = ben.sync.keys.read().unwrap().unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    let cy = group.reopen(cy).await;
    // Ben migrates in place and never passes: his raise has not landed.
    let ben = group.reopen(ben).await;
    ben.sync.keys.persist(&ben_keys).unwrap();
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    let (old_body, reset_body) = {
        let session = ana.sync.session.lock().await;
        let reads = session.history.rebuild_reads();
        assert_eq!(reads.len(), 1, "Garden is below the store");
        let (slot, _) = reads[0].snapshot.expect("the old store snapshot is the start");
        assert_eq!(slot, old);
        assert_eq!(reads[0].replay.len(), 1, "the reset is replayed over it");
        let path = |id: &coven_format::admin::SnapshotId| {
            crate::snapshot_object::path(group.store, id).unwrap()
        };
        (path(&session.history.announcements[&slot].snapshot), path(&reads[0].replay[0]))
    };
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE * 2
            + std::time::Duration::from_secs(1),
    );
    for _ in 0..4 {
        ana.sync.pass().await.unwrap();
    }
    assert!(group.storage.status(&old_body).await.unwrap().is_some(), "the start survives");
    assert!(group.storage.status(&reset_body).await.unwrap().is_some(), "the replay survives");

    for _ in 0..8 {
        cy.sync.pass().await.unwrap();
    }
    assert!(cy.database().rebuilds_waiting().await.unwrap().is_empty());
    let held = pins(ben.database()).await;
    assert_eq!(held.len(), 2, "Ben holds both pins");
    assert_eq!(pins(cy.database()).await, held);
    assert_eq!(
        cy.database().lost_values().await.unwrap(),
        ben.database().lost_values().await.unwrap()
    );
    for device in [&ana, &ben, &cy] {
        device.database().close().await.unwrap();
    }
}
