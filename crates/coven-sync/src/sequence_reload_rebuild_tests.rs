//! A circle's rebuild in the pass: the history it starts from and the marker it
//! restarts under (§16.6, §18.3).

use crate::sequence_sync::tests::*;
use coven_merge::Audience;

const EARLY: &str = "00000000-0000-4000-8000-000000000001";
const LATE: &str = "00000000-0000-4000-8000-000000000002";

fn floor_clock() -> std::time::SystemTime {
    std::time::UNIX_EPOCH
        + crate::sequence_sync::sequence_retention::GRACE
        + std::time::Duration::from_secs(1)
}

/// Ben's marker is queued and unsent, while retention (his own, over the
/// snapshots Ana made after the admission) publishes a floor past the slots
/// his rebuild would read from: it has no snapshot to start at before the
/// admission. The rebuild can't read those slots, so the device
/// queues a new marker, read through the slot it applied by then, and
/// rebuilds from Ana's snapshots under it.
#[tokio::test]
async fn a_rebuild_whose_history_retention_removed_restarts_under_a_new_marker() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    pin(ana.database(), EARLY, garden, "early").await;
    ana.sync.pass().await.unwrap();
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    pin(ana.database(), LATE, garden, "late").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Circle(garden)).await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    // Ben applies the admission and queues his marker.
    ben.sync.pass().await.unwrap();
    assert_eq!(ben.database().waiting_snapshot_headers().await.unwrap().len(), 1);
    let first = ben.database().rebuilds_waiting().await.unwrap()[&Audience::Circle(garden)];

    group.clock.set(floor_clock());
    {
        let mut session = ben.sync.session.lock().await;
        let mut ring = ben.sync.ring(&mut session, &ben.identity).await.unwrap();
        ben.sync.retain_sequence(&mut session, &mut ring, &ben.identity).await.unwrap();
    }
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ben).await;
    assert!(floor.header.first.get() > 1, "retention removed the slots from the start");

    // He sends the marker, then reads it and meets the floor.
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let waiting = ben.database().rebuilds_waiting().await.unwrap();
    assert_ne!(waiting[&Audience::Circle(garden)], first, "the rebuild waits on a new marker");
    for _ in 0..6 {
        ben.sync.pass().await.unwrap();
    }
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    assert_eq!(pins(ben.database()).await[0], (EARLY.to_string(), "early".to_string()));
    // Garden is held at his version, captured for its raise.
    let captured = ben.database().raise_captures().await.unwrap();
    assert!(captured.iter().any(|capture| capture.audience == Audience::Circle(garden)));
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana resets the store before Ben migrates and is added to Garden: the
/// reset's body is the store's newest snapshot at Ben's input slot, so the
/// rebuild starts the store from it, and the circle from Ana's snapshot of it.
#[tokio::test]
async fn a_rebuild_starts_the_store_from_a_reset_body() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let garden = ana.sync.create_circle("Garden".into()).await.unwrap();
    insert(ana.database(), "00000000-0000-4000-8000-0000000000a1", "store row").await;
    ana.sync.pass().await.unwrap();
    ana.sync.reset(Audience::Store).await.unwrap();
    pin(ana.database(), EARLY, garden, "early").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Circle(garden)).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ben = group.reopen(ben).await;
    ana.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let input = ben.database().waiting_snapshot_headers().await.unwrap()[0].header.read_through;
    {
        let session = ben.sync.session.lock().await;
        let starts = session.history.rebuild_starts(&Audience::Circle(garden), 0, input);
        let store = starts.store.expect("the store has a start");
        assert_eq!(store.purpose, coven_format::sequence::Purpose::Reset);
        assert!(starts.circle.is_some());
    }
    for _ in 0..6 {
        ben.sync.pass().await.unwrap();
    }
    assert!(ben.database().rebuilds_waiting().await.unwrap().is_empty());
    assert_eq!(pins(ben.database()).await, [(EARLY.to_string(), "early".to_string())]);
    assert_eq!(rows(ben.database()).await.len(), 1);
    let state = ben.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Circle(garden)), 2);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ben, on the old app, makes Garden and adds Cy; Ana's app migrates and
/// raises the store, so the floor she publishes holds a store snapshot at the
/// new version beside Garden's at the old one. Cy, on the new app, meets that
/// floor, which makes Garden readable to him at the old version: his
/// database, at the newer one, can't hold that snapshot, so Garden waits for
/// its rebuild and holds none of it (§18.3).
#[tokio::test]
async fn a_floor_reload_leaves_a_circle_below_the_version_gated_for_its_rebuild() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let cy = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &cy).await;
    cy.sync.pass().await.unwrap();
    let applied = cy.database().applied_slot().await.unwrap();
    let garden = ben.sync.create_circle("Garden".into()).await.unwrap();
    pin(ben.database(), EARLY, garden, "early").await;
    ben.sync.pass().await.unwrap();
    ben.sync.snapshot(Audience::Circle(garden)).await.unwrap();
    ben.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: cy.identity.member(),
        })
        .await
        .unwrap();
    // The store's snapshot at the old version covers all of that.
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    // Ben stays on the old app: Ana's marker would stop him.
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    let cy = group.reopen(cy).await;
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    let state = ana.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Store), 2);
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(floor_clock());
    {
        let mut session = ana.sync.session.lock().await;
        let mut ring = ana.sync.ring(&mut session, &ana.identity).await.unwrap();
        ana.sync.retain_sequence(&mut session, &mut ring, &ana.identity).await.unwrap();
    }
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    assert!(floor.header.first.get() > applied + 1);

    cy.sync.pass().await.unwrap();
    assert!(pins(cy.database()).await.is_empty(), "Garden's old rows are not in his database");
    assert!(cy
        .database()
        .rebuilds_waiting()
        .await
        .unwrap()
        .contains_key(&Audience::Circle(garden)));
    for _ in 0..6 {
        cy.sync.pass().await.unwrap();
    }
    assert!(cy.database().rebuilds_waiting().await.unwrap().is_empty());
    assert_eq!(pins(cy.database()).await, [(EARLY.to_string(), "early".to_string())]);
    for device in [&ana, &ben, &cy] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana raises the store to 2, which Cy applies on 2; Ben, on 2, then makes
/// Garden and adds Cy; Ana's next update raises the store to 4, and Cy's app
/// updates to 4 before he applies any of Garden. Garden is at 2: the only store snapshot at a version of at most 2 is
/// Ana's raise body, so the rebuild starts the store from a raise body, with
/// the store's raise to 4 left unread (§18.3).
#[tokio::test]
async fn a_rebuild_starts_the_store_from_a_raise_body() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let cy = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &cy).await;
    cy.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    let ben = group.reopen(ben).await;
    let cy = group.reopen(cy).await;
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    for _ in 0..2 {
        ben.sync.pass().await.unwrap();
        cy.sync.pass().await.unwrap();
    }
    let garden = ben.sync.create_circle("Garden".into()).await.unwrap();
    pin(ben.database(), EARLY, garden, "early").await;
    // The pin goes out before the admission, so the rebuild reads it.
    ben.sync.pass().await.unwrap();
    ben.sync
        .administer(coven_format::admin::StoreChange::AddCircleMember {
            circle: garden,
            member: cy.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    group.version.store(4, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    let cy = group.reopen(cy).await;
    cy.sync.pass().await.unwrap();
    let input = cy.database().waiting_snapshot_headers().await.unwrap()[0].header.read_through;
    {
        let session = cy.sync.session.lock().await;
        let state = &session.history.state;
        assert_eq!(state.reached(&Audience::Circle(garden)), 2);
        assert_eq!(state.reached(&Audience::Store), 4);
        let starts = session.history.rebuild_starts(&Audience::Circle(garden), 2, input);
        let store = starts.store.expect("the store has a start");
        assert_eq!(store.purpose, coven_format::sequence::Purpose::Raise(2));
    }
    for _ in 0..8 {
        cy.sync.pass().await.unwrap();
    }
    assert!(cy.database().rebuilds_waiting().await.unwrap().is_empty());
    assert_eq!(pins(cy.database()).await, [(EARLY.to_string(), "early".to_string())]);
    for device in [&ana, &ben, &cy] {
        device.sync.database.close().await.unwrap();
    }
}

/// Cy's app jumps from 1 to 4 before he applies Ana's raise of the store to
/// 2: its body is at a version his database can't hold. He rebuilds the store
/// from that body, migrates it to 4 and raises it, and Ana, who updates
/// afterwards, loads his raise and holds what he holds (§18.3).
#[tokio::test]
async fn a_device_ahead_of_the_stores_raise_rebuilds_and_raises_the_store() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let cy = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &cy).await;
    cy.sync.pass().await.unwrap();
    insert(ana.database(), "00000000-0000-4000-8000-0000000000a1", "before").await;
    ana.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    for _ in 0..3 {
        ana.sync.pass().await.unwrap();
    }
    assert_eq!(ana.sync.database.store_state().await.unwrap().reached(&Audience::Store), 2);
    insert(ana.database(), "00000000-0000-4000-8000-0000000000a2", "after").await;
    ana.sync.pass().await.unwrap();
    group.version.store(4, std::sync::atomic::Ordering::SeqCst);
    let cy = group.reopen(cy).await;
    cy.sync.pass().await.unwrap();
    assert!(cy.database().rebuilds_waiting().await.unwrap().contains_key(&Audience::Store));
    for _ in 0..6 {
        cy.sync.pass().await.unwrap();
    }
    assert!(cy.database().rebuilds_waiting().await.unwrap().is_empty());
    let state = cy.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Store), 4);
    assert_eq!(state.schema[&Audience::Store].snapshot.device, cy.sync.device);
    // Ana's second row landed after the slot his marker read through: a late
    // write, kept as a lost value where the migration drops old writes.
    assert_eq!(rows(cy.database()).await.len(), 1);
    assert_eq!(cy.database().lost_values().await.unwrap().len(), 1);
    let ana = group.reopen(ana).await;
    for _ in 0..4 {
        ana.sync.pass().await.unwrap();
    }
    assert_eq!(ana.sync.database.store_state().await.unwrap().reached(&Audience::Store), 4);
    assert_eq!(rows(ana.database()).await, rows(cy.database()).await);
    assert_eq!(
        ana.database().lost_values().await.unwrap(),
        cy.database().lost_values().await.unwrap()
    );
    for device in [&ana, &cy] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana, on the old app, publishes a floor past what Cy has applied; Cy's app,
/// which has gone past a raise of the store no one made, loads that floor.
/// Its store snapshot is at the old version, which his database can't hold, so
/// the store waits for its rebuild, which Cy raises himself instead of waiting
/// for an older app to update (§18.3).
#[tokio::test]
async fn a_floor_whose_store_snapshot_is_below_the_version_is_rebuilt() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let cy = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &cy).await;
    let applied = cy.database().applied_slot().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let cy = group.reopen(cy).await;
    insert(ana.database(), "00000000-0000-4000-8000-0000000000a1", "before").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    insert(ana.database(), "00000000-0000-4000-8000-0000000000a2", "after").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(floor_clock());
    {
        let mut session = ana.sync.session.lock().await;
        let mut ring = ana.sync.ring(&mut session, &ana.identity).await.unwrap();
        ana.sync.retain_sequence(&mut session, &mut ring, &ana.identity).await.unwrap();
    }
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    assert!(floor.header.first.get() > applied + 1);

    cy.sync.pass().await.unwrap();
    assert!(cy.database().rebuilds_waiting().await.unwrap().contains_key(&Audience::Store));
    for _ in 0..6 {
        cy.sync.pass().await.unwrap();
    }
    assert!(cy.database().rebuilds_waiting().await.unwrap().is_empty());
    let state = cy.sync.database.store_state().await.unwrap();
    assert_eq!(state.reached(&Audience::Store), 2);
    assert_eq!(state.schema[&Audience::Store].snapshot.device, cy.sync.device);
    assert_eq!(rows(cy.database()).await, rows(ana.database()).await);
    for device in [&ana, &cy] {
        device.sync.database.close().await.unwrap();
    }
}
