use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_merge::Audience;
use coven_storage::{test_utils::Faults, ObjectPath, Storage, StorageFailure};

#[tokio::test]
async fn reset_loads_the_announced_body_and_retains_later_eligible_writes() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before reset").await;
    ana.sync.pass().await.unwrap();
    ana.sync.reset(coven_merge::Audience::Store).await.unwrap();
    insert(&ana.sync.database, "b", "after reset").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    assert_eq!(rows(&ben.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn resetting_a_circle_preserves_other_audiences_without_requiring_their_snapshots() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "store-row", "kept").await;
    ana.sync.pass().await.unwrap();
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .reset(coven_merge::Audience::Circle(circle))
        .await
        .unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        [("store-row".into(), "kept".into())]
    );
    assert!(ana
        .sync
        .database
        .reloading_audiences()
        .await
        .unwrap()
        .is_empty());
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn loading_a_snapshot_checks_its_announced_digest_before_its_signature() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let slot = ana
        .sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let history = ben.sync.session.lock().await.history.clone();
    let announcement = &history.announcements[&slot];
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    let size = group.storage.status(&path).await.unwrap().unwrap().size;
    group
        .storage
        .corrupt_byte(&path, size as usize - 1)
        .await
        .unwrap();
    let snapshot = crate::snapshot_object::SnapshotObject::read_prefix(
        &ben.sync.database,
        ben.sync.storage.as_ref(),
        &path,
    )
    .await
    .unwrap();
    let ring = ben.sync.keys.read().unwrap().unwrap();
    snapshot
        .plaintext(ben.sync.storage.as_ref(), &ring)
        .await
        .unwrap();
    let result = snapshot
        .authenticate(&history, &announcement.snapshot, &announcement.digest)
        .await;
    assert!(
        matches!(
            result,
            Err(SyncError::Damaged(DamagedObject {
                failure: Refusal::WrongIdentity { .. },
                ..
            }))
        ),
        "{result:?}"
    );
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn an_app_reload_rebuilds_from_the_newest_snapshot_and_keeps_waiting_writes() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "in the snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    insert(&ana.sync.database, "b", "after the snapshot").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    insert(&ben.sync.database, "c", "waiting on Ben").await;
    let through = ben.sync.database.applied_slot().await.unwrap();
    ben.sync.reload_from_snapshot().await.unwrap();
    assert_eq!(ben.sync.database.applied_slot().await.unwrap(), through);
    assert_eq!(rows(&ben.sync.database).await.len(), 3);
    ben.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// The floor in storage, as `device` reads it.
async fn stored_floor(device: &Device) -> crate::floor_object::FloorObject {
    let mut ring = device.sync.keys.read().unwrap().unwrap();
    crate::floor_object::FloorObject::read(
        &device.sync.database,
        device.sync.storage.as_ref(),
        &ObjectPath::floor(device.sync.store()),
        None,
        &mut ring,
        &device.identity,
    )
    .await
    .unwrap()
    .unwrap()
}

/// Ana prepares a raise or reset of the store covering `c`; Ben then creates
/// his circle and announces its snapshot, and Ana's activation lands at `r`.
/// Ana's ordinary store snapshot covers `r`, and retention writes a floor
/// whose first kept slot follows Ben's circle snapshot, deleting `c + 1`. Ben,
/// applied through `r - 1`, would need those slots to apply the activation
/// from its body, so he loads the store from the floor's snapshot (§15).
async fn activation_whose_suffix_is_below_the_floor(raise: bool) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the preparation").await;
    ana.sync.pass().await.unwrap();
    let (ana, ben) = if raise {
        group.version.store(2, std::sync::atomic::Ordering::SeqCst);
        let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
        for device in [&ana, &ben, &ana] {
            device.sync.pass().await.unwrap();
        }
        (ana, ben)
    } else {
        (ana, ben)
    };
    let prepared = ana.sync.database.applied_slot().await.unwrap();
    let activation = ObjectPath::slot(group.store, (prepared + 2).try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(activation, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    let result = if raise {
        ana.sync.raise_schema(Audience::Store, 2).await
    } else {
        ana.sync.reset(Audience::Store).await
    };
    assert!(result.is_err(), "{result:?}");
    group.storage.set_faults(Faults::none()).await;
    if !raise {
        // Writes after the reset's coverage, which the reset discards.
        insert(&ben.sync.database, "b", "after the preparation").await;
        ben.sync.pass().await.unwrap();
    }
    let circle = ben.sync.create_circle("Ben's".into()).await.unwrap();
    ben.sync.snapshot(Audience::Circle(circle)).await.unwrap();
    let before = ben.sync.database.applied_slot().await.unwrap();
    let operation = ana.sync.database.operations().await.unwrap()[0].id;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    let activated = ana.sync.database.applied_slot().await.unwrap();
    assert_eq!(activated, before + 1);
    ana.sync.snapshot(Audience::Store).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert_eq!(floor.header.first.0, before);
    assert!(floor.header.first.0 > prepared + 1);
    assert!(group
        .storage
        .status(&ObjectPath::slot(
            group.store,
            (prepared + 1).try_into().unwrap()
        ))
        .await
        .unwrap()
        .is_none());
    ben.sync.pass().await.unwrap();
    assert!(ben.sync.database.applied_slot().await.unwrap() >= activated);
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        ben.sync.database.applied_slot().await.unwrap()
    );
    assert_eq!(
        rows(&ana.sync.database).await,
        [("a".into(), "before the preparation".into())]
    );
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    assert!(ben
        .sync
        .database
        .reloading_audiences()
        .await
        .unwrap()
        .is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_raise_whose_replay_is_below_the_floor_loads_the_floor_snapshot() {
    activation_whose_suffix_is_below_the_floor(true).await;
}

#[tokio::test]
async fn a_reset_whose_suffix_is_below_the_floor_reads_no_deleted_slot() {
    activation_whose_suffix_is_below_the_floor(false).await;
}

/// Ana adds Ben to Gifts after retention deleted its earlier history and
/// before she announces Gifts' first snapshot. The floor that follows names
/// that later snapshot; Ben, below the addition, starts Gifts there and reaches
/// the common target at its coverage (§15 "For a changed followed set").
#[tokio::test]
async fn a_newly_readable_circle_starts_from_the_floor_snapshot_announced_after_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    pin(&ana.sync.database, PIN_1, gifts, "before Ben").await;
    insert(&ana.sync.database, "a", "store").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    let before = ben.sync.database.applied_slot().await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember {
            circle: gifts,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    let added = ana.sync.database.applied_slot().await.unwrap();
    pin(&ana.sync.database, PIN_2, gifts, "after Ben").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert_eq!(floor.header.first.0, before);
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 1.try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    ben.sync.pass().await.unwrap();
    assert!(ben.sync.database.applied_slot().await.unwrap() > added);
    assert_eq!(
        pins(&ben.sync.database).await,
        [
            (PIN_1.into(), "before Ben".into()),
            (PIN_2.into(), "after Ben".into())
        ]
    );
    assert_eq!(rows(&ben.sync.database).await.len(), 1);
    assert!(ben
        .sync
        .database
        .reloading_audiences()
        .await
        .unwrap()
        .is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Without a floor no slot is deleted, so a circle that never had a snapshot
/// loads from slot zero when it becomes readable.
#[tokio::test]
async fn a_newly_readable_circle_without_snapshot_or_floor_loads_every_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    pin(&ana.sync.database, PIN_1, gifts, "before Ben").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    assert!(pins(&ben.sync.database).await.is_empty());
    ana.sync
        .administer(StoreChange::AddCircleMember {
            circle: gifts,
            member: ben.identity.member(),
        })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(
        pins(&ben.sync.database).await,
        [(PIN_1.into(), "before Ben".into())]
    );
    assert!(ben
        .sync
        .database
        .reloading_audiences()
        .await
        .unwrap()
        .is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

const PIN_1: &str = "00000000-0000-4000-8000-000000000001";
const PIN_2: &str = "00000000-0000-4000-8000-000000000002";

/// Ben's ordinary store snapshot, announced after Ana's reset preparation,
/// covers more than it; activating the stale preparation would be refused,
/// so Ana's operation recaptures and activates a body covering Ben's (§15).
#[tokio::test]
async fn a_preparation_covered_by_a_later_announcement_is_recaptured() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    ben.sync.pass().await.unwrap();
    let prepared = ana.sync.database.applied_slot().await.unwrap();
    let activation = ObjectPath::slot(group.store, (prepared + 2).try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(activation, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    assert!(ana.sync.reset(Audience::Store).await.is_err());
    group.storage.set_faults(Faults::none()).await;
    let ordinary = ben.sync.snapshot(Audience::Store).await.unwrap();
    let operation = ana.sync.database.operations().await.unwrap()[0].id;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    let history = ana.sync.session.lock().await.history.clone();
    let covered = history.announcements[&ordinary].covers;
    let boundary = history.boundaries.last().unwrap();
    let activated = history
        .announcements
        .values()
        .find(|announcement| &announcement.snapshot == boundary.boundary.snapshot())
        .unwrap();
    assert!(activated.covers >= covered);
    assert!(boundary.boundary.entry().0 > ordinary.0);
    ben.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.database.applied_slot().await.unwrap(),
        ben.sync.database.applied_slot().await.unwrap()
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A breaking migration journals its raise with the schema; passes publish
/// it for every audience it captured, and a member that migrated later
/// takes the published raise instead of raising again (§17.1).
#[tokio::test]
async fn a_breaking_migration_raises_its_captured_audiences_through_passes() {
    let group = Group::new();
    group
        .raises
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "before the raise").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .any(|record| record.kind == "schema-raise"));
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    for device in [&ana, &ben] {
        let state = device.sync.database.store_state().await.unwrap();
        assert_eq!(state.schema[&Audience::Store].number, 2);
        assert!(device
            .sync
            .database
            .raise_captures()
            .await
            .unwrap()
            .is_empty());
        assert!(device
            .sync
            .database
            .operations()
            .await
            .unwrap()
            .iter()
            .all(|record| record.kind != "schema-raise"));
    }
    assert_eq!(
        rows(&ana.sync.database).await,
        rows(&ben.sync.database).await
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
