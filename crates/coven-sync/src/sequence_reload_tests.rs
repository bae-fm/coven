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
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
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
    ana.sync.reset(coven_merge::Audience::Circle(circle)).await.unwrap();
    assert_eq!(rows(&ana.sync.database).await, [("store-row".into(), "kept".into())]);
    assert!(ana.sync.database.reloading_audiences().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn loading_a_snapshot_checks_its_announced_digest_before_its_signature() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let slot = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    let history = ben.sync.session.lock().await.history.clone();
    let announcement = &history.announcements[&slot];
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    let size = group.storage.status(&path).await.unwrap().unwrap().size;
    group.storage.corrupt_byte(&path, size as usize - 1).await.unwrap();
    let snapshot = crate::snapshot_object::SnapshotObject::read_prefix(
        &ben.sync.database,
        ben.sync.storage.as_ref(),
        &path,
    )
    .await
    .unwrap();
    let ring = ben.sync.keys.read().unwrap().unwrap();
    // The signature's last byte changed: every chunk still opens, and the
    // body is refused for its digest before its signature is checked.
    let result = snapshot
        .read_body(ben.sync.storage.as_ref(), &ring, announcement, None, None)
        .await
        .map(drop);
    assert!(
        matches!(
            result,
            Err(SyncError::Damaged(DamagedObject { failure: Refusal::WrongIdentity { .. }, .. }))
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
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
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
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana's write is claimed and the reply is lost. A reload at the app's
/// request settles that claim first: no second slot is taken, the write
/// leaves the queue, and the reload keeps its row (§19.2).
#[tokio::test]
async fn a_reload_settles_a_claim_in_flight_first() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "a", "in the snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    insert(&ana.sync.database, "b", "claimed").await;
    ana.storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    assert!(matches!(ana.sync.pass().await, Err(SyncError::Storage(_))));
    assert!(ana.sync.database.current_claim().await.unwrap().is_some());
    let applied = ana.sync.database.applied_slot().await.unwrap();

    ana.sync.reload_from_snapshot().await.unwrap();
    assert!(ana.sync.database.current_claim().await.unwrap().is_none());
    assert!(ana.sync.database.test_queued_writes().await.unwrap().is_empty());
    let applied_after = ana.sync.database.applied_slot().await.unwrap();
    assert!(applied_after > applied);
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, (applied_after + 1).try_into().unwrap()))
        .await
        .unwrap()
        .is_none());
    assert_eq!(rows(&ana.sync.database).await.len(), 2);
    ana.sync.database.close().await.unwrap();
}

/// Ana writes a row after her reset snapshot is captured and before her
/// reset entry applies. She has not read the reset, so every device, hers
/// included, ignores that row in the store with no lost value (§19.4).
#[tokio::test]
async fn the_resetting_devices_own_writes_after_its_capture_are_ignored() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "in the snapshot").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
        .await;
    let late = async {
        observed.await.unwrap();
        insert(&ana.sync.database, "late", "after the capture").await;
        resume.send(()).unwrap();
    };
    let (reset, ()) = tokio::join!(ana.sync.reset(Audience::Store), late);
    reset.unwrap();
    for _ in 0..2 {
        ana.sync.pass().await.unwrap();
        ben.sync.pass().await.unwrap();
    }
    for device in [&ana, &ben] {
        assert_eq!(rows(&device.sync.database).await, [("a".into(), "in the snapshot".into())]);
        assert!(device.sync.database.lost_values().await.unwrap().is_empty());
    }
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// A reset, a schema raise and a second reset of the store take effect in
/// slot order, and the history keeps all three entries (§19.4).
#[tokio::test]
async fn later_resets_and_raises_apply_in_slot_order_and_keep_the_earlier_entry() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "first").await;
    ana.sync.pass().await.unwrap();
    ana.sync.reset(Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let (ana, ben) = (group.reopen(ana).await, group.reopen(ben).await);
    for _ in 0..2 {
        ana.sync.pass().await.unwrap();
    }
    insert(&ana.sync.database, "b", "second").await;
    ana.sync.pass().await.unwrap();
    ana.sync.reset(Audience::Store).await.unwrap();
    for _ in 0..2 {
        ben.sync.pass().await.unwrap();
        ana.sync.pass().await.unwrap();
    }
    for device in [&ana, &ben] {
        let history = device.sync.session.lock().await.history.clone();
        let kinds: Vec<_> = history
            .boundaries
            .iter()
            .map(|coverage| matches!(coverage.boundary, coven_database::Boundary::Reset { .. }))
            .collect();
        assert_eq!(kinds, [true, false, true], "reset, raise, reset in slot order");
        let entries: Vec<_> =
            history.boundaries.iter().map(|coverage| coverage.boundary.entry()).collect();
        assert!(entries.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            rows(&device.sync.database).await,
            [("a".into(), "first".into()), ("b".into(), "second".into())]
        );
    }
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// Ana and Ben reset the store at once. Ana's reset entry lands first; Ben's
/// operation applies it and takes a new snapshot covering it before claiming
/// a later reset, so both devices end with both rows (§19.4).
#[tokio::test]
async fn of_two_resets_the_first_claim_applies_and_the_other_recaptures() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "ana's").await;
    ana.sync.pass().await.unwrap();
    ben.sync.pass().await.unwrap();
    insert(&ben.sync.database, "b", "ben's").await;
    let hold = || async {
        let (started, observed) = tokio::sync::oneshot::channel();
        let (resume, held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(coven_storage::ObjectPrefix::all(group.store), started, held)
            .await;
        (observed, resume)
    };
    let (upload, upload_resume) = hold().await;
    let resetting = ben.sync.reset(Audience::Store);
    let racing = async {
        upload.await.unwrap();
        let (announce, announce_resume) = hold().await;
        upload_resume.send(()).unwrap();
        announce.await.unwrap();
        let (activate, activate_resume) = hold().await;
        announce_resume.send(()).unwrap();
        activate.await.unwrap();
        ana.sync.reset(Audience::Store).await.unwrap();
        activate_resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(resetting, racing);
    result.unwrap();
    for _ in 0..2 {
        ana.sync.pass().await.unwrap();
        ben.sync.pass().await.unwrap();
    }
    let history = ben.sync.session.lock().await.history.clone();
    let resets: Vec<_> = history
        .boundaries
        .iter()
        .filter_map(|coverage| match &coverage.boundary {
            coven_database::Boundary::Reset { snapshot, .. } => Some(snapshot.device),
            coven_database::Boundary::SchemaChange(_) => None,
        })
        .collect();
    assert_eq!(resets, [ana.sync.device, ben.sync.device], "Ana's reset applies first");
    drop(history);
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    // Ben's write never read Ana's reset, so every device ignores it.
    assert_eq!(rows(&ana.sync.database).await, [("a".into(), "ana's".into())]);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

/// The floor in storage, as `device` reads it.
pub(crate) async fn stored_floor(device: &Device) -> crate::floor_object::FloorObject {
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
/// from its body, so he loads the store from the floor's snapshot (§16.5).
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
        .status(&ObjectPath::slot(group.store, (prepared + 1).try_into().unwrap()))
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
    assert_eq!(rows(&ana.sync.database).await, [("a".into(), "before the preparation".into())]);
    assert_eq!(rows(&ana.sync.database).await, rows(&ben.sync.database).await);
    assert!(ben.sync.database.reloading_audiences().await.unwrap().is_empty());
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
/// the common target at its coverage (§16.5, "Newly reading a circle").
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
        .administer(StoreChange::AddCircleMember { circle: gifts, member: ben.identity.member() })
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
        [(PIN_1.into(), "before Ben".into()), (PIN_2.into(), "after Ben".into())]
    );
    assert_eq!(rows(&ben.sync.database).await.len(), 1);
    assert!(ben.sync.database.reloading_audiences().await.unwrap().is_empty());
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
        .administer(StoreChange::AddCircleMember { circle: gifts, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(pins(&ben.sync.database).await, [(PIN_1.into(), "before Ben".into())]);
    assert!(ben.sync.database.reloading_audiences().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

const PIN_1: &str = "00000000-0000-4000-8000-000000000001";
const PIN_2: &str = "00000000-0000-4000-8000-000000000002";

// §16.5 and E6: an explicit reload resumes its original selection and target.
#[tokio::test]
async fn a_reload_reopens_with_its_selected_snapshot_and_target() {
    for background in [false, true] {
        let group = Group::new();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group.create(&ana).await;
        group.admit(&ana, &ben).await;
        insert(&ana.sync.database, "a", "original snapshot").await;
        ana.sync.pass().await.unwrap();
        let first = ana.sync.snapshot(Audience::Store).await.unwrap();
        ben.sync.pass().await.unwrap();
        let target = ben.database().applied_slot().await.unwrap();
        let original = ben.sync.session.lock().await.history.announcements[&first].snapshot.clone();
        let path = crate::snapshot_object::path(group.store, &original).unwrap();
        ben.storage
            .set_faults(Faults {
                read_failures: [(path, StorageFailure::PermissionDenied)].into(),
                ..Faults::none()
            })
            .await;
        assert!(ben.sync.reload_from_snapshot().await.is_err());
        let operation = ben
            .database()
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.kind == "sequence-reload")
            .expect("a failed explicit reload retains its selection")
            .id;
        assert!(ben
            .database()
            .operation_pending::<PendingRecord>(operation)
            .await
            .unwrap()
            .is_some());
        insert(&ben.sync.database, "local", "waiting throughout reload").await;
        ben.storage.set_faults(Faults::none()).await;
        insert(&ana.sync.database, "later", "outside the fixed target").await;
        ana.sync.pass().await.unwrap();
        let newest = ana.sync.snapshot(Audience::Store).await.unwrap();
        let newest = ana.sync.session.lock().await.history.announcements[&newest].snapshot.clone();
        let newest_path = crate::snapshot_object::path(group.store, &newest).unwrap();
        let ben = group.reopen(ben).await;
        let mut failures =
            std::collections::BTreeMap::from([(newest_path, StorageFailure::PermissionDenied)]);
        if background {
            failures.insert(
                ObjectPath::slot(group.store, (target + 1).try_into().unwrap()),
                StorageFailure::Network,
            );
        }
        ben.storage.set_faults(Faults { read_failures: failures, ..Faults::none() }).await;
        if background {
            assert!(ben.sync.retry_pending_operation(operation).await.is_err());
        } else {
            ben.sync.reload_from_snapshot().await.unwrap();
        }
        assert_eq!(ben.database().applied_slot().await.unwrap(), target);
        assert_eq!(
            rows(ben.database()).await,
            [
                ("a".into(), "original snapshot".into()),
                ("local".into(), "waiting throughout reload".into()),
            ]
        );
        assert!(ben.database().operation(operation).await.unwrap().is_none());
        ana.database().close().await.unwrap();
        ben.database().close().await.unwrap();
    }
}

// §16.5: authenticating a later floor must not apply its membership to the fixed target.
#[tokio::test]
async fn a_reload_keeps_its_selected_audiences_when_a_later_floor_records_departure() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    pin(ana.database(), PIN_1, circle, "snapshot").await;
    ana.sync.pass().await.unwrap();
    let first = ana.sync.snapshot(Audience::Circle(circle)).await.unwrap();
    pin(ana.database(), PIN_2, circle, "retained slot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    let target = ben.database().applied_slot().await.unwrap();
    let selected = ben.sync.session.lock().await.history.announcements[&first].clone();
    let path = crate::snapshot_object::path(group.store, &selected.snapshot).unwrap();
    ben.storage
        .set_faults(Faults {
            read_failures: [(path.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    assert!(ben.sync.reload_from_snapshot().await.is_err());
    ben.storage.set_faults(Faults::none()).await;
    ana.sync
        .administer(StoreChange::RemoveCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert!(floor.header.at > target);
    assert!(floor.header.first.0 <= selected.covers + 1);
    assert!(ana.storage.status(&path).await.unwrap().is_some());
    let ben = group.reopen(ben).await;
    ben.sync.reload_from_snapshot().await.unwrap();
    assert_eq!(ben.database().applied_slot().await.unwrap(), target);
    assert_eq!(
        pins(ben.database()).await,
        [(PIN_1.into(), "snapshot".into()), (PIN_2.into(), "retained slot".into()),]
    );
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}

// §11: dropping the caller while a body is loading leaves the operation resumable.
#[tokio::test]
async fn cancelling_a_reload_keeps_its_selected_target_and_waiting_writes() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(ana.database(), "a", "in the snapshot").await;
    ana.sync.pass().await.unwrap();
    let first = ana.sync.snapshot(Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    let target = ben.database().applied_slot().await.unwrap();
    let snapshot = ben.sync.session.lock().await.history.announcements[&first].snapshot.clone();
    let path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
    let (started, observed) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    ben.storage.hold_next_read(path, started, held).await;
    {
        let reload = ben.sync.reload_from_snapshot();
        tokio::pin!(reload);
        tokio::select! {
            result = &mut reload => panic!("held reload returned: {result:?}"),
            result = observed => result.unwrap(),
        }
        insert(ben.database(), "local", "while loading").await;
        assert_eq!(ben.database().applied_slot().await.unwrap(), target);
        assert!(ben
            .database()
            .operations()
            .await
            .unwrap()
            .iter()
            .any(|r| r.kind == "sequence-reload"));
    }
    assert!(release.send(()).is_err(), "the cancelled read released its receiver");
    let ben = group.reopen(ben).await;
    ben.sync.reload_from_snapshot().await.unwrap();
    assert_eq!(ben.database().applied_slot().await.unwrap(), target);
    assert_eq!(rows(ben.database()).await.len(), 2);
    assert!(ben.database().operations().await.unwrap().is_empty());
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}

// §19.1 and E6: repeating a reload cannot recheck a refused body without an update.
#[tokio::test]
async fn a_reload_preserves_a_refused_body_until_an_applicable_update() {
    for explicit in [false, true] {
        let group = Group::new();
        let ana = group.device(1).await;
        let ben = group.device(2).await;
        group.create(&ana).await;
        group.admit(&ana, &ben).await;
        let slot = ana.sync.snapshot(Audience::Store).await.unwrap();
        ben.sync.pass().await.unwrap();
        let snapshot = ben.sync.session.lock().await.history.announcements[&slot].snapshot.clone();
        let path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
        let size = group.storage.status(&path).await.unwrap().unwrap().size;
        group.storage.corrupt_byte(&path, size as usize - 1).await.unwrap();
        assert!(matches!(ben.sync.reload_from_snapshot().await, Err(SyncError::Damaged(_))));
        let operation = ben
            .database()
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.kind == "sequence-reload")
            .unwrap();
        let pending =
            ben.database().operation_pending::<PendingRecord>(operation.id).await.unwrap().unwrap();
        let result = if explicit {
            ben.sync.reload_from_snapshot().await
        } else {
            ben.sync.retry_pending_operation(operation.id).await.map(drop)
        };
        assert!(
            matches!(&result, Err(SyncError::Pending(record)) if **record == pending),
            "{result:?}"
        );
        assert_eq!(
            ben.database().operation_pending::<PendingRecord>(operation.id).await.unwrap(),
            Some(pending)
        );
        assert_eq!(
            ben.database().operation(operation.id).await.unwrap().unwrap().data,
            operation.data
        );
        ana.database().close().await.unwrap();
        ben.database().close().await.unwrap();
    }
}

async fn blocked_floor(compare: bool) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let circle = ana.sync.create_circle("Gifts".into()).await.unwrap();
    ana.sync
        .administer(StoreChange::AddCircleMember { circle, member: ben.identity.member() })
        .await
        .unwrap();
    ben.sync.pass().await.unwrap();
    let applied = ben.database().applied_slot().await.unwrap();
    insert(ana.database(), "new", "unread").await;
    pin(ana.database(), PIN_1, circle, "unread gift").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Circle(circle)).await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    let broken = ObjectPath::slot(group.store, (applied + 1).try_into().unwrap());
    group.storage.corrupt_byte(&broken, 0).await.unwrap();
    assert!(ben.sync.pass().await.is_err());
    assert!(ben.sync.session.lock().await.blocked.is_some());
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    {
        let mut session = ana.sync.session.lock().await;
        let mut ring = ana.sync.ring(&mut session, &ana.identity).await.unwrap();
        ana.sync.retain_sequence(&mut session, &mut ring, &ana.identity).await.unwrap();
    }
    let mut session = ben.sync.session.lock().await;
    let mut ring = ben.sync.ring(&mut session, &ben.identity).await.unwrap();
    let floor = stored_floor(&ben).await;
    let first = floor.header.first.0;
    assert!(first > applied + 1);
    let requests = ben.storage.request_count();
    let result = ben.sync.reload_floor(&mut session, floor, &mut ring, &ben.identity, None).await;
    assert!(matches!(result, Err(SyncError::Pending(_))));
    assert_eq!(ben.storage.request_count(), requests, "blocked reload opens no audience");
    assert_eq!(ben.database().applied_slot().await.unwrap(), applied);
    if compare {
        let input = serde_json::json!({"kind":"read","stopped":true,"applied":applied,
            "fetched":[applied + 1],"first_kept":first});
        assert_eq!(
            crate::tests::run_lean("COVEN_SEQUENCE_LEAN", &input),
            serde_json::json!({"reload":false,"apply":[]})
        );
    }
    drop(session);
    assert!(rows(ben.database()).await.is_empty());
    assert!(pins(ben.database()).await.is_empty());
    ana.database().close().await.unwrap();
    ben.database().close().await.unwrap();
}

#[tokio::test]
async fn blocked_device_reloads_no_audience() {
    blocked_floor(false).await;
}

#[tokio::test]
#[ignore = "requires COVEN_SEQUENCE_LEAN; scripts/check.sh builds sequenceRunner"]
async fn sequence_blocked_floor_matches_lean() {
    blocked_floor(true).await;
}

/// Ben is added to Gifts, whose announced snapshot storage no longer has.
/// His pass reports the snapshot as missing, and goes on reporting it; Gifts
/// stays unwritable for him while the store still takes his writes (§15.7).
#[tokio::test]
async fn a_newly_readable_circle_whose_snapshot_is_missing_reports_it() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let gifts = ana.sync.create_circle("Gifts".into()).await.unwrap();
    pin(&ana.sync.database, PIN_1, gifts, "before Ben").await;
    ana.sync.pass().await.unwrap();
    let announced = ana.sync.snapshot(Audience::Circle(gifts)).await.unwrap();
    ben.sync.pass().await.unwrap();
    let snapshot = ana.sync.session.lock().await.history.announcements[&announced].snapshot.clone();
    let path = crate::snapshot_object::path(group.store, &snapshot).unwrap();
    group
        .storage
        .set_faults(Faults {
            read_failures: [(path.clone(), StorageFailure::NotFound)].into(),
            ..Faults::none()
        })
        .await;
    ana.sync
        .administer(StoreChange::AddCircleMember { circle: gifts, member: ben.identity.member() })
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(ben.sync.pass().await.is_err());
        let SequenceStatus::Blocked(record) = ben.sync.state.subscribe().borrow().clone() else {
            panic!("the slot adding Ben blocks on the missing snapshot");
        };
        assert!(
            matches!(
                &record.reason,
                PendingReason::Storage { failure, .. } if *failure == StorageFailure::NotFound
            ),
            "{record:?}"
        );
        let pending = ben.sync.database.pending::<PendingRecord>().await.unwrap();
        assert!(
            pending.iter().any(|record| matches!(
                &record.reason,
                PendingReason::Waits(Prerequisite::Reload(Audience::Circle(c))) if *c == gifts
            )),
            "{pending:?}"
        );
    }
    let gifts_write = ben
        .sync
        .database
        .write(move |sql| {
            sql.execute("INSERT INTO pins VALUES(?1,?2,'fresh')", (PIN_2, gifts.to_string()))?;
            Ok(())
        })
        .await;
    assert!(
        matches!(gifts_write, Err(coven_database::DbError::AudienceReloading(Audience::Circle(c))) if c == gifts),
        "{gifts_write:?}"
    );
    insert(&ben.sync.database, "store", "still writable").await;
    // Once storage serves the snapshot again, the next pass loads Gifts.
    group.storage.set_faults(Faults::none()).await;
    ben.sync.pass().await.unwrap();
    assert_eq!(pins(&ben.sync.database).await, [(PIN_1.into(), "before Ben".into())]);
    assert!(ben.sync.database.pending::<PendingRecord>().await.unwrap().is_empty());
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// Ana's reset preparation is announced, then undone before activating (her
/// admin role goes, so the reset is refused and her journal deletes the
/// body). Ben applies the announcement without reading its body, changes no
/// rows, schema version or reset, and neither his explicit reload nor the
/// floor ever uses it (§16.3).
#[tokio::test]
async fn an_abandoned_preparation_stays_history_and_supplies_no_snapshot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(ana.database(), "a", "kept").await;
    ana.sync.pass().await.unwrap();
    let ordinary = ana.sync.snapshot(Audience::Store).await.unwrap();
    let prepared = ana.database().applied_slot().await.unwrap();
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
    let body = ana
        .storage
        .request_trace()
        .into_iter()
        .rev()
        .find_map(|request| match request {
            coven_storage::test_utils::MemoryRequest::Create(path)
                if path.snapshot_id().is_some() =>
            {
                Some(path)
            }
            _ => None,
        })
        .expect("the preparation's body was uploaded");
    ben.sync.pass().await.unwrap();
    let rows_before = rows(ben.database()).await;
    let state_before = ben.database().store_state().await.unwrap();
    let preparation = {
        let session = ben.sync.session.lock().await;
        let (slot, announcement) = session.history.announcements.last_key_value().unwrap();
        assert!(*slot > ordinary);
        assert_eq!(announcement.purpose, coven_format::sequence::Purpose::Reset);
        *slot
    };
    // Ana loses her admin role, so her reset can't finish and is undone.
    ben.sync
        .administer(StoreChange::ChangeRole {
            member: ana.identity.member(),
            role: coven_format::admin::MemberRole::Member,
        })
        .await
        .unwrap();
    let operation = ana.database().operations().await.unwrap()[0].id;
    ana.sync.pass().await.unwrap();
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(ana.database().operations().await.unwrap().is_empty());
    assert!(group.storage.status(&body).await.unwrap().is_none(), "the journal deleted the body");

    ben.sync.pass().await.unwrap();
    assert_eq!(rows(ben.database()).await, rows_before);
    let state = ben.database().store_state().await.unwrap();
    assert_eq!(state.schema, state_before.schema);
    assert_eq!(state.resets, state_before.resets);
    {
        let session = ben.sync.session.lock().await;
        let announcement = &session.history.announcements[&preparation];
        assert!(!session.history.usable(announcement), "an abandoned preparation is not usable");
    }
    let before = ben.storage.request_trace().len();
    ben.sync.reload_from_snapshot().await.unwrap();
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ben.sync.pass().await.unwrap();
    let floor = stored_floor(&ben).await;
    assert!(!floor.header.snapshots.contains(&preparation), "{:?}", floor.header.snapshots);
    {
        let session = ben.sync.session.lock().await;
        for slot in &floor.header.snapshots {
            let named = &session.history.announcements[slot];
            assert_eq!(named.purpose, coven_format::sequence::Purpose::Ordinary);
        }
    }
    let reads_body = ben.storage.request_trace().into_iter().any(|request| {
        matches!(request, coven_storage::test_utils::MemoryRequest::Read(path, _) if path == body)
    });
    assert!(!reads_body, "no device reads an abandoned preparation's body");
    assert!(ben.storage.request_trace().len() > before);
    assert_eq!(rows(ben.database()).await, rows_before);
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}
