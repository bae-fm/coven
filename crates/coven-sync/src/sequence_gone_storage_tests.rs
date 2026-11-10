use crate::sequence_sync::sequence_reload::tests::stored_floor;
use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_foundation::id_source::ObjectId;
use coven_merge::Audience;
use coven_storage::{
    test_utils::{Faults, MemoryRequest},
    ObjectPath, Storage,
};

fn photo(group: &Group, device: &Device, number: u128) -> ObjectPath {
    ObjectPath::file(group.store, device.sync.device, ObjectId(uuid::Uuid::from_u128(number)))
}

/// Ana's phone was sending a photo in parts when Ana's laptop removed it, so
/// the phone is gone for good and will never end that upload. Ben's phone
/// leaves it: Ben is neither the phone's member nor needed while Ana has a
/// device left. The laptop deletes the phone's files, so its next pass ends
/// the phone's upload, and the laptop's own upload stays (§17.10).
#[tokio::test]
async fn the_device_that_deletes_a_gone_devices_files_ends_its_uploads() {
    let group = Group::new();
    let phone = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&phone).await;
    group.admit(&phone, &ben).await;
    let laptop = group.laptop(&phone).await;
    phone.storage.started_upload(&photo(&group, &phone, 1)).await.unwrap();
    laptop.storage.started_upload(&photo(&group, &laptop, 2)).await.unwrap();
    laptop.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    ben.sync.pass().await.unwrap();
    assert_eq!(group.storage.unfinished_uploads().await.len(), 2, "not Ben's to end");
    laptop.sync.pass().await.unwrap();
    assert_eq!(
        group.storage.unfinished_uploads().await,
        [(laptop.sync.device, photo(&group, &laptop, 2))]
    );
    for device in [&phone, &ben, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// The phone keeps running until it reads its removal, and its key keeps
/// working: after the laptop's pass ended its uploads and listed its
/// snapshot folders, the phone starts another upload and stores a snapshot
/// body. The laptop's passes don't look again; the app's cleanup of gone
/// devices' files does, and ends the upload and deletes the body (§16.6,
/// §17.10).
#[tokio::test]
async fn the_apps_cleanup_ends_what_a_gone_device_left_since() {
    let group = Group::new();
    let phone = group.device(1).await;
    group.create(&phone).await;
    let laptop = group.laptop(&phone).await;
    laptop.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    laptop.sync.pass().await.unwrap();
    let late = photo(&group, &phone, 3);
    phone.storage.started_upload(&late).await.unwrap();
    let body = ObjectPath::snapshot(
        group.store,
        Audience::Store,
        phone.sync.device,
        std::num::NonZeroU64::new(9).unwrap(),
    );
    group.storage.plant(&body, b"a body stored after the listing").await.unwrap();
    laptop.sync.pass().await.unwrap();
    assert_eq!(group.storage.unfinished_uploads().await, [(phone.sync.device, late)]);
    assert!(group.storage.status(&body).await.unwrap().is_some());
    laptop.sync.clean_up_gone_devices_files().await.unwrap();
    assert!(group.storage.unfinished_uploads().await.is_empty());
    assert!(group.storage.status(&body).await.unwrap().is_none());
    for device in [&phone, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// Ana's phone announces two store snapshots, and her laptop removes it:
/// the laptop deletes the gone phone's storage, and lists its snapshot
/// folders once. Once grace passes, the laptop's floor passes the phone's
/// first announcement, which it forgets: it lists the phone's folders again
/// and deletes that body, which no kept announcement names. The second, which
/// the floor names, stays (§16.6, §16.8).
#[tokio::test]
async fn a_gone_devices_body_only_a_forgotten_announcement_named_goes() {
    let group = Group::new();
    let phone = group.device(1).await;
    group.create(&phone).await;
    let laptop = group.laptop(&phone).await;
    let mut bodies = Vec::new();
    for (id, title) in [("one", "first"), ("two", "second")] {
        insert(phone.database(), id, title).await;
        phone.sync.pass().await.unwrap();
        let slot = phone.sync.snapshot(Audience::Store).await.unwrap();
        let id = phone.sync.session.lock().await.history.announcements[&slot].snapshot.clone();
        bodies.push(crate::snapshot_object::path(group.store, &id).unwrap());
    }
    laptop.sync.administer(StoreChange::RemoveDevice { device: phone.sync.device }).await.unwrap();
    laptop.sync.pass().await.unwrap();
    let folder = coven_storage::ObjectPrefix::device_snapshots(
        group.store,
        Audience::Store,
        phone.sync.device,
    );
    let listings = || {
        laptop
            .storage
            .request_trace()
            .iter()
            .filter(|request| matches!(request, MemoryRequest::List(listed) if *listed == folder))
            .count()
    };
    assert_eq!(listings(), 1);
    assert!(group.storage.status(&bodies[0]).await.unwrap().is_some(), "not replaced yet");
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    laptop.sync.pass().await.unwrap();
    assert_eq!(listings(), 2, "forgetting the announcement lists the folder again");
    assert!(group.storage.status(&bodies[0]).await.unwrap().is_none());
    assert!(group.storage.status(&bodies[1]).await.unwrap().is_some(), "the floor names it");
    for device in [&phone, &laptop] {
        device.database().close().await.unwrap();
    }
}

/// Past grace, so every announcement made before it is mature.
fn past_grace(group: &Group) {
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
}

/// The slots of the original slots `device` keeps.
async fn original_slots(device: &Device) -> Vec<coven_merge::Slot> {
    device.database().original_slots().await.unwrap().into_iter().map(|o| o.slot).collect()
}

/// Ana announces store snapshots at three slots. Once the newest has passed
/// grace, her floor's `first` passes the first two: no rule reads them any
/// more. She forgets them, from her history and her original slots, so her
/// next store snapshot carries only the newest, and reopening rebuilds the
/// history she holds (§16.8).
#[tokio::test]
async fn announcements_the_floor_passed_are_forgotten() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut announced = Vec::new();
    for (id, title) in [("one", "first"), ("two", "second"), ("three", "third")] {
        insert(ana.database(), id, title).await;
        ana.sync.pass().await.unwrap();
        announced.push(ana.sync.snapshot(Audience::Store).await.unwrap());
    }
    let probe = coven_format::admin::SnapshotId {
        audience: Audience::Store,
        device: ana.sync.device,
        number: 1_000,
    };
    let section = || async {
        ana.database().test_snapshot_section_bytes(probe.clone()).await.unwrap()
            [coven_format::snapshot::section::ANNOUNCEMENT]
    };
    let before = section().await;
    let originals = original_slots(&ana).await;
    assert!(announced.iter().all(|slot| originals.contains(slot)));
    past_grace(&group);
    ana.sync.pass().await.unwrap();
    let held: Vec<_> =
        ana.sync.session.lock().await.history.announcements.keys().copied().collect();
    // The pass may announce a newer snapshot too, once its slots pass grace.
    assert!(held.contains(&announced[2]), "the newest stays: {held:?}");
    assert!(!held.contains(&announced[0]) && !held.contains(&announced[1]), "{held:?}");
    let originals = original_slots(&ana).await;
    assert!(!originals.contains(&announced[0]) && !originals.contains(&announced[1]));
    assert!(originals.contains(&announced[2]));
    let after = section().await;
    assert!(after < before, "the next snapshot's announcements shrink: {before} to {after}");
    let ana = group.reopen(ana).await;
    let rebuilt: Vec<_> =
        ana.sync.session.lock().await.history.announcements.keys().copied().collect();
    assert_eq!(rebuilt, held);
    ana.database().close().await.unwrap();
}

/// Ana's app raises the store to version 2: her raise's announcement and
/// its entry land. Two ordinary snapshots later pass grace and her floor
/// moves `first` past the raise's announcement. The raise's entry still
/// names it, so she keeps it, and reopening applies the raise again from it
/// (§16.8).
#[tokio::test]
async fn a_raises_announcement_stays_after_the_floor_passes_it() {
    let group = Group::new();
    group.raises.store(true, std::sync::atomic::Ordering::SeqCst);
    let ana = group.device(1).await;
    group.create(&ana).await;
    group.version.store(2, std::sync::atomic::Ordering::SeqCst);
    let ana = group.reopen(ana).await;
    ana.sync.pass().await.unwrap();
    ana.sync.pass().await.unwrap();
    let raised = {
        let session = ana.sync.session.lock().await;
        assert_eq!(session.history.boundaries.len(), 1, "the raise applied");
        *session
            .history
            .announcements
            .iter()
            .find(|(_, announcement)| {
                announcement.purpose == coven_format::sequence::Purpose::Raise(2)
            })
            .expect("the raise's announcement")
            .0
    };
    let mut ordinary = Vec::new();
    for (id, title) in [("one", "first"), ("two", "second")] {
        insert(ana.database(), id, title).await;
        ana.sync.pass().await.unwrap();
        ordinary.push(ana.sync.snapshot(Audience::Store).await.unwrap());
    }
    past_grace(&group);
    ana.sync.pass().await.unwrap();
    let floor = stored_floor(&ana).await;
    assert!(floor.header.first > raised, "the floor passed the raise's announcement");
    let held: Vec<_> =
        ana.sync.session.lock().await.history.announcements.keys().copied().collect();
    assert!(held.contains(&raised) && held.contains(&ordinary[1]), "{held:?}");
    assert!(!held.contains(&ordinary[0]), "the first ordinary one is forgotten: {held:?}");
    assert!(original_slots(&ana).await.contains(&raised));
    let ana = group.reopen(ana).await;
    let session = ana.sync.session.lock().await;
    assert!(session.history.announcements.contains_key(&raised));
    assert_eq!(session.history.boundaries.len(), 1, "rebuilding applies the raise again");
    drop(session);
    ana.database().close().await.unwrap();
}

/// Ana's first store snapshot is forgotten once her floor passes it. Her
/// next snapshot replaced it, and the walk over her announcements no longer
/// sees it: listing her own snapshot folder finds its body and deletes it,
/// and the newer one stays (§16.8).
#[tokio::test]
async fn a_forgotten_announcements_body_goes_with_the_listing_of_its_folder() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut bodies = Vec::new();
    for (id, title) in [("one", "first"), ("two", "second")] {
        insert(ana.database(), id, title).await;
        ana.sync.pass().await.unwrap();
        let slot = ana.sync.snapshot(Audience::Store).await.unwrap();
        bodies.push(announced_body(&group, &ana, slot).await);
    }
    let folder = coven_storage::ObjectPrefix::device_snapshots(
        group.store,
        Audience::Store,
        ana.sync.device,
    );
    let before = ana.storage.request_trace().len();
    past_grace(&group);
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&bodies[0]).await.unwrap().is_none());
    assert!(group.storage.status(&bodies[1]).await.unwrap().is_some());
    let trace = ana.storage.request_trace().split_off(before);
    assert!(
        trace
            .iter()
            .any(|request| matches!(request, MemoryRequest::List(listed) if *listed == folder)),
        "her own folder is listed: {trace:?}"
    );
    ana.database().close().await.unwrap();
}

/// Ana's third store snapshot is uploaded and its announcement's claim
/// fails, so its operation still holds the body, waiting to announce it.
/// Her retention then forgets her first announcement and lists her snapshot
/// folder: it deletes the first body and leaves the third, which its
/// operation names. The announcement then lands, her next floor names it,
/// and Ben, away all along, loads it (§16.8).
#[tokio::test]
async fn a_listing_leaves_a_body_its_operation_has_yet_to_announce() {
    use coven_storage::StorageFailure;
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let mut bodies = Vec::new();
    for (id, title) in [("one", "first"), ("two", "second")] {
        insert(ana.database(), id, title).await;
        ana.sync.pass().await.unwrap();
        let slot = ana.sync.snapshot(Audience::Store).await.unwrap();
        bodies.push(announced_body(&group, &ana, slot).await);
    }
    insert(ana.database(), "three", "third").await;
    ana.sync.pass().await.unwrap();
    let next = ana.database().applied_slot().await.unwrap() + 1;
    let claim = ObjectPath::slot(group.store, next.try_into().unwrap());
    group
        .storage
        .set_faults(Faults {
            create_failures: [(claim, (StorageFailure::ProviderUnavailable, 503))].into(),
            ..Faults::none()
        })
        .await;
    assert!(ana.sync.snapshot(Audience::Store).await.is_err());
    let folder = coven_storage::ObjectPrefix::device_snapshots(
        group.store,
        Audience::Store,
        ana.sync.device,
    );
    let waiting: Vec<_> = group
        .storage
        .list(&folder)
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.path)
        .filter(|path| !bodies.contains(path))
        .collect();
    assert_eq!(waiting.len(), 1, "the third body is uploaded");
    past_grace(&group);
    {
        let mut session = ana.sync.session.lock().await;
        let mut ring = group.ring.clone();
        ana.sync.retain_sequence(&mut session, &mut ring, &ana.identity).await.unwrap();
        ana.sync.retain_snapshots(&mut session, &mut ring, &ana.identity).await.unwrap();
    }
    assert!(group.storage.status(&bodies[0]).await.unwrap().is_none(), "the forgotten one goes");
    assert!(group.storage.status(&waiting[0]).await.unwrap().is_some(), "its operation names it");
    group.storage.set_faults(Faults::none()).await;
    let operation = ana
        .database()
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == "sequence")
        .expect("the snapshot waits to announce")
        .id;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    ana.sync.pass().await.unwrap();
    let named = stored_floor(&ana).await.header.snapshots;
    assert_eq!(
        announced_body(&group, &ana, named[0]).await,
        waiting[0],
        "the floor names the body the listing left"
    );
    ben.sync.pass().await.unwrap();
    assert_eq!(rows(ben.database()).await, rows(ana.database()).await);
    for device in [&ana, &ben] {
        device.database().close().await.unwrap();
    }
}

/// The path of the body the announcement at `slot` names.
async fn announced_body(group: &Group, device: &Device, slot: coven_merge::Slot) -> ObjectPath {
    let id = device.sync.session.lock().await.history.announcements[&slot].snapshot.clone();
    crate::snapshot_object::path(group.store, &id).unwrap()
}
