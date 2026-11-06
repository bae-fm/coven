use crate::sequence_sync::tests::*;
use coven_format::admin::StoreChange;
use coven_foundation::id_source::ObjectId;
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage};

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
