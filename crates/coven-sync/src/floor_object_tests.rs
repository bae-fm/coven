use crate::{
    floor_object::FloorObject, history::History, sequence_sync::tests::Group, slot_object,
};
use coven_format::test_utils as f;
use coven_storage::{ObjectPath, Storage};

use coven_format::test_utils::fixture_bytes as hex;

#[tokio::test]
async fn an_authenticated_floor_fixture_coexists_with_undeleted_original_slots() {
    let mut group = Group::new();
    group.store = f::STORE_ID;
    group.ring = coven_crypto::StoreKeyring::new(f::store_key());
    let device = group.device(1).await;
    let identity = f::identity();
    let mut ring = group.ring.clone();
    let first = hex(include_str!("../../coven-format/fixtures/sealed-admin.hex"));
    let second = hex(include_str!(
        "../../coven-format/fixtures/sealed-store-key.hex"
    ));
    let floor = hex(include_str!("../../coven-format/fixtures/sealed-floor.hex"));
    group
        .storage
        .create(&f::slot_path(1), &first)
        .await
        .unwrap();
    group
        .storage
        .create(&f::slot_path(2), &second)
        .await
        .unwrap();
    let path = ObjectPath::floor(f::STORE_ID);
    group.storage.create(&path, &floor).await.unwrap();
    let mut history = History::default();
    let creation = slot_object::open_bounded(first, &f::slot_path(1), &ring, &history).unwrap();
    history.consume(creation, identity.member());
    let observed = FloorObject::read(
        device.database(),
        &group.storage,
        &path,
        None,
        &mut ring,
        &identity,
    )
    .await
    .unwrap()
    .unwrap();
    observed.authenticate(&history).unwrap();
    assert_eq!(observed.header.first.0, 6);
    for number in [1, 2] {
        let old = f::slot_path(number);
        assert!(old.slot_number().unwrap() < observed.header.first);
        assert!(group.storage.status(&old).await.unwrap().is_some());
    }
    assert_eq!(observed.copies[0].object, second);
    let requests = group.storage.request_count();
    let reused = FloorObject::read(
        device.database(),
        &group.storage,
        &path,
        Some(&observed),
        &mut ring,
        &identity,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(group.storage.request_count() - requests, 1);
    assert_eq!(reused.header, observed.header);
    device.database().close().await.unwrap();
}
