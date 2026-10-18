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

#[tokio::test]
async fn a_signed_floor_whose_clear_first_disagrees_with_its_body_is_damaged() {
    use coven_format::{
        sealed_floor::{FloorPrefixDecoder, FloorPrefixEncoder},
        sealed_stream::StreamObjectLayout,
    };
    use std::io::{Cursor, Read};

    let fixture = hex(include_str!("../../coven-format/fixtures/sealed-floor.hex"));
    let mut decoder =
        FloorPrefixDecoder::start(&fixture[..FloorPrefixDecoder::HEADER_LENGTH]).unwrap();
    let mut length = FloorPrefixDecoder::HEADER_LENGTH;
    let mut copies = Vec::new();
    while decoder.remaining() > 0 {
        let size = decoder.copy_length(&fixture[length..]).unwrap();
        copies.push(decoder.copy(&fixture[length..length + size]).unwrap());
        length += size;
    }
    decoder.finish().unwrap();
    let path = ObjectPath::floor(f::STORE_ID);
    let mut layout = StreamObjectLayout::new();
    let chunk = layout
        .decode_chunk(&fixture[length..fixture.len() - 64])
        .unwrap();
    let plain = f::store_key()
        .derive()
        .open_object_chunk(path.as_str(), &fixture[..length], 0, 0, chunk)
        .unwrap();
    for first in [5, 7] {
        let mut group = Group::new();
        group.store = f::STORE_ID;
        group.ring = coven_crypto::StoreKeyring::new(f::store_key());
        let device = group.device(1).await;
        let (mut encoder, mut prefix) = FloorPrefixEncoder::start(
            f::store_key().id(),
            coven_merge::Slot(first),
            copies.len().try_into().unwrap(),
        )
        .unwrap();
        for copy in &copies {
            prefix.extend(encoder.copy(copy).unwrap());
        }
        encoder.finish().unwrap();
        let mut stream = crate::sealed_stream::SealedStream::new(
            Cursor::new(plain.clone()),
            path.clone(),
            prefix,
            None,
            f::store_key().derive(),
            f::identity(),
        );
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).unwrap();
        // Both the encryption and signature bind the conflicting prefix.
        // A mere tamper would fail before exercising the equality check.
        let mut digest = coven_crypto::ObjectHasher::new();
        digest.update(&bytes[..bytes.len() - 64]);
        let signature =
            coven_crypto::Signature::from_bytes(bytes[bytes.len() - 64..].try_into().unwrap());
        f::member()
            .signing
            .verify_object(path.as_str(), &digest.finish(), &signature)
            .unwrap();
        group.storage.create(&path, &bytes).await.unwrap();
        let result = FloorObject::read(
            device.database(),
            &group.storage,
            &path,
            None,
            &mut group.ring.clone(),
            &f::identity(),
        )
        .await;
        assert!(
            matches!(result, Err(crate::SyncError::Damaged(ref object))
            if object.path == path.as_str() && matches!(object.failure, crate::Refusal::Parse { .. })),
            "prefix first {first} disagrees with body first 6"
        );
        device.database().close().await.unwrap();
    }
}
