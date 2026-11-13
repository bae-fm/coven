use crate::{
    floor_object::FloorObject, history::History, sequence_sync::tests::Group, slot_object,
};
use coven_format::test_utils as f;
use coven_storage::{ObjectPath, Storage};

use coven_format::test_utils::fixture_bytes as hex;

/// Where the fixture's pointer says its body is: device 1's body 3.
fn body_path() -> ObjectPath {
    ObjectPath::floor_body(
        f::STORE_ID,
        coven_foundation::id_source::DeviceId(1),
        3.try_into().unwrap(),
    )
}

/// A pointer sealed and signed at the floor's path, naming `body` by digest.
fn pointer_to(body: &[u8], first: u64) -> Vec<u8> {
    use coven_format::floor_pointer::{FloorPointerHeader, FloorPointerPrefix};
    use std::io::Read;
    let mut digest = coven_crypto::ObjectHasher::new();
    digest.update(body);
    let prefix = FloorPointerPrefix {
        key: f::store_key().id(),
        first: coven_merge::slot(first),
        device: coven_foundation::id_source::DeviceId(1),
        number: coven_merge::slot(3),
        body: *digest.finish().as_bytes(),
    }
    .encode()
    .unwrap();
    let header = FloorPointerHeader { author: coven_foundation::id_source::DeviceId(1), at: 11 };
    let mut stream = crate::sealed_stream::SealedStream::new(
        std::io::Cursor::new(header.encode().unwrap()),
        ObjectPath::floor(f::STORE_ID),
        prefix,
        None,
        f::store_key().derive(),
        f::identity(),
    );
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    bytes
}

#[tokio::test]
async fn an_authenticated_floor_fixture_coexists_with_undeleted_original_slots() {
    let mut group = Group::new();
    group.store = f::STORE_ID;
    group.ring = coven_crypto::StoreKeyring::new(f::store_key());
    let device = group.device(1).await;
    let identity = f::identity();
    let mut ring = group.ring.clone();
    let first = hex(include_str!("../../coven-format/fixtures/sealed-admin.hex"));
    let second = hex(include_str!("../../coven-format/fixtures/sealed-store-key.hex"));
    let floor = hex(include_str!("../../coven-format/fixtures/sealed-floor.hex"));
    let pointer = hex(include_str!("../../coven-format/fixtures/sealed-floor-pointer.hex"));
    group.storage.create(&f::slot_path(1), &first).await.unwrap();
    group.storage.create(&f::slot_path(2), &second).await.unwrap();
    group.storage.create(&body_path(), &floor).await.unwrap();
    let path = ObjectPath::floor(f::STORE_ID);
    group.storage.create(&path, &pointer).await.unwrap();
    let mut history = History::default();
    let creation = slot_object::open_bounded(first, &f::slot_path(1), &ring, &history).unwrap();
    history.consume(creation, identity.member());
    let observed =
        FloorObject::read(device.database(), &group.storage, &path, None, &mut ring, &identity)
            .await
            .unwrap()
            .unwrap();
    observed.authenticate(&history).unwrap();
    assert_eq!(observed.header.first.get(), 6);
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
    let path = body_path();
    let mut layout = StreamObjectLayout::new();
    let chunk = layout.decode_chunk(&fixture[length..fixture.len() - 64]).unwrap();
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
            coven_merge::slot(first),
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
        f::member().signing.verify_object(path.as_str(), &digest.finish(), &signature).unwrap();
        group.storage.create(&path, &bytes).await.unwrap();
        // The pointer agrees with the body's clear prefix and digest.
        group
            .storage
            .create(&ObjectPath::floor(f::STORE_ID), &pointer_to(&bytes, first))
            .await
            .unwrap();
        let result = FloorObject::read(
            device.database(),
            &group.storage,
            &ObjectPath::floor(f::STORE_ID),
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

/// The pointer names its body by digest: a body that is not the one named is
/// refused, whoever signed it, and a pointer whose body is gone with the
/// pointer unchanged is a floor that cannot be read.
#[tokio::test]
async fn a_pointer_whose_body_differs_or_is_gone_is_refused() {
    let mut group = Group::new();
    group.store = f::STORE_ID;
    group.ring = coven_crypto::StoreKeyring::new(f::store_key());
    let device = group.device(1).await;
    let mut ring = group.ring.clone();
    let floor = hex(include_str!("../../coven-format/fixtures/sealed-floor.hex"));
    let pointer = ObjectPath::floor(f::STORE_ID);
    // The pointer names the digest of other bytes.
    let mut other = floor.clone();
    other[40] ^= 1;
    group.storage.create(&pointer, &pointer_to(&other, 6)).await.unwrap();
    group.storage.create(&body_path(), &floor).await.unwrap();
    let result = FloorObject::read(
        device.database(),
        &group.storage,
        &pointer,
        None,
        &mut ring,
        &f::identity(),
    )
    .await;
    assert!(
        matches!(result, Err(crate::SyncError::Damaged(ref object))
            if matches!(object.failure, crate::Refusal::WrongIdentity { .. })),
        "{:?}",
        result.err()
    );
    // The body is gone and the pointer has not moved.
    group.storage.delete(&body_path()).await.unwrap();
    let result = FloorObject::read(
        device.database(),
        &group.storage,
        &pointer,
        None,
        &mut ring,
        &f::identity(),
    )
    .await;
    assert!(result.is_err());
    device.database().close().await.unwrap();
}
