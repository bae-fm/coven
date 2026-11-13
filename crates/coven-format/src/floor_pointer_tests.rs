use super::*;
use crate::test_utils as f;
use coven_merge::slot;

fn prefix() -> FloorPointerPrefix {
    FloorPointerPrefix {
        key: f::store_key().id(),
        first: slot(6),
        device: DeviceId(4),
        number: slot(7),
        body: [0x2a; 32],
    }
}

#[test]
fn a_pointer_prefix_round_trips_and_names_its_body() {
    let bytes = prefix().encode().unwrap();
    assert_eq!(bytes.len(), FloorPointerPrefix::LENGTH);
    assert_eq!(bytes[0], 42);
    assert_eq!(FloorPointerPrefix::decode(&bytes).unwrap(), prefix());
    assert_eq!(
        prefix().body_path(f::STORE_ID),
        ObjectPath::floor_body(f::STORE_ID, DeviceId(4), std::num::NonZeroU64::new(7).unwrap())
    );
}

#[test]
fn a_pointer_prefix_refuses_other_kinds_lengths_and_zero_slots() {
    let bytes = prefix().encode().unwrap();
    let mut other = bytes.clone();
    other[0] = 39;
    assert!(FloorPointerPrefix::decode(&other).is_err());
    assert!(FloorPointerPrefix::decode(&bytes[..bytes.len() - 1]).is_err());
    let mut longer = bytes.clone();
    longer.push(0);
    assert!(FloorPointerPrefix::decode(&longer).is_err());
    for range in [19..27, 35..43] {
        let mut zero = bytes.clone();
        zero[range].fill(0);
        assert!(FloorPointerPrefix::decode(&zero).is_err());
    }
}

#[test]
fn a_pointer_header_frame_round_trips() {
    let header = FloorPointerHeader { author: DeviceId(4), at: 570 };
    let bytes = header.encode().unwrap();
    assert_eq!(bytes[0], 19);
    assert_eq!(FloorPointerHeader::decode(&bytes).unwrap(), header);
    assert!(FloorPointerHeader::decode(&bytes[..bytes.len() - 1]).is_err());
    assert!(crate::decode_frame(&bytes).is_ok());
}

#[test]
fn the_pointer_fixture_names_its_body_by_digest_and_opens_at_the_floor_path() {
    use crate::{sealed_stream::StreamObjectLayout, test_utils::fixture_bytes as hex};
    let bytes = hex(include_str!("../fixtures/sealed-floor-pointer.hex"));
    let body = hex(include_str!("../fixtures/sealed-floor.hex"));
    let prefix = FloorPointerPrefix::decode(&bytes[..FloorPointerPrefix::LENGTH]).unwrap();
    assert_eq!(prefix.key, f::store_key().id());
    assert_eq!((prefix.first.get(), prefix.device.0, prefix.number.get()), (6, 1, 3));
    let mut digest = coven_crypto::ObjectHasher::new();
    digest.update(&body);
    assert_eq!(&prefix.body, digest.finish().as_bytes());
    let path = ObjectPath::floor(f::STORE_ID);
    let mut layout = StreamObjectLayout::new();
    let chunk = layout.decode_chunk(&bytes[FloorPointerPrefix::LENGTH..bytes.len() - 64]).unwrap();
    let plain = f::store_key()
        .derive()
        .open_object_chunk(path.as_str(), &bytes[..FloorPointerPrefix::LENGTH], 0, 0, chunk)
        .unwrap();
    let header = FloorPointerHeader::decode(&plain).unwrap();
    assert_eq!((header.author.0, header.at), (1, 11));
    let mut signed = coven_crypto::ObjectHasher::new();
    signed.update(&bytes[..bytes.len() - 64]);
    let signature =
        coven_crypto::Signature::from_bytes(bytes[bytes.len() - 64..].try_into().unwrap());
    f::member().signing.verify_object(path.as_str(), &signed.finish(), &signature).unwrap();
}
