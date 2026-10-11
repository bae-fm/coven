use super::*;
use crate::{encode_frame, test_utils, Object};
use coven_foundation::id_source::CircleId;
use uuid::Uuid;

#[test]
fn d8_pending_report_can_publish_without_fingerprints() {
    // Device 1 reports write 2/4 refused as invalid, without claiming fingerprints.
    let bytes = crate::tests::hex(concat!(
        "0800010000002f",
        "0000000000000001",
        "00000000",
        "00000000",
        "00000001",
        "00000000",
        "00000001",
        "00",
        "0000000000000002",
        "0000000000000004",
        "00",
        "03",
    ));
    let decoded = Object::decode(&bytes).unwrap();
    assert_eq!(decoded.encode().unwrap(), bytes);
}

#[test]
fn nonempty_fingerprints_include_store_and_have_unique_ordered_audiences() {
    let mut posted = PostedPositions {
        pending: Vec::new(),
        schema_version: 1,
        device: DeviceId(1),
        writes: WritePositions(vec![test_utils::position()]),
        store_log: EntryPositions(vec![]),
        fingerprints: vec![Fingerprint {
            audience: Audience::Store,
            key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([1; 16])),
            bytes: coven_crypto::Fingerprint::from_bytes([0; 32]),
        }],
    };
    posted.fingerprints.push(Fingerprint {
        audience: Audience::Circle(CircleId(Uuid::from_bytes([1; 16]))),
        key: coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([2; 16])),
        bytes: coven_crypto::Fingerprint::from_bytes([1; 32]),
    });
    let object = Object::PostedPositions(posted.clone());
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    posted.fingerprints.reverse();
    assert!(Object::PostedPositions(posted.clone()).encode().is_err());
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
    posted.fingerprints.reverse();
    posted.fingerprints.push(posted.fingerprints[1].clone());
    assert!(Object::PostedPositions(posted.clone()).encode().is_err());
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
    posted.fingerprints.pop();
    posted.fingerprints.remove(0);
    assert!(Object::PostedPositions(posted.clone()).encode().is_err());
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
    posted.fingerprints.clear();
    let object = Object::PostedPositions(posted);
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
}
