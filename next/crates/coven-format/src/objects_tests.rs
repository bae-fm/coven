use super::*;
use crate::{encode_frame, test_utils, Object};
use coven_foundation::id_source::CircleId;
use uuid::Uuid;

#[test]
fn posted_fingerprints_include_store_and_have_unique_ordered_audiences() {
    let mut posted = PostedPositions {
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
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
    posted.fingerprints.clear();
    assert!(Object::PostedPositions(posted).encode().is_err());
}
