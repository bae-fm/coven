use super::*;
use crate::{encode_frame, test_utils, Object};
use coven_foundation::id_source::CircleId;
use uuid::Uuid;

#[test]
fn posted_fingerprints_include_store_and_have_unique_ordered_audiences() {
    let mut posted = PostedPositions {
        stuck: Vec::new(),
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

#[test]
fn stuck_records_are_typed_and_unique_per_ordered_log() {
    use crate::stuck::{LogObject, StuckFailure, StuckRecord};
    let mut posted = crate::test_utils::objects()
        .into_iter()
        .find_map(|object| match object {
            Object::PostedPositions(posted) => Some(posted),
            _ => None,
        })
        .unwrap();
    for failure in [
        StuckFailure::Decryption,
        StuckFailure::Signature,
        StuckFailure::Parse,
        StuckFailure::InvalidWrite,
    ] {
        posted.stuck[0].failure = failure;
        let object = Object::PostedPositions(posted.clone());
        assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    }
    let record = posted.stuck[0];
    posted.stuck.insert(0, record);
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
    posted.stuck.remove(0);
    posted.stuck.reverse();
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
    posted.stuck = vec![StuckRecord {
        object: LogObject::Write(coven_merge::WriteId {
            device: DeviceId(2),
            number: 0,
        }),
        failure: StuckFailure::Parse,
    }];
    assert!(Object::decode(&encode_frame(8, &posted).unwrap()).is_err());
}
