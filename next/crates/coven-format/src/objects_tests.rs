use super::*;
use crate::{encode_frame, test_utils, Object};
use coven_foundation::id_source::CircleId;
use uuid::Uuid;

#[test]
fn files_check_full_partial_empty_and_overflowing_chunk_positions() {
    let header = FileHeader {
        chunk_size: 3,
        total_size: 5,
    };
    header
        .validate_chunk(&FileChunk {
            index: 0,
            bytes: vec![1; 3],
        })
        .unwrap();
    header
        .validate_chunk(&FileChunk {
            index: 1,
            bytes: vec![1; 2],
        })
        .unwrap();
    for (index, length) in [(0, 2), (1, 3), (2, 1), (u64::MAX, 1)] {
        assert!(header
            .validate_chunk(&FileChunk {
                index,
                bytes: vec![1; length]
            })
            .is_err());
    }
    let empty = FileHeader {
        chunk_size: 65_536,
        total_size: 0,
    };
    let object = Object::FileHeader(empty.clone());
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    assert!(empty
        .validate_chunk(&FileChunk {
            index: 0,
            bytes: vec![1]
        })
        .is_err());
    for chunk_size in [0, u32::MAX] {
        assert!(Object::FileHeader(FileHeader {
            chunk_size,
            total_size: 1
        })
        .encode()
        .is_err());
    }
    let max = FileHeader {
        chunk_size: 1,
        total_size: u64::MAX,
    };
    max.validate_chunk(&FileChunk {
        index: u64::MAX - 1,
        bytes: vec![1],
    })
    .unwrap();
}

#[test]
fn posted_fingerprints_include_store_and_have_unique_ordered_audiences() {
    let mut posted = PostedPositions {
        device: DeviceId(1),
        writes: WritePositions(vec![test_utils::position()]),
        store_log: EntryPositions(vec![]),
        fingerprints: vec![Fingerprint {
            audience: Audience::Store,
            key_number: 1,
            bytes: coven_crypto::Fingerprint::from_bytes([0; 32]),
        }],
    };
    posted.fingerprints.push(Fingerprint {
        audience: Audience::Circle(CircleId(Uuid::from_bytes([1; 16]))),
        key_number: 2,
        bytes: coven_crypto::Fingerprint::from_bytes([1; 32]),
    });
    let object = Object::PostedPositions(posted.clone());
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    posted.fingerprints.reverse();
    assert!(Object::decode(&encode_frame(11, &posted).unwrap()).is_err());
    posted.fingerprints.clear();
    assert!(Object::PostedPositions(posted).encode().is_err());
}
