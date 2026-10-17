use super::*;
use crate::chunks::CHUNK_SIZE;
use crate::sealed_stream::StreamObjectLayout;
use crate::{
    chunks::PlaintextChunks, snapshot::SnapshotRecord, snapshot_stream::SnapshotChunkDecoder,
    test_utils,
};
use coven_crypto::StoreKey;
use coven_foundation::id_source::{IdSource, UuidIds};

#[test]
fn listed_snapshot_size_recovers_plaintext_at_chunk_boundaries() {
    let prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::from_bytes([1; 16])),
        covers: 9,
        purpose: Purpose::Ordinary,
    };
    let key = StoreKey::generate(prefix.key).unwrap().derive();
    let clear = prefix.encode().unwrap();
    for size in [
        1,
        CHUNK_SIZE - 1,
        CHUNK_SIZE,
        CHUNK_SIZE + 1,
        CHUNK_SIZE * 3,
    ] {
        let mut layout = StreamObjectLayout::new();
        let mut stored = clear.len() + 128;
        for chunk in vec![7; size].chunks(CHUNK_SIZE) {
            let bytes = key
                .seal_object_chunk(
                    "10101010-1010-1010-1010-101010101010/snapshots/store/1/1",
                    &clear,
                    0,
                    layout.index(),
                    chunk,
                )
                .unwrap();
            stored += layout.encode_chunk(&bytes).unwrap().len();
        }
        assert_eq!(prefix.plaintext_length(stored as u64).unwrap(), size as u64);
    }
    for section in (0..=44).chain((CHUNK_SIZE + 45)..=(CHUNK_SIZE + 88)) {
        assert!(prefix
            .plaintext_length((clear.len() + 128 + section) as u64)
            .is_err());
    }
    assert!(prefix.plaintext_length(0).is_err());
}

#[test]
fn sealed_snapshot_has_its_own_kind_outside_the_plaintext_frames() {
    let prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::from_bytes([1; 16])),
        covers: 9,
        purpose: Purpose::Ordinary,
    };
    let mut bytes = prefix.encode().unwrap();
    assert!(crate::frame_length(&bytes).is_err());
    assert!(crate::sealed_write::WriteObjectPrefix::decode(&bytes).is_err());
    for kind in (1..=5).chain(7..=13).chain([14]) {
        bytes[0] = kind;
        assert!(SnapshotObjectPrefix::decode(&bytes).is_err(), "kind {kind}");
    }
}

#[test]
fn a_snapshot_is_sealed_opened_and_applied_a_chunk_at_a_time() {
    let key_ids = UuidIds;
    let key = StoreKey::generate(KeyId(key_ids.new_id())).unwrap();
    let prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: key.id(),
        covers: 9,
        purpose: Purpose::Ordinary,
    };
    assert_eq!(
        SnapshotObjectPrefix::decode(&prefix.encode().unwrap()).unwrap(),
        prefix
    );
    let keys = key.derive();
    let mut writer = StreamObjectLayout::new();
    let mut reader = StreamObjectLayout::new();
    let mut snapshot = SnapshotChunkDecoder::new(prefix.clone());
    let mut oracle = test_utils::TestOracle {
        writes: Default::default(),
    };
    let mut records = Vec::new();
    for plain in PlaintextChunks::new(test_utils::chunked_snapshot_frames().into_iter().map(Ok)) {
        let plain = plain.unwrap();
        let sealed = keys
            .seal_object_chunk(
                "10101010-1010-1010-1010-101010101010/snapshots/store/1/1",
                &prefix.encode().unwrap(),
                0,
                writer.index(),
                &plain,
            )
            .unwrap();
        let piece = writer.encode_chunk(&sealed).unwrap();
        assert_eq!(reader.chunk_length(&piece[..4]).unwrap(), piece.len());
        let index = reader.index();
        let sealed = reader.decode_chunk(&piece).unwrap();
        let opened = keys
            .open_object_chunk(
                "10101010-1010-1010-1010-101010101010/snapshots/store/1/1",
                &prefix.encode().unwrap(),
                0,
                index,
                sealed,
            )
            .unwrap();
        let mut bytes = opened.as_slice();
        while let Some(record) = snapshot.next_record(&mut bytes, &oracle).unwrap() {
            if let SnapshotRecord::Write(write) = &record {
                oracle.writes.insert(write.id, write.clone());
            }
            records.push(record);
        }
        assert!(bytes.is_empty());
    }
    snapshot.finish().unwrap();
    assert_eq!(records.len(), test_utils::snapshot_records().len());
    assert!(reader.index() > 1);
    assert!(reader.chunk_length(&45u32.to_be_bytes()).is_err());
}

#[test]
fn prefix_and_chunk_lengths_are_bounded_and_have_no_aliases() {
    let key_ids = UuidIds;
    for audience in [
        Audience::Store,
        Audience::Circle(coven_crypto::CircleId(uuid::Uuid::from_u128(1))),
    ] {
        let prefix = SnapshotObjectPrefix {
            audience,
            key: KeyId(uuid::Uuid::from_bytes([0xab; 16])),
            covers: 9,
            purpose: Purpose::Ordinary,
        };
        let mut bytes = prefix.encode().unwrap();
        assert_eq!(SnapshotObjectPrefix::length(&bytes).unwrap(), bytes.len());
        assert_eq!(SnapshotObjectPrefix::decode(&bytes).unwrap(), prefix);
        for end in 0..bytes.len() {
            assert!(SnapshotObjectPrefix::decode(&bytes[..end]).is_err());
        }
        bytes.push(0);
        assert_eq!(
            SnapshotObjectPrefix::decode(&bytes),
            Err(Error::TrailingBytes)
        );
    }
    let mut layout = StreamObjectLayout::new();
    for length in [0u32, (CHUNK_SIZE + 1) as u32, u32::MAX] {
        assert!(layout.chunk_length(&length.to_be_bytes()).is_err());
    }
    let key = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let sealed = key
        .seal_object_chunk(
            "10101010-1010-1010-1010-101010101010/snapshots/store/1/1",
            b"prefix",
            0,
            0,
            b"bytes",
        )
        .unwrap();
    let mut piece = sealed::encode_chunk(&sealed).unwrap();
    for end in 0..piece.len() {
        assert!(layout.decode_chunk(&piece[..end]).is_err());
    }
    piece.push(0);
    assert!(layout.decode_chunk(&piece).is_err());
    piece.pop();
    layout.decode_chunk(&piece).unwrap();
    assert!(layout.decode_chunk(&piece).is_err());
}

#[test]
fn a_raise_snapshot_must_hold_the_schema_it_raises_to() {
    let header = test_utils::snapshot_header();
    let mut encoder_header = header.clone();
    encoder_header.schema_version = 3;
    let (_, frame) = crate::snapshot::SnapshotEncoder::start(encoder_header).unwrap();
    let prefix = test_utils::snapshot_prefix(&header);
    assert!(matches!(
        crate::snapshot::SnapshotDecoder::start(&frame, &prefix),
        Err(Error::Invalid {
            rule: crate::error::Rule::RaiseVersion,
            ..
        })
    ));
    let ordinary = SnapshotObjectPrefix {
        purpose: Purpose::Ordinary,
        ..prefix
    };
    assert!(crate::snapshot::SnapshotDecoder::start(&frame, &ordinary).is_ok());
}

#[test]
fn coverage_and_purpose_are_in_the_signed_prefix() {
    let base = test_utils::snapshot_prefix(&test_utils::snapshot_header());
    for audience in [
        Audience::Store,
        Audience::Circle(coven_crypto::CircleId(uuid::Uuid::from_u128(1))),
    ] {
        for purpose in [Purpose::Ordinary, Purpose::Raise(u32::MAX), Purpose::Reset] {
            let prefix = SnapshotObjectPrefix {
                audience: audience.clone(),
                purpose,
                covers: u64::MAX,
                ..base.clone()
            };
            let bytes = prefix.encode().unwrap();
            let offset = if audience == Audience::Store { 20 } else { 36 };
            assert_eq!(&bytes[offset..offset + 8], &u64::MAX.to_be_bytes());
            assert_eq!(SnapshotObjectPrefix::decode(&bytes).unwrap(), prefix);
            let mut signed = bytes.clone();
            signed.extend([0; 64]);
            assert_eq!(
                SnapshotObjectPrefix::signed_length(&signed).unwrap(),
                signed.len()
            );
            assert!(signed.len() <= SnapshotObjectPrefix::MAX_SIGNED_PREFIX);
            let mut bad = bytes;
            bad[offset + 8] = 3;
            assert!(SnapshotObjectPrefix::decode(&bad).is_err());
        }
    }
    assert!(SnapshotObjectPrefix {
        purpose: Purpose::Raise(0),
        ..base
    }
    .encode()
    .is_err());
}
