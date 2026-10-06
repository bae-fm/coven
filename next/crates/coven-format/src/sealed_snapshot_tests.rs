use super::*;
use crate::{
    chunks::PlaintextChunks, snapshot::SnapshotRecord, snapshot_stream::SnapshotChunkDecoder,
    test_utils,
};
use coven_crypto::StoreKey;
use coven_foundation::id_source::{IdSource, UuidIds};

#[test]
fn sealed_snapshot_has_its_own_kind_outside_the_plaintext_frames() {
    let prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::from_bytes([1; 16])),
        writes: crate::test_utils::snapshot_header().writes,
        store_log: crate::test_utils::snapshot_header().store_log,
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
        writes: crate::test_utils::snapshot_header().writes,
        store_log: crate::test_utils::snapshot_header().store_log,
    };
    assert_eq!(
        SnapshotObjectPrefix::decode(&prefix.encode().unwrap()).unwrap(),
        prefix
    );
    let keys = key.derive();
    let mut writer = SnapshotObjectLayout::new();
    let mut reader = SnapshotObjectLayout::new();
    let mut snapshot = SnapshotChunkDecoder::new(prefix.audience.clone());
    let mut oracle = test_utils::TestOracle {
        writes: Default::default(),
    };
    let mut records = Vec::new();
    for plain in PlaintextChunks::new(test_utils::chunked_snapshot_frames().into_iter().map(Ok)) {
        let plain = plain.unwrap();
        let sealed = keys
            .seal_object_chunk(
                "snapshots/store/1/1",
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
                "snapshots/store/1/1",
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
            writes: crate::test_utils::snapshot_header().writes,
            store_log: crate::test_utils::snapshot_header().store_log,
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
    let mut layout = SnapshotObjectLayout::new();
    for length in [0u32, (CHUNK_SIZE + 1) as u32, u32::MAX] {
        assert!(layout.chunk_length(&length.to_be_bytes()).is_err());
    }
    let key = StoreKey::generate(KeyId(key_ids.new_id()))
        .unwrap()
        .derive();
    let sealed = key
        .seal_object_chunk("snapshots/store/1/1", b"prefix", 0, 0, b"bytes")
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
fn positions_are_canonical_and_each_list_has_its_own_bound() {
    use crate::value::{EntryId, EntryPositions, WritePositions};
    use coven_foundation::id_source::DeviceId;
    use coven_merge::WriteId;
    let mut prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::nil()),
        writes: WritePositions(
            (0..MAX_ITEMS as u64)
                .map(|i| WriteId {
                    device: DeviceId(i),
                    number: 1,
                })
                .collect(),
        ),
        store_log: EntryPositions(
            (0..MAX_ITEMS as u64)
                .map(|i| EntryId {
                    device: DeviceId(i),
                    number: 1,
                })
                .collect(),
        ),
    };
    let bytes = prefix.encode().unwrap();
    assert_eq!(SnapshotObjectPrefix::decode(&bytes).unwrap(), prefix);
    prefix.writes.0.push(WriteId {
        device: DeviceId(MAX_ITEMS as u64),
        number: 1,
    });
    assert!(prefix.encode().is_err());
    prefix.writes.0.truncate(2);
    prefix.store_log.0.truncate(2);
    let bytes = prefix.encode().unwrap();
    let mut bad = bytes.clone();
    bad[20..24].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        SnapshotObjectPrefix::decode(&bad),
        Err(Error::Limit { .. })
    ));
    for offset in [24, 60] {
        let mut bad = bytes.clone();
        bad[offset..offset + 16].copy_from_slice(&bytes[offset + 16..offset + 32]);
        assert!(SnapshotObjectPrefix::decode(&bad).is_err());
        let mut bad = bytes.clone();
        bad[offset + 8..offset + 16].fill(0);
        assert!(SnapshotObjectPrefix::decode(&bad).is_err());
    }
}
