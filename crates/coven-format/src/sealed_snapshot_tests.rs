use super::*;
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
        caught_at: test_utils::caught_at(),
        raise: None,
        writes: crate::test_utils::snapshot_header().writes,
        store_log: crate::test_utils::snapshot_header().store_log,
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
        let mut layout = SnapshotObjectLayout::new();
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
fn signed_prefix_length_bounds_each_count() {
    for audience in [
        Audience::Store,
        Audience::Circle(coven_crypto::CircleId(uuid::Uuid::from_u128(1))),
    ] {
        let prefix = SnapshotObjectPrefix {
            audience,
            key: KeyId(uuid::Uuid::nil()),
            caught_at: test_utils::caught_at(),
            raise: None,
            writes: crate::test_utils::snapshot_header().writes,
            store_log: crate::test_utils::snapshot_header().store_log,
        };
        let mut signed = prefix.encode().unwrap();
        signed.extend([0; 64]);
        assert_eq!(
            SnapshotObjectPrefix::signed_length(&signed).unwrap(),
            signed.len()
        );
        let first = SnapshotObjectPrefix::routing_length(&signed).unwrap();
        let second = positions_end(&signed, first).unwrap();
        for offset in [first, second] {
            let mut bad = signed[..offset + 4].to_vec();
            bad[offset..].copy_from_slice(&u32::MAX.to_be_bytes());
            assert!(matches!(
                SnapshotObjectPrefix::signed_length(&bad),
                Err(Error::Limit { .. })
            ));
        }
    }
}

#[test]
fn sealed_snapshot_has_its_own_kind_outside_the_plaintext_frames() {
    let prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::from_bytes([1; 16])),
        caught_at: test_utils::caught_at(),
        raise: None,
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
        caught_at: test_utils::caught_at(),
        raise: None,
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
            caught_at: test_utils::caught_at(),
            raise: None,
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
fn positions_are_canonical_and_each_list_has_its_own_bound() {
    use crate::value::{EntryId, EntryPositions, WritePositions};
    use coven_foundation::id_source::DeviceId;
    use coven_merge::WriteId;
    let mut prefix = SnapshotObjectPrefix {
        audience: Audience::Store,
        key: KeyId(uuid::Uuid::nil()),
        caught_at: test_utils::caught_at(),
        raise: None,
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
    // The absent raise is one byte after the catch-up time.
    assert_eq!(bytes[32], 0);
    let mut bad = bytes.clone();
    bad[33..37].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        SnapshotObjectPrefix::decode(&bad),
        Err(Error::Limit { .. })
    ));
    for offset in [37, 73] {
        let mut bad = bytes.clone();
        bad[offset..offset + 16].copy_from_slice(&bytes[offset + 16..offset + 32]);
        assert!(SnapshotObjectPrefix::decode(&bad).is_err());
        let mut bad = bytes.clone();
        bad[offset + 8..offset + 16].fill(0);
        assert!(SnapshotObjectPrefix::decode(&bad).is_err());
    }
}

#[test]
fn caught_at_follows_the_key_and_keeps_its_subsecond_sample() {
    use crate::value::StorageTime;
    let prefix = test_utils::snapshot_prefix(&test_utils::snapshot_header());
    let bytes = prefix.encode().unwrap();
    // kind, version, store audience and key, then seconds and nanoseconds.
    assert_eq!(&bytes[20..28], &1_760_400_000u64.to_be_bytes());
    assert_eq!(&bytes[28..32], &123_456_789u32.to_be_bytes());
    assert_eq!(SnapshotObjectPrefix::decode(&bytes).unwrap(), prefix);
    // Windows SystemTime counts 100 ns intervals, so these samples use whole
    // hundreds of nanoseconds to mean the same instant on every platform.
    let sample = std::time::UNIX_EPOCH + std::time::Duration::new(1_760_400_000, 123_456_700);
    assert_eq!(
        StorageTime::from_system_time(sample).unwrap(),
        StorageTime::new(1_760_400_000, 123_456_700).unwrap()
    );
    assert!(StorageTime::from_system_time(
        std::time::UNIX_EPOCH - std::time::Duration::from_nanos(100)
    )
    .is_err());
    for end in 20..32 {
        assert_eq!(
            SnapshotObjectPrefix::decode(&bytes[..end]),
            Err(Error::Truncated)
        );
    }
}

#[test]
fn a_fraction_of_one_second_or_more_is_refused() {
    let bytes = test_utils::snapshot_prefix(&test_utils::snapshot_header())
        .encode()
        .unwrap();
    for (nanos, valid) in [
        (999_999_999u32, true),
        (1_000_000_000, false),
        (u32::MAX, false),
    ] {
        let mut changed = bytes.clone();
        changed[28..32].copy_from_slice(&nanos.to_be_bytes());
        let decoded = SnapshotObjectPrefix::decode(&changed);
        if valid {
            assert_eq!(decoded.unwrap().encode().unwrap(), changed);
        } else {
            assert_eq!(
                decoded,
                Err(Error::Invalid {
                    field: "storage time",
                    rule: crate::error::Rule::Nanoseconds,
                })
            );
        }
    }
    assert!(crate::value::StorageTime::new(0, 1_000_000_000).is_err());
}

#[test]
fn counting_admits_exactly_one_day_and_never_rounds_forward() {
    use crate::value::StorageTime;
    let time = |seconds, nanos| StorageTime::new(seconds, nanos).unwrap();
    let mut prefix = test_utils::snapshot_prefix(&test_utils::snapshot_header());
    prefix.caught_at = time(100, 500);
    for (landed, counts) in [
        (time(0, 0), true),
        (time(100, 500), true),
        (time(86_500, 499), true),
        (time(86_500, 500), true),
        (time(86_500, 501), false),
        (time(86_501, 0), false),
    ] {
        assert_eq!(prefix.counts(landed), counts, "{landed:?}");
    }
    // `caught_at = (100, 0)` admits (86500, 0) but not (86501, 0) (D9).
    prefix.caught_at = time(100, 0);
    assert!(prefix.counts(time(86_500, 0)));
    assert!(!prefix.counts(time(86_500, 1)));
    // A deadline past the representable range admits every representable time.
    prefix.caught_at = time(u64::MAX - 1, 0);
    assert!(prefix.counts(time(u64::MAX, 999_999_999)));
    prefix.caught_at = time(u64::MAX - StorageTime::DAY_SECONDS, 7);
    assert!(prefix.counts(time(u64::MAX, 7)));
    assert!(!prefix.counts(time(u64::MAX, 8)));
}

#[test]
fn a_raise_snapshot_signs_its_version_into_the_prefix() {
    let header = test_utils::snapshot_header();
    let prefix = test_utils::snapshot_prefix(&header);
    assert_eq!(prefix.raise.map(|v| v.get()), Some(header.schema_version));
    let bytes = prefix.encode().unwrap();
    // After the catch-up time: present, then the raised version.
    assert_eq!(&bytes[32..37], &[1, 0, 0, 0, 2]);
    assert_eq!(SnapshotObjectPrefix::decode(&bytes).unwrap(), prefix);
    for end in 32..37 {
        assert_eq!(
            SnapshotObjectPrefix::decode(&bytes[..end]),
            Err(Error::Truncated)
        );
    }
    let mut zero = bytes.clone();
    zero[33..37].fill(0);
    assert_eq!(
        SnapshotObjectPrefix::decode(&zero),
        Err(Error::Invalid {
            field: "snapshot raise version",
            rule: crate::error::Rule::Required,
        })
    );
    let mut tag = bytes.clone();
    tag[32] = 2;
    assert!(matches!(
        SnapshotObjectPrefix::decode(&tag),
        Err(Error::UnknownTag { .. })
    ));
    let ordinary = SnapshotObjectPrefix {
        raise: None,
        ..prefix.clone()
    };
    let encoded = ordinary.encode().unwrap();
    assert_eq!(encoded.len() + 4, bytes.len());
    assert_eq!(SnapshotObjectPrefix::decode(&encoded).unwrap(), ordinary);
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
        raise: None,
        ..prefix
    };
    assert!(crate::snapshot::SnapshotDecoder::start(&frame, &ordinary).is_ok());
}
