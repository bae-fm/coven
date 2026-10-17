use super::*;
use crate::sealed_single::SingleChunkObject;
use crate::sealed_snapshot::SnapshotObjectPrefix;
use crate::sealed_stream::StreamObjectLayout;
use crate::sealed_write::{ObjectChunk, WriteObjectLayout, WriteObjectPrefix};
use crate::snapshot::SnapshotRecord;
use crate::snapshot_stream::SnapshotChunkDecoder;
use crate::tests::mutations;
use crate::write_stream::{PartDecoder, WriteEncoder, WriteHeaderFrame};
use crate::{test_utils, tests::hex, Object};
use coven_crypto::{CircleKey, ObjectHasher, Signature, StoreKey};
use coven_foundation::id_source::{CircleId, KeyId};
use uuid::Uuid;

const WRITE_PATH: &str = "coven/write/10101010-1010-1010-1010-101010101010/1/3";
const SNAPSHOT_PATH: &str = "10101010-1010-1010-1010-101010101010/snapshots/store/1/1";

fn store_key() -> StoreKey {
    StoreKey::from_bytes(KeyId(Uuid::from_bytes([1; 16])), [17; 32])
}
fn circle_key() -> CircleKey {
    CircleKey::from_bytes(
        CircleId(Uuid::from_u128(1)),
        KeyId(Uuid::from_bytes([2; 16])),
        [18; 32],
    )
}
fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], Error> {
    let piece = bytes.get(..count).ok_or(Error::Truncated)?;
    *bytes = &bytes[count..];
    Ok(piece)
}

// Drive the real layout cursor with its authenticated-header input. Ciphertext
// remains opaque here: this exercises every envelope mutation without repeatedly
// opening unchanged chunks. The fixture tests below open every chunk and signature.
fn walk_write(
    mut bytes: &[u8],
    header: &[u8],
    lengths: &[u64],
    mut visit: impl FnMut(ObjectChunk, &[u8]),
) -> Result<(crate::sealed_write::HeaderTail, Signature), Error> {
    let length = WriteObjectPrefix::length(bytes)?;
    let prefix_bytes = take(&mut bytes, length)?;
    let prefix = WriteObjectPrefix::decode(prefix_bytes)?;
    assert_eq!(prefix.encode()?, prefix_bytes);
    let mut writer = WriteObjectLayout::new(prefix.clone(), header, lengths.to_vec())?;
    let mut layout = WriteObjectLayout::new(prefix.clone(), header, lengths.to_vec())?;
    let mut tail = None;
    loop {
        while layout.next_chunk().is_some() {
            let length = layout.chunk_length(bytes)?;
            let piece = take(&mut bytes, length)?;
            let (coordinate, sealed) = layout.decode_chunk(piece)?;
            assert_eq!(writer.encode_chunk(sealed)?, piece);
            visit(coordinate, sealed);
        }
        if tail.is_some() {
            break;
        }
        // The roots and header signature follow the header chunk.
        let piece = take(
            &mut bytes,
            WriteObjectPrefix::tail_length(prefix.part_keys.len()),
        )?;
        let decoded = prefix.decode_tail(piece)?;
        assert_eq!(writer.tail(&decoded)?, piece);
        layout.tail(&decoded)?;
        tail = Some(decoded);
    }
    let signature = layout.read_signature(bytes)?;
    assert_eq!(writer.signature(&signature)?, bytes);
    layout.finish(&[])?;
    writer.finish(&[])?;
    Ok((tail.expect("read the tail"), signature))
}

fn walk_snapshot(
    mut bytes: &[u8],
    mut visit: impl FnMut(u64, &[u8]),
) -> Result<(SnapshotObjectPrefix, Signature, Signature), Error> {
    let length = SnapshotObjectPrefix::length(bytes)?;
    let prefix_bytes = take(&mut bytes, length)?;
    let prefix = SnapshotObjectPrefix::decode(prefix_bytes)?;
    assert_eq!(prefix.encode()?, prefix_bytes);
    let prefix_signature = crate::sealed::signature(take(&mut bytes, 64)?)?;
    let mut layout = StreamObjectLayout::new();
    let mut writer = StreamObjectLayout::new();
    while bytes.len() > 64 {
        let length = layout.chunk_length(bytes)?;
        let index = layout.index();
        let piece = take(&mut bytes, length)?;
        let sealed = layout.decode_chunk(piece)?;
        assert_eq!(writer.encode_chunk(sealed)?, piece);
        visit(index, sealed);
    }
    Ok((prefix, prefix_signature, layout.read_signature(bytes)?))
}

// These bytes were produced independently with Python hashlib/hmac and libsodium.
// Store/circle key bytes are 0x11/0x12, signing seeds 0x33, and all nonces are
// public test material. Writes and entries use D11's content-bound nonces;
// fixtures/generate-deterministic.py reproduces them from plaintext fixtures.
#[test]
fn sealed_write_fixture_opens_both_parts_and_reseals_identically() {
    let bytes = hex(include_str!("../fixtures/sealed-write.hex"));
    let prefix_length = WriteObjectPrefix::length(&bytes).unwrap();
    let prefix_bytes = &bytes[..prefix_length];
    let prefix = WriteObjectPrefix::decode(prefix_bytes).unwrap();
    let chunk_length = WriteObjectPrefix::header_chunk_length(&bytes[prefix_length..]).unwrap();
    let sealed_header =
        WriteObjectPrefix::header_chunk(&bytes[prefix_length..prefix_length + chunk_length])
            .unwrap();
    let header_bytes = store_key()
        .derive()
        .open_object_chunk(WRITE_PATH, prefix_bytes, 0, 0, sealed_header)
        .unwrap();
    let header = WriteHeaderFrame::decode(&header_bytes).unwrap();
    // The row change carries the 128 KiB value once; its one operation is
    // that same change, written without repeating it.
    assert_eq!(header.parts[0].chunk_count(), 3);
    assert_eq!(header.parts.len(), 2);
    let lengths: Vec<_> = header.parts.iter().map(|p| p.plaintext_length).collect();
    let mut writer =
        WriteObjectLayout::new(prefix.clone(), &header_bytes, lengths.clone()).unwrap();
    let mut encoded = writer.prefix().unwrap();
    let mut decoders: Vec<_> = header
        .parts
        .iter()
        .cloned()
        .map(|p| PartDecoder::new(p).unwrap())
        .collect();
    let mut rows = [Vec::new(), Vec::new()];
    let (tail, signature) = walk_write(&bytes, &header_bytes, &lengths, |coordinate, sealed| {
        encoded.extend(writer.encode_chunk(sealed).unwrap());
        if coordinate.section == 0 {
            let start = encoded.len();
            let tail = prefix
                .decode_tail(&bytes[start..start + WriteObjectPrefix::tail_length(2)])
                .unwrap();
            encoded.extend(writer.tail(&tail).unwrap());
        }
        let keys = match coordinate.section {
            0 | 1 => {
                assert_eq!(coordinate.key, store_key().id());
                store_key().derive()
            }
            2 => {
                assert_eq!(coordinate.key, circle_key().id());
                circle_key().derive()
            }
            _ => panic!("unexpected section"),
        };
        let opened = keys
            .open_object_chunk(
                WRITE_PATH,
                prefix_bytes,
                coordinate.section,
                coordinate.index,
                sealed,
            )
            .unwrap();
        assert_eq!(
            keys.reseal_object_chunk(
                WRITE_PATH,
                prefix_bytes,
                coordinate.section,
                coordinate.index,
                &opened,
            ),
            sealed,
        );
        if coordinate.section == 0 {
            assert_eq!(opened, header_bytes);
        } else {
            let part = coordinate.section as usize - 1;
            rows[part].extend(decoders[part].chunk(&opened).unwrap());
        }
    })
    .unwrap();
    for decoder in &decoders {
        decoder.finish().unwrap();
    }
    let expected = test_utils::chunked_write();
    for (rows, part) in rows.iter().zip(&expected.parts) {
        assert_eq!(
            rows,
            &part
                .rows
                .iter()
                .cloned()
                .map(crate::dismissal::WriteFrame::Change)
                .chain(
                    part.executions
                        .iter()
                        .cloned()
                        .map(crate::dismissal::WriteFrame::Execution)
                )
                .collect::<Vec<_>>()
        );
    }
    // Python's independently parsed leaves and keyed tree give the roots the
    // header signature covers.
    for ((part, root), hasher) in expected.parts.iter().zip(&tail.roots).zip([
        store_key().derive().authorship_hasher(),
        circle_key().derive().authorship_hasher(),
    ]) {
        assert_eq!(
            crate::authorship::root(&hasher, &crate::authorship::part_leaves(part)).unwrap(),
            *root
        );
    }
    let header_chunk = &bytes[prefix_length..prefix_length + chunk_length];
    test_utils::member()
        .signing
        .verify_write_header(
            WRITE_PATH,
            prefix_bytes,
            header_chunk,
            &tail.roots,
            &tail.signature,
        )
        .unwrap();
    encoded.extend(writer.signature(&signature).unwrap());
    writer.finish(&[]).unwrap();
    assert_eq!(encoded, bytes);
    let mut hash = ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    test_utils::member()
        .signing
        .verify_object(WRITE_PATH, &hash.finish(), &signature)
        .unwrap();
    assert!(store_key()
        .derive()
        .open_object_chunk(
            "coven/write/10101010-1010-1010-1010-101010101010/1/4",
            prefix_bytes,
            0,
            0,
            sealed_header
        )
        .is_err());
    mutations(&bytes, |changed| {
        let _decoded = walk_write(changed, &header_bytes, &lengths, |_, _| {});
    });
}

#[test]
fn snapshot_fixture_binds_coverage_and_requires_its_plaintext_end_marker() {
    let bytes = hex(include_str!("../fixtures/sealed-snapshot.hex"));
    let prefix_length = SnapshotObjectPrefix::length(&bytes).unwrap();
    let prefix_bytes = &bytes[..prefix_length];
    let (prefix, prefix_signature) =
        SnapshotObjectPrefix::decode_signed(&bytes[..prefix_length + 64]).unwrap();
    test_utils::member()
        .signing
        .verify_prefix(SNAPSHOT_PATH, prefix_bytes, &prefix_signature)
        .unwrap();
    let mut reader = SnapshotChunkDecoder::new(prefix.clone());
    let mut oracle = test_utils::TestOracle {
        writes: Default::default(),
    };
    let mut writer = StreamObjectLayout::new();
    let mut encoded = prefix.encode().unwrap();
    encoded.extend_from_slice(prefix_signature.as_bytes());
    let mut plaintext = Vec::new();
    let mut count = 0;
    let (_, _, signature) = walk_snapshot(&bytes, |index, sealed| {
        encoded.extend(writer.encode_chunk(sealed).unwrap());
        let opened = store_key()
            .derive()
            .open_object_chunk(SNAPSHOT_PATH, prefix_bytes, 0, index, sealed)
            .unwrap();
        plaintext.extend_from_slice(&opened);
        let mut remaining = opened.as_slice();
        while let Some(record) = reader.next_record(&mut remaining, &oracle).unwrap() {
            if let SnapshotRecord::Write(write) = record {
                oracle.writes.insert(write.id, write);
            }
            count += 1;
        }
        assert!(remaining.is_empty());
        if index == 0 {
            assert!(reader.finish().is_err());
        }
    })
    .unwrap();
    let mut hash = ObjectHasher::new();
    hash.update(&encoded);
    test_utils::member()
        .signing
        .verify_object(SNAPSHOT_PATH, &hash.finish(), &signature)
        .unwrap();
    encoded.extend_from_slice(signature.as_bytes());
    reader.finish().unwrap();
    assert_eq!(reader.header().unwrap().covers, prefix.covers);
    assert_eq!(count, test_utils::snapshot_records().len());
    assert_eq!(plaintext, test_utils::chunked_snapshot_frames().concat());
    assert_eq!(encoded, bytes);
    mutations(&bytes, |changed| {
        let _decoded = walk_snapshot(changed, |_, _| {});
    });
}

#[test]
fn join_fixture_opens_with_the_invite_key_and_requested_signer() {
    let bytes = hex(include_str!("../fixtures/sealed-join-request.hex"));
    let path =
        "10101010-1010-1010-1010-101010101010/join-requests/55555555-5555-5555-5555-555555555555";
    let object = SingleChunkObject::decode(&bytes).unwrap();
    let prefix = object.prefix().encode().unwrap();
    let plain = test_utils::invite()
        .secret
        .join_request_key()
        .open_object_chunk(path, &prefix, 0, 0, object.chunk())
        .unwrap();
    assert_eq!(
        Object::decode(&plain).unwrap(),
        Object::JoinRequest(test_utils::join_request())
    );
    let mut hash = ObjectHasher::new();
    hash.update(&object.signed_bytes().unwrap());
    test_utils::replacement()
        .signing
        .verify_object(path, &hash.finish(), object.signature())
        .unwrap();
    assert_eq!(object.encode().unwrap(), bytes);
    mutations(&bytes, |changed| {
        if let Ok(object) = SingleChunkObject::decode(changed) {
            assert_eq!(object.encode().unwrap(), changed);
        }
    });
}

#[test]
fn snapshot_fixture_signatures_reject_truncation_and_every_changed_bit() {
    let bytes = hex(include_str!("../fixtures/sealed-snapshot.hex"));
    let length = SnapshotObjectPrefix::length(&bytes).unwrap();
    let author = test_utils::member().signing;
    let verify_prefix = |bytes: &[u8]| -> Result<(), Box<dyn std::error::Error>> {
        let (_, signature) = SnapshotObjectPrefix::decode_signed(bytes)?;
        author.verify_prefix(SNAPSHOT_PATH, &bytes[..length], &signature)?;
        Ok(())
    };
    let signed = &bytes[..length + 64];
    verify_prefix(signed).unwrap();
    for end in 0..signed.len() {
        assert!(verify_prefix(&signed[..end]).is_err());
    }
    let mut changed = signed.to_vec();
    for index in 0..changed.len() {
        for bit in 0..8 {
            changed[index] ^= 1 << bit;
            assert!(verify_prefix(&changed).is_err());
            changed[index] ^= 1 << bit;
        }
    }
    let (_, prefix_signature, signature) = walk_snapshot(&bytes, |_, _| {}).unwrap();
    assert!(author
        .verify_prefix(
            "10101010-1010-1010-1010-101010101010/snapshots/store/2/1",
            &bytes[..length],
            &prefix_signature
        )
        .is_err());
    let mut hash = ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    let digest = hash.finish();
    author
        .verify_object(SNAPSHOT_PATH, &digest, &signature)
        .unwrap();
    for end in bytes.len() - 64..bytes.len() {
        assert!(walk_snapshot(&bytes[..end], |_, _| {}).is_err());
    }
    let mut changed = *signature.as_bytes();
    for index in 0..changed.len() {
        for bit in 0..8 {
            changed[index] ^= 1 << bit;
            assert!(author
                .verify_object(SNAPSHOT_PATH, &digest, &Signature::from_bytes(changed))
                .is_err());
            changed[index] ^= 1 << bit;
        }
    }
    // The whole-object digest includes the prefix signature, not only the chunks.
    let mut changed = bytes[..bytes.len() - 64].to_vec();
    changed[length] ^= 1;
    let mut hash = ObjectHasher::new();
    hash.update(&changed);
    assert!(author
        .verify_object(SNAPSHOT_PATH, &hash.finish(), &signature)
        .is_err());
}

#[test]
fn opaque_write_layout_uses_supplied_frame_lengths() {
    let record = test_utils::chunked_write();
    let encoder = WriteEncoder::new(&record).unwrap();
    let lengths: Vec<_> = encoder
        .header()
        .parts
        .iter()
        .map(|p| p.plaintext_length)
        .collect();
    let prefix = WriteObjectPrefix {
        format: crate::FormatVersion::CURRENT,
        store_key: store_key().id(),
        part_keys: vec![store_key().id(), circle_key().id()],
    };
    // Another plaintext codec can supply frame bytes of the same declared length;
    // the sealed layer has no knowledge of that codec's tags or field ordering.
    let opaque_header = vec![0x99; encoder.header_frame().len()];
    assert!(WriteObjectLayout::new(prefix, &opaque_header, lengths).is_ok());
}

mod sequence {
    use crate::{
        chunks::FrameDecoder,
        floor::FloorFrames,
        sealed_floor::FloorPrefixDecoder,
        sealed_slot::{SlotKind, SlotObjectPrefix},
        sealed_stream::StreamObjectLayout,
        test_utils as f,
        tests::hex,
        writes_slot::{BatchPiece, WritesSlotDecoder},
    };
    use coven_crypto::ObjectHasher;

    fn open_stream(
        bytes: &[u8],
        prefix_length: usize,
        path: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        open_stream_parts(
            bytes,
            prefix_length,
            prefix_length,
            path,
            &f::store_key().derive(),
        )
    }

    fn open_stream_parts(
        bytes: &[u8],
        prefix_length: usize,
        body_start: usize,
        path: &str,
        keys: &coven_crypto::DerivedKeys,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let prefix = bytes.get(..prefix_length).ok_or(crate::Error::Truncated)?;
        let mut input = &bytes[body_start..];
        let mut layout = StreamObjectLayout::new();
        let mut plain = Vec::new();
        while input.len() > 64 {
            let length = layout.chunk_length(input)?;
            let piece = input.get(..length).ok_or(crate::Error::Truncated)?;
            let index = layout.index();
            let sealed = layout.decode_chunk(piece)?;
            plain.extend(keys.open_object_chunk(path, prefix, 0, index, sealed)?);
            input = &input[length..];
        }
        let signature = layout.read_signature(input)?;
        let mut hash = ObjectHasher::new();
        hash.update(&bytes[..bytes.len() - 64]);
        f::member()
            .signing
            .verify_object(path, &hash.finish(), &signature)?;
        Ok(plain)
    }

    #[test]
    fn floor_fixture_authenticates_original_copies_announcements_and_coverage() {
        let bytes = hex(include_str!("../fixtures/sealed-floor.hex"));
        let mut routing =
            FloorPrefixDecoder::start(&bytes[..FloorPrefixDecoder::HEADER_LENGTH]).unwrap();
        assert_eq!(routing.key, f::store_key().id());
        let mut prefix_length = FloorPrefixDecoder::HEADER_LENGTH;
        let mut copies = Vec::new();
        while routing.remaining() != 0 {
            let length = routing.copy_length(&bytes[prefix_length..]).unwrap();
            copies.push(
                routing
                    .copy(&bytes[prefix_length..prefix_length + length])
                    .unwrap(),
            );
            prefix_length += length;
        }
        routing.finish().unwrap();
        assert_eq!(copies.len(), 2);
        assert_eq!(copies[1].slot.0, 8);
        assert_eq!(
            copies[1].object,
            hex(include_str!("../fixtures/sealed-circle-key.hex"))
        );
        assert_eq!(copies[0].slot.0, 2);
        assert_eq!(
            copies[0].object,
            hex(include_str!("../fixtures/sealed-store-key.hex"))
        );
        let path = crate::path::ObjectPath::floor(f::STORE_ID);
        let plain = open_stream(&bytes, prefix_length, path.as_str()).unwrap();
        let mut input = plain.as_slice();
        let mut frames = FrameDecoder::new(None);
        let mut floor =
            FloorFrames::decode_header(&frames.next(&mut input).unwrap().unwrap()).unwrap();
        assert_eq!(floor.header().first.0, 6);
        assert_eq!(floor.header().at, 11);
        let mut originals = Vec::new();
        while let Some(frame) = frames.next(&mut input).unwrap() {
            if let Some(original) = floor.frame(&frame).unwrap() {
                originals.push(original);
            }
        }
        frames.finish().unwrap();
        floor.finish().unwrap();
        assert_eq!(originals.len(), 2);
        for (original, snapshot, sealed_announcement, coverage, claims, key) in [
            (
                &originals[0],
                hex(include_str!("../fixtures/sealed-snapshot.hex")),
                hex(include_str!("../fixtures/sealed-announcement.hex")),
                9,
                vec![(1, 8), (2, 1)],
                f::store_key().derive(),
            ),
            (
                &originals[1],
                hex(include_str!("../fixtures/sealed-circle-snapshot.hex")),
                hex(include_str!("../fixtures/sealed-circle-announcement.hex")),
                8,
                vec![(1, 7), (2, 1)],
                coven_crypto::CircleKey::from_bytes(
                    coven_crypto::CircleId(uuid::Uuid::from_bytes([2; 16])),
                    coven_foundation::id_source::KeyId(uuid::Uuid::from_bytes([3; 16])),
                    [18; 32],
                )
                .derive(),
            ),
        ] {
            assert_eq!(original.object, sealed_announcement);
            let original_prefix = SlotObjectPrefix::length(&original.object).unwrap();
            let path = f::slot_path(original.slot.0);
            let original_plain =
                open_stream(&original.object, original_prefix, path.as_str()).unwrap();
            let (_, crate::sequence::SlotFrame::Announcement(announcement)) =
                crate::sequence::SlotFrame::decode(&original_plain, &path).unwrap()
            else {
                panic!("announcement")
            };
            let length = crate::sealed_snapshot::SnapshotObjectPrefix::length(&snapshot).unwrap();
            let (prefix, signature) = crate::sealed_snapshot::SnapshotObjectPrefix::decode_signed(
                &snapshot[..length + 64],
            )
            .unwrap();
            assert_eq!(announcement.covers, coverage);
            assert_eq!(announcement.covers, prefix.covers);
            assert_eq!(announcement.purpose, prefix.purpose);
            let mut digest = ObjectHasher::new();
            digest.update(&snapshot);
            assert_eq!(&announcement.digest, digest.finish().as_bytes());
            let id = &announcement.snapshot;
            let path = crate::path::ObjectPath::snapshot(
                f::STORE_ID,
                id.audience.clone(),
                id.device,
                id.number.try_into().unwrap(),
            );
            f::member()
                .signing
                .verify_prefix(path.as_str(), &snapshot[..length], &signature)
                .unwrap();
            let plain =
                open_stream_parts(&snapshot, length, length + 64, path.as_str(), &key).unwrap();
            let mut decoder = crate::snapshot_stream::SnapshotChunkDecoder::new(prefix);
            let mut oracle = f::TestOracle {
                writes: Default::default(),
            };
            let mut included = Vec::new();
            for chunk in plain.chunks(65536) {
                let mut input = chunk;
                while let Some(record) = decoder.next_record(&mut input, &oracle).unwrap() {
                    match record {
                        crate::snapshot::SnapshotRecord::Write(write) => {
                            oracle.writes.insert(write.id, write);
                        }
                        crate::snapshot::SnapshotRecord::Claim(claim) => {
                            included.push((claim.device.0, claim.number))
                        }
                        _ => {}
                    }
                }
            }
            decoder.finish().unwrap();
            assert_eq!(decoder.header().unwrap().schema_version, 2);
            assert_eq!(included, claims);
            assert!(coverage >= floor.header().first.0 - 1);
        }
        // A stored slot below the advanced floor can still exist until deletion finishes.
        assert!(copies[0].slot < floor.header().first);
        assert!(open_stream(&bytes, prefix_length, f::slot_path(9).as_str()).is_err());
        for index in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            assert!(
                open_stream(&changed, prefix_length, path.as_str()).is_err(),
                "byte {index}"
            );
        }
    }

    #[test]
    fn writes_slot_fixture_preserves_the_exact_inner_object_across_outer_chunks() {
        let bytes = hex(include_str!("../fixtures/sealed-writes-slot.hex"));
        let prefix_length = SlotObjectPrefix::length(&bytes).unwrap();
        let prefix = SlotObjectPrefix::decode(&bytes[..prefix_length]).unwrap();
        assert_eq!(prefix.kind, SlotKind::Writes);
        let path = f::slot_path(6);
        let plain = open_stream(&bytes, prefix_length, path.as_str()).unwrap();
        let mut decoder = WritesSlotDecoder::new(path.clone());
        let mut inner = Vec::new();
        let expected = [
            hex(include_str!("../fixtures/sealed-write.hex")),
            hex(include_str!("../fixtures/sealed-migration-write.hex")),
        ];
        let headers = [f::chunked_write().header, migration_header()];
        let mut count = 0;
        for chunk in plain.chunks(65536) {
            let mut input = chunk;
            while let Some(piece) = decoder.next(&mut input).unwrap() {
                match piece {
                    BatchPiece::Header(header) => assert_eq!(header, f::slot_header(6, 5)),
                    BatchPiece::Write { length } => {
                        assert_eq!(length, expected[count].len() as u64);
                        decoder.check_header(&headers[count]).unwrap();
                    }
                    BatchPiece::Bytes(bytes) => inner.extend_from_slice(bytes),
                    BatchPiece::EndWrite => {
                        assert_eq!(inner, expected[count]);
                        inner.clear();
                        count += 1;
                    }
                }
            }
        }
        decoder.finish().unwrap();
        assert_eq!(count, 2);
        assert!(open_stream(&bytes, prefix_length, f::slot_path(7).as_str()).is_err());
        for index in [
            0,
            prefix_length,
            bytes.len() / 2,
            bytes.len() - 65,
            bytes.len() - 1,
        ] {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            assert!(open_stream(&changed, prefix_length, path.as_str()).is_err());
        }
    }

    fn migration_header() -> crate::write::WriteHeader {
        use crate::sealed_write::{WriteObjectLayout, WriteObjectPrefix};
        let bytes = hex(include_str!("../fixtures/sealed-migration-write.hex"));
        let prefix_length = WriteObjectPrefix::length(&bytes).unwrap();
        let prefix_bytes = &bytes[..prefix_length];
        let prefix = WriteObjectPrefix::decode(prefix_bytes).unwrap();
        let length = WriteObjectPrefix::header_chunk_length(&bytes[prefix_length..]).unwrap();
        let header_piece = &bytes[prefix_length..prefix_length + length];
        let sealed = WriteObjectPrefix::header_chunk(header_piece).unwrap();
        let id = coven_merge::WriteId {
            device: coven_foundation::id_source::DeviceId(1),
            number: 4,
        };
        let path = crate::writes_slot::identity(f::STORE_ID, id).unwrap();
        let plain = f::store_key()
            .derive()
            .open_object_chunk(&path, prefix_bytes, 0, 0, sealed)
            .unwrap();
        let header = crate::write_stream::WriteHeaderFrame::decode(&plain).unwrap();
        assert_eq!(header.header.position, id);
        assert_eq!(header.header.read_through, 5);
        assert_eq!(
            header.header.disposition,
            crate::write::WriteDisposition::Migration
        );
        assert!(header.parts.is_empty());
        let tail_at = prefix_length + length;
        let tail = prefix.decode_tail(&bytes[tail_at..tail_at + 64]).unwrap();
        f::member()
            .signing
            .verify_write_header(
                &path,
                prefix_bytes,
                header_piece,
                &tail.roots,
                &tail.signature,
            )
            .unwrap();
        let mut layout = WriteObjectLayout::new(prefix, &plain, vec![]).unwrap();
        layout.decode_chunk(header_piece).unwrap();
        layout.tail(&tail).unwrap();
        let signature = layout.read_signature(&bytes[tail_at + 64..]).unwrap();
        let mut hash = ObjectHasher::new();
        hash.update(&bytes[..bytes.len() - 64]);
        f::member()
            .signing
            .verify_object(&path, &hash.finish(), &signature)
            .unwrap();
        header.header
    }
}
