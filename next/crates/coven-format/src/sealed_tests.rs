use super::*;
use crate::sealed_single::SingleChunkObject;
use crate::sealed_snapshot::{SnapshotObjectLayout, SnapshotObjectPrefix};
use crate::sealed_write::{ObjectChunk, WriteObjectLayout, WriteObjectPrefix};
use crate::snapshot::SnapshotRecord;
use crate::snapshot_stream::SnapshotChunkDecoder;
use crate::tests::mutations;
use crate::write_stream::{PartDecoder, WriteEncoder, WriteHeaderFrame};
use crate::{test_utils, tests::hex, Object};
use coven_crypto::{CircleKey, ObjectHasher, Signature, StoreKey};
use coven_foundation::id_source::{CircleId, KeyId};
use uuid::Uuid;

const WRITE_PATH: &str = "devices/1/3";
const SNAPSHOT_PATH: &str = "snapshots/store/1/1";

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
) -> Result<Signature, Error> {
    let length = WriteObjectPrefix::length(bytes)?;
    let prefix_bytes = take(&mut bytes, length)?;
    let prefix = WriteObjectPrefix::decode(prefix_bytes)?;
    assert_eq!(prefix.encode()?, prefix_bytes);
    let mut writer = WriteObjectLayout::new(prefix.clone(), header, lengths.to_vec())?;
    let mut layout = WriteObjectLayout::new(prefix, header, lengths.to_vec())?;
    while layout.next_chunk().is_some() {
        let length = layout.chunk_length(bytes)?;
        let piece = take(&mut bytes, length)?;
        let (coordinate, sealed) = layout.decode_chunk(piece)?;
        assert_eq!(writer.encode_chunk(sealed)?, piece);
        visit(coordinate, sealed);
    }
    let signature = layout.read_signature(bytes)?;
    assert_eq!(writer.signature(&signature)?, bytes);
    layout.finish(&[])?;
    writer.finish(&[])?;
    Ok(signature)
}

fn walk_snapshot(
    mut bytes: &[u8],
    mut visit: impl FnMut(u64, &[u8]),
) -> Result<SnapshotObjectPrefix, Error> {
    let length = SnapshotObjectPrefix::length(bytes)?;
    let prefix_bytes = take(&mut bytes, length)?;
    let prefix = SnapshotObjectPrefix::decode(prefix_bytes)?;
    assert_eq!(prefix.encode()?, prefix_bytes);
    let mut layout = SnapshotObjectLayout::new();
    let mut writer = SnapshotObjectLayout::new();
    while !bytes.is_empty() {
        let length = layout.chunk_length(bytes)?;
        let index = layout.index();
        let piece = take(&mut bytes, length)?;
        let sealed = layout.decode_chunk(piece)?;
        assert_eq!(writer.encode_chunk(sealed)?, piece);
        visit(index, sealed);
    }
    Ok(prefix)
}

// These bytes were produced independently with Python hashlib/hmac and libsodium.
// Store/circle key bytes are 0x11/0x12, signing seeds 0x33, and all nonces are
// fixed public test material. Payloads are the existing plaintext fixture frames.
#[test]
fn sealed_write_fixture_opens_both_parts_and_reencodes_all_random_bytes() {
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
    let signature = walk_write(&bytes, &header_bytes, &lengths, |coordinate, sealed| {
        encoded.extend(writer.encode_chunk(sealed).unwrap());
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
    for (rows, part) in rows.into_iter().zip(expected.parts) {
        assert_eq!(
            rows,
            part.rows
                .iter()
                .cloned()
                .map(crate::dismissal::WriteFrame::Change)
                .collect::<Vec<_>>()
        );
    }
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
        .open_object_chunk("devices/1/4", prefix_bytes, 0, 0, sealed_header)
        .is_err());
    mutations(&bytes, |changed| {
        let _decoded = walk_write(changed, &header_bytes, &lengths, |_, _| {});
    });
}

#[test]
fn snapshot_fixture_binds_positions_and_requires_its_plaintext_end_marker() {
    let bytes = hex(include_str!("../fixtures/sealed-snapshot.hex"));
    let prefix_length = SnapshotObjectPrefix::length(&bytes).unwrap();
    let prefix_bytes = &bytes[..prefix_length];
    let prefix = SnapshotObjectPrefix::decode(prefix_bytes).unwrap();
    let mut reader = SnapshotChunkDecoder::new(prefix.audience.clone());
    let mut oracle = test_utils::TestOracle {
        writes: Default::default(),
    };
    let mut writer = SnapshotObjectLayout::new();
    let mut encoded = prefix.encode().unwrap();
    let mut plaintext = Vec::new();
    let mut count = 0;
    walk_snapshot(&bytes, |index, sealed| {
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
    reader.finish().unwrap();
    assert_eq!(reader.header().unwrap().writes, prefix.writes);
    assert_eq!(reader.header().unwrap().store_log, prefix.store_log);
    assert_eq!(count, test_utils::snapshot_records().len());
    assert_eq!(plaintext, test_utils::chunked_snapshot_frames().concat());
    assert_eq!(encoded, bytes);
    mutations(&bytes, |changed| {
        let _decoded = walk_snapshot(changed, |_, _| {});
    });
}

#[test]
fn single_chunk_fixtures_open_with_their_own_keys_and_authors() {
    for (text, path, kind) in [
        (
            include_str!("../fixtures/sealed-store-log.hex"),
            "store-log/1/1",
            33,
        ),
        (
            include_str!("../fixtures/sealed-positions.hex"),
            "positions/1",
            35,
        ),
        (
            include_str!("../fixtures/sealed-join-request.hex"),
            "join-requests/55555555-5555-5555-5555-555555555555",
            36,
        ),
    ] {
        let bytes = hex(text);
        let object = SingleChunkObject::decode(&bytes).unwrap();
        assert_eq!(bytes[0], kind);
        assert_eq!(object.encode().unwrap(), bytes);
        let prefix = object.prefix().encode().unwrap();
        let plain = match &object {
            SingleChunkObject::JoinRequest { .. } => test_utils::invite()
                .secret
                .join_request_key()
                .open_object_chunk(path, &prefix, 0, 0, object.chunk())
                .unwrap(),
            SingleChunkObject::StoreLog { key, .. }
            | SingleChunkObject::PostedPositions { key, .. } => {
                assert_eq!(*key, store_key().id());
                store_key()
                    .derive()
                    .open_object_chunk(path, &prefix, 0, 0, object.chunk())
                    .unwrap()
            }
        };
        let frame = Object::decode(&plain).unwrap();
        assert_eq!(frame.encode().unwrap(), plain);
        let author = match frame {
            Object::StoreLog(entry) => {
                assert_eq!(kind, 33);
                Some(entry.author)
            }
            Object::JoinRequest(request) => {
                assert_eq!(kind, 36);
                Some(request.keys.signing)
            }
            Object::PostedPositions(_) => {
                assert_eq!(kind, 35);
                None
            }
        };
        if let Some(author) = author {
            let mut hash = ObjectHasher::new();
            hash.update(&bytes[..bytes.len() - 64]);
            author
                .verify_object(path, &hash.finish(), object.signature().unwrap())
                .unwrap();
        } else {
            assert!(object.signature().is_none());
        }
        mutations(&bytes, |changed| {
            if let Ok(object) = SingleChunkObject::decode(changed) {
                assert_eq!(object.encode().unwrap(), changed);
            }
        });
    }
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
        store_key: store_key().id(),
        part_keys: vec![store_key().id(), circle_key().id()],
    };
    // Another plaintext codec can supply frame bytes of the same declared length;
    // the sealed layer has no knowledge of that codec's tags or field ordering.
    let opaque_header = vec![0x99; encoder.header_frame().len()];
    assert!(WriteObjectLayout::new(prefix, &opaque_header, lengths).is_ok());
}
