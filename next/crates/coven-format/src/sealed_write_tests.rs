use super::*;
use crate::{
    test_utils,
    write_stream::{PartDecoder, WriteEncoder},
};
use coven_crypto::{ObjectHasher, StoreKey};

fn key(byte: u8) -> StoreKey {
    StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([byte; 16])), [byte; 32])
}

#[test]
fn sealed_write_has_its_own_kind_outside_the_plaintext_frames() {
    let prefix = WriteObjectPrefix {
        store_key: key(1).id(),
        part_keys: vec![key(1).id()],
    };
    let mut bytes = prefix.encode().unwrap();
    assert!(crate::frame_length(&bytes).is_err());
    assert!(crate::sealed_snapshot::SnapshotObjectPrefix::decode(&bytes).is_err());
    for kind in (1..=5).chain(7..=13).chain([15]) {
        bytes[0] = kind;
        assert!(WriteObjectPrefix::decode(&bytes).is_err(), "kind {kind}");
    }
}

fn object() -> Vec<Vec<u8>> {
    let record = test_utils::chunked_write();
    let encoder = WriteEncoder::new(&record).unwrap();
    let mut layout = WriteObjectLayout::new(
        WriteObjectPrefix {
            store_key: key(1).id(),
            part_keys: vec![key(1).id(), key(2).id()],
        },
        encoder.header().clone(),
    )
    .unwrap();
    let mut pieces = vec![layout.prefix().unwrap()];
    let sealed = key(1)
        .derive()
        .seal_object_chunk("devices/1/3", 0, 0, encoder.header_frame())
        .unwrap();
    pieces.push(layout.encode_chunk(&sealed).unwrap());
    for part in 0..record.parts.len() {
        for plain in encoder.part_chunks(part).unwrap() {
            let coordinate = layout.next_chunk().unwrap();
            let sealed = key(part as u8 + 1)
                .derive()
                .seal_object_chunk(
                    "devices/1/3",
                    coordinate.section,
                    coordinate.index,
                    &plain.unwrap(),
                )
                .unwrap();
            pieces.push(layout.encode_chunk(&sealed).unwrap());
        }
    }
    let mut hash = ObjectHasher::new();
    for piece in &pieces {
        hash.update(piece);
    }
    let signature = test_utils::member_keys().sign_object("devices/1/3", &hash.finish());
    pieces.push(layout.signature(&signature).unwrap().to_vec());
    layout.finish(&[]).unwrap();
    pieces
}

fn read(pieces: &[Vec<u8>]) -> Result<(), Box<dyn std::error::Error>> {
    let prefix = WriteObjectPrefix::decode(&pieces[0])?;
    assert_eq!(
        WriteObjectPrefix::length(&pieces[0][..23])?,
        pieces[0].len()
    );
    let mut hash = ObjectHasher::new();
    hash.update(&pieces[0]);
    assert_eq!(
        WriteObjectPrefix::header_chunk_length(&pieces[1][..4])?,
        pieces[1].len()
    );
    let header = key(1).derive().open_object_chunk(
        "devices/1/3",
        0,
        0,
        WriteObjectPrefix::header_chunk(&pieces[1])?,
    )?;
    hash.update(&pieces[1]);
    let mut layout = prefix.opened_header(&header)?;
    let mut store = PartDecoder::new(layout.header().parts[0].clone())?;
    let mut rows = Vec::new();
    for piece in &pieces[2..pieces.len() - 1] {
        hash.update(piece);
        assert_eq!(layout.chunk_length(&piece[..4])?, piece.len());
        let (coordinate, sealed) = layout.decode_chunk(piece)?;
        if coordinate.key == key(1).id() {
            let plain = key(1).derive().open_object_chunk(
                "devices/1/3",
                coordinate.section,
                coordinate.index,
                sealed,
            )?;
            rows.extend(store.chunk(&plain)?);
        }
        // This device lacks the other key; it still consumes and hashes the part.
    }
    store.finish()?;
    assert_eq!(rows, test_utils::chunked_write().parts[0].rows);
    let signature = layout.read_signature(pieces.last().unwrap())?;
    layout.finish(&[])?;
    test_utils::member()
        .signing
        .verify_object("devices/1/3", &hash.finish(), &signature)?;
    Ok(())
}

#[test]
fn skipped_circle_chunks_still_participate_in_the_object_signature() {
    let mut pieces = object();
    read(&pieces).unwrap();
    let circle = pieces.len() - 2;
    pieces[circle][30] ^= 1;
    assert!(read(&pieces).is_err());
}

#[test]
fn layout_refuses_wrong_counts_lengths_signature_positions_and_trailing_bytes() {
    let record = test_utils::write();
    let encoder = WriteEncoder::new(&record).unwrap();
    let prefix = WriteObjectPrefix {
        store_key: key(1).id(),
        part_keys: vec![key(1).id()],
    };
    let signature = Signature::from_bytes([0; 64]);
    let mut layout = WriteObjectLayout::new(prefix.clone(), encoder.header().clone()).unwrap();
    assert!(layout.signature(&signature).is_err());
    assert!(layout.finish(&[]).is_err());
    for prefix in [0u32, SEALED_OBJECT_CHUNK_OVERHEAD as u32, u32::MAX] {
        assert!(layout.chunk_length(&prefix.to_be_bytes()).is_err());
    }
    assert!(layout.encode_chunk(&[]).is_err());
    let bad = WriteObjectPrefix {
        part_keys: vec![],
        ..prefix.clone()
    };
    assert!(WriteObjectLayout::new(bad, encoder.header().clone()).is_err());
    while let Some(chunk) = layout.next_chunk() {
        // Layout-only test: arbitrary bytes stand in for independently checked crypto output.
        layout
            .encode_chunk(&vec![
                0;
                SEALED_OBJECT_CHUNK_OVERHEAD + chunk.plaintext_length
            ])
            .unwrap();
    }
    assert!(layout.read_signature(&[0; 63]).is_err());
    assert!(layout.read_signature(&[0; 65]).is_err());
    layout.signature(&signature).unwrap();
    assert!(layout.encode_chunk(&[0; 50]).is_err());
    assert!(layout.signature(&signature).is_err());
    assert!(layout.finish(&[0]).is_err());
    layout.finish(&[]).unwrap();
    let bytes = prefix.encode().unwrap();
    for end in 0..bytes.len() {
        assert!(WriteObjectPrefix::decode(&bytes[..end]).is_err());
    }
    let mut hostile = bytes;
    hostile[19..23].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        WriteObjectPrefix::length(&hostile),
        Err(Error::Limit { .. })
    ));
}

#[test]
fn generated_prefixes_are_bounded_and_canonical() {
    let mut state = 0xd357_a213_u64;
    for n in 0..25_000 {
        let mut bytes: Vec<_> = (0..n % 128)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        if n % 2 == 0 && bytes.len() >= 3 {
            bytes[..3].copy_from_slice(&[14, 0, 1]);
        }
        if let Ok(prefix) = WriteObjectPrefix::decode(&bytes) {
            assert_eq!(prefix.encode().unwrap(), bytes);
        }
        let _length = WriteObjectPrefix::header_chunk_length(&bytes);
    }
}

#[test]
fn migration_object_authenticates_a_header_followed_directly_by_its_signature() {
    let mut record = test_utils::write();
    record.header.disposition = crate::write::WriteDisposition::Migration;
    record.parts.clear();
    let encoder = WriteEncoder::new(&record).unwrap();
    let mut layout = WriteObjectLayout::new(
        WriteObjectPrefix {
            store_key: key(1).id(),
            part_keys: vec![],
        },
        encoder.header().clone(),
    )
    .unwrap();
    let prefix = layout.prefix().unwrap();
    assert_eq!(WriteObjectPrefix::length(&prefix).unwrap(), 23);
    let sealed = key(1)
        .derive()
        .seal_object_chunk("devices/1/3", 0, 0, encoder.header_frame())
        .unwrap();
    let chunk = layout.encode_chunk(&sealed).unwrap();
    assert!(layout.next_chunk().is_none());
    let mut hash = ObjectHasher::new();
    hash.update(&prefix);
    hash.update(&chunk);
    let digest = hash.finish();
    let signature = test_utils::member_keys().sign_object("devices/1/3", &digest);
    let signature = layout.signature(&signature).unwrap();
    layout.finish(&[]).unwrap();
    let opened = key(1)
        .derive()
        .open_object_chunk(
            "devices/1/3",
            0,
            0,
            WriteObjectPrefix::header_chunk(&chunk).unwrap(),
        )
        .unwrap();
    let mut reader = WriteObjectPrefix::decode(&prefix)
        .unwrap()
        .opened_header(&opened)
        .unwrap();
    assert!(reader.next_chunk().is_none());
    assert!(reader.finish(&[]).is_err());
    let signature = reader.read_signature(&signature).unwrap();
    test_utils::member()
        .signing
        .verify_object("devices/1/3", &digest, &signature)
        .unwrap();
    reader.finish(&[]).unwrap();
    assert!(reader.finish(&[0]).is_err());
}
