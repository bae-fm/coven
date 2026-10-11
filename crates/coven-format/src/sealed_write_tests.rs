use super::*;
use crate::tests::Generator;
use crate::{
    test_utils,
    write_stream::{PartDecoder, WriteEncoder, WriteHeaderFrame},
};
use coven_crypto::{ObjectHasher, StoreKey};

fn key(byte: u8) -> StoreKey {
    StoreKey::from_bytes(KeyId(uuid::Uuid::from_bytes([byte; 16])), [byte; 32])
}

#[test]
fn sealed_write_has_its_own_kind_outside_the_plaintext_frames() {
    let prefix = WriteObjectPrefix {
        format: crate::FormatVersion::CURRENT,
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
            format: crate::FormatVersion::CURRENT,
            store_key: key(1).id(),
            part_keys: vec![key(1).id(), key(2).id()],
        },
        encoder.header_frame(),
        encoder
            .header()
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect(),
    )
    .unwrap();
    let prefix = layout.prefix().unwrap();
    let mut pieces = vec![prefix.clone()];
    let sealed = key(1)
        .derive()
        .seal_object_chunk("devices/1/3", &prefix, 0, 0, encoder.header_frame())
        .unwrap();
    pieces.push(layout.encode_chunk(&sealed).unwrap());
    for part in 0..record.parts.len() {
        for plain in encoder.part_chunks(part).unwrap() {
            let coordinate = layout.next_chunk().unwrap();
            let sealed = key(part as u8 + 1)
                .derive()
                .seal_object_chunk(
                    "devices/1/3",
                    &prefix,
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
    assert_eq!(
        sealed_length(
            encoder.header_frame().len(),
            &encoder
                .header()
                .parts
                .iter()
                .map(|p| p.plaintext_length)
                .collect::<Vec<_>>(),
        )
        .unwrap(),
        pieces.iter().map(|piece| piece.len() as u64).sum::<u64>(),
    );
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
        &pieces[0],
        0,
        0,
        WriteObjectPrefix::header_chunk(&pieces[1])?,
    )?;
    hash.update(&pieces[1]);
    let header_frame = WriteHeaderFrame::decode(&header)?;
    let mut layout = prefix.opened_header(
        &header,
        header_frame
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect(),
    )?;
    let mut store = PartDecoder::new(header_frame.parts[0].clone())?;
    let mut rows = Vec::new();
    for piece in &pieces[2..pieces.len() - 1] {
        hash.update(piece);
        assert_eq!(layout.chunk_length(&piece[..4])?, piece.len());
        let (coordinate, sealed) = layout.decode_chunk(piece)?;
        if coordinate.key == key(1).id() {
            let plain = key(1).derive().open_object_chunk(
                "devices/1/3",
                &pieces[0],
                coordinate.section,
                coordinate.index,
                sealed,
            )?;
            rows.extend(store.chunk(&plain)?);
        }
        // This device lacks the other key; it still consumes and hashes the part.
    }
    store.finish()?;
    assert_eq!(
        rows,
        test_utils::chunked_write().parts[0]
            .rows
            .iter()
            .cloned()
            .map(crate::dismissal::WriteFrame::Change)
            .collect::<Vec<_>>()
    );
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
        format: crate::FormatVersion::CURRENT,
        store_key: key(1).id(),
        part_keys: vec![key(1).id()],
    };
    let signature = Signature::from_bytes([0; 64]);
    let mut layout = WriteObjectLayout::new(
        prefix.clone(),
        encoder.header_frame(),
        encoder
            .header()
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect(),
    )
    .unwrap();
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
    assert!(WriteObjectLayout::new(
        bad,
        encoder.header_frame(),
        encoder
            .header()
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect()
    )
    .is_err());
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
    let mut source = Generator(0xd357_a213_u64);
    for n in 0..25_000 {
        let mut bytes: Vec<_> = (0..n % 128).map(|_| source.next() as u8).collect();
        if n % 2 == 0 && bytes.len() >= 3 {
            bytes[..3].copy_from_slice(&[32, 0, 1]);
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
            format: crate::FormatVersion::CURRENT,
            store_key: key(1).id(),
            part_keys: vec![],
        },
        encoder.header_frame(),
        encoder
            .header()
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect(),
    )
    .unwrap();
    let prefix = layout.prefix().unwrap();
    assert_eq!(WriteObjectPrefix::length(&prefix).unwrap(), 23);
    let sealed = key(1)
        .derive()
        .seal_object_chunk("devices/1/3", &prefix, 0, 0, encoder.header_frame())
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
    assert_eq!(
        sealed_length(encoder.header_frame().len(), &[]).unwrap(),
        (prefix.len() + chunk.len() + signature.len()) as u64,
    );
    let opened = key(1)
        .derive()
        .open_object_chunk(
            "devices/1/3",
            &prefix,
            0,
            0,
            WriteObjectPrefix::header_chunk(&chunk).unwrap(),
        )
        .unwrap();
    let mut reader = WriteObjectPrefix::decode(&prefix)
        .unwrap()
        .opened_header(
            &opened,
            WriteHeaderFrame::decode(&opened)
                .unwrap()
                .parts
                .iter()
                .map(|p| p.plaintext_length)
                .collect(),
        )
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

#[test]
fn stored_kind_and_chunk_length_match_the_storage_format() {
    let pieces = object();
    assert_eq!(&pieces[0][..3], &[32, 0, 1]);
    for piece in &pieces[1..pieces.len() - 1] {
        let length = u32::from_be_bytes(piece[..4].try_into().unwrap()) as usize;
        assert_eq!(piece.len(), length + 44);
    }
}

#[test]
fn stored_length_refuses_invalid_boundaries_and_overflow_without_allocating() {
    assert!(sealed_length(crate::FRAME_PREFIX_LEN - 1, &[]).is_err());
    assert!(sealed_length(MAX_OBJECT + 1, &[]).is_err());
    assert!(sealed_length(crate::FRAME_PREFIX_LEN, &[0]).is_err());
    assert!(sealed_length(crate::FRAME_PREFIX_LEN, &[u64::MAX]).is_err());
}
