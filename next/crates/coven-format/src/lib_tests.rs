use super::*;
use crate::codes::{InviteCode, RestoreCode};
use crate::error::Rule;
use zeroize::Zeroizing;

fn hex(text: &str) -> Vec<u8> {
    let bytes = text.trim().as_bytes();
    assert_eq!(bytes.len() % 2, 0);
    bytes
        .chunks_exact(2)
        .map(|p| {
            let digit = |b: u8| (b as char).to_digit(16).expect("hex digit") as u8;
            (digit(p[0]) << 4) | digit(p[1])
        })
        .collect()
}

#[test]
fn every_object_and_snapshot_section_has_pinned_bytes() {
    let expected: Vec<_> = include_str!("../fixtures/v1.hex")
        .lines()
        .map(hex)
        .collect();
    let objects = test_utils::encoded_examples();
    assert_eq!(objects.len(), expected.len());
    for (bytes, expected) in objects.into_iter().zip(expected) {
        assert_eq!(bytes.as_slice(), expected);
    }
}

#[test]
fn pinned_chunks_decode_with_their_declared_boundaries() {
    let pieces: Vec<_> = include_str!("../fixtures/v1.hex")
        .lines()
        .map(hex)
        .collect();
    let mut index = test_utils::frame_examples().len();
    for frame in &pieces[..index] {
        assert_eq!(reencode(frame).unwrap().as_slice(), frame);
    }
    let prefix = crate::sealed_write::WriteObjectPrefix::decode(&pieces[index]).unwrap();
    assert_eq!(prefix.encode().unwrap(), pieces[index]);
    index += 1;
    let header = crate::write_stream::WriteHeaderFrame::decode(&pieces[index]).unwrap();
    index += 1;
    assert_eq!(header.parts.len(), prefix.part_keys.len());
    let mut parts = Vec::new();
    for part in header.parts {
        let mut rows = Vec::new();
        let mut decoder = crate::write_stream::PartDecoder::new(part.clone()).unwrap();
        for _ in 0..part.chunk_count() {
            rows.extend(decoder.chunk(&pieces[index]).unwrap());
            index += 1;
        }
        decoder.finish().unwrap();
        parts.push(crate::write::WritePart {
            audience: part.audience,
            rows,
        });
    }
    assert_eq!(
        crate::write::WriteRecord {
            header: header.header,
            parts
        },
        test_utils::chunked_write()
    );
    let prefix = crate::sealed_snapshot::SnapshotObjectPrefix::decode(&pieces[index]).unwrap();
    index += 1;
    let mut decoder = crate::snapshot_stream::SnapshotChunkDecoder::new(prefix.audience);
    let mut oracle = test_utils::TestOracle {
        writes: Default::default(),
    };
    let mut count = 0;
    for piece in &pieces[index..] {
        let mut bytes = piece.as_slice();
        while let Some(record) = decoder.next_record(&mut bytes, &oracle).unwrap() {
            if let crate::snapshot::SnapshotRecord::Write(write) = record {
                oracle.writes.insert(write.id, write);
            }
            count += 1;
        }
    }
    decoder.finish().unwrap();
    assert_eq!(count, test_utils::snapshot_records().len());
}

// A reproducible generator exercises arbitrary bytes without a randomness
// capability or an external dependency. Canonical fixtures supply deep valid
// structures, then every bit and every truncation boundary is exercised.
struct Bytes(u64);
impl Bytes {
    fn next(&mut self) -> u8 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as u8
    }
}

fn reencode(bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>, Error> {
    frame_length(bytes)?;
    match bytes[0] {
        1 => Ok(Zeroizing::new(
            crate::write_stream::WriteHeaderFrame::decode(bytes)?.encode()?,
        )),
        13 => Ok(Zeroizing::new(
            crate::write::RowChange::decode(bytes)?.encode()?,
        )),
        3 => {
            let reader = crate::snapshot::SnapshotDecoder::start(bytes)?;
            Ok(Zeroizing::new(
                crate::snapshot::SnapshotEncoder::start(reader.header().clone())?.1,
            ))
        }
        4 => {
            let (_, mut input) = crate::wire::decode_frame(bytes)?;
            let record = crate::snapshot::SnapshotRecord::get(&mut input, &test_utils::oracle())?;
            input.finish()?;
            record.validate()?;
            Ok(Zeroizing::new(encode_frame_with(4, |out| record.put(out))?))
        }
        5 => {
            crate::wire::decode_frame(bytes)?.1.finish()?;
            Ok(Zeroizing::new(encode_frame_with(5, |_| Ok(()))?))
        }
        8 => RestoreCode::from_bytes(bytes)?.to_bytes(),
        9 => InviteCode::from_bytes(bytes)?.to_bytes(),
        _ => Ok(Zeroizing::new(Object::decode(bytes)?.encode()?)),
    }
}
fn decode_or_typed_error(bytes: &[u8]) {
    match reencode(bytes) {
        Ok(encoded) => assert_eq!(encoded.as_slice(), bytes),
        Err(error) => {
            let _: Error = error;
        }
    }
}

#[test]
fn arbitrary_truncated_and_bit_flipped_inputs_never_panic() {
    let mut source = Bytes(0xb5f2_106d_7538_901b);
    for n in 0..25_000 {
        let mut bytes: Vec<_> = (0..n % 1024).map(|_| source.next()).collect();
        decode_or_typed_error(&bytes);
        if bytes.len() >= 7 {
            // Reach the payload decoder as well as the prefix checks.
            bytes[0] = 1 + source.next() % 13;
            bytes[1..3].copy_from_slice(&1u16.to_be_bytes());
            let len = (bytes.len() - 7) as u32;
            bytes[3..7].copy_from_slice(&len.to_be_bytes());
            decode_or_typed_error(&bytes);
        }
    }
    for bytes in test_utils::frame_examples() {
        for end in 0..bytes.len() {
            assert!(reencode(&bytes[..end]).is_err());
        }
        for index in 0..bytes.len() {
            for bit in 0..8 {
                let mut changed = bytes.clone();
                changed[index] ^= 1 << bit;
                decode_or_typed_error(&changed);
            }
        }
    }
}

#[test]
fn prefix_limits_versions_and_trailing_bytes_are_typed_errors() {
    assert_eq!(
        Object::decode(&[0; 7]),
        Err(Error::UnknownTag {
            field: "object kind",
            tag: 0
        })
    );
    assert_eq!(
        Object::decode(&[5, 0, 2, 0, 0, 0, 0]),
        Err(Error::UnsupportedVersion(2))
    );
    assert!(matches!(
        frame_length(&[7, 0, 1, 255, 255, 255, 255]),
        Err(Error::Limit {
            field: "frame payload",
            ..
        })
    ));
    let mut end = encode_frame_with(5, |_| Ok(())).unwrap();
    end.push(0);
    assert_eq!(reencode(&end), Err(Error::TrailingBytes));
    end[6] = 1;
    assert_eq!(reencode(&end), Err(Error::TrailingBytes));
}

#[test]
fn hostile_nested_lengths_do_not_drive_unbounded_allocations() {
    // A valid write prefix/header up to had-read, followed by a hostile count.
    let mut bytes = crate::write_stream::WriteEncoder::new(&test_utils::write())
        .unwrap()
        .header_frame()
        .to_vec();
    bytes[39..43].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        crate::write_stream::WriteHeaderFrame::decode(&bytes),
        Err(Error::Limit {
            field: "collection",
            ..
        })
    ));
    let mut source = wire::Decoder::new(&[0, 1, 0, 0]).unwrap();
    assert_eq!(Vec::<String>::get(&mut source), Err(Error::Truncated));
    let mut source = wire::Decoder::new(&[255, 255, 255, 255]).unwrap();
    assert!(matches!(
        wire::get_blob(&mut source),
        Err(Error::Limit { field: "bytes", .. })
    ));
}

#[test]
fn malformed_strings_and_keys_remain_distinguishable() {
    let mut input = wire::Decoder::new(&[0, 0, 0, 1, 255]).unwrap();
    assert_eq!(String::get(&mut input), Err(Error::Utf8));
    let mut write = test_utils::write();
    write.parts[0].rows[0].row.key = vec![0];
    assert!(matches!(
        test_utils::write_plaintext(&write),
        Err(Error::Invalid {
            rule: Rule::KeyEncoding,
            ..
        })
    ));
}
