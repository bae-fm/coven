use super::*;
use crate::test_utils;
use crate::value::Value;
use coven_merge::Operation;
use std::collections::BTreeMap;

fn rows(count: usize, payload: usize) -> WriteRecord {
    let mut record = test_utils::write();
    record.parts[0].rows = (0..count)
        .map(|index| {
            let mut row = test_utils::write().parts.remove(0).rows.remove(0);
            row.row.key = crate::key::encode_key(&[Value::Integer(index as i64)]).unwrap();
            row.change.operation = Operation::Update(BTreeMap::from([(
                "x".into(),
                test_utils::column(Value::Blob(vec![7; payload])),
            )]));
            row
        })
        .collect();
    record
}

#[test]
fn write_size_and_row_count_are_not_frame_limits() {
    for record in [rows(70_000, 0), rows(3, 6 * 1024 * 1024)] {
        let encoder = WriteEncoder::new(&record).unwrap();
        let mut decoder = PartDecoder::new(encoder.header().parts[0].clone()).unwrap();
        let mut decoded = Vec::new();
        let mut length = encoder.header_frame().len() as u64;
        for chunk in encoder.part_chunks(0).unwrap() {
            let chunk = chunk.unwrap();
            length += chunk.len() as u64;
            decoded.extend(decoder.chunk(&chunk).unwrap());
        }
        decoder.finish().unwrap();
        assert_eq!(decoded, record.parts[0].rows);
        assert_eq!(length, encoder.plaintext_length());
    }
}

#[test]
fn one_row_still_must_fit_a_bounded_frame() {
    let mut record = rows(1, 8 * 1024 * 1024);
    let row = &mut record.parts[0].rows[0];
    let Operation::Update(columns) = &mut row.change.operation else {
        unreachable!()
    };
    columns.insert(
        "y".into(),
        test_utils::column(Value::Blob(vec![1; 8 * 1024 * 1024])),
    );
    row.old.insert("y".into(), Value::Null);
    assert!(matches!(
        WriteEncoder::new(&record),
        Err(Error::Limit { .. })
    ));
}

#[test]
fn two_parts_round_trip_with_rows_split_across_chunks() {
    let record = test_utils::chunked_write();
    let encoder = WriteEncoder::new(&record).unwrap();
    assert!(encoder.header().parts[0].chunk_count() > 2);
    let bytes = test_utils::write_plaintext(&record).unwrap();
    assert_eq!(decode_plaintext(&bytes).unwrap(), record);
    for end in [0, 6, encoder.header_frame().len(), bytes.len() - 1] {
        assert!(decode_plaintext(&bytes[..end]).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(decode_plaintext(&trailing), Err(Error::TrailingBytes));
}

#[test]
fn plaintext_encoder_fills_exactly_the_measured_buffer() {
    let record = test_utils::chunked_write();
    let encoder = WriteEncoder::new(&record).unwrap();
    let length = usize::try_from(encoder.plaintext_length()).unwrap();
    for wrong_length in [0, length - 1, length + 1] {
        let mut bytes = vec![0xa5; wrong_length];
        assert!(matches!(
            encoder.encode_plaintext(&mut bytes),
            Err(Error::Invalid {
                rule: Rule::StreamLength,
                ..
            })
        ));
        assert!(bytes.iter().all(|byte| *byte == 0xa5));
    }
    let mut bytes = vec![0xa5; length];
    encoder.encode_plaintext(&mut bytes).unwrap();
    assert_eq!(decode_plaintext(&bytes).unwrap(), record);
    let mut expected = encoder.header_frame().to_vec();
    for part in &record.parts {
        for row in &part.rows {
            expected.extend(row.encode().unwrap());
        }
    }
    assert_eq!(bytes, expected);
}

#[test]
fn counts_lengths_order_and_audience_are_checked_at_stream_boundaries() {
    let record = rows(2, 0);
    let encoder = WriteEncoder::new(&record).unwrap();
    let header = encoder.header().parts[0].clone();
    let frames: Vec<_> = record.parts[0]
        .rows
        .iter()
        .map(|r| r.encode().unwrap())
        .collect();
    for row_count in [1, 3] {
        let mut decoder = PartDecoder::new(PartHeader {
            row_count,
            ..header.clone()
        })
        .unwrap();
        assert!(decoder
            .chunk(&frames.concat())
            .and_then(|_| decoder.finish())
            .is_err());
    }
    for length in [header.plaintext_length - 1, header.plaintext_length + 1] {
        let mut decoder = PartDecoder::new(PartHeader {
            plaintext_length: length,
            ..header.clone()
        })
        .unwrap();
        assert!(decoder.chunk(&frames.concat()).is_err());
    }
    let mut decoder = PartDecoder::new(PartHeader {
        plaintext_length: 7,
        row_count: 1,
        ..header.clone()
    })
    .unwrap();
    assert!(matches!(
        decoder.chunk(&frames[0][..7]),
        Err(Error::Invalid {
            rule: Rule::StreamLength,
            ..
        })
    ));
    for stream in [
        [frames[1].clone(), frames[0].clone()].concat(),
        [frames[0].clone(), frames[0].clone()].concat(),
    ] {
        let mut decoder = PartDecoder::new(PartHeader {
            plaintext_length: stream.len() as u64,
            ..header.clone()
        })
        .unwrap();
        assert!(matches!(
            decoder.chunk(&stream),
            Err(Error::Invalid {
                rule: Rule::Order,
                ..
            })
        ));
    }
    let mut decoder = PartDecoder::new(PartHeader {
        audience: Audience::Circle(coven_crypto::CircleId(uuid::Uuid::from_u128(1))),
        ..header
    })
    .unwrap();
    assert!(matches!(
        decoder.chunk(&frames.concat()),
        Err(Error::Invalid {
            rule: Rule::Audience,
            ..
        })
    ));
}

#[test]
fn generated_plaintext_streams_are_rejected_or_reencode_identically() {
    let mut state = 0xb5f2_106d_7538_901bu64;
    for n in 0..25_000 {
        let bytes: Vec<_> = (0..n % 512)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        if let Ok(record) = decode_plaintext(&bytes) {
            assert_eq!(test_utils::write_plaintext(&record).unwrap(), bytes);
        }
        if bytes.len() >= FRAME_PREFIX_LEN {
            let header = PartHeader {
                audience: Audience::Store,
                row_count: 1,
                plaintext_length: bytes.len() as u64,
            };
            let mut decoder = PartDecoder::new(header).unwrap();
            if let Ok(rows) = decoder.chunk(&bytes) {
                if decoder.finish().is_ok() {
                    assert_eq!(
                        rows.iter()
                            .flat_map(|row| row.encode().unwrap())
                            .collect::<Vec<_>>(),
                        bytes
                    );
                }
            }
        }
    }
    let original = test_utils::write_plaintext(&test_utils::write()).unwrap();
    for end in 0..original.len() {
        assert!(decode_plaintext(&original[..end]).is_err());
    }
    for index in 0..original.len() {
        for bit in 0..8 {
            let mut changed = original.clone();
            changed[index] ^= 1 << bit;
            if let Ok(record) = decode_plaintext(&changed) {
                assert_eq!(test_utils::write_plaintext(&record).unwrap(), changed);
            }
        }
    }
}
