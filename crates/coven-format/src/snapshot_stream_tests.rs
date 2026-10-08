use super::*;
use crate::{chunks::PlaintextChunks, snapshot::SnapshotEncoder, test_utils, value::Value};

#[test]
fn losses_stream_more_rows_than_a_frame_can_hold() {
    let count = 70_000;
    let mut header = test_utils::snapshot_header();
    header.counts = [0, 0, 0, 0, count];
    let (mut encoder, first) = SnapshotEncoder::start(header).unwrap();
    let mut index = 0;
    let frames = std::iter::once(Ok(first)).chain(std::iter::from_fn(move || {
        if index > count {
            return None;
        }
        if index == count {
            index += 1;
            return Some(encoder.finish());
        }
        let mut row = test_utils::excluded_loss();
        row.row.key = crate::key::encode_key(&[Value::Integer(index as i64)]).unwrap();
        index += 1;
        Some(encoder.record(SnapshotRecord::Loss(row)))
    }));
    let mut decoder =
        SnapshotChunkDecoder::new(test_utils::snapshot_prefix(&test_utils::snapshot_header()));
    let oracle = test_utils::oracle();
    let mut received = 0;
    for chunk in PlaintextChunks::new(frames) {
        let chunk = chunk.unwrap();
        let mut bytes = chunk.as_slice();
        while let Some(record) = decoder.next_record(&mut bytes, &oracle).unwrap() {
            if let SnapshotRecord::Loss(loss) = record {
                assert_eq!(
                    crate::key::decode_key(&loss.row.key).unwrap(),
                    [Value::Integer(received)]
                );
                received += 1;
            }
        }
    }
    assert_eq!(received as u64, count);
    decoder.finish().unwrap();
}

#[test]
fn missing_end_markers_partial_frames_and_wrong_prefix_audiences_are_refused() {
    let frames = test_utils::snapshot_frames();
    let bytes = frames.concat();
    for end in 0..bytes.len() {
        let mut decoder =
            SnapshotChunkDecoder::new(test_utils::snapshot_prefix(&test_utils::snapshot_header()));
        let mut input = &bytes[..end];
        let result = (|| {
            while decoder
                .next_record(&mut input, &test_utils::oracle())?
                .is_some()
            {}
            decoder.finish()
        })();
        assert!(result.is_err(), "truncation {end}");
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    let mut decoder =
        SnapshotChunkDecoder::new(test_utils::snapshot_prefix(&test_utils::snapshot_header()));
    let mut input = trailing.as_slice();
    while decoder
        .next_record(&mut input, &test_utils::oracle())
        .unwrap()
        .is_some()
    {}
    assert!(decoder.finish().is_err());
    let mut decoder = SnapshotChunkDecoder::new(crate::sealed_snapshot::SnapshotObjectPrefix {
        audience: coven_merge::Audience::Circle(coven_crypto::CircleId(uuid::Uuid::from_u128(1))),
        ..test_utils::snapshot_prefix(&test_utils::snapshot_header())
    });
    assert!(decoder
        .next_record(&mut bytes.as_slice(), &test_utils::oracle())
        .is_err());
}

#[test]
fn generated_snapshot_chunks_never_panic() {
    let mut state = 0x29fa_143du64;
    for n in 0..25_000 {
        let mut bytes: Vec<_> = (0..n % 512)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        if n % 2 == 0 && bytes.len() >= 7 {
            bytes[..3].copy_from_slice(&[5, 0, 1]);
            let length = (bytes.len() - 7) as u32;
            bytes[3..7].copy_from_slice(&length.to_be_bytes());
        }
        let mut decoder =
            SnapshotChunkDecoder::new(test_utils::snapshot_prefix(&test_utils::snapshot_header()));
        let mut input = bytes.as_slice();
        let result = (|| {
            while decoder
                .next_record(&mut input, &test_utils::oracle())?
                .is_some()
            {}
            decoder.finish()
        })();
        assert!(result.is_err());
    }
}
