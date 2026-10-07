use super::*;
use crate::{
    test_utils,
    write_stream::{decode_plaintext, PartDecoder, WriteEncoder},
};

fn dismissal(column: &str) -> Dismissal {
    Dismissal {
        row: test_utils::row(),
        column: column.into(),
        write: coven_merge::WriteId {
            number: 2,
            ..test_utils::position()
        },
    }
}

#[test]
fn dismissal_has_d5_bytes_and_survives_streaming_with_row_changes() {
    let lost = dismissal("x");
    let bytes = lost.encode().unwrap();
    assert_eq!(bytes[0], 3);
    assert_eq!(Dismissal::decode(&bytes).unwrap(), lost);
    let mut record = test_utils::write();
    record.parts[0].dismissals = vec![lost.clone(), dismissal("y")];
    let encoder = WriteEncoder::new(&record).unwrap();
    assert_eq!(encoder.header().parts[0].record_count, 3);
    let mut decoder = PartDecoder::new(encoder.header().parts[0].clone()).unwrap();
    let frames = encoder
        .part_chunks(0)
        .unwrap()
        .flat_map(|chunk| decoder.chunk(&chunk.unwrap()).unwrap())
        .collect::<Vec<_>>();
    decoder.finish().unwrap();
    assert!(matches!(&frames[0], WriteFrame::Change(_)));
    assert_eq!(frames[1], WriteFrame::Dismissal(lost));
    assert_eq!(
        decode_plaintext(&test_utils::write_plaintext(&record).unwrap()).unwrap(),
        record
    );
    record.parts[0].rows.clear();
    assert_eq!(
        decode_plaintext(&test_utils::write_plaintext(&record).unwrap()).unwrap(),
        record
    );
}

#[test]
fn malformed_or_duplicate_dismissals_and_reversed_frames_are_refused() {
    let mut record = test_utils::write();
    record.parts[0].dismissals = vec![dismissal("x"), dismissal("x")];
    assert!(WriteEncoder::new(&record).is_err());
    record.parts[0].dismissals.pop();
    let encoder = WriteEncoder::new(&record).unwrap();
    let mut decoder = PartDecoder::new(encoder.header().parts[0].clone()).unwrap();
    let reversed = [
        record.parts[0].dismissals[0].encode().unwrap(),
        record.parts[0].rows[0].encode().unwrap(),
    ]
    .concat();
    assert!(decoder.chunk(&reversed).is_err());
    let mut lost = dismissal("");
    assert!(lost.encode().is_err());
    lost.column = "x".into();
    lost.write.number = 0;
    assert!(lost.encode().is_err());
    let bytes = dismissal("x").encode().unwrap();
    crate::tests::mutations(&bytes, |bytes| {
        if let Ok(value) = Dismissal::decode(bytes) {
            assert_eq!(value.encode().unwrap(), bytes);
        }
    });
}
