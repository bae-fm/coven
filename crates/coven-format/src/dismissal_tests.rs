use super::*;
use crate::{
    test_utils,
    write_stream::{decode_plaintext, PartDecoder, WriteEncoder},
};

/// The fixture write with only its row change, so its stream ends with the
/// dismissals these tests add.
fn changed_row() -> crate::write::WriteRecord {
    let mut record = test_utils::write();
    record.parts[0].executions.clear();
    record
}

fn dismissal(column: &str) -> Dismissal {
    Dismissal {
        row: test_utils::row(),
        target: DismissalTarget::Cell(column.into()),
        write: coven_merge::WriteId { number: 2, ..test_utils::position() },
    }
}

#[test]
fn dismissal_has_d5_bytes_and_survives_streaming_with_row_changes() {
    let lost = dismissal("x");
    let bytes = lost.encode().unwrap();
    assert_eq!(bytes[0], 3);
    assert_eq!(Dismissal::decode(&bytes).unwrap(), lost);
    let mut record = changed_row();
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
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&record).unwrap()).unwrap(), record);
    record.parts[0].rows.clear();
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&record).unwrap()).unwrap(), record);
}

#[test]
fn malformed_or_duplicate_dismissals_and_reversed_frames_are_refused() {
    let mut record = changed_row();
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
    lost.target = DismissalTarget::Cell("x".into());
    lost.write.number = 0;
    assert!(lost.encode().is_err());
    let bytes = dismissal("x").encode().unwrap();
    crate::tests::mutations(&bytes, |bytes| {
        if let Ok(value) = Dismissal::decode(bytes) {
            assert_eq!(value.encode().unwrap(), bytes);
        }
    });
}

#[test]
fn dismissals_require_explicit_peer_reads_or_implicit_own_predecessors() {
    let original = changed_row();
    for (device, number, accepted) in
        [(1, 2, true), (1, 3, false), (2, 1, true), (2, 2, false), (3, 1, false)]
    {
        let mut record = original.clone();
        record.parts[0].dismissals = vec![Dismissal {
            write: coven_merge::WriteId {
                device: coven_foundation::id_source::DeviceId(device),
                number,
            },
            ..dismissal("x")
        }];
        let encoded =
            test_utils::write_plaintext(&record).and_then(|_| test_utils::check_past(&record));
        assert_eq!(encoded.is_ok(), accepted);
        // Preserve the part length while replacing a covered identity in a valid stream.
        let mut covered = original.clone();
        covered.parts[0].dismissals = vec![dismissal("x")];
        let mut bytes = test_utils::write_plaintext(&covered).unwrap();
        let end = bytes.len();
        bytes[end - 16..end - 8].copy_from_slice(&device.to_be_bytes());
        bytes[end - 8..].copy_from_slice(&number.to_be_bytes());
        let decoded = decode_plaintext(&bytes).and_then(|record| {
            test_utils::check_past(&record)?;
            Ok(record)
        });
        if accepted {
            assert_eq!(decoded.unwrap(), record);
        } else {
            assert_eq!(
                decoded,
                Err(Error::Invalid { field: "dismissed write", rule: Rule::Coverage })
            );
        }
    }
}

#[test]
fn empty_rows_have_distinct_ordered_dismissals_and_their_own_write() {
    let cell = dismissal("x");
    let empty = Dismissal { target: DismissalTarget::EmptyRow, ..cell.clone() };
    let mut later = empty.clone();
    later.write.device = coven_foundation::id_source::DeviceId(2);
    later.write.number = 1;
    let bytes = empty.encode().unwrap();
    assert_eq!(
        bytes,
        test_utils::fixture_bytes(
            "0300010000001f000000017400000004206b0000000100000000000000010000000000000002"
        )
    );
    assert_eq!(Dismissal::decode(&bytes).unwrap(), empty);
    assert_ne!(bytes, cell.encode().unwrap());
    // The target tag follows RowId and precedes the write's two u64s.
    assert_eq!(bytes[bytes.len() - 17], 1);
    let mut unknown = bytes.clone();
    let tag = unknown.len() - 17;
    unknown[tag] = 2;
    assert!(matches!(
        Dismissal::decode(&unknown),
        Err(Error::UnknownTag { field: "dismissal target", tag: 2 })
    ));
    crate::tests::mutations(&bytes, |bytes| {
        if let Ok(value) = Dismissal::decode(bytes) {
            assert_eq!(value.encode().unwrap(), bytes);
        }
    });
    let mut record = changed_row();
    record.parts[0].dismissals = vec![cell, empty.clone(), later.clone()];
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&record).unwrap()).unwrap(), record);
    record.parts[0].dismissals.swap(0, 1);
    assert!(WriteEncoder::new(&record).is_err());
    record.parts[0].dismissals = vec![later, empty.clone()];
    assert!(WriteEncoder::new(&record).is_err());
    record.parts[0].dismissals = vec![empty.clone(), empty];
    assert!(WriteEncoder::new(&record).is_err());
}
