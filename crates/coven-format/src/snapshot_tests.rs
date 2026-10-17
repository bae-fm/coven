use super::*;
use crate::test_utils;
use crate::value::Value;
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, Cell, ColumnValue, MergeError, Parent, Timestamp};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn raw(record: &SnapshotRecord) -> Vec<u8> {
    encode_frame_with(6, |out| record.put(out)).unwrap()
}
fn single_header(section: usize, audience: Audience, count: u64) -> SnapshotHeader {
    let mut header = test_utils::snapshot_header();
    header.counts = [0; 12];
    header.counts[section] = count;
    header.id.audience = audience;
    header
}
fn round_trip(record: SnapshotRecord, audience: Audience) {
    let (mut writer, first) =
        SnapshotEncoder::start(single_header(record.section() as usize, audience, 1)).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    assert_eq!(
        reader
            .frame(
                &writer.record(record.clone()).unwrap(),
                &test_utils::oracle()
            )
            .unwrap(),
        Some(record.clone())
    );
    reader
        .frame(&writer.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    reader.finish().unwrap();
}
#[test]
fn every_section_streams_with_write_metadata_supplied_by_its_consumer() {
    let header = test_utils::snapshot_header();
    let (mut writer, first) = SnapshotEncoder::start(header.clone()).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let mut oracle = test_utils::TestOracle {
        writes: BTreeMap::new(),
    };
    assert_eq!(reader.header(), &header);
    for record in test_utils::snapshot_records() {
        let decoded = reader
            .frame(&writer.record(record.clone()).unwrap(), &oracle)
            .unwrap()
            .unwrap();
        assert_eq!(decoded, record);
        if let SnapshotRecord::Write(write) = decoded {
            oracle.writes.insert(write.id, write);
        }
    }
    assert!(reader.finish().is_err());
    assert_eq!(
        reader.frame(&writer.finish().unwrap(), &oracle).unwrap(),
        None
    );
    reader.finish().unwrap();
    assert!(writer.finish().is_err());
    assert!(reader
        .frame(&encode_frame_with(7, |_| Ok(())).unwrap(), &oracle)
        .is_err());
}
#[test]
fn rejected_frames_do_not_advance_counts_or_order() {
    let (mut writer, first) = SnapshotEncoder::start(test_utils::snapshot_header()).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let oracle = test_utils::oracle();
    assert!(writer.finish().is_err());
    assert!(reader
        .frame(&encode_frame_with(7, |_| Ok(())).unwrap(), &oracle)
        .is_err());
    let records = test_utils::snapshot_records();
    assert!(writer.record(records[1].clone()).is_err());
    assert!(reader.frame(&raw(&records[1]), &oracle).is_err());
    let mut row = test_utils::row();
    row.audience = Audience::Circle(CircleId(Uuid::from_bytes([1; 16])));
    let wrong = SnapshotRecord::Synced(SyncedRow {
        row,
        columns: BTreeMap::from([("x".into(), test_utils::column(Value::Null))]),
    });
    assert!(writer.record(wrong.clone()).is_err());
    assert!(reader.frame(&raw(&wrong), &oracle).is_err());
    for record in records {
        assert_eq!(
            reader
                .frame(&writer.record(record.clone()).unwrap(), &oracle)
                .unwrap(),
            Some(record)
        );
    }
    reader.frame(&writer.finish().unwrap(), &oracle).unwrap();
    reader.finish().unwrap();
}
#[test]
fn duplicate_identity_and_uncovered_writes_are_rejected() {
    let (mut writer, first) = SnapshotEncoder::start(single_header(1, Audience::Store, 2)).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let oracle = test_utils::oracle();
    let make = |number| {
        SnapshotRecord::Write(AppliedWrite {
            id: WriteId {
                device: DeviceId(1),
                number,
            },
            timestamp: Timestamp::new(number, 0, DeviceId(1)).unwrap(),
            slot: Slot(number * 3),
            read_through: 0,
        })
    };
    reader
        .frame(&writer.record(make(1)).unwrap(), &oracle)
        .unwrap();
    for number in [1, 4] {
        assert!(writer.record(make(number)).is_err());
        assert!(reader.frame(&raw(&make(number)), &oracle).is_err());
    }
    reader
        .frame(&writer.record(make(2)).unwrap(), &oracle)
        .unwrap();
    reader.frame(&writer.finish().unwrap(), &oracle).unwrap();
    reader.finish().unwrap();
}
#[test]
fn a_snapshot_larger_than_the_frame_bound_is_streamed_in_key_order() {
    let (mut writer, first) =
        SnapshotEncoder::start(single_header(0, Audience::Store, 10_000)).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let oracle = test_utils::oracle();
    let mut total = first.len();
    for i in -5000..5000 {
        let mut row = test_utils::row();
        row.key = crate::key::encode_key(&[Value::Integer(i)]).unwrap();
        let record = SnapshotRecord::Synced(SyncedRow {
            row,
            columns: BTreeMap::from([("x".into(), test_utils::column(Value::Blob(vec![1; 2048])))]),
        });
        let frame = writer.record(record.clone()).unwrap();
        total += frame.len();
        assert_eq!(reader.frame(&frame, &oracle).unwrap(), Some(record));
    }
    assert!(total > crate::wire::MAX_OBJECT);
    reader.frame(&writer.finish().unwrap(), &oracle).unwrap();
    reader.finish().unwrap();
}
#[test]
fn empty_snapshot_requires_an_exact_end_frame() {
    let mut header = test_utils::snapshot_header();
    header.counts = [0; 12];
    let (mut writer, first) = SnapshotEncoder::start(header).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let oracle = test_utils::oracle();
    assert!(reader.frame(&first, &oracle).is_err());
    let end = writer.finish().unwrap();
    assert!(SnapshotDecoder::start(&end, &test_utils::snapshot_prefix(writer.header())).is_err());
    for n in 0..end.len() {
        assert!(reader.frame(&end[..n], &oracle).is_err());
    }
    let mut extra = end.clone();
    extra.push(0);
    assert!(reader.frame(&extra, &oracle).is_err());
    reader.frame(&end, &oracle).unwrap();
    reader.finish().unwrap();
}

#[test]
fn eof_before_the_end_marker_is_truncation_at_every_record_boundary() {
    let header = test_utils::snapshot_header();
    let frames = test_utils::snapshot_frames();
    let mut reader =
        SnapshotDecoder::start(&frames[0], &test_utils::snapshot_prefix(&header)).unwrap();
    assert_eq!(reader.finish(), Err(Error::Truncated));
    for frame in &frames[1..frames.len() - 1] {
        reader.frame(frame, &test_utils::oracle()).unwrap();
        assert_eq!(reader.finish(), Err(Error::Truncated));
    }
    reader
        .frame(frames.last().unwrap(), &test_utils::oracle())
        .unwrap();
    assert_eq!(reader.finish(), Ok(()));

    let mut empty = header;
    empty.counts = [0; 12];
    let (mut writer, first) = SnapshotEncoder::start(empty).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    assert_eq!(reader.finish(), Err(Error::Truncated));
    reader
        .frame(&writer.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    assert_eq!(reader.finish(), Ok(()));
}
#[test]
fn synced_rows_use_merges_written_reference_validation() {
    let circle = Audience::Circle(CircleId(Uuid::from_bytes([1; 16])));
    for (audience, generation, expected) in [
        (Audience::Store, 2, MergeError::ParentGeneration(2)),
        (circle, 1, MergeError::ReferenceAudience(test_utils::row())),
    ] {
        let mut parent = test_utils::row();
        parent.audience = audience;
        let record = SnapshotRecord::Synced(SyncedRow {
            row: test_utils::row(),
            columns: BTreeMap::from([(
                "x".into(),
                ColumnValue {
                    value: Value::Integer(1),
                    parents: BTreeMap::from([(
                        coven_merge::ForeignKey::new(["fk"], "t", ["id"]),
                        Parent {
                            row: parent,
                            generation,
                        },
                    )]),
                },
            )]),
        });
        let (mut writer, first) =
            SnapshotEncoder::start(single_header(0, Audience::Store, 1)).unwrap();
        let mut reader =
            SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
        assert_eq!(
            writer.record(record.clone()),
            Err(Error::Merge(expected.clone()))
        );
        assert_eq!(
            reader.frame(&raw(&record), &test_utils::oracle()),
            Err(Error::Merge(expected))
        );
    }
    let row = test_utils::write().parts.remove(0).rows.remove(0);
    let coven_merge::Operation::Update(columns) = row.change.operation else {
        panic!("update");
    };
    round_trip(
        SnapshotRecord::Synced(SyncedRow {
            row: row.row,
            columns,
        }),
        Audience::Store,
    );
}
#[test]
fn merge_owns_snapshot_row_invariants_and_a_rejected_row_can_be_retried() {
    let original = test_utils::merge_row();
    let state = &original.state;
    let encode_parts = |generations: &BTreeMap<u64, WriteId>,
                        cells: &BTreeMap<String, Cell<Value>>| {
        encode_frame_with(6, |out| {
            3u8.put(out)?;
            state.row().put(out)?;
            generations.put(out)?;
            cells.put(out)
        })
        .unwrap()
    };
    let (writer, header) = SnapshotEncoder::start(single_header(3, Audience::Store, 1)).unwrap();
    let mut reader =
        SnapshotDecoder::start(&header, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let oracle = test_utils::oracle();
    let mut generations = state.generations().clone();
    let first = generations.remove(&1).unwrap();
    generations.insert(2, first);
    assert!(matches!(
        reader.frame(&encode_parts(&generations, state.cells()), &oracle),
        Err(Error::Merge(MergeError::GenerationGap(1)))
    ));
    generations = state.generations().clone();
    generations.insert(2, state.cells()["x"].write);
    assert!(matches!(
        reader.frame(&encode_parts(&generations, state.cells()), &oracle),
        Err(Error::Merge(MergeError::DeletedRowHasCells))
    ));
    let mut cells = state.cells().clone();
    cells.get_mut("x").unwrap().value.parents.insert(
        coven_merge::ForeignKey::new(["fk"], "t", ["id"]),
        Parent {
            row: test_utils::row(),
            generation: 2,
        },
    );
    assert!(matches!(
        reader.frame(&encode_parts(state.generations(), &cells), &oracle),
        Err(Error::Merge(MergeError::ParentGeneration(2)))
    ));
    let empty = test_utils::TestOracle {
        writes: BTreeMap::new(),
    };
    assert!(matches!(
        reader.frame(&raw(&SnapshotRecord::Merge(original.clone())), &empty),
        Err(Error::Merge(MergeError::MissingWrite(_)))
    ));
    assert_eq!(
        reader
            .frame(&raw(&SnapshotRecord::Merge(original.clone())), &oracle)
            .unwrap(),
        Some(SnapshotRecord::Merge(original))
    );
}
#[test]
fn decoded_snapshot_state_is_directly_usable_by_the_merge() {
    let original = test_utils::merge_row();
    let (writer, first) = SnapshotEncoder::start(single_header(3, Audience::Store, 1)).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let oracle = test_utils::oracle();
    let Some(SnapshotRecord::Merge(decoded)) = reader
        .frame(&raw(&SnapshotRecord::Merge(original.clone())), &oracle)
        .unwrap()
    else {
        panic!("merge row");
    };
    let write = coven_merge::Write::<Value> {
        id: WriteId {
            device: DeviceId(3),
            number: 1,
        },
        timestamp: Timestamp::new(10, 0, DeviceId(3)).unwrap(),
        had_read: oracle.writes.keys().copied().collect(),
        changes: BTreeMap::from([(
            test_utils::row(),
            coven_merge::Change {
                generation: 1,
                operation: coven_merge::Operation::Update(BTreeMap::from([(
                    "x".into(),
                    ColumnValue {
                        value: Value::Integer(5),
                        parents: BTreeMap::new(),
                    },
                )])),
            },
        )]),
    };
    let result = coven_merge::apply(&decoded.state, &write, &oracle).unwrap();
    assert_eq!(
        result,
        coven_merge::apply(&original.state, &write, &oracle).unwrap()
    );
    assert_eq!(result.state.cells()["x"].value.value, Value::Integer(5));
    assert!(result.state.lost().is_empty());
}

#[test]
fn snapshot_fixture_classifies_every_consumed_write_once() {
    let mut reader = SnapshotDecoder::start(
        &test_utils::snapshot_frames()[0],
        &test_utils::snapshot_prefix(&test_utils::snapshot_header()),
    )
    .unwrap();
    let mut classified = BTreeSet::new();
    for frame in &test_utils::snapshot_frames()[1..] {
        if let Some(SnapshotRecord::Write(write)) =
            reader.frame(frame, &test_utils::oracle()).unwrap()
        {
            assert!(classified.insert(write.id));
        }
    }
    reader.finish().unwrap();
    assert_eq!(
        classified,
        test_utils::snapshot_records()
            .into_iter()
            .filter_map(|record| match record {
                SnapshotRecord::Write(write) => Some(write.id),
                _ => None,
            })
            .collect()
    );
}

#[test]
fn applied_write_identity_timestamp_and_past_are_checked() {
    let write = test_utils::oracle().writes.into_values().next().unwrap();
    let mut cases = Vec::new();
    let mut invalid = write.clone();
    invalid.id.number = 0;
    cases.push((invalid, Rule::Required));
    let mut invalid = write.clone();
    invalid.timestamp = Timestamp::new(1, 0, DeviceId(9)).unwrap();
    cases.push((invalid, Rule::TimestampDevice));
    let mut invalid = write;
    invalid.read_through = invalid.slot.0;
    cases.push((invalid, Rule::Coverage));
    for (write, rule) in cases {
        let (mut writer, first) =
            SnapshotEncoder::start(single_header(1, Audience::Store, 1)).unwrap();
        let mut reader =
            SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
        let record = SnapshotRecord::Write(write);
        for result in [
            writer.record(record.clone()).map(|_| ()),
            reader
                .frame(&raw(&record), &test_utils::oracle())
                .map(|_| ()),
        ] {
            assert!(matches!(result, Err(Error::Invalid { rule: actual, .. }) if actual == rule));
        }
    }
}

#[test]
fn creations_name_their_own_insert_or_a_copy_from_another_audience() {
    round_trip(
        SnapshotRecord::Creation(test_utils::creation()),
        Audience::Store,
    );
    let refused = |creation: crate::authorship::Creation| {
        let (mut writer, _) = SnapshotEncoder::start(single_header(6, Audience::Store, 1)).unwrap();
        let record = SnapshotRecord::Creation(creation);
        assert!(writer
            .record(record.clone())
            .and_then(|_| record.validate_coverage(writer.header().covers, &test_utils::oracle()))
            .is_err());
    };
    // A copy names a creation in another audience, by an earlier write.
    let mut copy = test_utils::creation();
    copy.write = test_utils::position();
    copy.creator.row.audience = Audience::Circle(CircleId(Uuid::from_bytes([2; 16])));
    round_trip(SnapshotRecord::Creation(copy.clone()), Audience::Store);
    let mut same_write = copy.clone();
    same_write.creator.write = same_write.write;
    refused(same_write);
    let mut unmoved = test_utils::creation();
    unmoved.write = test_utils::position();
    refused(unmoved);
    let mut other_key = copy.clone();
    other_key.creator.row.key = crate::key::encode_key(&[Value::Integer(3)]).unwrap();
    refused(other_key);
    // Its referenced writes must be within snapshot coverage.
    let mut uncovered = copy;
    uncovered.creator.write = coven_merge::WriteId {
        device: DeviceId(3),
        number: 1,
    };
    refused(uncovered);
}

#[test]
fn write_evidence_has_one_root_per_part_key_and_decodable_openings() {
    round_trip(
        SnapshotRecord::Evidence(test_utils::write_evidence()),
        Audience::Store,
    );
    let refused = |evidence: crate::authorship::WriteEvidence| {
        let (mut writer, _) = SnapshotEncoder::start(single_header(7, Audience::Store, 1)).unwrap();
        let record = SnapshotRecord::Evidence(evidence);
        assert!(writer
            .record(record.clone())
            .and_then(|_| record.validate_coverage(writer.header().covers, &test_utils::oracle()))
            .is_err());
    };
    let mut roots = test_utils::write_evidence();
    roots.roots.push([0; 32]);
    refused(roots);
    let mut header = test_utils::write_evidence();
    header.header.truncate(20);
    refused(header);
    let mut prefix = test_utils::write_evidence();
    prefix.prefix[0] = 33;
    refused(prefix);
    let mut leaf = test_utils::write_evidence();
    leaf.proof.revealed[0].1 = vec![9];
    refused(leaf);
    let mut uncovered = test_utils::write_evidence();
    uncovered.write.device = DeviceId(3);
    refused(uncovered);
}

#[test]
fn claim_record_round_trips_the_complete_slot_digest() {
    for (number, digest) in [
        (0u64, None),
        (1, Some([0x31; 32])),
        (u64::MAX, Some([0x42; 32])),
    ] {
        let (_, first) = SnapshotEncoder::start(single_header(11, Audience::Store, 1)).unwrap();
        let mut reader = SnapshotDecoder::start(
            &first,
            &test_utils::snapshot_prefix(&single_header(11, Audience::Store, 1)),
        )
        .unwrap();
        let frame = encode_frame_with(6, |out| {
            11u8.put(out)?;
            DeviceId(1).put(out)?;
            number.put(out)?;
            digest.put(out)
        })
        .unwrap();
        let record = reader
            .frame(&frame, &test_utils::oracle())
            .unwrap()
            .unwrap();
        assert_eq!(raw(&record), frame);
    }
}

#[test]
fn last_claim_requires_a_digest_exactly_when_its_number_is_positive() {
    for (number, digest) in [(0, Some([0x31; 32])), (1, None)] {
        let record = SnapshotRecord::Claim(LastClaim {
            device: DeviceId(1),
            number,
            digest,
        });
        let (mut writer, first) =
            SnapshotEncoder::start(single_header(11, Audience::Store, 1)).unwrap();
        let mut reader =
            SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
        assert!(writer.record(record.clone()).is_err());
        assert!(reader.frame(&raw(&record), &test_utils::oracle()).is_err());
    }
}

#[test]
fn last_claims_preserve_zero_and_exhausted_counters_and_refuse_duplicate_devices() {
    for number in [0, 1, u64::MAX] {
        round_trip(
            SnapshotRecord::Claim(LastClaim {
                device: DeviceId(1),
                number,
                digest: (number > 0).then_some([0x31; 32]),
            }),
            Audience::Store,
        );
    }
    let (mut writer, first) =
        SnapshotEncoder::start(single_header(11, Audience::Store, 2)).unwrap();
    let mut reader =
        SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header())).unwrap();
    let record = SnapshotRecord::Claim(LastClaim {
        device: DeviceId(2),
        number: 8,
        digest: Some([0x81; 32]),
    });
    let frame = writer.record(record.clone()).unwrap();
    reader.frame(&frame, &test_utils::oracle()).unwrap();
    for device in [1, 2] {
        let invalid = SnapshotRecord::Claim(LastClaim {
            device: DeviceId(device),
            number: 9,
            digest: Some([0x91; 32]),
        });
        assert!(writer.record(invalid.clone()).is_err());
        assert!(reader.frame(&raw(&invalid), &test_utils::oracle()).is_err());
    }
    assert!(writer.finish().is_err());
    assert!(reader.finish().is_err());
}

#[test]
fn original_slot_sections_refuse_other_audiences_and_future_slots() {
    for record in test_utils::snapshot_records().into_iter().filter(|record| {
        matches!(
            record,
            SnapshotRecord::Admin(_) | SnapshotRecord::KeyCopy(_) | SnapshotRecord::Announcement(_)
        )
    }) {
        for header in [
            single_header(
                record.section() as usize,
                Audience::Circle(CircleId(Uuid::from_u128(1))),
                1,
            ),
            SnapshotHeader {
                covers: 0,
                ..single_header(record.section() as usize, Audience::Store, 1)
            },
        ] {
            let (mut writer, first) = SnapshotEncoder::start(header).unwrap();
            let mut reader =
                SnapshotDecoder::start(&first, &test_utils::snapshot_prefix(writer.header()))
                    .unwrap();
            assert!(writer.record(record.clone()).is_err());
            assert!(reader.frame(&raw(&record), &test_utils::oracle()).is_err());
        }
    }
}

#[test]
fn pinned_refused_and_reset_ignored_parts_preserve_claim_numbers() {
    for (bytes, verdict, last_claim) in include_str!("../fixtures/snapshot-claims.hex")
        .lines()
        .map(crate::tests::hex)
        .zip([ChangeVerdict::Refused, ChangeVerdict::Ignored(Slot(2))])
        .zip([8, 9])
        .map(|((bytes, verdict), claim)| (bytes, verdict, claim))
    {
        let mut frames = crate::chunks::FrameDecoder::new(None);
        let mut input = bytes.as_slice();
        let first = frames.next(&mut input).unwrap().unwrap();
        let mut prefix = test_utils::snapshot_prefix(&test_utils::snapshot_header());
        prefix.covers = 10;
        let mut reader = SnapshotDecoder::start(&first, &prefix).unwrap();
        let (mut writer, mut encoded) = SnapshotEncoder::start(reader.header().clone()).unwrap();
        let mut claims = Vec::new();
        let mut authored = 0;
        while let Some(frame) = frames.next(&mut input).unwrap() {
            if let Some(record) = reader.frame(&frame, &test_utils::oracle()).unwrap() {
                match &record {
                    SnapshotRecord::Authored(change) => {
                        assert_eq!(change.verdict, verdict);
                        authored += 1;
                    }
                    SnapshotRecord::Claim(claim) => {
                        claims.push((claim.device, claim.number, claim.digest))
                    }
                    _ => panic!("unexpected fixture section"),
                }
                encoded.extend(writer.record(record).unwrap());
            }
        }
        reader.finish().unwrap();
        encoded.extend(writer.finish().unwrap());
        assert_eq!(encoded, bytes);
        assert_eq!(authored, 1);
        let object = if last_claim == 8 {
            include_str!("../fixtures/sealed-refused-writes-slot.hex")
        } else {
            include_str!("../fixtures/sealed-reset-ignored-writes-slot.hex")
        };
        let mut digest = coven_crypto::ObjectHasher::new();
        digest.update(&crate::tests::hex(object));
        assert_eq!(
            claims,
            [
                (DeviceId(1), last_claim, Some(*digest.finish().as_bytes())),
                (DeviceId(2), 1, Some([0x21; 32]))
            ]
        );
    }
}

#[test]
fn history_extraction_preserves_original_objects_and_claim_digests() {
    let frames = test_utils::snapshot_frames();
    let prefix = test_utils::snapshot_prefix(&test_utils::snapshot_header());
    let mut reader = SnapshotDecoder::start(&frames[0], &prefix).unwrap();
    let mut history = Vec::new();
    for frame in &frames[1..] {
        if let Some(record) = reader.history_frame(frame).unwrap() {
            history.push(record);
        }
    }
    reader.finish().unwrap();
    assert_eq!(
        history,
        test_utils::snapshot_records()
            .into_iter()
            .filter(|record| matches!(
                record,
                SnapshotRecord::Admin(_)
                    | SnapshotRecord::KeyCopy(_)
                    | SnapshotRecord::Announcement(_)
                    | SnapshotRecord::Claim(_)
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn history_extraction_requires_counts_order_and_the_end_marker() {
    let frames = test_utils::snapshot_frames();
    let prefix = test_utils::snapshot_prefix(&test_utils::snapshot_header());
    let open = || SnapshotDecoder::start(&frames[0], &prefix).unwrap();
    let mut reader = open();
    assert!(reader.history_frame(frames.last().unwrap()).is_err());
    let mut reader = open();
    assert!(reader.history_frame(&frames[2]).is_err());
    for removed in 1..frames.len() {
        let mut reader = open();
        let result: Result<Vec<_>, _> = frames
            .iter()
            .enumerate()
            .skip(1)
            .filter(|(index, _)| *index != removed)
            .map(|(_, frame)| reader.history_frame(frame))
            .collect();
        assert!(result.is_err() || reader.finish().is_err());
    }
    let mut reader = open();
    for frame in &frames[1..] {
        reader.history_frame(frame).unwrap();
    }
    assert!(reader.history_frame(frames.last().unwrap()).is_err());
}
