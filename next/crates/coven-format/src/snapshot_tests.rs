use super::*;
use crate::test_utils;
use crate::value::Value;
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{
    Audience, Cell, ColumnValue, LostKey, LostValue, MergeError, Parent, RowState, Timestamp,
};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn raw(record: &SnapshotRecord) -> Vec<u8> {
    encode_frame_with(6, |out| record.put(out)).unwrap()
}
fn single_header(section: usize, audience: Audience, count: u64) -> SnapshotHeader {
    let mut header = test_utils::snapshot_header();
    header.counts = [0; 6];
    header.counts[section] = count;
    header.id.audience = audience;
    header
}
fn round_trip(record: SnapshotRecord, audience: Audience) {
    let (mut writer, first) =
        SnapshotEncoder::start(single_header(record.section() as usize, audience, 1)).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
    assert_eq!(
        reader
            .frame(
                &writer.record(record.clone()).unwrap(),
                &test_utils::oracle()
            )
            .unwrap(),
        Some(record.clone())
    );
    if let SnapshotRecord::LostWrite(write) = record {
        let mut row = test_utils::lost_write_row();
        row.change.row.audience = write.audience;
        let row = SnapshotRecord::LostWriteRow(row);
        assert_eq!(
            reader
                .frame(&writer.record(row.clone()).unwrap(), &test_utils::oracle())
                .unwrap(),
            Some(row)
        );
    }
    reader
        .frame(&writer.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    reader.finish().unwrap();
}
#[test]
fn every_section_streams_with_write_metadata_supplied_by_its_consumer() {
    let header = test_utils::snapshot_header();
    let (mut writer, first) = SnapshotEncoder::start(header.clone()).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
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
    let mut reader = SnapshotDecoder::start(&first).unwrap();
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
    let mut reader = SnapshotDecoder::start(&first).unwrap();
    let oracle = test_utils::oracle();
    let make = |number| {
        SnapshotRecord::Write(AppliedWrite {
            id: WriteId {
                device: DeviceId(1),
                number,
            },
            timestamp: Timestamp::new(number, 0, DeviceId(1)).unwrap(),
            had_read: WritePositions(vec![]),
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
    let mut reader = SnapshotDecoder::start(&first).unwrap();
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
    header.counts = [0; 6];
    let (mut writer, first) = SnapshotEncoder::start(header).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
    let oracle = test_utils::oracle();
    assert!(reader.frame(&first, &oracle).is_err());
    let end = writer.finish().unwrap();
    assert!(SnapshotDecoder::start(&end).is_err());
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
fn every_merge_rule_round_trips_including_another_circle_winning() {
    let row = test_utils::merge_row();
    let mut id = row.state.row().clone();
    id.audience = Audience::Circle(CircleId(Uuid::from_bytes([1; 16])));
    let state = RowState::from_parts(
        id.clone(),
        row.state.generations().clone(),
        row.state.cells().clone(),
        row.state.lost().clone(),
        &test_utils::oracle(),
    )
    .unwrap();
    let removed = BTreeSet::from([
        coven_merge::Rule::ForeignKey(coven_merge::ForeignKey::new(["p"], "t", ["id"])),
        coven_merge::Rule::Check("positive".into()),
        coven_merge::Rule::DeletedCircle,
        coven_merge::Rule::OtherAudience,
        coven_merge::Rule::Unique(["x"].into()),
    ]);
    // OtherAudience carries no invented winning-audience restriction: merge decides it.
    round_trip(
        SnapshotRecord::Merge(MergeRow { state, removed }),
        id.audience,
    );
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
        let mut reader = SnapshotDecoder::start(&first).unwrap();
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
                        cells: &BTreeMap<String, Cell<Value>>,
                        lost: &BTreeMap<LostKey, LostValue<Value>>| {
        encode_frame_with(6, |out| {
            3u8.put(out)?;
            state.row().put(out)?;
            generations.put(out)?;
            cells.put(out)?;
            lost.put(out)?;
            BTreeSet::<coven_merge::Rule>::new().put(out)
        })
        .unwrap()
    };
    let (_, header) = SnapshotEncoder::start(single_header(3, Audience::Store, 1)).unwrap();
    let mut reader = SnapshotDecoder::start(&header).unwrap();
    let oracle = test_utils::oracle();
    let mut generations = state.generations().clone();
    let first = generations.remove(&1).unwrap();
    generations.insert(2, first);
    assert!(matches!(
        reader.frame(
            &encode_parts(&generations, state.cells(), state.lost()),
            &oracle
        ),
        Err(Error::Merge(MergeError::GenerationGap(1)))
    ));
    generations = state.generations().clone();
    generations.insert(2, state.cells()["x"].write);
    assert!(matches!(
        reader.frame(
            &encode_parts(&generations, state.cells(), state.lost()),
            &oracle
        ),
        Err(Error::Merge(MergeError::DeletedRowHasCells))
    ));
    let mut lost = state.lost().clone();
    lost.values_mut().next().unwrap().replaced_by = first;
    assert!(matches!(
        reader.frame(
            &encode_parts(state.generations(), state.cells(), &lost),
            &oracle
        ),
        Err(Error::Merge(MergeError::InvalidLostValue(_)))
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
        reader.frame(
            &encode_parts(state.generations(), &cells, state.lost()),
            &oracle
        ),
        Err(Error::Merge(MergeError::ParentGeneration(2)))
    ));
    let empty = test_utils::TestOracle {
        writes: BTreeMap::new(),
    };
    assert!(matches!(
        reader.frame(&raw(&SnapshotRecord::Merge(original.clone())), &empty),
        Err(Error::Merge(MergeError::MissingWrite(_)))
    ));
    let mut read = oracle.clone();
    read.writes
        .get_mut(&state.cells()["x"].write)
        .unwrap()
        .had_read = WritePositions(vec![state.lost().keys().next().unwrap().write]);
    assert!(matches!(
        reader.frame(&raw(&SnapshotRecord::Merge(original.clone())), &read),
        Err(Error::Merge(MergeError::InvalidLostValue(_)))
    ));
    assert_eq!(
        reader
            .frame(&raw(&SnapshotRecord::Merge(original.clone())), &oracle)
            .unwrap(),
        Some(SnapshotRecord::Merge(original))
    );
}
#[test]
fn lost_writes_preserve_both_causes_and_require_matching_author_markers() {
    let entry = test_utils::loss_entry();
    for cause in [
        LostWriteCause::SchemaChange(1),
        LostWriteCause::Reset(entry),
    ] {
        round_trip(
            SnapshotRecord::LostWrite(LostWrite {
                header: test_utils::write().header,
                audience: Audience::Store,
                row_count: 1,
                cause,
            }),
            Audience::Store,
        );
    }
    let mut write = test_utils::write();
    write.header.disposition = crate::write::WriteDisposition::Lost(1);
    round_trip(
        SnapshotRecord::LostWrite(LostWrite {
            header: write.header.clone(),
            audience: Audience::Store,
            row_count: 1,
            cause: LostWriteCause::SchemaChange(1),
        }),
        Audience::Store,
    );
    for cause in [
        LostWriteCause::Reset(entry),
        LostWriteCause::SchemaChange(2),
    ] {
        let record = SnapshotRecord::LostWrite(LostWrite {
            header: write.header.clone(),
            audience: Audience::Store,
            row_count: 1,
            cause,
        });
        let (mut writer, first) =
            SnapshotEncoder::start(single_header(4, Audience::Store, 1)).unwrap();
        let mut reader = SnapshotDecoder::start(&first).unwrap();
        assert!(matches!(
            writer.record(record.clone()),
            Err(Error::Invalid {
                rule: Rule::LostWriteCause,
                ..
            })
        ));
        assert!(matches!(
            reader.frame(&raw(&record), &test_utils::oracle()),
            Err(Error::Invalid {
                rule: Rule::LostWriteCause,
                ..
            })
        ));
    }
}
#[test]
fn lost_write_audience_cause_coverage_and_write_order_are_checked() {
    let circle = Audience::Circle(CircleId(Uuid::from_bytes([1; 16])));
    let entry = test_utils::loss_entry();
    let (mut writer, first) = SnapshotEncoder::start(single_header(4, circle.clone(), 2)).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
    let oracle = test_utils::oracle();
    let wrong = SnapshotRecord::LostWrite(test_utils::lost_write());
    assert!(writer.record(wrong.clone()).is_err());
    assert!(reader.frame(&raw(&wrong), &oracle).is_err());
    let good = SnapshotRecord::LostWrite(LostWrite {
        audience: circle.clone(),
        ..test_utils::lost_write()
    });
    reader
        .frame(&writer.record(good.clone()).unwrap(), &oracle)
        .unwrap();
    let mut row = test_utils::lost_write_row();
    row.change.row.audience = circle;
    reader
        .frame(
            &writer.record(SnapshotRecord::LostWriteRow(row)).unwrap(),
            &oracle,
        )
        .unwrap();
    assert!(writer.record(good.clone()).is_err());
    assert!(reader.frame(&raw(&good), &oracle).is_err());
    let mut missing_cause = match good {
        SnapshotRecord::LostWrite(v) => v,
        _ => unreachable!(),
    };
    missing_cause.cause = LostWriteCause::Reset(crate::value::EntryId { number: 3, ..entry });
    assert!(matches!(
        reader.frame(&raw(&SnapshotRecord::LostWrite(missing_cause)), &oracle),
        Err(Error::Invalid {
            rule: Rule::Coverage,
            ..
        })
    ));
}
#[test]
fn decoded_snapshot_state_is_directly_usable_by_the_merge() {
    let original = test_utils::merge_row();
    let (_, first) = SnapshotEncoder::start(single_header(3, Audience::Store, 1)).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
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
    let mut reader = SnapshotDecoder::start(&test_utils::snapshot_frames()[0]).unwrap();
    let mut classified = BTreeSet::new();
    for frame in &test_utils::snapshot_frames()[1..] {
        match reader.frame(frame, &test_utils::oracle()).unwrap() {
            Some(SnapshotRecord::Write(write)) => assert!(classified.insert(write.id)),
            Some(SnapshotRecord::LostWrite(lost)) => {
                assert!(classified.insert(lost.header.position))
            }
            _ => {}
        }
    }
    reader.finish().unwrap();
    for position in &reader.header().writes.0 {
        for number in 1..=position.number {
            assert!(classified.contains(&WriteId {
                device: position.device,
                number
            }));
        }
    }
}
#[test]
fn schema_loss_coverage_uses_the_snapshot_version_without_a_store_log_entry() {
    let mut header = single_header(4, Audience::Store, 1);
    header.schema_version = 7;
    header.store_log.0.clear();
    let (mut writer, first) = SnapshotEncoder::start(header).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
    let make = |version| {
        SnapshotRecord::LostWrite(LostWrite {
            header: test_utils::write().header,
            audience: Audience::Store,
            row_count: 1,
            cause: LostWriteCause::SchemaChange(version),
        })
    };
    for record in [make(0), make(8)] {
        assert!(writer.record(record.clone()).is_err());
        assert!(reader.frame(&raw(&record), &test_utils::oracle()).is_err());
    }
    assert_eq!(
        reader
            .frame(&writer.record(make(7)).unwrap(), &test_utils::oracle())
            .unwrap(),
        Some(make(7))
    );
    reader
        .frame(
            &writer
                .record(SnapshotRecord::LostWriteRow(test_utils::lost_write_row()))
                .unwrap(),
            &test_utils::oracle(),
        )
        .unwrap();
    reader
        .frame(&writer.finish().unwrap(), &test_utils::oracle())
        .unwrap();
    reader.finish().unwrap();
}

#[test]
fn lost_row_records_require_their_header_audience_order_and_exact_count() {
    let mut header = test_utils::snapshot_header();
    header.counts = [0, 0, 0, 0, 1, 0];
    let (mut encoder, frame) = SnapshotEncoder::start(header).unwrap();
    let mut decoder = SnapshotDecoder::start(&frame).unwrap();
    let oracle = test_utils::oracle();
    let row = SnapshotRecord::LostWriteRow(test_utils::lost_write_row());
    assert!(encoder.record(row.clone()).is_err());
    assert!(decoder.frame(&raw(&row), &oracle).is_err());
    let lost = SnapshotRecord::LostWrite(LostWrite {
        row_count: 2,
        ..test_utils::lost_write()
    });
    decoder
        .frame(&encoder.record(lost.clone()).unwrap(), &oracle)
        .unwrap();
    let mut wrong_audience = test_utils::lost_write_row();
    wrong_audience.change.row.audience = Audience::Circle(CircleId(Uuid::from_u128(1)));
    for invalid in [SnapshotRecord::LostWriteRow(wrong_audience), lost] {
        assert!(encoder.record(invalid.clone()).is_err());
        assert!(decoder.frame(&raw(&invalid), &oracle).is_err());
    }
    decoder
        .frame(&encoder.record(row.clone()).unwrap(), &oracle)
        .unwrap();
    assert!(encoder.record(row.clone()).is_err());
    assert!(decoder.frame(&raw(&row), &oracle).is_err());
    assert!(encoder.finish().is_err());
    assert!(decoder
        .frame(&encode_frame_with(7, |_| Ok(())).unwrap(), &oracle)
        .is_err());
    let mut next = test_utils::lost_write_row();
    next.change.row.key = crate::key::encode_key(&[Value::Text("later".into())]).unwrap();
    decoder
        .frame(
            &encoder.record(SnapshotRecord::LostWriteRow(next)).unwrap(),
            &oracle,
        )
        .unwrap();
    assert!(encoder.record(row.clone()).is_err());
    assert!(decoder.frame(&raw(&row), &oracle).is_err());
    decoder.frame(&encoder.finish().unwrap(), &oracle).unwrap();
    decoder.finish().unwrap();
}

#[test]
fn lost_rows_belong_to_the_preceding_header_without_repeating_its_id() {
    let first = test_utils::lost_write();
    let mut second = first.clone();
    second.header.position.number += 1;
    let mut header = single_header(4, Audience::Store, 2);
    header.writes.0[0] = second.header.position;
    let (mut encoder, frame) = SnapshotEncoder::start(header).unwrap();
    let mut decoder = SnapshotDecoder::start(&frame).unwrap();
    let oracle = test_utils::oracle();
    let row = SnapshotRecord::LostWriteRow(test_utils::lost_write_row());
    let mut row_frames = Vec::new();
    for write in [first, second] {
        let header = SnapshotRecord::LostWrite(write);
        assert_eq!(
            decoder
                .frame(&encoder.record(header.clone()).unwrap(), &oracle)
                .unwrap(),
            Some(header)
        );
        let frame = encoder.record(row.clone()).unwrap();
        assert_eq!(decoder.frame(&frame, &oracle).unwrap(), Some(row.clone()));
        row_frames.push(frame);
    }
    assert_eq!(row_frames[0], row_frames[1]);
    decoder.frame(&encoder.finish().unwrap(), &oracle).unwrap();
    decoder.finish().unwrap();
}
