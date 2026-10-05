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
    encode_frame_with(4, |out| record.put(out)).unwrap()
}
fn single_header(section: usize, audience: Audience, count: u64) -> SnapshotHeader {
    let mut header = test_utils::snapshot_header();
    header.counts = [0; 5];
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
        Some(record)
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
        .frame(&encode_frame_with(5, |_| Ok(())).unwrap(), &oracle)
        .is_err());
}
#[test]
fn rejected_frames_do_not_advance_counts_or_order() {
    let (mut writer, first) = SnapshotEncoder::start(test_utils::snapshot_header()).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
    let oracle = test_utils::oracle();
    assert!(writer.finish().is_err());
    assert!(reader
        .frame(&encode_frame_with(5, |_| Ok(())).unwrap(), &oracle)
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
    header.counts = [0; 5];
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
        coven_merge::Rule::ForeignKey("p".into()),
        coven_merge::Rule::Check("positive".into()),
        coven_merge::Rule::DeletedCircle,
        coven_merge::Rule::OtherAudience,
        coven_merge::Rule::Unique("x".into()),
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
                        "fk".into(),
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
        encode_frame_with(4, |out| {
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
        "fk".into(),
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
        LostWriteCause::SchemaChange(entry),
        LostWriteCause::Reset(entry),
    ] {
        round_trip(
            SnapshotRecord::LostWrite(LostWrite {
                write: test_utils::write(),
                cause,
            }),
            Audience::Store,
        );
    }
    let mut write = test_utils::write();
    write.header.disposition = crate::write::WriteDisposition::Lost(entry);
    round_trip(
        SnapshotRecord::LostWrite(LostWrite {
            write: write.clone(),
            cause: LostWriteCause::SchemaChange(entry),
        }),
        Audience::Store,
    );
    for cause in [
        LostWriteCause::Reset(entry),
        LostWriteCause::SchemaChange(crate::value::EntryId { number: 3, ..entry }),
    ] {
        let record = SnapshotRecord::LostWrite(LostWrite {
            write: write.clone(),
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
    let mut write = test_utils::write();
    let mut part = write.parts[0].clone();
    part.audience = circle.clone();
    part.rows[0].row.audience = circle.clone();
    write.parts.push(part);
    let entry = test_utils::loss_entry();
    let make = |write| {
        SnapshotRecord::LostWrite(LostWrite {
            write,
            cause: LostWriteCause::Reset(entry),
        })
    };
    let (mut writer, first) = SnapshotEncoder::start(single_header(4, circle.clone(), 2)).unwrap();
    let mut reader = SnapshotDecoder::start(&first).unwrap();
    let oracle = test_utils::oracle();
    let both = make(write.clone());
    assert!(writer.record(both.clone()).is_err());
    assert!(reader.frame(&raw(&both), &oracle).is_err());
    write.parts.remove(0);
    let good = make(write);
    reader
        .frame(&writer.record(good.clone()).unwrap(), &oracle)
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
    let write = coven_merge::Write {
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
                assert!(classified.insert(lost.write.header.position))
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
