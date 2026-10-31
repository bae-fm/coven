use super::*;
use crate::write_stream::{decode_plaintext, PartHeader, WriteHeaderFrame};
use crate::{encode_frame, test_utils};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{ColumnValue, MergeError, Parent};
use uuid::Uuid;

// Bypass validation to exercise the decoder's independent checks.
fn raw(write: &WriteRecord) -> Vec<u8> {
    let streams: Vec<Vec<u8>> = write
        .parts
        .iter()
        .map(|part| {
            part.records()
                .flat_map(|record| match record {
                    PartRecord::Change(row) => encode_frame(2, row).unwrap(),
                    PartRecord::Execution(execution) => encode_frame(13, execution).unwrap(),
                })
                .collect()
        })
        .collect();
    let header = WriteHeaderFrame {
        header: write.header.clone(),
        parts: write
            .parts
            .iter()
            .zip(&streams)
            .map(|(part, bytes)| PartHeader {
                audience: part.audience.clone(),
                record_count: (part.rows.len() + part.executions.len()) as u64,
                plaintext_length: bytes.len() as u64,
            })
            .collect(),
    };
    let mut bytes = encode_frame(1, &header).unwrap();
    for stream in streams {
        bytes.extend(stream);
    }
    bytes
}
fn refuses(write: WriteRecord, expected: Error) {
    for result in [
        test_utils::write_plaintext(&write).and_then(|_| test_utils::check_past(&write)),
        decode_plaintext(&raw(&write)).and_then(|decoded| test_utils::check_past(&decoded)),
    ] {
        assert_eq!(result.as_ref().map(|_| ()), Err(&expected));
    }
}
fn values() -> BTreeMap<String, ColumnValue<Value>> {
    BTreeMap::from([("x".into(), test_utils::column(Value::Integer(2)))])
}
#[test]
fn merge_operations_own_generation_checks_and_preserve_old_values() {
    for (operation, generation, old) in [
        (Operation::Insert(values()), 0, BTreeMap::new()),
        (Operation::Insert(values()), 2, BTreeMap::new()),
        (Operation::Update(values()), 1, BTreeMap::from([("x".into(), Value::Null)])),
        (Operation::Delete, 1, BTreeMap::from([("x".into(), Value::Integer(1))])),
        (Operation::Delete, 1, BTreeMap::new()),
    ] {
        let mut write = test_utils::write();
        let row = &mut write.parts[0].rows[0];
        // These cases check the net change alone; it has no operations.
        row.execution = Default::default();
        row.change = Change { generation, operation };
        row.old = old;
        if generation == 0 {
            row.sources.generation = None;
        }
        if matches!(row.change.operation, Operation::Insert(_)) {
            row.sources.setters.clear();
        }

        assert_eq!(decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(), write);
        write.parts[0].rows[0].change.generation += 1;
        refuses(write, Error::Merge(MergeError::GenerationParity(generation + 1)));
    }
    let mut write = test_utils::write();
    write.parts[0].rows[0].old.clear();
    refuses(write, Error::Invalid { field: "old columns", rule: Rule::ColumnOperation });
    let mut write = test_utils::write();
    write.parts[0].rows[0].change = Change { generation: u64::MAX, operation: Operation::Delete };
    refuses(write, Error::Merge(MergeError::GenerationExhausted));
}
fn set_parent(write: &mut WriteRecord, part: usize, audience: Audience, generation: u64) {
    let mut values = values();
    let mut row = test_utils::row();
    row.audience = audience;
    values.get_mut("x").unwrap().parents.insert(
        coven_merge::ForeignKey::new(["fk"], row.table.clone(), ["id"]),
        Parent { row, generation },
    );
    write.parts[part].rows[0].change.operation = Operation::Update(values);
}
#[test]
fn row_part_and_reference_audiences_are_enforced() {
    let circle_id = Audience::Circle(CircleId(Uuid::from_bytes([1; 16])));
    let mut write = test_utils::write();
    write.parts[0].audience = circle_id.clone();
    refuses(write, Error::Invalid { field: "write part audience", rule: Rule::Audience });
    let mut write = test_utils::write();
    set_parent(&mut write, 0, circle_id.clone(), 1);
    refuses(write, Error::Merge(MergeError::ReferenceAudience(test_utils::row())));
    let mut write = test_utils::write();
    let mut circle = write.parts[0].clone();
    circle.audience = circle_id.clone();
    circle.rows[0].row.audience = circle_id.clone();
    circle.executions[0].row.audience = circle_id;
    write.parts.push(circle);
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(), write);
    set_parent(&mut write, 1, Audience::Circle(CircleId(Uuid::from_bytes([2; 16]))), 1);
    let row = write.parts[1].rows[0].row.clone();
    refuses(write, Error::Merge(MergeError::ReferenceAudience(row)));
}
#[test]
fn duplicate_rows_and_parts_are_refused() {
    let mut write = test_utils::write();
    write.parts.push(write.parts[0].clone());
    refuses(write, Error::Invalid { field: "write parts", rule: Rule::Order });
    let mut write = test_utils::write();
    let row = write.parts[0].rows[0].clone();
    write.parts[0].rows.push(row);
    refuses(write, Error::Invalid { field: "write part rows", rule: Rule::Order });
}
#[test]
fn timestamp_and_parent_generation_are_checked() {
    let mut write = test_utils::write();
    write.header.timestamp = Timestamp::new(2, 3, DeviceId(2)).unwrap();
    refuses(write, Error::Invalid { field: "write timestamp", rule: Rule::TimestampDevice });
    let mut write = test_utils::write();
    set_parent(&mut write, 0, Audience::Store, 2);
    refuses(write, Error::Merge(MergeError::ParentGeneration(2)));
}
#[test]
fn a_lost_disposition_is_never_transmitted() {
    // Tag 1 is unused: exclusion is a received interpretation (D5).
    let write = test_utils::write();
    let mut bytes = test_utils::write_plaintext(&write).unwrap();
    // Frame prefix, write identity, timestamp, read-through and schema.
    let at = crate::FRAME_PREFIX_LEN + 32 + 8 + 4;
    assert_eq!(bytes[at], 0);
    bytes[at] = 1;
    assert!(matches!(
        decode_plaintext(&bytes),
        Err(Error::UnknownTag { field: "write disposition", tag: 1 })
    ));
}

#[test]
fn only_migration_writes_have_no_parts() {
    let mut write = test_utils::write();
    write.parts.clear();
    {
        write.header.disposition = WriteDisposition::Apply;
        refuses(write.clone(), Error::Invalid { field: "write parts", rule: Rule::Required });
    }
    write.header.disposition = WriteDisposition::Migration;
    let bytes = test_utils::write_plaintext(&write).unwrap();
    assert_eq!(decode_plaintext(&bytes).unwrap(), write);
    write.parts = test_utils::write().parts;
    refuses(write, Error::Invalid { field: "migration write parts", rule: Rule::StreamLength });
}

#[test]
fn source_presence_reads_and_replaced_roles_are_checked_by_both_codecs() {
    let mut missing = test_utils::write();
    missing.parts[0].rows[0].sources.generation = None;
    refuses(missing, Error::Invalid { field: "generation source", rule: Rule::Generation });
    let mut unread = test_utils::write();
    unread.parts[0].rows[0].sources.generation.as_mut().unwrap().write = unread.header.position;
    refuses(unread, Error::Invalid { field: "source read", rule: Rule::Coverage });
    let mut wrong_role = test_utils::write();
    let row = &mut wrong_role.parts[0].rows[0];
    row.sources.setters.insert("absent".into(), row.sources.setters["x"].clone());
    refuses(wrong_role, Error::Invalid { field: "replaced setters", rule: Rule::ColumnOperation });
}

#[test]
fn source_roles_are_canonical_and_repeated_identities_are_preserved() {
    let mut write = test_utils::write();
    let row = &mut write.parts[0].rows[0];
    row.change.operation = Operation::Delete;
    row.old.clear();
    row.execution = Default::default();
    let change = row.sources.generation.clone().unwrap();
    let setter =
        crate::write_sources::CellSource { change: change.clone(), column: "original".into() };
    row.sources.setters = BTreeMap::from([("b".into(), setter.clone()), ("a".into(), setter)]);
    assert_eq!(row.sources.changes().collect::<Vec<_>>(), vec![&change, &change, &change]);
    let sources = encode_frame(99, &row.sources).unwrap();
    let generation = encode_frame(99, &row.sources.generation).unwrap();
    let encoded = row.encode().unwrap();
    // No copied creation and an empty execution follow the sources.
    let tail = [0u8; 10];
    assert!(encoded[..encoded.len() - tail.len()].ends_with(&sources[7..]));
    assert!(encoded.ends_with(&tail));
    assert_eq!(RowChange::decode(&encoded).unwrap(), *row);
    let mut malformed = encoded.clone();
    let source_offset = encoded.len() - tail.len() - (sources.len() - 7);
    // Move the first setter key after the second: the map must refuse this order.
    let first_name = source_offset + (generation.len() - 7) + 4 + 4;
    assert_eq!(malformed[first_name], b'a');
    malformed[first_name] = b'c';
    assert!(matches!(RowChange::decode(&malformed), Err(Error::Invalid { rule: Rule::Order, .. })));
    malformed[first_name] = b'b';
    assert!(matches!(RowChange::decode(&malformed), Err(Error::Invalid { rule: Rule::Order, .. })));
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(), write);
}

#[test]
fn operations_are_ordered_continuous_and_start_at_the_change_generation() {
    let write = test_utils::write();
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(), write);
    let mut same = test_utils::write();
    same.parts[0].executions[0].execution.steps[1].sequence = 1;
    refuses(same, Error::Invalid { field: "execution steps", rule: Rule::Order });
    let mut gap = test_utils::write();
    gap.parts[0].executions[0].execution.steps[1].change.generation = 3;
    refuses(gap, Error::Invalid { field: "execution step generation", rule: Rule::Generation });
    let mut parity = test_utils::write();
    parity.parts[0].executions[0].execution.steps[0].change.generation = 1;
    refuses(parity, Error::Merge(MergeError::GenerationParity(1)));
    let mut key_update = test_utils::write();
    key_update.parts[0].executions[0].execution.steps[0].key_update = true;
    refuses(key_update, Error::Invalid { field: "key update step", rule: Rule::ColumnOperation });
    // Only a delete is the old half of a primary-key update.
    let mut moved = test_utils::write();
    moved.parts[0].executions[0].execution.steps[1].key_update = true;
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&moved).unwrap()).unwrap(), moved);
    let mut empty = test_utils::write();
    empty.parts[0].executions[0].execution.steps.clear();
    refuses(empty, Error::Invalid { field: "execution steps", rule: Rule::Required });
    // A row has one record: its change, or its operations alone.
    let mut both = test_utils::write();
    both.parts[0].executions[0].row = both.parts[0].rows[0].row.clone();
    assert_eq!(
        test_utils::write_plaintext(&both),
        Err(Error::Invalid { field: "write part executions", rule: Rule::Audience })
    );
    // The stream refuses the second record for the row as out of order.
    assert_eq!(
        decode_plaintext(&raw(&both)),
        Err(Error::Invalid { field: "write part rows", rule: Rule::Order })
    );
    // An operation's parent obeys the same audience rule as a change's.
    let mut parent = test_utils::write();
    let circle = Audience::Circle(CircleId(Uuid::from_bytes([1; 16])));
    let Operation::Insert(columns) =
        &mut parent.parts[0].executions[0].execution.steps[0].change.operation
    else {
        unreachable!()
    };
    let mut row = test_utils::row();
    row.audience = circle;
    columns.get_mut("x").unwrap().parents.insert(
        coven_merge::ForeignKey::new(["fk"], row.table.clone(), ["id"]),
        Parent { row, generation: 1 },
    );
    let execution_row = parent.parts[0].executions[0].row.clone();
    refuses(parent, Error::Merge(MergeError::ReferenceAudience(execution_row)));
}

fn copied(write: &mut WriteRecord) -> &mut RowChange {
    let row = &mut write.parts[0].rows[0];
    row.change = Change { generation: 0, operation: Operation::Insert(values()) };
    row.old.clear();
    row.sources = Default::default();
    row.execution = Default::default();
    row.creator = Some(ChangeId {
        write: WriteId { device: DeviceId(2), number: 1 },
        row: RowId {
            audience: Audience::Circle(CircleId(Uuid::from_bytes([1; 16]))),
            ..test_utils::row()
        },
    });
    row
}

#[test]
fn only_a_generation_zero_insert_copies_another_audiences_creation() {
    let mut write = test_utils::write();
    copied(&mut write);
    assert_eq!(decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(), write);
    let invalid = || Error::Invalid { field: "copied creation", rule: Rule::ColumnOperation };
    let mut same_audience = test_utils::write();
    copied(&mut same_audience).creator.as_mut().unwrap().row.audience = Audience::Store;
    refuses(same_audience, invalid());
    let mut other_key = test_utils::write();
    copied(&mut other_key).creator.as_mut().unwrap().row.key =
        crate::key::encode_key(&[Value::Text("other".into())]).unwrap();
    refuses(other_key, invalid());
    let mut other_table = test_utils::write();
    copied(&mut other_table).creator.as_mut().unwrap().row.table = "u".into();
    refuses(other_table, invalid());
    let mut readd = test_utils::write();
    let row = copied(&mut readd);
    row.change.generation = 2;
    row.sources.generation = test_utils::write().parts[0].rows[0].sources.generation.clone();
    refuses(readd, invalid());
    let mut update = test_utils::write();
    update.parts[0].rows[0].creator = copied(&mut test_utils::write()).creator.clone();
    refuses(update, invalid());
}

#[test]
fn copied_creations_and_operation_sources_must_have_been_read() {
    let mut unread = test_utils::write();
    copied(&mut unread).creator.as_mut().unwrap().write.device = DeviceId(3);
    refuses(unread, Error::Invalid { field: "copied creation read", rule: Rule::Coverage });
    let mut source = test_utils::write();
    let unread =
        ChangeId { write: WriteId { device: DeviceId(3), number: 1 }, row: test_utils::row() };
    source.parts[0].executions[0].execution.sources.generation = Some(unread.clone());
    refuses(source, Error::Invalid { field: "source read", rule: Rule::Coverage });
    // Sources the operations replaced beyond the change's own are read too.
    let mut setter = test_utils::write();
    setter.parts[0].rows[0].execution.sources.setters.insert(
        "y".into(),
        crate::write_sources::CellSource { change: unread, column: "y".into() },
    );
    refuses(setter, Error::Invalid { field: "source read", rule: Rule::Coverage });
}

/// A step that is exactly its row change's own change is written once; the
/// same step written out again has a second encoding and is refused (D5).
#[test]
fn a_step_equal_to_its_row_change_is_written_once() {
    let mut change = test_utils::write().parts.remove(0).rows.remove(0);
    change.execution = Execution {
        steps: vec![Step { sequence: 0, change: change.change.clone(), key_update: false }],
        sources: Default::default(),
    };
    let compact = change.encode().unwrap();
    assert_eq!(RowChange::decode(&compact).unwrap(), change);
    // The step's own bytes: sequence, tag 0, key update.
    let mut tail = Encoder::new();
    change.execution.put_against(&mut tail, Some(&change.change)).unwrap();
    assert_eq!(
        tail.bytes,
        [&[0, 0, 0, 1][..], &[0, 0, 0, 0], &[0], &[0], &[0], &[0, 0, 0, 0]].concat()
    );
    // The same step spelled out in full.
    let mut full = Encoder::new();
    change.row.put(&mut full).unwrap();
    change.change.put(&mut full).unwrap();
    change.old.put(&mut full).unwrap();
    change.sources.put(&mut full).unwrap();
    change.creator.put(&mut full).unwrap();
    1u32.put(&mut full).unwrap();
    0u32.put(&mut full).unwrap();
    1u8.put(&mut full).unwrap();
    change.change.put(&mut full).unwrap();
    0u8.put(&mut full).unwrap();
    change.execution.sources.put(&mut full).unwrap();
    let mut input = Decoder::new(&full.bytes).unwrap();
    assert!(matches!(RowChange::get(&mut input), Err(Error::Invalid { field: "step change", .. })));
    // An execution record has no own change: a `0` step is unknown there.
    let mut record = Encoder::new();
    change.row.put(&mut record).unwrap();
    1u32.put(&mut record).unwrap();
    0u32.put(&mut record).unwrap();
    0u8.put(&mut record).unwrap();
    let mut input = Decoder::new(&record.bytes).unwrap();
    assert!(matches!(
        RowExecution::get(&mut input),
        Err(Error::UnknownTag { field: "step change", tag: 0 })
    ));
}
