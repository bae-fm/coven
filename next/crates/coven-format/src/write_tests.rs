use super::*;
use crate::write_stream::{decode_plaintext, PartHeader, WriteHeaderFrame};
use crate::{encode_frame, test_utils};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{ColumnValue, MergeError, Parent};
use uuid::Uuid;

#[test]
fn header_contains_both_write_and_store_log_frontiers() {
    let record = test_utils::write();
    let encoder = crate::write_stream::WriteEncoder::new(&record).unwrap();
    // D5: identity, timestamp, one write position, the empty store-log
    // frontier, schema, disposition, and one audience stream descriptor.
    assert_eq!(
        encoder.header_frame().len(),
        7 + 16 + 16 + 20 + 4 + 4 + 1 + 4 + 17
    );
}

// Bypass validation to exercise the decoder's independent checks.
fn raw(write: &WriteRecord) -> Vec<u8> {
    let streams: Vec<Vec<u8>> = write
        .parts
        .iter()
        .map(|part| {
            part.rows
                .iter()
                .flat_map(|row| encode_frame(2, row).unwrap())
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
                record_count: part.rows.len() as u64,
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
        test_utils::write_plaintext(&write).map(|_| ()),
        decode_plaintext(&raw(&write)).map(|_| ()),
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
        (
            Operation::Update(values()),
            1,
            BTreeMap::from([("x".into(), Value::Null)]),
        ),
        (
            Operation::Delete,
            1,
            BTreeMap::from([("x".into(), Value::Integer(1))]),
        ),
        (Operation::Delete, 1, BTreeMap::new()),
    ] {
        let mut write = test_utils::write();
        let row = &mut write.parts[0].rows[0];
        row.change = Change {
            generation,
            operation,
        };
        row.old = old;
        assert_eq!(
            decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(),
            write
        );
        write.parts[0].rows[0].change.generation += 1;
        refuses(
            write,
            Error::Merge(MergeError::GenerationParity(generation + 1)),
        );
    }
    let mut write = test_utils::write();
    write.parts[0].rows[0].old.clear();
    refuses(
        write,
        Error::Invalid {
            field: "old columns",
            rule: Rule::ColumnOperation,
        },
    );
    let mut write = test_utils::write();
    write.parts[0].rows[0].change = Change {
        generation: u64::MAX,
        operation: Operation::Delete,
    };
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
    refuses(
        write,
        Error::Invalid {
            field: "write part audience",
            rule: Rule::Audience,
        },
    );
    let mut write = test_utils::write();
    set_parent(&mut write, 0, circle_id.clone(), 1);
    refuses(
        write,
        Error::Merge(MergeError::ReferenceAudience(test_utils::row())),
    );
    let mut write = test_utils::write();
    let mut circle = write.parts[0].clone();
    circle.audience = circle_id.clone();
    circle.rows[0].row.audience = circle_id;
    write.parts.push(circle);
    assert_eq!(
        decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(),
        write
    );
    set_parent(
        &mut write,
        1,
        Audience::Circle(CircleId(Uuid::from_bytes([2; 16]))),
        1,
    );
    let row = write.parts[1].rows[0].row.clone();
    refuses(write, Error::Merge(MergeError::ReferenceAudience(row)));
}
#[test]
fn duplicate_rows_and_parts_are_refused() {
    let mut write = test_utils::write();
    write.parts.push(write.parts[0].clone());
    refuses(
        write,
        Error::Invalid {
            field: "write parts",
            rule: Rule::Order,
        },
    );
    let mut write = test_utils::write();
    let row = write.parts[0].rows[0].clone();
    write.parts[0].rows.push(row);
    refuses(
        write,
        Error::Invalid {
            field: "write part rows",
            rule: Rule::Order,
        },
    );
}
#[test]
fn timestamp_had_read_and_parent_generation_are_checked() {
    let mut write = test_utils::write();
    write.header.timestamp = Timestamp::new(2, 3, DeviceId(2)).unwrap();
    refuses(
        write,
        Error::Invalid {
            field: "write timestamp",
            rule: Rule::TimestampDevice,
        },
    );
    let mut write = test_utils::write();
    write.header.had_read = WritePositions(vec![write.header.position]);
    refuses(
        write,
        Error::Invalid {
            field: "had-read own device",
            rule: Rule::OwnPosition,
        },
    );
    let mut write = test_utils::write();
    set_parent(&mut write, 0, Audience::Store, 2);
    refuses(write, Error::Merge(MergeError::ParentGeneration(2)));
}
#[test]
fn schema_change_marker_uses_a_positive_schema_version() {
    let mut write = test_utils::write();
    write.header.disposition = WriteDisposition::Lost(u32::MAX);
    assert_eq!(
        decode_plaintext(&test_utils::write_plaintext(&write).unwrap()).unwrap(),
        write
    );
    write.header.disposition = WriteDisposition::Lost(0);
    refuses(
        write,
        Error::Invalid {
            field: "breaking schema version",
            rule: Rule::Required,
        },
    );
}

#[test]
fn only_migration_writes_have_no_parts() {
    let mut write = test_utils::write();
    write.parts.clear();
    for disposition in [WriteDisposition::Apply, WriteDisposition::Lost(2)] {
        write.header.disposition = disposition;
        refuses(
            write.clone(),
            Error::Invalid {
                field: "write parts",
                rule: Rule::Required,
            },
        );
    }
    write.header.disposition = WriteDisposition::Migration;
    let mut lost = test_utils::lost_write();
    lost.header = write.header.clone();
    assert_eq!(
        lost.validate(),
        Err(Error::Invalid {
            field: "migration write parts",
            rule: Rule::StreamLength,
        })
    );
    let bytes = test_utils::write_plaintext(&write).unwrap();
    assert_eq!(decode_plaintext(&bytes).unwrap(), write);
    assert_eq!(
        bytes,
        include_str!("../fixtures/migration.hex")
            .trim()
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>()
    );
    write.parts = test_utils::write().parts;
    refuses(
        write,
        Error::Invalid {
            field: "migration write parts",
            rule: Rule::StreamLength,
        },
    );
}
