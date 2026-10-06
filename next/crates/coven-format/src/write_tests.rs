use super::*;
use crate::{encode_frame, test_utils, Object};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{ColumnValue, MergeError, Parent};
use uuid::Uuid;

fn refuses(write: WriteRecord, expected: Error) {
    for result in [
        Object::Write(write.clone()).encode().map(|_| ()),
        Object::decode(&encode_frame(1, &write).unwrap()).map(|_| ()),
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
        let object = Object::Write(write.clone());
        assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
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
    let object = Object::Write(write.clone());
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
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
fn schema_change_marker_uses_a_store_log_identity() {
    let mut write = test_utils::write();
    write.header.disposition = WriteDisposition::Lost(EntryId {
        device: DeviceId(1),
        number: 1,
    });
    let object = Object::Write(write.clone());
    assert_eq!(Object::decode(&object.encode().unwrap()).unwrap(), object);
    write.header.disposition = WriteDisposition::Lost(EntryId {
        device: DeviceId(1),
        number: 0,
    });
    refuses(
        write,
        Error::Invalid {
            field: "log number",
            rule: Rule::Required,
        },
    );
}
