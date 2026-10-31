use super::*;
use coven_merge::ColumnValue;
use std::collections::BTreeMap;

fn cell(value: Value) -> ColumnValue<Value> {
    ColumnValue { value, parents: BTreeMap::new() }
}

fn record(coordinates: &str, hash: Value) -> BTreeMap<String, ColumnValue<Value>> {
    BTreeMap::from([
        (ID.into(), cell(Value::Text("0b7e5c1a-3d2f-4e8b-9c6a-1f0e2d3c4b5a".into()))),
        (AUDIENCE.into(), cell(Value::Text("store".into()))),
        (SIZE.into(), cell(Value::Integer(5))),
        (HASH.into(), cell(hash)),
        (COORDINATES.into(), cell(Value::Text(coordinates.into()))),
        (NAMESPACE.into(), cell(Value::Text("photos".into()))),
    ])
}

fn check(columns: &BTreeMap<String, ColumnValue<Value>>, complete: bool) -> bool {
    check_cells(&columns.iter().map(|(n, v)| (n.as_str(), v)).collect(), complete).is_ok()
}

const STORED: &str = "file 1 0b7e5c1a-3d2f-4e8b-9c6a-1f0e2d3c4b5a \
    7777777777777777777777777777777777777777777777777777777777777777";

#[test]
fn a_record_is_in_storage_exactly_when_it_has_a_content_hash() {
    assert!(check(&record("device 1", Value::Null), true));
    assert!(check(&record(STORED, Value::Blob(vec![1; 32])), true));
    assert!(!check(&record("device 1", Value::Blob(vec![1; 32])), true));
    assert!(!check(&record(STORED, Value::Null), true));
    assert!(!check(&record(STORED, Value::Blob(vec![1; 31])), true));
    assert!(!check(&record("somewhere", Value::Null), true));
}

#[test]
fn a_later_change_sets_only_where_the_file_is() {
    let full = record(STORED, Value::Blob(vec![1; 32]));
    let place: BTreeMap<_, _> = full
        .iter()
        .filter(|(name, _)| PLACE.contains(&name.as_str()))
        .map(|(n, v)| (n.clone(), v.clone()))
        .collect();
    assert!(check(&place, false));
    let mut size = place.clone();
    size.insert(SIZE.into(), cell(Value::Integer(6)));
    assert!(!check(&size, false));
    let mut half = place;
    half.remove(HASH);
    assert!(!check(&half, false));
    let mut without = full;
    without.remove(SIZE);
    assert!(!check(&without, true));
}

#[test]
fn renames_carry_references_and_drops_end_them() {
    let names = crate::migration_names::MigrationMatch {
        tables: BTreeMap::from([("photos".into(), "pictures".into())]),
        columns: BTreeMap::from([(("photos".into(), "file".into()), "image".into())]),
    };
    assert_eq!(
        renamed(
            &[
                ("photos".into(), "file".into()),
                ("photos".into(), "thumbnail".into()),
                ("albums".into(), "cover".into()),
            ],
            &names
        ),
        vec![("pictures".to_owned(), "image".to_owned())]
    );
}
