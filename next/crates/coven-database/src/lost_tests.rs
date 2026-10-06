use super::*;
use crate::{tests::TestStore, CovenError, Database};
use coven_format::{key::encode_key, value::Value as WireValue};
use coven_foundation::id_source::DeviceId;
use coven_merge::ColumnValue;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn setter(number: u64) -> WriteId {
    WriteId {
        device: DeviceId(u64::MAX),
        number,
    }
}
fn cell(value: WireValue) -> ColumnValue<WireValue> {
    ColumnValue {
        value,
        parents: BTreeMap::new(),
    }
}
fn key() -> Vec<u8> {
    encode_key(&[
        WireValue::Text("id\0é".into()),
        WireValue::Integer(i64::MIN),
        WireValue::Blob(vec![0, 255]),
    ])
    .unwrap()
}

fn insert(
    db: &Database,
    column: Option<i64>,
    value: Vec<u8>,
    setters: Vec<u8>,
    kind: &str,
    replacement: Vec<u8>,
) {
    db.commit_writer(|sql| {
        sql.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES ('notes',?1,'store',?2,?3,?4,?5,?6,?7)",
            (key(), 0u64.to_be_bytes().to_vec(), column, value, setters, kind, replacement)).unwrap();
    });
}

#[tokio::test]
async fn losses_preserve_storage_classes_each_setter_and_every_replacement() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO coven_columns(id,table_name,column_name) VALUES (1,'notes','title')")
            .unwrap()
    });
    insert(
        &db,
        Some(1),
        encode_column_value(&cell(WireValue::Blob(vec![0, 255]))).unwrap(),
        encode_write_id(&setter(u64::MAX)).unwrap(),
        "write",
        encode_write_id(&setter(2)).unwrap(),
    );
    let values: BTreeMap<String, ColumnValue<WireValue>> = BTreeMap::from([
        ("a".into(), cell(WireValue::Null)),
        ("b".into(), cell(WireValue::Integer(i64::MIN))),
        ("c".into(), cell(WireValue::Real(1.25f64.to_bits()))),
        ("d".into(), cell(WireValue::Text("text\0é".into()))),
        ("e".into(), cell(WireValue::Blob(vec![]))),
    ]);
    let setters: BTreeMap<String, WriteId> = values
        .keys()
        .enumerate()
        .map(|(i, column)| (column.clone(), setter(i as u64 + 1)))
        .collect();
    insert(
        &db,
        None,
        encode_columns(&values).unwrap(),
        encode_setters(&setters).unwrap(),
        "rules",
        encode_rules(&BTreeSet::from([
            Rule::Check("positive".into()),
            Rule::DeletedCircle,
            Rule::OtherAudience,
        ]))
        .unwrap(),
    );
    let entry = EntryId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
    };
    for cause in [
        LostWriteCause::SchemaChange(u32::MAX),
        LostWriteCause::Reset(entry),
    ] {
        insert(
            &db,
            None,
            encode_columns(&values).unwrap(),
            encode_setters(&setters).unwrap(),
            "excluded",
            encode_lost_write_cause(&cause).unwrap(),
        );
    }
    let losses = db.lost_values().await.unwrap();
    assert_eq!(losses.len(), 4);
    assert_eq!(
        losses[0].key.values(),
        [
            Value::Text("id\0é".into()),
            Value::Integer(i64::MIN),
            Value::Blob(vec![0, 255])
        ]
    );
    assert_eq!(
        losses[0].lost,
        Lost::Cell(LostCell {
            column: "title".into(),
            value: Value::Blob(vec![0, 255]),
            set_by: setter(u64::MAX)
        })
    );
    assert_eq!(losses[0].replaced_by, Replacement::Write(setter(2)));
    let expected: Vec<_> = [
        Value::Null,
        Value::Integer(i64::MIN),
        Value::Real(1.25),
        Value::Text("text\0é".into()),
        Value::Blob(vec![]),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, value)| LostCell {
        column: char::from(b'a' + i as u8).to_string(),
        value,
        set_by: setter(i as u64 + 1),
    })
    .collect();
    for loss in &losses[1..] {
        assert_eq!(loss.lost, Lost::Row(expected.clone()));
    }
    assert_eq!(
        losses[1].replaced_by,
        Replacement::Rules(vec![
            RemovalRule::Check {
                constraint: "positive".into()
            },
            RemovalRule::DeletedCircle,
            RemovalRule::OtherAudience
        ])
    );
    assert_eq!(
        losses[2].replaced_by,
        Replacement::SchemaChange { version: u32::MAX }
    );
    assert_eq!(losses[3].replaced_by, Replacement::Reset(entry));
    db.close().await.unwrap();
    let ro = store
        .builder(vec![], vec![])
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(ro.lost_values().await.unwrap(), losses);
    ro.close().await.unwrap();
}

#[tokio::test]
async fn removal_rules_retain_their_identities_without_an_app_schema() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let values = BTreeMap::from([("title".into(), cell(WireValue::Text("lost".into())))]);
    let setters = BTreeMap::from([("title".into(), setter(1))]);
    let rules = BTreeSet::from([
        Rule::ForeignKey(coven_merge::ForeignKey::new(
            ["a", "b"],
            "parents",
            ["x", "y"],
        )),
        Rule::ForeignKey(coven_merge::ForeignKey::new(
            ["a", "b"],
            "other_parents",
            ["x", "y"],
        )),
        Rule::Unique(coven_merge::UniqueConstraint {
            terms: vec!["lower(title)".into(), "author".into()],
            partial: Some("deleted = 0".into()),
        }),
    ]);
    insert(
        &db,
        None,
        encode_columns(&values).unwrap(),
        encode_setters(&setters).unwrap(),
        "rules",
        encode_rules(&rules).unwrap(),
    );
    assert_eq!(
        db.lost_values().await.unwrap()[0].replaced_by,
        Replacement::Rules(vec![
            RemovalRule::ForeignKey {
                columns: vec!["a".into(), "b".into()],
                parent: "other_parents".into(),
                parent_columns: vec!["x".into(), "y".into()]
            },
            RemovalRule::ForeignKey {
                columns: vec!["a".into(), "b".into()],
                parent: "parents".into(),
                parent_columns: vec!["x".into(), "y".into()]
            },
            RemovalRule::Unique {
                terms: vec!["lower(title)".into(), "author".into()],
                partial: Some("deleted = 0".into())
            },
        ])
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn corrupt_values_fail_typed_and_live_losses_recover_after_repair() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut query = db.subscribe_lost_values();
    assert!(query.next().await.unwrap().is_empty());
    let values: BTreeMap<String, ColumnValue<WireValue>> =
        BTreeMap::from([("title".into(), cell(WireValue::Text("lost".into())))]);
    let setters = BTreeMap::from([("title".into(), setter(1))]);
    let rules = encode_rules(&BTreeSet::from([Rule::DeletedCircle])).unwrap();
    insert(
        &db,
        None,
        encode_columns(&values).unwrap(),
        encode_setters(&BTreeMap::from([("other".into(), setter(1))])).unwrap(),
        "rules",
        rules,
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), query.next())
            .await
            .unwrap(),
        Err(CovenError::Database(DbError::DamagedDatabase))
    ));
    db.commit_writer(|sql| {
        sql.internal_execute(
            "UPDATE coven_lost SET set_by=?1",
            [encode_setters(&setters).unwrap()],
        )
        .unwrap()
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), query.next())
            .await
            .unwrap()
            .unwrap()
            .len(),
        1
    );
    db.commit_writer(|sql| sql.batch("UPDATE coven_lost SET value=x'ff'").unwrap());
    assert!(matches!(
        query.next().await,
        Err(CovenError::Database(DbError::DamagedDatabase))
    ));
    db.commit_writer(|sql| sql.batch("DELETE FROM coven_lost").unwrap());
    assert!(query.next().await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn wrong_column_table_is_a_damaged_database() {
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE notes(title)")
        .await
        .unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO coven_columns VALUES (1,'other_table','title')")
            .unwrap()
    });
    insert(
        &db,
        Some(1),
        encode_column_value(&cell(WireValue::Null)).unwrap(),
        encode_write_id(&setter(1)).unwrap(),
        "write",
        encode_write_id(&setter(2)).unwrap(),
    );
    assert!(matches!(
        db.lost_values().await,
        Err(CovenError::Database(DbError::DamagedDatabase))
    ));
    db.close().await.unwrap();
}
