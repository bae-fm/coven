use crate::{tests::TestStore, Database, DbError};
use crate::{Lost, LostCell, RemovalRule, Replacement, WriteId};
use coven_format::{key::encode_key, merge_fields::*, value::Value as WireValue};
use coven_foundation::clock::HANG_GUARD;
use coven_foundation::id_source::DeviceId;
use coven_merge::{ColumnValue, Rule};
use rusqlite::types::Value;
use std::collections::{BTreeMap, BTreeSet};

fn setter(number: u64) -> WriteId {
    WriteId { device: DeviceId(u64::MAX), number }
}
fn cell(value: WireValue) -> ColumnValue<WireValue> {
    ColumnValue { value, parents: BTreeMap::new() }
}
fn key() -> Vec<u8> {
    encode_key(&[
        WireValue::Text("id\0é".into()),
        WireValue::Integer(i64::MIN),
        WireValue::Blob(vec![0, 255]),
    ])
    .unwrap()
}

/// Store a loss as coven does: excluded, dependent and unauthorized ones
/// frozen, as the table's check requires (format.md D7.2).
fn insert(
    db: &Database,
    column: Option<i64>,
    value: Vec<u8>,
    setters: Vec<u8>,
    kind: &str,
    replacement: Vec<u8>,
) {
    db.commit_writer(|sql| {
        sql.internal_execute("INSERT INTO _coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by,frozen) VALUES ('notes',?1,'store',?2,?3,?4,?5,?6,?7,?8)",
            (key(), 0u64.to_be_bytes().to_vec(), column, value, setters, kind, replacement, matches!(kind, "excluded" | "dependency" | "unauthorized"))).unwrap();
    });
}

#[tokio::test]
async fn losses_preserve_storage_classes_each_setter_and_every_replacement() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let title =
        db.commit_writer(|sql| crate::row_queries::frozen_column(sql, "notes", "title").unwrap());
    insert(
        &db,
        Some(title),
        encode_column_value(&cell(WireValue::Blob(vec![0, 255]))).unwrap(),
        encode_write_id(&setter(u64::MAX)).unwrap(),
        "write",
        encode_write_id(&setter(2)).unwrap(),
    );
    let entry = coven_merge::slot(u64::MAX);
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
            Rule::DeletedCircle(entry),
            Rule::OtherAudience,
        ]))
        .unwrap(),
    );
    insert(
        &db,
        None,
        encode_columns(&values).unwrap(),
        encode_setters(&setters).unwrap(),
        "excluded",
        encode_exclusion(setter(1), u32::MAX).unwrap(),
    );
    // A dependent change names its failed sources in role order, with repeats.
    let source = |write| coven_format::write_sources::ChangeId {
        write,
        row: coven_merge::RowId {
            table: "notes".into(),
            key: key(),
            audience: coven_merge::Audience::Store,
        },
    };
    insert(
        &db,
        None,
        encode_columns(&values).unwrap(),
        encode_setters(&setters).unwrap(),
        "dependency",
        coven_format::merge_fields::encode_dependency(
            setter(1),
            &[source(setter(7)), source(setter(7)), source(setter(8))],
        )
        .unwrap(),
    );
    let losses = db.lost_values().await.unwrap();
    assert_eq!(losses.len(), 4);
    // Excluded and dependent losses keep old-shape values, frozen (§18.7).
    assert_eq!(
        losses.iter().map(|loss| loss.frozen).collect::<Vec<_>>(),
        [false, false, true, true]
    );
    assert_eq!(
        losses[0].key.values(),
        [Value::Text("id\0é".into()), Value::Integer(i64::MIN), Value::Blob(vec![0, 255])]
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
            RemovalRule::Check { constraint: "positive".into() },
            RemovalRule::DeletedCircle { entry },
            RemovalRule::OtherAudience
        ])
    );
    assert_eq!(losses[2].replaced_by, Replacement::SchemaChange { version: u32::MAX });
    let named = |write| crate::ChangeSource {
        write,
        table: "notes".into(),
        key: losses[3].key.clone(),
        audience: coven_merge::Audience::Store,
    };
    assert_eq!(
        losses[3].replaced_by,
        Replacement::DataDependency {
            write: setter(1),
            sources: vec![named(setter(7)), named(setter(7)), named(setter(8))]
        }
    );
    db.close().await.unwrap();
    let ro = store.builder(vec![], vec![]).open_read_only().await.unwrap();
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
        Rule::ForeignKey(coven_merge::ForeignKey::new(["a", "b"], "parents", ["x", "y"])),
        Rule::ForeignKey(coven_merge::ForeignKey::new(["a", "b"], "other_parents", ["x", "y"])),
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
    assert!(query.next().await.expect("the live query answers").unwrap().is_empty());
    let values: BTreeMap<String, ColumnValue<WireValue>> =
        BTreeMap::from([("title".into(), cell(WireValue::Text("lost".into())))]);
    let setters = BTreeMap::from([("title".into(), setter(1))]);
    let rules = encode_rules(&BTreeSet::from([Rule::DeletedCircle(coven_merge::slot(1))])).unwrap();
    insert(
        &db,
        None,
        encode_columns(&values).unwrap(),
        encode_setters(&BTreeMap::from([("other".into(), setter(1))])).unwrap(),
        "rules",
        rules,
    );
    assert!(matches!(
        tokio::time::timeout(HANG_GUARD, query.next())
            .await
            .unwrap()
            .expect("the live query answers"),
        Err(DbError::DamagedDatabase)
    ));
    db.commit_writer(|sql| {
        sql.internal_execute(
            "UPDATE _coven_lost SET set_by=?1",
            [encode_setters(&setters).unwrap()],
        )
        .unwrap()
    });
    assert_eq!(
        tokio::time::timeout(HANG_GUARD, query.next())
            .await
            .unwrap()
            .expect("the live query answers")
            .unwrap()
            .len(),
        1
    );
    db.commit_writer(|sql| sql.batch("UPDATE _coven_lost SET value=x'ff'").unwrap());
    assert!(matches!(
        query.next().await.expect("the live query answers"),
        Err(DbError::DamagedDatabase)
    ));
    db.commit_writer(|sql| sql.batch("DELETE FROM _coven_lost").unwrap());
    assert!(query.next().await.expect("the live query answers").unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn wrong_column_table_is_a_damaged_database() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE notes(title)").await.unwrap();
    let column = db.commit_writer(|sql| {
        crate::row_queries::frozen_column(sql, "other_table", "title").unwrap()
    });
    insert(
        &db,
        Some(column),
        encode_column_value(&cell(WireValue::Null)).unwrap(),
        encode_write_id(&setter(1)).unwrap(),
        "write",
        encode_write_id(&setter(2)).unwrap(),
    );
    assert!(matches!(db.lost_values().await, Err(DbError::DamagedDatabase)));
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_lost_row_does_not_require_an_accepted_generation() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let key = crate::tests::text_key("f47ac10b-58cc-4372-a567-0e02b2c3d479");
    let write = WriteId { device: DeviceId(u64::MAX), number: u64::MAX };
    let values = BTreeMap::from([(
        "title".into(),
        ColumnValue { value: WireValue::Text("lost".into()), parents: BTreeMap::new() },
    )]);
    let setters = BTreeMap::from([("title".into(), write)]);
    for cause in [1, u32::MAX] {
        db.inspect_writer(|sql| {
            sql.internal_execute("INSERT INTO _coven_lost(table_name,key,audience,generation,value,set_by,replacement_kind,replaced_by,frozen) VALUES (?1,?2,?3,?4,?5,?6,'excluded',?7,1)",
                ("notes", &key, "store", 0u64.to_be_bytes().to_vec(), encode_columns(&values).unwrap(), encode_setters(&setters).unwrap(), encode_exclusion(write, cause).unwrap())).unwrap();
            let stored = sql.query_row("SELECT replaced_by FROM _coven_lost WHERE id=last_insert_rowid()", [], |r| r.get::<_, Vec<u8>>(0)).unwrap();
            assert_eq!(decode_exclusion(&stored).unwrap().1, cause);
            assert!(sql.internal_execute("UPDATE _coven_lost SET frozen=0 WHERE id=last_insert_rowid()", []).is_err());
            assert_eq!(sql.query_row("SELECT count(*) FROM _coven_rows", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
        });
    }
    db.close().await.unwrap();
}

/// A row has at most one active rules loss; a second is a damaged store,
/// which a read reports as `DamagedDatabase` (§19.3), not a panic.
#[tokio::test]
async fn a_second_rules_loss_for_one_row_is_damaged_database() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let values = BTreeMap::from([("a".to_owned(), cell(WireValue::Integer(1)))]);
    let setters = BTreeMap::from([("a".to_owned(), setter(1))]);
    for _ in 0..2 {
        insert(
            &db,
            None,
            encode_columns(&values).unwrap(),
            encode_setters(&setters).unwrap(),
            "rules",
            encode_rules(&BTreeSet::from([Rule::OtherAudience])).unwrap(),
        );
    }
    let row = coven_merge::RowId {
        table: "notes".into(),
        key: key(),
        audience: coven_merge::Audience::Store,
    };
    assert!(matches!(
        db.inspect_writer(|sql| crate::loss_record::removed(sql, &row, 0)),
        Err(DbError::DamagedDatabase)
    ));
}

/// Lose the record of every row removal rules took out, as a damaged
/// database would: the merge row stays present with no values anywhere.
pub(crate) fn lose_removed_rows(db: &crate::sqlite::DatabaseConnection) {
    db.internal_execute(
        "DELETE FROM _coven_lost WHERE column_id IS NULL AND replacement_kind='rules'",
        [],
    )
    .unwrap();
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_lost
           (table_name,key,audience,generation,column_id,value,set_by,frozen,replacement_kind,replaced_by)
           SELECT tab,key,audience,counter(1),CASE WHEN i%3=0 THEN i END,
                  zeroblob(64),n,i%2,CASE WHEN i%3=0 THEN 'write' ELSE 'rules' END,n FROM fixture;",
    )
    .unwrap();
}
