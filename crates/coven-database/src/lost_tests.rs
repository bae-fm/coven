use super::*;
use crate::{tests::TestStore, CovenError, Database};
use coven_format::{key::encode_key, merge_fields::*, value::Value as WireValue};
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
        sql.internal_execute("INSERT INTO _coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by,frozen) VALUES ('notes',?1,'store',?2,?3,?4,?5,?6,?7,?8)",
            (key(), 0u64.to_be_bytes().to_vec(), column, value, setters, kind, replacement, matches!(kind, "excluded" | "dependency"))).unwrap();
    });
}

#[tokio::test]
async fn losses_preserve_storage_classes_each_setter_and_every_replacement() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.commit_writer(|sql| {
        sql.batch(
            "INSERT INTO _coven_columns(id,table_name,column_name) VALUES (1,'notes','title')",
        )
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
    let entry = coven_merge::Slot(u64::MAX);
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
            RemovalRule::DeletedCircle { entry },
            RemovalRule::OtherAudience
        ])
    );
    assert_eq!(
        losses[2].replaced_by,
        Replacement::SchemaChange { version: u32::MAX }
    );
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
    let rules = encode_rules(&BTreeSet::from([Rule::DeletedCircle(coven_merge::Slot(1))])).unwrap();
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
            "UPDATE _coven_lost SET set_by=?1",
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
    db.commit_writer(|sql| sql.batch("UPDATE _coven_lost SET value=x'ff'").unwrap());
    assert!(matches!(
        query.next().await,
        Err(CovenError::Database(DbError::DamagedDatabase))
    ));
    db.commit_writer(|sql| sql.batch("DELETE FROM _coven_lost").unwrap());
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
        sql.batch("INSERT INTO _coven_columns VALUES (1,'other_table','title')")
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

#[tokio::test]
async fn a_lost_row_does_not_require_an_accepted_generation() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let key = coven_format::key::encode_key(&[WireValue::Text(
        "f47ac10b-58cc-4372-a567-0e02b2c3d479".into(),
    )])
    .unwrap();
    let write = WriteId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
    };
    let values = BTreeMap::from([(
        "title".into(),
        ColumnValue {
            value: WireValue::Text("lost".into()),
            parents: BTreeMap::new(),
        },
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

/// Losses under schema a migration drops (§18.7, D7): Lean
/// `Migration.freezeDropped` keeps every affected record, frozen under its
/// old names with its original cause and setters.
mod dropped_schema {
    use crate::snapshot_write::tests::assert_loaded_losses;
    use crate::write::tests::{count, records, sql};
    use crate::{tests::TestStore, Database, Lost, Migration, Replacement, RowIdentity};
    use crate::{LostValue, SyncedTable};
    use coven_foundation::id_source::SequentialIds;

    const TAGS: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL DEFAULT ''); CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY,label TEXT NOT NULL)";

    fn tables(names: &[&str]) -> Vec<SyncedTable> {
        names
            .iter()
            .map(|name| SyncedTable::new(*name, RowIdentity::SharedKey))
            .collect()
    }

    /// `b` edits `column` of `row` concurrently with `a`; `a` keeps one loss.
    async fn concurrent_loss(
        a: &Database,
        b: &Database,
        inserted: &'static str,
        a_edit: &'static str,
        b_edit: &'static str,
    ) {
        sql(a, inserted).await.unwrap();
        a.receive(records(a).last().unwrap().clone().into())
            .await
            .unwrap();
        b.receive(records(a).last().unwrap().clone().into())
            .await
            .unwrap();
        sql(b, b_edit).await.unwrap();
        sql(a, a_edit).await.unwrap();
        a.receive(records(b).last().unwrap().clone().into())
            .await
            .unwrap();
        assert_eq!(a.lost_values().await.unwrap().len(), 1);
    }

    async fn assert_frozen(db: &Database, before: &[LostValue]) {
        let losses = db.lost_values().await.unwrap();
        assert_eq!(
            losses.len(),
            1,
            "schema {}, slot {}",
            db.schema_version().await.unwrap(),
            db.applied_slot().await.unwrap()
        );
        assert_eq!(
            (
                &losses[0].table,
                &losses[0].key,
                &losses[0].lost,
                &losses[0].replaced_by
            ),
            (
                &before[0].table,
                &before[0].key,
                &before[0].lost,
                &before[0].replaced_by
            )
        );
        assert_eq!(cell_column(&losses[0]), "body");
    }

    fn cell_column(loss: &LostValue) -> &str {
        match &loss.lost {
            Lost::Cell(cell) => &cell.column,
            Lost::Row(_) => panic!("{loss:?}"),
        }
    }

    fn evidence(db: &Database, table: &str) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        db.inspect_writer(|sql| {
            sql.query(
                "SELECT write_id,admission FROM _coven_authored_changes WHERE table_name=?1 ORDER BY write_id,key",
                [table],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        })
    }

    /// A dropped table's cell loss stays displayed, frozen under the table's
    /// name; its original changes and verdicts stay as source evidence. A
    /// snapshot carries the loss to a device that never had the table, with
    /// equal snapshot records, and dismissal removes it on both.
    #[tokio::test]
    async fn a_dropped_tables_losses_stay_frozen_under_its_name() {
        let ids = SequentialIds::new();
        let stores: Vec<_> = (0..3).map(|_| TestStore::with_ids(&ids)).collect();
        let a = stores[0]
            .schema(tables(&["notes", "tags"]), TAGS)
            .await
            .unwrap();
        let b = stores[1]
            .schema(tables(&["notes", "tags"]), TAGS)
            .await
            .unwrap();
        concurrent_loss(
            &a,
            &b,
            "INSERT INTO tags VALUES('t','first')",
            "UPDATE tags SET label='from a'",
            "UPDATE tags SET label='from b'",
        )
        .await;
        let before = a.lost_values().await.unwrap();
        let sources = evidence(&a, "tags");
        assert!(!sources.is_empty());
        a.close().await.unwrap();
        let migrations = || {
            vec![
                Migration::addition(1, "tags", TAGS),
                Migration::breaking(2, "drop tags", "DROP TABLE tags", crate::OldWrites::Drop),
            ]
        };
        let a = stores[0]
            .builder(tables(&["notes"]), migrations())
            .open()
            .await
            .unwrap();
        let after = a.lost_values().await.unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(
            (
                &after[0].table,
                &after[0].key,
                &after[0].lost,
                &after[0].replaced_by
            ),
            (
                &before[0].table,
                &before[0].key,
                &before[0].lost,
                &before[0].replaced_by
            )
        );
        assert_eq!(after[0].table, "tags");
        assert_eq!(cell_column(&after[0]), "label");
        assert!(matches!(
            after[0].target,
            crate::lost::LossTarget::Acknowledgements(_)
        ));
        assert_eq!(evidence(&a, "tags"), sources, "verdicts outlive the table");
        let fresh = stores[2]
            .builder(tables(&["notes"]), migrations())
            .open()
            .await
            .unwrap();
        for write in records(&fresh) {
            a.receive(write.into()).await.unwrap();
        }
        assert_loaded_losses(&a, &fresh).await;
        a.dismiss_lost_values(&after).await.unwrap();
        assert!(a.lost_values().await.unwrap().is_empty());
        assert_loaded_losses(&a, &fresh).await;
        for db in [a, b, fresh] {
            db.close().await.unwrap();
        }
    }

    /// A dropped column's cell loss stays frozen with the old name. A later
    /// migration adding a column of that name, then renaming it, moves only
    /// live cells: the frozen loss still names the dropped column, also after
    /// a snapshot reload with equal snapshot records.
    #[tokio::test]
    async fn a_dropped_columns_loss_keeps_its_name_through_reuse_and_rename() {
        let ids = SequentialIds::new();
        let stores: Vec<_> = (0..3).map(|_| TestStore::with_ids(&ids)).collect();
        let a = stores[0].schema(tables(&["notes"]), TAGS).await.unwrap();
        let b = stores[1].schema(tables(&["notes"]), TAGS).await.unwrap();
        concurrent_loss(
            &a,
            &b,
            "INSERT INTO notes VALUES('n','title','first')",
            "UPDATE notes SET body='from a'",
            "UPDATE notes SET body='from b'",
        )
        .await;
        let before = a.lost_values().await.unwrap();
        a.close().await.unwrap();
        let migrations = |through: u32| {
            [
                Migration::addition(1, "tags", TAGS),
                Migration::breaking(
                    2,
                    "drop body",
                    "ALTER TABLE notes DROP COLUMN body",
                    crate::OldWrites::Drop,
                ),
                Migration::breaking(
                    3,
                    "body again",
                    "ALTER TABLE notes ADD COLUMN body TEXT NOT NULL DEFAULT ''",
                    crate::OldWrites::Drop,
                ),
                Migration::breaking(
                    4,
                    "rename body",
                    "ALTER TABLE notes RENAME COLUMN body TO content",
                    crate::OldWrites::Drop,
                ),
            ]
            .into_iter()
            .take(through as usize)
            .collect::<Vec<_>>()
        };
        let a = stores[0]
            .builder(tables(&["notes"]), migrations(2))
            .open()
            .await
            .unwrap();
        assert_frozen(&a, &before).await;
        a.close().await.unwrap();
        let a = stores[0]
            .builder(tables(&["notes"]), migrations(3))
            .open()
            .await
            .unwrap();
        sql(&a, "UPDATE notes SET body='new body'").await.unwrap();
        assert_frozen(&a, &before).await;
        a.close().await.unwrap();
        let a = stores[0]
            .builder(tables(&["notes"]), migrations(4))
            .open()
            .await
            .unwrap();
        assert_frozen(&a, &before).await;
        let content: String = a
            .read(|sql| Ok(sql.query_row("SELECT content FROM notes", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(content, "new body");
        let fresh = stores[2]
            .builder(tables(&["notes"]), migrations(4))
            .open()
            .await
            .unwrap();
        for write in records(&fresh) {
            a.receive(write.into()).await.unwrap();
        }
        assert_loaded_losses(&a, &fresh).await;
        assert_frozen(&fresh, &before).await;
        assert_eq!(count(&fresh, "notes"), 1);
        for db in [a, b, fresh] {
            db.close().await.unwrap();
        }
    }

    /// Migration freezes deleted-circle values with their original columns.
    #[tokio::test]
    async fn a_hidden_rows_dropped_column_is_kept_in_a_frozen_whole_row_loss() {
        use crate::migration_state::tests::{circle_row, circle_schema, circle_tables};
        let store = TestStore::new();
        let db = store
            .schema(circle_tables(), circle_schema(true))
            .await
            .unwrap();
        let row = circle_row();
        db.receive(row.into()).await.unwrap();
        let circle = coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(1));
        crate::tests::delete_circle(&db, circle).await.unwrap();
        let hidden = db.lost_values().await.unwrap();
        assert_eq!(hidden.len(), 1);
        db.close().await.unwrap();
        let db = store
            .builder(
                circle_tables(),
                vec![
                    Migration::addition(1, "rows", circle_schema(true)),
                    Migration::breaking(
                        2,
                        "drop c",
                        "ALTER TABLE rows DROP COLUMN c",
                        crate::OldWrites::Drop,
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        let losses = db.lost_values().await.unwrap();
        assert_eq!(losses.len(), 1, "{losses:#?}");
        let columns = |loss: &LostValue| match &loss.lost {
            Lost::Row(cells) => cells.iter().map(|c| c.column.clone()).collect::<Vec<_>>(),
            Lost::Cell(_) => panic!("{loss:?}"),
        };
        let frozen = losses
            .iter()
            .find(|l| matches!(l.target, crate::lost::LossTarget::Acknowledgements(_)))
            .unwrap();
        assert_eq!(&frozen.lost, &hidden[0].lost);
        assert!(matches!(
            frozen.replaced_by,
            Replacement::SchemaChange { version: 2 }
        ));
        assert!(columns(frozen).contains(&"c".to_string()));
        assert_eq!(count(&db, "rows"), 0);
        db.close().await.unwrap();
    }
}
