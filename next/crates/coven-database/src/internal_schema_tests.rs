use std::collections::{BTreeMap, BTreeSet};

use crate::{sqlite::DatabaseConnection, tests::TestStore, Migration, RowIdentity, SyncedTable};
use coven_format::{
    merge_fields::*,
    snapshot_rows::LostWriteCause,
    value::{EntryId, Value, WritePositions},
};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{
    Audience, Cell, ColumnValue, History, LostKey, LostValue, Parent, RowId, RowState, Rule,
    Timestamp, Write, WriteId,
};

const NOTE_ID: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

#[tokio::test]
async fn uploads_start_unattempted_and_retain_sealed_bytes_across_reopen() {
    use crate::write::tests::{notes, records, sql, NOTES};
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('42','title','body')")
        .await
        .unwrap();
    let original = records(&db);
    db.inspect_writer(|db| {
        let sealed: Option<Vec<u8>> = db
            .query_row("SELECT sealed_bytes FROM coven_uploads", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(sealed, None);
        db.internal_execute(
            "UPDATE coven_uploads SET sealed_bytes=?1",
            [b"sealed upload".as_slice()],
        )
        .unwrap();
    });
    db.close().await.unwrap();
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(records(&db), original);
    db.inspect_writer(|db| {
        let sealed: Vec<u8> = db
            .query_row("SELECT sealed_bytes FROM coven_uploads", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(sealed, b"sealed upload");
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM coven_uploads WHERE sealed_bytes IS NULL",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    });
    db.close().await.unwrap();
}

fn declarations() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)
        .key_columns(["id", "number"])
        .audience_column("audience")]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::sql(1, "notes", "CREATE TABLE notes(id TEXT NOT NULL, number INTEGER NOT NULL, audience TEXT NOT NULL, title TEXT, body BLOB, PRIMARY KEY(id,number))")]
}

fn audience(audience: &Audience) -> String {
    match audience {
        Audience::Store => "store".into(),
        Audience::Circle(id) => id.to_string(),
    }
}

fn read_audience(value: String) -> Audience {
    if value == "store" {
        Audience::Store
    } else {
        Audience::Circle(CircleId(uuid::Uuid::parse_str(&value).unwrap()))
    }
}

fn sql_value(value: &Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Integer(n) => (*n).into(),
        Value::Real(bits) => f64::from_bits(*bits).into(),
        Value::Text(s) => s.clone().into(),
        Value::Blob(bytes) => bytes.clone().into(),
    }
}
fn sample() -> (RowState<Value>, History<Value>, BTreeSet<Rule>) {
    let row = RowId {
        table: "notes".into(),
        key: coven_format::key::encode_key(&[
            Value::Text(NOTE_ID.into()),
            Value::Integer(i64::MIN),
        ])
        .unwrap(),
        audience: Audience::Circle(CircleId(uuid::Uuid::from_u128(1))),
    };
    let ids: Vec<_> = (1..=6)
        .map(|n| WriteId {
            device: DeviceId(if n == 2 { u64::MAX } else { n }),
            number: if n == 2 { u64::MAX } else { 1 },
        })
        .collect();
    let seen = [
        vec![],
        vec![0],
        vec![0],
        vec![0, 2],
        vec![0, 2, 3],
        vec![0, 2, 3],
    ];
    let oracle = History::new(ids.iter().enumerate().map(|(i, id)| Write {
        id: *id,
        timestamp: Timestamp::new(i as u64 + 1, u16::MAX, id.device).unwrap(),
        had_read: seen[i].iter().map(|i| ids[*i]).collect(),
        changes: BTreeMap::new(),
    }))
    .unwrap();
    let value = |value| ColumnValue {
        value,
        parents: BTreeMap::from([(
            coven_merge::ForeignKey::new(["title", "body"], "parents", ["id", "locale"]),
            Parent {
                row: RowId {
                    table: "parents".into(),
                    key: coven_format::key::encode_key(&[Value::Blob(vec![0, 255])]).unwrap(),
                    audience: Audience::Store,
                },
                generation: u64::MAX,
            },
        )]),
    };
    let state = RowState::from_parts(
        row,
        BTreeMap::from([(1, ids[0]), (2, ids[2]), (3, ids[3])]),
        BTreeMap::from([
            (
                "title".into(),
                Cell {
                    write: ids[5],
                    value: value(Value::Text("title\0é".into())),
                },
            ),
            (
                "body".into(),
                Cell {
                    write: ids[3],
                    value: value(Value::Blob(vec![0, 255, 42])),
                },
            ),
        ]),
        BTreeMap::from([
            (
                LostKey {
                    column: "title".into(),
                    write: ids[1],
                },
                LostValue {
                    incarnation: 1,
                    value: value(Value::Integer(i64::MIN)),
                    replaced_by: ids[2],
                },
            ),
            (
                LostKey {
                    column: "title".into(),
                    write: ids[4],
                },
                LostValue {
                    incarnation: 3,
                    value: value(Value::Real(1.5_f64.to_bits())),
                    replaced_by: ids[5],
                },
            ),
        ]),
        &oracle,
    )
    .unwrap();
    (
        state,
        oracle,
        BTreeSet::from([
            Rule::ForeignKey(coven_merge::ForeignKey::new(
                ["parent_fk"],
                "parents",
                ["id"],
            )),
            Rule::Check("range".into()),
            Rule::Unique(["title_unique"].into()),
            Rule::DeletedCircle,
            Rule::OtherAudience,
        ]),
    )
}

// Test the actual tables and format codecs, without a production merge adapter.
fn put_state(
    db: &DatabaseConnection,
    state: &RowState<Value>,
    oracle: &History<Value>,
    removed: &BTreeSet<Rule>,
) {
    let mut writes = BTreeMap::new();
    for write in oracle.writes().values() {
        db.internal_execute(
            "INSERT INTO coven_writes(timestamp,number,had_read) VALUES (?1,?2,?3)",
            (
                encode_timestamp(&write.timestamp).unwrap(),
                write.id.number.to_be_bytes().to_vec(),
                encode_write_positions(&WritePositions(write.had_read.iter().copied().collect()))
                    .unwrap(),
            ),
        )
        .unwrap();
        writes.insert(
            write.id,
            db.query_row("SELECT last_insert_rowid()", [], |r| r.get::<_, i64>(0))
                .unwrap(),
        );
    }
    let mut rows = BTreeMap::new();
    for (generation, write) in state.generations() {
        db.internal_execute("INSERT INTO coven_rows(table_name,key,audience,generation,write_id) VALUES (?1,?2,?3,?4,?5)",
            (&state.row().table, &state.row().key, audience(&state.row().audience), generation.to_be_bytes().to_vec(), writes[write])).unwrap();
        rows.insert(
            *generation,
            db.query_row("SELECT last_insert_rowid()", [], |r| r.get::<_, i64>(0))
                .unwrap(),
        );
    }
    let mut columns = BTreeMap::new();
    let names: BTreeSet<_> = state
        .cells()
        .keys()
        .chain(state.lost().keys().map(|key| &key.column))
        .collect();
    for name in names {
        db.internal_execute(
            "INSERT INTO coven_columns(table_name,column_name) VALUES (?1,?2)",
            (&state.row().table, name),
        )
        .unwrap();
        columns.insert(
            name.clone(),
            db.query_row("SELECT last_insert_rowid()", [], |r| r.get::<_, i64>(0))
                .unwrap(),
        );
    }
    for (name, cell) in state.cells() {
        db.internal_execute(
            "INSERT INTO coven_cells(column_id,row_id,write_id) VALUES (?1,?2,?3)",
            (
                columns[name],
                rows[&state.generation()],
                writes[&cell.write],
            ),
        )
        .unwrap();
        for (key, parent) in &cell.value.parents {
            let key = crate::row_queries::foreign_key(db, &state.row().table, key).unwrap();
            db.internal_execute("INSERT INTO coven_references(row_id,column_id,foreign_key_id,parent_table,parent_key,parent_audience,parent_generation) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING", (rows[&state.generation()],columns[name],key,&parent.row.table,&parent.row.key,audience(&parent.row.audience),parent.generation.to_be_bytes().as_slice())).unwrap();
        }
    }
    for (key, lost) in state.lost() {
        db.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES (?1,?2,?3,?4,?5,?6,?7,'write',?8)",
            (&state.row().table, &state.row().key, audience(&state.row().audience), lost.incarnation.to_be_bytes().to_vec(), columns[&key.column],
             encode_column_value(&lost.value).unwrap(), encode_write_id(&key.write).unwrap(), encode_write_id(&lost.replaced_by).unwrap())).unwrap();
    }
    if removed.is_empty() {
        let key = coven_format::key::decode_key(&state.row().key).unwrap();
        db.internal_execute(
            "INSERT INTO notes(id,number,audience,title,body) VALUES (?1,?2,?3,?4,?5)",
            (
                sql_value(&key[0]),
                sql_value(&key[1]),
                audience(&state.row().audience),
                sql_value(&state.cells()["title"].value.value),
                sql_value(&state.cells()["body"].value.value),
            ),
        )
        .unwrap();
    } else {
        let values = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.value.clone()))
            .collect();
        let setters = state
            .cells()
            .iter()
            .map(|(name, cell)| (name.clone(), cell.write))
            .collect();
        db.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES (?1,?2,?3,?4,NULL,?5,?6,'rules',?7)",
            (&state.row().table, &state.row().key, audience(&state.row().audience), state.generation().to_be_bytes().to_vec(),
             encode_columns(&values).unwrap(), encode_setters(&setters).unwrap(), encode_rules(removed).unwrap())).unwrap();
    }
}

fn get_state(db: &DatabaseConnection) -> (RowState<Value>, BTreeSet<Rule>) {
    let writes = db
        .query(
            "SELECT id,timestamp,number,had_read FROM coven_writes ORDER BY id",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .unwrap();
    let mut history = Vec::new();
    let mut write_ids = BTreeMap::new();
    for (local, timestamp, number, seen) in writes {
        let timestamp = decode_timestamp(&timestamp).unwrap();
        let id = WriteId {
            device: timestamp.device(),
            number: u64::from_be_bytes(number.try_into().unwrap()),
        };
        history.push(Write::<Value> {
            id,
            timestamp,
            had_read: decode_write_positions(&seen)
                .unwrap()
                .0
                .into_iter()
                .collect(),
            changes: BTreeMap::new(),
        });
        write_ids.insert(local, id);
    }
    let oracle = History::new(history).unwrap();
    let row = db
        .query_row(
            "SELECT table_name,key,audience FROM coven_rows ORDER BY generation LIMIT 1",
            [],
            |r| {
                Ok(RowId {
                    table: r.get(0)?,
                    key: r.get(1)?,
                    audience: read_audience(r.get(2)?),
                })
            },
        )
        .unwrap();
    let generations = db
        .query(
            "SELECT generation,write_id FROM coven_rows ORDER BY generation",
            [],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?)),
        )
        .unwrap()
        .into_iter()
        .map(|(g, w)| (u64::from_be_bytes(g.try_into().unwrap()), write_ids[&w]))
        .collect();
    let mut lost = BTreeMap::new();
    let mut removed_row = None;
    for (generation, name, value, setters, replacement) in db.query(
        "SELECT l.generation,c.column_name,l.value,l.set_by,l.replaced_by FROM coven_lost l LEFT JOIN coven_columns c ON c.id=l.column_id", [], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, Vec<u8>>(2)?, r.get::<_, Vec<u8>>(3)?, r.get::<_, Vec<u8>>(4)?))
        }).unwrap() {
        match name {
            Some(name) => {
                lost.insert(LostKey { column: name, write: decode_write_id(&setters).unwrap() }, LostValue {
                    incarnation: u64::from_be_bytes(generation.try_into().unwrap()),
                    value: decode_column_value(&value).unwrap(), replaced_by: decode_write_id(&replacement).unwrap(),
                });
            }
            None => {
                assert!(removed_row.is_none());
                let setters = decode_setters(&setters).unwrap();
                let cells = decode_columns(&value).unwrap().into_iter().map(|(name, value)| {
                    let write = setters[&name]; (name, Cell { write, value })
                }).collect();
                removed_row = Some((cells, decode_rules(&replacement).unwrap()));
            }
        }
    }
    let (cells, removed) = match removed_row {
        Some(removed) => removed,
        None => {
            let key = coven_format::key::decode_key(&row.key).unwrap();
            let values = db
                .query_row(
                    "SELECT title,body FROM notes WHERE id=?1 AND number=?2",
                    (sql_value(&key[0]), sql_value(&key[1])),
                    |r| {
                        Ok(BTreeMap::from([
                            ("title".to_owned(), Value::Text(r.get(0)?)),
                            ("body".to_owned(), Value::Blob(r.get(1)?)),
                        ]))
                    },
                )
                .unwrap();
            let mut references = BTreeMap::<String, BTreeMap<_, _>>::new();
            for (column,key,parent) in db.query("SELECT c.column_name,f.identity,v.parent_table,v.parent_key,v.parent_audience,v.parent_generation FROM coven_references v JOIN coven_foreign_keys f ON f.id=v.foreign_key_id JOIN coven_columns c ON c.id=v.column_id", [], |r| Ok((r.get::<_,String>(0)?,decode_foreign_key(&r.get::<_,Vec<u8>>(1)?).unwrap(),Parent { row: RowId { table:r.get(2)?,key:r.get(3)?,audience:read_audience(r.get(4)?) },generation:crate::write_encoding::counter(r.get(5)?) }))).unwrap() { references.entry(column).or_default().insert(key,parent); }
            let cells = db.query("SELECT c.column_name,v.write_id FROM coven_cells v JOIN coven_columns c ON c.id=v.column_id", [], |r| Ok((r.get::<_,String>(0)?, r.get::<_,i64>(1)?))).unwrap().into_iter().map(|(name,write)| {
                let parents = references.remove(&name).unwrap_or_default();
                let value = ColumnValue { value:values[&name].clone(), parents };
                (name,Cell { write:write_ids[&write],value })
            }).collect();
            (cells, BTreeSet::new())
        }
    };
    (
        RowState::from_parts(row, generations, cells, lost, &oracle).unwrap(),
        removed,
    )
}

#[tokio::test]
async fn row_state_round_trips_from_app_values_or_removed_values() {
    for removed in [false, true] {
        let store = TestStore::new();
        let db = store
            .builder(declarations(), migrations())
            .open()
            .await
            .unwrap();
        let (state, oracle, rules) = sample();
        let rules = if removed { rules } else { BTreeSet::new() };
        db.inspect_writer(|sql| {
            sql.transaction(|sql| {
                put_state(sql, &state, &oracle, &rules);
                assert_eq!(
                    sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))
                        .unwrap(),
                    i64::from(!removed)
                );
                Ok(())
            })
            .unwrap()
        });
        db.close().await.unwrap();
        let db = store
            .builder(declarations(), migrations())
            .open()
            .await
            .unwrap();
        db.inspect_writer(|sql| assert_eq!(get_state(sql), (state, rules)));
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn migrations_change_present_values_without_a_second_copy() {
    let store = TestStore::new();
    let db = store
        .builder(declarations(), migrations())
        .open()
        .await
        .unwrap();
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES('f47ac10b-58cc-4372-a567-0e02b2c3d479',1,'store','before',x'00ff2a')").await.unwrap();
    let original = crate::write::tests::records(&db).remove(0);
    db.close().await.unwrap();
    let mut migrations = migrations();
    migrations.push(Migration::sql(
        2,
        "rewrite title",
        "UPDATE notes SET title='migrated'",
    ));
    let db = store
        .builder(declarations(), migrations)
        .open()
        .await
        .unwrap();
    let migration = crate::write::tests::records(&db).pop().unwrap();
    db.inspect_writer_schema(|sql, schema| {
        let visible = crate::write_rows::AppView::after(sql, schema);
        let merge = crate::merge_store::MergeStore::new(sql, &visible);
        let state = merge.row(&original.parts[0].rows[0].row).unwrap().state;
        assert_eq!(
            state.cells()["title"].value.value,
            Value::Text("migrated".into())
        );
        assert_eq!(state.cells()["title"].write, migration.header.position);
        assert_eq!(state.cells()["body"].write, original.header.position);
        assert_eq!(
            state.cells()["body"].value.value,
            Value::Blob(vec![0, 255, 42])
        );
        assert!(state.lost().is_empty());
        let cells = sql
            .query(
                "SELECT name FROM pragma_table_info('coven_cells') ORDER BY cid",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(cells, ["column_id", "row_id", "write_id"]);
    });
    db.close().await.unwrap();
}
#[tokio::test]
async fn only_the_spec_tables_are_created() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|sql| {
        let tables = sql
            .query(
                "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            tables,
            [
                "coven_applied_boundaries",
                "coven_cells",
                "coven_circle_members",
                "coven_circles",
                "coven_claims",
                "coven_columns",
                "coven_constraints",
                "coven_deleted_circles",
                "coven_device_files",
                "coven_devices",
                "coven_file_removals",
                "coven_fingerprint_leaves",
                "coven_fingerprint_sums",
                "coven_foreign_keys",
                "coven_lost",
                "coven_lost_references",
                "coven_members",
                "coven_operations",
                "coven_positions",
                "coven_references",
                "coven_rows",
                "coven_store_log",
                "coven_store_state",
                "coven_uploads",
                "coven_user_files",
                "coven_writes"
            ]
        );
        let cells = sql
            .query(
                "SELECT name FROM pragma_table_info('coven_cells') ORDER BY cid",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(cells, ["column_id", "row_id", "write_id"]);
        let fields = sql
            .query(
                "SELECT name FROM pragma_table_info('coven_operations') ORDER BY cid",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            fields,
            ["id", "kind", "last_step", "data", "started_by", "failure"]
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_lost_row_does_not_require_an_accepted_generation() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let (state, _, _) = sample();
    let values = state
        .cells()
        .iter()
        .map(|(name, cell)| (name.clone(), cell.value.clone()))
        .collect();
    let setters = state
        .cells()
        .iter()
        .map(|(name, cell)| (name.clone(), cell.write))
        .collect();
    let entry = EntryId {
        device: DeviceId(u64::MAX),
        number: u64::MAX,
    };
    for cause in [
        LostWriteCause::SchemaChange(u32::MAX),
        LostWriteCause::Reset(entry),
    ] {
        db.inspect_writer(|sql| {
            sql.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,value,set_by,replacement_kind,replaced_by) VALUES (?1,?2,?3,?4,?5,?6,'excluded',?7)",
                (&state.row().table, &state.row().key, audience(&state.row().audience), 0u64.to_be_bytes().to_vec(), encode_columns(&values).unwrap(), encode_setters(&setters).unwrap(), encode_lost_write_cause(&cause).unwrap())).unwrap();
            let stored = sql.query_row("SELECT replaced_by FROM coven_lost WHERE id=last_insert_rowid()", [], |r| r.get::<_, Vec<u8>>(0)).unwrap();
            assert_eq!(decode_lost_write_cause(&stored).unwrap(), cause);
            assert_eq!(sql.query_row("SELECT count(*) FROM coven_rows", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
        });
    }
    db.close().await.unwrap();
}
