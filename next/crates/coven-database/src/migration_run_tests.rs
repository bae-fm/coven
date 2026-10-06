use crate::{
    tests::TestStore,
    write::tests::{count, notes, records, sql, NOTES},
    Database, Migration, MigrationChange, RowIdentity, SyncedTable,
};
use coven_format::write::WriteDisposition;
use std::collections::BTreeMap;

fn sequence(extra: Vec<Migration>) -> Vec<Migration> {
    std::iter::once(Migration::sql(1, "notes", NOTES))
        .chain(extra)
        .collect()
}

#[tokio::test]
async fn migrations_leave_unrelated_merge_rows_unloaded_in_a_ten_thousand_row_store() {
    const SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,body TEXT); CREATE TABLE library(id TEXT NOT NULL PRIMARY KEY,body BLOB); CREATE TABLE local_cache(id TEXT NOT NULL PRIMARY KEY,body BLOB)";
    for breaking in [false, true] {
        let fixture = TestStore::new();
        let tables = || {
            ["notes", "library"]
                .into_iter()
                .map(|t| SyncedTable::new(t, RowIdentity::SharedKey))
                .collect()
        };
        let db = fixture.schema(tables(), SCHEMA).await.unwrap();
        sql(&db,"WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000) INSERT INTO library SELECT printf('%05d',i),zeroblob(256) FROM n; INSERT INTO local_cache SELECT * FROM library; INSERT INTO notes VALUES('n','Title','body')").await.unwrap();
        db.close().await.unwrap();
        let mut migrations = vec![
            Migration::sql(1, "initial", SCHEMA),
            Migration::sql(2, "addition", "ALTER TABLE notes ADD COLUMN color TEXT; ALTER TABLE local_cache RENAME COLUMN body TO payload"),
        ];
        if breaking {
            migrations.push(Migration::sql(3,"rename and backfill","ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=lower(name)"));
        }
        let db = fixture.builder(tables(), migrations).open().await.unwrap();
        let loads = db.inspect_writer(|db| db.merge_loads());
        assert!(
            !loads.contains_key("library"),
            "untouched merge records were loaded: {loads:?}"
        );
        if breaking {
            assert!(
                loads.values().sum::<usize>() < 32,
                "migration retained the library: {loads:?}"
            );
            assert_eq!(setters(&db, "notes", "n")["name"], 1);
            assert_eq!(setters(&db, "notes", "n")["slug"], 2);
        } else {
            assert!(
                loads.is_empty(),
                "an addition must not read merge state: {loads:?}"
            );
        }
        let statements = db.inspect_writer(|db| db.fullscan_statements());
        assert!(
            !statements
                .iter()
                .any(|(sql, _)| sql.contains("FROM main.\"local_cache\"")),
            "local rows must never be snapshotted"
        );
        for (statement, rows) in &statements {
            if [
                "coven_rows",
                "coven_cells",
                "coven_references",
                "coven_writes",
            ]
            .iter()
            .any(|table| statement.contains(table))
            {
                assert!(
                    *rows < 100,
                    "migration scanned untouched merge rows: {statement}: {rows}"
                );
            }
        }
        if !breaking {
            assert!(
                !statements
                    .iter()
                    .any(|(sql, _)| sql.contains("coven_migration_before_")),
                "an addition must copy no rows: {statements:?}"
            );
        }
        db.close().await.unwrap();
    }
}

fn setters(db: &Database, table: &str, key: &str) -> BTreeMap<String, u64> {
    let key =
        coven_format::key::encode_key(&[coven_format::value::Value::Text(key.into())]).unwrap();
    db.inspect_writer(|db| db.query("SELECT c.column_name,w.number FROM coven_cells v JOIN coven_columns c ON c.id=v.column_id JOIN coven_writes w ON w.id=v.write_id JOIN coven_rows r ON r.id=v.row_id WHERE r.table_name=?1 AND r.key=?2 ORDER BY c.column_name", rusqlite::params![table,key], |r| Ok((r.get(0)?, crate::write_encoding::counter(r.get(1)?)))).unwrap().into_iter().collect())
}

async fn seeded(store: &TestStore) -> Database {
    let db = store
        .builder(notes(), sequence(vec![]))
        .open()
        .await
        .unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','Old title','body'); INSERT INTO notes VALUES('deleted','gone','')").await.unwrap();
    sql(&db, "UPDATE notes SET title='New title' WHERE id='n'")
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn title_setter_survives_while_slug_and_changed_rows_belong_to_one_migration_write() {
    let store = TestStore::new();
    seeded(&store).await.close().await.unwrap();
    let db = store.builder(notes(), sequence(vec![
        Migration::sql(2, "name and slug", "ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=lower(replace(name,' ','-')); DELETE FROM notes WHERE id='deleted'; INSERT INTO notes VALUES('new','Inserted','','inserted')"),
        Migration::sql(3, "backfill", "UPDATE notes SET body='changed' WHERE id='n'"),
        Migration::sql(4, "color", "ALTER TABLE notes ADD COLUMN color TEXT"),
    ])).open().await.unwrap();
    let writes = records(&db);
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[2].header.disposition, WriteDisposition::Migration);
    assert!(writes[2].parts.is_empty());
    assert_eq!(writes[2].header.schema_version, 4);
    assert_eq!(
        setters(&db, "notes", "n"),
        [
            ("id".into(), 1),
            ("name".into(), 2),
            ("slug".into(), 3),
            ("body".into(), 3),
            ("color".into(), 3)
        ]
        .into()
    );
    assert!(setters(&db, "notes", "new").values().all(|w| *w == 3));
    assert!(setters(&db, "notes", "deleted").is_empty());
    assert_eq!(
        db.applied_migrations()
            .unwrap()
            .iter()
            .map(|m| m.change.clone())
            .collect::<Vec<_>>(),
        [
            MigrationChange::Breaking,
            MigrationChange::Breaking,
            MigrationChange::Addition
        ]
    );
    sql(&db, "UPDATE notes SET name='later' WHERE id='n'")
        .await
        .unwrap();
    assert_eq!(setters(&db, "notes", "n")["name"], 4);
    db.close().await.unwrap();
}

#[tokio::test]
async fn drops_rebuilds_and_drop_recreate_preserve_only_matching_cells() {
    for migration in [
        "ALTER TABLE notes DROP COLUMN body",
        "CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL); INSERT INTO rebuilt SELECT id,title FROM notes; DROP TABLE notes; ALTER TABLE rebuilt RENAME TO notes",
        "CREATE TEMP TABLE backup AS SELECT id,title FROM notes; DROP TABLE notes; CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL); INSERT INTO notes SELECT id,title FROM backup; DROP TABLE backup",
    ] {
        let store = TestStore::new();
        seeded(&store).await.close().await.unwrap();
        let db = store.builder(notes(), sequence(vec![Migration::sql(2, "change", migration)])).open().await.unwrap();
        assert_eq!(setters(&db,"notes","n"), [("id".into(),1),("title".into(),2)].into(), "{migration}");
        assert_eq!(records(&db).last().unwrap().header.disposition, WriteDisposition::Migration);
        assert_eq!(db.inspect_writer(|db| db.query_row("SELECT count(*) FROM coven_columns WHERE column_name='body'",[],|r|r.get::<_,i64>(0)).unwrap()),0);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn additions_make_no_write_but_updates_make_an_addition_breaking() {
    for (statement, change, number) in [
        (
            "ALTER TABLE notes ADD COLUMN slug TEXT",
            MigrationChange::Addition,
            2,
        ),
        (
            "ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=title",
            MigrationChange::Breaking,
            3,
        ),
        (
            "UPDATE notes SET title='temporary'; UPDATE notes SET title='New title' WHERE id='n'",
            MigrationChange::Breaking,
            3,
        ),
    ] {
        let store = TestStore::new();
        seeded(&store).await.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                sequence(vec![Migration::sql(2, "change", statement)]),
            )
            .open()
            .await
            .unwrap();
        assert_eq!(db.applied_migrations().unwrap()[0].change, change);
        assert_eq!(records(&db).len(), number);
        assert_eq!(setters(&db, "notes", "n")["title"], 2);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_rename_chains_override_names_through_every_context_method() {
    const INITIAL: &str = "CREATE TABLE a(id TEXT NOT NULL PRIMARY KEY,x TEXT,y TEXT); CREATE TABLE b(id TEXT NOT NULL PRIMARY KEY,x TEXT,y TEXT)";
    for method in 0..4 {
        let store = TestStore::new();
        let db = store
            .schema(
                vec![
                    SyncedTable::new("a", RowIdentity::SharedKey),
                    SyncedTable::new("b", RowIdentity::SharedKey),
                ],
                INITIAL,
            )
            .await
            .unwrap();
        sql(
            &db,
            "INSERT INTO a VALUES('n','a','old'); INSERT INTO b VALUES('n','b','old')",
        )
        .await
        .unwrap();
        sql(&db, "UPDATE a SET x='from a'").await.unwrap();
        sql(&db, "UPDATE b SET y='from b'").await.unwrap();
        db.close().await.unwrap();
        let db = store
            .builder(
                vec![SyncedTable::new("b", RowIdentity::SharedKey)],
                vec![
                    Migration::sql(1, "initial", INITIAL),
                    Migration::run(2, "rename", move |context| {
                        for statement in [
                            "DROP TABLE b",
                            "ALTER TABLE a RENAME TO intermediate",
                            "ALTER TABLE intermediate RENAME TO b",
                            "ALTER TABLE b DROP COLUMN y",
                            "ALTER TABLE b RENAME COLUMN x TO temporary",
                            "ALTER TABLE b RENAME COLUMN temporary TO y",
                        ] {
                            match method {
                                0 => {
                                    context.execute(statement, [])?;
                                }
                                1 => context.execute_batch(statement)?,
                                2 => {
                                    assert!(matches!(
                                        context.query_row(statement, [], |_| Ok(())),
                                        Err(rusqlite::Error::QueryReturnedNoRows)
                                    ));
                                }
                                _ => {
                                    assert!(context
                                        .query(statement, [], |_| Ok(()))
                                        .unwrap()
                                        .is_empty());
                                }
                            }
                        }
                        Ok(())
                    }),
                ],
            )
            .open()
            .await
            .unwrap();
        assert_eq!(
            setters(&db, "b", "n"),
            [("id".into(), 1), ("y".into(), 2)].into()
        );
        assert_eq!(
            db.inspect_writer(|db| db
                .query_row(
                    "SELECT count(*) FROM coven_rows WHERE table_name<>'b'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap()),
            0
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_migration_write_failure_rolls_back_schema_data_and_metadata() {
    let store = TestStore::new();
    let db = seeded(&store).await;
    let old = records(&db);
    db.close().await.unwrap();
    // The first millisecond a 48-bit timestamp can't hold; Windows can still
    // represent it as a SystemTime.
    let clock = std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(1 << 48),
    ));
    let error = store
        .builder(
            notes(),
            sequence(vec![Migration::sql(
                2,
                "backfill",
                "ALTER TABLE notes RENAME COLUMN title TO name; UPDATE notes SET name='changed'",
            )]),
        )
        .clock(clock)
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        crate::tests::database_error(error),
        crate::DbError::ClockOutOfRange
    ));
    let db = store
        .builder(notes(), sequence(vec![]))
        .open()
        .await
        .unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    assert_eq!(records(&db), old);
    assert_eq!(
        setters(&db, "notes", "n"),
        [("id".into(), 1), ("body".into(), 1), ("title".into(), 2)].into()
    );
    assert_eq!(count(&db, "notes"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_removed_row_is_forgotten_but_its_loss_survives_migration_and_reinsertion() {
    use coven_format::{
        value::{Value, WritePositions},
        write::{RowChange, WriteHeader, WritePart, WriteRecord},
    };
    use coven_foundation::id_source::DeviceId;
    use coven_merge::{Audience, Change, ColumnValue, Operation, Timestamp, WriteId};
    const INITIAL: &str =
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT CHECK(title<>'bad'),body TEXT)";
    let store = TestStore::new();
    let db = store.schema(notes(), INITIAL).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','good','body')")
        .await
        .unwrap();
    let original = records(&db).remove(0);
    let id = WriteId {
        device: DeviceId(99),
        number: 1,
    };
    let record = WriteRecord {
        header: WriteHeader {
            position: id,
            timestamp: Timestamp::new(original.header.timestamp.milliseconds() + 1, 0, id.device)
                .unwrap(),
            had_read: WritePositions(vec![original.header.position]),
            schema_version: 1,
            disposition: WriteDisposition::Apply,
        },
        parts: vec![WritePart {
            audience: Audience::Store,
            rows: vec![RowChange {
                row: original.parts[0].rows[0].row.clone(),
                change: Change {
                    generation: 1,
                    operation: Operation::Update(
                        [(
                            "title".into(),
                            ColumnValue {
                                value: Value::Text("bad".into()),
                                parents: BTreeMap::new(),
                            },
                        )]
                        .into(),
                    ),
                },
                old: [("title".into(), Value::Text("good".into()))].into(),
            }],
        }],
    };
    db.apply_downloaded(record.into()).await.unwrap();
    assert_eq!(count(&db, "notes"), 0);
    let lost = db.lost_values().await.unwrap();
    assert_eq!(lost.len(), 1);
    db.close().await.unwrap();
    let migrations = || {
        vec![Migration::sql(1,"initial",INITIAL),Migration::sql(2,"drop check","DROP TABLE notes; CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,body TEXT)")]
    };
    let db = store.builder(notes(), migrations()).open().await.unwrap();
    assert_eq!(db.lost_values().await.unwrap(), lost);
    for table in [
        "notes",
        "coven_rows",
        "coven_cells",
        "coven_references",
        "coven_claims",
        "coven_lost_references",
    ] {
        assert_eq!(count(&db, table), 0, "{table}");
    }
    sql(&db, "INSERT INTO notes VALUES('n','new','new')")
        .await
        .unwrap();
    assert_eq!(db.lost_values().await.unwrap(), lost);
    assert_eq!(
        records(&db).last().unwrap().parts[0].rows[0]
            .change
            .generation,
        0
    );
    db.close().await.unwrap();
    let db = store.builder(notes(), migrations()).open().await.unwrap();
    assert_eq!(db.lost_values().await.unwrap(), lost);
    sql(&db, "UPDATE notes SET title='later'").await.unwrap();
    assert_eq!(count(&db, "notes"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn renames_keep_reference_generations_and_a_migration_insert_uses_the_merge() {
    const INITIAL: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL)";
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("parents", RowIdentity::SharedKey),
                SyncedTable::new("children", RowIdentity::SharedKey),
            ],
            INITIAL,
        )
        .await
        .unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![SyncedTable::new("roots",RowIdentity::SharedKey).key_columns(["key"]),SyncedTable::new("children",RowIdentity::SharedKey)], vec![Migration::sql(1,"initial",INITIAL),Migration::sql(2,"rename","ALTER TABLE parents RENAME TO roots; ALTER TABLE roots RENAME COLUMN id TO key; ALTER TABLE children RENAME COLUMN parent TO root; INSERT INTO children VALUES('new','p')")]).open().await.unwrap();
    assert_eq!(setters(&db, "children", "c")["root"], 1);
    assert_eq!(setters(&db, "children", "new")["root"], 2);
    assert_eq!(count(&db, "coven_references"), 2);
    db.inspect_writer(|db| {
        let references = db.query("SELECT f.identity,v.parent_table,v.parent_generation FROM coven_references v JOIN coven_foreign_keys f ON f.id=v.foreign_key_id",[],|r|Ok((coven_format::merge_fields::decode_foreign_key(&r.get::<_,Vec<u8>>(0)?).unwrap(),r.get::<_,String>(1)?,crate::write_encoding::counter(r.get(2)?)))).unwrap();
        for (key,table,generation) in references {
            assert_eq!(key,coven_merge::ForeignKey::new(["root"],"roots",["key"]));
            assert_eq!(table,"roots");
            assert_eq!(generation,1);
        }
    });
    sql(&db, "DELETE FROM roots").await.unwrap();
    assert_eq!(
        db.read(|db| Ok(db.query_row(
            "SELECT count(*) FROM children WHERE root IS NULL",
            [],
            |r| r.get::<_, i64>(0)
        )?))
        .await
        .unwrap(),
        2
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_new_reference_column_is_set_by_the_migration() {
    const INITIAL: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY)";
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ]
    };
    let store = TestStore::new();
    let db = store.schema(tables(), INITIAL).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let db = store.builder(tables(),vec![Migration::sql(1,"initial",INITIAL),Migration::sql(2,"reference","ALTER TABLE children ADD COLUMN parent TEXT REFERENCES parents(id); UPDATE children SET parent='p'")]).open().await.unwrap();
    assert_eq!(setters(&db, "children", "c")["parent"], 2);
    assert_eq!(count(&db, "coven_references"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn dropping_and_recreating_identical_schema_records_deleted_rows() {
    let store = TestStore::new();
    seeded(&store).await.close().await.unwrap();
    let db = store.builder(notes(),sequence(vec![Migration::sql(2,"empty","DROP TABLE notes; CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL DEFAULT '')")])).open().await.unwrap();
    assert_eq!(
        db.applied_migrations().unwrap()[0].change,
        MigrationChange::Breaking
    );
    assert_eq!(count(&db, "notes"), 0);
    assert!(setters(&db, "notes", "n").is_empty());
    assert_eq!(records(&db).len(), 3);
    db.close().await.unwrap();
}

#[tokio::test]
async fn migration_inserts_obey_the_independent_key_rule() {
    let store = TestStore::new();
    let error = store.builder(vec![SyncedTable::new("notes",RowIdentity::IndependentUuid)],vec![Migration::sql(1,"invalid key","CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); INSERT INTO notes VALUES('not-a-uuid')")]).open().await.err().unwrap();
    assert!(matches!(
        crate::tests::database_error(error),
        crate::DbError::KeyNotUuid { .. }
    ));
    let db = store.schema(notes(), NOTES).await.unwrap();
    assert_eq!(count(&db, "coven_writes"), 0);
    assert_eq!(count(&db, "notes"), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn dropping_a_foreign_key_sets_the_changed_reference() {
    const INITIAL: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id))";
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ]
    };
    let store = TestStore::new();
    let db = store.schema(tables(), INITIAL).await.unwrap();
    sql(
        &db,
        "INSERT INTO parents VALUES('p'); INSERT INTO children VALUES('c','p')",
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    let db = store.builder(tables(), vec![Migration::sql(1,"initial",INITIAL), Migration::sql(2,"drop reference","CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,parent TEXT); INSERT INTO rebuilt SELECT * FROM children; DROP TABLE children; ALTER TABLE rebuilt RENAME TO children")]).open().await.unwrap();
    assert_eq!(setters(&db, "children", "c")["parent"], 2);
    assert_eq!(count(&db, "coven_references"), 0);
    sql(&db, "DELETE FROM parents").await.unwrap();
    assert_eq!(count(&db, "children"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn new_foreign_keys_set_existing_cells_and_name_the_new_parent_generation() {
    const INITIAL: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT)";
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ]
    };
    let store = TestStore::new();
    let db = store.schema(tables(), INITIAL).await.unwrap();
    sql(&db, "INSERT INTO children VALUES('c','p'),('null',NULL)")
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store.builder(tables(),vec![Migration::sql(1,"initial",INITIAL), Migration::sql(2,"add reference", "INSERT INTO parents VALUES('p'); CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id) ON DELETE SET NULL); INSERT INTO rebuilt SELECT * FROM children; DROP TABLE children; ALTER TABLE rebuilt RENAME TO children")]).open().await.unwrap();
    for key in ["c", "null"] {
        assert_eq!(
            setters(&db, "children", key),
            [("id".into(), 1), ("parent".into(), 2)].into()
        );
    }
    assert_eq!(setters(&db, "parents", "p")["id"], 2);
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row(
                "SELECT parent_generation FROM coven_references",
                [],
                |r| Ok(crate::write_encoding::counter(r.get(0)?))
            )
            .unwrap()),
        1
    );
    sql(&db, "DELETE FROM parents").await.unwrap();
    assert_eq!(count(&db, "children"), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn rebuilding_the_same_schema_is_breaking_even_when_no_values_change() {
    for (copy, title_setter) in [("title", 2), ("upper(title)", 3)] {
        let store = TestStore::new();
        seeded(&store).await.close().await.unwrap();
        let db = store.builder(notes(),sequence(vec![Migration::run(2,"rebuild",move |c| {
            c.execute_batch(&format!("CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL,body TEXT NOT NULL DEFAULT ''); INSERT INTO rebuilt SELECT id,{copy},body FROM notes; DROP TABLE notes; ALTER TABLE rebuilt RENAME TO notes"))?;
            Ok(())
        })])).open().await.unwrap();
        assert_eq!(
            db.applied_migrations().unwrap()[0].change,
            MigrationChange::Breaking
        );
        assert_eq!(setters(&db, "notes", "n")["title"], title_setter);
        assert_eq!(records(&db).len(), 3);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_explicit_column_rename_survives_a_subsequent_table_rebuild() {
    let store = TestStore::new();
    seeded(&store).await.close().await.unwrap();
    let db = store.builder(notes(),sequence(vec![Migration::sql(2,"rename and rebuild","ALTER TABLE notes RENAME COLUMN title TO name; CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL,body TEXT); INSERT INTO rebuilt SELECT * FROM notes; DROP TABLE notes; ALTER TABLE rebuilt RENAME TO notes")])).open().await.unwrap();
    assert_eq!(setters(&db, "notes", "n")["name"], 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn changing_primary_key_arity_deletes_old_rows_and_inserts_new_ones() {
    let store = TestStore::new();
    seeded(&store).await.close().await.unwrap();
    let db = store.builder(vec![SyncedTable::new("notes",RowIdentity::SharedKey).key_columns(["id","part"])],sequence(vec![Migration::sql(2,"key","CREATE TABLE rebuilt(id TEXT NOT NULL,part INT NOT NULL,title TEXT NOT NULL,body TEXT,PRIMARY KEY(id,part)); INSERT INTO rebuilt SELECT id,1,title,body FROM notes; DROP TABLE notes; ALTER TABLE rebuilt RENAME TO notes")])).open().await.unwrap();
    assert_eq!(count(&db, "notes"), 2);
    db.inspect_writer(|db| {
        let numbers = db
            .query(
                "SELECT w.number FROM coven_cells c JOIN coven_writes w ON w.id=c.write_id",
                [],
                |r| Ok(crate::write_encoding::counter(r.get(0)?)),
            )
            .unwrap();
        assert_eq!(numbers, vec![3; 8]);
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn renaming_a_partially_updated_composite_reference_keeps_both_setters() {
    const INITIAL: &str = "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT,UNIQUE(a,b)); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT,FOREIGN KEY(a,b) REFERENCES parents(a,b))";
    let tables = || {
        vec![
            SyncedTable::new("parents", RowIdentity::SharedKey),
            SyncedTable::new("children", RowIdentity::SharedKey),
        ]
    };
    let store = TestStore::new();
    let db = store.schema(tables(), INITIAL).await.unwrap();
    sql(&db,"INSERT INTO parents VALUES('p','a','b'),('q','a','c'); INSERT INTO children VALUES('child','a','b')").await.unwrap();
    sql(&db, "UPDATE children SET b='c'").await.unwrap();
    db.close().await.unwrap();
    let db = store
        .builder(
            tables(),
            vec![
                Migration::sql(1, "initial", INITIAL),
                Migration::sql(
                    2,
                    "rename",
                    "ALTER TABLE children RENAME COLUMN a TO renamed",
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    assert_eq!(
        setters(&db, "children", "child"),
        [("id".into(), 1), ("renamed".into(), 1), ("b".into(), 2)].into()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn matching_cells_survive_absence_between_migrations_of_one_run() {
    let store = TestStore::new();
    seeded(&store).await.close().await.unwrap();
    let db = store
        .builder(
            notes(),
            sequence(vec![
                Migration::sql(2, "drop", "ALTER TABLE notes DROP COLUMN body"),
                Migration::sql(
                    3,
                    "recreate",
                    "ALTER TABLE notes ADD COLUMN body TEXT DEFAULT 'body'",
                ),
            ]),
        )
        .open()
        .await
        .unwrap();
    assert_eq!(setters(&db, "notes", "n")["body"], 1);
    assert_eq!(setters(&db, "notes", "deleted")["body"], 3);
    assert_eq!(records(&db).len(), 3);
    assert_eq!(records(&db)[2].header.schema_version, 3);
    db.close().await.unwrap();
}

#[tokio::test]
async fn retiring_a_removed_row_keeps_its_cell_loss_names_while_live_cells_are_renamed() {
    const INITIAL: &str =
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT CHECK(title<>'bad'),body TEXT)";
    let store = TestStore::new();
    let db = store.schema(notes(), INITIAL).await.unwrap();
    sql(
        &db,
        "INSERT INTO notes VALUES('removed','good','body'),('live','good','body')",
    )
    .await
    .unwrap();
    let original = records(&db).remove(0);
    sql(
        &db,
        "UPDATE notes SET title='concurrent' WHERE id='removed'",
    )
    .await
    .unwrap();
    let latest = records(&db).pop().unwrap();
    let remote = downloaded_update(
        original,
        "removed",
        latest.header.timestamp,
        &[("title", "good", "bad")],
    );
    db.apply_downloaded(remote.into()).await.unwrap();
    let lost = db.lost_values().await.unwrap();
    assert_eq!(lost.len(), 2);
    db.close().await.unwrap();
    let db = store.builder(vec![SyncedTable::new("renamed",RowIdentity::SharedKey)],vec![Migration::sql(1,"initial",INITIAL),Migration::sql(2,"rename","ALTER TABLE notes RENAME TO renamed; ALTER TABLE renamed RENAME COLUMN title TO name")]).open().await.unwrap();
    assert_eq!(db.lost_values().await.unwrap(), lost);
    assert_eq!(setters(&db, "renamed", "live")["name"], 1);
    assert!(setters(&db, "renamed", "removed").is_empty());
    sql(&db, "UPDATE renamed SET name='later' WHERE id='live'")
        .await
        .unwrap();
    assert_eq!(db.lost_values().await.unwrap(), lost);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_renamed_unique_constraint_replaces_the_dropped_columns_constraint() {
    const INITIAL: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,x TEXT,y TEXT); CREATE UNIQUE INDEX x_index ON notes(x); CREATE UNIQUE INDEX y_index ON notes(y)";
    let store = TestStore::new();
    let db = store.schema(notes(), INITIAL).await.unwrap();
    sql(
        &db,
        "INSERT INTO notes VALUES('a','x','y'),('b','other-x','other-y')",
    )
    .await
    .unwrap();
    let original = records(&db).remove(0);
    let timestamp = original.header.timestamp;
    let remote = downloaded_update(
        original,
        "b",
        timestamp,
        &[("x", "other-x", "x"), ("y", "other-y", "y")],
    );
    db.apply_downloaded(remote.into()).await.unwrap();
    assert_eq!(count(&db, "coven_constraints"), 2);
    let original = db.inspect_writer(|db| {
        db.query_row(
            "SELECT id FROM coven_constraints WHERE identity=?1",
            [coven_format::merge_fields::encode_unique_constraint(&["x"].into()).unwrap()],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
    });
    let lost = db.lost_values().await.unwrap();
    db.close().await.unwrap();
    let db = store.builder(notes(),vec![Migration::sql(1,"initial",INITIAL),Migration::sql(2,"replace column","DROP INDEX y_index; ALTER TABLE notes DROP COLUMN y; ALTER TABLE notes RENAME COLUMN x TO y")]).open().await.unwrap();
    assert_eq!(db.lost_values().await.unwrap(), lost);
    assert_eq!(count(&db, "coven_constraints"), 1);
    db.inspect_writer(|db| {
        let (id, identity) = db
            .query_row("SELECT id,identity FROM coven_constraints", [], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .unwrap();
        assert_eq!(id, original);
        assert_eq!(
            coven_format::merge_fields::decode_unique_constraint(&identity).unwrap(),
            ["y"].into()
        );
    });
    assert_eq!(count(&db, "notes"), 1);
    db.close().await.unwrap();
}

fn downloaded_update(
    mut original: coven_format::write::WriteRecord,
    key: &str,
    after: coven_merge::Timestamp,
    columns: &[(&str, &str, &str)],
) -> coven_format::write::WriteRecord {
    use coven_format::value::{Value, WritePositions};
    use coven_foundation::id_source::DeviceId;
    use coven_merge::{Change, ColumnValue, Operation, Timestamp, WriteId};
    let past = original.header.position;
    let id = WriteId {
        device: DeviceId(99),
        number: 1,
    };
    original.header.position = id;
    original.header.timestamp = Timestamp::new(after.milliseconds() + 1, 0, id.device).unwrap();
    original.header.had_read = WritePositions(vec![past]);
    let key = coven_format::key::encode_key(&[Value::Text(key.into())]).unwrap();
    original.parts[0].rows.retain(|row| row.row.key == key);
    assert_eq!(original.parts[0].rows.len(), 1);
    let row = &mut original.parts[0].rows[0];
    row.change = Change {
        generation: row.change.generation + 1,
        operation: Operation::Update(
            columns
                .iter()
                .map(|(name, _, value)| {
                    (
                        (*name).into(),
                        ColumnValue {
                            value: Value::Text((*value).into()),
                            parents: BTreeMap::new(),
                        },
                    )
                })
                .collect(),
        ),
    };
    row.old = columns
        .iter()
        .map(|(name, value, _)| ((*name).into(), Value::Text((*value).into())))
        .collect();
    original
}
