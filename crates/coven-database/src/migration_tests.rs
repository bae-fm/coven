use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::{database_error, TestStore},
    *,
};

#[tokio::test]
async fn final_declarations_validate_only_after_the_whole_run() {
    let store = TestStore::new();
    let db = store
        .builder(
            vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).key_columns(["note_id"])],
            vec![
                Migration::addition(
                    1,
                    "initial",
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)",
                ),
                Migration::breaking(
                    2,
                    "rename",
                    "ALTER TABLE notes RENAME COLUMN id TO note_id",
                    crate::OldWrites::Drop,
                ),
                Migration::addition(3, "body", "ALTER TABLE notes ADD COLUMN body TEXT"),
            ],
        )
        .open()
        .await
        .unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 3);
    assert_eq!(
        db.applied_migrations()
            .unwrap()
            .iter()
            .map(|m| m.change.clone())
            .collect::<Vec<_>>(),
        [
            MigrationChange::Addition,
            MigrationChange::Breaking,
            MigrationChange::Addition
        ]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_later_failure_rolls_back_every_pending_migration_and_version() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.close().await.unwrap();
    let mut migrations = notes_migrations();
    migrations.extend([
        Migration::addition(2, "add", "ALTER TABLE notes ADD COLUMN color TEXT"),
        Migration::breaking_with(
            3,
            "backfill",
            |sql| {
                sql.execute(
                    "INSERT INTO notes(id,title) VALUES (?1,?2)",
                    ("f47ac10b-58cc-4372-a567-0e02b2c3d479", "before failure"),
                )?;
                sql.execute_batch("SELECT missing_column FROM notes")?;
                Ok(())
            },
            crate::OldWrites::Drop,
        ),
    ]);
    let error = store
        .builder(notes_tables(), migrations)
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        CovenError::Migration(MigrationError::Failed {
            version: 3,
            name: "backfill",
            ..
        })
    ));
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    db.inspect_writer(|sql| {
        assert_eq!(
            sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            sql.query_row(
                "SELECT count(*) FROM pragma_table_info('notes') WHERE name = 'color'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn final_schema_failure_rolls_back_the_entire_app_run() {
    let store = TestStore::new();
    let error = store
        .builder(
            notes_tables(),
            vec![
                Migration::addition(
                    1,
                    "initial",
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)",
                ),
                Migration::breaking(
                    2,
                    "remove key",
                    "DROP TABLE notes; CREATE TABLE notes(title TEXT)",
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        database_error(error),
        DbError::Schema(SchemaError::NoPrimaryKey { .. })
    ));
    let raw = rusqlite::Connection::open(store.database_path()).unwrap();
    assert_eq!(
        raw.query_row("PRAGMA user_version", [], |r| r.get::<_, i32>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        raw.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = 'notes'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn migration_numbering_and_newer_database_errors_are_exact() {
    let store = TestStore::new();
    let error = store
        .builder(vec![], vec![Migration::addition(2, "gap", "SELECT 1")])
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        CovenError::Migration(MigrationError::NotContiguous {
            position: 0,
            found: 2,
            expected: 1
        })
    ));
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.close().await.unwrap();
    for read_only in [true, false] {
        let builder = store.builder(notes_tables(), vec![]);
        let error = if read_only {
            builder.open_read_only().await.map(|_| ())
        } else {
            builder.open().await.map(|_| ())
        }
        .err()
        .unwrap();
        assert!(matches!(
            error,
            CovenError::Migration(MigrationError::SchemaTooNew {
                current: 1,
                supported: 0
            })
        ));
    }
}

#[tokio::test]
async fn read_only_open_never_runs_pending_app_migrations() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let mut migrations = notes_migrations();
    migrations.push(Migration::addition_with(2, "must not run", |_| {
        panic!("read-only migration")
    }));
    let reader = store
        .builder(notes_tables(), migrations)
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(reader.schema_version().await.unwrap(), 1);
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn classify_each_sqlite_definition_change() {
    let cases = [
        (
            "ALTER TABLE notes ADD COLUMN color TEXT",
            MigrationChange::Addition,
        ),
        (
            "ALTER TABLE notes ADD COLUMN color TEXT DEFAULT 'blue'",
            MigrationChange::Addition,
        ),
        (
            "ALTER TABLE notes ADD COLUMN n INTEGER CHECK(n > 0)",
            MigrationChange::Breaking,
        ),
        (
            "CREATE INDEX titles ON notes(title)",
            MigrationChange::Breaking,
        ),
        (
            "CREATE TABLE extra(id TEXT PRIMARY KEY); CREATE INDEX extra_ids ON extra(id)",
            MigrationChange::NoChange,
        ),
        (
            "CREATE TABLE extra(id TEXT PRIMARY KEY); CREATE TRIGGER extra_insert AFTER INSERT ON extra BEGIN SELECT 1; END",
            MigrationChange::NoChange,
        ),
        (
            "CREATE TABLE extra(id TEXT PRIMARY KEY)",
            MigrationChange::NoChange,
        ),
        (
            "CREATE VIEW titles AS SELECT title FROM notes",
            MigrationChange::NoChange,
        ),
        (
            "ALTER TABLE notes RENAME COLUMN title TO name",
            MigrationChange::Breaking,
        ),
        (
            "ALTER TABLE notes DROP COLUMN body",
            MigrationChange::Breaking,
        ),
        (
            "CREATE TRIGGER no_op AFTER INSERT ON notes BEGIN SELECT 1; END",
            MigrationChange::Breaking,
        ),
    ];
    for (sql, expected) in cases {
        let store = TestStore::new();
        let mut migrations = notes_migrations();
        migrations.push(if expected == MigrationChange::Breaking {
            Migration::breaking(2, "change", sql, crate::OldWrites::Drop)
        } else {
            Migration::addition(2, "change", sql)
        });
        let db = store
            .builder(notes_tables(), migrations)
            .open()
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(
            db.applied_migrations().unwrap()[1].change,
            expected,
            "{sql}"
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn migration_context_queries_and_parameters_use_the_actual_transaction() {
    let store = TestStore::new();
    let db = store
        .builder(
            vec![],
            vec![Migration::addition_with(1, "data", |sql| {
                sql.execute_batch("CREATE TABLE local(id INTEGER, title TEXT)")?;
                assert_eq!(
                    sql.execute("INSERT INTO local VALUES (?1, ?2)", (7, "title"))?,
                    1
                );
                assert_eq!(
                    sql.query_row("SELECT title FROM local WHERE id=?1", [7], |r| r
                        .get::<_, String>(0))?,
                    "title"
                );
                assert_eq!(
                    sql.query("SELECT id FROM local", [], |r| r.get::<_, i64>(0))?,
                    [7]
                );
                Ok(())
            })],
        )
        .open()
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reordering_existing_columns_is_a_breaking_schema_change() {
    let store = TestStore::new();
    let db = store
        .builder(
            vec![SyncedTable::new("synced", RowIdentity::SharedKey)],
            vec![
                Migration::addition(1, "initial", "CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT)"),
                Migration::breaking(2, "reorder", "DROP TABLE synced; CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY,b TEXT,a TEXT)", crate::OldWrites::Drop),
            ],
        )
        .open()
        .await
        .unwrap();
    assert_eq!(
        db.applied_migrations().unwrap()[1].change,
        MigrationChange::Breaking
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn batch_sql_reports_execution_errors_after_the_first_result_row() {
    let store = TestStore::new();
    let error = store
        .builder(
            vec![],
            vec![Migration::addition(
                1,
                "overflow",
                "SELECT abs(x) FROM (SELECT 1 AS x UNION ALL SELECT -9223372036854775808)",
            )],
        )
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(database_error(error), DbError::Sqlite(_)));
}

#[tokio::test]
async fn an_implicit_sqlite_rollback_cannot_escape_the_owned_transaction() {
    for continue_writing in [false, true] {
        let store = TestStore::new();
        let result = store
            .builder(
                vec![],
                vec![Migration::addition_with(1, "rollback", move |sql| {
                    sql.execute_batch(
                        "CREATE TABLE local(id INTEGER UNIQUE); INSERT INTO local VALUES (1)",
                    )?;
                    assert!(sql
                        .execute("INSERT OR ROLLBACK INTO local VALUES (1)", [])
                        .is_err());
                    if continue_writing {
                        // Even an app that catches its SQL error cannot begin autocommit work.
                        assert!(sql
                            .execute_batch("CREATE TABLE escaped(id INTEGER)")
                            .is_err());
                    }
                    Ok(())
                })],
            )
            .open()
            .await;
        assert!(
            result.is_err(),
            "the rolled-back migration cannot be committed"
        );
        let raw = rusqlite::Connection::open(store.database_path()).unwrap();
        assert_eq!(
            raw.query_row("PRAGMA user_version", [], |r| r.get::<_, i32>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            raw.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name IN ('local','escaped')",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn local_schema_and_views_have_no_migration_change() {
    for change in [
        "SELECT 1",
        "INSERT INTO local VALUES('key','title',NULL)",
        "ALTER TABLE local ADD COLUMN color TEXT CHECK(color <> '')",
        "ALTER TABLE local RENAME COLUMN title TO name",
        "ALTER TABLE local DROP COLUMN extra",
        "DROP TABLE local; CREATE TABLE local(title BLOB,id INTEGER PRIMARY KEY)",
        "CREATE INDEX local_extra ON local(extra)",
        "CREATE UNIQUE INDEX unique_extra ON local(extra)",
        "DROP INDEX local_titles",
        "DROP INDEX local_titles; CREATE INDEX local_titles ON local(extra)",
        "CREATE TRIGGER inserted AFTER INSERT ON local BEGIN SELECT 1; END",
        "DROP TRIGGER edited",
        "DROP TRIGGER edited; CREATE TRIGGER edited AFTER DELETE ON local BEGIN SELECT 2; END",
        "CREATE VIEW titles AS SELECT title FROM local",
        "CREATE VIEW titles AS SELECT title FROM notes",
        "DROP VIEW existing",
        "DROP VIEW existing; CREATE VIEW existing AS SELECT extra FROM local",
        "CREATE TRIGGER view_insert INSTEAD OF INSERT ON existing BEGIN SELECT 1; END",
        "CREATE TABLE another(id TEXT); CREATE INDEX another_ids ON another(id)",
    ] {
        let store = TestStore::new();
        let db = store.builder(notes_tables(), vec![
            Migration::addition(1, "tables", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE TABLE local(id TEXT PRIMARY KEY,title TEXT,extra TEXT); CREATE INDEX local_titles ON local(title); CREATE TRIGGER edited AFTER UPDATE ON local BEGIN SELECT 1; END; CREATE VIEW existing AS SELECT title FROM notes"),
            Migration::addition(2, "change", change),
        ]).open().await.unwrap();
        assert_eq!(
            db.applied_migrations().unwrap()[1].change,
            MigrationChange::NoChange,
            "{change}"
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn new_synced_tables_with_indexes_and_triggers_are_additions() {
    for schema in [
        "CREATE TABLE extra(id TEXT NOT NULL PRIMARY KEY)",
        "CREATE TABLE extra(id TEXT NOT NULL PRIMARY KEY); CREATE INDEX extra_ids ON extra(id)",
        "CREATE TABLE extra(id TEXT NOT NULL PRIMARY KEY); CREATE TRIGGER inserted AFTER INSERT ON extra BEGIN SELECT 1; END",
    ] {
        let store = TestStore::new();
        let db = store.schema(vec![SyncedTable::new("EXTRA", RowIdentity::SharedKey)], schema).await.unwrap();
        assert_eq!(db.applied_migrations().unwrap()[0].change, MigrationChange::Addition);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn dropping_or_renaming_any_table_is_breaking() {
    for initially_synced in [false, true] {
        for (sql, declarations) in [
            ("DROP TABLE old", vec![]),
            ("ALTER TABLE old RENAME TO renamed", vec![]),
            (
                "ALTER TABLE old RENAME TO renamed",
                vec![SyncedTable::new("renamed", RowIdentity::SharedKey)],
            ),
        ] {
            let store = TestStore::new();
            let initial = || {
                Migration::addition(
                    1,
                    "initial",
                    "CREATE TABLE old(id TEXT NOT NULL PRIMARY KEY)",
                )
            };
            let tables = if initially_synced {
                vec![SyncedTable::new("old", RowIdentity::SharedKey)]
            } else {
                vec![]
            };
            store
                .builder(tables, vec![initial()])
                .open()
                .await
                .unwrap()
                .close()
                .await
                .unwrap();
            let db = store
                .builder(
                    declarations,
                    vec![
                        initial(),
                        Migration::breaking(2, "change", sql, crate::OldWrites::Drop),
                    ],
                )
                .open()
                .await
                .unwrap();
            assert_eq!(
                db.applied_migrations().unwrap()[0].change,
                MigrationChange::Breaking,
                "{sql}, initially synced: {initially_synced}"
            );
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn local_changes_do_not_hide_synced_additions_or_breaks() {
    for (sql, expected) in [
        ("ALTER TABLE notes ADD COLUMN extra TEXT; CREATE INDEX local_titles ON local(title)", MigrationChange::Addition),
        ("CREATE INDEX note_titles ON notes(title); ALTER TABLE local ADD COLUMN extra TEXT", MigrationChange::Breaking),
        ("DROP INDEX existing_index", MigrationChange::Breaking),
        ("DROP TRIGGER existing_trigger", MigrationChange::Breaking),
        ("DROP TRIGGER existing_trigger; CREATE TRIGGER existing_trigger AFTER DELETE ON notes BEGIN SELECT 2; END", MigrationChange::Breaking),
    ] {
        let store = TestStore::new();
        let db = store.builder(vec![SyncedTable::new("NOTES", RowIdentity::SharedKey)], vec![
            Migration::addition(1, "initial", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE INDEX existing_index ON notes(title); CREATE TRIGGER existing_trigger AFTER INSERT ON notes BEGIN SELECT 1; END; CREATE TABLE local(title TEXT)"),
            if expected == MigrationChange::Breaking {
            Migration::breaking(2, "change", sql, crate::OldWrites::Drop)
        } else { Migration::addition(2, "change", sql) },
        ]).open().await.unwrap();
        assert_eq!(db.applied_migrations().unwrap()[1].change, expected, "{sql}");
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn moving_an_index_or_trigger_between_local_and_synced_tables_is_breaking() {
    for (before, after) in [("local", "notes"), ("notes", "local")] {
        for (definition, kind) in [
            ("INDEX effect ON", "INDEX"),
            ("TRIGGER effect AFTER INSERT ON", "TRIGGER"),
        ] {
            let suffix = if kind == "INDEX" {
                "(title)"
            } else {
                " BEGIN SELECT 1; END"
            };
            let initial = format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE TABLE local(title TEXT); CREATE {definition} {before}{suffix}");
            let changed = format!("DROP {kind} effect; CREATE {definition} {after}{suffix}");
            let store = TestStore::new();
            let db = store
                .builder(
                    notes_tables(),
                    vec![
                        Migration::addition_with(1, "initial", move |sql| {
                            sql.execute_batch(&initial)?;
                            Ok(())
                        }),
                        Migration::breaking_with(
                            2,
                            "change",
                            move |sql| {
                                sql.execute_batch(&changed)?;
                                Ok(())
                            },
                            crate::OldWrites::Drop,
                        ),
                    ],
                )
                .open()
                .await
                .unwrap();
            assert_eq!(
                db.applied_migrations().unwrap()[1].change,
                MigrationChange::Breaking,
                "{kind} from {before} to {after}"
            );
            db.close().await.unwrap();
        }
    }
}

pub(crate) mod declarations {
    use crate::{
        tests::{database_error, TestStore},
        write::tests::{notes, sql, NOTES},
        DbError, Migration, OldWrites,
    };
    use serde_json::json;
    use std::{collections::BTreeMap, sync::Arc};

    #[derive(Debug, PartialEq)]
    struct Committed {
        version: u32,
        schema: Vec<(String, String, String, Option<String>)>,
        tables: BTreeMap<String, Vec<Vec<crate::types::Value>>>,
    }

    // Inspect the committed file before reopening can run any registration work.
    fn committed(store: &TestStore) -> Committed {
        let db = rusqlite::Connection::open_with_flags(
            store.database_path(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let version = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        let schema: Vec<(String, String, String, Option<String>)> = db
            .prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let tables = schema
            .iter()
            .filter(|object| object.0 == "table")
            .map(|object| {
                let values = db
                    .prepare(&format!(
                        "SELECT * FROM {} ORDER BY 1",
                        crate::sql::identifier(&object.1)
                    ))
                    .unwrap()
                    .query_map([], |r| {
                        (0..r.as_ref().column_count()).map(|i| r.get(i)).collect()
                    })
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                (object.1.clone(), values)
            })
            .collect();
        Committed {
            version,
            schema,
            tables,
        }
    }

    pub(crate) async fn compare(runner: Option<&std::ffi::OsStr>) {
        for (statement, actual_breaking) in [
            ("ALTER TABLE notes RENAME COLUMN title TO name", true),
            ("UPDATE notes SET title='candidate'", true),
            ("DELETE FROM notes", true),
            ("INSERT INTO notes VALUES('new','candidate','body')", true),
            ("ALTER TABLE notes ADD COLUMN color TEXT", false),
            (
                "CREATE TABLE cache(value TEXT); INSERT INTO cache VALUES('candidate')",
                false,
            ),
            ("SELECT 1", false),
        ] {
            for populated in [false, true] {
                for code in [false, true] {
                    for convert in [false, true] {
                        if actual_breaking && convert {
                            continue;
                        }
                        let store = TestStore::new();
                        let db = store.schema(notes(), NOTES).await.unwrap();
                        crate::raise_capture::tests::connect(&db).await;
                        if populated {
                            sql(&db, "INSERT INTO notes VALUES('n','original','body')")
                                .await
                                .unwrap();
                            crate::upload::tests::attempt(&db, 255).await.unwrap();
                            sql(&db, "UPDATE notes SET title='waiting'").await.unwrap();
                        }
                        db.close().await.unwrap();
                        let before = committed(&store);
                        let body = move |context: &crate::MigrationContext<'_>| {
                            context.execute_batch(statement)?;
                            Ok(())
                        };
                        let migration = if actual_breaking {
                            if code {
                                Migration::addition_with(3, "mismatch", body)
                            } else {
                                Migration::addition(3, "mismatch", statement)
                            }
                        } else {
                            let old_writes = if convert {
                                OldWrites::Convert(Arc::new(|_| {
                                    panic!("a refused migration cannot convert")
                                }))
                            } else {
                                OldWrites::Drop
                            };
                            if code {
                                Migration::breaking_with(3, "mismatch", body, old_writes)
                            } else {
                                Migration::breaking(3, "mismatch", statement, old_writes)
                            }
                        };
                        let error = store
                            .builder(
                                notes(),
                                vec![
                                    Migration::addition(1, "notes", NOTES),
                                    Migration::breaking(
                                        2,
                                        "backfill",
                                        "UPDATE notes SET body='candidate'",
                                        OldWrites::Drop,
                                    ),
                                    migration,
                                    Migration::addition(
                                        4,
                                        "extra",
                                        "ALTER TABLE notes ADD COLUMN extra TEXT",
                                    ),
                                ],
                            )
                            .migration_operation(|_| panic!("a refused batch cannot publish"))
                            .open()
                            .await
                            .err()
                            .expect("mismatched kind must be refused");
                        let DbError::MigrationKindMismatch { version } = database_error(error)
                        else {
                            panic!("expected the typed migration-kind error")
                        };
                        assert_eq!(
                            version, 3,
                            "{statement}, code {code}, populated {populated}"
                        );
                        let after = committed(&store);
                        assert_eq!(
                            after, before,
                            "{statement}, code {code}, populated {populated}"
                        );
                        if let Some(runner) = runner {
                            let declaration = if actual_breaking {
                                "addition"
                            } else if convert {
                                "convert"
                            } else {
                                "drop"
                            };
                            let model = crate::tests::lean_model(
                                runner,
                                json!({
                                    "version": before.version, "raise": 4, "values": [5], "steps": [
                                        {"version": 2, "breaking": true, "declaration": "drop"},
                                        {"version": 3, "breaking": actual_breaking, "declaration": declaration, "digit": 3},
                                        {"version": 4, "breaking": false, "declaration": "addition"},
                                    ],
                                }),
                            );
                            assert_eq!(
                                model,
                                json!({"mismatch": version, "version": after.version})
                            );
                        }
                        store
                            .schema(notes(), NOTES)
                            .await
                            .unwrap()
                            .close()
                            .await
                            .unwrap();
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn migration_kind_mismatches_roll_back_every_pending_step() {
        compare(None).await;
    }

    #[tokio::test]
    async fn committed_classifications_refuse_changed_declarations_before_running_new_steps() {
        for breaking in [false, true] {
            for read_only in [false, true] {
                let store = TestStore::new();
                let original = || {
                    vec![
                        Migration::addition(1, "notes", NOTES),
                        if breaking {
                            Migration::breaking(
                                2,
                                "index",
                                "CREATE INDEX titles ON notes(title)",
                                OldWrites::Drop,
                            )
                        } else {
                            Migration::addition(
                                2,
                                "color",
                                "ALTER TABLE notes ADD COLUMN color TEXT",
                            )
                        },
                    ]
                };
                store
                    .builder(notes(), original())
                    .open()
                    .await
                    .unwrap()
                    .close()
                    .await
                    .unwrap();
                let before = committed(&store);
                let mut changed = original();
                changed[1] = if breaking {
                    Migration::addition(2, "wrong", "CREATE INDEX titles ON notes(title)")
                } else {
                    Migration::breaking(
                        2,
                        "wrong",
                        "ALTER TABLE notes ADD COLUMN color TEXT",
                        OldWrites::Drop,
                    )
                };
                changed.push(Migration::addition_with(3, "must not run", |_| {
                    panic!("old declaration mismatch")
                }));
                let builder = store.builder(notes(), changed);
                let error = if read_only {
                    builder.open_read_only().await.map(|_| ())
                } else {
                    builder.open().await.map(|_| ())
                }
                .unwrap_err();
                assert!(matches!(
                    database_error(error),
                    DbError::MigrationKindMismatch { version: 2 }
                ));
                assert_eq!(committed(&store), before);
            }
        }
    }
}
