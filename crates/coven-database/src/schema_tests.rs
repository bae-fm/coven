use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::{database_error, TestStore},
    *,
};

async fn rejects(sql: &'static str, tables: Vec<SyncedTable>, expected: SchemaError) {
    let store = TestStore::new();
    let error = store
        .schema(tables, sql)
        .await
        .err()
        .expect("invalid schema must fail");
    match database_error(error) {
        DbError::Schema(error) => assert_eq!(error, expected),
        error => panic!("expected {expected:?}, received {error:?}"),
    }
}

fn shared(name: &str) -> SyncedTable {
    SyncedTable::new(name, RowIdentity::SharedKey)
}
fn independent(name: &str) -> SyncedTable {
    SyncedTable::new(name, RowIdentity::IndependentUuid)
}

#[tokio::test]
async fn set_default_is_refused_on_open_and_after_migrating() {
    for (child_synced, parent_synced) in [(true, true), (true, false), (false, true)] {
        for action in [
            "ON DELETE SET DEFAULT",
            "ON DELETE CASCADE ON UPDATE SET DEFAULT",
        ] {
            for default in ["", "DEFAULT 'fallback'", "NOT NULL DEFAULT 'fallback'"] {
                let store = TestStore::new();
                let tables = || {
                    let mut tables = Vec::new();
                    if child_synced {
                        tables.push(shared("CHILD"));
                    }
                    if parent_synced {
                        tables.push(shared("PARENT"));
                    }
                    tables
                };
                let schema = format!("CREATE TABLE Parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE Child(id TEXT NOT NULL PRIMARY KEY, parent TEXT {default} REFERENCES pArEnT(id) {action})");
                let migration_schema = schema.clone();
                let expected = SchemaError::SetDefault {
                    table: "Child".into(),
                    key: "(\"parent\") REFERENCES \"pArEnT\" (\"id\")".into(),
                };
                let error = store
                    .builder(
                        tables(),
                        vec![Migration::run(1, "foreign key", move |sql| {
                            sql.execute_batch(&migration_schema)?;
                            Ok(())
                        })],
                    )
                    .open()
                    .await
                    .err()
                    .expect("SET DEFAULT must fail migration");
                assert!(
                    matches!(database_error(error), DbError::Schema(found) if found == expected)
                );
                let db = store.builder(vec![], vec![]).open().await.unwrap();
                db.inspect_writer(|writer| {
                    assert_eq!(writer.schema_version().unwrap(), 0);
                    assert_eq!(writer.query_row("SELECT count(*) FROM sqlite_schema WHERE name IN ('Parent','Child')", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
                    writer.batch(&schema).unwrap();
                });
                db.close().await.unwrap();
                for read_only in [false, true] {
                    let builder = store.builder(tables(), vec![]);
                    let error = if read_only {
                        builder.open_read_only().await.map(|_| ())
                    } else {
                        builder.open().await.map(|_| ())
                    }
                    .expect_err("SET DEFAULT must fail open");
                    assert!(
                        matches!(database_error(error), DbError::Schema(found) if found == expected)
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn set_default_errors_identify_composite_implicit_and_self_references() {
    for (schema, key) in [
        (
            "CREATE TABLE parent(a TEXT,b TEXT,PRIMARY KEY(a,b)); CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY,x TEXT,y TEXT,FOREIGN KEY(x,y) REFERENCES parent(a,b) ON DELETE SET DEFAULT)",
            "(\"x\", \"y\") REFERENCES \"parent\" (\"a\", \"b\")",
        ),
        (
            "CREATE TABLE parent(id TEXT PRIMARY KEY); CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY,p TEXT REFERENCES parent ON UPDATE SET DEFAULT)",
            "(\"p\") REFERENCES \"parent\"",
        ),
        (
            "CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY,p TEXT REFERENCES child(id) ON DELETE SET DEFAULT)",
            "(\"p\") REFERENCES \"child\" (\"id\")",
        ),
        (
            "CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY,p TEXT REFERENCES missing(id) ON DELETE SET DEFAULT)",
            "(\"p\") REFERENCES \"missing\" (\"id\")",
        ),
    ] {
        let expected = SchemaError::SetDefault {
            table: "child".into(),
            key: key.into(),
        };
        assert!(expected.to_string().contains(key));
        assert!(expected.to_string().contains("child"));
        rejects(schema, vec![shared("child")], expected).await;
    }
}

#[tokio::test]
async fn a_set_default_migration_preserves_the_previous_schema_and_rows() {
    const INITIAL: &str = "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY,p TEXT REFERENCES parent(id) ON DELETE CASCADE)";
    for event in ["DELETE", "UPDATE"] {
        let store = TestStore::new();
        let tables = || vec![shared("parent"), shared("child")];
        let db = store.schema(tables(), INITIAL).await.unwrap();
        crate::write::tests::sql(
            &db,
            "INSERT INTO parent VALUES('p'); INSERT INTO child VALUES('c','p')",
        )
        .await
        .unwrap();
        db.close().await.unwrap();
        let schema = format!("CREATE TABLE rebuilt(id TEXT NOT NULL PRIMARY KEY,p TEXT REFERENCES parent(id) ON {event} SET DEFAULT); INSERT INTO rebuilt SELECT * FROM child; DROP TABLE child; ALTER TABLE rebuilt RENAME TO child");
        let error = store
            .builder(
                tables(),
                vec![
                    Migration::sql(1, "schema", INITIAL),
                    Migration::run(2, "change reference", move |sql| {
                        sql.execute_batch(&schema)?;
                        Ok(())
                    }),
                ],
            )
            .open()
            .await
            .err()
            .expect("SET DEFAULT must fail migration");
        assert!(
            matches!(database_error(error), DbError::Schema(SchemaError::SetDefault {table, key}) if table == "child" && key == "(\"p\") REFERENCES \"parent\" (\"id\")")
        );
        let db = store.schema(tables(), INITIAL).await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(), 1);
        db.read(|sql| {
            assert_eq!(sql.query_row("SELECT p FROM child WHERE id='c'", [], |r| r.get::<_, String>(0))?, "p");
            assert_eq!(sql.query_row("SELECT sql FROM sqlite_schema WHERE name='child'", [], |r| r.get::<_, String>(0))?, "CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY,p TEXT REFERENCES parent(id) ON DELETE CASCADE)");
            assert_eq!(sql.query_row("SELECT count(*) FROM sqlite_schema WHERE name='rebuilt'", [], |r| r.get::<_, i64>(0))?, 0);
            Ok(())
        }).await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn set_default_between_local_tables_remains_sqlites_action() {
    const SCHEMA: &str = "CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE parent(id TEXT PRIMARY KEY); CREATE TABLE child(p TEXT NOT NULL DEFAULT 'fallback' REFERENCES parent(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT); CREATE TABLE null_default(p TEXT NOT NULL REFERENCES parent(id) ON DELETE SET DEFAULT)";
    let store = TestStore::new();
    let db = store.schema(vec![shared("synced")], SCHEMA).await.unwrap();
    crate::write::tests::sql(&db, "INSERT INTO parent VALUES('p'),('fallback'); INSERT INTO child VALUES('p'); UPDATE parent SET id='q' WHERE id='p'").await.unwrap();
    db.read(|sql| {
        assert_eq!(
            sql.query_row("SELECT p FROM child", [], |r| r.get::<_, String>(0))?,
            "fallback"
        );
        Ok(())
    })
    .await
    .unwrap();
    crate::write::tests::sql(
        &db,
        "UPDATE child SET p='q'; DELETE FROM parent WHERE id='q'",
    )
    .await
    .unwrap();
    db.read(|sql| {
        assert_eq!(
            sql.query_row("SELECT p FROM child", [], |r| r.get::<_, String>(0))?,
            "fallback"
        );
        Ok(())
    })
    .await
    .unwrap();
    db.close().await.unwrap();
    store
        .schema(vec![shared("synced")], SCHEMA)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    store
        .builder(
            vec![shared("synced")],
            vec![Migration::sql(1, "schema", SCHEMA)],
        )
        .open_read_only()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn primary_key_rules_have_distinct_errors() {
    rejects(
        "CREATE TABLE notes (title TEXT)",
        notes_tables(),
        SchemaError::NoPrimaryKey {
            table: "notes".into(),
        },
    )
    .await;
    for schema in [
        "CREATE TABLE notes (id INTEGER PRIMARY KEY)",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT)",
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY DEFAULT 'generated')",
    ] {
        rejects(
            schema,
            notes_tables(),
            SchemaError::GeneratedPrimaryKey {
                table: "notes".into(),
            },
        )
        .await;
    }
    rejects(
        "CREATE TABLE notes (id BLOB NOT NULL PRIMARY KEY)",
        notes_tables(),
        SchemaError::IndependentKeyNotUuid {
            table: "notes".into(),
        },
    )
    .await;
    rejects(
        "CREATE TABLE notes (id TEXT NOT NULL, other TEXT NOT NULL, PRIMARY KEY(other,id))",
        vec![independent("notes").key_columns(["id", "other"])],
        SchemaError::KeyColumns {
            table: "notes".into(),
        },
    )
    .await;
}

#[tokio::test]
async fn opening_does_not_validate_existing_uuid_values() {
    let store = TestStore::new();
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)";
    let db = store.schema(notes_tables(), schema).await.unwrap();
    db.inspect_writer(|writer| {
        writer
            .batch("INSERT INTO notes VALUES('not a uuid')")
            .unwrap();
    });
    db.close().await.unwrap();
    store
        .schema(notes_tables(), schema)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    store
        .builder(notes_tables(), vec![Migration::sql(1, "notes", schema)])
        .open_read_only()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn opening_ten_thousand_independent_rows_does_not_scan_app_tables() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.inspect_writer(|writer| {
        writer.batch("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000) INSERT INTO notes(id,title) SELECT printf('00000000-0000-4000-8000-%012x',i),'title' FROM n").unwrap();
    });
    db.close().await.unwrap();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let statements = db.inspect_writer(|writer| writer.fullscan_statements());
    assert!(!statements.is_empty(), "the open must be traced");
    for (statement, steps) in statements {
        // Metadata enumeration may scan sqlite_schema and pragma virtual tables.
        if steps > 0 {
            assert!(
                statement.contains("sqlite_schema") || statement.contains("pragma_"),
                "app table scan ({steps} steps): {statement}"
            );
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn key_foreign_keys_cannot_replace_values_on_delete_or_update() {
    for schema in [
        "CREATE TABLE p (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY REFERENCES p(id) ON DELETE SET NULL)",
        "CREATE TABLE p (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY REFERENCES p(id) ON UPDATE SET NULL)",
    ] { rejects(schema, vec![shared("p"), shared("c")], SchemaError::PrimaryKeyAction { table: "c".into(), column: "id".into() }).await; }
}

#[tokio::test]
async fn root_audience_requires_a_nonnull_text_column() {
    for schema in [
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY)",
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, audience TEXT)",
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, audience INTEGER NOT NULL)",
    ] {
        rejects(
            schema,
            vec![independent("notes").audience_column("audience")],
            SchemaError::AudienceColumn {
                table: "notes".into(),
                column: "audience".into(),
            },
        )
        .await;
    }
}

#[tokio::test]
async fn descendant_audience_requires_one_foreign_key_column_into_synced_data() {
    rejects("CREATE TABLE p (id TEXT NOT NULL, x TEXT NOT NULL, PRIMARY KEY(id,x)); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, p_id TEXT, p_x TEXT, FOREIGN KEY(p_id,p_x) REFERENCES p(id,x))", vec![shared("p").key_columns(["id","x"]), independent("c").audience_from("p_id")], SchemaError::AudienceForeignKeyColumns { table: "c".into() }).await;
    rejects("CREATE TABLE p (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, p_id TEXT REFERENCES p(id))", vec![independent("c").audience_from("p_id")], SchemaError::AudienceForeignKeyTarget { table: "c".into(), column: "p_id".into() }).await;
    rejects(
        "CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, p_id TEXT)",
        vec![independent("c").audience_from("p_id")],
        SchemaError::AudienceForeignKeyTarget {
            table: "c".into(),
            column: "p_id".into(),
        },
    )
    .await;
    for schema in [
        "CREATE TABLE p (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, p_id TEXT REFERENCES p(id) ON DELETE SET NULL)",
        "CREATE TABLE p (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, p_id TEXT REFERENCES p(id) ON UPDATE SET NULL)",
    ] { rejects(schema, vec![shared("p"), independent("c").audience_from("p_id")], SchemaError::AudienceForeignKeyAction { table: "c".into(), column: "p_id".into() }).await; }
}

#[tokio::test]
async fn audience_cycles_include_self_references_and_longer_cycles() {
    rejects(
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES notes(id))",
        vec![independent("notes").audience_from("parent")],
        SchemaError::AudienceCycle {
            table: "notes".into(),
        },
    )
    .await;
    rejects("CREATE TABLE a (id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES b(id)); CREATE TABLE b (id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES c(id)); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, parent TEXT REFERENCES a(id))", vec![independent("a").audience_from("parent"), independent("b").audience_from("parent"), independent("c").audience_from("parent")], SchemaError::AudienceCycle { table: "a".into() }).await;
}

#[tokio::test]
async fn uniqueness_and_shared_keys_are_scoped_to_the_audience() {
    rejects("CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, title TEXT); CREATE UNIQUE INDEX title_unique ON notes(title)", vec![independent("notes").audience_column("audience")], SchemaError::AudienceConstraint { table: "notes".into(), constraint: "title_unique".into() }).await;
    rejects(
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL)",
        vec![shared("notes").audience_column("audience")],
        SchemaError::AudienceConstraint {
            table: "notes".into(),
            constraint: "PRIMARY KEY".into(),
        },
    )
    .await;
    rejects("CREATE TABLE p (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, p_id TEXT REFERENCES p(id), label TEXT); CREATE UNIQUE INDEX label_unique ON c(label)", vec![shared("p"), independent("c").audience_from("p_id")], SchemaError::AudienceConstraint { table: "c".into(), constraint: "label_unique".into() }).await;
    rejects("CREATE TABLE p (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (id TEXT NOT NULL PRIMARY KEY, p_id TEXT REFERENCES p(id))", vec![shared("p"), shared("c").audience_from("p_id")], SchemaError::AudienceConstraint { table: "c".into(), constraint: "PRIMARY KEY".into() }).await;
    let store = TestStore::new();
    let db = store.schema(vec![shared("p").key_columns(["audience","id"]).audience_column("audience"), shared("s"), shared("c").key_columns(["s_id","id"]).audience_from("s_id")], "CREATE TABLE p (audience TEXT NOT NULL, id TEXT NOT NULL, title TEXT, PRIMARY KEY(audience,id), UNIQUE(audience,title)); CREATE TABLE s (id TEXT NOT NULL PRIMARY KEY); CREATE TABLE c (s_id TEXT NOT NULL REFERENCES s(id), id TEXT NOT NULL, label TEXT, PRIMARY KEY(s_id,id)); CREATE UNIQUE INDEX scoped_label ON c(s_id,lower(label))").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn unguarded_shared_triggers_are_schema_errors() {
    for schema in [
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT); CREATE TRIGGER edited AFTER UPDATE ON notes BEGIN SELECT 1; END",
        "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT); CREATE TRIGGER edited AFTER UPDATE ON notes WHEN NOT coven_applying() OR 1 BEGIN SELECT 1; END",
    ] { rejects(schema, vec![independent("notes").shared_trigger("edited")], SchemaError::SharedTriggerGuard { table: "notes".into(), trigger: "edited".into() }).await; }
}

#[tokio::test]
async fn file_declarations_check_every_named_column_and_accept_custom_names() {
    let file = || FileDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy);
    let store = TestStore::new();
    let error = store
        .schema(
            vec![shared("files").carries_files(file())],
            "CREATE TABLE files (id TEXT NOT NULL PRIMARY KEY, size INTEGER, hash BLOB)",
        )
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumn { table, column }) if table == "files" && column == "location")
    );
    let db = store.schema(vec![shared("files").carries_files(file().with_id_column("file_id").with_size_column("bytes").with_hash_column("sha").with_location_column("place").write_once())], "CREATE TABLE files (id TEXT NOT NULL PRIMARY KEY, file_id TEXT, bytes INTEGER, sha BLOB, place TEXT)").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn opening_an_existing_schema_rechecks_declarations() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.close().await.unwrap();
    let error = store
        .builder(
            vec![independent("notes").key_columns(["title"])],
            notes_migrations(),
        )
        .open()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::KeyColumns { table }) if table == "notes")
    );
}

#[tokio::test]
async fn declarations_have_distinct_errors() {
    rejects(
        "SELECT 1",
        vec![shared("notes")],
        SchemaError::MissingTable {
            table: "notes".into(),
        },
    )
    .await;
    rejects(
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)",
        vec![shared("notes"), shared("NOTES")],
        SchemaError::DuplicateTable {
            table: "NOTES".into(),
        },
    )
    .await;
    for declaration in [
        independent("notes")
            .audience_column("audience")
            .audience_from("parent"),
        independent("notes")
            .audience_from("parent")
            .audience_column("audience"),
        independent("notes")
            .audience_column("old")
            .audience_from("parent")
            .audience_column("audience"),
        independent("notes")
            .audience_from("old")
            .audience_column("audience")
            .audience_from("parent"),
    ] {
        rejects("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, audience TEXT NOT NULL, parent TEXT REFERENCES notes(id))", vec![declaration], SchemaError::TwoAudiences { table: "notes".into() }).await;
    }
    for schema in [
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)",
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE local(id TEXT); CREATE TRIGGER edited AFTER UPDATE ON local WHEN NOT coven_applying() BEGIN SELECT 1; END",
    ] {
        rejects(schema, vec![shared("notes").shared_trigger("edited")], SchemaError::MissingTrigger { table: "notes".into(), trigger: "edited".into() }).await;
    }
}

#[tokio::test]
async fn impossible_actions_are_refused_in_synced_and_local_tables() {
    for synced in [false, true] {
        for event in ["DELETE", "UPDATE"] {
            for default in ["", "DEFAULT 'valid'"] {
                let store = TestStore::new();
                let tables = if synced {
                    vec![shared("child")]
                } else {
                    vec![]
                };
                let schema = format!("CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE child(id TEXT NOT NULL PRIMARY KEY,parent TEXT NOT NULL {default} REFERENCES parent(id) ON {event} SET NULL)");
                let error = store
                    .builder(
                        tables,
                        vec![Migration::run(1, "invalid action", move |c| {
                            c.execute_batch(&schema)?;
                            Ok(())
                        })],
                    )
                    .open()
                    .await
                    .err()
                    .expect("impossible action must fail migration");
                assert!(
                    matches!(database_error(error), DbError::Schema(SchemaError::ImpossibleAction { table, column }) if table == "child" && column == "parent"),
                    "{synced}, {event}, {default}"
                );
            }
        }
    }
}

#[tokio::test]
async fn opening_an_existing_schema_refuses_an_impossible_local_action() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|db| db.batch("CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE child(parent TEXT NOT NULL REFERENCES parent(id) ON UPDATE SET NULL)").unwrap());
    db.close().await.unwrap();
    for read_only in [false, true] {
        let builder = store.builder(vec![], vec![]);
        let error = if read_only {
            builder.open_read_only().await.map(|_| ())
        } else {
            builder.open().await.map(|_| ())
        }
        .expect_err("invalid existing schema");
        assert!(
            matches!(database_error(error), DbError::Schema(SchemaError::ImpossibleAction { table, column }) if table == "child" && column == "parent")
        );
    }
}

#[tokio::test]
async fn nullable_set_null_actions_are_accepted() {
    for definition in [
        "parent TEXT REFERENCES parent(id) ON DELETE SET NULL",
        "parent TEXT REFERENCES parent(id) ON UPDATE SET NULL",
    ] {
        let store = TestStore::new();
        let schema = format!(
            "CREATE TABLE parent(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE child({definition})"
        );
        let db = store
            .builder(
                vec![],
                vec![Migration::run(1, "actions", move |c| {
                    c.execute_batch(&schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn nullable_primary_keys_are_refused_on_open_and_after_migrating() {
    for (schema, key, column) in [
        ("CREATE TABLE notes(id TEXT PRIMARY KEY)", vec!["id"], "id"),
        (
            "CREATE TABLE notes(id TEXT NOT NULL,seq INT,PRIMARY KEY(id,seq))",
            vec!["id", "seq"],
            "seq",
        ),
    ] {
        let store = TestStore::new();
        let tables = || vec![shared("notes").key_columns(key.clone())];
        let error = store
            .schema(tables(), schema)
            .await
            .err()
            .expect("nullable key must fail migration");
        assert!(
            matches!(database_error(error), DbError::Schema(SchemaError::NullableKey { table, column: found }) if table == "notes" && found == column)
        );
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        db.inspect_writer(|db| db.batch(schema).unwrap());
        db.close().await.unwrap();
        let error = store
            .builder(tables(), vec![])
            .open()
            .await
            .err()
            .expect("nullable key must fail open");
        assert!(
            matches!(database_error(error), DbError::Schema(SchemaError::NullableKey { table, column: found }) if table == "notes" && found == column)
        );
    }
}

#[tokio::test]
async fn independent_keys_require_text_affinity_on_open_and_after_migrating() {
    for kind in ["BLOB", "UUID", "INT", "CHARINT", "REAL", "NUMERIC", ""] {
        let store = TestStore::new();
        let schema = format!("CREATE TABLE notes(id {kind} NOT NULL PRIMARY KEY,title TEXT)");
        let migration_schema = schema.clone();
        let error = store
            .builder(
                notes_tables(),
                vec![Migration::run(1, "key type", move |sql| {
                    sql.execute_batch(&migration_schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .err()
            .expect("non-text key must fail migration");
        assert!(
            matches!(database_error(error), DbError::Schema(SchemaError::IndependentKeyNotUuid { table }) if table == "notes"),
            "{kind}"
        );
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        db.inspect_writer(|writer| {
            assert_eq!(writer.schema_version().unwrap(), 0);
            assert_eq!(
                writer
                    .query_row(
                        "SELECT count(*) FROM sqlite_schema WHERE name='notes'",
                        [],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                0
            );
            writer.batch(&schema).unwrap();
        });
        db.close().await.unwrap();
        for read_only in [false, true] {
            let builder = store.builder(notes_tables(), vec![]);
            let error = if read_only {
                builder.open_read_only().await.map(|_| ())
            } else {
                builder.open().await.map(|_| ())
            }
            .expect_err("non-text key must fail open");
            assert!(
                matches!(database_error(error), DbError::Schema(SchemaError::IndependentKeyNotUuid { table }) if table == "notes"),
                "{kind}"
            );
        }
    }
    for kind in ["TEXT", "VARCHAR(36)", "CLOB", "UUID TEXT"] {
        let store = TestStore::new();
        let schema =
            format!("CREATE TABLE notes(n INT NOT NULL,id {kind} NOT NULL,PRIMARY KEY(n,id))");
        let db = store
            .builder(
                vec![independent("notes").key_columns(["n", "id"])],
                vec![Migration::run(1, "key type", move |sql| {
                    sql.execute_batch(&schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .unwrap();
        db.close().await.unwrap();
    }
}
