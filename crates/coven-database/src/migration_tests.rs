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
                Migration::sql(
                    1,
                    "initial",
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)",
                ),
                Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN id TO note_id"),
                Migration::sql(3, "body", "ALTER TABLE notes ADD COLUMN body TEXT"),
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
        Migration::sql(2, "add", "ALTER TABLE notes ADD COLUMN color TEXT"),
        Migration::run(3, "backfill", |sql| {
            sql.execute(
                "INSERT INTO notes(id,title) VALUES (?1,?2)",
                ("f47ac10b-58cc-4372-a567-0e02b2c3d479", "before failure"),
            )?;
            sql.execute_batch("SELECT missing_column FROM notes")?;
            Ok(())
        }),
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
                Migration::sql(
                    1,
                    "initial",
                    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY)",
                ),
                Migration::sql(
                    2,
                    "remove key",
                    "DROP TABLE notes; CREATE TABLE notes(title TEXT)",
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
        .builder(vec![], vec![Migration::sql(2, "gap", "SELECT 1")])
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
    migrations.push(Migration::run(2, "must not run", |_| {
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
        migrations.push(Migration::sql(2, "change", sql));
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
            vec![Migration::run(1, "data", |sql| {
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
                Migration::sql(1, "initial", "CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY,a TEXT,b TEXT)"),
                Migration::sql(
                    2,
                    "reorder",
                    "DROP TABLE synced; CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY,b TEXT,a TEXT)",
                ),
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
            vec![Migration::sql(
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
                vec![Migration::run(1, "rollback", move |sql| {
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
            Migration::sql(1, "tables", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE TABLE local(id TEXT PRIMARY KEY,title TEXT,extra TEXT); CREATE INDEX local_titles ON local(title); CREATE TRIGGER edited AFTER UPDATE ON local BEGIN SELECT 1; END; CREATE VIEW existing AS SELECT title FROM notes"),
            Migration::sql(2, "change", change),
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
                Migration::sql(
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
                    vec![initial(), Migration::sql(2, "change", sql)],
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
            Migration::sql(1, "initial", "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT); CREATE INDEX existing_index ON notes(title); CREATE TRIGGER existing_trigger AFTER INSERT ON notes BEGIN SELECT 1; END; CREATE TABLE local(title TEXT)"),
            Migration::sql(2, "change", sql),
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
                        Migration::run(1, "initial", move |sql| {
                            sql.execute_batch(&initial)?;
                            Ok(())
                        }),
                        Migration::run(2, "change", move |sql| {
                            sql.execute_batch(&changed)?;
                            Ok(())
                        }),
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

mod queued {
    //! A local breaking migration leaves every queued original record as it
    //! was authored (§17.1); only the local state and its marker change.
    use crate::{
        tests::TestStore,
        write::tests::{notes, records, sql, NOTES},
        *,
    };
    use coven_format::{value::Value as WireValue, write::WriteDisposition};
    use coven_merge::{Operation, RowId, RowState};
    use std::sync::Arc;

    fn queued_bytes(db: &Database) -> Vec<Vec<u8>> {
        db.inspect_writer(|db| {
            db.query(
                "SELECT record FROM _coven_uploads ORDER BY number",
                [],
                |r| r.get(0),
            )
            .unwrap()
        })
    }
    fn initial() -> Migration {
        Migration::sql(1, "notes", NOTES)
    }
    fn state(db: &Database, row: &RowId) -> RowState<WireValue> {
        db.inspect_writer_schema(|db, schema| {
            let app = crate::write_rows::AppView::after(db, schema);
            crate::merge_store::MergeStore::new(db, &app)
                .row(row)
                .unwrap()
                .state
        })
    }

    #[tokio::test]
    async fn neither_additions_nor_breaking_changes_touch_queued_records_or_call_converters() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')")
            .await
            .unwrap();
        // A chunked record keeps its bytes as one write.
        sql(&db,"WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1200) INSERT INTO notes SELECT printf('%04d',i),'title',zeroblob(256) FROM n").await.unwrap();
        let original = queued_bytes(&db);
        assert!(original[1].len() > 4 * coven_format::chunks::CHUNK_SIZE);
        db.close().await.unwrap();
        let migrations = || {
            vec![
                initial(),
                Migration::sql(2, "color", "ALTER TABLE notes ADD COLUMN color TEXT")
                    .writes(|_| panic!("a local migration never converts the queue")),
                Migration::sql(3, "rename", "ALTER TABLE notes RENAME COLUMN title TO name")
                    .writes(|_| panic!("a local migration never converts the queue")),
            ]
        };
        let db = store.builder(notes(), migrations()).open().await.unwrap();
        assert_eq!(queued_bytes(&db)[..2], original[..]);
        assert_eq!(
            records(&db)[2].header.disposition,
            WriteDisposition::Migration
        );
        db.close().await.unwrap();
        let db = store.builder(notes(), migrations()).open().await.unwrap();
        assert_eq!(queued_bytes(&db)[..2], original[..]);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn an_attempted_and_an_untried_record_keep_their_local_setters() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')")
            .await
            .unwrap();
        crate::upload::tests::attempt(&db, 255).await.unwrap();
        sql(&db, "UPDATE notes SET title='two'").await.unwrap();
        let original = queued_bytes(&db);
        let writes = records(&db);
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name"),
                ],
            )
            .open()
            .await
            .unwrap();
        assert_eq!(queued_bytes(&db)[..2], original[..]);
        let current = state(&db, &writes[1].parts[0].rows[0].row);
        assert_eq!(current.cells()["name"].write, writes[1].header.position);
        assert_eq!(current.cells()["body"].write, writes[0].header.position);
        assert!(!current.cells().contains_key("title"));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn another_applied_winner_keeps_its_cell_through_the_migration() {
        let ids = coven_foundation::id_source::SequentialIds::new();
        let store = TestStore::with_ids(&ids);
        let peer_store = TestStore::with_ids(&ids);
        let db = store.schema(notes(), NOTES).await.unwrap();
        let peer = peer_store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')")
            .await
            .unwrap();
        db.receive(records(&db)[0].clone().into()).await.unwrap();
        peer.receive(records(&db).remove(0).into()).await.unwrap();
        sql(&peer, "UPDATE notes SET title='other device'")
            .await
            .unwrap();
        let record = records(&peer).remove(0);
        let remote = record.header.position.device;
        db.receive(record.into()).await.unwrap();
        peer.close().await.unwrap();
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name"),
                ],
            )
            .open()
            .await
            .unwrap();
        let row = &records(&db)[0].parts[0].rows[0].row;
        let current = state(&db, row);
        assert_eq!(current.cells()["name"].write.device, remote);
        assert_eq!(
            current.cells()["name"].value.value,
            WireValue::Text("other device".into())
        );
        assert!(current.lost().is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn removed_and_added_columns_take_new_setters_without_reusing_positions() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')")
            .await
            .unwrap();
        let original = queued_bytes(&db);
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::sql(
                        2,
                        "replace body",
                        "ALTER TABLE notes DROP COLUMN body; ALTER TABLE notes ADD COLUMN extra TEXT",
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        assert_eq!(queued_bytes(&db)[..1], original[..]);
        let queue = records(&db);
        let current = state(&db, &queue[0].parts[0].rows[0].row);
        assert!(!current.cells().contains_key("body"));
        assert_eq!(current.cells()["title"].write, queue[0].header.position);
        assert_eq!(current.cells()["extra"].write, queue[1].header.position);
        assert_eq!(current.cells()["extra"].value.value, WireValue::Null);
        sql(&db, "UPDATE notes SET extra='edited'").await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_column_used_only_by_attempted_writes_can_be_dropped() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('42','one','body')")
            .await
            .unwrap();
        crate::upload::tests::attempt(&db, 1).await.unwrap();
        sql(&db, "UPDATE notes SET title='two'").await.unwrap();
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::sql(2, "drop body", "ALTER TABLE notes DROP COLUMN body"),
                ],
            )
            .open()
            .await
            .unwrap();
        let actual = records(&db);
        let current = state(&db, &actual[1].parts[0].rows[0].row);
        assert!(!current.cells().contains_key("body"));
        assert_eq!(current.cells()["title"].write, actual[1].header.position);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn additions_around_the_breaking_change_keep_their_distinct_setters() {
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        crate::raise_capture::tests::connect(&db).await;
        sql(&db, "INSERT INTO notes VALUES('42','one','body')")
            .await
            .unwrap();
        db.close().await.unwrap();
        let db = store
            .builder(
                notes(),
                vec![
                    initial(),
                    Migration::sql(
                        2,
                        "color",
                        "ALTER TABLE notes ADD COLUMN color TEXT DEFAULT 'blue'",
                    ),
                    Migration::sql(3,"rename and index","ALTER TABLE notes RENAME COLUMN title TO name; CREATE UNIQUE INDEX colors ON notes(color)"),
                    Migration::sql(
                        4,
                        "length",
                        "ALTER TABLE notes ADD COLUMN length INTEGER DEFAULT 7",
                    ),
                ],
            )
            .open()
            .await
            .unwrap();
        let actual = records(&db);
        // The original record keeps its schema; the batch raises once, to 4.
        assert_eq!(actual[0].header.schema_version, 1);
        assert_eq!(
            db.raise_captures()
                .await
                .unwrap()
                .first()
                .map_or(0, |c| c.version),
            4
        );
        let current = state(&db, &actual[0].parts[0].rows[0].row);
        assert!(!current.cells().contains_key("color"));
        assert_eq!(current.cells()["length"].write, actual[1].header.position);
        assert_eq!(
            db.read(
                |sql| Ok(sql.query_row("SELECT color,length FROM notes", [], |r| Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?
                )))?)
            )
            .await
            .unwrap(),
            ("blue".into(), 7)
        );
        sql(&db, "UPDATE notes SET name='two'").await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn publication_intent_commits_with_the_schema_queue_and_migration_write() {
        let migrations = || {
            vec![
                Migration::sql(1, "notes", NOTES),
                Migration::sql(2, "rename", "ALTER TABLE notes RENAME COLUMN title TO name"),
            ]
        };
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        crate::raise_capture::tests::connect(&db).await;
        sql(&db, "INSERT INTO notes VALUES('42','title','body')")
            .await
            .unwrap();
        let original = records(&db);
        db.close().await.unwrap();
        assert!(store
            .builder(notes(), migrations())
            .migration_operation(|_| Err(DbError::ClockOutOfRange))
            .open()
            .await
            .is_err());
        let db = store.schema(notes(), NOTES).await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(), 1);
        assert_eq!(records(&db), original);
        assert!(db.operations().await.unwrap().is_empty());
        assert!(db.raise_captures().await.unwrap().is_empty());
        assert_eq!(
            db.raise_captures()
                .await
                .unwrap()
                .first()
                .map_or(0, |c| c.version),
            0
        );
        db.close().await.unwrap();
        let db = store
            .builder(notes(), migrations())
            .migration_operation(|version| {
                Ok(NewOperation {
                    kind: "test-publication".into(),
                    data: version.to_be_bytes().to_vec(),
                    started_by: "coven".into(),
                    pauses_writes: false,
                })
            })
            .open()
            .await
            .unwrap();
        let pending = db.operations().await.unwrap().remove(0);
        assert_eq!(pending.last_step, 1);
        assert_eq!(pending.data, 2u32.to_be_bytes());
        assert_eq!(db.schema_version().await.unwrap(), 2);
        assert_eq!(
            records(&db).last().unwrap().header.disposition,
            WriteDisposition::Migration
        );
        assert_eq!(db.raise_captures().await.unwrap().len(), 1);
        db.close().await.unwrap();
        let db = store
            .builder(notes(), migrations())
            .migration_operation(|_| panic!("already committed"))
            .open()
            .await
            .unwrap();
        assert_eq!(db.operations().await.unwrap()[0].id, pending.id);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_late_new_schema_title_competes_with_the_title_setter_and_leaves_the_slug() {
        use coven_format::write::{WriteHeader, WritePart, WriteRecord};
        use coven_foundation::{clock::FixedClock, id_source::DeviceId};
        use coven_merge::{Audience, Change, ColumnValue, Timestamp, WriteId};
        use std::{
            collections::BTreeMap,
            time::{Duration, UNIX_EPOCH},
        };
        let store = TestStore::new();
        let db = store.schema(notes(), NOTES).await.unwrap();
        sql(&db, "INSERT INTO notes VALUES('n','title','body')")
            .await
            .unwrap();
        sql(&db, "UPDATE notes SET title='new title'")
            .await
            .unwrap();
        let original = records(&db);
        let old_stamp = original[1].header.timestamp.milliseconds();
        db.close().await.unwrap();
        let db = store.builder(notes(),vec![initial(),Migration::sql(2,"slug","ALTER TABLE notes RENAME COLUMN title TO name; ALTER TABLE notes ADD COLUMN slug TEXT; UPDATE notes SET slug=replace(name,' ','-')").writes(|r| { r.rename_column("title","name"); Ok(()) })]).clock(Arc::new(FixedClock::new(UNIX_EPOCH+Duration::from_millis(old_stamp+1000)))).open().await.unwrap();
        let late = WriteId {
            device: DeviceId(999),
            number: 1,
        };
        let row = original[0].parts[0].rows[0].row.clone();
        let record = WriteRecord {
            header: WriteHeader {
                position: late,
                timestamp: Timestamp::new(old_stamp + 1, 0, late.device).unwrap(),
                read_through: 1,
                schema_version: 2,
                disposition: WriteDisposition::Apply,
            },
            parts: vec![WritePart {
                executions: Vec::new(),
                dismissals: Vec::new(),
                audience: Audience::Store,
                rows: vec![coven_format::write::RowChange {
                    creator: None,
                    execution: Default::default(),
                    row: row.clone(),
                    change: Change {
                        generation: 1,
                        operation: Operation::Update(
                            [(
                                "name".into(),
                                ColumnValue {
                                    value: WireValue::Text("late title".into()),
                                    parents: BTreeMap::new(),
                                },
                            )]
                            .into(),
                        ),
                    },
                    old: [("name".into(), WireValue::Text("title".into()))].into(),
                    sources: coven_format::write_sources::WriteSources {
                        generation: Some(coven_format::write_sources::ChangeId {
                            write: original[0].header.position,
                            row: row.clone(),
                        }),
                        setters: [(
                            "name".into(),
                            coven_format::write_sources::CellSource {
                                change: coven_format::write_sources::ChangeId {
                                    write: original[0].header.position,
                                    row: row.clone(),
                                },
                                column: "title".into(),
                            },
                        )]
                        .into(),
                    },
                }],
            }],
        };
        for write in records(&db) {
            db.receive(write.into()).await.unwrap();
        }
        assert_eq!(
            db.receive(record.into()).await.unwrap(),
            ApplyOutcome::Applied
        );
        let current = state(&db, &row);
        assert_eq!(current.cells()["name"].write, late);
        assert_eq!(
            current.cells()["name"].value.value,
            WireValue::Text("late title".into())
        );
        assert_eq!(
            current.cells()["slug"].value.value,
            WireValue::Text("new-title".into())
        );
        assert_eq!(
            current.cells()["slug"].write,
            records(&db)[2].header.position
        );
        db.close().await.unwrap();
    }
}
