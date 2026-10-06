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
            MigrationChange::Addition,
        ),
        (
            "CREATE TABLE extra(id TEXT PRIMARY KEY); CREATE TRIGGER extra_insert AFTER INSERT ON extra BEGIN SELECT 1; END",
            MigrationChange::Addition,
        ),
        (
            "CREATE TABLE extra(id TEXT PRIMARY KEY)",
            MigrationChange::Addition,
        ),
        (
            "CREATE VIEW titles AS SELECT title FROM notes",
            MigrationChange::Breaking,
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
            vec![],
            vec![
                Migration::sql(1, "initial", "CREATE TABLE local(a TEXT,b TEXT)"),
                Migration::sql(
                    2,
                    "reorder",
                    "DROP TABLE local; CREATE TABLE local(b TEXT,a TEXT)",
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
