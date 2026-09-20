use super::*;
use rusqlite::session::{ConflictAction, Session};

#[test]
fn historical_insert_survives_a_removed_column_and_a_required_derived_column() {
    let migrations = vec![
        Migration::sql(1, "records", "CREATE TABLE records (id TEXT PRIMARY KEY, value TEXT, origin TEXT, _updated_at TEXT NOT NULL)"),
        Migration::sql(2, "remove_origin", "ALTER TABLE records DROP COLUMN origin")
            .changesets(vec![TableChangesetMigration::new("records", &[], |row, _| {
                row.columns.retain(|column| column.name != "origin");
                Ok(())
            })]),
        Migration::sql(3, "record_kind", "ALTER TABLE records ADD COLUMN kind TEXT NOT NULL DEFAULT 'record'")
            .changesets(vec![TableChangesetMigration::new("records", &[], |row, _| {
                row.columns.push(ChangesetColumn {
                    name: "kind".into(),
                    old: (row.operation == ChangeOp::Delete).then(|| Value::Text("record".into())),
                    new: (row.operation == ChangeOp::Insert).then(|| Value::Text("record".into())),
                    primary_key: false,
                });
                Ok(())
            })]),
    ];
    let author = Connection::open_in_memory().unwrap();
    migrations[0].up.apply(&author).unwrap();
    let mut capture = Session::new(&author).unwrap();
    capture.attach(Some("records")).unwrap();
    author
        .execute(
            "INSERT INTO records VALUES ('id', 'title', 'typed', 'clock')",
            [],
        )
        .unwrap();
    let mut original = Vec::new();
    capture.changeset_strm(&mut original).unwrap();
    let receiver = Connection::open_in_memory().unwrap();
    for migration in &migrations {
        migration.up.apply(&receiver).unwrap();
    }
    let converted = migrate_changeset(&receiver, &migrations, 1, &original).unwrap();
    receiver
        .apply_strm(&mut &converted[..], None::<fn(&str) -> bool>, |_, _| {
            ConflictAction::SQLITE_CHANGESET_ABORT
        })
        .unwrap();
    let actual: (String, String, String) = receiver
        .query_row("SELECT value, _updated_at, kind FROM records", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(actual, ("title".into(), "clock".into(), "record".into()));
}

fn identity_migrations() -> Vec<Migration> {
    vec![
        Migration::sql(1, "records", "CREATE TABLE records (id TEXT PRIMARY KEY, catalog TEXT NOT NULL, external_key TEXT NOT NULL, parent TEXT, value BLOB, _updated_at TEXT NOT NULL)"),
        Migration::sql(2, "known_parent", "ALTER TABLE records RENAME COLUMN parent TO album")
            .changesets(vec![TableChangesetMigration::new("records", &["catalog", "external_key"], |row, identity| {
                let external_key = identity.get("external_key").expect("declared identity");
                let parent = row.columns.iter_mut().find(|column| column.name == "parent").expect("registered column");
                parent.name = "album".into();
                for value in [&mut parent.old, &mut parent.new] {
                    if value.as_ref() == Some(external_key) { *value = Some(Value::Null); }
                }
                Ok(())
            })]),
    ]
}

fn capture(connection: &Connection, sql: &str) -> Vec<u8> {
    capture_indirect(connection, sql, false)
}

fn capture_indirect(connection: &Connection, sql: &str, indirect: bool) -> Vec<u8> {
    let mut session = Session::new(connection).expect("capture session");
    session.set_indirect(indirect);
    session.attach(Some("records")).expect("attach records");
    connection.execute_batch(sql).expect("author change");
    let mut bytes = Vec::new();
    session.changeset_strm(&mut bytes).expect("capture bytes");
    bytes
}

#[test]
fn sparse_update_uses_immutable_identity_and_preserves_null_blob_and_indirect_cells() {
    let migrations = identity_migrations();
    let author = Connection::open_in_memory().unwrap();
    migrations[0].up.apply(&author).unwrap();
    author.execute_batch("INSERT INTO records VALUES ('id', 'catalog', 'key', 'known-parent', X'0001FF', 'before')").unwrap();
    let original = capture_indirect(
        &author,
        "UPDATE records SET parent = 'key', value = NULL, _updated_at = 'after' WHERE id = 'id'",
        true,
    );
    let receiver = Connection::open_in_memory().unwrap();
    for migration in &migrations {
        migration.up.apply(&receiver).unwrap();
    }
    receiver.execute_batch("INSERT INTO records VALUES ('id', 'catalog', 'key', 'known-parent', X'0001FF', 'before')").unwrap();
    let history = ApplicationSchemaHistory::new(Arc::from(migrations)).unwrap();
    history.validate_source(&receiver, 1, &original).unwrap();
    let converted = history.migrate(&receiver, 1, &original).unwrap();
    let rows = history.decode(&receiver, 2, &converted).unwrap();
    assert!(rows[0].indirect);
    assert_eq!(rows[0].columns[1].new, None);
    assert_eq!(rows[0].columns[3].new, Some(Value::Null));
    assert_eq!(rows[0].columns[4].old, Some(Value::Blob(vec![0, 1, 255])));
    assert_eq!(rows[0].columns[4].new, Some(Value::Null));
    receiver
        .apply_strm(&mut &converted[..], None::<fn(&str) -> bool>, |_, _| {
            ConflictAction::SQLITE_CHANGESET_ABORT
        })
        .unwrap();
    let values: (Option<String>, Option<Vec<u8>>, String) = receiver
        .query_row("SELECT album, value, _updated_at FROM records", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(values, (None, None, "after".into()));
    assert_eq!(
        history.decode(&receiver, 1, &original).unwrap()[0].columns[3].new,
        Some(Value::Text("key".into()))
    );
}

#[test]
fn missing_sparse_update_keeps_delete_wins_and_identity_mutation_is_refused() {
    let migrations = identity_migrations();
    let author = Connection::open_in_memory().unwrap();
    migrations[0].up.apply(&author).unwrap();
    author
        .execute_batch("INSERT INTO records VALUES ('id', 'catalog', 'key', NULL, NULL, 'before')")
        .unwrap();
    let missing = capture(
        &author,
        "UPDATE records SET parent = 'key', _updated_at = 'after' WHERE id = 'id'",
    );
    let changed_identity = capture(
        &author,
        "UPDATE records SET external_key = 'other', _updated_at = 'last' WHERE id = 'id'",
    );
    let receiver = Connection::open_in_memory().unwrap();
    for migration in &migrations {
        migration.up.apply(&receiver).unwrap();
    }
    let history = ApplicationSchemaHistory::new(Arc::from(migrations)).unwrap();
    assert!(history.migrate(&receiver, 1, &missing).unwrap().is_empty());
    assert!(matches!(
        history.migrate(&receiver, 1, &changed_identity),
        Err(DbError::ChangesetMigration(
            ChangesetMigrationError::MutableIdentity { .. }
        ))
    ));
}

#[test]
fn equal_width_renames_cannot_recover_an_unversioned_write_by_column_count() {
    let migrations = identity_migrations();
    let connection = Connection::open_in_memory().unwrap();
    migrations[0].up.apply(&connection).unwrap();
    let captured = capture(
        &connection,
        "INSERT INTO records VALUES ('id', 'catalog', 'key', 'key', NULL, 'stamp')",
    );
    migrations[1].up.apply(&connection).unwrap();
    connection.pragma_update(None, "user_version", 2).unwrap();
    let history = ApplicationSchemaHistory::new(Arc::from(migrations)).unwrap();
    assert!(matches!(
        history.recover_version(&connection, &[&captured], None),
        Err(DbError::ChangesetMigration(
            ChangesetMigrationError::AmbiguousVersion { .. }
        ))
    ));
    assert_eq!(
        history
            .recover_version(&connection, &[&captured], Some(1))
            .unwrap(),
        1
    );
}

#[test]
fn versions_with_identical_row_meanings_allow_recovery_without_guessing_a_rename() {
    let migrations = vec![
        Migration::sql(
            1,
            "records",
            "CREATE TABLE records (id TEXT PRIMARY KEY, value TEXT)",
        ),
        Migration::sql(2, "index", "CREATE INDEX record_value ON records(value)"),
    ];
    let connection = Connection::open_in_memory().unwrap();
    for migration in &migrations {
        migration.up.apply(&connection).unwrap();
    }
    connection.pragma_update(None, "user_version", 2).unwrap();
    let captured = capture(&connection, "INSERT INTO records VALUES ('id', 'value')");
    let history = ApplicationSchemaHistory::new(Arc::from(migrations)).unwrap();
    assert_eq!(
        history
            .recover_version(&connection, &[&captured], None)
            .unwrap(),
        1
    );
}

#[test]
fn malformed_source_bytes_are_classified_as_changeset_input_errors() {
    let connection = Connection::open_in_memory().unwrap();
    let history = ApplicationSchemaHistory::new(Arc::from(identity_migrations())).unwrap();
    let error = history
        .validate_source(&connection, 1, b"not a SQLite session changeset")
        .unwrap_err();
    assert!(matches!(error, DbError::Changeset(_)), "{error}");
}

#[test]
fn host_transformations_cannot_change_authenticated_row_identity_or_ordering() {
    for mutation in ["primary key", "row clock", "operation", "indirect"] {
        let migrations = vec![
            Migration::sql(
                1,
                "records",
                "CREATE TABLE records (id TEXT PRIMARY KEY, value TEXT, _updated_at TEXT NOT NULL)",
            ),
            Migration::sql(2, "index", "CREATE INDEX record_value ON records(value)").changesets(
                vec![TableChangesetMigration::new(
                    "records",
                    &[],
                    move |row, _| {
                        match mutation {
                            "primary key" => row.columns[0].new = Some(Value::Text("other".into())),
                            "row clock" => row.columns[2].new = Some(Value::Text("later".into())),
                            "operation" => row.operation = ChangeOp::Delete,
                            "indirect" => row.indirect = true,
                            _ => unreachable!(),
                        }
                        Ok(())
                    },
                )],
            ),
        ];
        let author = Connection::open_in_memory().unwrap();
        migrations[0].up.apply(&author).unwrap();
        let bytes = capture(
            &author,
            "INSERT INTO records VALUES ('id', 'value', 'clock')",
        );
        let receiver = Connection::open_in_memory().unwrap();
        for migration in &migrations {
            migration.up.apply(&receiver).unwrap();
        }
        let error = migrate_changeset(&receiver, &migrations, 1, &bytes)
            .expect_err("a host adapter cannot rewrite authenticated identity or ordering");
        match mutation {
            "primary key" => assert!(matches!(
                error,
                DbError::ChangesetMigration(ChangesetMigrationError::MutableIdentity { .. })
            )),
            _ => assert!(
                error.to_string().contains("altered operation"),
                "{mutation}: {error}"
            ),
        }
        let count: i64 = receiver
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0, "{mutation}");
    }
}
