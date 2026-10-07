use super::*;
use crate::tests::{database_error, TestStore};
use crate::{DbError, Migration, SchemaError};

const SCHEMA: &str =
    "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location BLOB)";

fn declaration() -> FileDecl {
    FileDecl::new(
        "attachments",
        Provenance::AppProvided,
        CacheFill::CacheEager,
    )
}

fn table(file: FileDecl) -> Vec<SyncedTable> {
    vec![SyncedTable::new("attachments", RowIdentity::SharedKey).carries_files(file)]
}

fn renamed(role: usize, name: &str) -> FileDecl {
    match role {
        0 => declaration().with_id_column(name),
        1 => declaration().with_size_column(name),
        2 => declaration().with_hash_column(name),
        3 => declaration().with_location_column(name),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn default_and_custom_file_columns_are_checked_on_the_synced_table() {
    for (file, schema) in [
        (declaration(), SCHEMA),
        (
            FileDecl::new(
                "originals",
                Provenance::UserProvided,
                CacheFill::CacheLazy,
            )
            .with_id_column("FILE")
            .with_size_column("BYTES")
            .with_hash_column("DIGEST")
            .with_location_column("WHERE_AT")
            .write_once(),
            "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT,bytes INTEGER,digest BLOB,where_at BLOB)",
        ),
    ] {
        let store = TestStore::new();
        let db = store.schema(table(file.clone()), schema).await.unwrap();
        db.close().await.unwrap();
        for read_only in [false, true] {
            let builder = store.builder(table(file.clone()), vec![Migration::sql(1, "files", schema)]);
            if read_only {
                builder.open_read_only().await.unwrap().close().await.unwrap();
            } else {
                builder.open().await.unwrap().close().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn every_missing_file_column_names_its_table_on_open_and_after_migration() {
    for role in 0..4 {
        let store = TestStore::new();
        let error = store
            .schema(table(renamed(role, "missing")), SCHEMA)
            .await
            .err()
            .unwrap();
        assert!(
            matches!(database_error(error), DbError::Schema(SchemaError::FileColumn { table, column }) if table == "attachments" && column == "missing")
        );
        let db = store.schema(table(declaration()), SCHEMA).await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(), 1);
        db.close().await.unwrap();
        for read_only in [false, true] {
            let builder = store.builder(
                table(renamed(role, "missing")),
                vec![Migration::sql(1, "files", SCHEMA)],
            );
            let result = if read_only {
                builder.open_read_only().await.map(|_| ())
            } else {
                builder.open().await.map(|_| ())
            };
            assert!(
                matches!(database_error(result.unwrap_err()), DbError::Schema(SchemaError::FileColumn { table, column }) if table == "attachments" && column == "missing")
            );
        }
    }
}

#[tokio::test]
async fn final_file_columns_validate_after_all_migrations_and_failure_rolls_them_back() {
    let store = TestStore::new();
    store
        .schema(table(declaration()), SCHEMA)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let migrations = || {
        vec![
            Migration::sql(1, "files", SCHEMA),
            Migration::sql(
                2,
                "rename size",
                "ALTER TABLE attachments RENAME COLUMN size TO bytes",
            ),
            Migration::sql(
                3,
                "rename hash",
                "ALTER TABLE attachments RENAME COLUMN hash TO digest",
            ),
        ]
    };
    let error = store
        .builder(table(declaration()), migrations())
        .open()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumn { table, column }) if table == "attachments" && column == "size")
    );
    let db = store.schema(table(declaration()), SCHEMA).await.unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    db.close().await.unwrap();
    let db = store
        .builder(
            table(
                declaration()
                    .with_size_column("bytes")
                    .with_hash_column("digest"),
            ),
            migrations(),
        )
        .open()
        .await
        .unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 3);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_view_or_another_tables_columns_cannot_satisfy_a_file_declaration() {
    for schema in [
        "CREATE TABLE local(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location BLOB)",
        "CREATE TABLE local(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location BLOB); CREATE VIEW attachments AS SELECT * FROM local",
    ] {
        let store = TestStore::new();
        let error = store.schema(table(declaration()), schema).await.err().unwrap();
        assert!(matches!(database_error(error), DbError::Schema(SchemaError::MissingTable { table }) if table == "attachments"));
    }
}

#[tokio::test]
async fn hash_and_location_must_allow_null_on_both_opens_and_after_migrating() {
    for (schema, column) in [
        ("CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB NOT NULL,location TEXT)", "hash"),
        ("CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT NOT NULL)", "location"),
    ] {
        let store = TestStore::new();
        // The table is valid ordinary SQL, but not a valid file declaration.
        let ordinary = || vec![SyncedTable::new("attachments", RowIdentity::SharedKey)];
        store.schema(ordinary(), schema).await.unwrap().close().await.unwrap();
        for read_only in [false, true] {
            let builder = store.builder(table(declaration()), vec![Migration::sql(1, "files", schema)]);
            let result = if read_only {builder.open_read_only().await.map(|_| ())} else {builder.open().await.map(|_| ())};
            assert!(matches!(database_error(result.unwrap_err()), DbError::Schema(SchemaError::FileColumnNotNullable {table,column: actual}) if table=="attachments" && actual==column));
        }
        let fresh = TestStore::new();
        fresh.schema(table(declaration()), SCHEMA).await.unwrap().close().await.unwrap();
        let error = fresh.builder(table(declaration()), vec![
            Migration::sql(1, "files", SCHEMA),
            Migration::run(2, "rebuild", move |sql| {sql.execute_batch(&format!("DROP TABLE attachments; {schema}"))?; Ok(())}),
        ]).open().await.err().unwrap();
        assert!(matches!(database_error(error), DbError::Schema(SchemaError::FileColumnNotNullable {table,column: actual}) if table=="attachments" && actual==column));
        let db = fresh.schema(table(declaration()), SCHEMA).await.unwrap();
        assert_eq!(db.schema_version().await.unwrap(),1);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn file_columns_refuse_reference_replacement_on_open_and_after_migrating() {
    for column in ["file", "size", "hash", "location"] {
        for event in ["DELETE", "UPDATE"] {
            let schema = format!(
                "CREATE TABLE tokens(id TEXT NOT NULL PRIMARY KEY); \
                 CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT,size INTEGER,hash BLOB,location TEXT, \
                 FOREIGN KEY({column}) REFERENCES tokens(id) ON {event} SET NULL)"
            );
            let tables = |files| {
                let mut attachment = SyncedTable::new("attachments", RowIdentity::SharedKey);
                if files {
                    attachment = attachment.carries_files(declaration().with_id_column("FILE"));
                }
                vec![
                    SyncedTable::new("tokens", RowIdentity::SharedKey),
                    attachment,
                ]
            };
            let migrations = || {
                let schema = schema.clone();
                vec![Migration::run(1, "files", move |sql| {
                    sql.execute_batch(&schema)?;
                    Ok(())
                })]
            };
            let store = TestStore::new();
            // Ordinary references are valid. Adding a file declaration must
            // refuse them even when no migration needs to run.
            store
                .builder(tables(false), migrations())
                .open()
                .await
                .unwrap()
                .close()
                .await
                .unwrap();
            for read_only in [false, true] {
                let builder = store.builder(tables(true), migrations());
                let result = if read_only {
                    builder.open_read_only().await.map(|_| ())
                } else {
                    builder.open().await.map(|_| ())
                };
                assert_file_action(result.unwrap_err(), column);
            }
            let fresh = TestStore::new();
            fresh
                .schema(table(declaration()), SCHEMA)
                .await
                .unwrap()
                .close()
                .await
                .unwrap();
            let migrated = schema.clone();
            let result = fresh
                .builder(
                    tables(true),
                    vec![
                        Migration::sql(1, "files", SCHEMA),
                        Migration::run(2, "file reference", move |sql| {
                            sql.execute_batch(&format!("DROP TABLE attachments; {migrated}"))?;
                            Ok(())
                        }),
                    ],
                )
                .open()
                .await
                .map(|_| ());
            assert_file_action(result.unwrap_err(), column);
            // The failed migration preserves the previous table and version.
            let db = fresh.schema(table(declaration()), SCHEMA).await.unwrap();
            assert_eq!(db.schema_version().await.unwrap(), 1);
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn a_foreign_key_action_cannot_leave_a_file_reference_without_its_bytes() {
    // Ana deletes a token while Ben, offline, attaches a file referring to it.
    // Applying Ana's deletion on Ben would SET NULL only the file id, leaving
    // its hash and location attached to no file. Refuse that schema up front.
    let store = TestStore::new();
    let result = store.schema(
        vec![
            SyncedTable::new("tokens", RowIdentity::SharedKey),
            SyncedTable::new("attachments", RowIdentity::SharedKey)
                .carries_files(declaration().with_id_column("file")),
        ],
        "CREATE TABLE tokens(id TEXT NOT NULL PRIMARY KEY); \
         CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT REFERENCES tokens(id) ON DELETE SET NULL,size INTEGER,hash BLOB,location TEXT)",
    ).await;
    assert_file_action(result.err().unwrap(), "file");
}

fn assert_file_action(error: crate::CovenError, column: &str) {
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileForeignKeyAction {table, column: actual}) if table == "attachments" && actual.eq_ignore_ascii_case(column))
    );
}
