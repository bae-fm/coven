use super::*;
use crate::tests::{database_error, TestStore};
use crate::{DbError, Migration, SchemaError};

const SCHEMA: &str =
    "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location BLOB)";

fn declaration() -> FileDecl {
    FileDecl::new(
        "attachments",
        Provenance::AppProvided,
        Uploads::WhenAttached,
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
                Uploads::WhenAsked,
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
