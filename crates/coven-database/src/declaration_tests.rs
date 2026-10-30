use super::*;
use crate::tests::{database_error, TestStore};
use crate::{DbError, Migration, SchemaError};

#[test]
fn declaration_getters_expose_defaults_without_opening_a_database() {
    let table = SyncedTable::new("Notes", RowIdentity::IndependentUuid);
    assert_eq!(table.name(), "Notes");
    assert_eq!(table.identity(), &RowIdentity::IndependentUuid);
    assert_eq!(table.get_key_columns(), ["id"]);
    assert!(!table.is_owner_only());
    assert_eq!(table.get_audience_column(), None);
    assert_eq!(table.get_audience_from(), None);
    assert!(table.file_columns().is_empty());
    assert!(table.shared_triggers().is_empty());

    let file = FileColumn::new("File");
    assert_eq!(file.column(), "File");
    assert!(!file.is_write_once());

    let namespace = Namespace::new("Originals", Provenance::UserProvided, 12345);
    assert_eq!(namespace.name(), "Originals");
    assert_eq!(namespace.kind(), &Provenance::UserProvided);
    assert_eq!(namespace.cache_budget(), 12345);
}

#[test]
fn declaration_getters_preserve_the_builders_choices_and_spelling() {
    let table = SyncedTable::new("Attachments", RowIdentity::SharedKey)
        .key_columns(["Note", "Attachment"])
        .audience_from("Note")
        .owner_only()
        .shared_trigger("RecordInsert")
        .shared_trigger("RecordUpdate")
        .shared_trigger("RecordInsert")
        .file_column(FileColumn::new("Original").write_once())
        .file_column(FileColumn::new("Preview"));
    assert_eq!(table.name(), "Attachments");
    assert_eq!(table.identity(), &RowIdentity::SharedKey);
    assert_eq!(table.get_key_columns(), ["Note", "Attachment"]);
    assert!(table.is_owner_only());
    assert_eq!(table.get_audience_column(), None);
    assert_eq!(table.get_audience_from(), Some("Note"));
    assert_eq!(
        table.shared_triggers(),
        ["RecordInsert", "RecordUpdate", "RecordInsert"]
    );
    assert_eq!(
        table.file_columns(),
        [
            FileColumn::new("Original").write_once(),
            FileColumn::new("Preview")
        ]
    );

    let table = table.key_columns(["ReplacementKey"]);
    assert_eq!(table.get_key_columns(), ["ReplacementKey"]);
}

#[test]
fn audience_getters_preserve_conflicting_declarations_in_either_order() {
    let root = SyncedTable::new("Notes", RowIdentity::IndependentUuid)
        .audience_column("OldAudience")
        .audience_column("Audience");
    assert_eq!(root.get_audience_column(), Some("Audience"));
    assert_eq!(root.get_audience_from(), None);

    for table in [
        root.audience_from("Note"),
        SyncedTable::new("Notes", RowIdentity::IndependentUuid)
            .audience_from("Note")
            .audience_column("Audience"),
    ] {
        assert_eq!(table.get_audience_column(), Some("Audience"));
        assert_eq!(table.get_audience_from(), Some("Note"));
        let table = table.audience_from("Parent").audience_column("Readers");
        assert_eq!(table.get_audience_column(), Some("Readers"));
        assert_eq!(table.get_audience_from(), Some("Parent"));
    }
}

const SCHEMA: &str = "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT)";

fn table(columns: &[&str]) -> Vec<SyncedTable> {
    let mut table = SyncedTable::new("attachments", RowIdentity::SharedKey);
    for column in columns {
        table = table.file_column(FileColumn::new(*column));
    }
    vec![table]
}

async fn open(store: &TestStore, tables: Vec<SyncedTable>, read_only: bool) -> Result<(), DbError> {
    let builder = store.builder(tables, vec![Migration::addition(1, "files", SCHEMA)]);
    let result = if read_only {
        builder.open_read_only().await.map(|_| ())
    } else {
        builder.open().await.map(|_| ())
    };
    result.map_err(database_error)
}

#[tokio::test]
async fn file_columns_are_checked_on_the_synced_table_in_any_case() {
    for column in ["file", "FILE"] {
        let store = TestStore::new();
        let db = store.schema(table(&[column]), SCHEMA).await.unwrap();
        db.close().await.unwrap();
        for read_only in [false, true] {
            open(&store, table(&[column]), read_only).await.unwrap();
        }
    }
}

#[tokio::test]
async fn a_missing_file_column_names_its_table_on_open_and_after_migration() {
    let store = TestStore::new();
    let error = store
        .schema(table(&["missing"]), SCHEMA)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumn { table, column }) if table == "attachments" && column == "missing")
    );
    let db = store.schema(table(&["file"]), SCHEMA).await.unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    db.close().await.unwrap();
    for read_only in [false, true] {
        let error = open(&store, table(&["missing"]), read_only)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, DbError::Schema(SchemaError::FileColumn { table, column }) if table == "attachments" && column == "missing"),
            "{error:?}"
        );
    }
}

#[tokio::test]
async fn final_file_columns_validate_after_all_migrations_and_failure_rolls_them_back() {
    let store = TestStore::new();
    store
        .schema(table(&["file"]), SCHEMA)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let migrations = || {
        vec![
            Migration::addition(1, "files", SCHEMA),
            Migration::breaking(
                2,
                "rename file",
                "ALTER TABLE attachments RENAME COLUMN file TO original",
                crate::OldWrites::Drop,
            ),
        ]
    };
    let error = store
        .builder(table(&["file"]), migrations())
        .open()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumn { table, column }) if table == "attachments" && column == "file")
    );
    let db = store.schema(table(&["file"]), SCHEMA).await.unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    db.close().await.unwrap();
    let db = store
        .builder(table(&["original"]), migrations())
        .open()
        .await
        .unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_view_or_another_tables_columns_cannot_satisfy_a_file_column() {
    for schema in [
        "CREATE TABLE local(id TEXT NOT NULL PRIMARY KEY,file TEXT)",
        "CREATE TABLE local(id TEXT NOT NULL PRIMARY KEY,file TEXT); CREATE VIEW attachments AS SELECT * FROM local",
    ] {
        let store = TestStore::new();
        let error = store.schema(table(&["file"]), schema).await.err().unwrap();
        assert!(matches!(database_error(error), DbError::Schema(SchemaError::MissingTable { table }) if table == "attachments"));
    }
}

#[tokio::test]
async fn a_file_column_declared_twice_is_refused() {
    let store = TestStore::new();
    let error = store
        .schema(table(&["file", "FILE"]), SCHEMA)
        .await
        .err()
        .unwrap();
    let error = database_error(error);
    assert!(
        matches!(&error, DbError::Schema(SchemaError::FileColumnRepeated { table, column }) if table == "attachments" && column == "file"),
        "{error:?}"
    );
}

/// Coven writes a file column when its row moves to another audience, so the
/// column can't be generated or also reference another table.
#[tokio::test]
async fn a_file_column_coven_could_not_write_is_refused() {
    let generated = "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT GENERATED ALWAYS AS (id) VIRTUAL)";
    let store = TestStore::new();
    let error = store
        .schema(table(&["file"]), generated)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumnGenerated { table, column }) if table == "attachments" && column == "file")
    );
    let referencing = "CREATE TABLE tokens(id TEXT NOT NULL PRIMARY KEY); \
         CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT REFERENCES tokens(id))";
    let tables = |files: bool| {
        let mut tables = table(if files { &["file"] } else { &[] });
        tables.push(SyncedTable::new("tokens", RowIdentity::SharedKey));
        tables
    };
    let store = TestStore::new();
    let error = store.schema(tables(true), referencing).await.err().unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumnForeignKey { table, column }) if table == "attachments" && column == "file")
    );
    // After a migration too, which then leaves the earlier schema in place.
    let fresh = TestStore::new();
    fresh
        .schema(table(&["file"]), SCHEMA)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let error = fresh
        .builder(
            tables(true),
            vec![
                Migration::addition(1, "files", SCHEMA),
                Migration::breaking_with(
                    2,
                    "file reference",
                    move |sql| {
                        sql.execute_batch(&format!("DROP TABLE attachments; {referencing}"))?;
                        Ok(())
                    },
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumnForeignKey { table, column }) if table == "attachments" && column == "file")
    );
    let db = fresh.schema(table(&["file"]), SCHEMA).await.unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 1);
    db.close().await.unwrap();
}

/// §17.2: a file column is the only way a row references a file. An app's
/// own SQL key into coven's files table is refused, on a file column or not,
/// in a synced table or a local one, however the table is spelled.
#[tokio::test]
async fn an_sql_foreign_key_into_the_files_table_is_refused() {
    for (files, schema) in [
        (
            &["file"][..],
            "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT REFERENCES coven_files(id))",
        ),
        (
            &[][..],
            "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT REFERENCES coven_files(id))",
        ),
        (
            &["file"][..],
            "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,file TEXT); \
             CREATE TABLE seen(file TEXT PRIMARY KEY REFERENCES COVEN_FILES ON DELETE CASCADE)",
        ),
    ] {
        let store = TestStore::new();
        let error = database_error(store.schema(table(files), schema).await.err().unwrap());
        assert!(
            matches!(&error, DbError::Schema(SchemaError::FilesForeignKey { key, .. })
                if key.to_ascii_lowercase().contains("references \"coven_files\"")),
            "{schema}: {error:?}"
        );
    }
}

/// §18.1: which columns hold file references changes only with a breaking
/// migration, since every device's rows must agree on it.
#[tokio::test]
async fn file_columns_change_only_with_a_migration() {
    let store = TestStore::new();
    store
        .schema(table(&["file"]), SCHEMA)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    for (tables, read_only) in [
        (table(&[]), false),
        (table(&[]), true),
        (
            vec![SyncedTable::new("attachments", RowIdentity::SharedKey)
                .file_column(FileColumn::new("file").write_once())],
            false,
        ),
    ] {
        let error = open(&store, tables, read_only).await.unwrap_err();
        assert!(
            matches!(&error, DbError::Schema(SchemaError::FileColumnsChanged { table }) if table == "attachments"),
            "{error:?}"
        );
    }
    let db = store
        .builder(
            table(&[]),
            vec![
                Migration::addition(1, "files", SCHEMA),
                Migration::breaking(
                    2,
                    "plain ids",
                    "UPDATE attachments SET file=NULL",
                    crate::OldWrites::Drop,
                ),
            ],
        )
        .open()
        .await
        .unwrap();
    assert_eq!(db.schema_version().await.unwrap(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_namespace_declared_twice_is_refused() {
    let store = TestStore::new();
    let error = store
        .builder(
            table(&["file"]),
            vec![Migration::addition(1, "files", SCHEMA)],
        )
        .namespaces(vec![
            Namespace::new("files", Provenance::AppProvided, 1),
            Namespace::new("files", Provenance::UserProvided, 2),
        ])
        .open()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::DuplicateNamespace { namespace }) if namespace == "files")
    );
}
