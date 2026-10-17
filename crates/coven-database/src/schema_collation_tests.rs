use crate::{
    schema::tests::shared,
    tests::{database_error, TestStore},
    DbError, Migration, SchemaError, SyncedTable,
};

async fn refuses_collation(
    sql: String,
    tables: Vec<SyncedTable>,
    table: &str,
    column: &str,
    collation: &str,
) {
    let store = TestStore::new();
    let migration = sql.clone();
    let error = store
        .builder(
            tables,
            vec![Migration::addition_with(1, "schema", move |context| {
                context.execute_batch(&migration)?;
                Ok(())
            })],
        )
        .open()
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} must be refused"));
    let expected = SchemaError::KeyCollation {
        table: table.into(),
        column: column.into(),
        collation: collation.into(),
    };
    match database_error(error) {
        DbError::Schema(error) => assert_eq!(error, expected, "{sql}"),
        error => panic!("{sql}: expected {expected:?}, received {error:?}"),
    }
}

#[tokio::test]
async fn key_columns_that_fold_text_are_refused_on_every_key_kind() {
    for collation in ["NOCASE", "RTRIM"] {
        let c = collation;
        for (sql, tables, table, column) in [
            (format!("CREATE TABLE notes(id TEXT COLLATE {c} NOT NULL PRIMARY KEY)"), vec![shared("notes")], "notes", "id"),
            (format!("CREATE TABLE notes(id TEXT NOT NULL, PRIMARY KEY(id COLLATE {c})) WITHOUT ROWID"), vec![shared("notes")], "notes", "id"),
            (format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT COLLATE {c} UNIQUE)"), vec![shared("notes")], "notes", "title"),
            (format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, UNIQUE(title COLLATE {c}))"), vec![shared("notes")], "notes", "title"),
            (format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT); CREATE UNIQUE INDEX titled ON notes(title COLLATE {c})"), vec![shared("notes")], "notes", "title"),
            (format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT); CREATE UNIQUE INDEX folded ON notes(lower(title) COLLATE {c})"), vec![shared("notes")], "notes", "folded"),
            // A BINARY index term still refuses the column's own collation, which
            // SQLite applies to an ordinary `title = ?` comparison.
            (format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT COLLATE {c}); CREATE UNIQUE INDEX titled ON notes(title COLLATE BINARY)"), vec![shared("notes")], "notes", "title"),
        ] {
            refuses_collation(sql, tables, table, column, collation).await;
        }
        let references = |child: &str, parent: &str| {
            format!("CREATE TABLE parents(id TEXT {parent} NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY, parent TEXT {child} REFERENCES parents(id) ON DELETE CASCADE)")
        };
        let folded = format!("COLLATE {c}");
        for (tables, child, parent, table, column) in [
            (
                vec![shared("parents"), shared("children")],
                folded.as_str(),
                "",
                "children",
                "parent",
            ),
            (
                vec![shared("parents")],
                folded.as_str(),
                "",
                "children",
                "parent",
            ),
            (
                vec![shared("children")],
                folded.as_str(),
                "",
                "children",
                "parent",
            ),
            (
                vec![shared("children")],
                "",
                folded.as_str(),
                "parents",
                "id",
            ),
        ] {
            refuses_collation(references(child, parent), tables, table, column, collation).await;
        }
    }
}

/// Coven's connections register no app collation, so a key declared with one
/// fails SQLite's own open-time integrity check before schema validation, which
/// refuses every collation other than BINARY by name.
#[tokio::test]
async fn an_app_defined_key_collation_is_refused_on_open() {
    let store = TestStore::new();
    store
        .builder(vec![], vec![])
        .open()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let raw = rusqlite::Connection::open(store.database_path()).unwrap();
    raw.create_collation("reverse", |a, b| b.cmp(a)).unwrap();
    raw.execute_batch("CREATE TABLE notes(id TEXT COLLATE reverse NOT NULL PRIMARY KEY)")
        .unwrap();
    drop(raw);
    let error = store
        .builder(vec![shared("notes")], vec![])
        .open()
        .await
        .err()
        .expect("custom key collation must be refused");
    match database_error(error) {
        DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message))) => {
            assert_eq!(message, "no such collation sequence: reverse")
        }
        error => panic!("expected the unknown collation, received {error:?}"),
    }
}

#[tokio::test]
async fn binary_keys_and_folded_values_outside_keys_are_accepted() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![shared("notes"), shared("tags")],
            "CREATE TABLE notes(id TEXT COLLATE BINARY NOT NULL PRIMARY KEY, title TEXT COLLATE NOCASE, body TEXT COLLATE RTRIM, slug TEXT, UNIQUE(slug COLLATE BINARY));
             CREATE UNIQUE INDEX lowered ON notes(lower(title));
             CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY, note TEXT COLLATE BINARY REFERENCES notes(id) ON DELETE CASCADE);
             CREATE TABLE local_words(word TEXT COLLATE NOCASE PRIMARY KEY, note TEXT REFERENCES local_words(word))",
        )
        .await
        .unwrap();
    db.close().await.unwrap();
}
