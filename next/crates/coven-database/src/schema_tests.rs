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
        "CREATE TABLE notes (id TEXT PRIMARY KEY DEFAULT 'generated')",
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
        "CREATE TABLE notes (id TEXT, other TEXT, PRIMARY KEY(other,id))",
        vec![independent("notes").key_columns(["id", "other"])],
        SchemaError::KeyColumns {
            table: "notes".into(),
        },
    )
    .await;
}

#[tokio::test]
async fn uuid_values_must_be_canonical_lowercase_v4_or_v7() {
    for schema in [
        "CREATE TABLE notes (id TEXT PRIMARY KEY); INSERT INTO notes VALUES ('not a uuid')",
        "CREATE TABLE notes (id TEXT PRIMARY KEY); INSERT INTO notes VALUES ('F47AC10B-58CC-4372-A567-0E02B2C3D479')",
        "CREATE TABLE notes (id TEXT PRIMARY KEY); INSERT INTO notes VALUES ('f47ac10b-58cc-1372-a567-0e02b2c3d479')",
        "CREATE TABLE notes (id TEXT PRIMARY KEY); INSERT INTO notes VALUES (NULL)",
        "CREATE TABLE notes (id TEXT PRIMARY KEY); INSERT INTO notes VALUES ('f47ac10b-58cc-4372-7567-0e02b2c3d479')",
    ] { rejects(schema, notes_tables(), SchemaError::IndependentKeyNotUuid { table: "notes".into() }).await; }
    let store = TestStore::new();
    let database = store.schema(vec![independent("notes").key_columns(["number", "id"])], "CREATE TABLE notes (number INTEGER, id TEXT, PRIMARY KEY(number,id)); INSERT INTO notes VALUES (2,'f47ac10b-58cc-4372-a567-0e02b2c3d479'), (3,'018f22bb-aaaa-7777-8ccc-000000000001')").await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn key_foreign_keys_cannot_replace_values_on_delete_or_update() {
    for schema in [
        "CREATE TABLE p (id TEXT PRIMARY KEY); CREATE TABLE c (id TEXT PRIMARY KEY REFERENCES p(id) ON DELETE SET NULL)",
        "CREATE TABLE p (id TEXT PRIMARY KEY); CREATE TABLE c (id TEXT PRIMARY KEY REFERENCES p(id) ON UPDATE SET DEFAULT)",
    ] { rejects(schema, vec![shared("p"), shared("c")], SchemaError::PrimaryKeyAction { table: "c".into(), column: "id".into() }).await; }
}

#[tokio::test]
async fn root_audience_requires_a_nonnull_text_column() {
    for schema in [
        "CREATE TABLE notes (id TEXT PRIMARY KEY)",
        "CREATE TABLE notes (id TEXT PRIMARY KEY, audience TEXT)",
        "CREATE TABLE notes (id TEXT PRIMARY KEY, audience INTEGER NOT NULL)",
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
    rejects("CREATE TABLE p (id TEXT, x TEXT, PRIMARY KEY(id,x)); CREATE TABLE c (id TEXT PRIMARY KEY, p_id TEXT, p_x TEXT, FOREIGN KEY(p_id,p_x) REFERENCES p(id,x))", vec![shared("p").key_columns(["id","x"]), independent("c").audience_from("p_id")], SchemaError::AudienceForeignKeyColumns { table: "c".into() }).await;
    rejects("CREATE TABLE p (id TEXT PRIMARY KEY); CREATE TABLE c (id TEXT PRIMARY KEY, p_id TEXT REFERENCES p(id))", vec![independent("c").audience_from("p_id")], SchemaError::AudienceForeignKeyTarget { table: "c".into(), column: "p_id".into() }).await;
    rejects(
        "CREATE TABLE c (id TEXT PRIMARY KEY, p_id TEXT)",
        vec![independent("c").audience_from("p_id")],
        SchemaError::AudienceForeignKeyTarget {
            table: "c".into(),
            column: "p_id".into(),
        },
    )
    .await;
    for schema in [
        "CREATE TABLE p (id TEXT PRIMARY KEY); CREATE TABLE c (id TEXT PRIMARY KEY, p_id TEXT REFERENCES p(id) ON DELETE SET NULL)",
        "CREATE TABLE p (id TEXT PRIMARY KEY); CREATE TABLE c (id TEXT PRIMARY KEY, p_id TEXT REFERENCES p(id) ON UPDATE SET DEFAULT)",
    ] { rejects(schema, vec![shared("p"), independent("c").audience_from("p_id")], SchemaError::AudienceForeignKeyAction { table: "c".into(), column: "p_id".into() }).await; }
}

#[tokio::test]
async fn audience_cycles_include_self_references_and_longer_cycles() {
    rejects(
        "CREATE TABLE notes (id TEXT PRIMARY KEY, parent TEXT REFERENCES notes(id))",
        vec![independent("notes").audience_from("parent")],
        SchemaError::AudienceCycle {
            table: "notes".into(),
        },
    )
    .await;
    rejects("CREATE TABLE a (id TEXT PRIMARY KEY, parent TEXT REFERENCES b(id)); CREATE TABLE b (id TEXT PRIMARY KEY, parent TEXT REFERENCES c(id)); CREATE TABLE c (id TEXT PRIMARY KEY, parent TEXT REFERENCES a(id))", vec![independent("a").audience_from("parent"), independent("b").audience_from("parent"), independent("c").audience_from("parent")], SchemaError::AudienceCycle { table: "a".into() }).await;
}

#[tokio::test]
async fn uniqueness_and_shared_keys_are_scoped_to_the_audience() {
    rejects("CREATE TABLE notes (id TEXT PRIMARY KEY, audience TEXT NOT NULL, title TEXT); CREATE UNIQUE INDEX title_unique ON notes(title)", vec![independent("notes").audience_column("audience")], SchemaError::AudienceConstraint { table: "notes".into(), constraint: "title_unique".into() }).await;
    rejects(
        "CREATE TABLE notes (id TEXT PRIMARY KEY, audience TEXT NOT NULL)",
        vec![shared("notes").audience_column("audience")],
        SchemaError::AudienceConstraint {
            table: "notes".into(),
            constraint: "PRIMARY KEY".into(),
        },
    )
    .await;
    rejects("CREATE TABLE p (id TEXT PRIMARY KEY); CREATE TABLE c (id TEXT PRIMARY KEY, p_id TEXT REFERENCES p(id), label TEXT); CREATE UNIQUE INDEX label_unique ON c(label)", vec![shared("p"), independent("c").audience_from("p_id")], SchemaError::AudienceConstraint { table: "c".into(), constraint: "label_unique".into() }).await;
    rejects("CREATE TABLE p (id TEXT PRIMARY KEY); CREATE TABLE c (id TEXT PRIMARY KEY, p_id TEXT REFERENCES p(id))", vec![shared("p"), shared("c").audience_from("p_id")], SchemaError::AudienceConstraint { table: "c".into(), constraint: "PRIMARY KEY".into() }).await;
    let store = TestStore::new();
    let db = store.schema(vec![shared("p").key_columns(["audience","id"]).audience_column("audience"), shared("s"), shared("c").key_columns(["s_id","id"]).audience_from("s_id")], "CREATE TABLE p (audience TEXT NOT NULL, id TEXT NOT NULL, title TEXT, PRIMARY KEY(audience,id), UNIQUE(audience,title)); CREATE TABLE s (id TEXT PRIMARY KEY); CREATE TABLE c (s_id TEXT REFERENCES s(id), id TEXT, label TEXT, PRIMARY KEY(s_id,id)); CREATE UNIQUE INDEX scoped_label ON c(s_id,lower(label))").await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn unguarded_shared_triggers_are_schema_errors() {
    for schema in [
        "CREATE TABLE notes (id TEXT PRIMARY KEY, title TEXT); CREATE TRIGGER edited AFTER UPDATE ON notes BEGIN SELECT 1; END",
        "CREATE TABLE notes (id TEXT PRIMARY KEY, title TEXT); CREATE TRIGGER edited AFTER UPDATE ON notes WHEN NOT coven_applying() OR 1 BEGIN SELECT 1; END",
    ] { rejects(schema, vec![independent("notes").shared_trigger("edited")], SchemaError::SharedTriggerGuard { table: "notes".into(), trigger: "edited".into() }).await; }
}

#[tokio::test]
async fn file_declarations_check_every_named_column_and_accept_custom_names() {
    let file = || {
        FileDecl::new(
            "audio",
            Provenance::UserProvided,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )
    };
    let store = TestStore::new();
    let error = store
        .schema(
            vec![shared("files").carries_files(file())],
            "CREATE TABLE files (id TEXT PRIMARY KEY, size INTEGER, hash BLOB)",
        )
        .await
        .err()
        .unwrap();
    assert!(
        matches!(database_error(error), DbError::Schema(SchemaError::FileColumn { table, column }) if table == "files" && column == "location")
    );
    let db = store.schema(vec![shared("files").carries_files(file().with_id_column("file_id").with_size_column("bytes").with_hash_column("sha").with_location_column("place").write_once())], "CREATE TABLE files (id TEXT PRIMARY KEY, file_id TEXT, bytes INTEGER, sha BLOB, place TEXT)").await.unwrap();
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
        "CREATE TABLE notes(id TEXT PRIMARY KEY)",
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
        rejects("CREATE TABLE notes(id TEXT PRIMARY KEY, audience TEXT NOT NULL, parent TEXT REFERENCES notes(id))", vec![declaration], SchemaError::TwoAudiences { table: "notes".into() }).await;
    }
    for schema in [
        "CREATE TABLE notes(id TEXT PRIMARY KEY)",
        "CREATE TABLE notes(id TEXT PRIMARY KEY); CREATE TABLE local(id TEXT); CREATE TRIGGER edited AFTER UPDATE ON local WHEN NOT coven_applying() BEGIN SELECT 1; END",
    ] {
        rejects(schema, vec![shared("notes").shared_trigger("edited")], SchemaError::MissingTrigger { table: "notes".into(), trigger: "edited".into() }).await;
    }
}
