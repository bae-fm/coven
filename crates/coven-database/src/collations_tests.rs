use super::*;
use crate::sqlite::DatabaseConnection;
use crate::tests::{database_error, remote_update, TestStore};
use crate::write::tests::{records, sql};
use crate::{Migration, RowIdentity, SyncedTable};
use coven_foundation::id_source::SequentialIds;

/// Text in reverse byte order: unlike `BINARY`, so a result shows which
/// order sorted it.
fn reversed() -> Collations {
    Collations::default().with("reversed", |left: &str, right: &str| right.cmp(left))
}

fn notes() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

/// A synced table that compares its titles by the app's collation in a
/// column declaration, an index and a CHECK.
const ORDERED_NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL COLLATE reversed, CHECK (title >= 'z' COLLATE reversed)); CREATE INDEX notes_reversed ON notes(title COLLATE reversed)";

/// The index arrives in a later migration, which runs on the writer.
fn ordered_migrations() -> Vec<Migration> {
    vec![
        Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
        ),
        Migration::addition(
            2,
            "ordered",
            "CREATE TABLE ordered(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL COLLATE reversed, CHECK (title >= 'z' COLLATE reversed)); CREATE INDEX ordered_reversed ON ordered(title COLLATE reversed)",
        ),
    ]
}

const IN_ORDER: &str = "SELECT title FROM notes ORDER BY title COLLATE reversed";

fn titles(connection: &DatabaseConnection, sql: &str) -> Vec<String> {
    connection.query(sql, [], |row| row.get(0)).unwrap()
}

fn plan(connection: &DatabaseConnection, sql: &str) -> String {
    connection
        .query(&format!("EXPLAIN QUERY PLAN {sql}"), [], |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .join("; ")
}

#[tokio::test]
async fn order_by_the_apps_collation_uses_its_function_on_every_connection() {
    let store = TestStore::new();
    let migrations = || {
        vec![Migration::addition(
            1,
            "notes",
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
        )]
    };
    let database = store
        .builder(notes(), migrations())
        .collations(reversed())
        .open()
        .await
        .unwrap();
    sql(
        &database,
        "INSERT INTO notes VALUES ('1','apples'),('2','cherries'),('3','bananas')",
    )
    .await
    .unwrap();
    let expected = ["cherries", "bananas", "apples"];
    let read = database
        .read(|sql| Ok(sql.query(IN_ORDER, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    database.inspect_writer(|writer| assert_eq!(titles(writer, IN_ORDER), expected));
    database.inspect_readers(|reader| assert_eq!(titles(reader, IN_ORDER), expected));

    let reading = store
        .builder(notes(), migrations())
        .collations(reversed())
        .open_read_only()
        .await
        .unwrap();
    let read = reading
        .read(|sql| Ok(sql.query(IN_ORDER, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    reading.inspect_writer(|writer| assert_eq!(titles(writer, IN_ORDER), expected));
    reading.inspect_readers(|reader| assert_eq!(titles(reader, IN_ORDER), expected));
    reading.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn an_index_in_the_apps_order_is_used_and_stays_ordered_through_remote_writes() {
    let ids = SequentialIds::new();
    let ana = TestStore::with_ids(&ids);
    let ben = TestStore::with_ids(&ids);
    let open = |store: &TestStore| {
        store
            .builder(
                notes(),
                vec![Migration::addition(1, "notes", ORDERED_NOTES)],
            )
            .collations(reversed())
            .open()
    };
    let ana_db = open(&ana).await.unwrap();
    let ben_db = open(&ben).await.unwrap();
    sql(
        &ana_db,
        "INSERT INTO notes VALUES ('1','apples'),('2','cherries'),('3','bananas')",
    )
    .await
    .unwrap();
    // Ben applies Ana's write, maintaining his own index in the same order.
    for record in records(&ana_db) {
        ben_db.receive(record.into()).await.unwrap();
    }
    sql(&ben_db, "INSERT INTO notes VALUES ('4','dates')")
        .await
        .unwrap();
    // Another device renames Ben's note.
    remote_update(
        &ben_db,
        "notes",
        "4",
        &[("title", coven_format::value::Value::Text("figs".into()))],
    )
    .await;
    // The CHECK compares by the app's order too: in reverse order every title
    // before 'z' in bytes comes after it, so it passes the titles above, which
    // BINARY would refuse, and refuses only one past 'z'.
    match sql(&ben_db, "INSERT INTO notes VALUES ('5','zebras')").await {
        Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))) => {
            assert_eq!(failure.code, rusqlite::ErrorCode::ConstraintViolation)
        }
        result => panic!("expected the CHECK to refuse, received {result:?}"),
    }

    ben_db.inspect_readers(|reader| {
        let ordered = plan(reader, IN_ORDER);
        assert!(ordered.contains("INDEX notes_reversed"), "{ordered}");
        assert_eq!(
            titles(reader, IN_ORDER),
            ["figs", "cherries", "bananas", "apples"]
        );
        let search = "SELECT title FROM notes WHERE title = 'figs' COLLATE reversed";
        let searched = plan(reader, search);
        assert!(
            searched.contains("INDEX notes_reversed (title=?)"),
            "{searched}"
        );
        assert_eq!(titles(reader, search), ["figs"]);
    });
    ben_db.inspect_writer(|writer| {
        let check: Vec<String> = writer
            .query("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(check, ["ok"]);
    });
    ben_db.close().await.unwrap();
    ana_db.close().await.unwrap();
    // Opening checks integrity, the app's index included.
    let reopened = open(&ben).await.unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn a_migration_declares_the_apps_collation_on_the_writer() {
    let store = TestStore::new();
    let tables = || {
        vec![
            SyncedTable::new("notes", RowIdentity::SharedKey),
            SyncedTable::new("ordered", RowIdentity::SharedKey),
        ]
    };
    let database = store
        .builder(tables(), ordered_migrations())
        .collations(reversed())
        .open()
        .await
        .unwrap();
    database.close().await.unwrap();

    // Without the registration, SQLite can't build the index.
    let store = TestStore::new();
    let error = store
        .builder(tables(), ordered_migrations())
        .open()
        .await
        .err()
        .expect("the index needs its collation");
    match database_error(error) {
        DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message))) => {
            assert_eq!(message, "no such collation sequence: reversed")
        }
        error => panic!("expected the unknown collation, received {error:?}"),
    }
}

#[tokio::test]
async fn sqlites_own_names_and_a_repeated_name_are_refused() {
    for name in ["BINARY", "nocase", "Rtrim"] {
        let store = TestStore::new();
        let error = store
            .builder(notes(), vec![])
            .collations(Collations::default().with(name, |left: &str, right: &str| left.cmp(right)))
            .open()
            .await
            .err()
            .expect("SQLite's own name is refused");
        match database_error(error) {
            DbError::ReservedCollation { name: refused } => assert_eq!(refused, name),
            error => panic!("{name}: expected ReservedCollation, received {error:?}"),
        }
        store.assert_writer_unlocked();
    }
    let store = TestStore::new();
    let error = store
        .builder(notes(), vec![])
        .collations(reversed().with("REVERSED", |left: &str, right: &str| left.cmp(right)))
        .open()
        .await
        .err()
        .expect("a repeated name is refused");
    match database_error(error) {
        DbError::DuplicateCollation { name } => assert_eq!(name, "REVERSED"),
        error => panic!("expected DuplicateCollation, received {error:?}"),
    }
    store.assert_writer_unlocked();
}

#[tokio::test]
async fn reading_a_discarded_database_checks_its_app_indexes_with_the_apps_collations() {
    let store = TestStore::new();
    let database = store
        .builder(
            notes(),
            vec![Migration::addition(1, "notes", ORDERED_NOTES)],
        )
        .collations(reversed())
        .open()
        .await
        .unwrap();
    database
        .initialize_store_name("Household".into())
        .await
        .unwrap();
    sql(&database, "INSERT INTO notes VALUES ('1','apples')")
        .await
        .unwrap();
    database.close().await.unwrap();

    let operations = crate::discarded_operations(store.database_path(), reversed())
        .await
        .unwrap();
    assert!(operations.is_empty());
    // The integrity check reads the app's index, so it can't run without the
    // collation; it fails rather than reporting the database damaged.
    match crate::discarded_operations(store.database_path(), Collations::default()).await {
        Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message)))) => {
            assert_eq!(message, "no such collation sequence: reversed")
        }
        result => panic!("expected the unknown collation, received {result:?}"),
    }
    // Reading the name compares nothing by the app's order.
    assert_eq!(
        crate::read_store_name(store.directory()).await.unwrap(),
        "Household"
    );
    assert_eq!(
        crate::discarded_store_name(store.database_path())
            .await
            .unwrap(),
        "Household"
    );
}

/// The child process of the test below: a write whose index comparison
/// panics.
#[test]
fn panicking_collation_writer() {
    let Ok(path) = std::env::var("COVEN_COLLATION_PANIC_FILE") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let layout = coven_foundation::files::StoreDirectories::new(
        path.parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned(),
    );
    let store = coven_foundation::id_source::StoreId(
        uuid::Uuid::parse_str(&std::env::var("COVEN_COLLATION_PANIC_STORE").unwrap()).unwrap(),
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = runtime
        .block_on(
            crate::DatabaseBuilder::new(layout.store_dir(&store))
                .synced_tables(notes())
                .migrations(vec![Migration::addition(1, "notes", ORDERED_NOTES)])
                .collations(
                    Collations::default().with("reversed", |left: &str, right: &str| {
                        // Opening's integrity check compares the stored
                        // titles; only the new one panics.
                        assert!(
                            left != "bananas" && right != "bananas",
                            "the comparison panicked"
                        );
                        right.cmp(left)
                    }),
                )
                .open(),
        )
        .unwrap();
    let written = runtime.block_on(sql(&database, "INSERT INTO notes VALUES ('2','bananas')"));
    panic!("the write returned {written:?}");
}

#[test]
fn a_panicking_comparison_ends_the_process_before_its_write_commits() {
    let store = TestStore::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let open = || {
        store
            .builder(
                notes(),
                vec![Migration::addition(1, "notes", ORDERED_NOTES)],
            )
            .collations(reversed())
            .open()
    };
    let database = runtime.block_on(open()).unwrap();
    runtime
        .block_on(sql(&database, "INSERT INTO notes VALUES ('1','apples')"))
        .unwrap();
    let before = crate::tests::contents(&database);
    runtime.block_on(database.close()).unwrap();

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "collations::tests::panicking_collation_writer",
            "--nocapture",
        ])
        .env("COVEN_COLLATION_PANIC_FILE", store.database_path())
        .env("COVEN_COLLATION_PANIC_STORE", store.id().to_string())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("the comparison panicked"), "{stderr}");
    assert!(!stderr.contains("the write returned"), "{stderr}");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(output.status.signal(), Some(6), "SIGABRT: {stderr}");
    }
    #[cfg(not(unix))]
    assert!(!output.status.success(), "{stderr}");

    // The write never committed, and the index is intact.
    let database = runtime.block_on(open()).unwrap();
    assert_eq!(crate::tests::contents(&database), before);
    runtime.block_on(database.close()).unwrap();
}
