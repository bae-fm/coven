use super::*;
use crate::sqlite::DatabaseConnection;
use crate::tests::{database_error, remote_update, try_remote_update, TestStore};
use crate::write::tests::{records, sql};
use crate::{Migration, RowIdentity, SyncedTable};
use coven_foundation::id_source::SequentialIds;

/// Text in reverse byte order: unlike `BINARY`, so a result shows which
/// order sorted it.
fn reversed() -> SqlFunctions {
    SqlFunctions::default().collation("reversed", 1, |left: &str, right: &str| right.cmp(left))
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
        .query(&format!("EXPLAIN QUERY PLAN {sql}"), [], |row| row.get::<_, String>(3))
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
    let database =
        store.builder(notes(), migrations()).sql_functions(reversed()).open().await.unwrap();
    sql(&database, "INSERT INTO notes VALUES ('1','apples'),('2','cherries'),('3','bananas')")
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
        .sql_functions(reversed())
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
            .builder(notes(), vec![Migration::addition(1, "notes", ORDERED_NOTES)])
            .sql_functions(reversed())
            .open()
    };
    let ana_db = open(&ana).await.unwrap();
    let ben_db = open(&ben).await.unwrap();
    sql(&ana_db, "INSERT INTO notes VALUES ('1','apples'),('2','cherries'),('3','bananas')")
        .await
        .unwrap();
    // Ben applies Ana's write, maintaining his own index in the same order.
    for record in records(&ana_db) {
        ben_db.receive(record.into()).await.unwrap();
    }
    sql(&ben_db, "INSERT INTO notes VALUES ('4','dates')").await.unwrap();
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
        assert_eq!(titles(reader, IN_ORDER), ["figs", "cherries", "bananas", "apples"]);
        let search = "SELECT title FROM notes WHERE title = 'figs' COLLATE reversed";
        let searched = plan(reader, search);
        assert!(searched.contains("INDEX notes_reversed (title=?)"), "{searched}");
        assert_eq!(titles(reader, search), ["figs"]);
    });
    ben_db.inspect_writer(|writer| {
        let check: Vec<String> =
            writer.query("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
        assert_eq!(check, ["ok"]);
    });
    ben_db.close().await.unwrap();
    ana_db.close().await.unwrap();
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
        .sql_functions(reversed())
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
            .sql_functions(
                SqlFunctions::default()
                    .collation(name, 1, |left: &str, right: &str| left.cmp(right)),
            )
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
        .sql_functions(
            reversed().collation("REVERSED", 1, |left: &str, right: &str| left.cmp(right)),
        )
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

/// Reading a discarded database's journal, or a store's name, compares
/// nothing in the app's order, so it needs none of the app's collations.
#[tokio::test]
async fn reading_a_discarded_database_needs_none_of_the_apps_collations() {
    let store = TestStore::new();
    let database = store
        .builder(notes(), vec![Migration::addition(1, "notes", ORDERED_NOTES)])
        .sql_functions(reversed())
        .open()
        .await
        .unwrap();
    database.initialize_store_name("Household".into()).await.unwrap();
    sql(&database, "INSERT INTO notes VALUES ('1','apples')").await.unwrap();
    database.close().await.unwrap();

    let operations = crate::discarded_operations(store.database_path()).await.unwrap();
    assert!(operations.is_empty());
    assert_eq!(crate::read_store_name(store.directory()).await.unwrap(), "Household");
    assert_eq!(crate::discarded_store_name(store.database_path()).await.unwrap(), "Household");
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
        path.parent().unwrap().parent().unwrap().parent().unwrap().to_owned(),
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
                .sql_functions(SqlFunctions::default().collation(
                    "reversed",
                    1,
                    |left: &str, right: &str| {
                        // Opening may compare the stored titles; only
                        // the new one panics.
                        assert!(left != "bananas" && right != "bananas", "the comparison panicked");
                        right.cmp(left)
                    },
                ))
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
            .builder(notes(), vec![Migration::addition(1, "notes", ORDERED_NOTES)])
            .sql_functions(reversed())
            .open()
    };
    let database = runtime.block_on(open()).unwrap();
    runtime.block_on(sql(&database, "INSERT INTO notes VALUES ('1','apples')")).unwrap();
    let before = crate::tests::contents(&database);
    runtime.block_on(database.close()).unwrap();

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sql_functions::tests::panicking_collation_writer", "--nocapture"])
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

/// A Unicode lowercase, unlike SQLite's `lower`, which folds ASCII only.
fn fold() -> SqlFunctions {
    SqlFunctions::default().scalar(
        "fold",
        1,
        Determinism::Deterministic { version: 1 },
        |arguments| Ok(arguments.get::<String>(0)?.to_lowercase()),
    )
}

const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)";

/// A synced table whose index, CHECK and view call the app's function.
const FOLDED_NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, CHECK (fold(title) <> 'banned')); CREATE INDEX notes_folded ON notes(fold(title)); CREATE VIEW folded_titles AS SELECT fold(title) AS title FROM notes";

const MATCHING: &str = "SELECT title FROM notes WHERE fold(title) = 'école' ORDER BY id";

/// SQLite's message for a statement it refused.
fn sqlite_message(error: DbError) -> String {
    match error {
        DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message)))
        | DbError::Sqlite(rusqlite::Error::SqlInputError { msg: message, .. }) => message,
        error => panic!("expected SQLite's refusal, received {error:?}"),
    }
}

#[tokio::test]
async fn the_apps_function_is_called_on_every_connection() {
    let store = TestStore::new();
    let migrations = || vec![Migration::addition(1, "notes", NOTES)];
    let database = store.builder(notes(), migrations()).sql_functions(fold()).open().await.unwrap();
    sql(&database, "INSERT INTO notes VALUES ('1','École'),('2','ecole'),('3','ÉCOLE')")
        .await
        .unwrap();
    // SQLite's own lower leaves É alone.
    let lower = "SELECT title FROM notes WHERE lower(title) = 'école' ORDER BY id";
    database.inspect_readers(|reader| assert!(titles(reader, lower).is_empty()));
    let expected = ["École", "ÉCOLE"];
    let read = database
        .read(|sql| Ok(sql.query(MATCHING, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    database.inspect_writer(|writer| assert_eq!(titles(writer, MATCHING), expected));
    database.inspect_readers(|reader| assert_eq!(titles(reader, MATCHING), expected));

    let reading =
        store.builder(notes(), migrations()).sql_functions(fold()).open_read_only().await.unwrap();
    let read = reading
        .read(|sql| Ok(sql.query(MATCHING, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, expected);
    reading.inspect_writer(|writer| assert_eq!(titles(writer, MATCHING), expected));
    reading.inspect_readers(|reader| assert_eq!(titles(reader, MATCHING), expected));
    reading.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn an_index_check_and_view_on_the_apps_function_hold_through_remote_writes() {
    let ids = SequentialIds::new();
    let ana = TestStore::with_ids(&ids);
    let ben = TestStore::with_ids(&ids);
    let open = |store: &TestStore| {
        store
            .builder(notes(), vec![Migration::addition(1, "notes", FOLDED_NOTES)])
            .sql_functions(fold())
            .open()
    };
    let ana_db = open(&ana).await.unwrap();
    let ben_db = open(&ben).await.unwrap();
    sql(&ana_db, "INSERT INTO notes VALUES ('1','École'),('2','Apples')").await.unwrap();
    // Ben applies Ana's write, maintaining his own index by the function.
    for record in records(&ana_db) {
        ben_db.receive(record.into()).await.unwrap();
    }
    sql(&ben_db, "INSERT INTO notes VALUES ('3','Dates')").await.unwrap();
    // Another device renames Ben's note.
    remote_update(
        &ben_db,
        "notes",
        "3",
        &[("title", coven_format::value::Value::Text("ÉCOLE".into()))],
    )
    .await;
    // The CHECK calls the function too: "BANNED" folds to the refused title.
    match sql(&ben_db, "INSERT INTO notes VALUES ('4','BANNED')").await {
        Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))) => {
            assert_eq!(failure.code, rusqlite::ErrorCode::ConstraintViolation)
        }
        result => panic!("expected the CHECK to refuse, received {result:?}"),
    }

    ben_db.inspect_readers(|reader| {
        let searched = plan(reader, MATCHING);
        assert!(searched.contains("INDEX notes_folded (<expr>=?)"), "{searched}");
        assert_eq!(titles(reader, MATCHING), ["École", "ÉCOLE"]);
        // Views call only functions marked safe for them.
        assert_eq!(
            titles(reader, "SELECT title FROM folded_titles ORDER BY title"),
            ["apples", "école", "école"]
        );
    });
    ben_db.inspect_writer(|writer| {
        let check: Vec<String> =
            writer.query("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
        assert_eq!(check, ["ok"]);
    });
    ben_db.close().await.unwrap();
    ana_db.close().await.unwrap();
}

#[tokio::test]
async fn a_migration_needs_the_function_and_its_determinism() {
    let migrations = || {
        vec![
            Migration::addition(1, "notes", NOTES),
            Migration::addition(
                2,
                "words",
                "CREATE TABLE words(word TEXT NOT NULL); CREATE INDEX words_folded ON words(fold(word))",
            ),
        ]
    };
    let store = TestStore::new();
    let database = store.builder(notes(), migrations()).sql_functions(fold()).open().await.unwrap();
    database.close().await.unwrap();

    // Without the registration, SQLite can't build the index.
    let store = TestStore::new();
    let error = store
        .builder(notes(), migrations())
        .open()
        .await
        .err()
        .expect("the index needs its function");
    assert_eq!(sqlite_message(database_error(error)), "no such function: fold");

    // An index can't hold what a nondeterministic function returns.
    let store = TestStore::new();
    let varying =
        SqlFunctions::default().scalar("fold", 1, Determinism::Nondeterministic, |arguments| {
            Ok(arguments.get::<String>(0)?.to_lowercase())
        });
    let error = store
        .builder(notes(), migrations())
        .sql_functions(varying)
        .open()
        .await
        .err()
        .expect("the index needs a deterministic function");
    assert_eq!(
        sqlite_message(database_error(error)),
        "non-deterministic functions prohibited in index expressions"
    );
}

#[tokio::test]
async fn the_regexp_operator_calls_the_apps_regexp() {
    // SQLite leaves `regexp` for the app to define, so it isn't refused.
    let store = TestStore::new();
    let database = store
        .builder(notes(), vec![Migration::addition(1, "notes", NOTES)])
        .sql_functions(SqlFunctions::default().scalar(
            "regexp",
            2,
            Determinism::Deterministic { version: 1 },
            |arguments| {
                let pattern = arguments.get::<String>(0)?;
                Ok(arguments.get::<String>(1)?.starts_with(pattern.as_str()))
            },
        ))
        .open()
        .await
        .unwrap();
    sql(&database, "INSERT INTO notes VALUES ('1','apples'),('2','bananas')").await.unwrap();
    let read = database
        .read(|sql| {
            Ok(sql.query("SELECT title FROM notes WHERE title REGEXP 'ban'", [], |row| {
                row.get::<_, String>(0)
            })?)
        })
        .await
        .unwrap();
    assert_eq!(read, ["bananas"]);
    database.close().await.unwrap();
}

#[tokio::test]
async fn sqlites_and_covens_names_and_a_repeated_name_are_refused() {
    for name in [
        "lower",
        "LIKE",
        "json_extract",
        "coven_applying",
        "_coven_loss_order",
        "coven_title",
        "_Coven_title",
    ] {
        let store = TestStore::new();
        let error = store
            .builder(notes(), vec![])
            .sql_functions(SqlFunctions::default().scalar(
                name,
                1,
                Determinism::Deterministic { version: 1 },
                |arguments| arguments.get::<String>(0),
            ))
            .open()
            .await
            .err()
            .expect("a name SQLite or coven has is refused");
        match database_error(error) {
            DbError::ReservedFunction { name: refused } => assert_eq!(refused, name),
            error => panic!("{name}: expected ReservedFunction, received {error:?}"),
        }
        store.assert_writer_unlocked();
    }
    let store = TestStore::new();
    let error = store
        .builder(notes(), vec![])
        .sql_functions(fold().scalar(
            "FOLD",
            2,
            Determinism::Deterministic { version: 1 },
            |arguments| arguments.get::<String>(0),
        ))
        .open()
        .await
        .err()
        .expect("a repeated name is refused");
    match database_error(error) {
        DbError::DuplicateFunction { name } => assert_eq!(name, "FOLD"),
        error => panic!("expected DuplicateFunction, received {error:?}"),
    }
    store.assert_writer_unlocked();
}

/// Refuses one title, so a write holding it fails wherever the index
/// computes it.
fn refusing() -> SqlFunctions {
    SqlFunctions::default().scalar(
        "fold",
        1,
        Determinism::Deterministic { version: 1 },
        |arguments| {
            let title = arguments.get::<String>(0)?;
            if title == "poison" {
                return Err(rusqlite::Error::UserFunctionError("fold refuses poison".into()));
            }
            Ok(title.to_lowercase())
        },
    )
}

#[tokio::test]
async fn a_functions_error_fails_the_write_or_apply_and_commits_nothing() {
    let store = TestStore::new();
    let database = store
        .builder(
            notes(),
            vec![Migration::addition(
                1,
                "notes",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL); CREATE INDEX notes_folded ON notes(fold(title))",
            )],
        )
        .sql_functions(refusing())
        .open()
        .await
        .unwrap();
    sql(&database, "INSERT INTO notes VALUES ('1','apples')").await.unwrap();
    // A remote write that applies, so the next one finds this device's own
    // writes already published.
    remote_update(
        &database,
        "notes",
        "1",
        &[("title", coven_format::value::Value::Text("pears".into()))],
    )
    .await;
    let before = crate::tests::contents(&database);
    let refused = sql(&database, "INSERT INTO notes VALUES ('2','poison')").await;
    assert_eq!(sqlite_message(refused.unwrap_err()), "fold refuses poison");
    assert_eq!(crate::tests::contents(&database), before);
    // Another device's write computes the same index entry when it applies.
    let applied = try_remote_update(
        &database,
        "notes",
        "1",
        &[("title", coven_format::value::Value::Text("poison".into()))],
    )
    .await;
    assert_eq!(sqlite_message(applied.unwrap_err()), "fold refuses poison");
    assert_eq!(crate::tests::contents(&database), before);
    database.close().await.unwrap();
}

/// Reading a discarded database's journal, or a store's name, calls
/// nothing of the app's, so it needs none of the app's functions.
#[tokio::test]
async fn reading_a_discarded_database_needs_none_of_the_apps_functions() {
    let store = TestStore::new();
    let database = store
        .builder(notes(), vec![Migration::addition(1, "notes", FOLDED_NOTES)])
        .sql_functions(fold())
        .open()
        .await
        .unwrap();
    database.initialize_store_name("Household".into()).await.unwrap();
    sql(&database, "INSERT INTO notes VALUES ('1','École')").await.unwrap();
    database.close().await.unwrap();

    let operations = crate::discarded_operations(store.database_path()).await.unwrap();
    assert!(operations.is_empty());
    assert_eq!(crate::read_store_name(store.directory()).await.unwrap(), "Household");
    assert_eq!(crate::discarded_store_name(store.database_path()).await.unwrap(), "Household");
}

/// The child process of the test below: a write whose index computation
/// panics.
#[test]
fn panicking_function_writer() {
    let Ok(path) = std::env::var("COVEN_FUNCTION_PANIC_FILE") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let layout = coven_foundation::files::StoreDirectories::new(
        path.parent().unwrap().parent().unwrap().parent().unwrap().to_owned(),
    );
    let store = coven_foundation::id_source::StoreId(
        uuid::Uuid::parse_str(&std::env::var("COVEN_FUNCTION_PANIC_STORE").unwrap()).unwrap(),
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = runtime
        .block_on(
            crate::DatabaseBuilder::new(layout.store_dir(&store))
                .synced_tables(notes())
                .migrations(vec![Migration::addition(1, "notes", FOLDED_NOTES)])
                .sql_functions(SqlFunctions::default().scalar(
                    "fold",
                    1,
                    Determinism::Deterministic { version: 1 },
                    |arguments| {
                        let title = arguments.get::<String>(0)?;
                        // Opening may fold the stored title; only the
                        // new one panics.
                        assert!(title != "bananas", "the function panicked");
                        Ok(title.to_lowercase())
                    },
                ))
                .open(),
        )
        .unwrap();
    let written = runtime.block_on(sql(&database, "INSERT INTO notes VALUES ('2','bananas')"));
    panic!("the write returned {written:?}");
}

#[test]
fn a_panicking_function_ends_the_process_before_its_write_commits() {
    let store = TestStore::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let open = || {
        store
            .builder(notes(), vec![Migration::addition(1, "notes", FOLDED_NOTES)])
            .sql_functions(fold())
            .open()
    };
    let database = runtime.block_on(open()).unwrap();
    runtime.block_on(sql(&database, "INSERT INTO notes VALUES ('1','apples')")).unwrap();
    let before = crate::tests::contents(&database);
    runtime.block_on(database.close()).unwrap();

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sql_functions::tests::panicking_function_writer", "--nocapture"])
        .env("COVEN_FUNCTION_PANIC_FILE", store.database_path())
        .env("COVEN_FUNCTION_PANIC_STORE", store.id().to_string())
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("the function panicked"), "{stderr}");
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

/// A store whose planner reads an analyzed index, with a function of no
/// arguments the app's SQL compares that index to.
async fn analyzed_words(
    store: &TestStore,
    determinism: Determinism,
) -> crate::CovenResult<crate::Database> {
    store
        .builder(
            notes(),
            vec![
                Migration::addition(1, "notes", NOTES),
                Migration::addition(
                    2,
                    "words",
                    "CREATE TABLE words(word TEXT NOT NULL); CREATE INDEX words_word ON words(word); INSERT INTO words VALUES ('a'),('b'),('c'); ANALYZE words",
                ),
            ],
        )
        .sql_functions(
            SqlFunctions::default().scalar("answer", 0, determinism, |_| Ok("b".to_owned())),
        )
        .open()
        .await
}

const ANSWERED: &str = "SELECT word FROM words WHERE word = answer()";

/// SQLite evaluates a deterministic function of no arguments while it plans
/// a statement against an analyzed index, passing a null argument array.
/// Registered, the read below ended the process (rusqlite's slice from a null
/// pointer), so the open refuses the function.
#[tokio::test]
async fn a_deterministic_function_without_arguments_is_refused() {
    let store = TestStore::new();
    match analyzed_words(&store, Determinism::Deterministic { version: 1 }).await {
        Ok(database) => {
            let read = database
                .read(|sql| Ok(sql.query(ANSWERED, [], |row| row.get::<_, String>(0))?))
                .await;
            panic!("a constant function was registered, and the read returned {read:?}");
        }
        Err(error) => match database_error(error) {
            DbError::ConstantFunction { name } => assert_eq!(name, "answer"),
            error => panic!("expected ConstantFunction, received {error:?}"),
        },
    }
    store.assert_writer_unlocked();

    // Varying, it is called only as the statement runs, like coven's own
    // coven_applying().
    let store = TestStore::new();
    let database = analyzed_words(&store, Determinism::Nondeterministic).await.unwrap();
    let read = database
        .read(|sql| Ok(sql.query(ANSWERED, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, ["b"]);
    database.close().await.unwrap();
}
