use crate::sqlite::DatabaseConnection;
use crate::tests::{database_error, TestStore};
use crate::write::tests::sql;
use crate::{
    CovenResult, Database, DbError, Determinism, Migration, RowIdentity, SqlFunctions, SyncedTable,
};

fn notes() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

/// The synced notes, and `schema`: a device-local table, or an index on
/// the notes, which only the migration that creates them may add.
fn migrations(schema: &'static str) -> Vec<Migration> {
    vec![Migration::addition_with(1, "notes", move |context| {
        context.execute_batch(
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
        )?;
        context.execute_batch(schema)?;
        Ok(())
    })]
}

/// `ordered` at version 1 sorts text backwards; version 2 sorts it forwards.
fn ordered(version: u32) -> SqlFunctions {
    SqlFunctions::default().collation("ordered", version, move |left: &str, right: &str| {
        match version {
            1 => right.cmp(left),
            _ => left.cmp(right),
        }
    })
}

/// `fold` at version 1 lowercases; version 2 uppercases. `regexp` at
/// version 1 matches a prefix; version 2 matches anywhere.
fn folding(version: u32) -> SqlFunctions {
    let determinism = Determinism::Deterministic { version };
    SqlFunctions::default()
        .scalar("fold", 1, determinism, move |arguments| {
            let text = arguments.get::<String>(0)?;
            Ok(match version {
                1 => text.to_lowercase(),
                _ => text.to_uppercase(),
            })
        })
        .scalar("regexp", 2, determinism, move |arguments| {
            let pattern = arguments.get::<String>(0)?;
            let text = arguments.get::<String>(1)?;
            Ok(match version {
                1 => text.starts_with(pattern.as_str()),
                _ => text.contains(pattern.as_str()),
            })
        })
}

async fn open(
    store: &TestStore,
    schema: &'static str,
    functions: SqlFunctions,
) -> CovenResult<Database> {
    store.builder(notes(), migrations(schema)).sql_functions(functions).open().await
}

/// The REINDEX statements the writer ran as the store opened.
fn reindexed(database: &Database) -> Vec<String> {
    database.inspect_writer(|writer| {
        writer
            .profiled_statements()
            .into_iter()
            .map(|statement| statement.sql)
            .filter(|sql| sql.starts_with("REINDEX"))
            .collect()
    })
}

fn rows(connection: &DatabaseConnection, sql: &str) -> Vec<String> {
    connection.query(sql, [], |row| row.get(0)).unwrap()
}

fn plan(connection: &DatabaseConnection, sql: &str) -> String {
    connection
        .query(&format!("EXPLAIN QUERY PLAN {sql}"), [], |row| row.get::<_, String>(3))
        .unwrap()
        .join("; ")
}

fn integrity(connection: &DatabaseConnection) -> Vec<String> {
    rows(connection, "PRAGMA integrity_check")
}

const ORDERED_WORDS: &str = "CREATE TABLE words(word TEXT NOT NULL); CREATE INDEX words_ordered ON words(word COLLATE ordered)";

const AFTER_B: &str =
    "SELECT word FROM words WHERE word > 'b' COLLATE ordered ORDER BY word COLLATE ordered";

async fn ordered_words(store: &TestStore) {
    let database = open(store, ORDERED_WORDS, ordered(1)).await.unwrap();
    sql(&database, "INSERT INTO words VALUES ('apples'),('bananas'),('cherries'),('dates')")
        .await
        .unwrap();
    database.close().await.unwrap();
}

/// Ana's app changes the order it calls `ordered` and registers version 2.
/// Opening rebuilds the index in that order before anything reads it, so
/// the writer and the readers find the rows the new order puts after "b".
#[tokio::test]
async fn a_new_collation_version_rebuilds_its_indexes() {
    let store = TestStore::new();
    ordered_words(&store).await;

    let database = open(&store, ORDERED_WORDS, ordered(2)).await.unwrap();
    assert_eq!(reindexed(&database), ["REINDEX \"ordered\""]);
    let expected = ["bananas", "cherries", "dates"];
    database.inspect_writer(|writer| {
        assert!(plan(writer, AFTER_B).contains("INDEX words_ordered"));
        assert_eq!(rows(writer, AFTER_B), expected);
        assert_eq!(integrity(writer), ["ok"]);
    });
    database.inspect_readers(|reader| assert_eq!(rows(reader, AFTER_B), expected));
    database.close().await.unwrap();

    // The new version is recorded: the next open rebuilds nothing.
    let database = open(&store, ORDERED_WORDS, ordered(2)).await.unwrap();
    assert!(reindexed(&database).is_empty());
    database.close().await.unwrap();
}

/// The version alone decides: an order changed under the same version is
/// not rebuilt, and SQLite finds the index out of order.
#[tokio::test]
async fn an_unchanged_version_rebuilds_nothing() {
    let store = TestStore::new();
    ordered_words(&store).await;
    let database = open(&store, ORDERED_WORDS, ordered(1)).await.unwrap();
    assert!(reindexed(&database).is_empty());
    database.close().await.unwrap();

    let forwards_at_one =
        SqlFunctions::default().collation("ordered", 1, |left: &str, right: &str| left.cmp(right));
    let database = open(&store, ORDERED_WORDS, forwards_at_one).await.unwrap();
    assert!(reindexed(&database).is_empty());
    database.inspect_writer(|writer| {
        assert_ne!(rows(writer, AFTER_B), ["bananas", "cherries", "dates"]);
        assert_ne!(integrity(writer), ["ok"]);
    });
    database.close().await.unwrap();
}

const FOLDED_WORDS: &str = "CREATE TABLE words(word TEXT NOT NULL); CREATE UNIQUE INDEX words_folded ON words(fold(word)); CREATE INDEX words_matching ON words(word) WHERE word REGEXP 'an'; CREATE INDEX words_plain ON words(word, length(word))";

const FOLDED_APPLES: &str = "SELECT word FROM words WHERE fold(word) = fold('Apples')";

/// A new version of `fold` and of `regexp` rebuilds the index on `fold`,
/// which is unique but on a device-local table, and the partial index whose
/// condition calls `regexp` through its operator, and not the index that
/// calls neither.
#[tokio::test]
async fn a_new_function_version_rebuilds_exactly_the_indexes_that_call_it() {
    let store = TestStore::new();
    let database = open(&store, FOLDED_WORDS, folding(1)).await.unwrap();
    sql(&database, "INSERT INTO words VALUES ('apples'),('bananas'),('cherries')").await.unwrap();
    database.close().await.unwrap();

    let database = open(&store, FOLDED_WORDS, folding(2)).await.unwrap();
    let mut rebuilt = reindexed(&database);
    rebuilt.sort();
    assert_eq!(rebuilt, ["REINDEX main.\"words_folded\"", "REINDEX main.\"words_matching\""]);
    database.inspect_writer(|writer| {
        assert!(plan(writer, FOLDED_APPLES).contains("INDEX words_folded"));
        assert_eq!(rows(writer, FOLDED_APPLES), ["apples"]);
        // Version 2 matches "an" anywhere: "bananas" now, not "apples".
        let matching = "SELECT word FROM words INDEXED BY words_matching WHERE word REGEXP 'an'";
        assert_eq!(rows(writer, matching), ["bananas"]);
        assert_eq!(integrity(writer), ["ok"]);
    });
    database.close().await.unwrap();
}

/// A CHECK and a synced table's unique index decide which rows every
/// device keeps, and a generated column keeps what it computed, so a
/// version they use can't change: the open refuses it and changes nothing,
/// and the old version still opens.
#[tokio::test]
async fn a_version_a_check_unique_index_or_generated_column_uses_cannot_change() {
    for (schema, functions, name, table) in [
        (
            "CREATE TABLE words(word TEXT NOT NULL CHECK (fold(word) <> 'banned'))",
            folding as fn(u32) -> SqlFunctions,
            "fold",
            "words",
        ),
        (
            "CREATE TABLE words(word TEXT NOT NULL, folded TEXT AS (fold(word)) STORED)",
            folding,
            "fold",
            "words",
        ),
        (
            "CREATE TABLE words(word TEXT NOT NULL, CHECK (word REGEXP '[a-z]'))",
            folding,
            "regexp",
            "words",
        ),
        (
            "CREATE TABLE words(word TEXT NOT NULL COLLATE ordered, CHECK (word <> 'banned'))",
            ordered,
            "ordered",
            "words",
        ),
        (
            "CREATE TABLE words(word TEXT NOT NULL, CHECK (word >= 'a' COLLATE ordered))",
            ordered,
            "ordered",
            "words",
        ),
        ("CREATE UNIQUE INDEX notes_folded ON notes(fold(title))", folding, "fold", "notes"),
        (
            "CREATE UNIQUE INDEX notes_titled ON notes(id) WHERE title > 'm' COLLATE ordered",
            ordered,
            "ordered",
            "notes",
        ),
    ] {
        let store = TestStore::new();
        open(&store, schema, functions(1)).await.unwrap().close().await.unwrap();
        let error =
            open(&store, schema, functions(2)).await.err().expect("the version change is refused");
        match database_error(error) {
            DbError::VersionChangeRefused { name: refused, table: refused_table } => {
                assert_eq!((refused.as_str(), refused_table.as_str()), (name, table), "{schema}")
            }
            error => panic!("{schema}: expected VersionChangeRefused, received {error:?}"),
        }
        store.assert_writer_unlocked();
        let database = open(&store, schema, functions(1)).await.unwrap();
        assert!(reindexed(&database).is_empty(), "{schema}");
        database.close().await.unwrap();
    }
}

/// A read-only handle, such as a widget's, can't rebuild indexes, so it
/// refuses ones built with another version until a writable open rebuilds
/// them.
#[tokio::test]
async fn a_read_only_open_refuses_indexes_built_with_another_version() {
    let store = TestStore::new();
    ordered_words(&store).await;
    let read_only = |version: u32| {
        store
            .builder(notes(), migrations(ORDERED_WORDS))
            .sql_functions(ordered(version))
            .open_read_only()
    };
    match read_only(2).await.err().map(database_error) {
        Some(DbError::VersionNotRebuilt { name, built, registered }) => {
            assert_eq!((name.as_str(), built, registered), ("ordered", 1, 2))
        }
        error => panic!("expected VersionNotRebuilt, received {error:?}"),
    }
    read_only(1).await.unwrap().close().await.unwrap();

    open(&store, ORDERED_WORDS, ordered(2)).await.unwrap().close().await.unwrap();
    let reading = read_only(2).await.unwrap();
    let read = reading
        .read(|sql| Ok(sql.query(AFTER_B, [], |row| row.get::<_, String>(0))?))
        .await
        .unwrap();
    assert_eq!(read, ["bananas", "cherries", "dates"]);
    reading.close().await.unwrap();
}

/// Rebuilding a unique index in a new order that makes two of its values
/// equal fails with SQLite's constraint error, and the whole rebuild rolls
/// back: the old version still opens.
#[tokio::test]
async fn a_rebuild_a_unique_index_refuses_changes_nothing() {
    const TAGS: &str = "CREATE TABLE words(word TEXT NOT NULL UNIQUE COLLATE ordered)";
    let caseless = SqlFunctions::default().collation("ordered", 2, |left: &str, right: &str| {
        left.to_lowercase().cmp(&right.to_lowercase())
    });
    let store = TestStore::new();
    let database = open(&store, TAGS, ordered(1)).await.unwrap();
    sql(&database, "INSERT INTO words VALUES ('rock'),('Rock')").await.unwrap();
    database.close().await.unwrap();
    match open(&store, TAGS, caseless).await.err().map(database_error) {
        Some(DbError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))) => {
            assert_eq!(failure.code, rusqlite::ErrorCode::ConstraintViolation)
        }
        error => panic!("expected the unique index to refuse, received {error:?}"),
    }
    store.assert_writer_unlocked();
    let database = open(&store, TAGS, ordered(1)).await.unwrap();
    assert!(reindexed(&database).is_empty());
    database.close().await.unwrap();
}

const SCORED_WORDS: &str = "CREATE TABLE words(word TEXT NOT NULL, score REAL NOT NULL, shown TEXT AS (CAST(score AS TEXT)) VIRTUAL, kept TEXT AS (CAST(score AS TEXT)) STORED);
     CREATE INDEX words_score_text ON words(CAST(score AS TEXT));
     CREATE INDEX words_shown ON words(shown);
     CREATE INDEX words_kept ON words(kept);
     CREATE INDEX words_scored ON words(word) WHERE CAST(score AS TEXT) > '0';
     CREATE INDEX words_plain ON words(word, score)";

/// The SQLite library the store records as the one that built its indexes.
fn recorded_library(database: &Database) -> Vec<i64> {
    database.inspect_writer(|writer| {
        writer
            .query(
                "SELECT version FROM _coven_sql_versions WHERE kind='library' AND name='sqlite'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    })
}

/// A new database records the SQLite that creates its tables, in the same
/// transaction: its indexes are all built by that SQLite, so its first
/// open has nothing to rebuild.
#[test]
fn a_new_database_records_the_sqlite_that_created_it() {
    let directory = tempfile::tempdir().unwrap();
    let connection = DatabaseConnection::open(
        &directory.path().join("new.db"),
        false,
        crate::authorization::SqlAuthorization::new(&[]),
        &crate::SqliteSettings::default(),
        &SqlFunctions::default(),
    )
    .unwrap();
    connection.prepare_internal_schema().unwrap();
    let recorded: Vec<i64> = connection
        .query(
            "SELECT version FROM _coven_sql_versions WHERE kind='library' AND name='sqlite'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded, [i64::from(rusqlite::version_number())]);
}

/// Ana's app indexes each score as text. SQLite 3.53 writes a third as
/// `0.33333333333333332` where 3.52 wrote `0.333333333333333`, so an index
/// an older SQLite built can disagree with the table. Once the store records
/// an older SQLite, the next writable open rebuilds every expression index,
/// coven's own included, and no other: not an index on a stored column,
/// nor one whose condition alone is an expression. It records this
/// SQLite's version, so the open after that rebuilds nothing.
#[tokio::test]
async fn a_new_sqlite_version_rebuilds_every_expression_index() {
    let library = i64::from(rusqlite::version_number());
    let store = TestStore::new();
    let database = open(&store, SCORED_WORDS, SqlFunctions::default()).await.unwrap();
    assert_eq!(recorded_library(&database), [library]);
    sql(&database, "INSERT INTO words(word,score) VALUES ('apples',1.0/3),('bananas',2.5)")
        .await
        .unwrap();
    database.inspect_writer(|writer| {
        writer
            .internal_execute(
                "UPDATE _coven_sql_versions SET version=3052000 WHERE kind='library'",
                [],
            )
            .unwrap();
    });
    database.close().await.unwrap();

    let database = open(&store, SCORED_WORDS, SqlFunctions::default()).await.unwrap();
    assert_eq!(
        reindexed(&database),
        [
            "REINDEX main.\"_coven_write_identity\"",
            "REINDEX main.\"words_score_text\"",
            "REINDEX main.\"words_shown\"",
        ]
    );
    assert_eq!(recorded_library(&database), [library]);
    database.inspect_writer(|writer| {
        let third = "SELECT word FROM words INDEXED BY words_score_text WHERE CAST(score AS TEXT) = '0.33333333333333332'";
        assert_eq!(rows(writer, third), ["apples"]);
        assert_eq!(integrity(writer), ["ok"]);
    });
    database.close().await.unwrap();

    let database = open(&store, SCORED_WORDS, SqlFunctions::default()).await.unwrap();
    assert!(reindexed(&database).is_empty());
    database.close().await.unwrap();
}
