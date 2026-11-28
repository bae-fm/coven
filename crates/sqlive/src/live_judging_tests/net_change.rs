//! A transaction is judged by each row's net change: a transaction whose writes
//! to a row cancel out reruns no live query for that row, and a row changed
//! more than once reruns a query as its change from before the transaction to
//! its end does (SPEC.md section 2). Each capture records the operations its
//! own way, so each test runs every cancelling pair through each of them: the
//! app's write on a device-local table and on a table whose operations it keeps
//! whole, the host's transaction under a session, a streamed transaction, and a
//! write whose host phase undoes its app phase.

use crate::tests::{next, no_values, QueryError, TestDatabase, TestError, TestOptions};
use crate::LiveQuery;

const SCHEMA: &str = "CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT, pinned INTEGER);
     INSERT INTO notes VALUES (1, 'a', 0), (2, 'b', 0)";

/// How a test's transaction reaches the database.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// The app's write, with `notes` a device-local table.
    Local,
    /// The app's write, which keeps each operation on `notes` whole.
    Kept,
    /// The host's own transaction, which a session records.
    Host,
    /// A streamed transaction.
    Stream,
}

const PATHS: [Path; 4] = [Path::Local, Path::Kept, Path::Host, Path::Stream];

fn open(path: Path) -> TestDatabase {
    let captured = match path {
        Path::Kept => ["notes".to_owned()].into(),
        _ => Default::default(),
    };
    TestDatabase::open_with(SCHEMA, TestOptions { captured, ..TestOptions::default() })
}

/// Run `sql` in one transaction along `path`.
fn run(db: &TestDatabase, path: Path, sql: &str) {
    match path {
        Path::Local | Path::Kept => db.write(sql).unwrap(),
        Path::Host => db.commit(sql),
        Path::Stream => db
            .host(|writer| writer.stream_transaction(|writer| writer.batch(sql)))
            .unwrap_or_else(|error| panic!("{sql} commits: {error}")),
    }
}

/// The first note's title, read by its key.
fn first(db: &TestDatabase) -> LiveQuery<Vec<String>, QueryError> {
    db.subscribe_rows("SELECT title FROM notes WHERE id = 1", no_values(), |r| {
        r.get::<_, String>(0)
    })
}

/// Every note's title, read with no condition.
fn every(db: &TestDatabase) -> LiveQuery<Vec<String>, QueryError> {
    db.subscribe_rows("SELECT title FROM notes ORDER BY id", no_values(), |r| r.get::<_, String>(0))
}

/// Writes whose changes to each row cancel out: an insert and a delete, an
/// update and an update back, a delete and an insert of the same row, as
/// `REPLACE` makes, and a key changed and changed back.
const CANCELLING: [&str; 5] = [
    "INSERT INTO notes VALUES (3, 'c', 0); DELETE FROM notes WHERE id = 3",
    "UPDATE notes SET title = 'z' WHERE id = 1; UPDATE notes SET title = 'a' WHERE id = 1",
    "DELETE FROM notes WHERE id = 1; INSERT INTO notes VALUES (1, 'a', 0)",
    "REPLACE INTO notes SELECT * FROM notes WHERE id = 1",
    "UPDATE notes SET id = 9 WHERE id = 1; UPDATE notes SET id = 1 WHERE id = 9",
];

#[tokio::test]
async fn cancelling_writes_rerun_no_query() {
    for path in PATHS {
        let db = open(path);
        let mut by_key = first(&db);
        let mut all = every(&db);
        assert_eq!(next(&mut by_key).await, ["a"]);
        assert_eq!(next(&mut all).await, ["a", "b"]);
        for sql in CANCELLING {
            run(&db, path, sql);
            assert!(!by_key.is_marked_for_rerun(), "{path:?}: `{sql}` reran the query by key");
            assert!(!all.is_marked_for_rerun(), "{path:?}: `{sql}` reran the query of every row");
        }
        db.close();
    }
}

/// A row changed and changed again reruns the queries that read it, which
/// read its last value.
#[tokio::test]
async fn a_row_changed_twice_reruns_with_its_last_value() {
    for path in PATHS {
        let db = open(path);
        let mut by_key = first(&db);
        let mut all = every(&db);
        assert_eq!(next(&mut by_key).await, ["a"]);
        assert_eq!(next(&mut all).await, ["a", "b"]);
        run(
            &db,
            path,
            "UPDATE notes SET title = 'y' WHERE id = 1; UPDATE notes SET title = 'x' WHERE id = 1",
        );
        assert_eq!(next(&mut by_key).await, ["x"], "{path:?}");
        assert_eq!(next(&mut all).await, ["x", "b"], "{path:?}");
        // A row inserted and then changed is an insert of its last value.
        run(
            &db,
            path,
            "INSERT INTO notes VALUES (3, 'c', 0); UPDATE notes SET title = 'd' WHERE id = 3",
        );
        assert!(!by_key.is_marked_for_rerun(), "{path:?}");
        assert_eq!(next(&mut all).await, ["x", "b", "d"], "{path:?}");
        // A column changed back beside one that changed reruns a query that
        // reads the one that changed.
        run(&db, path, "UPDATE notes SET title = 'w', pinned = 1 WHERE id = 1; UPDATE notes SET pinned = 0 WHERE id = 1");
        assert_eq!(next(&mut by_key).await, ["w"], "{path:?}");
        assert_eq!(next(&mut all).await, ["w", "b", "d"], "{path:?}");
        db.close();
    }
}

/// A column changed and changed back, beside a column that ends changed,
/// reruns no query that reads only the first column.
#[tokio::test]
async fn a_column_changed_back_reruns_no_query_of_it_alone() {
    for path in PATHS {
        let db = open(path);
        let mut all = every(&db);
        assert_eq!(next(&mut all).await, ["a", "b"]);
        run(&db, path, "UPDATE notes SET title = 'z', pinned = 1 WHERE id = 1; UPDATE notes SET title = 'a' WHERE id = 1");
        assert!(!all.is_marked_for_rerun(), "{path:?}");
        db.close();
    }
}

/// A write's host phase that undoes what its app phase did leaves the rows
/// as they were, so it reruns no query.
#[tokio::test]
async fn a_host_phase_that_undoes_the_app_phase_reruns_no_query() {
    for captured in [false, true] {
        let db = open(if captured { Path::Kept } else { Path::Local });
        let mut by_key = first(&db);
        let mut all = every(&db);
        assert_eq!(next(&mut by_key).await, ["a"]);
        assert_eq!(next(&mut all).await, ["a", "b"]);
        let phases = |app: &'static str, host: &'static str| {
            db.host(|writer| {
                writer.write_with(
                    |writer, _operations| {
                        let _write = writer.write_scope();
                        writer.app_batch(app).map_err(TestError::from)
                    },
                    |writer, _operations, ()| writer.batch(host),
                )
            })
            .unwrap();
        };
        phases(
            "UPDATE notes SET title = 'z' WHERE id = 1",
            "UPDATE notes SET title = 'a' WHERE id = 1",
        );
        phases("INSERT INTO notes VALUES (3, 'c', 0)", "DELETE FROM notes WHERE id = 3");
        assert!(!by_key.is_marked_for_rerun(), "captured: {captured}");
        assert!(!all.is_marked_for_rerun(), "captured: {captured}");
        phases(
            "UPDATE notes SET title = 'z' WHERE id = 1",
            "UPDATE notes SET title = 'y' WHERE id = 1",
        );
        assert_eq!(next(&mut by_key).await, ["y"], "captured: {captured}");
        assert_eq!(next(&mut all).await, ["y", "b"], "captured: {captured}");
        db.close();
    }
}

/// FTS5 rewrites a row an UPDATE names as a delete and an insert. An update
/// that changes no column reruns no query of the table.
#[tokio::test]
async fn an_fts5_rewrite_that_changes_nothing_reruns_no_query() {
    let db = TestDatabase::open(
        "CREATE VIRTUAL TABLE search USING fts5(title, body);
         INSERT INTO search(rowid, title, body) VALUES (1, 'a', 'x'), (2, 'b', 'y')",
    );
    let mut titles =
        db.subscribe_rows("SELECT title FROM search ORDER BY rowid", no_values(), |r| {
            r.get::<_, String>(0)
        });
    assert_eq!(next(&mut titles).await, ["a", "b"]);
    db.write("UPDATE search SET title = title WHERE rowid = 1").unwrap();
    assert!(!titles.is_marked_for_rerun());
    db.write("UPDATE search SET body = 'z' WHERE rowid = 1").unwrap();
    assert!(!titles.is_marked_for_rerun());
    db.write("UPDATE search SET title = 'c' WHERE rowid = 1").unwrap();
    assert_eq!(next(&mut titles).await, ["c", "b"]);
    db.close();
}
