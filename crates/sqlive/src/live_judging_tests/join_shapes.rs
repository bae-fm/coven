//! Joins other than one column equal to another, each narrowing the joined
//! table as the equality would: `IS` between two tables.

use super::lookup::follows;
use crate::tests::TestDatabase;

/// A candidate's failures belong to its verdict by a pair of columns that
/// can hold NULL, joined by `IS`: a failure of another verdict wakes
/// nothing, and one of this verdict with a NULL run does.
#[tokio::test]
async fn an_is_join_narrows_and_matches_nulls() {
    let db = TestDatabase::open(
        "CREATE TABLE verdict(id INTEGER PRIMARY KEY, hash TEXT, run INTEGER);
        CREATE TABLE failure(id INTEGER PRIMARY KEY, hash TEXT, run INTEGER, message TEXT);
        INSERT INTO verdict VALUES (1,'h1',NULL),(2,'h2',1);
        INSERT INTO failure VALUES (1,'h1',NULL,'a'),(2,'h2',1,'b'),(3,'h1',2,'c')",
    );
    follows(&db, "SELECT f.message FROM verdict v JOIN failure f ON f.hash IS v.hash AND f.run IS v.run WHERE v.hash = 'h1' ORDER BY f.id", vec![], &[
        ("UPDATE failure SET message = 'bb' WHERE id = 2", false),
        ("UPDATE failure SET message = 'cc' WHERE id = 3", false),
        ("INSERT INTO failure VALUES (4,'h2',NULL,'d')", false),
        ("UPDATE failure SET message = 'aa' WHERE id = 1", true),
        ("UPDATE failure SET run = NULL WHERE id = 3", true),
        ("UPDATE failure SET hash = 'h2', run = 1 WHERE id = 3", true),
        ("UPDATE verdict SET run = 2 WHERE id = 1", true),
        ("UPDATE failure SET message = 'a' WHERE id = 1", false),
    ])
    .await;
    // A verdict a LEFT JOIN adds gives NULLs with no row when none joins,
    // which match a failure's NULL run: the failures narrow by their hash
    // alone.
    follows(&db, "SELECT f.message FROM failure f LEFT JOIN verdict v ON v.id = 9 WHERE f.run IS v.run AND f.hash = 'h1' ORDER BY f.id", vec![], &[
        ("UPDATE failure SET message = 'bbb' WHERE id = 2", false),
        ("UPDATE failure SET run = 5 WHERE id = 1", true),
    ])
    .await;
    db.close();
}
