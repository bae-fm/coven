use crate::tests::{TestDatabase, TestOptions};
use std::num::{NonZeroU32, NonZeroUsize};

/// A database of 200 blobs of 4000 bytes, with two readers, a checkpointer
/// and a thread keeping the planner's statistics beside its writer.
fn blobs() -> TestDatabase {
    let db = TestDatabase::open_with(
        "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
        TestOptions {
            readers: NonZeroUsize::new(2).unwrap(),
            checkpoint_pages: NonZeroU32::new(1000),
            statistics: true,
            ..TestOptions::default()
        },
    );
    db.commit(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<200)
         INSERT INTO blobs(body) SELECT randomblob(4000) FROM n",
    );
    db
}

/// Each connection's cached bytes: the readers', the checkpointer's, the
/// planner statistics' and the writer's.
fn cache_used(db: &TestDatabase) -> Vec<i32> {
    let mut used = Vec::new();
    db.with_database(|database| {
        database.each_connection(|connection| used.push(connection.cache_used().unwrap()))
    });
    used
}

/// One release, on one reader, empties every connection's page cache: SQLite
/// as sqlive builds it keeps the unused pages of every connection in one pool
/// (§4). A SQLite built without `SQLITE_ENABLE_MEMORY_MANAGEMENT` keeps a
/// pool per connection, and only the reader's would empty. Any release, by
/// any test in the process, empties the pool too; so each connection's cache
/// is measured as it fills, and what the release leaves is what a second one
/// leaves.
#[test]
fn one_release_empties_every_connections_page_cache() {
    let db = blobs();
    // Fill each connection's cache with every page of its database.
    let mut before = Vec::new();
    db.with_database(|database| {
        database.each_connection(|connection| {
            let check: String =
                connection.query_row("PRAGMA quick_check", [], |r| r.get(0)).unwrap();
            assert_eq!(check, "ok");
            before.push(connection.cache_used().unwrap());
        })
    });
    // Two readers, the checkpointer's, the planner statistics' and the
    // writer.
    assert_eq!(before.len(), 5);
    db.read(|reader| reader.release_memory()).unwrap();
    let after = cache_used(&db);
    for (connection, (before, after)) in before.iter().zip(&after).enumerate() {
        assert!(
            after < before,
            "connection {connection} held {before} bytes, and {after} after releasing"
        );
    }
    db.read(|reader| reader.release_memory()).unwrap();
    assert_eq!(cache_used(&db), after, "the release left nothing to free");
    // The connections held the table's 200 blobs, and give back at least as
    // much.
    let released: i32 = before.iter().zip(&after).map(|(b, a)| b - a).sum();
    assert!(released > 4 * 200 * 4000, "released {released} bytes");
    // Reads load what they need again.
    let count: i64 =
        db.read(|reader| reader.query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))).unwrap();
    assert_eq!(count, 200);
    db.close();
}
