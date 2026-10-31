use crate::tests::TestStore;
use crate::Database;

async fn blobs(store: &TestStore) -> Database {
    let db = store
        .schema(
            Vec::new(),
            "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
        )
        .await
        .unwrap();
    db.write(|sql| {
        sql.execute(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<200)
             INSERT INTO blobs(body) SELECT randomblob(4000) FROM n",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    db
}

/// Each connection's cached bytes: the readers', the input database's, the
/// checkpointers' and the writer's.
fn cache_used(db: &Database) -> Vec<i32> {
    let mut used = Vec::new();
    db.inspect_connections(|connection| used.push(connection.cache_used().unwrap()));
    used
}

/// One release, on one reader, empties every connection's page cache: SQLite
/// as coven builds it keeps the unused pages of every connection in one pool
/// (§4). A SQLite built without `SQLITE_ENABLE_MEMORY_MANAGEMENT` keeps a
/// pool per connection, and only the reader's would empty. Any release, by
/// any test in the process, empties the pool too; so each connection's cache
/// is measured as it fills, and what the release leaves is what a second one
/// leaves.
#[tokio::test]
async fn one_release_empties_every_connections_page_cache() {
    let store = TestStore::new();
    let db = blobs(&store).await;
    // Fill each connection's cache with every page of its database.
    let mut before = Vec::new();
    db.inspect_connections(|connection| {
        let check: String = connection
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(check, "ok");
        before.push(connection.cache_used().unwrap());
    });
    // Four readers, the input database's reader and writer, its checkpointer
    // and the store's, and the writer.
    assert_eq!(before.len(), 9);
    db.release_memory().await.unwrap();
    let after = cache_used(&db);
    for (connection, (before, after)) in before.iter().zip(&after).enumerate() {
        assert!(
            after < before,
            "connection {connection} held {before} bytes, and {after} after releasing"
        );
    }
    db.release_memory().await.unwrap();
    assert_eq!(cache_used(&db), after, "the release left nothing to free");
    // The store's own connections held the table's 200 blobs, and give back
    // at least as much.
    let released: i32 = before.iter().zip(&after).map(|(b, a)| b - a).sum();
    assert!(released > 5 * 200 * 4000, "released {released} bytes");
    // Reads load what they need again.
    let count: i64 = db
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(count, 200);
    db.close().await.unwrap();
}

/// A release while a write holds the writer returns without waiting for it:
/// it runs on a reader (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_memory_never_waits_for_a_write() {
    let store = TestStore::new();
    let db = blobs(&store).await;
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
    let writing = tokio::spawn({
        let db = db.clone();
        async move {
            db.write(move |sql| {
                sql.execute("UPDATE blobs SET body=randomblob(4000)", [])?;
                entered_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                Ok(())
            })
            .await
        }
    });
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    // The write holds the writer until `finish_tx` sends, which comes only
    // after the release returns; a release that waited for it would fail
    // here.
    tokio::time::timeout(std::time::Duration::from_secs(10), db.release_memory())
        .await
        .expect("the release returns while the write holds the writer")
        .unwrap();
    finish_tx.send(()).unwrap();
    writing.await.unwrap().unwrap();
    db.close().await.unwrap();
}
