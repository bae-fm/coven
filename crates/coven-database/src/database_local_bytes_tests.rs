use crate::database::file_database::{cache, tests::stored, FileDatabase};
use crate::file_write::tests::open;
use crate::tests::TestStore;
use crate::{DbError, LiveQuery, Provenance, SqlReadContext};
use coven_foundation::files::FileName;

/// One namespace's row of `coven_cache_bytes`: its budget, cached, pinned
/// and pinning bytes.
type CacheRow = (String, i64, i64, i64, i64);

fn cache_rows(sql: SqlReadContext<'_>) -> Result<Vec<CacheRow>, DbError> {
    Ok(sql.query(
        "SELECT namespace,budget,cached,pinned,pinning FROM coven_cache_bytes ORDER BY namespace",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )?)
}

/// The app's SQL reads each namespace's budget and cache counters, and the
/// partial downloads pins left in it, from `coven_cache_bytes`, and a live
/// query of it runs again as they change (§17.8).
#[tokio::test]
async fn coven_cache_bytes_reads_each_namespaces_counters() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let mut rows: LiveQuery<_, DbError> = db.subscribe(cache_rows).process(Ok);
    let budget = 1 << 30;
    assert_eq!(rows.next_inline().await.unwrap().unwrap(), [("files".into(), budget, 0, 0, 0)]);
    let file = stored(&db, vec![5; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    owner.cache_range(&file, 0, 1, FileName::new("chunk").unwrap(), vec![2; 100]).await.unwrap();
    assert_eq!(rows.next_inline().await.unwrap().unwrap(), [("files".into(), budget, 100, 0, 0)]);
    db.commit_writer(|sql, _| {
        cache::tests::partial(sql, "partial-a", "partial-a", 30);
        cache::tests::partial(sql, "partial-b", "partial-b", 12);
    });
    assert_eq!(rows.next_inline().await.unwrap().unwrap(), [("files".into(), budget, 100, 0, 42)]);
    db.write(|sql| sql.set_cache_budget("files", 1_000)).await.unwrap();
    assert_eq!(rows.next_inline().await.unwrap().unwrap(), [("files".into(), 1_000, 100, 0, 42)]);
    db.close().await.unwrap();
}

/// A budget past SQLite's largest integer is refused, and the budget stays
/// as it was.
#[tokio::test]
async fn a_budget_past_sqlites_integers_is_refused() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let error = db.write(|sql| sql.set_cache_budget("files", u64::MAX)).await.unwrap_err();
    assert!(matches!(error, DbError::TooLarge { field: "cache budget", .. }), "{error:?}");
    assert_eq!(db.cache_bytes().await.unwrap().namespaces[0].budget, 1 << 30);
    db.close().await.unwrap();
}
