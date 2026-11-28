use crate::database::file_database::{cache, tests::stored, FileDatabase};
use crate::file_write::tests::open;
use crate::test_utils::LiveSql;
use crate::tests::TestStore;
use crate::{DbError, LiveQuery, Provenance};
use coven_foundation::files::FileName;

/// One namespace's row of `coven_cache_bytes`: its budget, cached, pinned
/// and pinning bytes.
type CacheRow = (String, i64, i64, i64, i64);

fn cache_rows() -> LiveSql<CacheRow> {
    LiveSql::of(
        "SELECT namespace,budget,cached,pinned,pinning FROM coven_cache_bytes ORDER BY namespace",
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )
}

/// The app's SQL reads each namespace's budget and cache counters, and the
/// bytes of the partial downloads that pins left in the namespace, from
/// `coven_cache_bytes`, and a live query of the view runs again as they
/// change (§17.8).
#[tokio::test]
async fn coven_cache_bytes_reads_each_namespaces_counters() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let mut rows: LiveQuery<_, DbError> = db.subscribe(cache_rows()).process(Ok);
    let budget = 1 << 30;
    assert_eq!(crate::tests::next(&mut rows).await, [("files".into(), budget, 0, 0, 0)]);
    let file = stored(&db, vec![5; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    owner.cache_range(&file, 0, 1, FileName::new("chunk").unwrap(), vec![2; 100]).await.unwrap();
    assert_eq!(crate::tests::next(&mut rows).await, [("files".into(), budget, 100, 0, 0)]);
    db.commit_writer(|sql, _| {
        cache::tests::partial(sql, "partial-a", "partial-a", 30);
        cache::tests::partial(sql, "partial-b", "partial-b", 12);
    });
    assert_eq!(crate::tests::next(&mut rows).await, [("files".into(), budget, 100, 0, 42)]);
    db.write(|sql| sql.set_cache_budget("files", 1_000)).await.unwrap();
    assert_eq!(crate::tests::next(&mut rows).await, [("files".into(), 1_000, 100, 0, 42)]);
    db.close().await.unwrap();
}

/// `set_cache_budget` refuses a budget past SQLite's largest integer and
/// leaves the budget as it was.
#[tokio::test]
async fn a_budget_past_sqlites_integers_is_refused() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let error = db.write(|sql| sql.set_cache_budget("files", u64::MAX)).await.unwrap_err();
    assert!(matches!(error, DbError::TooLarge { field: "cache budget", .. }), "{error:?}");
    assert_eq!(db.cache_bytes().await.unwrap().namespaces[0].budget, 1 << 30);
    db.close().await.unwrap();
}

/// The database file's size, read from `coven_library_bytes`.
fn database_bytes() -> LiveSql<i64> {
    LiveSql::of("SELECT bytes FROM coven_library_bytes WHERE part='database'", |r| r.get(0))
}

/// The size of the store's database file on disk.
fn file_size(store: &TestStore) -> i64 {
    let path = store.database_path();
    i64::try_from(std::fs::metadata(path).unwrap().len()).unwrap()
}

/// `coven_library_bytes` holds the database file's size as the store opens,
/// and a write that grows the file changes it only once a checkpoint has
/// moved the write into the file: a live query of it then runs once more,
/// with the file's size on disk (§17.8).
#[tokio::test]
async fn a_checkpoint_that_grows_the_database_file_writes_its_size() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let mut sizes: LiveQuery<_, DbError> =
        db.subscribe(database_bytes()).process(|bytes| Ok(bytes[0]));
    let opened = crate::tests::next(&mut sizes).await;
    assert_eq!(opened, file_size(&store));
    db.write(|sql| {
        for row in 0..64 {
            sql.execute(
                "INSERT INTO files(id,title) VALUES(?1,?2)",
                (format!("grow-{row}"), "x".repeat(16 * 1024)),
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();
    // The write is in the WAL: the file and the row keep their size.
    assert_eq!(file_size(&store), opened);
    let before = sizes.runs().runs();
    db.checkpoint(crate::CheckpointMode::Truncate).await.unwrap();
    let grown = crate::tests::next(&mut sizes).await;
    assert!(grown > opened, "{grown} > {opened}");
    assert_eq!(grown, file_size(&store));
    assert_eq!(sizes.runs().runs(), before + 1);
    db.close().await.unwrap();
}

/// The bytes of coven's copies of app files, read from `coven_library_bytes`.
fn copy_bytes() -> LiveSql<i64> {
    LiveSql::of("SELECT bytes FROM coven_library_bytes WHERE part='copies'", |r| r.get(0))
}

/// A staged copy counts among the store's own files from the write that
/// keeps it, at its size on disk, until the write that releases it, and a
/// live query of the copies' bytes runs again for each (§17.8). The upload
/// completing keeps the copy until no reset can switch the file back
/// (§17.11).
#[tokio::test]
async fn a_kept_copy_counts_until_it_is_released() {
    use crate::file_write::tests::{complete_uploads, owned_paths, stage};
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let mut copies: LiveQuery<_, DbError> =
        db.subscribe(copy_bytes()).process(|bytes| Ok(bytes[0]));
    assert_eq!(crate::tests::next(&mut copies).await, 0);
    stage(&db, vec![5; 3000], true).await.unwrap();
    let on_disk: u64 =
        owned_paths(&store).iter().map(|path| std::fs::metadata(path).unwrap().len()).sum();
    assert_eq!(on_disk, 3000);
    assert_eq!(crate::tests::next(&mut copies).await, 3000);
    complete_uploads(&db).await;
    assert!(!owned_paths(&store).is_empty(), "a reset may switch the file back");
    db.test_past_every_reset().await;
    assert_eq!(crate::tests::next(&mut copies).await, 0);
    db.close().await.unwrap();
}
