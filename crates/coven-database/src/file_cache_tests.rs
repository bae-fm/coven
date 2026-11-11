/// Record a cache entry of `size` bytes at `path`, for chunk 0 of the stored
/// object at `object_path` in the `files` namespace, last read at `read`.
pub(crate) fn cached(
    db: &crate::sqlite::DatabaseConnection,
    object_path: &str,
    path: &str,
    size: i64,
    read: i64,
) -> Result<usize, crate::DbError> {
    db.internal_execute(
        "INSERT INTO _coven_cache(namespace,object_path,chunk,path,size,last_read)
         VALUES('files',?1,0,?2,?3,?4)",
        (object_path, path, size, read),
    )
}

/// Record a cache range at `path` as being written, as a crash mid-write
/// leaves it.
pub(crate) fn writing(db: &crate::sqlite::DatabaseConnection, path: &str) {
    db.internal_execute("INSERT INTO _coven_cache_writes(path) VALUES(?1)", [path]).unwrap();
}

/// Fill this domain's tables from the rows of the plan audit's `fixture`
/// table, inside its transaction (`internal_schema_scan_reasons_tests.rs`).
pub(crate) fn fill_for_plans(db: &rusqlite::Connection) {
    db.execute_batch(
        "INSERT INTO _coven_cache(namespace,object_path,chunk,path,size,last_read,checked_hash)
           SELECT audience,'file_' || i,CASE WHEN (i/64)%4=0 THEN -2 ELSE 0 END,'cache_' || i,
                  65536,i,
                  CASE WHEN (i/64)%4=0 THEN key END FROM fixture;
         INSERT INTO _coven_cache_budgets(namespace,bytes) SELECT 'audience_' || i,counter(1048576) FROM fixture WHERE true ON CONFLICT(namespace) DO UPDATE SET bytes=excluded.bytes;
         INSERT INTO _coven_cache_writes SELECT 'writing_' || i FROM fixture;
         INSERT INTO _coven_cache_partials(namespace,object_path,path,size)
           SELECT audience,'partial_' || i,'partial_' || i,65536 FROM fixture;",
    )
    .unwrap();
}

use crate::database::file_database::{tests::stored, FileDatabase};
use crate::{
    file_write::tests::{complete_uploads, open},
    tests::TestStore,
    Database, DbError, FileRef, Provenance,
};
use coven_foundation::files::FileName;

/// A stored file of these bytes, referenced by row `8`, beside `stored`'s.
pub(crate) async fn stored_second(db: &Database, bytes: Vec<u8>) -> FileRef {
    let file = db
        .staged_write::<_, _, _, _, DbError>(
            move |batch| Ok(batch.put_file("files", bytes, crate::Place::Cloud)),
            move |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('8',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    complete_uploads(db).await;
    db.file_ref(file).await.unwrap()
}

/// A namespace's `cached` and `pinned` byte counts are the sums of its
/// chunks and headers, and of its whole copies, read from the entries
/// themselves.
fn counters(db: &Database) -> (i64, i64) {
    db.inspect_writer(|sql| {
        let recorded = (
            super::cached(sql, "files").unwrap(),
            sql.query_row(
                "SELECT pinned FROM _coven_cache_budgets WHERE namespace='files'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
        );
        let sum = |pinned: i64| -> i64 {
            sql.query_row(
                "SELECT coalesce(sum(size),0) FROM _coven_cache WHERE namespace='files' AND (chunk=-2)=?1",
                [pinned],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(recorded, (sum(0), sum(1)));
        recorded
    })
}

/// The namespace's counters change with its cache records in their
/// transaction: publication, eviction, a failed publication, pinning and
/// unpinning (Appendix C.3).
#[tokio::test]
async fn cache_counters_follow_their_records() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    owner.cache_range(&file, -1, 1, FileName::new("header").unwrap(), vec![1; 11]).await.unwrap();
    owner.cache_range(&file, 0, 1, FileName::new("chunk").unwrap(), vec![2; 100]).await.unwrap();
    assert_eq!(counters(&db), (111, 0));
    // A refused publication commits nothing, the counters included.
    assert!(owner
        .cache_range(&file, 1, 1, FileName::new("chunk").unwrap(), vec![3; 50])
        .await
        .is_err());
    assert_eq!(counters(&db), (111, 0));
    db.write(|sql| sql.set_cache_budget("files", 1_000)).await.unwrap();
    assert_eq!(counters(&db), (111, 0));
    let id = file.id();
    db.write(move |sql| sql.evict_file(id)).await.unwrap();
    assert_eq!(counters(&db), (0, 0));
    db.close().await.unwrap();
}

/// A pinned entry's bytes are counted as pinned, not as cached: a published
/// pin, then the entries trimmed and evicted around it.
#[tokio::test]
async fn pinned_bytes_are_counted_apart_from_cached_bytes() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let pinned = stored(&db, vec![5; 100]).await;
    let other = stored_second(&db, vec![6; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    let reservation =
        owner.reserve_cache_file(&pinned, FileName::new("whole").unwrap()).await.unwrap();
    std::fs::write(
        store.database_path().parent().unwrap().join("cache").join("whole"),
        b"whole file",
    )
    .unwrap();
    owner.commit_pin(owner.prepared_pin(vec![reservation], Vec::new()).unwrap()).await.unwrap();
    let (cached, pinned_bytes) = counters(&db);
    assert_eq!(cached, 0);
    assert!(pinned_bytes > 100, "an encrypted copy is larger than its plaintext");
    owner.cache_range(&other, 0, 1, FileName::new("chunk").unwrap(), vec![2; 30]).await.unwrap();
    assert_eq!(counters(&db), (30, pinned_bytes));
    db.write(|sql| sql.set_cache_budget("files", 0)).await.unwrap();
    assert_eq!(counters(&db), (0, pinned_bytes));
    let id = pinned.id();
    db.write(move |sql| sql.evict_file(id)).await.unwrap();
    assert_eq!(counters(&db), (0, 0));
    db.close().await.unwrap();
}

/// Pinned copies don't count toward the budget (§17.8): a namespace whose
/// pinned bytes are over its budget still caches unpinned bytes up to it, and
/// trims none of the pinned.
#[tokio::test]
async fn a_pinned_copy_over_the_budget_crowds_out_no_cached_bytes() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let pinned = stored(&db, vec![5; 100]).await;
    let other = stored_second(&db, vec![6; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    let reservation =
        owner.reserve_cache_file(&pinned, FileName::new("whole").unwrap()).await.unwrap();
    std::fs::write(
        store.database_path().parent().unwrap().join("cache").join("whole"),
        b"whole file",
    )
    .unwrap();
    owner.commit_pin(owner.prepared_pin(vec![reservation], Vec::new()).unwrap()).await.unwrap();
    db.write(|sql| sql.set_cache_budget("files", 40)).await.unwrap();
    assert!(counters(&db).1 > 40);
    owner.cache_range(&other, 0, 1, FileName::new("first").unwrap(), vec![2; 30]).await.unwrap();
    owner.trim_cache("files").await.unwrap();
    let names = |db: &Database| {
        db.inspect_writer(|sql| {
            sql.query("SELECT path FROM _coven_cache ORDER BY path", [], |r| r.get::<_, String>(0))
                .unwrap()
        })
    };
    assert_eq!(names(&db), ["first", "whole"]);
    // A second range takes the unpinned bytes over the budget: the older
    // goes, the pinned copy stays.
    owner.cache_range(&other, 1, 1, FileName::new("second").unwrap(), vec![3; 30]).await.unwrap();
    owner.trim_cache("files").await.unwrap();
    assert_eq!(names(&db), ["second", "whole"]);
    db.close().await.unwrap();
}

/// Unpinning a file removes its cached chunks and header as well, with no
/// pin to release (§17.8): the files' copies leave the device, and the
/// counters follow.
#[tokio::test]
async fn unpinning_removes_cached_chunks_and_headers() {
    use crate::file_write::tests::local_count;
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stored(&db, vec![5; 2 * 64 * 1024]).await;
    let owner = FileDatabase::new(db.clone());
    owner.cache_range(&file, -1, 1, FileName::new("header").unwrap(), vec![1; 11]).await.unwrap();
    owner.cache_range(&file, 0, 1, FileName::new("chunk").unwrap(), vec![2; 100]).await.unwrap();
    db.write(|sql| sql.set_cache_budget("files", 1 << 30)).await.unwrap();
    assert_eq!(counters(&db), (111, 0));
    let id = file.id();
    db.write(move |sql| sql.unpin(&[id])).await.unwrap();
    assert_eq!(local_count(&db, "_coven_cache"), 0);
    assert_eq!(counters(&db), (0, 0));
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    let cache = store.database_path().parent().unwrap().join("cache");
    assert!(!cache.join("header").exists() && !cache.join("chunk").exists());
    db.close().await.unwrap();
}

/// Clearing a namespace's cache removes its unpinned chunks and headers and
/// leaves its pinned copy and every other namespace alone; the counters
/// follow and the bytes go once the write has committed (§17.8).
#[tokio::test]
async fn clearing_a_namespace_removes_only_its_unpinned_entries() {
    use crate::file_write::tests::{complete_uploads, local_count, namespaces, tables, SCHEMA};
    let store = TestStore::new();
    let mut declared = namespaces(Provenance::AppProvided);
    declared.push(crate::Namespace::new("other", Provenance::AppProvided, 1 << 30));
    let db = store
        .builder(tables(), vec![crate::Migration::addition(1, "files", SCHEMA)])
        .namespaces(declared)
        .open()
        .await
        .unwrap();
    let pinned = stored(&db, vec![5; 100]).await;
    let chunked = stored_second(&db, vec![6; 2 * 64 * 1024]).await;
    let elsewhere = db
        .staged_write::<_, _, _, _, DbError>(
            |batch| Ok(batch.put_file("other", vec![7; 2 * 64 * 1024], crate::Place::Cloud)),
            |sql, file| {
                sql.execute("INSERT INTO files(id,file) VALUES('9',?1)", [file])?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    complete_uploads(&db).await;
    let elsewhere = db.file_ref(elsewhere).await.unwrap();
    let owner = FileDatabase::new(db.clone());
    let cache = store.database_path().parent().unwrap().join("cache");
    let reservation =
        owner.reserve_cache_file(&pinned, FileName::new("whole").unwrap()).await.unwrap();
    std::fs::write(cache.join("whole"), b"whole file").unwrap();
    owner.commit_pin(owner.prepared_pin(vec![reservation], Vec::new()).unwrap()).await.unwrap();
    owner
        .cache_range(&chunked, -1, 1, FileName::new("header").unwrap(), vec![1; 11])
        .await
        .unwrap();
    owner.cache_range(&chunked, 0, 1, FileName::new("chunk").unwrap(), vec![2; 30]).await.unwrap();
    owner
        .cache_range(&elsewhere, 0, 1, FileName::new("other").unwrap(), vec![3; 40])
        .await
        .unwrap();
    let (_, pinned_bytes) = counters(&db);
    assert_eq!(local_count(&db, "_coven_cache"), 4);
    let failed = db
        .write(|sql| {
            sql.clear_cache("files")?;
            Err::<(), _>(DbError::Invariant { what: "the app gives up" })
        })
        .await;
    assert!(matches!(failed, Err(DbError::Invariant { .. })));
    assert_eq!(local_count(&db, "_coven_cache"), 4);
    db.write(|sql| sql.clear_cache("files")).await.unwrap();
    let left = db.inspect_writer(|sql| {
        sql.query("SELECT path FROM _coven_cache ORDER BY path", [], |r| r.get::<_, String>(0))
            .unwrap()
    });
    assert_eq!(left, ["other", "whole"]);
    assert_eq!(counters(&db), (0, pinned_bytes));
    assert_eq!(local_count(&db, "_coven_file_removals"), 0);
    assert!(!cache.join("header").exists() && !cache.join("chunk").exists());
    assert!(cache.join("whole").exists() && cache.join("other").exists());
    assert!(matches!(
        db.write(|sql| sql.clear_cache("missing")).await,
        Err(DbError::UndeclaredNamespace { namespace }) if namespace == "missing"
    ));
    db.close().await.unwrap();
}
