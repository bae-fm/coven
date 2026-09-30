//! What unpinning leaves in the evictable cache.

use super::*;

/// Unpinning moves each kept copy into the cache, and the cache holds to its
/// namespace budget afterwards: a whole release unpinned at once must not
/// leave the cache over its limit until some later cache write.
#[tokio::test]
async fn unpin_keeps_the_cache_within_its_budget() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = read_test_db(db_store_dir.clone(), "audio");
    let store_database = StoreDatabase::new(&db);
    let home = crate::sync::test_helpers::test_cloud_home();
    let (storage, cloud_storage) = create_store(&db, db_store_dir.clone(), home).await;
    let (blobs, _bytes) = prepare_exact_remote_blobs(&storage)
        .await
        .install_many(&db, 3)
        .await;
    crate::sync::test_owner_graph::TestOwnerGraph::new(
        store_database.clone(),
        db_store_dir.clone(),
    )
    .pin_blobs(Some(cloud_storage.clone()), &blobs, &|_| {})
    .await
    .expect("pin every blob");
    assert!(blobs
        .iter()
        .all(|blob| pinned_path(&db_store_dir, blob).exists()));

    // Each blob is 1,000 bytes; the budget holds one of them, not all three.
    let budget = 1_500;
    store_database
        .set_cache_budget("audio", budget)
        .await
        .expect("set budget");
    StoreBlobCache::new(store_database.clone(), db_store_dir.clone())
        .unpin(&blobs)
        .await
        .expect("unpin every blob");

    assert!(
        blobs
            .iter()
            .all(|blob| !pinned_path(&db_store_dir, blob).exists()),
        "unpin leaves nothing in the pinned folder",
    );
    let cached = db_store_dir.cache_total_bytes("audio").await;
    assert!(
        cached <= budget,
        "unpin leaves {cached} cached bytes against a {budget}-byte budget",
    );
}
