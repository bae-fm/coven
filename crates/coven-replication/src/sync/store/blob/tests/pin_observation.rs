//! What a host sees while and after a pin runs.

use super::*;

/// A pin of a whole release reports its way to done: every report counts the
/// blobs kept so far and the stored bytes those blobs and the downloads in
/// flight have brought over, never going backwards, and the last one is the
/// whole set.
#[tokio::test]
async fn pin_reports_progress_until_every_blob_is_kept() {
    let db_store_dir = crate::sync::test_helpers::test_store_dir();
    let db = read_test_db_with_download_limit(db_store_dir.clone(), "audio", 1);
    let home = crate::sync::test_helpers::test_cloud_home();
    let (storage, cloud_storage) = create_store(&db, db_store_dir.clone(), home.clone()).await;
    let (blobs, _bytes) = prepare_exact_remote_blobs(&storage)
        .await
        .install_many(&db, 3)
        .await;
    let stored_total: u64 = blobs
        .iter()
        .map(|blob| blob.stored().expect("remote blob").object().stored_size())
        .sum();
    let reports = std::sync::Mutex::new(Vec::new());

    crate::sync::test_owner_graph::TestOwnerGraph::new(StoreDatabase::new(&db), db_store_dir)
        .pin_blobs(Some(cloud_storage.clone()), &blobs, &|progress| {
            reports.lock().unwrap().push(progress);
        })
        .await
        .expect("pin every blob");

    let reports = reports.into_inner().unwrap();
    assert!(!reports.is_empty());
    assert!(reports.windows(2).all(|pair| {
        pair[0].blobs_pinned <= pair[1].blobs_pinned && pair[0].bytes_pinned <= pair[1].bytes_pinned
    }));
    assert!(reports
        .iter()
        .all(|report| report.blobs_total == 3 && report.bytes_total == stored_total));
    let last = reports.last().unwrap();
    assert_eq!(
        (last.blobs_pinned, last.bytes_pinned),
        (3, stored_total),
        "{reports:?}"
    );
}
