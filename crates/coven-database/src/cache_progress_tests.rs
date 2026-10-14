use super::*;
use crate::{
    file_write::tests::{attach, tables, SCHEMA},
    tests::TestStore,
    FileDatabase, Provenance,
};

/// Eager reservations persist with their cancellation; a new request
/// replaces a cancelled set, and a finished set retires together (§16.4).
#[tokio::test]
async fn eager_reservations_and_cancellation_survive_reopening() {
    let store = TestStore::new();
    let open = || store.schema(tables(Provenance::AppProvided), SCHEMA);
    let db = open().await.unwrap();
    assert_eq!(
        db.eager_cache_fill_status().await.unwrap(),
        EagerCacheFillStatus::Idle
    );
    attach(&db, b"bytes".to_vec(), true).await.unwrap();
    let file = db.file_ref("files", "7").await.unwrap();
    let owner = FileDatabase::new(db.clone());
    // Still queued here, the file is wholly local: its coverage is complete.
    let whole = PinProgress {
        files_completed: 1,
        files_total: 1,
        bytes_cached: 5,
        bytes_total: 5,
    };
    owner.request_eager(vec![file.clone()]).await.unwrap();
    assert_eq!(
        db.eager_cache_fill_status().await.unwrap(),
        EagerCacheFillStatus::Downloading(whole.clone())
    );
    owner.cancel_eager().await.unwrap();
    db.close().await.unwrap();
    let db = open().await.unwrap();
    let owner = FileDatabase::new(db.clone());
    assert_eq!(
        db.eager_cache_fill_status().await.unwrap(),
        EagerCacheFillStatus::Cancelled(whole.clone())
    );
    // A cancelled set never retires; a new request starts a fresh set.
    assert!(!owner.retire_eager().await.unwrap());
    owner.request_eager(vec![file.clone()]).await.unwrap();
    assert_eq!(
        db.eager_cache_fill_status().await.unwrap(),
        EagerCacheFillStatus::Downloading(whole)
    );
    assert!(owner.retire_eager().await.unwrap());
    assert_eq!(
        db.eager_cache_fill_status().await.unwrap(),
        EagerCacheFillStatus::Idle
    );
    // Cancelling with nothing requested records nothing.
    owner.cancel_eager().await.unwrap();
    assert_eq!(
        db.eager_cache_fill_status().await.unwrap(),
        EagerCacheFillStatus::Idle
    );
    db.close().await.unwrap();
}

/// Pin answers follow the requested keys' order; a key without a row or
/// file is `None`, and a file still queued here reads as not pinned.
#[tokio::test]
async fn rows_pinned_answers_in_key_order() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"bytes".to_vec(), true).await.unwrap();
    let mut live = db.subscribe_rows_pinned("files", vec!["absent".into(), "7".into()]);
    assert_eq!(live.next().await.unwrap(), vec![None, Some(false)]);
    assert_eq!(
        db.rows_pinned("files", vec!["7".into(), "absent".into()])
            .await
            .unwrap(),
        vec![Some(false), None]
    );
    db.close().await.unwrap();
}
