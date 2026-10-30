use super::*;
use crate::{
    file_write::tests::{complete_uploads, open, stage},
    tests::TestStore,
    Provenance,
};
use coven_foundation::files::FileName;

/// A file still queued here is whole on this device: pin progress counts
/// it complete, before and after reopening (§17.7).
#[tokio::test]
async fn queued_file_counts_whole_across_reopening() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stage(&db, b"bytes".to_vec(), true).await.unwrap();
    let file = db.file_ref(file).await.unwrap();
    let whole = PinProgress {
        files_completed: 1,
        files_total: 1,
        bytes_cached: 5,
        bytes_total: 5,
    };
    assert_eq!(db.pin_progress(vec![file.clone()]).await.unwrap(), whole);
    db.close().await.unwrap();
    let db = open(&store, Provenance::AppProvided).await;
    assert_eq!(db.pin_progress(vec![file]).await.unwrap(), whole);
    db.close().await.unwrap();
}

/// Pin answers follow the requested files' order; a file this device has no
/// record of is `None`, and a file still queued here reads as not pinned.
#[tokio::test]
async fn files_pinned_answers_in_request_order() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stage(&db, b"bytes".to_vec(), true).await.unwrap();
    let absent = coven_foundation::id_source::FileId(uuid::Uuid::from_u128(1));
    let mut live = db.subscribe_files_pinned(vec![absent, file]);
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        vec![None, Some(false)]
    );
    assert_eq!(
        db.files_pinned(vec![file, absent]).await.unwrap(),
        vec![Some(false), None]
    );
    db.close().await.unwrap();
}

/// The live answer follows the file's pin, as `coven_pins` lists it, and
/// matches the one-shot call; it reads no merge state, which a write
/// records for no live query (§4).
#[tokio::test]
async fn a_live_pin_answer_follows_coven_pins() {
    let store = TestStore::new();
    let db = open(&store, Provenance::AppProvided).await;
    let file = stage(&db, b"bytes".to_vec(), true).await.unwrap();
    complete_uploads(&db).await;
    let absent = coven_foundation::id_source::FileId(uuid::Uuid::from_u128(1));
    let mut live = db.subscribe_files_pinned(vec![file, absent]);
    assert_eq!(live.next().await.unwrap().unwrap(), [Some(false), None]);
    let stored = db.file_ref(file).await.unwrap();
    crate::FileDatabase::new(db.clone())
        .reserve_cache_file(&stored, FileName::new("whole").unwrap())
        .await
        .unwrap()
        .publish()
        .await
        .unwrap();
    assert_eq!(live.next().await.unwrap().unwrap(), [Some(true), None]);
    assert_eq!(
        db.files_pinned(vec![file, absent]).await.unwrap(),
        [Some(true), None]
    );
    db.close().await.unwrap();
}
