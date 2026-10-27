use super::*;
use crate::{
    file_write::tests::{attach, tables, SCHEMA},
    tests::TestStore,
    Provenance,
};

/// A file still queued here is whole on this device: pin progress counts
/// it complete, before and after reopening (§17.6).
#[tokio::test]
async fn queued_file_counts_whole_across_reopening() {
    let store = TestStore::new();
    let open = || store.schema(tables(Provenance::AppProvided), SCHEMA);
    let db = open().await.unwrap();
    attach(&db, b"bytes".to_vec(), true).await.unwrap();
    let file = db.file_ref("files", "7").await.unwrap();
    let whole = PinProgress {
        files_completed: 1,
        files_total: 1,
        bytes_cached: 5,
        bytes_total: 5,
    };
    assert_eq!(db.pin_progress(vec![file.clone()]).await.unwrap(), whole);
    db.close().await.unwrap();
    let db = open().await.unwrap();
    assert_eq!(db.pin_progress(vec![file]).await.unwrap(), whole);
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
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        vec![None, Some(false)]
    );
    assert_eq!(
        db.rows_pinned("files", vec!["7".into(), "absent".into()])
            .await
            .unwrap(),
        vec![Some(false), None]
    );
    db.close().await.unwrap();
}

/// The live answer follows the row's pin, as `coven_pins` lists it, and
/// matches the one-shot call; it reads no merge state, which a write
/// records for no live query (§4).
#[tokio::test]
async fn a_live_pin_answer_follows_coven_pins() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"bytes".to_vec(), true).await.unwrap();
    let mut live = db.subscribe_rows_pinned("files", vec!["7".into(), "absent".into()]);
    assert_eq!(live.next().await.unwrap().unwrap(), [Some(false), None]);
    let file = db.file_ref("files", "7").await.unwrap();
    crate::FileDatabase::new(db.clone())
        .pin_complete(&file)
        .await
        .unwrap();
    assert_eq!(live.next().await.unwrap().unwrap(), [Some(true), None]);
    assert_eq!(
        db.rows_pinned("files", vec!["7".into(), "absent".into()])
            .await
            .unwrap(),
        [Some(true), None]
    );
    db.close().await.unwrap();
}
