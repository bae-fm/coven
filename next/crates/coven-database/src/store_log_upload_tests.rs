use super::*;
use crate::{
    tests::TestStore,
    write::tests::{notes, records, sql, NOTES},
    Migration,
};
use coven_foundation::clock::FixedClock;
use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

#[tokio::test]
async fn local_write_follows_fixed_entry_even_after_clock_moves_back() {
    let store = TestStore::new();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(100)));
    let db = store
        .builder(notes(), vec![Migration::sql(1, "notes", NOTES)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let entry = coven_format::test_utils::store_log();
    db.prepare_store_log(entry.author, entry.change, |_, _| {
        Ok::<_, DbError>(SealedStoreLog {
            bytes: vec![1],
            keys: Vec::new(),
        })
    })
    .await
    .unwrap();
    let fixed = db
        .local_store_log()
        .await
        .unwrap()
        .upload
        .unwrap()
        .entry
        .timestamp;
    clock.set(UNIX_EPOCH);
    sql(&db, "INSERT INTO notes VALUES('a','title','body')")
        .await
        .unwrap();
    assert!(records(&db)[0].header.timestamp > fixed);
}

#[tokio::test]
async fn failed_sealing_reserves_nothing_and_pending_entry_blocks_another() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = coven_format::test_utils::store_log();
    assert!(db
        .prepare_store_log(entry.author.clone(), entry.change.clone(), |_, _| Err::<
            SealedStoreLog,
            _,
        >(
            DbError::StoreClosed
        ))
        .await
        .is_err());
    assert!(db.local_store_log().await.unwrap().upload.is_none());
    let id = db
        .prepare_store_log(entry.author.clone(), entry.change.clone(), |_, _| {
            Ok::<_, DbError>(SealedStoreLog {
                bytes: vec![1, 2],
                keys: vec![StoreLogKeyUpload {
                    path: "sealed key".into(),
                    bytes: vec![3, 4],
                }],
            })
        })
        .await
        .unwrap();
    assert_eq!(id.number, 1);
    let fixed = db.local_store_log().await.unwrap().upload;
    assert!(matches!(
        db.prepare_store_log(entry.author, entry.change, |_, _| panic!(
            "must not reseal while pending"
        ))
        .await,
        Err(DbError::StoreLogUploadPending(_))
    ));
    db.close().await.unwrap();
    let reopened = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(reopened.local_store_log().await.unwrap().upload, fixed);
}

#[tokio::test]
async fn applying_entry_and_retiring_fixed_bytes_roll_back_together() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = coven_format::test_utils::store_log();
    let id = db
        .prepare_store_log(entry.author, entry.change, |_, _| {
            Ok::<_, DbError>(SealedStoreLog {
                bytes: vec![1],
                keys: vec![StoreLogKeyUpload {
                    path: "key".into(),
                    bytes: vec![2],
                }],
            })
        })
        .await
        .unwrap();
    let pending = db.local_store_log().await.unwrap().upload.unwrap();
    let checked = crate::ReplayEntry {
        entry: pending.entry.clone(),
        check: crate::StoreLogCheck::Allowed,
    };
    // Replay is caller-supplied data at this database boundary.
    let result = crate::StoreLogReplay {
        entries: [(id, crate::EntryOutcome::Kept)].into(),
        ..Default::default()
    };
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER reject_retirement BEFORE DELETE ON coven_store_log_uploads BEGIN SELECT RAISE(ABORT,'retirement failed'); END;").unwrap());
    assert!(db
        .apply_store_log(checked.clone(), result.clone())
        .await
        .is_err());
    let state = db.local_store_log().await.unwrap();
    assert!(state.log.entries.is_empty());
    assert_eq!(state.upload, Some(pending));
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER reject_retirement").unwrap());
    db.apply_store_log(checked, result).await.unwrap();
    let state = db.local_store_log().await.unwrap();
    assert!(state.upload.is_none());
    assert_eq!(state.log.entries.len(), 1);
    db.inspect_writer(|sql| {
        assert_eq!(
            sql.query_row(
                "SELECT count(*) FROM coven_store_log_key_uploads",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        )
    });
}
