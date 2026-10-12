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
    db.prepare_store_log(entry.author, entry.change, |_, _, _| {
        Ok::<_, DbError>(StoreLogSealing {
            key: KeyId(uuid::Uuid::from_u128(1)),
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
    assert!(matches!(
        db.prepare_store_log(entry.author.clone(), entry.change.clone(), |_, _, _| Err::<
            StoreLogSealing,
            _,
        >(
            DbError::StoreClosed
        ))
        .await,
        Err(DbError::StoreClosed)
    ));
    assert!(db.local_store_log().await.unwrap().upload.is_none());
    let id = db
        .prepare_store_log(
            entry.author.clone(),
            entry.change.clone(),
            |_, _, mut copies| {
                copies.push(|_| Ok::<_, DbError>(vec![3, 4]))?;
                Ok::<_, DbError>(StoreLogSealing {
                    key: KeyId(uuid::Uuid::from_u128(1)),
                    keys: copies.into_uploads(),
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(id.number, 1);
    let fixed = db.local_store_log().await.unwrap().upload;
    assert_eq!(
        fixed.as_ref().unwrap().format,
        coven_format::FormatVersion::V1
    );
    assert!(matches!(
        db.prepare_store_log(entry.author, entry.change, |_, _, _| panic!(
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
async fn applying_entry_and_retiring_the_queue_roll_back_together() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = coven_format::test_utils::store_log();
    let id = db
        .prepare_store_log(entry.author, entry.change, |_, _, mut copies| {
            copies.push(|_| Ok::<_, DbError>(vec![2]))?;
            Ok::<_, DbError>(StoreLogSealing {
                key: KeyId(uuid::Uuid::from_u128(1)),
                keys: copies.into_uploads(),
            })
        })
        .await
        .unwrap();
    let pending = db.local_store_log().await.unwrap().upload.unwrap();
    for copy in pending.sealing.keys {
        db.complete_key_upload(copy).await.unwrap();
    }
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
    db.inspect_writer(|sql| {
        sql.fail_at(
            "reject_retirement",
            "BEFORE DELETE ON _coven_store_log_uploads",
            "retirement failed",
        )
    });
    assert!(matches!(
        db.apply_store_log(checked.clone(), result.clone()).await,
        Err(DbError::Sqlite(_))
    ));
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
            sql.query_row("SELECT count(*) FROM _coven_key_uploads", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        )
    });
}

#[tokio::test]
async fn entry_copies_and_operation_commit_or_roll_back_as_one_reservation() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let historical = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![1]))
        .await
        .unwrap();
    let operation = db
        .start_operation(crate::NewOperation {
            kind: "copy test".into(),
            data: vec![],
            started_by: "test".into(),
        })
        .await
        .unwrap();
    let entry = coven_format::test_utils::store_log();
    let reserve = |copies: crate::KeyUploadBatch, operation| {
        let mut copies = copies;
        copies.push(|_| Ok::<_, DbError>(vec![2]))?;
        copies.push(|_| Ok::<_, DbError>(vec![3]))?;
        Ok::<_, DbError>((
            StoreLogSealing {
                key: KeyId(uuid::Uuid::from_u128(1)),
                keys: copies.into_uploads(),
            },
            crate::OperationUpdate {
                id: operation,
                previous: 0,
                last_step: 1,
                data: vec![4],
            },
        ))
    };
    db.inspect_writer(|sql| {
        sql.fail_at(
            "reject_step",
            "BEFORE UPDATE ON _coven_operations",
            "step failed",
        )
    });
    assert!(db
        .prepare_operation_entry(
            entry.author.clone(),
            entry.change.clone(),
            move |_, _, copies| reserve(copies, operation)
        )
        .await
        .is_err());
    assert!(db.local_store_log().await.unwrap().upload.is_none());
    assert_eq!(
        db.key_uploads().await.unwrap(),
        std::slice::from_ref(&historical)
    );
    assert_eq!(db.operation(operation).await.unwrap().unwrap().last_step, 0);
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER reject_step").unwrap());
    let id = db
        .prepare_operation_entry(entry.author, entry.change, move |_, _, copies| {
            reserve(copies, operation)
        })
        .await
        .unwrap();
    let copies = db.key_uploads().await.unwrap();
    assert_eq!(
        copies
            .iter()
            .map(|copy| copy.number.get())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(copies[0].before_entry, None);
    assert!(copies[1..].iter().all(|copy| copy.before_entry == Some(id)));
    db.finish_operation(operation).await.unwrap();
    assert_eq!(db.key_uploads().await.unwrap(), copies);
    let upload = db.local_store_log().await.unwrap().upload.unwrap();
    let checked = crate::ReplayEntry {
        entry: upload.entry,
        check: crate::StoreLogCheck::Allowed,
    };
    let replay = crate::StoreLogReplay {
        entries: [(id, crate::EntryOutcome::Kept)].into(),
        ..Default::default()
    };
    assert!(db
        .apply_store_log(checked.clone(), replay.clone())
        .await
        .is_err());
    assert!(db.local_store_log().await.unwrap().log.entries.is_empty());
    for copy in copies {
        db.complete_key_upload(copy).await.unwrap();
    }
    db.apply_store_log(checked, replay).await.unwrap();
    assert!(db.local_store_log().await.unwrap().upload.is_none());
    let next = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![5]))
        .await
        .unwrap();
    assert_eq!(next.number.get(), 4);
}

#[tokio::test]
async fn a_sealing_callback_cannot_commit_a_gap() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = coven_format::test_utils::store_log();
    assert!(matches!(
        db.prepare_store_log(entry.author, entry.change, |_, _, mut copies| {
            copies.push(|_| Ok::<_, DbError>(vec![1]))?;
            copies.push(|_| Ok::<_, DbError>(vec![2]))?;
            let mut keys = copies.into_uploads();
            keys.remove(0);
            Ok::<_, DbError>(StoreLogSealing {
                key: KeyId(uuid::Uuid::from_u128(1)),
                keys,
            })
        })
        .await,
        Err(DbError::KeyUploadChanged { .. })
    ));
    assert!(db.key_uploads().await.unwrap().is_empty());
    assert!(db.local_store_log().await.unwrap().upload.is_none());
}
