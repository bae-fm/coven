use super::*;
use crate::pending::tests::Record as PendingRecord;
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
async fn oversized_access_reserves_neither_an_entry_nor_key_copies() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = crate::tests::store_log();
    let result = db
        .prepare_store_log(
            entry.author.clone(),
            StoreChange::SetAccess {
                member: entry.author.clone(),
                access: coven_format::MemberAccess::ProviderAccount("a".repeat(4 * 1024 * 1024)),
            },
            |_, _, _| -> Result<StoreLogSealing, DbError> {
                panic!("an oversized change cannot reach key-copy preparation")
            },
        )
        .await;
    assert!(matches!(result, Err(DbError::InvalidStoreLogEntry { .. })));
    assert!(db.local_store_log().await.unwrap().upload.is_none());
    assert!(db.key_uploads().await.unwrap().is_empty());
    let next = db
        .prepare_store_log(entry.author, entry.change, |_, _, copies| {
            Ok::<_, DbError>(StoreLogSealing {
                key: KeyId(uuid::Uuid::from_u128(1)),
                keys: copies.into_uploads(),
            })
        })
        .await
        .unwrap();
    assert_eq!(next.number, 1);
}

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
    let entry = crate::tests::store_log();
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
async fn batch_clock_uses_saved_and_queued_timestamps_without_decoding_entries() {
    use crate::store_log::tests::{checked, circle_history};
    let store = TestStore::new();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let db = store
        .builder(notes(), vec![Migration::sql(1, "notes", NOTES)])
        .clock(clock)
        .open()
        .await
        .unwrap();
    let history = circle_history(coven_foundation::id_source::CircleId(
        uuid::Uuid::from_u128(2),
    ));
    let before = db.local_store_log().await.unwrap().log;
    db.apply_store_log_batch::<PendingRecord>(
        before,
        history[..4]
            .iter()
            .map(|(entry, _)| checked(entry.clone()))
            .collect(),
        history[3].1.clone(),
        vec![],
        vec![],
        Vec::new(),
    )
    .await
    .unwrap();
    let work = crate::test_utils::WorkCounter::default();
    db.test_track_work(work.clone()).await.unwrap();
    let entry = crate::tests::store_log();
    db.prepare_store_log(
        entry.author.clone(),
        StoreChange::SetAccess {
            member: entry.author,
            access: coven_format::MemberAccess::ProviderAccount("account".into()),
        },
        move |_, entry, _| {
            assert!(entry.timestamp > history[3].0.timestamp);
            Ok::<_, DbError>(StoreLogSealing {
                key: KeyId(uuid::Uuid::from_u128(1)),
                keys: vec![],
            })
        },
    )
    .await
    .unwrap();
    assert_eq!(work.take().entry_decodes, 0);
    let fixed = db
        .local_store_log()
        .await
        .unwrap()
        .upload
        .unwrap()
        .entry
        .timestamp;
    work.take();
    sql(&db, "INSERT INTO notes VALUES('a','title','body')")
        .await
        .unwrap();
    assert_eq!(work.take().entry_decodes, 0);
    assert!(records(&db)[0].header.timestamp > fixed);
}

#[tokio::test]
async fn failed_sealing_reserves_nothing_and_pending_entry_blocks_another() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let entry = crate::tests::store_log();
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
    let (entry, mut result) = crate::store_log::tests::circle_history(
        coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(2)),
    )[0]
    .clone();
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
        stored_at: UNIX_EPOCH,
        check: crate::StoreLogCheck::Allowed,
    };
    // The supplied creation replay names the writer that reserved the entry.
    result.entries = [(id, crate::EntryOutcome::Kept)].into();
    let device = result.state.devices.pop_first().unwrap().1;
    result.state.devices.insert(id.device, device);
    db.inspect_writer(|sql| {
        sql.fail_at(
            "reject_retirement",
            "BEFORE DELETE ON _coven_store_log_uploads",
            "retirement failed",
        )
    });
    assert!(matches!(
        db.apply_store_log::<PendingRecord>(checked.clone(), result.clone(), Vec::new())
            .await,
        Err(DbError::Sqlite(_))
    ));
    let state = db.local_store_log().await.unwrap();
    assert!(state.log.entries.is_empty());
    assert_eq!(state.upload, Some(pending));
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER reject_retirement").unwrap());
    db.apply_store_log::<PendingRecord>(checked, result, Vec::new())
        .await
        .unwrap();
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
    let (entry, mut replay) = crate::store_log::tests::circle_history(
        coven_foundation::id_source::CircleId(uuid::Uuid::from_u128(2)),
    )[0]
    .clone();
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
        stored_at: UNIX_EPOCH,
        check: crate::StoreLogCheck::Allowed,
    };
    replay.entries = [(id, crate::EntryOutcome::Kept)].into();
    let device = replay.state.devices.pop_first().unwrap().1;
    replay.state.devices.insert(id.device, device);
    assert!(db
        .apply_store_log::<PendingRecord>(checked.clone(), replay.clone(), Vec::new())
        .await
        .is_err());
    assert!(db.local_store_log().await.unwrap().log.entries.is_empty());
    for copy in copies {
        db.complete_key_upload(copy).await.unwrap();
    }
    db.apply_store_log::<PendingRecord>(checked, replay, Vec::new())
        .await
        .unwrap();
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
    let entry = crate::tests::store_log();
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
