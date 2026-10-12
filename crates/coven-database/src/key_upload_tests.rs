use crate::{tests::TestStore, DbError};

#[tokio::test]
async fn fixed_copy_survives_reopen_and_confirmation_preserves_the_next_number() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let fixed = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(fixed.number.get(), 1);
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(
        db.key_uploads().await.unwrap(),
        std::slice::from_ref(&fixed)
    );
    db.complete_key_upload(fixed.clone()).await.unwrap();
    db.complete_key_upload(fixed).await.unwrap();
    assert!(db.key_uploads().await.unwrap().is_empty());
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let next = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![4]))
        .await
        .unwrap();
    assert_eq!(next.number.get(), 2);
}

#[tokio::test]
async fn failed_sealing_or_insertion_consumes_no_number() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert!(matches!(
        db.prepare_key_upload(|_| Err::<Vec<u8>, _>(DbError::StoreClosed))
            .await,
        Err(DbError::StoreClosed)
    ));
    db.inspect_writer(|sql| {
        sql.fail_at(
            "reject_copy",
            "BEFORE INSERT ON _coven_key_uploads",
            "copy failed",
        )
    });
    assert!(matches!(
        db.prepare_key_upload(|_| Ok::<_, DbError>(vec![1])).await,
        Err(DbError::Sqlite(_))
    ));
    assert!(db.key_uploads().await.unwrap().is_empty());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER reject_copy").unwrap());
    let copy = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![2]))
        .await
        .unwrap();
    assert_eq!(copy.number.get(), 1);
}

#[tokio::test]
async fn failed_retirement_rolls_back_publication_progress_and_preserves_bytes() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let copy = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![1]))
        .await
        .unwrap();
    db.inspect_writer(|sql| {
        sql.fail_at(
            "reject_retirement",
            "BEFORE DELETE ON _coven_key_uploads",
            "retirement failed",
        )
    });
    assert!(matches!(
        db.complete_key_upload(copy.clone()).await,
        Err(DbError::Sqlite(_))
    ));
    assert_eq!(db.key_uploads().await.unwrap(), std::slice::from_ref(&copy));
    db.inspect_writer(|sql| {
        assert_eq!(
            sql.query_row("SELECT count(*) FROM _coven_key_publications", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            0
        );
        sql.batch("DROP TRIGGER reject_retirement").unwrap();
    });
    db.complete_key_upload(copy).await.unwrap();
    assert!(db.key_uploads().await.unwrap().is_empty());
}

#[tokio::test]
async fn confirmation_cannot_skip_a_number_or_change_fixed_bytes() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let first = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![1]))
        .await
        .unwrap();
    let second = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![2]))
        .await
        .unwrap();
    assert!(matches!(
        db.complete_key_upload(second.clone()).await,
        Err(DbError::KeyUploadChanged { .. })
    ));
    let mut changed = first.clone();
    changed.bytes = vec![3];
    assert!(matches!(
        db.complete_key_upload(changed).await,
        Err(DbError::KeyUploadChanged { .. })
    ));
    assert_eq!(
        db.key_uploads().await.unwrap(),
        [first.clone(), second.clone()]
    );
    db.complete_key_upload(first).await.unwrap();
    db.complete_key_upload(second).await.unwrap();
}

#[tokio::test]
async fn unsigned_copy_numbers_exhaust_without_wrapping_or_reserving() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let writer = db.local_store_log().await.unwrap().device;
    db.inspect_writer(|sql| {
        sql.internal_execute(
            "INSERT INTO _coven_key_publications VALUES(?1,?2)",
            (
                writer.0.to_be_bytes().as_slice(),
                (u64::MAX - 1).to_be_bytes().as_slice(),
            ),
        )
        .unwrap();
    });
    let last = db
        .prepare_key_upload(|_| Ok::<_, DbError>(vec![1]))
        .await
        .unwrap();
    assert_eq!(last.number.get(), u64::MAX);
    assert!(matches!(
        db.prepare_key_upload(|_| -> Result<Vec<u8>, DbError> {
            panic!("exhausted sequence must not seal")
        })
        .await,
        Err(DbError::KeyCopyNumberExhausted)
    ));
    db.complete_key_upload(last).await.unwrap();
    assert!(matches!(
        db.prepare_key_upload(|_| Ok::<_, DbError>(vec![2])).await,
        Err(DbError::KeyCopyNumberExhausted)
    ));
}
