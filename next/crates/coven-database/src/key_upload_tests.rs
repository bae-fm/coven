use crate::{tests::TestStore, DbError};

#[tokio::test]
async fn fixed_copy_survives_reopen_without_resealing() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let fixed = db
        .prepare_key_upload("key path".into(), || Ok::<_, DbError>(vec![1, 2, 3]))
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let retried = db
        .prepare_key_upload("key path".into(), || -> Result<Vec<u8>, DbError> {
            panic!("a retry must not seal again")
        })
        .await
        .unwrap();
    assert_eq!(retried, fixed);
    db.complete_key_upload("key path".into()).await.unwrap();
    db.complete_key_upload("key path".into()).await.unwrap();
    db.inspect_writer(|sql| {
        assert_eq!(
            sql.query_row("SELECT count(*) FROM coven_key_uploads", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    });
}

#[tokio::test]
async fn failed_sealing_or_insertion_publishes_no_fixed_copy() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert!(db
        .prepare_key_upload("key path".into(), || Err::<Vec<u8>, _>(
            DbError::StoreClosed
        ))
        .await
        .is_err());
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER reject_copy BEFORE INSERT ON coven_key_uploads BEGIN SELECT RAISE(ABORT,'copy failed'); END;").unwrap());
    assert!(db
        .prepare_key_upload("key path".into(), || Ok::<_, DbError>(vec![1]))
        .await
        .is_err());
    db.inspect_writer(|sql| {
        assert_eq!(
            sql.query_row("SELECT count(*) FROM coven_key_uploads", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        sql.batch("DROP TRIGGER reject_copy").unwrap();
    });
    assert_eq!(
        db.prepare_key_upload("key path".into(), || Ok::<_, DbError>(vec![2]))
            .await
            .unwrap(),
        vec![2]
    );
}

#[tokio::test]
async fn failed_retirement_keeps_the_original_copy() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.prepare_key_upload("key path".into(), || Ok::<_, DbError>(vec![1]))
        .await
        .unwrap();
    db.inspect_writer(|sql| sql.batch("CREATE TRIGGER reject_retirement BEFORE DELETE ON coven_key_uploads BEGIN SELECT RAISE(ABORT,'retirement failed'); END;").unwrap());
    assert!(db.complete_key_upload("key path".into()).await.is_err());
    assert_eq!(
        db.prepare_key_upload("key path".into(), || -> Result<Vec<u8>, DbError> {
            panic!("failed retirement must retain fixed bytes")
        })
        .await
        .unwrap(),
        vec![1]
    );
}
