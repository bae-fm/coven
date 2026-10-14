use super::*;
use crate::{
    file_write::tests::{attach, local_count, tables, SCHEMA},
    tests::TestStore,
    FileDatabase, NewOperation, OperationUpdate, Provenance,
};
use coven_crypto::SecretBytes;

fn wait(failures: u32, seconds: u64) -> RetryWait {
    RetryWait {
        failures,
        not_before: std::time::UNIX_EPOCH + Duration::from_secs(1_000_000 + seconds),
        wait: Duration::from_secs(seconds),
    }
}

fn operation() -> NewOperation {
    NewOperation {
        kind: "invite".into(),
        data: vec![1],
        started_by: "app".into(),
        reserves_snapshot: false,
    }
}

#[tokio::test]
async fn waits_survive_reopening_by_subject() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let id = db.start_operation(operation()).await.unwrap();
    db.record_retry(RetrySubject::Provider, wait(0, 600))
        .await
        .unwrap();
    db.record_retry(RetrySubject::Join, wait(3, 4))
        .await
        .unwrap();
    db.record_retry(RetrySubject::Operation(id), wait(1, 1))
        .await
        .unwrap();
    // A newer delay replaces the subject's T and W rather than adding a row.
    db.record_retry(RetrySubject::Join, wait(4, 8))
        .await
        .unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut waits = db.retry_waits().await.unwrap();
    waits.sort_by_key(|(subject, _)| *subject);
    assert_eq!(
        waits,
        [
            (RetrySubject::Operation(id), wait(1, 1)),
            (RetrySubject::Join, wait(4, 8)),
            (RetrySubject::Provider, wait(0, 600)),
        ]
    );
    db.clear_retry(RetrySubject::Join).await.unwrap();
    assert_eq!(db.retry_wait(RetrySubject::Join).await.unwrap(), None);
    assert_eq!(
        db.retry_wait(RetrySubject::Provider).await.unwrap(),
        Some(wait(0, 600))
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn sub_millisecond_waits_round_up_when_persisted() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut short = wait(1, 1);
    short.wait = Duration::from_micros(1_000_001);
    db.record_retry(RetrySubject::Join, short).await.unwrap();
    assert_eq!(
        db.retry_wait(RetrySubject::Join)
            .await
            .unwrap()
            .unwrap()
            .wait,
        Duration::from_millis(1_001)
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_operation_wait_ends_with_its_condition() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let id = db.start_operation(operation()).await.unwrap();
    let subject = RetrySubject::Operation(id);
    db.record_retry(subject, wait(2, 2)).await.unwrap();
    // Advancing is new work: its next failure starts the backoff again.
    db.advance_operation(OperationUpdate {
        id,
        previous: 0,
        last_step: 1,
        data: vec![2],
    })
    .await
    .unwrap();
    assert_eq!(db.retry_wait(subject).await.unwrap(), None);
    // An explicit app retry overrides the ordinary backoff.
    db.record_retry(subject, wait(3, 4)).await.unwrap();
    db.retry_operation(id).await.unwrap();
    assert_eq!(db.retry_wait(subject).await.unwrap(), None);
    db.record_retry(subject, wait(1, 1)).await.unwrap();
    db.finish_operation(id).await.unwrap();
    assert!(db.retry_waits().await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_upload_failure_records_its_wait_atomically_and_completion_removes_it() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let owner = FileDatabase::new(db.clone());
    attach(&db, b"bytes".to_vec(), true).await.unwrap();
    let id = owner.uploads().await.unwrap()[0].id;
    assert_eq!(owner.uploads().await.unwrap()[0].retry, None);
    owner
        .fail_upload(id, SecretBytes::new(b"network".to_vec()), Some(wait(1, 1)))
        .await
        .unwrap();
    assert_eq!(owner.uploads().await.unwrap()[0].retry, Some(wait(1, 1)));
    // A failure that waits for the app keeps no automatic deadline.
    owner
        .fail_upload(id, SecretBytes::new(b"quota".to_vec()), None)
        .await
        .unwrap();
    let queued = owner.uploads().await.unwrap();
    assert_eq!(queued[0].retry, None);
    assert_eq!(queued[0].failure.as_ref().unwrap().as_bytes(), b"quota");
    owner
        .fail_upload(id, SecretBytes::new(b"network".to_vec()), Some(wait(2, 2)))
        .await
        .unwrap();
    owner.complete_upload(id).await.unwrap();
    assert_eq!(local_count(&db, "_coven_retries"), 0);
    db.close().await.unwrap();
}
