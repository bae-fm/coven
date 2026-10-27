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
        pauses_writes: false,
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
async fn an_upload_failure_records_its_wait_and_pending_record_and_completion_removes_both() {
    use crate::pending::tests::{missing, Record, Subject};
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    let owner = FileDatabase::new(db.clone());
    attach(&db, b"bytes".to_vec(), true).await.unwrap();
    let id = owner.uploads().await.unwrap()[0].id;
    let work = || owner.upload_work::<Record>(|_| Subject::Number(7));
    assert_eq!(work().await.unwrap()[0].1, None);
    owner
        .fail_upload(id, missing(7), Some(wait(1, 1)))
        .await
        .unwrap();
    assert_eq!(owner.uploads().await.unwrap()[0].retry, Some(wait(1, 1)));
    // The failure is this device's pending record about the file, read with
    // its queue entry.
    let recorded = work().await.unwrap().remove(0).1.unwrap();
    assert_eq!(db.pending::<Record>().await.unwrap(), vec![recorded]);
    // A failure that waits for the app keeps no automatic deadline.
    owner.fail_upload(id, missing(7), None).await.unwrap();
    assert_eq!(owner.uploads().await.unwrap()[0].retry, None);
    owner
        .fail_upload(id, missing(7), Some(wait(2, 2)))
        .await
        .unwrap();
    // One transaction retires the queue row, its wait and its record.
    owner
        .complete_upload(id, Subject::Number(7), None)
        .await
        .unwrap();
    assert_eq!(local_count(&db, "_coven_retries"), 0);
    assert!(owner.uploads().await.unwrap().is_empty());
    assert!(db.pending::<Record>().await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_pause_setting_and_queue_survive_reopening_in_one_query() {
    let store = TestStore::new();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    attach(&db, b"bytes".to_vec(), true).await.unwrap();
    let owner = FileDatabase::new(db.clone());
    let mut queue = db.subscribe(|sql| sql.upload_queue());
    let first = queue.next().await.expect("the live query answers").unwrap();
    assert!(!first.paused);
    assert_eq!(first.uploads.len(), 1);
    owner.set_uploads_paused(true).await.unwrap();
    assert!(
        queue
            .next()
            .await
            .expect("the live query answers")
            .unwrap()
            .paused
    );
    let session = SecretBytes::new(b"session".to_vec());
    owner
        .record_session(
            first.uploads[0].id,
            session,
            crate::UploadProgress {
                bytes_confirmed: 0,
                bytes_total: 100,
            },
        )
        .await
        .unwrap();
    let recorded = queue.next().await.expect("the live query answers").unwrap();
    assert_eq!(
        recorded.uploads[0].session.as_ref().unwrap().as_bytes(),
        b"session"
    );
    drop(queue);
    db.close().await.unwrap();
    let db = store
        .schema(tables(Provenance::AppProvided), SCHEMA)
        .await
        .unwrap();
    assert_eq!(db.upload_queue().await.unwrap(), recorded);
    let owner = FileDatabase::new(db.clone());
    assert!(owner.uploads_paused().await.unwrap());
    owner.set_uploads_paused(false).await.unwrap();
    assert!(!db.upload_queue().await.unwrap().paused);
    db.close().await.unwrap();
}
