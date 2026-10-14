//! Eager cache fill (§16.4, E8, SP step 7): commit-driven fills, one file's
//! failure as its own pending record, retries and cancellation.
use super::*;

#[tokio::test]
async fn eager_commit_observation_handles_download_apply() {
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
    let b = Fixture::new(Provenance::AppProvided, CacheFill::CacheEager).await;
    b.files.close().await;
    let files = Files::new(
        FileDatabase::new(b.database.clone()),
        b.directory.clone(),
        Some(a.storage.clone()),
        b.clock.clone(),
        b.send.clone(),
        b.ids.clone(),
        crate::TransferLimits::default(),
    );
    let mut status = files.subscribe_eager_cache_fill_status();
    for write in a.database.test_queued_writes().await.unwrap() {
        b.database
            .apply_downloaded::<crate::PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if files.inner.database.missing_bytes(&file).await.unwrap() == 0 {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let requests = a.storage.request_count();
    let reference = b.database.file_ref("files", "eager").await.unwrap();
    assert_eq!(
        files.read_file(&reference).await.unwrap(),
        vec![72; CHUNK * 2]
    );
    assert_eq!(a.storage.request_count(), requests);
    files.close().await;
    b.database.close().await.unwrap();
    a.close().await;
}

/// This device's pending reasons about files, in subject order.
async fn file_records(f: &Fixture) -> Vec<crate::PendingReason> {
    f.database
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| matches!(record.subject, crate::PendingSubject::File { .. }))
        .map(|record| record.reason)
        .collect()
}

#[tokio::test]
async fn one_files_failure_leaves_its_record_while_the_fill_continues() {
    use crate::{PendingReason, PendingSubject, Refusal};
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let damaged = a.uploaded("b-damaged", vec![82; CHUNK + 5]).await;
    let other = a.uploaded("c-other-content", vec![83; 30]).await;
    let good = a.uploaded("d-good", vec![84; CHUNK * 2]).await;
    // Attached after the drains with uploads paused, so it is still waiting.
    a.files.set_uploads_paused(true);
    let waiting = a.attach("files", "a-waiting", vec![81; 40]).await;
    a.storage
        .corrupt_byte(&a.path(&damaged), 11 + 3)
        .await
        .unwrap();
    let b = Fixture::new(Provenance::AppProvided, CacheFill::CacheEager).await;
    b.files
        .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    let other_key = coven_format::key::encode_key(&[coven_format::value::Value::Text(
        "c-other-content".into(),
    )])
    .unwrap();
    for mut write in a.database.test_queued_writes().await.unwrap() {
        for part in &mut write.parts {
            for row in &mut part.rows {
                if row.row.key != other_key {
                    continue;
                }
                if let coven_merge::Operation::Insert(columns) = &mut row.change.operation {
                    columns.get_mut("hash").unwrap().value =
                        coven_format::value::Value::Blob(vec![0; 32]);
                }
            }
        }
        b.database
            .apply_downloaded::<crate::PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    b.files.sync_files().await.unwrap();
    // The last file completed although every earlier one failed.
    assert_eq!(
        b.files.inner.database.missing_bytes(&good).await.unwrap(),
        0
    );
    let subject = |file: &FileRef| PendingSubject::File {
        device: file.uploader(),
        file: file.id(),
    };
    let records: std::collections::BTreeMap<_, _> = b
        .database
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .map(|record| (format!("{:?}", record.subject), record.reason))
        .collect();
    assert_eq!(records.len(), 3, "{records:?}");
    assert_eq!(
        records[&format!("{:?}", subject(&waiting))],
        PendingReason::Waits(crate::Prerequisite::Object(a.path(&waiting)))
    );
    assert!(matches!(
        records[&format!("{:?}", subject(&damaged))],
        PendingReason::Refused(Refusal::Decryption { .. })
    ));
    assert!(matches!(
        records[&format!("{:?}", subject(&other))],
        PendingReason::Refused(Refusal::ContentHash)
    ));
    assert!(matches!(
        b.files.inner.eager.borrow().clone(),
        EagerCacheFillStatus::Idle
    ));
    // A commit or connection wake fills only changed references: it neither
    // retries the waiting file nor touches the status the pass left.
    let status = b.files.subscribe_eager_cache_fill_status();
    b.files.inner.fill_eager(EagerWork::NewReferences).await;
    assert!(!status.has_changed().unwrap());
    // Retry: the waiting file arrives and completes; refused bytes are not
    // fetched again automatically.
    a.drain().await;
    let damaged_reads = |reads: &[(coven_storage::ObjectPath, u64, u64)]| {
        reads
            .iter()
            .filter(|(path, _, _)| *path == a.path(&damaged) || *path == a.path(&other))
            .count()
    };
    let before = damaged_reads(&a.storage.reads().await);
    b.files.sync_files().await.unwrap();
    assert_eq!(
        b.files
            .inner
            .database
            .missing_bytes(&waiting)
            .await
            .unwrap(),
        0
    );
    assert_eq!(damaged_reads(&a.storage.reads().await), before);
    assert_eq!(file_records(&b).await.len(), 2);
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn a_sync_preserves_eager_cancellation_and_retries_network_failures() {
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
    let b = Fixture::new(Provenance::AppProvided, CacheFill::CacheEager).await;
    b.files
        .set_storage(Some(a.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    a.storage.set_online(false);
    let mut status = b.files.subscribe_eager_cache_fill_status();
    for write in a.database.test_queued_writes().await.unwrap() {
        b.database
            .apply_downloaded::<crate::PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    // The network failure is the file's pending record, not a fill state.
    b.files.sync_files().await.unwrap();
    assert!(matches!(
        file_records(&b).await.as_slice(),
        [crate::PendingReason::Storage {
            failure: coven_storage::StorageFailure::Network,
            ..
        }]
    ));
    a.storage.set_online(true);
    b.files.sync_files().await.unwrap();
    assert_eq!(
        b.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    assert!(file_records(&b).await.is_empty());
    b.files.evict_file(&file).await.unwrap();
    a.storage
        .set_faults(coven_storage::test_utils::Faults {
            delay: Duration::from_millis(50),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    // A newly observed version is eligible; cancel after its first request.
    let second = a.uploaded("another", vec![73; CHUNK * 2]).await;
    for write in a.database.test_queued_writes().await.unwrap() {
        b.database
            .apply_downloaded::<crate::PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    status
        .wait_for(|s| matches!(s, EagerCacheFillStatus::Downloading(_)))
        .await
        .unwrap();
    b.files.cancel_eager_cache_fill();
    status
        .wait_for(|s| matches!(s, EagerCacheFillStatus::Cancelled(_)))
        .await
        .unwrap();
    b.files.sync_files().await.unwrap();
    assert!(b.files.inner.database.missing_bytes(&second).await.unwrap() > 0);
    b.close().await;
    a.close().await;
}
