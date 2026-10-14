//! Eager cache fill (§16.4, E8, SP step 7): commit-driven fills, one file's
//! failure as its own pending record, retries under persisted waits and the
//! provider cooldown (§16.5), and cancellation.
use super::*;
use coven_database::RetrySubject;
use coven_foundation::clock::{Clock, FixedClock};
use coven_storage::{test_utils::Faults, StorageFailure};

fn clock() -> Arc<FixedClock> {
    Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ))
}

/// Pass `seconds` and keep §10's catch-up fresh for any upload in between.
fn advance(f: &Fixture, clock: &FixedClock, seconds: u64) {
    clock.set(clock.now() + Duration::from_secs(seconds));
    f.send.begin();
    f.send
        .complete(crate::send_gate::Binding::Device, Vec::new())
        .unwrap();
}

/// A device eager for `files` that reads `a`'s storage through its own
/// connection, so a `Retry-After` it receives is its own store's cooldown.
async fn eager_reader(a: &Fixture, clock: &Arc<FixedClock>) -> Fixture {
    let b = Fixture::with_clock(
        Provenance::AppProvided,
        CacheFill::CacheEager,
        clock.clone(),
    )
    .await;
    connect(&b, a).await;
    b
}

async fn connect(b: &Fixture, a: &Fixture) {
    // Only the explicit fills below download.
    b.files.stop_background_for_counts().await;
    let storage = Arc::new(a.storage.with_cooldown(b.files.inner.waits.clone()));
    b.files
        .set_storage(Some(storage), std::future::ready(Ok(())))
        .await
        .unwrap();
}

async fn apply_all(a: &Fixture, b: &Fixture) {
    for write in a.database.test_queued_writes().await.unwrap() {
        b.database
            .apply_downloaded::<crate::PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
}

/// Requests for `file`'s object at `a`'s storage.
async fn reads_of(a: &Fixture, file: &FileRef) -> usize {
    let path = a.path(file);
    a.storage
        .reads()
        .await
        .iter()
        .filter(|(read, _, _)| *read == path)
        .count()
}

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
        b.send.clone(),
        b.files.inner.waits.clone(),
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
    a.files.set_uploads_paused(true).await.unwrap();
    let waiting = a.attach("files", "a-waiting", vec![81; 40]).await;
    a.storage
        .corrupt_byte(&a.path(&damaged), 11 + 3)
        .await
        .unwrap();
    let clock = clock();
    let b = Fixture::with_clock(
        Provenance::AppProvided,
        CacheFill::CacheEager,
        clock.clone(),
    )
    .await;
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
    // A wake before the waiting file's retry is due neither requests it
    // nor touches the status the pass left.
    let status = b.files.subscribe_eager_cache_fill_status();
    b.files.inner.fill_eager().await;
    assert!(!status.has_changed().unwrap());
    // Retry once due: the waiting file arrives and completes; refused bytes
    // are not fetched again automatically.
    a.drain().await;
    clock.set(clock.now() + Duration::from_secs(1));
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
    let clock = clock();
    let b = Fixture::with_clock(
        Provenance::AppProvided,
        CacheFill::CacheEager,
        clock.clone(),
    )
    .await;
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
    clock.set(clock.now() + Duration::from_secs(1));
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

/// A failed eager download persists `T` and `W` with its record before it
/// waits: no fill requests it again before its deadline, reopening included,
/// and the completed download takes the wait with its record (§16.5).
#[tokio::test]
async fn a_failed_download_is_not_requested_before_its_deadline() {
    let clock = clock();
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
    let mut b = eager_reader(&a, &clock).await;
    apply_all(&a, &b).await;
    a.storage.set_online(false);
    b.files.sync_files().await.unwrap();
    let waits = b.files.inner.database.download_waits().await.unwrap();
    let [(subject, wait)] = waits.as_slice() else {
        panic!("one persisted wait: {waits:?}");
    };
    assert_eq!(*subject, crate::files::file_cache::download_subject(&file));
    assert_eq!(wait.failures, 1);
    assert_eq!(wait.wait, Duration::from_secs(1));
    assert_eq!(wait.not_before, clock.now() + Duration::from_secs(1));
    a.storage.set_online(true);
    let reads = reads_of(&a, &file).await;
    b.files.sync_files().await.unwrap();
    assert_eq!(reads_of(&a, &file).await, reads);
    // Reopening arms the remaining wait from the persisted row.
    b.reopen().await;
    connect(&b, &a).await;
    b.files.sync_files().await.unwrap();
    assert_eq!(reads_of(&a, &file).await, reads);
    assert_eq!(file_records(&b).await.len(), 1);
    advance(&b, &clock, 1);
    b.files.sync_files().await.unwrap();
    assert_eq!(
        b.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    assert!(file_records(&b).await.is_empty());
    assert!(b
        .files
        .inner
        .database
        .download_waits()
        .await
        .unwrap()
        .is_empty());
    b.close().await;
    a.close().await;
}

/// A `Retry-After` a download receives is the store's cooldown: no eager
/// download, even one whose own wait ended, and no upload is requested until
/// it ends (SP waiting).
#[tokio::test]
async fn a_provider_cooldown_holds_eager_downloads_and_uploads_alike() {
    let clock = clock();
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
    let b = eager_reader(&a, &clock).await;
    apply_all(&a, &b).await;
    a.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(600),
            ..Faults::none()
        })
        .await;
    b.files.sync_files().await.unwrap();
    let cooldown = b
        .database
        .retry_wait(RetrySubject::Provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cooldown.wait, Duration::from_secs(600));
    let photo = b.attach("other", "photo", b"photo".to_vec()).await;
    b.files.inner.state.lock().unwrap().paused = false;
    // The download's own one-second wait ends; the cooldown still holds it
    // and the upload queue.
    advance(&b, &clock, 1);
    let requests = a.storage.request_trace().len();
    let status = b.files.subscribe_eager_cache_fill_status();
    b.files.sync_files().await.unwrap();
    // Not attempted at all: the fill never started.
    assert!(!status.has_changed().unwrap());
    assert!(matches!(
        b.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::AllInBackoff
    ));
    assert_eq!(a.storage.request_trace().len(), requests);
    advance(&b, &clock, 599);
    b.files.sync_files().await.unwrap();
    assert_eq!(
        b.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    b.files.inner.drain_uploads(false).await.unwrap();
    assert!(!b.queued(&photo).await);
    b.close().await;
    a.close().await;
}

/// The file worker wakes when a download's wait ends, without a pass or
/// another commit (SP waiting).
#[tokio::test]
async fn the_worker_retries_a_download_when_its_wait_ends() {
    let clock = clock();
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
    let b = Fixture::with_clock(
        Provenance::AppProvided,
        CacheFill::CacheEager,
        clock.clone(),
    )
    .await;
    b.files
        .set_storage(
            Some(Arc::new(
                a.storage.with_cooldown(b.files.inner.waits.clone()),
            )),
            std::future::ready(Ok(())),
        )
        .await
        .unwrap();
    a.storage.set_online(false);
    let mut status = b.files.subscribe_eager_cache_fill_status();
    apply_all(&a, &b).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while b
            .files
            .inner
            .database
            .download_waits()
            .await
            .unwrap()
            .is_empty()
        {
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    a.storage.set_online(true);
    clock.set(clock.now() + Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(5), async {
        while b.files.inner.database.missing_bytes(&file).await.unwrap() > 0 {
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    b.close().await;
    a.close().await;
}
