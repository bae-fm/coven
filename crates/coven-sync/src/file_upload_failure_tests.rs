//! Recoverable upload failures make progress without an app retry.
use super::*;
use crate::{FileMissingReason, FileSourceFailure, Retry};

#[tokio::test]
async fn restored_sources_upload_after_backoff_without_provider_probes() {
    for failure in [
        FileSourceFailure::Missing,
        FileSourceFailure::Changed,
        FileSourceFailure::Integrity,
    ] {
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
        let provenance = match failure {
            FileSourceFailure::Integrity => Provenance::AppProvided,
            _ => Provenance::UserProvided,
        };
        let f = Fixture::with_clock(provenance, CacheFill::CacheLazy, clock.clone()).await;
        f.files.stop_background_for_counts().await;
        let bytes = vec![41; CHUNK * 2];
        let (file, path) = match failure {
            FileSourceFailure::Integrity => {
                let file = f.attach("files", "photo", bytes.clone()).await;
                let directory = f
                    .root
                    .path()
                    .join("stores")
                    .join(f.directory.id().to_string())
                    .join("files");
                let paths: Vec<_> = std::fs::read_dir(directory)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect();
                assert_eq!(paths.len(), 1);
                (file, paths[0].clone())
            }
            _ => (
                f.original("photo", &bytes).await,
                f.root.path().join("photo"),
            ),
        };
        let saved = f.root.path().join("saved");
        std::fs::rename(&path, &saved).unwrap();
        if failure != FileSourceFailure::Missing {
            std::fs::write(&path, vec![42; bytes.len()]).unwrap();
        }
        f.files.set_uploads_paused(false).await.unwrap();
        let requests = f.storage.request_trace().len();
        for wait in [1, 2] {
            assert_eq!(
                drained(f.files.inner.drain_uploads(false).await.unwrap()),
                0
            );
            assert_eq!(
                f.upload_reason(&file).await,
                Some(PendingReason::FileUnavailable(FileMissingReason::Source(
                    failure
                )))
            );
            assert_eq!(retry(&f).await.unwrap().wait, Duration::from_secs(wait));
            assert_eq!(retry_in(&f), Some(Duration::from_secs(wait)));
            assert_eq!(f.storage.request_trace().len(), requests);
            if wait == 1 {
                advance(&f, &clock, wait);
            }
        }
        if failure != FileSourceFailure::Missing {
            std::fs::remove_file(&path).unwrap();
        }
        // Restoring the original retains its captured modification time.
        std::fs::rename(saved, &path).unwrap();
        assert_eq!(
            drained(f.files.inner.drain_uploads(false).await.unwrap()),
            0
        );
        advance(&f, &clock, 2);
        assert_eq!(
            drained(f.files.inner.drain_uploads(false).await.unwrap()),
            1
        );
        assert!(!f.queued(&file).await);
        assert_eq!(f.upload_reason(&file).await, None);
        assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
        f.close().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_disk_failure_retries_after_its_backoff() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = Fixture::with_clock(
        Provenance::UserProvided,
        CacheFill::CacheLazy,
        clock.clone(),
    )
    .await;
    f.files.stop_background_for_counts().await;
    let file = f.original("photo", b"photo").await;
    let path = f.root.path().join("photo");
    let saved = f.root.path().join("saved");
    std::fs::rename(&path, &saved).unwrap();
    std::os::unix::fs::symlink(&path, &path).unwrap();
    f.files.set_uploads_paused(false).await.unwrap();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(
        f.upload_reason(&file).await,
        Some(PendingReason::Failed(crate::LocalFailure::Disk))
    );
    assert_eq!(retry_in(&f), Some(Duration::from_secs(1)));
    std::fs::remove_file(&path).unwrap();
    std::fs::rename(saved, path).unwrap();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    advance(&f, &clock, 1);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    f.close().await;
}

#[tokio::test]
async fn full_storage_retries_with_the_shared_backoff_and_cap() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = fixture(&clock).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    for wait in [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300] {
        f.storage
            .set_faults(fail_next(StorageFailure::QuotaExceeded, None))
            .await;
        assert_eq!(
            drained(f.files.inner.drain_uploads(false).await.unwrap()),
            0
        );
        assert_eq!(retry_in(&f), Some(Duration::from_secs(wait)));
        advance(&f, &clock, wait);
    }
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    f.close().await;
}

#[tokio::test]
async fn new_credentials_retry_every_waiting_upload_once() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = fixture(&clock).await;
    for (name, failure) in [
        ("sign-in", StorageFailure::Authentication),
        ("permission", StorageFailure::PermissionDenied),
    ] {
        f.attach("files", name, b"photo".to_vec()).await;
        f.storage.set_faults(fail_next(failure, None)).await;
        assert_eq!(
            drained(f.files.inner.drain_uploads(false).await.unwrap()),
            0
        );
    }
    assert_eq!(f.queue().await.files.len(), 2);
    assert_eq!(retry_in(&f), None);
    let requests = f.storage.request_trace().len();
    advance(&f, &clock, 600);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    let storage = Arc::new(f.storage.with_cooldown(f.files.inner.waits.clone()));
    assert!(f
        .files
        .set_storage(
            Some(storage.clone()),
            std::future::ready(Err(SyncError::NoStorage))
        )
        .await
        .is_err());
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    f.files
        .set_storage(Some(storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    // A connection permits one attempt; failure waits for the next change.
    f.storage
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::Authentication,
            ..Faults::none()
        })
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    f.files
        .set_storage(Some(storage), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        2
    );
    assert!(f
        .database
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .is_empty());
    f.close().await;
}

#[tokio::test]
async fn failures_that_never_clear_stay_visible_through_reconnect_and_manual_retry() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let mut f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::Protocol, None))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    let records = f.database.pending::<crate::PendingRecord>().await.unwrap();
    assert_eq!(records[0].retry(), Retry::Never);
    assert_eq!(retry_in(&f), None);
    let requests = f.storage.request_trace().len();
    advance(&f, &clock, 600);
    f.files
        .set_storage(Some(f.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(drained(f.files.retry_uploads_now().await.unwrap()), 0);
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.files.set_uploads_paused(false).await.unwrap();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    assert_eq!(
        f.database.pending::<crate::PendingRecord>().await.unwrap(),
        records
    );
    assert!(f.queued(&file).await);
    f.close().await;
}

#[tokio::test]
async fn reopening_permits_one_attempt_for_a_connection_failure() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let mut f = fixture(&clock).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::Authentication, None))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.files.set_uploads_paused(false).await.unwrap();
    f.storage
        .set_faults(fail_next(StorageFailure::Authentication, None))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    let requests = f.storage.request_trace().len();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        0
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    assert_eq!(retry_in(&f), None);
    f.files
        .set_storage(Some(f.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        1
    );
    f.close().await;
}

#[tokio::test]
async fn manual_retry_drains_recoverable_uploads_early() {
    for failure in [StorageFailure::Network, StorageFailure::Authentication] {
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
        let f = fixture(&clock).await;
        let file = f.attach("files", "photo", b"photo".to_vec()).await;
        f.storage.set_faults(fail_next(failure, None)).await;
        assert_eq!(
            drained(f.files.inner.drain_uploads(false).await.unwrap()),
            0
        );
        assert_eq!(drained(f.files.retry_uploads_now().await.unwrap()), 1);
        assert!(!f.queued(&file).await);
        f.close().await;
    }
}

#[tokio::test]
async fn the_worker_retries_a_restored_original_without_an_app_call() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = Fixture::with_clock(
        Provenance::UserProvided,
        CacheFill::CacheLazy,
        clock.clone(),
    )
    .await;
    let file = f.original("photo", b"photo").await;
    let path = f.root.path().join("photo");
    let saved = f.root.path().join("saved");
    std::fs::rename(&path, &saved).unwrap();
    let mut uploads = f.database.subscribe_upload_queue();
    f.files.set_uploads_paused(false).await.unwrap();
    for failures in [1, 2] {
        tokio::time::timeout(Duration::from_secs(5), async {
            while retry(&f).await.is_none_or(|wait| wait.failures < failures) {
                uploads.next().await.unwrap();
            }
        })
        .await
        .unwrap();
        // The drain releases its guard after persisting and arming the timer.
        drop(f.files.inner.drain.lock().await);
        assert_eq!(f.storage.request_trace().len(), 0);
        if failures == 1 {
            clock.set(clock.now() + Duration::from_secs(1));
        }
    }
    std::fs::rename(saved, path).unwrap();
    clock.set(clock.now() + Duration::from_secs(2));
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.queued(&file).await {
            uploads.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(f.upload_reason(&file).await, None);
    f.close().await;
}

#[tokio::test]
async fn the_worker_retries_sign_in_failure_on_client_swap() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::Authentication, None))
        .await;
    let mut pending = f.database.subscribe_pending::<crate::PendingRecord>();
    f.files.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.upload_reason(&file).await.is_none() {
            pending.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    drop(f.files.inner.drain.lock().await);
    assert_eq!(retry_in(&f), None);
    let mut uploads = f.database.subscribe_upload_queue();
    f.files
        .set_storage(
            Some(Arc::new(
                f.storage.with_cooldown(f.files.inner.waits.clone()),
            )),
            std::future::ready(Ok(())),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.queued(&file).await {
            uploads.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(f.upload_reason(&file).await, None);
    f.close().await;
}
