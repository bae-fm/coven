//! Recoverable upload failures make progress without an app retry.
use super::*;
use crate::{FileMissingReason, FileSourceFailure, Retry};

/// A moved original fails at opening, before any provider request, and its
/// move says it is missing. A same-size change to coven's copy opens, and the
/// first chunk check refuses it before any byte is sent. Both retry under
/// backoff until the source is back (§17.3, §17.8).
#[tokio::test]
async fn restored_sources_upload_after_backoff() {
    for failure in [FileSourceFailure::Missing, FileSourceFailure::Integrity] {
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
        let provenance = match failure {
            FileSourceFailure::Integrity => Provenance::AppProvided,
            _ => Provenance::UserProvided,
        };
        let f = Fixture::with_clock(provenance, clock.clone()).await;
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
            assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
            if failure == FileSourceFailure::Missing {
                assert_eq!(f.upload_reason(&file).await, None);
                assert_eq!(
                    f.move_reasons().await,
                    [PendingReason::Original {
                        path: path.clone(),
                        failure
                    }]
                );
            } else {
                assert_eq!(
                    f.upload_reason(&file).await,
                    Some(PendingReason::FileUnavailable(FileMissingReason::Source(
                        failure
                    )))
                );
            }
            assert_eq!(retry(&f).await.unwrap().wait, Duration::from_secs(wait));
            assert_eq!(retry_in(&f), Some(Duration::from_secs(wait)));
            if failure == FileSourceFailure::Integrity {
                assert_eq!(f.storage.transferred().await.0, 0);
            } else {
                assert_eq!(f.storage.request_trace().len(), requests);
            }
            if wait == 1 {
                advance(&clock, wait);
            }
        }
        if failure != FileSourceFailure::Missing {
            std::fs::remove_file(&path).unwrap();
        }
        // Restoring the original retains its captured modification time.
        std::fs::rename(saved, &path).unwrap();
        assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
        advance(&clock, 2);
        assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 1);
        assert!(!f.queued(&file).await);
        assert_eq!(f.upload_reason(&file).await, None);
        assert!(f.move_reasons().await.is_empty());
        assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
        f.close().await;
    }
}

/// An original that changes before its upload fails its write's move whole:
/// no file of the write switches, each stays on this device only, and the
/// move's record names the original until the app cancels the move
/// (§17.3).
#[tokio::test]
async fn a_changed_original_fails_its_move_and_switches_nothing() {
    let f = Fixture::new(Provenance::UserProvided).await;
    f.files.stop_background_for_counts().await;
    let mut prepared = Vec::new();
    for name in ["kept", "changed"] {
        let path = f.root.path().join(name);
        std::fs::write(&path, vec![5; CHUNK + 3]).unwrap();
        prepared.push(
            coven_database::prepare_user_file(&path, |_| {})
                .await
                .unwrap(),
        );
    }
    let files = f
        .database
        .write_with_files::<_, _, _, _, coven_database::DbError>(
            move |batch| {
                Ok(prepared
                    .into_iter()
                    .map(|prepared| {
                        batch.put_user_file("files", coven_database::UserSource::Cloud(prepared))
                    })
                    .collect::<Vec<_>>())
            },
            |sql, files| {
                for (id, file) in ["kept", "changed"].iter().zip(&files) {
                    sql.execute("INSERT INTO files(id,file) VALUES(?1,?2)", (id, file))?;
                }
                Ok(files)
            },
        )
        .await
        .unwrap();
    let changed = f.root.path().join("changed");
    std::fs::write(&changed, vec![6; CHUNK + 3]).unwrap();
    let later = std::time::SystemTime::now() + Duration::from_secs(5);
    std::fs::File::options()
        .write(true)
        .open(&changed)
        .unwrap()
        .set_modified(later)
        .unwrap();
    f.files.set_uploads_paused(false).await.unwrap();
    f.files.inner.drain_uploads().await.unwrap();
    let records = f.database.pending::<crate::PendingRecord>().await.unwrap();
    let [record] = records.as_slice() else {
        panic!("{records:?}");
    };
    let crate::PendingSubject::Move { id } = record.subject else {
        panic!("{record:?}");
    };
    assert_eq!(
        record.reason,
        PendingReason::Original {
            path: changed.clone(),
            failure: FileSourceFailure::Changed
        }
    );
    assert_eq!(record.retry(), Retry::AppAction);
    f.files.inner.drain_uploads().await.unwrap();
    assert!(f.files.inner.database.uploads().await.unwrap().is_empty());
    for file in &files {
        let file = f.database.file_ref(*file).await.unwrap();
        assert!(matches!(
            file.place(),
            coven_database::FilePlace::DeviceOnly { .. }
        ));
    }
    let kept = f.database.file_ref(files[0]).await.unwrap();
    assert_eq!(f.files.read_file(&kept).await.unwrap(), vec![5; CHUNK + 3]);
    // Cancelling the failed move ends its record; the files stay here.
    f.database
        .write(move |sql| sql.cancel_move(id))
        .await
        .unwrap();
    assert!(f.move_reasons().await.is_empty());
    f.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn a_disk_failure_retries_after_its_backoff() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
    f.files.stop_background_for_counts().await;
    let file = f.original("photo", b"photo").await;
    let path = f.root.path().join("photo");
    let saved = f.root.path().join("saved");
    std::fs::rename(&path, &saved).unwrap();
    std::os::unix::fs::symlink(&path, &path).unwrap();
    f.files.set_uploads_paused(false).await.unwrap();
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    assert_eq!(
        f.upload_reason(&file).await,
        Some(PendingReason::Failed(crate::LocalFailure::Disk))
    );
    assert_eq!(retry_in(&f), Some(Duration::from_secs(1)));
    std::fs::remove_file(&path).unwrap();
    std::fs::rename(saved, path).unwrap();
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    advance(&clock, 1);
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 1);
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
        assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
        assert_eq!(retry_in(&f), Some(Duration::from_secs(wait)));
        advance(&clock, wait);
    }
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 1);
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
        assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    }
    assert_eq!(f.queue().await.files.len(), 2);
    assert_eq!(retry_in(&f), None);
    let requests = f.storage.request_trace().len();
    advance(&clock, 600);
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
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
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
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
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    f.files
        .set_storage(Some(storage), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 2);
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
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    let records = f.database.pending::<crate::PendingRecord>().await.unwrap();
    assert_eq!(records[0].retry(), Retry::Never);
    assert_eq!(retry_in(&f), None);
    let requests = f.storage.request_trace().len();
    advance(&clock, 600);
    f.files
        .set_storage(Some(f.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    assert_eq!(
        drained(f.files.inner.drain_uploads_retrying().await.unwrap()),
        0
    );
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.files.set_uploads_paused(false).await.unwrap();
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
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
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.files.set_uploads_paused(false).await.unwrap();
    f.storage
        .set_faults(fail_next(StorageFailure::Authentication, None))
        .await;
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    let requests = f.storage.request_trace().len();
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
    assert_eq!(f.storage.request_trace().len(), requests);
    assert_eq!(retry_in(&f), None);
    f.files
        .set_storage(Some(f.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 1);
    f.close().await;
}

#[tokio::test]
async fn manual_retry_drains_recoverable_uploads_early() {
    for failure in [StorageFailure::Network, StorageFailure::Authentication] {
        let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
        let f = fixture(&clock).await;
        let file = f.attach("files", "photo", b"photo".to_vec()).await;
        f.storage.set_faults(fail_next(failure, None)).await;
        assert_eq!(drained(f.files.inner.drain_uploads().await.unwrap()), 0);
        assert_eq!(
            drained(f.files.inner.drain_uploads_retrying().await.unwrap()),
            1
        );
        assert!(!f.queued(&file).await);
        f.close().await;
    }
}

#[tokio::test]
async fn the_worker_retries_a_restored_original_without_an_app_call() {
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
    let file = f.original("photo", b"photo").await;
    let path = f.root.path().join("photo");
    let saved = f.root.path().join("saved");
    std::fs::rename(&path, &saved).unwrap();
    let mut uploads = f.subscribe_queue();
    f.files.set_uploads_paused(false).await.unwrap();
    for failures in [1, 2] {
        tokio::time::timeout(Duration::from_secs(5), async {
            while retry(&f).await.is_none_or(|wait| wait.failures < failures) {
                uploads
                    .next()
                    .await
                    .expect("the live query answers")
                    .unwrap();
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
            uploads
                .next()
                .await
                .expect("the live query answers")
                .unwrap();
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
    let f = Fixture::with_clock(Provenance::AppProvided, clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::Authentication, None))
        .await;
    let mut pending = f.subscribe_pending();
    f.files.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.upload_reason(&file).await.is_none() {
            pending
                .next()
                .await
                .expect("the live query answers")
                .unwrap();
        }
    })
    .await
    .unwrap();
    drop(f.files.inner.drain.lock().await);
    assert_eq!(retry_in(&f), None);
    let mut uploads = f.subscribe_queue();
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
            uploads
                .next()
                .await
                .expect("the live query answers")
                .unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(f.upload_reason(&file).await, None);
    f.close().await;
}
