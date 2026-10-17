//! Eager downloads retain recoverable provider failures until they can retry.
use super::*;
use crate::{PendingReason, Retry};

#[tokio::test]
async fn full_storage_retries_eager_downloads_under_backoff() {
    let clock = clock();
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
    let b = eager_reader(&a, &clock).await;
    apply_all(&a, &b).await;
    for wait in [1, 2, 4] {
        a.storage
            .set_faults(Faults {
                fail_next: 1,
                failure: StorageFailure::QuotaExceeded,
                ..Faults::none()
            })
            .await;
        b.files.sync_files().await.unwrap();
        let waits = b.files.inner.database.download_waits().await.unwrap();
        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].1.wait, Duration::from_secs(wait));
        let records = b.database.pending::<crate::PendingRecord>().await.unwrap();
        assert_eq!(records[0].retry(), Retry::Automatic);
        let requests = a.storage.request_trace().len();
        b.files.sync_files().await.unwrap();
        assert_eq!(a.storage.request_trace().len(), requests);
        advance(&b, &clock, wait);
    }
    b.files.sync_files().await.unwrap();
    assert_eq!(
        b.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    assert!(file_records(&b).await.is_empty());
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn eager_provider_failures_wait_for_connection_changes_only_when_recoverable() {
    for failure in [
        StorageFailure::Authentication,
        StorageFailure::PermissionDenied,
        StorageFailure::Protocol,
    ] {
        let clock = clock();
        let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
        let file = a.uploaded("eager", vec![72; CHUNK * 2]).await;
        let mut b = eager_reader(&a, &clock).await;
        apply_all(&a, &b).await;
        a.storage
            .set_faults(Faults {
                fail_next: 1,
                failure,
                ..Faults::none()
            })
            .await;
        b.files.sync_files().await.unwrap();
        let records = b.database.pending::<crate::PendingRecord>().await.unwrap();
        assert_eq!(
            records[0].reason,
            PendingReason::Storage {
                provider: coven_storage::CloudProvider::Dropbox,
                failure
            }
        );
        assert_eq!(
            records[0].retry(),
            if failure == StorageFailure::Protocol {
                Retry::Never
            } else {
                Retry::AppAction
            }
        );
        assert!(b
            .files
            .inner
            .database
            .download_waits()
            .await
            .unwrap()
            .is_empty());
        assert!(b.files.inner.next_retry().is_none());
        let requests = a.storage.request_trace().len();
        advance(&b, &clock, 600);
        b.files.sync_files().await.unwrap();
        assert_eq!(a.storage.request_trace().len(), requests);
        connect(&b, &a).await;
        b.files.sync_files().await.unwrap();
        if failure == StorageFailure::Protocol {
            b.reopen().await;
            connect(&b, &a).await;
            b.files.sync_files().await.unwrap();
            assert_eq!(a.storage.request_trace().len(), requests);
            assert_eq!(
                b.database.pending::<crate::PendingRecord>().await.unwrap(),
                records
            );
        } else {
            assert_eq!(
                b.files.inner.database.missing_bytes(&file).await.unwrap(),
                0
            );
            assert!(file_records(&b).await.is_empty());
        }
        b.close().await;
        a.close().await;
    }
}

#[tokio::test]
async fn cancellation_while_waiting_for_credentials_survives_reconnection() {
    let clock = clock();
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK]).await;
    let b = eager_reader(&a, &clock).await;
    apply_all(&a, &b).await;
    a.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::Authentication,
            ..Faults::none()
        })
        .await;
    b.files.sync_files().await.unwrap();
    let records = file_records(&b).await;
    b.files.cancel_eager_cache_fill().await.unwrap();
    let requests = a.storage.request_trace().len();
    connect(&b, &a).await;
    b.files.sync_files().await.unwrap();
    assert_eq!(a.storage.request_trace().len(), requests);
    assert_eq!(file_records(&b).await, records);
    assert_eq!(
        b.files.inner.database.missing_bytes(&file).await.unwrap(),
        file.plaintext_size()
    );
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn reconnect_wakes_the_eager_worker_without_a_pass() {
    let clock = clock();
    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let file = a.uploaded("eager", vec![72; CHUNK]).await;
    let b = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheEager, clock).await;
    let storage = Arc::new(a.storage.with_cooldown(b.files.inner.waits.clone()));
    b.files
        .set_storage(Some(storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    a.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::Authentication,
            ..Faults::none()
        })
        .await;
    let mut pending = b.database.subscribe_pending::<crate::PendingRecord>();
    let mut status = b.database.subscribe_eager_cache_fill_status();
    apply_all(&a, &b).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while file_records(&b).await.is_empty() {
            pending.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    drop(b.files.inner.eager_fill.lock().await);
    assert!(b.files.inner.next_retry().is_none());
    b.files
        .set_storage(Some(storage), std::future::ready(Ok(())))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while b.files.inner.database.missing_bytes(&file).await.unwrap() > 0 {
            status.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    b.close().await;
    a.close().await;
}

#[tokio::test]
async fn carried_pins_survive_a_provider_sign_in_failure() {
    let clock = clock();
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
    f.files.stop_background_for_counts().await;
    let file = f.attach("files", "pinned", vec![72; CHUNK]).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    f.files.set_uploads_paused(false).await.unwrap();
    f.files.inner.drain_uploads(false).await.unwrap();
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::Authentication,
            ..Faults::none()
        })
        .await;
    f.files.inner.fill_eager().await.unwrap();
    assert_eq!(
        f.files.inner.database.pin_requests().await.unwrap(),
        vec![file.clone()]
    );
    let requests = f.storage.request_trace().len();
    f.files.inner.fill_eager().await.unwrap();
    assert_eq!(f.storage.request_trace().len(), requests);
    f.files
        .set_storage(Some(f.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    f.files.inner.fill_eager().await.unwrap();
    assert!(f
        .files
        .inner
        .database
        .pin_requests()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        f.database
            .pin_progress(vec![file])
            .await
            .unwrap()
            .files_completed,
        1
    );
    f.close().await;
}

#[tokio::test]
#[ignore = "requires the Lean downloadRetryRunner"]
async fn io_differential_download_retries() {
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for (failure, clears) in [
        (StorageFailure::Network, "byItself"),
        (StorageFailure::QuotaExceeded, "byItself"),
        (StorageFailure::Authentication, "byPerson"),
        (StorageFailure::PermissionDenied, "byPerson"),
        (StorageFailure::Protocol, "never"),
    ] {
        for now in [0, 1, 2] {
            for provider in [0, 2] {
                for changed in [false, true] {
                    let clock = clock();
                    let a = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
                    a.uploaded("eager", vec![72; CHUNK]).await;
                    let b = eager_reader(&a, &clock).await;
                    apply_all(&a, &b).await;
                    a.storage
                        .set_faults(Faults {
                            fail_next: 1,
                            failure,
                            ..Faults::none()
                        })
                        .await;
                    b.files.sync_files().await.unwrap();
                    let timed = !b
                        .files
                        .inner
                        .database
                        .download_waits()
                        .await
                        .unwrap()
                        .is_empty();
                    coven_storage::Cooldown::record(
                        b.files.inner.waits.as_ref(),
                        Duration::from_secs(provider),
                    )
                    .await
                    .unwrap();
                    advance(&b, &clock, now);
                    if changed {
                        connect(&b, &a).await;
                    }
                    let requests = a.storage.request_trace().len();
                    b.files.sync_files().await.unwrap();
                    actual.push(serde_json::json!({
                        "timed": timed,
                        "retried": a.storage.request_trace().len() > requests,
                    }));
                    inputs.push(serde_json::json!({"clears": clears, "now": now, "provider": provider, "changed": changed}));
                    b.close().await;
                    a.close().await;
                }
            }
        }
    }
    let output = crate::tests::run_lean("COVEN_DOWNLOAD_RETRY_LEAN", &serde_json::json!(inputs));
    let expected: Vec<serde_json::Value> = serde_json::from_value(output).unwrap();
    assert_eq!(actual, expected, "cases: {inputs:?}");
}
