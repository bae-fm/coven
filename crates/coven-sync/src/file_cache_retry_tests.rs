//! Carried pins retain recoverable provider failures until they can retry.
use super::*;
use crate::{PendingReason, Retry};

#[tokio::test]
async fn full_storage_retries_carried_pins_under_backoff() {
    let clock = clock();
    let (f, file) = carried_pin(&clock, "pinned", &[72; CHUNK * 2]).await;
    for wait in [1, 2, 4] {
        f.storage
            .set_faults(Faults {
                fail_next: 1,
                failure: StorageFailure::QuotaExceeded,
                ..Faults::none()
            })
            .await;
        f.files.inner.keep_pins().await.unwrap();
        let waits = f.files.inner.database.download_waits().await.unwrap();
        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].1.wait, Duration::from_secs(wait));
        let records = f.database.pending::<crate::PendingRecord>().await.unwrap();
        assert_eq!(records[0].retry(), Retry::Automatic);
        // No caller awaits the run: its record carries the error.
        let cause = f.file_cause(&file).await.unwrap();
        assert!(cause.contains("QuotaExceeded"), "{cause}");
        let requests = f.storage.request_trace().len();
        f.files.inner.keep_pins().await.unwrap();
        assert_eq!(f.storage.request_trace().len(), requests);
        advance(&clock, wait);
    }
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(
        f.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    assert!(file_records(&f).await.is_empty());
    f.close().await;
}

#[tokio::test]
async fn pin_provider_failures_wait_for_connection_changes_only_when_recoverable() {
    for failure in [
        StorageFailure::Authentication,
        StorageFailure::PermissionDenied,
        StorageFailure::Protocol,
    ] {
        let clock = clock();
        let (mut f, file) = carried_pin(&clock, "pinned", &[72; CHUNK * 2]).await;
        f.storage
            .set_faults(Faults {
                fail_next: 1,
                failure,
                ..Faults::none()
            })
            .await;
        f.files.inner.keep_pins().await.unwrap();
        let records = f.database.pending::<crate::PendingRecord>().await.unwrap();
        assert_eq!(
            records[0].reason,
            PendingReason::Storage {
                provider: coven_storage::CloudProvider::Dropbox,
                failure,
                answered: false,
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
        assert!(f
            .files
            .inner
            .database
            .download_waits()
            .await
            .unwrap()
            .is_empty());
        assert!(f.files.inner.next_pin_retry().is_none());
        let requests = f.storage.request_trace().len();
        advance(&clock, 600);
        f.files.inner.keep_pins().await.unwrap();
        assert_eq!(f.storage.request_trace().len(), requests);
        reconnect(&f).await;
        f.files.inner.keep_pins().await.unwrap();
        if failure == StorageFailure::Protocol {
            f.reopen().await;
            reconnect(&f).await;
            f.files.inner.keep_pins().await.unwrap();
            assert_eq!(f.storage.request_trace().len(), requests);
            assert_eq!(
                f.database.pending::<crate::PendingRecord>().await.unwrap(),
                records
            );
        } else {
            assert_eq!(
                f.files.inner.database.missing_bytes(&file).await.unwrap(),
                0
            );
            assert!(file_records(&f).await.is_empty());
        }
        f.close().await;
    }
}

#[tokio::test]
async fn reconnect_wakes_the_pin_worker_without_a_pass() {
    let clock = clock();
    let f = Fixture::with_clock(Provenance::UserProvided, clock).await;
    let storage = Arc::new(f.storage.with_cooldown(f.files.inner.waits.clone()));
    f.files
        .set_storage(Some(storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    let file = f.original("pinned", &[72; CHUNK]).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    // The upload succeeds; the carried pin's read needs new credentials.
    f.storage
        .set_faults(Faults {
            read_failures: [(f.path(&file), StorageFailure::Authentication)].into(),
            ..Faults::none()
        })
        .await;
    let mut pending = f.database.subscribe_pending::<crate::PendingRecord>();
    let mut progress = f.database.subscribe_pin_progress(vec![file.clone()]);
    f.files.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while file_records(&f).await.is_empty() {
            pending
                .next()
                .await
                .expect("the live query answers")
                .unwrap();
        }
    })
    .await
    .unwrap();
    drop(f.files.inner.pin_run.lock().await);
    assert!(f.files.inner.next_pin_retry().is_none());
    f.storage.set_faults(Faults::none()).await;
    f.files
        .set_storage(Some(storage), std::future::ready(Ok(())))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let progress = progress
                .next()
                .await
                .expect("the live query answers")
                .unwrap();
            if progress.bytes_cached == progress.bytes_total {
                break;
            }
        }
    })
    .await
    .unwrap();
    f.close().await;
}

#[tokio::test]
async fn carried_pins_survive_a_provider_sign_in_failure() {
    let clock = clock();
    let (f, file) = carried_pin(&clock, "pinned", &[72; CHUNK]).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::Authentication,
            ..Faults::none()
        })
        .await;
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(
        f.files.inner.database.pin_requests().await.unwrap(),
        vec![file.clone()]
    );
    let requests = f.storage.request_trace().len();
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(f.storage.request_trace().len(), requests);
    reconnect(&f).await;
    f.files.inner.keep_pins().await.unwrap();
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
                    let (f, _) = carried_pin(&clock, "pinned", &[72; CHUNK]).await;
                    f.storage
                        .set_faults(Faults {
                            fail_next: 1,
                            failure,
                            ..Faults::none()
                        })
                        .await;
                    f.files.inner.keep_pins().await.unwrap();
                    let timed = !f
                        .files
                        .inner
                        .database
                        .download_waits()
                        .await
                        .unwrap()
                        .is_empty();
                    coven_storage::Cooldown::record(
                        f.files.inner.waits.as_ref(),
                        Duration::from_secs(provider),
                    )
                    .await
                    .unwrap();
                    advance(&clock, now);
                    if changed {
                        reconnect(&f).await;
                    }
                    let requests = f.storage.request_trace().len();
                    f.files.inner.keep_pins().await.unwrap();
                    actual.push(serde_json::json!({
                        "timed": timed,
                        "retried": f.storage.request_trace().len() > requests,
                    }));
                    inputs.push(serde_json::json!({"clears": clears, "now": now, "provider": provider, "changed": changed}));
                    f.close().await;
                }
            }
        }
    }
    let output = crate::tests::run_lean("COVEN_DOWNLOAD_RETRY_LEAN", &serde_json::json!(inputs));
    let expected: Vec<serde_json::Value> = serde_json::from_value(output).unwrap();
    assert_eq!(actual, expected, "cases: {inputs:?}");
}

/// Downloads receives the same controlled boundary outcomes as the real reader;
/// completion and retry disposition come from its durable file subject.
#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_download() {
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for (fetched, authenticated, hash, disk) in [
        (true, true, true, true),
        (false, true, true, true),
        (false, false, true, true),
        (true, false, true, true),
        (true, true, false, true),
        (true, true, true, false),
    ] {
        let a = Fixture::new(Provenance::AppProvided).await;
        let file = a.uploaded("download", vec![72; CHUNK]).await;
        let b = Fixture::new(Provenance::AppProvided).await;
        connect(&b, &a).await;
        for mut write in a.database.test_queued_writes().await.unwrap() {
            if !hash {
                for row in write.parts.iter_mut().flat_map(|part| &mut part.rows) {
                    if let coven_merge::Operation::Insert(columns)
                    | coven_merge::Operation::Update(columns) = &mut row.change.operation
                    {
                        // The completing write is the one naming the file.
                        if let Some(hash) = columns
                            .get_mut("hash")
                            .filter(|hash| hash.value != coven_format::value::Value::Null)
                        {
                            hash.value = coven_format::value::Value::Blob(vec![0; 32]);
                        }
                    }
                }
            }
            crate::tests::apply_write(&b.database, write).await.unwrap();
        }
        let reference = b.database.file_ref("files", "download").await.unwrap();
        if !authenticated {
            a.storage.corrupt_byte(&a.path(&file), 15).await.unwrap();
        }
        a.storage.set_online(fetched);
        let cache = b
            .root
            .path()
            .join("stores")
            .join(b.directory.id().to_string())
            .join("cache");
        let saved = cache.with_file_name("saved-cache");
        if !disk {
            // A file where the cache directory belongs causes a real local
            // persistence failure without depending on the process's UID.
            if cache.exists() {
                std::fs::rename(&cache, &saved).unwrap();
            }
            std::fs::write(&cache, b"not a directory").unwrap();
        }
        let result = b.files.read_file(&reference).await;
        let pending = b.database.pending::<crate::PendingRecord>().await.unwrap();
        assert!(pending.len() <= 1, "{pending:?}");
        actual.push(serde_json::json!({"complete": result.is_ok(),
            "refused": pending.iter().any(|record| matches!(record.reason, PendingReason::Refused(_))),
            "automatic": pending.iter().any(|record| record.retry() == Retry::Automatic)}));
        inputs.push(serde_json::json!({"check":"download", "fetched":fetched,
            "authenticated":authenticated, "hash":hash, "disk":disk}));
        if !disk {
            std::fs::remove_file(&cache).unwrap();
            if saved.exists() {
                std::fs::rename(&saved, &cache).unwrap();
            }
        }
        b.close().await;
        a.close().await;
    }
    assert_eq!(actual, crate::tests::io_runner(&inputs), "{inputs:?}");
}

/// A carried pin that found no network waits its backoff until the app's
/// network report ends it; one the provider answered keeps its wait (§6.7).
#[tokio::test]
async fn a_network_report_ends_only_an_offline_download_wait() {
    for answered in [false, true] {
        let clock = clock();
        let (f, file) = carried_pin(&clock, "pinned", &[72; CHUNK * 2]).await;
        f.storage
            .set_faults(Faults {
                fail_next: 1,
                failure: StorageFailure::Network,
                response: answered.then_some(503),
                ..Faults::none()
            })
            .await;
        f.files.inner.keep_pins().await.unwrap();
        let before = f.files.inner.database.download_waits().await.unwrap();
        assert_eq!(before[0].1.wait, Duration::from_secs(1));
        let waits = &f.files.inner.waits;
        waits.network_changed();
        waits
            .end_reported_waits(crate::NetworkOwner::Downloads)
            .await
            .unwrap();
        let after = f.files.inner.database.download_waits().await.unwrap();
        if answered {
            assert_eq!(after, before);
        } else {
            assert_eq!(after[0].1.failures, 1);
            assert!(after[0].1.wait.is_zero());
        }
        f.files.inner.keep_pins().await.unwrap();
        assert_eq!(
            f.files.inner.database.missing_bytes(&file).await.unwrap() == 0,
            !answered,
            "answered {answered}"
        );
        f.close().await;
    }
}

/// Ana pulls to refresh while her carried pin waits after the provider
/// answered with a server error, which a network report leaves alone. Her
/// retry ends that wait, keeping its failure count, and the download
/// completes now (§6.7).
#[tokio::test]
async fn the_persons_retry_ends_an_answered_download_wait() {
    let clock = clock();
    let (f, file) = carried_pin(&clock, "pinned", &[73; CHUNK * 2]).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::Network,
            response: Some(503),
            ..Faults::none()
        })
        .await;
    f.files.inner.keep_pins().await.unwrap();
    let before = f.files.inner.database.download_waits().await.unwrap();
    assert_eq!(before[0].1.wait, Duration::from_secs(1));
    let waits = &f.files.inner.waits;
    waits.network_changed();
    waits
        .end_reported_waits(crate::NetworkOwner::Downloads)
        .await
        .unwrap();
    assert_eq!(
        f.files.inner.database.download_waits().await.unwrap(),
        before
    );
    waits.retry_requested();
    assert!(waits
        .end_reported_waits(crate::NetworkOwner::Downloads)
        .await
        .unwrap());
    let after = f.files.inner.database.download_waits().await.unwrap();
    assert_eq!(after[0].1.failures, before[0].1.failures);
    assert!(after[0].1.wait.is_zero());
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(
        f.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    f.close().await;
}
