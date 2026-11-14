//! A download's outcome as the shared boundary records it.
use super::*;
use crate::{PendingReason, Retry};
use coven_foundation::files::FileLease;

/// Downloads receives the same controlled boundary outcomes as the real reader
/// and pin; completion and retry disposition come from its durable file
/// subject, which only a pin's failure records.
#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_download() {
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for (pin, (fetched, authenticated, hash, disk)) in [false, true].into_iter().flat_map(|pin| {
        [
            (true, true, true, true),
            (false, true, true, true),
            (false, false, true, true),
            (true, false, true, true),
            (true, true, false, true),
            (true, true, true, false),
        ]
        .map(|outcomes| (pin, outcomes))
    }) {
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
                        // The completing write is the one storing the file.
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
        let reference = b.database.file_ref(file.id()).await.unwrap();
        if !authenticated {
            a.storage.corrupt_byte(&a.path(&file), 15).await.unwrap();
        }
        a.storage.set_online(fetched);
        let cache = b.directory.area_path(coven_foundation::files::FileArea::Cache);
        let saved = cache.with_file_name("saved-cache");
        if !disk {
            // A file where the cache directory belongs causes a real local
            // persistence failure without depending on the process's UID.
            if cache.exists() {
                std::fs::rename(&cache, &saved).unwrap();
            }
            std::fs::write(&cache, b"not a directory").unwrap();
        }
        let result = if pin {
            b.files.pin(std::slice::from_ref(&reference)).await
        } else {
            b.files.read_file(&reference, &FileLease::for_test().await).await.map(|_bytes| ())
        };
        let pending = b.database.pending::<crate::PendingRecord>().await.unwrap();
        assert!(pending.len() <= 1, "{pending:?}");
        actual.push(serde_json::json!({"complete": result.is_ok(),
            "refused": pending.iter().any(|record| matches!(record.reason, PendingReason::Refused(_))),
            "next_call": pending.iter().any(|record| record.retry() == Retry::AppAction)}));
        inputs.push(serde_json::json!({"check":"download", "pin":pin, "fetched":fetched,
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

/// A read that succeeds settles the download conditions it checked: one
/// reaching the end of the file every one, one that stops short all but a
/// refusal (§17.7). Each case pins a two-chunk file first, offline or over a
/// second chunk that fails authentication, then reads its first chunk, or
/// the whole file once the pin's cause is gone.
#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_read_settles() {
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for (before, whole) in
        [("network", false), ("refused", false), ("network", true), ("refused", true)]
    {
        let f = Fixture::new(Provenance::AppProvided).await;
        let bytes = vec![73; CHUNK * 2];
        let file = f.uploaded("settle", bytes.clone()).await;
        let second_chunk = 11 + (CHUNK + 16) + 3;
        let refused = before == "refused";
        if refused {
            f.storage.corrupt_byte(&f.path(&file), second_chunk).await.unwrap();
        } else {
            f.storage.set_online(false);
        }
        assert!(f.files.pin(std::slice::from_ref(&file)).await.is_err());
        assert_eq!(download_retries(&f).await.len(), 1, "{before}");
        if refused && whole {
            f.storage.corrupt_byte(&f.path(&file), second_chunk).await.unwrap();
        }
        f.storage.set_online(true);
        if whole {
            assert_eq!(
                f.files.read_file(&file, &FileLease::for_test().await).await.unwrap(),
                bytes
            );
        } else {
            let stream =
                f.files.open_file_stream(&file, &FileLease::for_test().await).await.unwrap();
            stream.read_at(0, 1).await.unwrap();
        }
        let retries = download_retries(&f).await;
        actual.push(serde_json::json!({
            "refused": retries.contains(&Retry::AfterUpdate),
            "next_call": retries.contains(&Retry::AppAction)}));
        inputs.push(serde_json::json!({"check": "read_settles", "before": before, "whole": whole}));
        f.close().await;
    }
    assert_eq!(actual, crate::tests::io_runner(&inputs), "{inputs:?}");
}

/// A pin's failed download belongs to the file's transfers, not to sync: a
/// download's record that names a state of its own, as a full disk does,
/// leaves the sync state as it was (§19.1).
#[tokio::test]
async fn a_pins_failed_download_names_no_sync_state() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("pinned", vec![23; CHUNK]).await;
    f.files.stop_background_for_counts().await;
    let location = f.location(&file).await;
    f.files
        .inner
        .database
        .fail_download(crate::PendingRecord::new(
            crate::PendingSubject::Download {
                device: location.uploader(),
                object: location.object(),
            },
            crate::PendingReason::Failed(crate::LocalFailure::DiskFull),
        ))
        .await
        .unwrap();
    assert_eq!(file_pending(&f).await.len(), 1);
    let status = crate::sync_status(&f.database).await.unwrap();
    assert!(
        !matches!(status.state, crate::SyncState::Failed(_)),
        "a download's record names a state of sync: {status:?}"
    );
    f.close().await;
}
