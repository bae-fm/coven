//! A download's outcome as the shared boundary records it.
use super::*;
use crate::{PendingReason, Retry};

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
            "next_call": pending.iter().any(|record| record.retry() == Retry::AppAction)}));
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
