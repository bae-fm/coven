use crate::*;
use coven_storage::test_utils::{Faults, MemoryStorage};
use coven_storage::Storage;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// A store on in-memory S3 that takes one request up to `single_limit`
/// bytes and parts of `part_size`, with uploads paused and sync stopped.
async fn paused_store(
    single_limit: u64,
    part_size: usize,
) -> (tempfile::TempDir, Arc<MemoryStorage>, CovenHandle) {
    let root = tempfile::tempdir().unwrap();
    let app = TestCoven::new();
    let memory = Arc::new(
        MemoryStorage::builder()
            .clock(Arc::new(SystemClock))
            .transfer_limits(single_limit, part_size)
            .build()
            .unwrap(),
    );
    let layout = StoreLayout::new(root.path().into());
    let directory = app
        .create_closed_store(&layout, "Transfers", Arc::new(UuidIds))
        .await
        .unwrap();
    let handle =
        app.builder(layout)
            .storage_connector(memory.clone())
            .synced_tables(vec![SyncedTable::new("photos", RowIdentity::SharedKey)
                .file_column(FileColumn::new("file"))])
            .namespaces(vec![Namespace::new(
                "photos",
                Provenance::AppProvided,
                1 << 30,
            )])
            .migrations(vec![Migration::addition(
                1,
                "photos",
                "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT)",
            )])
            .open(directory.id())
            .await
            .unwrap();
    handle
        .setup_s3_storage(
            memory.config(),
            "Ana's phone",
            "key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    let mut status = handle.subscribe_sync_status();
    status
        .wait_for(|s| matches!(s, SyncStatus::Synced { .. }))
        .await
        .unwrap();
    handle.stop_sync();
    status
        .wait_for(|s| matches!(s, SyncStatus::Stopped))
        .await
        .unwrap();
    handle.unlock_store_key().await.unwrap();
    handle.set_uploads_paused(true).await.unwrap();
    (root, memory, handle)
}

/// Stage `bytes` for a new photo row and return its file.
async fn photo(handle: &CovenHandle, id: &str, bytes: Vec<u8>) -> FileRef {
    let key = id.to_string();
    let file = handle
        .write_with_files(
            move |batch| Ok(batch.put_file("photos", bytes)),
            move |sql, file| {
                sql.execute("INSERT INTO photos(id,file) VALUES(?1,?2)", (key, file))?;
                Ok(file)
            },
        )
        .await
        .unwrap();
    handle.file_ref(file).await.unwrap()
}

fn encrypted(file: &FileRef) -> u64 {
    coven_format::file::FileHeader::new(file.size())
        .encrypted_size()
        .unwrap()
}

/// Read changes until `done` says the file reached what the test waits for,
/// keeping each progress heard for the file in order.
async fn hear_until(
    watch: &mut TransferWatch,
    file: FileId,
    heard: &mut Vec<Option<TransferProgress>>,
    done: impl Fn(Option<TransferProgress>) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let changes = watch.next().await.expect("the watch is open");
            for change in changes {
                let now = match change {
                    TransferChange::Reset(files) => files.get(&file).copied(),
                    TransferChange::Updated {
                        file: moved,
                        progress,
                    } if moved == file => Some(progress),
                    TransferChange::Removed { file: ended } if ended == file => None,
                    _ => continue,
                };
                heard.push(now);
                if done(now) {
                    return;
                }
            }
        }
    })
    .await
    .expect("the file's progress arrived");
}

/// A photo sent in one request shows its whole encrypted size from the
/// start, and every byte handed over while the provider has yet to answer;
/// it is removed once stored (E7).
#[tokio::test]
async fn a_file_sent_in_one_request_shows_its_bytes_going() {
    let (_root, memory, handle) = paused_store(1024 * 1024, 64 * 1024).await;
    let file = photo(&handle, "beach", vec![7; 100 * 1024]).await;
    let total = encrypted(&file);
    let mut watch = handle.subscribe_upload_transfers();
    assert_eq!(
        watch.next().await,
        Some(vec![TransferChange::Reset(BTreeMap::new())])
    );
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    memory
        .set_faults(Faults {
            creation_gate: Some(gate.clone()),
            ..Faults::none()
        })
        .await;
    handle.set_uploads_paused(false).await.unwrap();
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, |now| {
        now.is_some_and(|now| now.bytes_transferred == total)
    })
    .await;
    assert_eq!(
        heard[0],
        Some(TransferProgress {
            bytes_transferred: 0,
            bytes_total: total
        }),
        "the total is known before any byte goes"
    );
    // Held for its reply, the photo is still being sent.
    let sending = handle
        .read(|sql| {
            Ok(
                sql.query_row("SELECT count(*) FROM coven_sending", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(sending, 1);
    gate.add_permits(1);
    hear_until(&mut watch, file.id(), &mut heard, |now| now.is_none()).await;
    handle.close().await.unwrap();
}

/// A photo sent in parts shows each part's bytes as they are handed over,
/// counted through the whole object (E7).
#[tokio::test]
async fn a_file_sent_in_parts_shows_each_parts_bytes() {
    let part = 64 * 1024;
    let (_root, memory, handle) = paused_store(part as u64, part).await;
    let file = photo(&handle, "album", vec![9; 3 * part]).await;
    let total = encrypted(&file);
    let mut watch = handle.subscribe_upload_transfers();
    watch.next().await.unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    memory
        .set_faults(Faults {
            part_gate: Some(gate.clone()),
            ..Faults::none()
        })
        .await;
    handle.set_uploads_paused(false).await.unwrap();
    let mut heard = Vec::new();
    // Each part is heard once its bytes are handed over, before the
    // provider confirms it: 64, 128 and 192 KiB, then the last 59 bytes.
    let mut reached = 0;
    while reached < total {
        reached = (reached + part as u64).min(total);
        hear_until(&mut watch, file.id(), &mut heard, |now| {
            now.is_some_and(|now| now.bytes_transferred == reached)
        })
        .await;
        gate.add_permits(1);
    }
    hear_until(&mut watch, file.id(), &mut heard, |now| now.is_none()).await;
    let moved: Vec<u64> = heard
        .iter()
        .flatten()
        .map(|progress| progress.bytes_transferred)
        .collect();
    assert!(moved.windows(2).all(|pair| pair[0] <= pair[1]), "{moved:?}");
    assert!(heard
        .iter()
        .flatten()
        .all(|progress| progress.bytes_total == total));
    handle.close().await.unwrap();
}

/// A pin's download shows its whole encrypted size from the start, and its
/// bytes as they arrive, before the file is kept; it is removed once kept
/// (E8).
#[tokio::test]
async fn a_pins_download_shows_its_bytes_arriving() {
    let (_root, memory, handle) = paused_store(1024 * 1024, 64 * 1024).await;
    let file = photo(&handle, "sunset", vec![3; 300 * 1024]).await;
    let total = encrypted(&file);
    handle.set_uploads_paused(false).await.unwrap();
    let mut queue = handle.subscribe(|sql| {
        Ok(
            sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    });
    while queue.next().await.unwrap().unwrap() != 0 {}
    let file = handle.file_ref(file.id()).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the upload stored the photo");
    };
    let mut watch = handle.subscribe_download_transfers();
    assert_eq!(
        watch.next().await,
        Some(vec![TransferChange::Reset(BTreeMap::new())])
    );
    let at = 150_000;
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    memory
        .hold_body_at(
            ObjectPath::file(handle.store_id(), location.uploader(), location.object()),
            at,
            observed,
            resumed,
        )
        .await;
    let pin = handle.pin(std::slice::from_ref(&file));
    tokio::pin!(pin);
    tokio::select! {
        result = &mut pin => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, |now| {
        now.is_some_and(|now| now.bytes_transferred == at)
    })
    .await;
    assert!(heard
        .iter()
        .flatten()
        .all(|progress| progress.bytes_total == total));
    let downloading = handle
        .read(|sql| {
            Ok(
                sql.query_row("SELECT count(*) FROM coven_downloading", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(downloading, 1);
    resume.send(()).unwrap();
    pin.await.unwrap();
    hear_until(&mut watch, file.id(), &mut heard, |now| now.is_none()).await;
    let moved: Vec<u64> = heard
        .iter()
        .flatten()
        .map(|progress| progress.bytes_transferred)
        .collect();
    assert!(moved.windows(2).all(|pair| pair[0] <= pair[1]), "{moved:?}");
    handle.close().await.unwrap();
}

/// Closing the store ends the watch.
#[tokio::test]
async fn closing_the_store_ends_the_upload_watch() {
    let (_root, _memory, handle) = paused_store(1024 * 1024, 64 * 1024).await;
    let mut watch = handle.subscribe_upload_transfers();
    watch.next().await.unwrap();
    handle.close().await.unwrap();
    assert_eq!(watch.next().await, None);
}
