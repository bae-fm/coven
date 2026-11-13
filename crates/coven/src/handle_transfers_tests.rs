use crate::*;
use coven_foundation::clock::HANG_GUARD;
use coven_storage::test_utils::{BodyFailure, Faults, MemoryStorage};
use coven_storage::Storage;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A store on in-memory S3 that takes one request up to `single_limit`
/// bytes and parts of `part_size`, with uploads paused and passes held.
async fn paused_store(
    single_limit: u64,
    part_size: usize,
) -> (tempfile::TempDir, Arc<MemoryStorage>, CovenHandle) {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let memory = Arc::new(
        MemoryStorage::builder()
            .clock(Arc::new(SystemClock))
            .transfer_limits(single_limit, part_size)
            .build()
            .unwrap(),
    );
    let directory = app.create_closed_store("Transfers", Arc::new(UuidIds)).await.unwrap();
    let connector = crate::tests::Connector::new(memory.clone());
    let handle =
        app.builder()
            .storage_connector(connector.clone())
            .synced_tables(vec![SyncedTable::new("photos", RowIdentity::SharedKey)
                .file_column(FileColumn::new("file"))])
            .namespaces(vec![Namespace::new("photos", Provenance::AppProvided, 1 << 30)])
            .migrations(vec![Migration::addition(
                1,
                "photos",
                "CREATE TABLE photos(id TEXT NOT NULL PRIMARY KEY,file TEXT)",
            )])
            .open(directory.id(), |_| ())
            .await
            .unwrap();
    handle
        .setup_s3_storage(memory.config(), "key".into(), SecretText::new("secret".into()), |_| {})
        .await
        .unwrap();
    // The first pass after setup ends synced.
    handle.test_last_pass().wait_for(|last| *last == Some(LastPass::Synced)).await;
    handle.test_hold_passes(true).await;
    handle.set_uploads_paused(true).await.unwrap();
    (root, memory, handle)
}

/// Stage `bytes` for a new photo row and return its file.
async fn photo(handle: &CovenHandle, id: &str, bytes: Vec<u8>) -> FileRef {
    let key = id.to_string();
    let file = handle
        .test_staged_write(
            move |batch| Ok::<_, CovenError>(batch.put_file("photos", bytes, crate::Place::Cloud)),
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
    coven_format::file::FileHeader::new(file.size()).encrypted_size().unwrap()
}

/// Let uploads go until the queue is empty, and return the file as stored,
/// with the path of its stored object.
async fn stored(handle: &CovenHandle, file: &FileRef) -> (FileRef, ObjectPath) {
    handle.set_uploads_paused(false).await.unwrap();
    let mut queue: crate::LiveQuery<_, crate::CovenError> = handle.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM coven_uploads", [], |r| r.get::<_, i64>(0))?)
    });
    while queue.next().await.unwrap().unwrap() != 0 {}
    let file = handle.file_ref(file.id()).await.unwrap();
    let FilePlace::Cloud { location, .. } = file.place() else {
        panic!("the upload stored the photo");
    };
    let object = location.path(handle.store_id());
    (file, object)
}

/// What a reader heard of one file: how far its transfer had got, or how
/// it ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Heard {
    Moved(TransferProgress),
    Ended(TransferEnd),
}

impl Heard {
    fn moved(self) -> Option<TransferProgress> {
        match self {
            Heard::Moved(progress) => Some(progress),
            Heard::Ended(_) => None,
        }
    }
}

/// Read changes until `done` says the file reached what the test waits for,
/// keeping each thing heard of the file in order.
async fn hear_until(
    watch: &mut TransferWatch,
    file: FileId,
    heard: &mut Vec<Heard>,
    done: impl Fn(Heard) -> bool,
) {
    tokio::time::timeout(HANG_GUARD, async {
        loop {
            let changes = watch.next().await.expect("the watch is open");
            for change in changes {
                let now = match change {
                    TransferChange::Reset(files) => match files.get(&file) {
                        Some(progress) => Heard::Moved(*progress),
                        None => continue,
                    },
                    TransferChange::Updated { file: moved, progress } if moved == file => {
                        Heard::Moved(progress)
                    }
                    TransferChange::Removed { file: ended, end } if ended == file => {
                        Heard::Ended(end)
                    }
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

/// Whether the file's transfer has ended.
fn ended(now: Heard) -> bool {
    matches!(now, Heard::Ended(_))
}

/// Whether the file has got `bytes` of the way.
fn reached(bytes: u64) -> impl Fn(Heard) -> bool {
    move |now| now.moved().is_some_and(|now| now.bytes_transferred == bytes)
}

/// The bytes moved, in the order heard.
fn moved(heard: &[Heard]) -> Vec<u64> {
    heard.iter().filter_map(|now| now.moved()).map(|now| now.bytes_transferred).collect()
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
    assert_eq!(watch.next().await, Some(vec![TransferChange::Reset(BTreeMap::new())]));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    memory.gate_creations(gate.clone()).await;
    handle.set_uploads_paused(false).await.unwrap();
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, reached(total)).await;
    assert_eq!(
        heard[0],
        Heard::Moved(TransferProgress { bytes_transferred: 0, bytes_total: total }),
        "the total is known before any byte goes"
    );
    // Held for its reply, the photo is still being sent.
    let sending = handle
        .read(|sql| {
            Ok::<_, CovenError>(
                sql.query_row("SELECT count(*) FROM coven_sending", [], |r| r.get::<_, i64>(0))?,
            )
        })
        .await
        .unwrap();
    assert_eq!(sending, 1);
    gate.add_permits(1);
    hear_until(&mut watch, file.id(), &mut heard, ended).await;
    assert_eq!(heard.last(), Some(&Heard::Ended(TransferEnd::Finished)));
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
    memory.gate_parts(gate.clone()).await;
    handle.set_uploads_paused(false).await.unwrap();
    let mut heard = Vec::new();
    // Each part is heard once its bytes are handed over, before the
    // provider confirms it: 64, 128 and 192 KiB, then the last 59 bytes.
    let mut reached = 0;
    while reached < total {
        reached = (reached + part as u64).min(total);
        hear_until(&mut watch, file.id(), &mut heard, self::reached(reached)).await;
        gate.add_permits(1);
    }
    hear_until(&mut watch, file.id(), &mut heard, ended).await;
    assert_eq!(heard.last(), Some(&Heard::Ended(TransferEnd::Finished)));
    let moved = moved(&heard);
    assert!(moved.windows(2).all(|pair| pair[0] <= pair[1]), "{moved:?}");
    assert!(heard.iter().filter_map(|now| now.moved()).all(|now| now.bytes_total == total));
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
    let (file, object) = stored(&handle, &file).await;
    let mut watch = handle.subscribe_download_transfers();
    assert_eq!(watch.next().await, Some(vec![TransferChange::Reset(BTreeMap::new())]));
    let at = 150_000;
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    memory.hold_body_at(object, at, observed, resumed).await;
    let pin = handle.pin(std::slice::from_ref(&file));
    tokio::pin!(pin);
    tokio::select! {
        result = &mut pin => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, reached(at)).await;
    assert!(heard.iter().filter_map(|now| now.moved()).all(|now| now.bytes_total == total));
    let downloading = handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query_row(
                "SELECT count(*) FROM coven_downloading",
                [],
                |r| r.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(downloading, 1);
    resume.send(()).unwrap();
    pin.await.unwrap();
    hear_until(&mut watch, file.id(), &mut heard, ended).await;
    assert_eq!(heard.last(), Some(&Heard::Ended(TransferEnd::Finished)));
    let moved = moved(&heard);
    assert!(moved.windows(2).all(|pair| pair[0] <= pair[1]), "{moved:?}");
    handle.close().await.unwrap();
}

/// A photo whose upload the provider refuses is heard ending as failed,
/// not as stored (E7).
#[tokio::test]
async fn a_refused_upload_is_heard_as_failed() {
    let (_root, memory, handle) = paused_store(1024 * 1024, 64 * 1024).await;
    let file = photo(&handle, "beach", vec![7; 100 * 1024]).await;
    let mut watch = handle.subscribe_upload_transfers();
    watch.next().await.unwrap();
    memory.set_faults(Faults { refuse_file_uploads: 1, ..Faults::none() }).await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    memory.gate_creations(gate.clone()).await;
    handle.set_uploads_paused(false).await.unwrap();
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, reached(encrypted(&file))).await;
    gate.add_permits(1);
    hear_until(&mut watch, file.id(), &mut heard, ended).await;
    assert_eq!(heard.last(), Some(&Heard::Ended(TransferEnd::Failed)));
    handle.close().await.unwrap();
}

/// A photo sent in parts that the pause stops between parts is heard ending
/// as held: it stays queued, and nothing failed (E7).
#[tokio::test]
async fn an_upload_the_pause_stops_is_heard_as_held() {
    let part = 64 * 1024;
    let (_root, memory, handle) = paused_store(part as u64, part).await;
    let file = photo(&handle, "album", vec![9; 3 * part]).await;
    let mut watch = handle.subscribe_upload_transfers();
    watch.next().await.unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    memory.gate_parts(gate.clone()).await;
    handle.set_uploads_paused(false).await.unwrap();
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, reached(part as u64)).await;
    handle.set_uploads_paused(true).await.unwrap();
    gate.add_permits(1);
    hear_until(&mut watch, file.id(), &mut heard, ended).await;
    assert_eq!(heard.last(), Some(&Heard::Ended(TransferEnd::Held)));
    handle.close().await.unwrap();
}

/// A pin's download whose bytes stop coming is heard ending as failed,
/// never as finished, and its failure goes to the pin's call (E8).
#[tokio::test]
async fn a_pins_failed_download_is_heard_as_failed() {
    let (_root, memory, handle) = paused_store(1024 * 1024, 64 * 1024).await;
    let file = photo(&handle, "sunset", vec![3; 300 * 1024]).await;
    let (file, object) = stored(&handle, &file).await;
    let mut watch = handle.subscribe_download_transfers();
    watch.next().await.unwrap();
    let at = 150_000;
    memory
        .set_faults(Faults {
            body_failures: [(object.clone(), BodyFailure::Interrupted(at + 1000))].into(),
            ..Faults::none()
        })
        .await;
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    memory.hold_body_at(object, at, observed, resumed).await;
    let pin = handle.pin(std::slice::from_ref(&file));
    tokio::pin!(pin);
    tokio::select! {
        result = &mut pin => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, reached(at)).await;
    resume.send(()).unwrap();
    pin.await.unwrap_err();
    hear_until(&mut watch, file.id(), &mut heard, ended).await;
    assert_eq!(heard.last(), Some(&Heard::Ended(TransferEnd::Failed)));
    handle.close().await.unwrap();
}

/// A pin's download whose caller stops waiting is heard ending as dropped
/// (E8).
#[tokio::test]
async fn a_pins_dropped_download_is_heard_as_dropped() {
    let (_root, memory, handle) = paused_store(1024 * 1024, 64 * 1024).await;
    let file = photo(&handle, "sunset", vec![3; 300 * 1024]).await;
    let (file, object) = stored(&handle, &file).await;
    let mut watch = handle.subscribe_download_transfers();
    watch.next().await.unwrap();
    let at = 150_000;
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel::<()>();
    memory.hold_body_at(object, at, observed, resumed).await;
    let mut pin = Box::pin(handle.pin(std::slice::from_ref(&file)));
    tokio::select! {
        result = &mut pin => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    let mut heard = Vec::new();
    hear_until(&mut watch, file.id(), &mut heard, reached(at)).await;
    drop(pin);
    hear_until(&mut watch, file.id(), &mut heard, ended).await;
    assert_eq!(heard.last(), Some(&Heard::Ended(TransferEnd::Dropped)));
    drop(resume);
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
