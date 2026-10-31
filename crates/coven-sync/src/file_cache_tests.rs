//! Reads and pins (§17.7, §17.8, E8): one file's failure as its own pending
//! record.
use super::*;
use coven_storage::StorageFailure;

#[tokio::test]
async fn local_reads_settle_only_the_source_condition_they_checked() {
    let f = Fixture::new(Provenance::UserProvided).await;
    let file = f.original("original", b"kept original").await;
    f.files.stop_background_for_counts().await;
    let source = f.root.path().join("original");
    let moved = f.root.path().join("moved");
    std::fs::rename(&source, &moved).unwrap();
    assert!(matches!(
        f.files.read_file(&file).await,
        Err(FileReadError::UserFileMissing { .. })
    ));
    // The original's move says it is missing, and retries.
    assert!(file_records(&f).await.is_empty());
    assert_eq!(
        f.move_reasons().await,
        [crate::PendingReason::Original {
            path: source.clone(),
            failure: crate::FileSourceFailure::Missing
        }]
    );
    let uploads = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(uploads[0].retry.unwrap().failures, 1);
    std::fs::rename(&moved, &source).unwrap();
    assert_eq!(f.files.read_file(&file).await.unwrap(), b"kept original");
    assert!(file_records(&f).await.is_empty());
    assert!(f.move_reasons().await.is_empty());
    assert!(f.files.inner.database.uploads().await.unwrap()[0]
        .retry
        .is_none());

    f.storage.set_online(false);
    f.files.set_uploads_paused(false).await.unwrap();
    f.files.inner.drain_uploads_retrying().await.unwrap();
    let failed_upload = file_records(&f).await;
    assert!(matches!(
        failed_upload.as_slice(),
        [crate::PendingReason::Storage {
            failure: StorageFailure::Network,
            ..
        }]
    ));
    assert_eq!(f.files.read_file(&file).await.unwrap(), b"kept original");
    assert!(matches!(
        f.files.pin(std::slice::from_ref(&file)).await,
        Err(FileReadError::DeviceOnly { .. })
    ));
    assert_eq!(file_records(&f).await, failed_upload);
    assert_eq!(f.queue().await.files.len(), 1);
    f.close().await;
}

/// An original whose name isn't UTF-8, as Linux allows, is prepared,
/// staged, read and uploaded like any other. Its move's record names it
/// while it is missing, by its path's own bytes, and the read that finds it
/// back settles that record.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_original_whose_name_isnt_utf8_is_read_and_uploaded() {
    use std::os::unix::ffi::OsStrExt;
    let f = Fixture::new(Provenance::UserProvided).await;
    let source = f
        .root
        .path()
        .join(std::ffi::OsStr::from_bytes(b"Caf\xE9.flac"));
    std::fs::write(&source, b"latin-1 track").unwrap();
    let file = f.original_at("cafe", &source).await;
    f.files.stop_background_for_counts().await;
    assert_eq!(f.files.read_file(&file).await.unwrap(), b"latin-1 track");
    let moved = f.root.path().join("moved");
    std::fs::rename(&source, &moved).unwrap();
    assert!(matches!(
        f.files.read_file(&file).await,
        Err(FileReadError::UserFileMissing { path, .. }) if path == source
    ));
    assert_eq!(
        f.move_reasons().await,
        [crate::PendingReason::Original {
            path: source.clone(),
            failure: crate::FileSourceFailure::Missing
        }]
    );
    std::fs::rename(&moved, &source).unwrap();
    assert_eq!(f.files.read_file(&file).await.unwrap(), b"latin-1 track");
    assert!(f.move_reasons().await.is_empty());
    f.drain().await;
    let stored = f.database.file_ref(file.id()).await.unwrap();
    assert!(matches!(stored.place(), FilePlace::Cloud { .. }));
    assert_eq!(f.files.read_file(&stored).await.unwrap(), b"latin-1 track");
    f.close().await;
}

#[tokio::test]
async fn read_only_downloads_keep_typed_errors_without_writing_pending_records() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("read-only", vec![24; 100]).await;
    f.files.stop_background_for_counts().await;
    let reader = builder(&f.directory, &f.ids, &f.clock, &f.namespaces)
        .open_read_only()
        .await
        .unwrap();
    let transfers = Arc::new(coven_storage::TransferBudget::new(
        crate::TransferLimits::default(),
    ));
    let files = Files::new(
        FileDatabase::read_only(reader.clone()),
        f.directory.clone(),
        Some(Arc::new(f.storage.with_transfers(transfers.clone()))),
        f.send.clone(),
        Arc::new(crate::RetryWaits::in_memory(f.clock.clone())),
        f.ids.clone(),
        transfers,
    );
    f.storage.set_online(false);
    assert!(
        matches!(files.read_file(&file).await, Err(FileReadError::Storage(ref error))
        if error.failure() == StorageFailure::Network)
    );
    f.storage.set_online(true);
    assert_eq!(files.read_file(&file).await.unwrap(), vec![24; 100]);
    assert!(file_records(&f).await.is_empty());
    files.close().await;
    reader.close().await.unwrap();
    f.close().await;
}

/// A read's failure goes to its caller and leaves nothing pending: only
/// that caller's next read tries again. A pin is a lasting wish for the
/// bytes, so its failure stays a pending record until a later pin keeps
/// them (§17.7, §17.8).
#[tokio::test]
async fn a_failed_read_records_nothing_and_a_failed_pin_records_until_a_pin_succeeds() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("explicit", vec![19; CHUNK * 2]).await;
    f.files.stop_background_for_counts().await;
    f.storage.set_online(false);
    let network = |error: FileReadError| matches!(error, FileReadError::Storage(ref error) if error.failure() == StorageFailure::Network);
    assert!(network(f.files.read_file(&file).await.unwrap_err()));
    assert!(file_records(&f).await.is_empty());
    assert_eq!(f.file_cause(&file).await, None);
    assert!(network(
        f.files.pin(std::slice::from_ref(&file)).await.unwrap_err()
    ));
    let failed = file_records(&f).await;
    assert!(matches!(
        failed.as_slice(),
        [crate::PendingReason::Storage {
            failure: StorageFailure::Network,
            ..
        }]
    ));
    // Only the app's next call retries it.
    assert_eq!(download_retries(&f).await, [crate::Retry::AppAction]);
    assert!(f.database.retry_waits().await.unwrap().is_empty());
    // The app call received the error; the record holds none.
    assert_eq!(f.file_cause(&file).await, None);
    // A read failing meanwhile leaves the pin's record as it was.
    assert!(network(f.files.read_file(&file).await.unwrap_err()));
    assert_eq!(file_records(&f).await, failed);
    f.storage.set_online(true);
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    assert!(file_records(&f).await.is_empty());
    f.close().await;
}

/// A download's record is about stored bytes some file over them names. The
/// write deleting the last such file's record ends it: no pin or read can
/// try those bytes again (§19.1).
#[tokio::test]
async fn deleting_the_last_file_over_failed_bytes_ends_their_download_record() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let kept = f.uploaded("kept", vec![21; CHUNK]).await;
    let file = f.uploaded("failed", vec![22; CHUNK]).await;
    f.files.stop_background_for_counts().await;
    f.storage.set_online(false);
    for failed in [&kept, &file] {
        assert!(f.files.pin(std::slice::from_ref(failed)).await.is_err());
    }
    assert_eq!(download_retries(&f).await.len(), 2);
    let failed = f.location(&file).await;
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='failed'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let kept_location = f.location(&kept).await;
    let only_kept = [crate::PendingSubject::Download {
        device: kept_location.uploader(),
        file: kept_location.object(),
    }];
    let subjects = |records: Vec<crate::PendingRecord>| {
        records
            .into_iter()
            .map(|record| record.subject)
            .collect::<Vec<_>>()
    };
    assert_eq!(subjects(file_pending(&f).await), only_kept);
    // A failure a pin in flight meanwhile reports records nothing either.
    f.files
        .inner
        .database
        .fail_download(crate::PendingRecord::new(
            crate::PendingSubject::Download {
                device: failed.uploader(),
                file: failed.object(),
            },
            crate::PendingReason::NoStorage,
        ))
        .await
        .unwrap();
    assert_eq!(subjects(file_pending(&f).await), only_kept);
    f.close().await;
}

/// An empty file's header is its whole object, so opening it reads storage:
/// with none connected, the open fails and records nothing, while a pin's
/// failure records the missing connection until a read or pin succeeds.
#[tokio::test]
async fn an_empty_files_failed_open_records_nothing_and_its_pin_records_no_storage() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("empty", Vec::new()).await;
    f.files
        .set_storage(None, std::future::ready(Ok(())))
        .await
        .unwrap();
    assert!(matches!(
        f.files.open_file_stream(&file).await,
        Err(FileReadError::NoStorage)
    ));
    assert!(file_records(&f).await.is_empty());
    assert!(matches!(
        f.files.pin(std::slice::from_ref(&file)).await,
        Err(FileReadError::NoStorage)
    ));
    assert_eq!(file_records(&f).await, [crate::PendingReason::NoStorage]);
    f.files
        .set_storage(Some(f.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert!(f.files.read_file(&file).await.unwrap().is_empty());
    assert!(file_records(&f).await.is_empty());
    f.close().await;
}

/// Bytes that fail authentication fail a read with the chunk's error and
/// record nothing; a pin's failure is a permanent refusal of those bytes.
#[tokio::test]
async fn an_authentication_failure_refuses_the_bytes_a_pin_downloads() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("damaged", vec![20; CHUNK]).await;
    f.storage.corrupt_byte(&f.path(&file), 15).await.unwrap();
    // The error keeps the chunk's authentication failure.
    assert!(matches!(
        f.files.read_file(&file).await,
        Err(FileReadError::Integrity { cause: Some(_), .. })
    ));
    assert!(file_records(&f).await.is_empty());
    assert!(matches!(
        f.files.pin(std::slice::from_ref(&file)).await,
        Err(FileReadError::Integrity { cause: Some(_), .. })
    ));
    assert!(matches!(
        file_records(&f).await.as_slice(),
        [crate::PendingReason::Refused(
            crate::Refusal::Decryption { .. }
        )]
    ));
    assert_eq!(download_retries(&f).await, [crate::Retry::AfterUpdate]);
    f.close().await;
}

/// `b` connected to `a`'s storage, with only explicit calls downloading.
async fn connect(b: &Fixture, a: &Fixture) {
    // Only the explicit runs below download.
    b.files.stop_background_for_counts().await;
    let storage = Arc::new(
        a.storage
            .with_cooldown(b.files.inner.waits.clone())
            .with_transfers(b.files.inner.transfers.clone()),
    );
    b.files
        .set_storage(Some(storage), std::future::ready(Ok(())))
        .await
        .unwrap();
}

/// This device's pending records about its uploads and downloads, in
/// subject order.
async fn file_pending(f: &Fixture) -> Vec<crate::PendingRecord> {
    f.database
        .pending::<crate::PendingRecord>()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| {
            matches!(
                record.subject,
                crate::PendingSubject::Upload { .. } | crate::PendingSubject::Download { .. }
            )
        })
        .collect()
}

/// This device's pending reasons about files, in subject order.
async fn file_records(f: &Fixture) -> Vec<crate::PendingReason> {
    file_pending(f)
        .await
        .into_iter()
        .map(|record| record.reason)
        .collect()
}

/// How each of this device's download records retries.
async fn download_retries(f: &Fixture) -> Vec<crate::Retry> {
    file_pending(f)
        .await
        .into_iter()
        .filter(|record| matches!(record.subject, crate::PendingSubject::Download { .. }))
        .map(|record| record.retry())
        .collect()
}

#[path = "file_cache_retry_tests.rs"]
mod retries;

/// A file still uploading is on this device only, so pinning it is refused,
/// as on every other device; stored, it pins, and reading it then needs no
/// request (§17.8, E8).
#[tokio::test]
async fn a_queued_file_is_pinned_only_once_stored() {
    for original in [false, true] {
        let provenance = match original {
            false => Provenance::AppProvided,
            true => Provenance::UserProvided,
        };
        let f = Fixture::new(provenance).await;
        let bytes = vec![81; CHUNK * 3];
        let queued = match original {
            false => f.attach("files", "kept", bytes.clone()).await,
            true => f.original("kept", &bytes).await,
        };
        assert!(matches!(
            f.files.pin(std::slice::from_ref(&queued)).await,
            Err(FileReadError::DeviceOnly { file, .. }) if file == queued.id()
        ));
        let pinned = || f.database.files_pinned(vec![queued.id()]);
        assert_eq!(pinned().await.unwrap(), [Some(false)]);
        f.drain().await;
        let kept = f.database.file_ref(queued.id()).await.unwrap();
        f.files.pin(std::slice::from_ref(&kept)).await.unwrap();
        assert_eq!(pinned().await.unwrap(), [Some(true)]);
        assert_eq!(
            f.database
                .pin_progress(vec![kept.clone()])
                .await
                .unwrap()
                .files_completed,
            1
        );
        let before = f.storage.request_count();
        assert_eq!(f.files.read_file(&kept).await.unwrap(), bytes);
        assert_eq!(f.storage.request_count(), before);
        f.close().await;
    }
}

/// Pin progress is a query over committed, verified cache coverage: bytes a
/// concurrent reader caches count, a failed read adds none, eviction removes
/// them, and reopening reads the same answer (§17.8, E8).
#[tokio::test]
async fn pin_progress_reads_committed_coverage() {
    let mut f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("progress", vec![77; CHUNK * 3]).await;
    let total = 3 * CHUNK as u64;
    let progress = |cached| crate::PinProgress {
        files_completed: 0,
        files_total: 1,
        bytes_cached: cached,
        bytes_total: total,
    };
    let mut live = f.database.subscribe_pin_progress(vec![file.clone()]);
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        progress(0)
    );
    // Another reader's verified chunk is coverage the moment it commits.
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream.read_at(CHUNK as u64, 1).await.unwrap();
    assert_eq!(
        live.next().await.expect("the live query answers").unwrap(),
        progress(CHUNK as u64)
    );
    // A read that fails authentication commits nothing.
    f.storage
        .corrupt_byte(&f.path(&file), 11 + 2 * (CHUNK + 16) + 3)
        .await
        .unwrap();
    assert!(matches!(
        stream.read_at(2 * CHUNK as u64, 1).await,
        Err(FileReadError::Integrity { .. })
    ));
    assert_eq!(
        f.database.pin_progress(vec![file.clone()]).await.unwrap(),
        progress(CHUNK as u64)
    );
    drop(stream);
    f.reopen().await;
    assert_eq!(
        f.database.pin_progress(vec![file.clone()]).await.unwrap(),
        progress(CHUNK as u64)
    );
    f.files.evict_file(&file).await.unwrap();
    assert_eq!(
        f.database.pin_progress(vec![file]).await.unwrap(),
        progress(0)
    );
    f.close().await;
}

/// The rows whose files `coven_downloading` lists.
async fn downloading(f: &Fixture) -> Vec<String> {
    f.database
        .read(|sql| {
            Ok(sql.query(
                "SELECT f.id FROM coven_downloading AS d JOIN files AS f ON f.file=d.file",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap()
}

/// A pin's download is listed in `coven_downloading` while it runs, and
/// gone once the file is kept whole (§4, §17.8).
#[tokio::test]
async fn a_pins_download_is_listed_while_it_runs() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("downloading", vec![5; CHUNK * 3]).await;
    f.files.stop_background_for_counts().await;
    assert!(downloading(&f).await.is_empty());
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    f.storage
        .hold_body_at(f.path(&file), 1, observed, resumed)
        .await;
    let pin = f.files.pin(std::slice::from_ref(&file));
    tokio::pin!(pin);
    tokio::select! {
        result = &mut pin => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    assert_eq!(downloading(&f).await, ["downloading"]);
    resume.send(()).unwrap();
    pin.await.unwrap();
    assert!(downloading(&f).await.is_empty());
    f.close().await;
}

/// Files in the store's cache folder, published or not.
fn cache_files(f: &Fixture) -> usize {
    std::fs::read_dir(
        f.root
            .path()
            .join("stores")
            .join(f.directory.id().to_string())
            .join("cache"),
    )
    .map(|entries| entries.count())
    .unwrap_or(0)
}

/// A reference taken before its record went still reads the bytes it
/// names, but the cache keeps none of them: the read caches nothing, and a
/// pin fails with `NoLongerStored` and records nothing (§17.8). Kept, they
/// would stay for good, a pinned copy exempt from eviction, since no record
/// could read or unpin them.
#[tokio::test]
async fn bytes_no_record_names_are_read_but_never_kept() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes: Vec<u8> = (0..CHUNK * 2).map(|i| (i % 241) as u8).collect();
    let file = f.uploaded("gone", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='gone'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    assert_eq!(cache_files(&f), 0, "the read keeps nothing");
    assert!(matches!(
        f.files.pin(std::slice::from_ref(&file)).await,
        Err(FileReadError::Database(DbError::NoLongerStored { file: id })) if id == file.id()
    ));
    assert_eq!(cache_files(&f), 0, "the pin keeps nothing");
    assert!(file_pending(&f).await.is_empty());
    f.close().await;
}

/// A fetched range is authenticated whole, then published as one file and
/// one row; reopening serves it without a request (§17.7, Appendix C.3).
#[tokio::test]
async fn a_verified_range_publishes_as_one_entry() {
    let mut f = Fixture::new(Provenance::AppProvided).await;
    let bytes: Vec<u8> = (0..CHUNK * 4).map(|i| (i % 249) as u8).collect();
    let file = f.uploaded("range", bytes.clone()).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(0, 4 * CHUNK as u64).await.unwrap(), bytes);
    drop(stream);
    // The header and one four-chunk range.
    assert_eq!(cache_files(&f), 2);
    f.reopen().await;
    let requests = f.storage.request_count();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    assert_eq!(
        stream
            .read_at(2 * CHUNK as u64 + 5, CHUNK as u64)
            .await
            .unwrap(),
        bytes[2 * CHUNK + 5..3 * CHUNK + 5]
    );
    assert_eq!(f.storage.request_count(), requests);
    f.close().await;
}

/// A chunk failing authentication in the middle of a range publishes none
/// of the range, and leaves no unnamed bytes behind.
#[tokio::test]
async fn a_failed_chunk_publishes_none_of_its_range() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("damaged-range", vec![61; CHUNK * 3]).await;
    f.storage
        .corrupt_byte(&f.path(&file), 11 + (CHUNK + 16) + 3)
        .await
        .unwrap();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    assert!(matches!(
        stream.read_at(0, 3 * CHUNK as u64).await,
        Err(FileReadError::Integrity { .. })
    ));
    assert_eq!(cache_files(&f), 1, "only the header");
    assert_eq!(
        f.database
            .pin_progress(vec![file])
            .await
            .unwrap()
            .bytes_cached,
        0
    );
    f.close().await;
}

/// Overlapping readers share one transfer: the second waits for the first
/// range and reads it from the cache (§6.7).
#[tokio::test]
async fn overlapping_readers_fetch_each_byte_once() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes: Vec<u8> = (0..CHUNK * 4).map(|i| (i % 241) as u8).collect();
    let file = f.uploaded("shared", bytes.clone()).await;
    let first = f.files.open_file_stream(&file).await.unwrap();
    let second = f.files.open_file_stream(&file).await.unwrap();
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            delay: Duration::from_millis(50),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let path = f.path(&file);
    let before: u64 = f
        .storage
        .reads()
        .await
        .iter()
        .filter(|(read, _, _)| *read == path)
        .map(|(_, _, length)| *length)
        .sum();
    let (a, b) = tokio::join!(
        first.read_at(0, 3 * CHUNK as u64),
        second.read_at(CHUNK as u64, 3 * CHUNK as u64)
    );
    assert_eq!(a.unwrap(), bytes[..3 * CHUNK]);
    assert_eq!(b.unwrap(), bytes[CHUNK..]);
    let fetched: u64 = f
        .storage
        .reads()
        .await
        .iter()
        .filter(|(read, _, _)| *read == path)
        .map(|(_, _, length)| *length)
        .sum::<u64>()
        - before;
    // The header arrives with the first reader's range (§17.7).
    assert_eq!(fetched, 11 + 4 * (CHUNK as u64 + 16));
    f.close().await;
}

/// A cancelled fetch publishes nothing and leaves no bytes behind.
#[tokio::test]
async fn a_cancelled_range_leaves_no_bytes() {
    let mut f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("cancelled", vec![62; CHUNK * 2]).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            delay: Duration::from_millis(200),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stream.read_at(0, 1))
            .await
            .is_err()
    );
    drop(stream);
    f.storage
        .set_faults(coven_storage::test_utils::Faults::none())
        .await;
    f.reopen().await;
    // The header travels with the cancelled range, so nothing is cached.
    assert_eq!(cache_files(&f), 0);
    assert_eq!(
        f.database
            .pin_progress(vec![file])
            .await
            .unwrap()
            .bytes_cached,
        0
    );
    f.close().await;
}

/// A fetch stops at the first cached chunk after the ones it needs, and that
/// chunk's cached bytes, read once, finish the step (§17.7).
#[tokio::test]
async fn a_fetch_reads_the_cached_chunk_it_stops_at_once() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes: Vec<u8> = (0..CHUNK * 3).map(|i| (i % 239) as u8).collect();
    let file = f.uploaded("stops", bytes.clone()).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream.read_at(2 * CHUNK as u64, 1).await.unwrap();
    let counter = coven_foundation::test_work::Counter::default();
    assert_eq!(
        counter
            .run(stream.read_at(0, 3 * CHUNK as u64))
            .await
            .unwrap(),
        bytes
    );
    assert_eq!(counter.take().file_range_reads, 1);
    f.close().await;
}

/// Under a full budget, the cached chunk a fetch stops at counts as read
/// before the fetched range is cached, so the eviction that follows takes
/// another file's older chunk instead and the read needs no second request
/// (§17.8).
#[tokio::test]
async fn the_cached_chunk_a_fetch_stops_at_outlasts_older_entries() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("three", vec![71; CHUNK * 3]).await;
    let other = f.uploaded("other-file", vec![72; CHUNK]).await;
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream.read_at(2 * CHUNK as u64, 1).await.unwrap();
    f.files.read_file(&other).await.unwrap();
    // Both headers and three chunks: the fetched pair leaves room for one of
    // the two chunks already cached.
    f.files
        .set_cache_budget("files", 2 * 11 + 3 * (CHUNK as u64 + 16))
        .await
        .unwrap();
    let before = f.storage.ranges().await.len();
    assert_eq!(
        stream.read_at(0, 3 * CHUNK as u64).await.unwrap(),
        vec![71; CHUNK * 3]
    );
    let ranges = f.storage.ranges().await;
    assert_eq!(
        ranges[before..]
            .iter()
            .map(|range| (range.start(), range.len()))
            .collect::<Vec<_>>(),
        [(11, 2 * (CHUNK as u64 + 16))]
    );
    assert!(f
        .files
        .inner
        .database
        .cached(&other, 0)
        .await
        .unwrap()
        .is_none());
    f.close().await;
}

/// A pin publishes the whole file it wrote at the size its layout fixes,
/// without reading any of it back (§17.8, Appendix C.3).
#[tokio::test]
async fn a_pin_publishes_its_whole_file_without_reading_it_back() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes: Vec<u8> = (0..CHUNK * 2 + 7).map(|i| (i % 233) as u8).collect();
    let file = f.uploaded("pinned", bytes.clone()).await;
    let counter = coven_foundation::test_work::Counter::default();
    counter
        .run(f.files.pin(std::slice::from_ref(&file)))
        .await
        .unwrap();
    assert_eq!(counter.take().file_range_reads, 0);
    let progress = f.database.pin_progress(vec![file.clone()]).await.unwrap();
    assert_eq!(progress.files_completed, 1);
    let requests = f.storage.request_count();
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    assert_eq!(f.storage.request_count(), requests);
    f.close().await;
}

#[tokio::test]
async fn eviction_uses_chunk_recency_and_keeps_namespace_budgets_independent() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("one", vec![67; CHUNK * 4]).await;
    let other = f.attach("other", "other", vec![68; CHUNK]).await;
    f.drain().await;
    let other = f.database.file_ref(other.id()).await.unwrap();
    f.files.read_file(&other).await.unwrap();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    f.files
        .set_cache_budget("files", 11 + 2 * (CHUNK as u64 + 16))
        .await
        .unwrap();
    for index in [0, 1, 0, 2] {
        stream.read_at(index * CHUNK as u64, 1).await.unwrap();
    }
    assert!(f
        .files
        .inner
        .database
        .cached(&file, 0)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&file, 1)
        .await
        .unwrap()
        .is_none());
    assert!(f
        .files
        .inner
        .database
        .cached(&file, 2)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&file, -1)
        .await
        .unwrap()
        .is_some());
    let requests = f.storage.request_count();
    f.files.read_file(&other).await.unwrap();
    assert_eq!(f.storage.request_count(), requests);
    f.close().await;
}

#[tokio::test]
async fn opening_a_file_respects_a_zero_cache_budget_including_empty_files() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.set_cache_budget("files", 0).await.unwrap();
    for bytes in [Vec::new(), vec![71; CHUNK]] {
        let id = format!("length-{}", bytes.len());
        let file = f.uploaded(&id, bytes.clone()).await;
        let stream = f.files.open_file_stream(&file).await.unwrap();
        assert!(f
            .files
            .inner
            .database
            .cached(&file, -1)
            .await
            .unwrap()
            .is_none());
        assert_eq!(stream.read_at(0, bytes.len() as u64).await.unwrap(), bytes);
        assert!(f
            .files
            .inner
            .database
            .cached(&file, -1)
            .await
            .unwrap()
            .is_none());
    }
    f.close().await;
}

#[tokio::test]
async fn pinning_preserves_cached_tail_chunks_while_assembling_the_whole_file() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("cached-tail", vec![74; CHUNK * 4]).await;
    f.files
        .set_cache_budget("files", 11 + 2 * (CHUNK as u64 + 16))
        .await
        .unwrap();
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream
        .read_at(3 * CHUNK as u64, CHUNK as u64)
        .await
        .unwrap();
    // Only the cached tail is verified before pinning; the pin publishes the
    // rest with it as one checked file.
    let before = f.database.pin_progress(vec![file.clone()]).await.unwrap();
    assert_eq!(before.bytes_total, 4 * CHUNK as u64);
    assert_eq!(before.bytes_cached, CHUNK as u64);
    assert_eq!(before.files_completed, 0);
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    let after = f.database.pin_progress(vec![file]).await.unwrap();
    assert_eq!(after.bytes_cached, 4 * CHUNK as u64);
    assert_eq!(after.files_completed, 1);
    f.close().await;
}

#[tokio::test]
async fn unused_headers_do_not_displace_recently_read_chunks() {
    let f = Fixture::new(Provenance::AppProvided).await;
    // An empty file's header is its whole object, cached on opening.
    let old = f.uploaded("old-header", Vec::new()).await;
    f.files.open_file_stream(&old).await.unwrap();
    assert!(f
        .files
        .inner
        .database
        .cached(&old, -1)
        .await
        .unwrap()
        .is_some());
    let recent = f.uploaded("recent", vec![76; CHUNK]).await;
    f.files
        .set_cache_budget("files", 11 + CHUNK as u64 + 16)
        .await
        .unwrap();
    f.files
        .open_file_stream(&recent)
        .await
        .unwrap()
        .read_at(0, 1)
        .await
        .unwrap();
    assert!(f
        .files
        .inner
        .database
        .cached(&recent, 0)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&recent, -1)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .files
        .inner
        .database
        .cached(&old, -1)
        .await
        .unwrap()
        .is_none());
    f.close().await;
}
