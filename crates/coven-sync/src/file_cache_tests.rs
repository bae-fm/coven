//! Reads, pins and carried pins (§17.5, §17.6, E8, §6.7): one file's
//! failure as its own pending record, and retries under persisted waits and
//! the provider cooldown (§6.7).
use super::*;
use coven_database::RetrySubject;
use coven_foundation::clock::{Clock, FixedClock};
use coven_storage::{test_utils::Faults, StorageFailure};

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
    assert_eq!(
        file_records(&f).await,
        [crate::PendingReason::FileUnavailable(
            crate::FileMissingReason::Source(crate::FileSourceFailure::Missing)
        )]
    );
    let uploads = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(uploads[0].retry.unwrap().failures, 1);
    assert!(f
        .files
        .inner
        .database
        .download_waits()
        .await
        .unwrap()
        .is_empty());
    std::fs::rename(&moved, &source).unwrap();
    assert_eq!(f.files.read_file(&file).await.unwrap(), b"kept original");
    assert!(file_records(&f).await.is_empty());
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
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(file_records(&f).await, failed_upload);
    assert_eq!(f.queue().await.files.len(), 1);
    f.close().await;
}

#[tokio::test]
async fn read_only_downloads_keep_typed_errors_without_writing_pending_records() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("read-only", vec![24; 100]).await;
    f.files.stop_background_for_counts().await;
    let reader = builder(&f.directory, &f.ids, &f.clock, &f.declarations)
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

#[tokio::test]
async fn explicit_reads_and_pins_record_failures_and_clear_them_after_success() {
    for pin in [false, true] {
        let f = Fixture::new(Provenance::AppProvided).await;
        let file = f.uploaded("explicit", vec![19; CHUNK * 2]).await;
        f.files.stop_background_for_counts().await;
        f.storage.set_online(false);
        let error = if pin {
            f.files.pin(std::slice::from_ref(&file)).await.unwrap_err()
        } else {
            f.files.read_file(&file).await.unwrap_err()
        };
        assert!(
            matches!(error, FileReadError::Storage(ref error) if error.failure() == StorageFailure::Network)
        );
        assert!(matches!(
            file_records(&f).await.as_slice(),
            [crate::PendingReason::Storage {
                failure: StorageFailure::Network,
                ..
            }]
        ));
        assert_eq!(
            f.files.inner.database.download_waits().await.unwrap().len(),
            1
        );
        // The app call received the error; the record holds none.
        assert_eq!(f.file_cause(&file).await, None);
        f.storage.set_online(true);
        if pin {
            f.files.pin(std::slice::from_ref(&file)).await.unwrap();
        } else {
            assert_eq!(f.files.read_file(&file).await.unwrap(), vec![19; CHUNK * 2]);
        }
        assert!(file_records(&f).await.is_empty());
        assert!(f
            .files
            .inner
            .database
            .download_waits()
            .await
            .unwrap()
            .is_empty());
        f.close().await;
    }
}

#[tokio::test]
async fn empty_file_open_records_a_missing_connection() {
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
    assert_eq!(file_records(&f).await, [crate::PendingReason::NoStorage]);
    f.files
        .set_storage(Some(f.storage.clone()), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert!(f.files.read_file(&file).await.unwrap().is_empty());
    assert!(file_records(&f).await.is_empty());
    f.close().await;
}

#[tokio::test]
async fn explicit_authentication_failure_is_a_permanent_file_refusal() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let file = f.uploaded("damaged", vec![20; CHUNK]).await;
    f.storage.corrupt_byte(&f.path(&file), 15).await.unwrap();
    // The error keeps the chunk's authentication failure.
    assert!(matches!(
        f.files.read_file(&file).await,
        Err(FileReadError::Integrity { cause: Some(_), .. })
    ));
    assert!(matches!(
        file_records(&f).await.as_slice(),
        [crate::PendingReason::Refused(
            crate::Refusal::Decryption { .. }
        )]
    ));
    assert!(f
        .files
        .inner
        .database
        .download_waits()
        .await
        .unwrap()
        .is_empty());
    f.close().await;
}

fn clock() -> Arc<FixedClock> {
    Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ))
}

/// Advance the retry clock by `seconds` (§6.7).
fn advance(clock: &FixedClock, seconds: u64) {
    clock.set(clock.now() + Duration::from_secs(seconds));
}

/// A device whose pinned original finished uploading: its pin is carried
/// to the stored file, which only the pin worker downloads (§17.6). Its
/// connection takes `Retry-After` as its own store's cooldown, and only the
/// explicit runs below download.
async fn carried_pin(clock: &Arc<FixedClock>, id: &str, bytes: &[u8]) -> (Fixture, FileRef) {
    let f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
    let file = f.original(id, bytes).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    reconnect(&f).await;
    f.files.set_uploads_paused(false).await.unwrap();
    f.files.inner.drain_uploads_retrying().await.unwrap();
    assert!(!f.queued(&file).await);
    assert_eq!(
        f.files.inner.database.pin_requests().await.unwrap(),
        vec![file.clone()]
    );
    (f, file)
}

/// A new connection to the device's own storage.
async fn reconnect(f: &Fixture) {
    connect(f, f).await;
}

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

/// A carried pin whose stored bytes fail authentication is refused for
/// good, and the run still keeps the next one whole (§17.6).
#[tokio::test]
async fn one_pins_failure_leaves_its_record_while_the_run_continues() {
    use crate::{PendingReason, PendingSubject, Refusal};
    let clock = clock();
    let f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
    let damaged = f.original("a-damaged", &[82; CHUNK + 5]).await;
    let good = f.original("b-good", &[84; CHUNK * 2]).await;
    f.files.pin(&[damaged.clone(), good.clone()]).await.unwrap();
    reconnect(&f).await;
    f.files.set_uploads_paused(false).await.unwrap();
    f.files.inner.drain_uploads_retrying().await.unwrap();
    f.storage
        .corrupt_byte(&f.path(&damaged), 11 + 3)
        .await
        .unwrap();
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(
        f.files.inner.database.missing_bytes(&good).await.unwrap(),
        0
    );
    let records = f.database.pending::<crate::PendingRecord>().await.unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(
        records[0].subject,
        PendingSubject::File {
            device: damaged.uploader(),
            file: damaged.id(),
        }
    );
    assert!(matches!(
        records[0].reason,
        PendingReason::Refused(Refusal::Decryption { .. })
    ));
    // A refusal settles its carried pin: no run fetches the bytes again.
    assert!(f
        .files
        .inner
        .database
        .pin_requests()
        .await
        .unwrap()
        .is_empty());
    let reads = reads_of(&f, &damaged).await;
    advance(&clock, 600);
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(reads_of(&f, &damaged).await, reads);
    f.close().await;
}

/// A pin run that cannot read its own work returns the database failure to
/// its worker, which logs it (§17.6).
#[tokio::test]
async fn a_pin_run_that_cannot_read_its_work_fails_its_step() {
    let clock = clock();
    let (f, _) = carried_pin(&clock, "pinned", &[72; CHUNK]).await;
    f.database.close().await.unwrap();
    assert!(matches!(
        f.files.inner.keep_pins().await,
        Err(crate::SyncError::Database(
            coven_database::DbError::StoreClosed
        ))
    ));
    f.files.close().await;
}

#[path = "file_cache_retry_tests.rs"]
mod retries;

/// A failed carried pin persists `T` and `W` with its record before it
/// waits: no run requests it again before its deadline, reopening included,
/// and the completed download takes the wait with its record (§6.7).
#[tokio::test]
async fn a_failed_download_is_not_requested_before_its_deadline() {
    let clock = clock();
    let (mut f, file) = carried_pin(&clock, "pinned", &[72; CHUNK * 2]).await;
    f.storage.set_online(false);
    f.files.inner.keep_pins().await.unwrap();
    let waits = f.files.inner.database.download_waits().await.unwrap();
    let [(subject, wait)] = waits.as_slice() else {
        panic!("one persisted wait: {waits:?}");
    };
    assert_eq!(
        *subject,
        crate::files::file_cache_retry::download_subject(&file)
    );
    assert_eq!(wait.failures, 1);
    assert_eq!(wait.wait, Duration::from_secs(1));
    assert_eq!(wait.not_before, clock.now() + Duration::from_secs(1));
    f.storage.set_online(true);
    let reads = reads_of(&f, &file).await;
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(reads_of(&f, &file).await, reads);
    // Reopening arms the remaining wait from the persisted row.
    f.reopen().await;
    reconnect(&f).await;
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(reads_of(&f, &file).await, reads);
    assert_eq!(file_records(&f).await.len(), 1);
    advance(&clock, 1);
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(
        f.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    assert!(file_records(&f).await.is_empty());
    assert!(f
        .files
        .inner
        .database
        .download_waits()
        .await
        .unwrap()
        .is_empty());
    f.close().await;
}

/// A `Retry-After` a download receives is the store's cooldown: no carried
/// pin, even one whose own wait ended, and no upload is requested until it
/// ends (§6.7).
#[tokio::test]
async fn a_provider_cooldown_holds_carried_pins_and_uploads_alike() {
    let clock = clock();
    let (f, file) = carried_pin(&clock, "pinned", &[72; CHUNK * 2]).await;
    f.storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            retry_after: Some(600),
            ..Faults::none()
        })
        .await;
    f.files.inner.keep_pins().await.unwrap();
    let cooldown = f
        .database
        .retry_wait(RetrySubject::Provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cooldown.wait, Duration::from_secs(600));
    let photo = f.attach("other", "photo", b"photo".to_vec()).await;
    // The download's own one-second wait ends; the cooldown still holds it
    // and the upload queue.
    advance(&clock, 1);
    let requests = f.storage.request_trace().len();
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(
        f.files.inner.drain_uploads().await.unwrap(),
        DrainOutcome::Drained { uploaded: 0 }
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    advance(&clock, 599);
    f.files.inner.keep_pins().await.unwrap();
    assert_eq!(
        f.files.inner.database.missing_bytes(&file).await.unwrap(),
        0
    );
    f.files.inner.drain_uploads().await.unwrap();
    assert!(!f.queued(&photo).await);
    f.close().await;
}

/// The pin worker wakes when a download's wait ends, without a pass or
/// another commit (§6.7).
#[tokio::test]
async fn the_worker_retries_a_download_when_its_wait_ends() {
    let clock = clock();
    let f = Fixture::with_clock(Provenance::UserProvided, clock.clone()).await;
    f.files
        .set_storage(
            Some(Arc::new(
                f.storage
                    .with_cooldown(f.files.inner.waits.clone())
                    .with_transfers(f.files.inner.transfers.clone()),
            )),
            std::future::ready(Ok(())),
        )
        .await
        .unwrap();
    let file = f.original("pinned", &[72; CHUNK * 2]).await;
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    // The upload succeeds; the carried pin's first read finds no network.
    f.storage
        .set_faults(Faults {
            read_failures: [(f.path(&file), StorageFailure::Network)].into(),
            ..Faults::none()
        })
        .await;
    // The failure's record commits with its wait.
    let mut pending = f.database.subscribe_pending::<crate::PendingRecord>();
    let mut progress = f.database.subscribe_pin_progress(vec![file.clone()]);
    let mut rearmed = f.files.inner.waits.rearmed();
    f.files.set_uploads_paused(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while f
            .files
            .inner
            .database
            .download_waits()
            .await
            .unwrap()
            .is_empty()
        {
            pending.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    // The worker starts the wait's timer only after the record commits. A
    // clock set before then would move the timer's start, not its end.
    tokio::time::timeout(Duration::from_secs(5), async {
        while f
            .files
            .inner
            .waits
            .next_due(|subject| matches!(subject, RetrySubject::Download { .. }))
            .is_none()
        {
            rearmed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    f.storage.set_faults(Faults::none()).await;
    clock.set(clock.now() + Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let progress = progress.next().await.unwrap();
            if progress.bytes_cached == progress.bytes_total {
                break;
            }
        }
    })
    .await
    .unwrap();
    f.close().await;
}

/// A pin taken while the file is still queued here is kept through its
/// upload without the app pinning again: completing an app file's upload
/// pins coven's own copy, and an original's carries the pin to the stored
/// file, which the file worker keeps whole from storage. Either way reading
/// it then needs no request. A pin released while queued leaves nothing
/// pinned (§17.6, E8).
#[tokio::test]
async fn a_pin_taken_while_queued_survives_the_upload() {
    for original in [false, true] {
        let provenance = match original {
            false => Provenance::AppProvided,
            true => Provenance::UserProvided,
        };
        let f = Fixture::new(provenance).await;
        // Only the explicit pin run below keeps the carried pin.
        f.files.stop_background_for_counts().await;
        let bytes = vec![81; CHUNK * 3];
        let attach = |id: &'static str| {
            let f = &f;
            let bytes = bytes.clone();
            async move {
                match original {
                    false => f.attach("files", id, bytes).await,
                    true => f.original(id, &bytes).await,
                }
            }
        };
        let kept = attach("kept").await;
        let released = attach("released").await;
        f.files
            .pin(&[kept.clone(), released.clone()])
            .await
            .unwrap();
        f.files
            .unpin(std::slice::from_ref(&released))
            .await
            .unwrap();
        let pinned = || {
            f.database
                .rows_pinned("files", vec!["kept".into(), "released".into()])
        };
        assert_eq!(pinned().await.unwrap(), [Some(true), Some(false)]);
        f.drain().await;
        assert!(!f.queued(&kept).await);
        assert_eq!(pinned().await.unwrap(), [Some(true), Some(false)]);
        // A carried pin completes only once its whole copy is kept.
        assert_eq!(
            f.database
                .pin_progress(vec![kept.clone()])
                .await
                .unwrap()
                .files_completed,
            u64::from(!original)
        );
        f.files.inner.keep_pins().await.unwrap();
        assert_eq!(pinned().await.unwrap(), [Some(true), Some(false)]);
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
                .pin_progress(vec![kept.clone(), released.clone()])
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
/// them, and reopening reads the same answer (§17.6, E8).
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
    assert_eq!(live.next().await.unwrap(), progress(0));
    // Another reader's verified chunk is coverage the moment it commits.
    let stream = f.files.open_file_stream(&file).await.unwrap();
    stream.read_at(CHUNK as u64, 1).await.unwrap();
    assert_eq!(live.next().await.unwrap(), progress(CHUNK as u64));
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

/// A fetched range is authenticated whole, then published as one file and
/// one row; reopening serves it without a request (§17.5, Appendix C.3).
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
    // The header arrives with the first reader's range (§17.5).
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
