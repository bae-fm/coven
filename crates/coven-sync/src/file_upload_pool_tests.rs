//! The upload worker keeps the upload limit's number of uploads in flight
//! while files keep arriving, and pausing, a cooldown or a metered network
//! the app keeps files off stops new starts (§17.9, E7).

use super::*;
use coven_foundation::clock::HANG_GUARD;
use coven_storage::test_utils::MemoryRequest;
use std::num::NonZeroUsize;
use tokio::sync::Semaphore;

/// Every create held at the gate until the test adds a permit.
async fn gated(f: &Fixture, uploads: usize) -> Arc<Semaphore> {
    let gate = Arc::new(Semaphore::new(0));
    f.storage.gate_creations(gate.clone()).await;
    f.files.set_transfer_limits(TransferLimits {
        uploads: NonZeroUsize::new(uploads).unwrap(),
        downloads: NonZeroUsize::MIN,
    });
    gate
}

/// The paths of the creates the provider has seen start, in order.
fn creates(f: &Fixture) -> Vec<coven_storage::ObjectPath> {
    f.storage
        .request_trace()
        .into_iter()
        .filter_map(|request| match request {
            MemoryRequest::Create(path) => Some(path),
            _ => None,
        })
        .collect()
}

async fn until_creates(f: &Fixture, count: usize) {
    let mut requests = f.storage.subscribe_requests();
    tokio::time::timeout(HANG_GUARD, async {
        while creates(f).len() < count {
            requests.changed().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{count} creates never started: {:?}", creates(f)));
}

async fn until_queued(f: &Fixture, count: usize) {
    let mut queue = f.subscribe_queue();
    tokio::time::timeout(HANG_GUARD, async {
        while queue.next().await.expect("the live query answers").unwrap().files.len() != count {}
    })
    .await
    .unwrap();
}

/// Each file goes in one request held open at the provider.
fn photo(n: u8) -> Vec<u8> {
    vec![n; 100]
}

/// Ben imports photos one after another while the first is uploading: three
/// go out at once, each one queued meanwhile starts as soon as a slot frees,
/// and raising the limit starts another without waiting for any to finish.
#[tokio::test]
async fn uploads_queued_during_an_upload_fill_every_free_slot() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let gate = gated(&f, 3).await;
    let mut files = vec![f.attach("files", "a", photo(1)).await];
    f.set_uploads_paused(false).await;
    until_creates(&f, 1).await;
    for (n, id) in ["b", "c", "d", "e"].into_iter().enumerate() {
        files.push(f.attach("files", id, photo(n as u8 + 2)).await);
    }
    // Where each upload sends its file, while all are queued.
    let mut paths = Vec::new();
    for file in &files {
        let location = f.location(file).await;
        paths.push(location.path(f.directory.id()));
    }
    until_creates(&f, 3).await;
    // Three requests are open at the provider and none has finished: the three
    // oldest files'. Uploads that start together reach the provider in either
    // order, so only which files went out is fixed.
    assert_eq!(
        creates(&f).into_iter().collect::<std::collections::BTreeSet<_>>(),
        paths[..3].iter().cloned().collect::<std::collections::BTreeSet<_>>()
    );
    assert_eq!(f.storage.peak_requests(), 3);
    // The queue shows the three being sent, with no progress until stored.
    let queue = f.queue().await;
    assert_eq!(
        queue.files.iter().map(|upload| (upload.phase, upload.progress)).collect::<Vec<_>>(),
        [
            (UploadPhase::Uploading, None),
            (UploadPhase::Uploading, None),
            (UploadPhase::Uploading, None),
            (UploadPhase::Waiting, None),
            (UploadPhase::Waiting, None),
        ]
    );
    // The first finishes; the oldest file queued meanwhile starts at once.
    gate.add_permits(1);
    until_creates(&f, 4).await;
    assert_eq!(creates(&f)[3], paths[3]);
    // A higher limit starts the last file with the others still open.
    f.files.set_transfer_limits(TransferLimits {
        uploads: NonZeroUsize::new(4).unwrap(),
        downloads: NonZeroUsize::MIN,
    });
    until_creates(&f, 5).await;
    assert_eq!(creates(&f)[4], paths[4]);
    assert!(f.queued(&files[1]).await);
    gate.add_permits(4);
    until_queued(&f, 0).await;
    assert_eq!(f.storage.peak_requests(), 4);
    for (n, file) in files.iter().enumerate() {
        assert_eq!(f.files.read_file(file).await.unwrap(), photo(n as u8 + 1));
    }
    f.close().await;
}

/// What stops new uploads in a test.
#[derive(Clone, Copy, Debug)]
enum Stop {
    Pause,
    Cooldown,
    /// The network turns metered while the app moves files only on
    /// unmetered networks (§17.9).
    Metered,
}

/// Pausing, a provider cooldown, and a metered network the app keeps files
/// off let the uploads in flight finish but start no other, however many
/// slots are free.
#[tokio::test]
async fn pausing_a_cooldown_and_a_metered_network_start_no_new_upload() {
    for stop in [Stop::Pause, Stop::Cooldown, Stop::Metered] {
        let f = Fixture::new(Provenance::AppProvided).await;
        let gate = gated(&f, 2).await;
        for id in ["a", "b", "c", "d"] {
            f.attach("files", id, photo(7)).await;
        }
        f.set_unmetered_only(true).await;
        f.set_uploads_paused(false).await;
        until_creates(&f, 2).await;
        match stop {
            Stop::Cooldown => coven_storage::Cooldown::record(
                f.files.inner.waits.as_ref(),
                Duration::from_secs(600),
            )
            .await
            .unwrap(),
            Stop::Pause => f.set_uploads_paused(true).await,
            Stop::Metered => f.network.set_metered(true),
        }
        gate.add_permits(2);
        until_queued(&f, 2).await;
        // The run ends once its attempts did, and started nothing more.
        drop(f.files.inner.drain.lock().await);
        assert_eq!(creates(&f).len(), 2, "{stop:?}");
        let queue = f.queue().await;
        assert!(queue.files.iter().all(|upload| upload.phase == UploadPhase::Waiting));
        f.close().await;
    }
}

/// Ben pauses uploads while two are in flight, his phone moves to cellular,
/// and both uploads then fail. Nothing was committed since the run last read
/// the queue, which held no bound upload, so no bound upload can be waiting:
/// the paused run ends, though the network's change made what it read stale
/// (§17.4).
#[tokio::test]
async fn a_paused_run_ends_when_no_bound_upload_could_have_been_queued() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let gate = gated(&f, 2).await;
    for id in ["a", "b"] {
        f.attach("files", id, photo(7)).await;
    }
    f.storage
        .set_faults(coven_storage::test_utils::Faults {
            fail_next: 2,
            failure: coven_storage::StorageFailure::QuotaExceeded,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    f.set_uploads_paused(false).await;
    until_creates(&f, 2).await;
    f.set_uploads_paused(true).await;
    // His phone moves to cellular, which makes the run's read stale without
    // queueing anything.
    f.network.set_metered(true);
    gate.add_permits(2);
    // The run ends once both attempts did.
    drop(f.files.inner.drain.lock().await);
    assert_eq!(creates(&f).len(), 2);
    let queue = f.queue().await;
    assert_eq!(queue.files.len(), 2);
    f.close().await;
}

/// Ben cancels a photo's move while its upload is in flight. The provider
/// stores the bytes, but no file switches to them: the upload is retired, and
/// the run counts no file uploaded (§17.3, §17.9).
#[tokio::test]
async fn an_upload_whose_move_was_cancelled_counts_no_file_uploaded() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.files.stop_background_for_counts().await;
    let gate = gated(&f, 1).await;
    let file = f.attach("files", "a", photo(1)).await;
    let id = f.upload_of(&file).await.expect("the photo is queued").of;
    f.set_uploads_paused(false).await;
    let inner = f.files.inner.clone();
    let run = tokio::spawn(async move { inner.drain_uploads().await });
    until_creates(&f, 1).await;
    f.database.write(move |sql| sql.cancel_move(id)).await.unwrap();
    gate.add_permits(1);
    assert_eq!(run.await.unwrap().unwrap(), DrainOutcome::Drained { uploaded: 0 });
    assert!(f.files.inner.database.all_uploads().await.unwrap().is_empty());
    let photo = f.database.file_ref(file.id()).await.unwrap();
    assert!(matches!(photo.place(), coven_database::FilePlace::DeviceOnly { .. }));
    f.close().await;
}

/// Ben moves files only on unmetered networks. On cellular his photo
/// doesn't upload, and an early retry says the network is metered; once his
/// phone joins Wi-Fi it goes up with no call of his app's. Turning the
/// setting off lets the next one go on cellular too (§17.9).
#[tokio::test]
async fn a_metered_network_holds_trailing_uploads_until_it_is_unmetered() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.set_unmetered_only(true).await;
    f.network.set_metered(true);
    f.set_uploads_paused(false).await;
    f.attach("files", "a", photo(1)).await;
    assert_eq!(
        tokio::time::timeout(HANG_GUARD, f.files.retry_uploads_now()).await.unwrap().unwrap(),
        DrainOutcome::Metered
    );
    assert!(creates(&f).is_empty());
    f.network.join(false);
    until_queued(&f, 0).await;
    assert_eq!(creates(&f).len(), 1);
    f.network.join(true);
    f.attach("files", "b", photo(2)).await;
    assert_eq!(f.files.inner.drain_uploads().await.unwrap(), DrainOutcome::Metered);
    f.set_unmetered_only(false).await;
    until_queued(&f, 0).await;
    assert_eq!(creates(&f).len(), 2);
    f.close().await;
}

/// The upload worker panics as Ben's photo starts uploading: his app's
/// early retry hears the bug instead of waiting for a run that never
/// comes, and so does a retry asked after it (E15).
#[tokio::test]
async fn a_panicking_upload_worker_answers_app_retries_with_its_bug() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let faults = coven_storage::test_utils::Faults {
        panic_file_uploads: 1,
        ..coven_storage::test_utils::Faults::none()
    };
    f.storage.set_faults(faults).await;
    f.attach("files", "a", photo(1)).await;
    f.set_uploads_paused(false).await;
    let answered = tokio::time::timeout(HANG_GUARD, f.files.retry_uploads_now())
        .await
        .expect("the retry is answered");
    let Err(SyncError::Bug(bug)) = answered else {
        panic!("the retry heard no bug: {answered:?}");
    };
    assert_eq!(bug.operation, "the upload worker");
    assert!(bug.message.starts_with("the upload of"), "{bug:?}");
    assert!(
        matches!(f.files.retry_uploads_now().await, Err(SyncError::Bug(later)) if later == bug),
        "a later retry hears the bug"
    );
    assert_eq!(f.bugs.take(), [bug], "the app's bug sink heard it too");
    f.close().await;
}

/// A write of the retry effect fails, as on a full disk, and fails the
/// handle: the upload worker ends on that failure, and Ben's app's early
/// retry, and any after it, hear it instead of waiting for a run that never
/// comes.
#[tokio::test]
async fn an_upload_worker_ended_by_a_failed_handle_answers_app_retries() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.database.test_fail_retry_effect().await.unwrap();
    assert!(f.files.inner.waits.sync_stopped().await.is_err(), "the write failed the handle");
    for _ in 0..2 {
        let answered = tokio::time::timeout(HANG_GUARD, f.files.retry_uploads_now())
            .await
            .expect("the retry is answered");
        let Err(error) = answered else { panic!("the retry heard no failure: {answered:?}") };
        assert!(
            std::iter::successors(Some(&error as &dyn std::error::Error), |e| e.source())
                .any(|e| e.to_string().contains("injected retry effect failure")),
            "{error:?}"
        );
    }
    f.close().await;
}

/// An app retry joins the run in progress rather than waiting for it: the
/// run takes it at its next read of the queue, covering the upload still in
/// flight and the one waiting, and answers once both attempts have ended.
#[tokio::test]
async fn an_app_retry_joins_the_run_in_progress() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let gate = gated(&f, 2).await;
    for id in ["a", "b", "c"] {
        f.attach("files", id, photo(9)).await;
    }
    f.set_uploads_paused(false).await;
    until_creates(&f, 2).await;
    // Both slots are busy, so the run reads the queue, and takes the retry,
    // only once the first upload ends.
    let answer = f.files.inner.ask_retry().unwrap();
    gate.add_permits(1);
    until_creates(&f, 3).await;
    gate.add_permits(2);
    assert_eq!(
        tokio::time::timeout(HANG_GUARD, answer.answered()).await.unwrap().unwrap(),
        DrainOutcome::Drained { uploaded: 2 }
    );
    until_queued(&f, 0).await;
    assert_eq!(f.storage.peak_requests(), 2);
    f.close().await;
}

/// An app that resumes uploads and then retries is never told uploads are
/// paused, even when the worker's run read the pause before the resume
/// committed: the resume's change waits until the run has acted on what it
/// read, so the retry it asks next is a later run's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_after_resuming_is_never_answered_paused() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.set_uploads_paused(true).await;
    let (read, reading) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    f.files.inner.hold_after_settings_read((read, held));
    // The attachment wakes the worker, whose run reads the pause.
    f.attach("files", "photo", photo(1)).await;
    tokio::time::timeout(HANG_GUARD, reading).await.expect("the run reads the settings").unwrap();
    let waiting = f.files.inner.change_waits_from_now();
    let retried = async {
        f.set_uploads_paused(false).await;
        f.files.retry_uploads_now().await.unwrap()
    };
    let release = async {
        waiting.await;
        resume.send(()).unwrap();
    };
    let (answer, ()) = tokio::join!(retried, release);
    assert_ne!(answer, DrainOutcome::Paused);
    f.close().await;
}

/// A pause that starts while a file uploads ends the run once that upload
/// ends. An app that resumes and retries as the run ends is still not told
/// uploads are paused: the gate holds the run where it found itself paused
/// with nothing in flight while the app resumes and asks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_after_resuming_as_a_paused_run_ends_is_not_answered_paused() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let gate = gated(&f, 1).await;
    f.attach("files", "a", photo(1)).await;
    f.attach("files", "b", photo(2)).await;
    f.set_uploads_paused(false).await;
    until_creates(&f, 1).await;
    f.set_uploads_paused(true).await;

    let (reached, mut ending) = tokio::sync::mpsc::unbounded_channel();
    let (proceed, held) = std::sync::mpsc::channel::<()>();
    // Only the first end waits; later runs pass straight through.
    let held = Mutex::new(Some(held));
    *f.files.inner.blocked_end_gate.lock().unwrap() = Some(Arc::new(move || {
        if let Some(held) = held.lock().unwrap().take() {
            reached.send(()).unwrap();
            held.recv().unwrap();
        }
    }));
    gate.add_permits(1);
    tokio::time::timeout(HANG_GUARD, ending.recv())
        .await
        .expect("the run ends paused once its upload ends")
        .unwrap();

    f.set_uploads_paused(false).await;
    let answer = f.files.inner.ask_retry().unwrap();
    proceed.send(()).unwrap();
    gate.add_permits(1);
    assert_eq!(
        tokio::time::timeout(HANG_GUARD, answer.answered())
            .await
            .expect("the retry is answered")
            .unwrap(),
        DrainOutcome::Drained { uploaded: 1 }
    );
    f.close().await;
}

/// A file sent in one request shows as uploading while its request is open,
/// with no progress: only storage's confirmation retires it (E7).
#[tokio::test]
async fn a_single_request_is_uploading_without_progress_until_completion_retires_it() {
    let f = Fixture::new(Provenance::AppProvided).await;
    f.attach("files", "single", vec![44; 19]).await;

    let (started, receiving) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(coven_storage::ObjectPrefix::files(f.directory.id()), started, resume)
        .await;
    f.set_uploads_paused(false).await;
    let files = f.files.clone();
    let task = tokio::spawn(async move { files.retry_uploads_now().await });
    tokio::time::timeout(HANG_GUARD, receiving).await.unwrap().unwrap();
    // The file is being sent, but bytes in flight are not progress: it has
    // none until storage confirms and completion retires it.
    let mut uploads = f.subscribe_queue();
    let queued = uploads.next().await.expect("the live query answers").unwrap().files.remove(0);
    assert_eq!((queued.phase, queued.progress), (UploadPhase::Uploading, None));
    release.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert!(uploads.next().await.expect("the live query answers").unwrap().files.is_empty());
    f.close().await;
}

/// Ana's outbox counts the files being sent as one live query over
/// `coven_uploads`, which runs again as each upload starts or ends, with no
/// write: the worker's attempts are what `sending` reads (E4, E7).
#[tokio::test]
async fn the_worker_s_attempts_are_what_sending_reads() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let gate = gated(&f, 2).await;
    let mut files = Vec::new();
    for (n, id) in ["a", "b", "c"].into_iter().enumerate() {
        files.push(f.attach("files", id, photo(n as u8)).await.id().to_string());
    }
    let mut counts: coven_database::LiveQuery<_, DbError> = f.database.subscribe(|sql| {
        let sending =
            sql.query("SELECT s.file FROM coven_sending AS s ORDER BY s.file", [], |row| {
                row.get::<_, String>(0)
            })?;
        let (counted, queued) = sql.query_row(
            "SELECT count(*) FILTER (WHERE sending),count(*) FROM coven_uploads",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        Ok((sending, counted, queued))
    });
    // The files being sent, how many `sending` counts, and how many are queued.
    type Outbox = (Vec<String>, i64, i64);
    let mut until = async |done: &dyn Fn(&Outbox) -> bool| loop {
        let answer = tokio::time::timeout(HANG_GUARD, counts.next())
            .await
            .expect("the live query answers")
            .expect("the live query is open")
            .unwrap();
        if done(&answer) {
            break answer;
        }
    };
    assert_eq!(until(&|_| true).await, (Vec::new(), 0, 3));
    f.set_uploads_paused(false).await;
    until_creates(&f, 2).await;
    let (sending, counted, queued) = until(&|(sending, _, _)| sending.len() == 2).await;
    let mut first = files[..2].to_vec();
    first.sort();
    assert_eq!((sending, counted, queued), (first, 2, 3));
    // One finishes and the last starts.
    gate.add_permits(1);
    until_creates(&f, 3).await;
    let (sending, counted, queued) = until(&|(sending, _, queued)| {
        *queued == 2 && sending.len() == 2 && sending.contains(&files[2])
    })
    .await;
    assert!(sending.iter().all(|file| files.contains(file)));
    assert_eq!((counted, queued), (2, 2));
    gate.add_permits(2);
    assert_eq!(
        until(&|(sending, _, queued)| sending.is_empty() && *queued == 0).await,
        (Vec::new(), 0, 0)
    );
    f.close().await;
}

/// One create carries a file up to the provider's limit, across several
/// encryption chunks; one byte past it, or past the buffered bound on a
/// provider accepting more, the file goes up in a recorded session (§17.9).
#[tokio::test]
async fn a_file_goes_in_one_request_up_to_the_providers_limit() {
    use coven_storage::BUFFERED_REQUEST_LIMIT;
    let encrypted =
        |plain: usize| coven_format::file::FileHeader::new(plain as u64).encrypted_size().unwrap();
    let small = CHUNK * 3 + 18;
    // Plaintext whose encryption is exactly the buffered bound.
    let bound = (BUFFERED_REQUEST_LIMIT - 11) as usize / (CHUNK + 16) * CHUNK
        + ((BUFFERED_REQUEST_LIMIT - 11) as usize % (CHUNK + 16) - 16);
    assert_eq!(encrypted(bound), BUFFERED_REQUEST_LIMIT);
    for (plain, limit, single) in [
        (small, encrypted(small) + 1, true),
        (small, encrypted(small), true),
        (small, encrypted(small) - 1, false),
        (bound, 2 * BUFFERED_REQUEST_LIMIT, true),
        (bound + 1, 2 * BUFFERED_REQUEST_LIMIT, false),
    ] {
        let clock: coven_foundation::clock::ClockRef = std::sync::Arc::new(
            coven_foundation::clock::FixedClock::new(std::time::SystemTime::UNIX_EPOCH),
        );
        let f = Fixture::with_single_limit(Provenance::AppProvided, clock, limit).await;
        let bytes: Vec<u8> = (0..plain).map(|i| (i % 251) as u8).collect();
        let staged = f.attach("files", "song", bytes.clone()).await;
        let before = f.storage.request_trace().len();
        f.drain().await;
        let file = f.database.file_ref(staged.id()).await.unwrap();
        let sent: Vec<_> = f.storage.request_trace()[before..]
            .iter()
            .filter(|request| matches!(request, MemoryRequest::Create(_) | MemoryRequest::Other(_)))
            .cloned()
            .collect();
        let path = f.path(&file);
        if single {
            assert_eq!(sent, [MemoryRequest::Create(path.clone())], "{plain} {limit}");
        } else {
            assert_eq!(sent.first(), Some(&MemoryRequest::Other("begin_upload")));
            assert_eq!(sent.last(), Some(&MemoryRequest::Other("finish_upload")));
            assert!(!sent.contains(&MemoryRequest::Create(path.clone())));
        }
        assert!(!f.queued(&file).await);
        assert_eq!(f.storage.status(&path).await.unwrap().unwrap().size, encrypted(plain));
        assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
        f.close().await;
    }
}
