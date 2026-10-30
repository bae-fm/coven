//! The upload worker keeps the upload limit's number of uploads in flight
//! while files keep arriving, and pausing or a cooldown stops new starts
//! (§17.8, E7).

use super::*;
use coven_storage::test_utils::{Faults, MemoryRequest};
use std::num::NonZeroUsize;
use tokio::sync::Semaphore;

/// Every create held at the gate until the test adds a permit.
async fn gated(f: &Fixture, uploads: usize) -> Arc<Semaphore> {
    let gate = Arc::new(Semaphore::new(0));
    f.storage
        .set_faults(Faults {
            creation_gate: Some(gate.clone()),
            ..Faults::none()
        })
        .await;
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
    tokio::time::timeout(Duration::from_secs(10), async {
        while creates(f).len() < count {
            requests.changed().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{count} creates never started: {:?}", creates(f)));
}

async fn until_queued(f: &Fixture, count: usize) {
    let mut queue = f.subscribe_queue();
    tokio::time::timeout(Duration::from_secs(10), async {
        while queue
            .next()
            .await
            .expect("the live query answers")
            .unwrap()
            .files
            .len()
            != count
        {}
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
    f.files.set_uploads_paused(false).await.unwrap();
    until_creates(&f, 1).await;
    for (n, id) in ["b", "c", "d", "e"].into_iter().enumerate() {
        files.push(f.attach("files", id, photo(n as u8 + 2)).await);
    }
    // Where each upload sends its file, while all are queued.
    let mut paths = Vec::new();
    for file in &files {
        let location = f.location(file).await;
        paths.push(coven_storage::ObjectPath::file(
            f.directory.id(),
            location.uploader(),
            location.object(),
        ));
    }
    until_creates(&f, 3).await;
    // Three requests are open at the provider and none has finished: the three
    // oldest files'. Uploads that start together reach the provider in either
    // order, so only which files went out is fixed.
    assert_eq!(
        creates(&f)
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        paths[..3]
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    );
    assert_eq!(f.storage.peak_requests(), 3);
    // The queue shows the three being sent, with no progress until stored.
    let queue = f.queue().await;
    assert_eq!(
        queue
            .files
            .iter()
            .map(|upload| (upload.phase, upload.progress))
            .collect::<Vec<_>>(),
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

/// Pausing, and a provider cooldown, let the uploads in flight finish but
/// start no other, however many slots are free.
#[tokio::test]
async fn pausing_and_a_cooldown_start_no_new_upload() {
    for cooldown in [false, true] {
        let f = Fixture::new(Provenance::AppProvided).await;
        let gate = gated(&f, 2).await;
        for id in ["a", "b", "c", "d"] {
            f.attach("files", id, photo(7)).await;
        }
        f.files.set_uploads_paused(false).await.unwrap();
        until_creates(&f, 2).await;
        if cooldown {
            coven_storage::Cooldown::record(f.files.inner.waits.as_ref(), Duration::from_secs(600))
                .await
                .unwrap();
        } else {
            f.files.set_uploads_paused(true).await.unwrap();
        }
        gate.add_permits(2);
        until_queued(&f, 2).await;
        // The run ends once its attempts did, and started nothing more.
        drop(f.files.inner.drain.lock().await);
        assert_eq!(creates(&f).len(), 2);
        let queue = f.queue().await;
        assert!(queue
            .files
            .iter()
            .all(|upload| upload.phase == UploadPhase::Waiting));
        f.close().await;
    }
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
    f.files.set_uploads_paused(false).await.unwrap();
    until_creates(&f, 2).await;
    // Both slots are busy, so the run reads the queue, and takes the retry,
    // only once the first upload ends.
    let answer = f.files.inner.ask_retry().unwrap();
    gate.add_permits(1);
    until_creates(&f, 3).await;
    gate.add_permits(2);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), answer)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        DrainOutcome::Drained { uploaded: 2 }
    );
    until_queued(&f, 0).await;
    assert_eq!(f.storage.peak_requests(), 2);
    f.close().await;
}

/// An app that resumes uploads and then retries is never told uploads are
/// paused. Each attachment wakes the worker, whose run reads the pause setting;
/// the app resumes a different number of yields later each round, and other
/// reads keep the store's readers busy, as an app's screens do, so in some
/// rounds the run's read began before the resume committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_resuming_is_never_answered_paused() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let database = f.files.inner.database.clone();
            tokio::spawn(async move {
                loop {
                    database.uploads().await.unwrap();
                }
            })
        })
        .collect();
    for round in 0..300u16 {
        f.files.set_uploads_paused(true).await.unwrap();
        f.attach("files", &format!("photo-{round}"), photo(round as u8))
            .await;
        for _ in 0..round % 16 {
            tokio::task::yield_now().await;
        }
        f.files.set_uploads_paused(false).await.unwrap();
        assert_ne!(
            f.files.retry_uploads_now().await.unwrap(),
            DrainOutcome::Paused,
            "round {round}"
        );
    }
    for reader in readers {
        reader.abort();
    }
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
    f.files.set_uploads_paused(false).await.unwrap();
    until_creates(&f, 1).await;
    f.files.set_uploads_paused(true).await.unwrap();

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
    tokio::time::timeout(Duration::from_secs(10), ending.recv())
        .await
        .expect("the run ends paused once its upload ends")
        .unwrap();

    f.files.set_uploads_paused(false).await.unwrap();
    let answer = f.files.inner.ask_retry().unwrap();
    proceed.send(()).unwrap();
    gate.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), answer)
            .await
            .expect("the retry is answered")
            .unwrap()
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
        .hold_next_creation(
            coven_storage::ObjectPrefix::files(f.directory.id()),
            started,
            resume,
        )
        .await;
    f.files.set_uploads_paused(false).await.unwrap();
    let files = f.files.clone();
    let task = tokio::spawn(async move { files.retry_uploads_now().await });
    tokio::time::timeout(Duration::from_secs(20), receiving)
        .await
        .unwrap()
        .unwrap();
    // The file is being sent, but bytes in flight are not progress: it has
    // none until storage confirms and completion retires it.
    let mut uploads = f.subscribe_queue();
    let queued = uploads
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .files
        .remove(0);
    assert_eq!(
        (queued.phase, queued.progress),
        (UploadPhase::Uploading, None)
    );
    release.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert!(uploads
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .files
        .is_empty());
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
    let mut counts = f.database.subscribe(|sql| {
        let sending = sql.query(
            "SELECT s.file FROM coven_sending AS s ORDER BY s.file",
            [],
            |row| row.get::<_, String>(0),
        )?;
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
        let answer = tokio::time::timeout(Duration::from_secs(10), counts.next())
            .await
            .expect("the live query answers")
            .expect("the live query is open")
            .unwrap();
        if done(&answer) {
            break answer;
        }
    };
    assert_eq!(until(&|_| true).await, (Vec::new(), 0, 3));
    f.files.set_uploads_paused(false).await.unwrap();
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
/// provider accepting more, the file goes up in a recorded session (§17.8).
#[tokio::test]
async fn a_file_goes_in_one_request_up_to_the_providers_limit() {
    use coven_storage::BUFFERED_REQUEST_LIMIT;
    let encrypted = |plain: usize| {
        coven_format::file::FileHeader::new(plain as u64)
            .encrypted_size()
            .unwrap()
    };
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
            assert_eq!(
                sent,
                [MemoryRequest::Create(path.clone())],
                "{plain} {limit}"
            );
        } else {
            assert_eq!(sent.first(), Some(&MemoryRequest::Other("begin_upload")));
            assert_eq!(sent.last(), Some(&MemoryRequest::Other("finish_upload")));
            assert!(!sent.contains(&MemoryRequest::Create(path.clone())));
        }
        assert!(!f.queued(&file).await);
        assert_eq!(
            f.storage.status(&path).await.unwrap().unwrap().size,
            encrypted(plain)
        );
        assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
        f.close().await;
    }
}
