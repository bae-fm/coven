//! The upload worker keeps the upload limit's number of uploads in flight
//! while files keep arriving, and pausing or a cooldown stops new starts
//! (§17.7, E7).

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
        while queue.next().await.unwrap().files.len() != count {}
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
    until_creates(&f, 3).await;
    // Three requests are open at the provider and none has finished.
    assert_eq!(
        creates(&f),
        files[..3]
            .iter()
            .map(|file| f.path(file))
            .collect::<Vec<_>>()
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
    assert_eq!(creates(&f)[3], f.path(&files[3]));
    // A higher limit starts the last file with the others still open.
    f.files.set_transfer_limits(TransferLimits {
        uploads: NonZeroUsize::new(4).unwrap(),
        downloads: NonZeroUsize::MIN,
    });
    until_creates(&f, 5).await;
    assert_eq!(creates(&f)[4], f.path(&files[4]));
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
    let queued = uploads.next().await.unwrap().files.remove(0);
    assert_eq!(
        (queued.phase, queued.progress),
        (UploadPhase::Uploading, None)
    );
    release.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert!(uploads.next().await.unwrap().files.is_empty());
    f.close().await;
}
