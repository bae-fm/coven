//! A file whose provider keeps its parts apart sends them together, on the
//! connections the upload limit leaves free, and records each as it comes
//! back (§17.9).

use super::*;
use coven_foundation::clock::HANG_GUARD;
use coven_storage::test_utils::{Faults, MemoryRequest};
use std::num::NonZeroUsize;
use tokio::sync::Semaphore;

fn uploads(f: &Fixture, at_once: usize) {
    f.files.set_transfer_limits(TransferLimits {
        uploads: NonZeroUsize::new(at_once).unwrap(),
        downloads: NonZeroUsize::MIN,
    });
}

/// How many parts have gone out to the provider.
fn parts(f: &Fixture) -> usize {
    f.storage
        .request_trace()
        .into_iter()
        .filter(|request| *request == MemoryRequest::Other("upload_part"))
        .count()
}

async fn until_parts(f: &Fixture, count: usize) {
    let mut requests = f.storage.subscribe_requests();
    tokio::time::timeout(HANG_GUARD, async {
        while parts(f) < count {
            requests.changed().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{count} parts never went out together: {}", parts(f)));
}

/// Ana uploads one video to S3 with the upload limit at 3 and nothing else
/// queued: its first part starts the upload alone, then three of its parts
/// are in flight at once, none of them answered yet. Her app's bar, the
/// confirmed bytes and those of each part in flight, never goes back as
/// the parts come back in any order.
#[tokio::test]
async fn a_lone_files_parts_go_together_up_to_the_limit() {
    let f = Fixture::with_parts_together(Provenance::AppProvided).await;
    uploads(&f, 3);
    let gate = Arc::new(Semaphore::new(0));
    f.storage.gate_parts(gate.clone()).await;
    let bytes = vec![7; CHUNK * 8];
    let file = f.attach("files", "video", bytes.clone()).await;
    let mut watch = f.database.uploads_in_flight().subscribe();
    let heard = tokio::spawn(async move {
        let mut moved = Vec::new();
        while let Some(changes) = watch.next().await {
            for change in changes {
                match change {
                    coven_database::TransferChange::Updated { progress, .. } => {
                        moved.push(progress.bytes_transferred);
                    }
                    coven_database::TransferChange::Removed { .. } => return moved,
                    coven_database::TransferChange::Reset(_) => {}
                }
            }
        }
        moved
    });
    f.set_uploads_paused(false).await;
    until_parts(&f, 1).await;
    gate.add_permits(1);
    until_parts(&f, 4).await;
    gate.add_permits(64);
    f.drain().await;
    assert!(!f.queued(&file).await);
    let moved = heard.await.unwrap();
    assert!(!moved.is_empty());
    assert!(moved.windows(2).all(|pair| pair[0] <= pair[1]), "{moved:?}");
    let stored = f.database.file_ref(file.id()).await.unwrap();
    assert_eq!(f.files.read_file(&stored).await.unwrap(), bytes);
    f.close().await;
}

/// The reply to one of Ana's parts is lost while two others are in
/// flight: the attempt starts no more parts, records the two as they come
/// back, and fails. Its retry resumes from every part the provider holds,
/// the lost reply's part among them, and sends only the rest: no part goes
/// twice.
#[tokio::test]
async fn a_retry_after_a_lost_reply_sends_only_the_parts_the_provider_lacks() {
    let f = Fixture::with_parts_together(Provenance::AppProvided).await;
    uploads(&f, 3);
    let gate = Arc::new(Semaphore::new(0));
    f.storage.gate_parts(gate.clone()).await;
    let bytes = vec![9; CHUNK * 8];
    let file = f.attach("files", "video", bytes.clone()).await;
    let total = f.upload_of(&file).await.unwrap().bytes_total;
    f.set_uploads_paused(false).await;
    until_parts(&f, 1).await;
    gate.add_permits(1);
    until_parts(&f, 4).await;
    f.storage.set_faults(Faults { lose_part_reply: true, ..Faults::none() }).await;
    gate.add_permits(64);
    // The attempt in flight ends failed; the next one is the retry.
    f.files.inner.drain_uploads_retrying().await.unwrap();
    assert!(f.upload_reason(&file).await.is_some(), "the lost reply failed the attempt");
    f.drain().await;
    assert!(!f.queued(&file).await);
    assert_eq!(parts(&f) as u64, total.div_ceil(CHUNK as u64), "no part went twice");
    let stored = f.database.file_ref(file.id()).await.unwrap();
    assert_eq!(f.files.read_file(&stored).await.unwrap(), bytes);
    f.close().await;
}

/// Ana's video sends two parts at once with the upload limit at 2 when she
/// adds a photo. The photo waits for a connection; when the video's first
/// part in flight ends, the photo takes the connection it frees, before the
/// video sends another part.
#[tokio::test]
async fn an_upload_waiting_takes_a_freed_connection_before_another_part() {
    let f = Fixture::with_parts_together(Provenance::AppProvided).await;
    uploads(&f, 2);
    let gate = Arc::new(Semaphore::new(0));
    f.storage.gate_parts(gate.clone()).await;
    let video = f.attach("files", "video", vec![3; CHUNK * 8]).await;
    f.set_uploads_paused(false).await;
    until_parts(&f, 1).await;
    gate.add_permits(1);
    until_parts(&f, 3).await;
    let photo = f.attach("files", "photo", vec![4; 100]).await;
    let photo_path = f.location(&photo).await.path(f.directory.id());
    tokio::time::timeout(HANG_GUARD, async {
        while !f.files.inner.lane_waiting(super::file_upload_pool::Lane::Trailing) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the photo waits for a connection");
    gate.add_permits(1);
    let mut requests = f.storage.subscribe_requests();
    let created = || f.storage.request_trace().contains(&MemoryRequest::Create(photo_path.clone()));
    tokio::time::timeout(HANG_GUARD, async {
        while !created() {
            requests.changed().await.unwrap();
        }
    })
    .await
    .expect("the photo starts on the freed connection");
    assert_eq!(parts(&f), 3, "the video sent no part meanwhile");
    gate.add_permits(64);
    f.drain().await;
    assert!(!f.queued(&video).await && !f.queued(&photo).await);
    f.close().await;
}
