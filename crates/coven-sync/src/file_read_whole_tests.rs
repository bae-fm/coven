//! A pin's download of one file sends as many of its requests at once as
//! the download limit lets go, and writes their chunks in order (§17.8).

use super::*;
use coven_foundation::clock::HANG_GUARD;
use coven_storage::test_utils::MemoryRequest;
use std::num::NonZeroUsize;

/// The size of a request to storage, which a download moves in steps of.
const REQUEST: usize = 1024 * 1024;

/// Sixty chunks take four requests of up to 1 MiB.
fn video() -> Vec<u8> {
    (0..CHUNK * 60).map(|i| (i % 251) as u8).collect()
}

fn downloads(f: &Fixture, at_once: usize) {
    f.files.set_transfer_limits(TransferLimits {
        uploads: NonZeroUsize::MIN,
        downloads: NonZeroUsize::new(at_once).unwrap(),
    });
}

/// How many requests for `file`'s bytes have gone out.
fn reads(f: &Fixture, file: &FileRef) -> usize {
    f.storage
        .request_trace()
        .into_iter()
        .filter(|request| matches!(request, MemoryRequest::Read(path, _) if *path == f.path(file)))
        .count()
}

/// Wait until `count` requests for `file`'s bytes have gone out, while
/// `prepare` runs.
async fn until_reads<P: std::future::Future>(
    f: &Fixture,
    file: &FileRef,
    count: usize,
    prepare: &mut std::pin::Pin<&mut P>,
) {
    let mut requests = f.storage.subscribe_requests();
    let reached = async {
        while reads(f, file) < count {
            requests.changed().await.unwrap();
        }
    };
    tokio::select! {
        _ = prepare.as_mut() => panic!("the prepare ended before {count} requests went out"),
        reached = tokio::time::timeout(HANG_GUARD, reached) => reached.unwrap_or_else(|_| {
            panic!("{count} requests never went out together: {}", reads(f, file))
        }),
    }
}

/// Ben pins one video with the download limit at three: its first request
/// goes alone, checking the file's header, and the next three go together,
/// none of them answered yet.
#[tokio::test]
async fn a_lone_files_pin_sends_as_many_requests_as_the_limit_lets_go() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes = video();
    let file = f.uploaded("video", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    downloads(&f, 3);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    f.storage.gate_reads(gate.clone()).await;
    let prepare = f.files.pin(std::slice::from_ref(&file));
    tokio::pin!(prepare);
    until_reads(&f, &file, 1, &mut prepare).await;
    gate.add_permits(1);
    until_reads(&f, &file, 4, &mut prepare).await;
    gate.add_permits(3);
    prepare.await.unwrap();
    assert_eq!(reads(&f, &file), 4, "each request went out once");
    assert_eq!(f.database.files_pinned(vec![file.id()]).await.unwrap(), vec![Some(true)]);
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    f.close().await;
}

/// Prepare `file`'s pin with its second request held partway, until the
/// other three have come in, then stop it. The fourth went out only once the
/// first was written and kept, freeing its place.
async fn stopped_with_second_held(f: &Fixture, file: &FileRef) {
    let (observed, held) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    let hold = REQUEST as u64 + 1;
    f.storage.hold_body_at(f.path(file), hold, observed, resumed).await;
    // The second request is chunks 15 to 29; all but its bytes from the
    // hold on can arrive.
    let second_end = coven_format::file::FileHeader::new(file.size()).chunk(29).unwrap().end();
    let mut watch = f.database.downloads_in_flight().subscribe();
    let prepare = f.files.prepare_pin(std::slice::from_ref(file));
    tokio::pin!(prepare);
    tokio::select! {
        result = &mut prepare => panic!("the download was not held: {result:?}"),
        held = held => held.unwrap(),
    }
    let arrived = async {
        loop {
            let changes = watch.next().await.expect("the store is open");
            let reached = changes.iter().any(|change| match change {
                coven_database::TransferChange::Updated { progress, .. } => {
                    progress.bytes_transferred + (second_end - hold) == progress.bytes_total
                }
                _ => false,
            });
            if reached {
                return;
            }
        }
    };
    tokio::select! {
        result = &mut prepare => panic!("the download was not held: {result:?}"),
        arrived = tokio::time::timeout(HANG_GUARD, arrived) => arrived.unwrap(),
    }
    // Returning drops the prepare, with its second request held.
    drop(resume);
}

/// Ben's prepare stops while its second request is held partway and the
/// third and fourth have come in: only the first request's bytes follow on from what
/// the copy holds, so only they are kept, and the next prepare fetches the
/// other three again.
#[tokio::test]
async fn a_stopped_prepare_keeps_only_the_bytes_that_follow_on() {
    let f = Fixture::new(Provenance::AppProvided).await;
    let bytes = video();
    let file = f.uploaded("video", bytes.clone()).await;
    f.files.stop_background_for_counts().await;
    downloads(&f, 3);
    stopped_with_second_held(&f, &file).await;
    let (_, kept) =
        f.files.inner.database.pin_download_kept(&file).await.unwrap().expect("bytes are kept");
    assert!(
        kept > REQUEST as u64 / 2 && kept <= REQUEST as u64,
        "only the first request's bytes are kept: {kept}"
    );

    let before = reads(&f, &file);
    f.files.pin(std::slice::from_ref(&file)).await.unwrap();
    assert_eq!(reads(&f, &file) - before, 3, "the three requests after the first go again");
    assert_eq!(f.files.read_file(&file).await.unwrap(), bytes);
    f.close().await;
}
