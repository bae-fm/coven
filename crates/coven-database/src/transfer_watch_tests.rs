use super::*;
use crate::observation::CommitObserver;
use crate::transfers_in_flight::TransfersInFlight;
use coven_foundation::id_source::DeviceId;
use std::time::Duration;

fn file(n: u128) -> FileId {
    FileId(uuid::Uuid::from_u128(n))
}

fn progress(bytes_transferred: u64, bytes_total: u64) -> TransferProgress {
    TransferProgress { bytes_transferred, bytes_total }
}

async fn next(watch: &mut TransferWatch) -> Vec<TransferChange> {
    tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("the watch answers")
        .expect("the watch is open")
}

/// The first read is every file in flight; a step of bytes names only the
/// file it moved; a file that starts or ends is named; two steps before a
/// read are heard as the later one.
#[tokio::test]
async fn a_reader_hears_each_file_whose_progress_moved() {
    let transfers = TransfersInFlight::new(CommitObserver::new());
    let uploads = transfers.uploads;
    let first = uploads.start(DeviceId(1), file(1), vec![file(1)], progress(0, 100));
    let mut watch = uploads.subscribe();
    assert_eq!(
        next(&mut watch).await,
        [TransferChange::Reset(BTreeMap::from([(file(1), progress(0, 100))]))]
    );
    let second = uploads.start(DeviceId(1), file(2), vec![file(2)], progress(40, 200));
    assert_eq!(
        next(&mut watch).await,
        [TransferChange::Updated { file: file(2), progress: progress(40, 200) }]
    );
    first.meter().reached(10);
    first.meter().reached(30);
    assert_eq!(
        next(&mut watch).await,
        [TransferChange::Updated { file: file(1), progress: progress(30, 100) }]
    );
    drop(second);
    assert_eq!(next(&mut watch).await, [TransferChange::Removed { file: file(2) }]);
    drop(first);
    assert_eq!(next(&mut watch).await, [TransferChange::Removed { file: file(1) }]);
}

/// A meter whose attempt ended moves nothing, even when a later attempt of
/// the same file is in flight; of two attempts at once, the newer shows.
#[tokio::test]
async fn an_ended_attempt_moves_no_later_one() {
    let transfers = TransfersInFlight::new(CommitObserver::new());
    let downloads = transfers.downloads;
    let ended = downloads.start(DeviceId(1), file(1), vec![file(1)], progress(0, 100));
    let stale = ended.meter();
    drop(ended);
    let older = downloads.start(DeviceId(1), file(1), vec![file(1)], progress(0, 100));
    let newer = downloads.start(DeviceId(1), file(1), vec![file(1)], progress(0, 100));
    let mut watch = downloads.subscribe();
    stale.reached(99);
    older.meter().reached(20);
    newer.meter().reached(50);
    let changes = next(&mut watch).await;
    assert_eq!(changes, [TransferChange::Reset(BTreeMap::from([(file(1), progress(50, 100))]))]);
    drop(newer);
    assert_eq!(
        next(&mut watch).await,
        [TransferChange::Updated { file: file(1), progress: progress(20, 100) }]
    );
    drop(older);
}

/// Closing the store ends the watch, at its first read too.
#[tokio::test]
async fn closing_the_store_ends_the_watch() {
    let observer = CommitObserver::new();
    let transfers = TransfersInFlight::new(observer.clone());
    let mut open = transfers.uploads.subscribe();
    assert_eq!(next(&mut open).await, [TransferChange::Reset(BTreeMap::new())]);
    let waiting = tokio::spawn(async move { open.next().await });
    observer.close();
    assert_eq!(waiting.await.unwrap(), None);
    let mut closed = transfers.uploads.subscribe();
    assert_eq!(closed.next().await, None);
}
