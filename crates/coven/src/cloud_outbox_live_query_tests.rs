//! Cancellation of the cloud-outbox subscription.

use std::future::Future;
use std::task::Poll;

use crate::blob_facade_tests::open_local;

/// A host drives the subscription inside `select!`, so a `next()` can be
/// dropped after it has taken the change it woke for and before its read
/// finishes. The next call must still deliver that change.
#[tokio::test]
async fn a_cancelled_next_does_not_lose_the_snapshot_it_was_reading() {
    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let handle = open_local(crate::StoreDir::new_ephemeral(tmp.path()));
    let mut outbox = handle.subscribe_cloud_outbox();

    {
        let mut first = std::pin::pin!(outbox.next());
        let polled = std::future::poll_fn(|cx| Poll::Ready(first.as_mut().poll(cx))).await;
        assert!(
            polled.is_pending(),
            "the first poll starts the snapshot read and waits on it"
        );
    }

    let snapshot = tokio::time::timeout(std::time::Duration::from_secs(5), outbox.next())
        .await
        .expect("the cancelled read's snapshot is still due")
        .expect("read the outbox");
    assert!(snapshot.uploads.is_empty());
    assert!(snapshot.make_remotes.is_empty());
}
