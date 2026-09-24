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

/// `make_remote` returns once its queue rows and intent are committed, and
/// that commit wakes the subscription: a host needs no receipt of its own to
/// know the outbox changed, and no manual refresh after the call.
#[tokio::test]
async fn make_remote_wakes_the_subscription_with_its_committed_queue() {
    tokio::spawn(run_make_remote_wakes_the_subscription_with_its_committed_queue())
        .await
        .expect("outbox wake task");
}

async fn run_make_remote_wakes_the_subscription_with_its_committed_queue() {
    use crate::blob_facade_tests::{builder, ExternalPhotoTestHost};
    use coven_replication::sync::test_helpers::*;

    coven_keys::keys::test_keyring::install();
    let tmp = tempfile::tempdir().expect("store directory");
    let owner = coven_keys::keys::UserKeypair::generate();
    let encryption = crate::EncryptionService::from_key([42; 32]);
    let handle = builder(crate::StoreDir::new_ephemeral(tmp.path()))
        .synced_tables(test_synced_tables_with_blob(crate::BlobDecl::new(
            "photos",
            crate::Provenance::UserProvided,
            crate::CacheFill::CacheLazy,
        )))
        .migrations(test_migrations())
        .key_custody(crate::KeyCustody::InMemory(crate::MasterKeyring::from(
            encryption.clone(),
        )))
        .identity_custody(crate::IdentityCustody::InMemory(owner.clone()))
        .open()
        .expect("open the store");
    let home = test_cloud_home();
    handle
        .create_test_store("blob-facade-test", owner, home.clone())
        .await
        .expect("create the Store");
    handle
        .connect_sync_with_test_home_caller_driven(
            home,
            coven_storage::CloudCipher::Encrypted(encryption),
        )
        .await
        .expect("connect the store to its home");
    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = b"a photo the user owns".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");
    handle
        .write_note_with_external_photo("note-1", "photo-1", &path, &bytes)
        .await
        .expect("write the note and register its photo");

    let mut outbox = handle.subscribe_cloud_outbox();
    let initial = outbox.next().await.expect("initial snapshot");
    assert!(initial.uploads.is_empty() && initial.make_remotes.is_empty());

    handle
        .make_remote_with_discovered_order_for_test("notes", "note-1", "Notes Root", false)
        .await
        .expect("start the transition");
    let woken = tokio::time::timeout(std::time::Duration::from_secs(5), outbox.next())
        .await
        .expect("the make_remote commit wakes the subscription")
        .expect("read the outbox");
    assert_eq!(woken.uploads.len(), 1, "{woken:?}");
    assert_eq!(woken.make_remotes.len(), 1, "{woken:?}");
}
