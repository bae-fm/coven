//! Watching whether rows are kept offline.

use crate::blob_facade_tests::{builder, ExternalPhotoTestHost};
use coven_replication::sync::test_helpers::*;

async fn next_answer(query: &mut crate::RowsPinnedLiveQuery) -> Vec<Option<bool>> {
    tokio::time::timeout(std::time::Duration::from_secs(30), query.next())
        .await
        .expect("the pin state changes")
        .expect("answer the rows")
}

/// A host marks rows "kept offline" from this subscription alone: the sync
/// loop keeping an upload's copy, and the host unpinning it, each deliver the
/// new answer without the host polling.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pin_state_changes_are_delivered_to_a_subscriber() {
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
        .create_test_store("rows-pinned-watch", owner, home.clone())
        .await
        .expect("create the Store");
    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = b"a photo kept offline".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");
    handle
        .write_note_with_external_photo("note-1", "photo-1", &path, &bytes)
        .await
        .expect("write the note and its photo");

    let mut pinned = handle.subscribe_rows_pinned("note_photos", vec!["photo-1".to_string()]);
    assert_eq!(next_answer(&mut pinned).await, vec![Some(false)]);

    handle
        .connect_sync_with_test_home(home, coven_storage::CloudCipher::Encrypted(encryption))
        .await
        .expect("connect the store with its loop");
    handle
        .make_remote_with_discovered_order_for_test("notes", "note-1", "Notes Root", true)
        .await
        .expect("make the root Remote and keep its copy");
    handle.sync_now();
    assert_eq!(next_answer(&mut pinned).await, vec![Some(true)]);
    // With the loop stopped no commit follows: only the kept copy moving out
    // of the pinned folder can announce the unpin.
    handle.stop_sync();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), pinned.next())
            .await
            .is_err(),
        "the commits the stopped loop made leave the answer unchanged"
    );

    let photo = handle
        .row_blob_ref("note_photos", "photo-1")
        .await
        .expect("resolve the Remote photo");
    handle.unpin(&[photo]).await.expect("unpin the photo");
    assert_eq!(next_answer(&mut pinned).await, vec![Some(false)]);
}
