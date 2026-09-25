//! How a host tells why a blob read failed.

use crate::blob_facade_tests::{builder, ExternalPhotoTestHost};
use crate::rows_pinned_live_query_tests::next_answer;
use coven_replication::sync::test_helpers::*;

/// A provider refusing a blob read for bad credentials is something the user
/// fixes in settings, not an internal fault; the host tells them apart from
/// the error's typed backend classification.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blob_read_refused_by_the_provider_names_why() {
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
        .create_test_store("refused-blob-read", owner, home.clone())
        .await
        .expect("create the Store");
    let user_dir = tempfile::tempdir().expect("user directory");
    let bytes = b"a photo the provider will refuse".to_vec();
    let path = user_dir.path().join("photo.jpg");
    std::fs::write(&path, &bytes).expect("write the user's file");
    handle
        .write_note_with_external_photo("note-1", "photo-1", &path, &bytes)
        .await
        .expect("write the note and its photo");
    let mut pinned = handle.subscribe_rows_pinned("note_photos", vec!["photo-1".to_string()]);
    assert_eq!(next_answer(&mut pinned).await, vec![Some(false)]);
    handle
        .connect_sync_with_test_home(
            home.clone(),
            coven_storage::CloudCipher::Encrypted(encryption),
        )
        .await
        .expect("connect the store with its loop");
    handle
        .make_remote_with_discovered_order_for_test("notes", "note-1", "Notes Root", true)
        .await
        .expect("make the root Remote");
    handle.sync_now();
    assert_eq!(next_answer(&mut pinned).await, vec![Some(true)]);
    handle.stop_sync();
    handle
        .connect_sync_with_test_home(
            home.clone(),
            coven_storage::CloudCipher::Encrypted(crate::EncryptionService::from_key([42; 32])),
        )
        .await
        .expect("reconnect");

    let photo = handle
        .row_blob_ref("note_photos", "photo-1")
        .await
        .expect("resolve the Remote photo");
    handle.evict_blob(&photo).await.expect("drop the kept copy");
    home.fail_exact_stream_reads_with(Some(crate::StorageBackendFailure::Authentication));
    let error = handle
        .read_blob(&photo)
        .await
        .expect_err("the provider refuses the read");
    assert_eq!(
        error.backend_failure(),
        Some(crate::StorageBackendFailure::Authentication),
        "{error}"
    );
    handle.stop_sync();
}
