//! Exact blob references bind a row-visible locator to one immutable provider
//! object. Same-id objects with different plaintext have different locator hashes
//! and slots, and provider rollback at the correct slot fails the stored hash
//! check before any plaintext is published locally.

use crate::storage::cloud::ExactSlotStorage;
use crate::storage::SyncStorage;
use crate::sync::test_helpers::TestStore;
use coven_keys::encryption::EncryptionService;
use coven_protocol::blob::locator::{BlobLocatorError, StoredBlobRef};
use coven_protocol::objects::{BlobSpoolProtection, StorageError};

const BLOB_ID: &str = "blobxxxx";
const STORE_KEY: [u8; 32] = [42; 32];

fn protection() -> EncryptionService {
    EncryptionService::from_key(STORE_KEY)
}

#[tokio::test]
async fn a_same_id_planted_blob_cannot_replace_the_signed_exact_reference() {
    let database = crate::sync::test_helpers::open_test_db();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &database,
        "blob-reference-substitution",
        coven_keys::keys::UserKeypair::generate(),
        home,
    )
    .await
    .expect("create blob substitution test Store");
    let real = b"THE-OWNERS-REAL-BLOB";
    let planted = b"THE-ATTACKERS-FAKED!";
    assert_eq!(real.len(), planted.len(), "fixture uses equal-size blobs");
    assert_ne!(real, planted);

    let real_blob = store
        .create_exact_opaque_blob("photos", BLOB_ID, real)
        .await;
    let planted_blob = store
        .create_exact_opaque_blob("photos", BLOB_ID, planted)
        .await;
    assert_ne!(
        real_blob.locator().locator_hash(),
        planted_blob.locator().locator_hash()
    );
    assert_ne!(real_blob.object().slot(), planted_blob.object().slot());

    assert!(matches!(
        StoredBlobRef::new(real_blob.locator().clone(), planted_blob.object().clone()),
        Err(BlobLocatorError::ObjectKeyMismatch { .. })
    ));

    let directory = tempfile::tempdir().expect("create materialization directory");
    let destination = directory.path().join("real");
    let staged = store
        .storage()
        .stage_verified_blob_plaintext(
            &real_blob,
            BlobSpoolProtection::Opaque(protection()),
            &destination,
        )
        .await
        .expect("stage the referenced exact blob");
    assert_eq!(tokio::fs::read(staged.path()).await.unwrap(), real);
    staged.commit().await.expect("publish verified plaintext");
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), real);
}

#[tokio::test]
async fn provider_rollback_at_the_exact_slot_is_refused_before_plaintext_publication() {
    let database = crate::sync::test_helpers::open_test_db();
    let home = crate::sync::test_helpers::test_cloud_home();
    let store = TestStore::create(
        &database,
        "blob-provider-rollback",
        coven_keys::keys::UserKeypair::generate(),
        home.clone(),
    )
    .await
    .expect("create provider rollback test Store");
    let previous = b"blob-content-VERSION1";
    let current = b"blob-content-VERSION2";
    assert_eq!(
        previous.len(),
        current.len(),
        "fixture uses equal-size blobs"
    );
    assert_ne!(previous, current);

    let previous_blob = store
        .create_exact_opaque_blob("photos", BLOB_ID, previous)
        .await;
    let current_blob = store
        .create_exact_opaque_blob("photos", BLOB_ID, current)
        .await;
    let previous_stored = home
        .read_at(previous_blob.object().slot())
        .await
        .expect("read prior stored representation");
    let current_stored = home
        .read_at(current_blob.object().slot())
        .await
        .expect("read current stored representation");
    home.replace_exact_object(current_blob.object().slot(), previous_stored);

    let directory = tempfile::tempdir().expect("create materialization directory");
    let destination = directory.path().join("current");
    assert!(matches!(
        store
            .storage()
            .stage_verified_blob_plaintext(
                &current_blob,
                BlobSpoolProtection::Opaque(protection()),
                &destination,
            )
            .await,
        Err(StorageError::InvalidContent(_))
    ));
    assert!(!destination.exists());

    home.replace_exact_object(current_blob.object().slot(), current_stored);
    let staged = store
        .storage()
        .stage_verified_blob_plaintext(
            &current_blob,
            BlobSpoolProtection::Opaque(protection()),
            &destination,
        )
        .await
        .expect("stage restored exact blob");
    assert_eq!(tokio::fs::read(staged.path()).await.unwrap(), current);
    staged.commit().await.expect("publish verified plaintext");
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), current);
}
