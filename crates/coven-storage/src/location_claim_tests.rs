use super::*;
use crate::test_utils::{Faults, MemoryStorage, STORE_ID};
use crate::{CloudProvider, StorageConfig};

fn location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "auto".into(),
        endpoint: Some("https://account.r2.cloudflarestorage.com".parse().unwrap()),
        profile: None,
    }
}

#[tokio::test]
async fn memory_location_claims_are_exclusive_and_retryable() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
    let (a, b) = tokio::join!(storage.claim_location(STORE_ID), storage.claim_location(other));
    let winner = match (a, b) {
        (Ok(()), Err(error)) => {
            assert_eq!(error.failure(), occupied().failure());
            STORE_ID
        }
        (Err(error), Ok(())) => {
            assert_eq!(error.failure(), occupied().failure());
            other
        }
        results => panic!("exactly one store must pass: {results:?}"),
    };
    storage.claim_location(winner).await.unwrap();
    storage.check_location_claim(winner).await.unwrap();
    assert_eq!(
        storage.provider.read_location_marker().await.unwrap(),
        winner.to_string().as_bytes()
    );
}

#[tokio::test]
async fn missing_and_malformed_markers_refuse_without_reclaiming() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    for bytes in
        [None, Some(Vec::new()), Some(vec![0; 16]), Some(format!("{STORE_ID}\n").into_bytes())]
    {
        storage.plant_location_marker(bytes.clone()).await;
        assert_eq!(
            storage.check_location_claim(STORE_ID).await.unwrap_err().failure(),
            occupied().failure()
        );
        match bytes {
            None => assert_eq!(
                storage.provider.read_location_marker().await.unwrap_err().failure(),
                StorageFailure::NotFound
            ),
            Some(bytes) => {
                assert_eq!(
                    storage.claim_location(STORE_ID).await.unwrap_err().failure(),
                    occupied().failure()
                );
                assert_eq!(storage.provider.read_location_marker().await.unwrap(), bytes);
            }
        }
    }
}

#[tokio::test]
async fn a_lost_create_reply_retains_the_claim_for_retry() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    assert_eq!(
        storage.claim_location(STORE_ID).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    storage.claim_location(STORE_ID).await.unwrap();
    storage.set_online(false);
    assert_eq!(
        storage.check_location_claim(STORE_ID).await.unwrap_err().failure(),
        StorageFailure::Network
    );
}

#[tokio::test]
async fn joining_requires_the_marker_to_name_this_store() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
    storage.claim_location(other).await.unwrap();
    let invitation = crate::StorageInvitation::for_account(storage.config()).unwrap();
    assert_eq!(
        storage.join(STORE_ID, &invitation).await.unwrap_err().failure(),
        occupied().failure()
    );
    storage.join(other, &invitation).await.unwrap();
}

/// A Dropbox shared folder, a OneDrive folder and an S3 bucket each hold one
/// store, which the marker names; the Dropbox App folder and a CloudKit zone
/// hold no marker (§20.3).
#[tokio::test]
async fn only_locations_every_member_reaches_whole_hold_a_marker() {
    let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
    let app_folder = StorageConfig::Dropbox {
        account_label: None,
        location: crate::DropboxLocation::AppFolder,
        folder_label: None,
    };
    for (builder, marked) in [
        (MemoryStorage::builder().location(location()), true),
        (MemoryStorage::builder().provider(CloudProvider::Dropbox), true),
        (MemoryStorage::builder().provider(CloudProvider::OneDrive), true),
        (MemoryStorage::builder().location(app_folder), false),
        (MemoryStorage::builder().provider(CloudProvider::CloudKit), false),
    ] {
        let storage = builder.build().unwrap();
        storage.claim_location(STORE_ID).await.unwrap();
        let config = storage.config();
        assert_eq!(storage.provider.read_location_marker().await.is_ok(), marked, "{config:?}");
        match storage.claim_location(other).await {
            Err(error) if marked => assert_eq!(error.failure(), occupied().failure()),
            Ok(()) if !marked => storage.check_location_claim(other).await.unwrap(),
            result => panic!("{config:?}: {result:?}"),
        }
    }
}
