use super::*;
use crate::{
    test_utils::{Faults, MemoryStorage, STORE_ID},
    S3Profile, S3ProfileName,
};

fn location() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "bucket".into(),
        region: "auto".into(),
        endpoint: Some("https://account.r2.cloudflarestorage.com".parse().unwrap()),
        profile: None,
        prefix: crate::S3Prefix::default(),
    }
}

#[tokio::test]
async fn memory_bucket_claims_are_exclusive_and_retryable() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
    let (a, b) = tokio::join!(storage.claim_bucket(STORE_ID), storage.claim_bucket(other));
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
    storage.claim_bucket(winner).await.unwrap();
    storage.check_bucket(winner).await.unwrap();
    assert_eq!(storage.provider.read_bucket_claim().await.unwrap(), winner.to_string().as_bytes());
}

#[tokio::test]
async fn missing_and_malformed_markers_refuse_without_reclaiming() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    for bytes in
        [None, Some(Vec::new()), Some(vec![0; 16]), Some(format!("{STORE_ID}\n").into_bytes())]
    {
        storage.plant_bucket_claim(bytes.clone()).await;
        assert_eq!(
            storage.check_bucket(STORE_ID).await.unwrap_err().failure(),
            occupied().failure()
        );
        match bytes {
            None => assert_eq!(
                storage.provider.read_bucket_claim().await.unwrap_err().failure(),
                StorageFailure::NotFound
            ),
            Some(bytes) => {
                assert_eq!(
                    storage.claim_bucket(STORE_ID).await.unwrap_err().failure(),
                    occupied().failure()
                );
                assert_eq!(storage.provider.read_bucket_claim().await.unwrap(), bytes);
            }
        }
    }
}

#[tokio::test]
async fn a_lost_create_reply_retains_the_claim_for_retry() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    storage.set_faults(Faults { lose_completion_reply: true, ..Faults::none() }).await;
    assert_eq!(
        storage.claim_bucket(STORE_ID).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    storage.claim_bucket(STORE_ID).await.unwrap();
    storage.set_online(false);
    assert_eq!(
        storage.check_bucket(STORE_ID).await.unwrap_err().failure(),
        StorageFailure::Network
    );
}

#[tokio::test]
async fn prefix_scoped_locations_need_no_marker_requests() {
    for profile in [
        S3ProfileName::Aws,
        S3ProfileName::Gcs,
        S3ProfileName::RustFs,
        S3ProfileName::SeaweedFs,
        S3ProfileName::CephRgw,
        S3ProfileName::VersityGw,
    ] {
        let storage = MemoryStorage::builder()
            .location(StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "auto".into(),
                endpoint: Some("http://localhost:9000".parse().unwrap()),
                profile: Some(S3Profile::Named(profile)),
                prefix: crate::S3Prefix::parse("stores").unwrap(),
            })
            .build()
            .unwrap();
        storage.claim_bucket(STORE_ID).await.unwrap();
        storage.check_bucket(STORE_ID).await.unwrap();
        assert_eq!(storage.request_count(), 0);
    }
}

#[tokio::test]
async fn joining_requires_the_marker_to_name_this_store() {
    let storage = MemoryStorage::builder().location(location()).build().unwrap();
    let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
    storage.claim_bucket(other).await.unwrap();
    let invitation = crate::StorageInvitation::for_account(storage.config()).unwrap();
    assert_eq!(
        storage.join(STORE_ID, &invitation).await.unwrap_err().failure(),
        occupied().failure()
    );
    storage.join(other, &invitation).await.unwrap();
}
