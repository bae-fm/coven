use super::*;
use crate::{
    test_utils::{Faults, MemoryStorage},
    StorageConfig,
};
use coven_foundation::{
    clock::FixedClock,
    id_source::{DeviceId, FileId},
};
use std::sync::Arc;

#[tokio::test]
async fn duplicate_upload_cleanup_failure_is_not_an_expected_refusal() {
    let storage = MemoryStorage::new(
        StorageConfig::S3 {
            bucket: "test".into(),
            region: "test".into(),
            prefix: "store".into(),
            endpoint: None,
        },
        Arc::new(FixedClock::new(std::time::SystemTime::UNIX_EPOCH)),
    )
    .unwrap()
    .with_transfer_limits(1024, 16)
    .unwrap();
    storage
        .set_faults(Faults {
            fail_duplicate_cleanup: true,
            ..Faults::none()
        })
        .await;
    let path = ObjectPath::file(DeviceId(1), FileId(uuid::Uuid::from_u128(1)));
    let StorageSetupError::ProviderCheck {
        check: StorageCheck::CreateOnce,
        source: StorageError::Cleanup { operation, cleanup },
    } = check_provider(&storage, &path, b"sealed test bytes")
        .await
        .unwrap_err()
    else {
        panic!("duplicate upload cleanup failure was lost");
    };
    assert_eq!(operation.failure(), StorageFailure::AlreadyExists);
    assert_eq!(cleanup.failure(), StorageFailure::Network);
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
}
