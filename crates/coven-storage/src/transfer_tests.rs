use super::*;
use crate::test_utils::MemoryStorage;
use crate::test_utils::STORE_ID;
use crate::StorageFailure;
use coven_foundation::id_source::DeviceId;

fn storage() -> MemoryStorage {
    MemoryStorage::builder()
        .transfer_limits(16, 4)
        .provider(crate::CloudProvider::Dropbox)
        .build()
        .unwrap()
}

#[tokio::test]
async fn create_routes_the_boundary_without_starting_the_unused_request() {
    let storage = storage();
    let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    for size in [0, 15, 16] {
        let called = std::sync::atomic::AtomicBool::new(false);
        upload_bytes(&storage, &path, &vec![1; size], async {
            called.store(true, std::sync::atomic::Ordering::SeqCst);
            Err(StorageError::Failure(StorageFailure::PermissionDenied))
        })
        .await
        .unwrap_err();
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }
    upload_bytes(&storage, &path, &[7; 17], async {
        panic!("oversized single request")
    })
    .await
    .unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), [7; 17]);
    assert!(storage.create(&path, &[8; 17]).await.is_err());
    assert_eq!(
        storage.create(&path, &[7; 17]).await.unwrap_err().failure(),
        StorageFailure::AlreadyExists
    );
    assert_eq!(storage.read(&path).await.unwrap(), [7; 17]);
}

#[test]
fn an_assembled_body_uses_the_providers_limit_within_the_buffered_bound() {
    for (provider, expected) in [
        (BUFFERED_REQUEST_LIMIT - 1, BUFFERED_REQUEST_LIMIT - 1),
        (BUFFERED_REQUEST_LIMIT, BUFFERED_REQUEST_LIMIT),
        (BUFFERED_REQUEST_LIMIT + 1, BUFFERED_REQUEST_LIMIT),
        (5 * 1024 * 1024 * 1024, BUFFERED_REQUEST_LIMIT),
    ] {
        let storage = MemoryStorage::builder()
            .transfer_limits(provider, 4)
            .build()
            .unwrap();
        assert_eq!(buffered_request_limit(&storage), expected);
    }
}
