use super::*;
use crate::test_utils::MemoryStorage;
use crate::{StorageConfig, StorageFailure};
use coven_foundation::id_source::DeviceId;

fn storage() -> MemoryStorage {
    MemoryStorage::new(
        StorageConfig::Dropbox {
            namespace_id: "namespace".into(),
        },
        std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap()
}

#[tokio::test]
async fn create_routes_the_boundary_without_starting_the_unused_request() {
    let storage = storage();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
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
    storage.create_once(&path, &[7; 17]).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), [7; 17]);
}
