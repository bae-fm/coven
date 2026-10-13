use super::*;
use crate::test_utils::STORE_ID;
use crate::test_utils::{Faults, MemoryStorage};
use coven_foundation::id_source::{DeviceId, FileId};

#[tokio::test]
async fn absent_malformed_and_authenticated_clocks_remain_distinct() {
    use coven_format::{clock, test_utils as format};
    let storage = MemoryStorage::builder()
        .transfer_limits(1024, 16)
        .build()
        .unwrap();
    let path = ObjectPath::clock(STORE_ID, DeviceId(1));
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
    storage.replace(&path, &[]).await.unwrap();
    let malformed = storage.read(&path).await.unwrap();
    assert!(matches!(
        clock::open(
            &malformed,
            &path,
            &format::store_key(),
            &format::member().signing
        ),
        Err(clock::ClockError::Format(coven_format::Error::Truncated))
    ));
    assert_eq!(
        storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap()
            .len(),
        1
    );
    let sealed = format::sealed_clock();
    storage.replace(&path, &sealed).await.unwrap();
    let stored = storage.read(&path).await.unwrap();
    assert_eq!(
        clock::open(
            &stored,
            &path,
            &format::store_key(),
            &format::member().signing
        )
        .unwrap()
        .device,
        DeviceId(1)
    );
    let test_path = ObjectPath::file(STORE_ID, DeviceId(1), FileId(uuid::Uuid::from_u128(1)));
    check_provider(&storage, &test_path, b"sealed test bytes")
        .await
        .unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), sealed);
    assert_eq!(
        storage
            .list(&ObjectPrefix::all(STORE_ID))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn provider_check_preserves_an_occupied_clock_path() {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::clock(STORE_ID, DeviceId(1));
    storage.replace(&path, b"existing clock").await.unwrap();
    assert!(matches!(
        check_provider(&storage, &path, b"test bytes").await,
        Err(StorageSetupError::ProviderCheck { check: StorageCheck::Create, source })
            if source.failure() == StorageFailure::AlreadyExists
    ));
    assert_eq!(storage.read(&path).await.unwrap(), b"existing clock");
}

#[tokio::test]
async fn duplicate_upload_cleanup_failure_is_not_an_expected_refusal() {
    let storage = MemoryStorage::builder()
        .transfer_limits(1024, 16)
        .build()
        .unwrap();
    storage
        .set_faults(Faults {
            fail_duplicate_cleanup: true,
            ..Faults::none()
        })
        .await;
    let path = ObjectPath::file(STORE_ID, DeviceId(1), FileId(uuid::Uuid::from_u128(1)));
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
    assert!(storage
        .list(&ObjectPrefix::all(STORE_ID))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn fresh_probe_may_be_omitted_from_setup_listing() {
    use crate::CloudProvider;
    for provider in [
        CloudProvider::S3,
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let storage = MemoryStorage::builder().provider(provider).build().unwrap();
        storage
            .set_faults(Faults {
                hide_listings: true,
                ..Faults::none()
            })
            .await;
        let path = ObjectPath::file(STORE_ID, DeviceId(1), FileId(uuid::Uuid::from_u128(8)));
        check_provider(&storage, &path, b"sealed test bytes")
            .await
            .unwrap();
        assert_eq!(
            storage.read(&path).await.unwrap_err().failure(),
            StorageFailure::NotFound
        );
    }
}
