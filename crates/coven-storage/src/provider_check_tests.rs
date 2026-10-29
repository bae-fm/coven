use super::*;
use crate::test_utils::STORE_ID;
use crate::test_utils::{Faults, MemoryStorage};
use coven_foundation::id_source::{DeviceId, FileId};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn provider_check_preserves_an_occupied_path() {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"existing floor").await.unwrap();
    assert!(matches!(
        check_provider(&storage, &path, b"test bytes").await,
        Err(StorageSetupError::ProviderCheck { check: StorageCheck::Create, source })
            if source.failure() == StorageFailure::AlreadyExists
    ));
    assert_eq!(storage.read(&path).await.unwrap(), b"existing floor");
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

/// The epoch, with every wait recorded and passed at once.
#[derive(Default)]
struct Waits(std::sync::Mutex<Vec<Duration>>);
impl coven_foundation::clock::Clock for Waits {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH
    }
    fn elapsed(&self) -> Duration {
        self.0.lock().unwrap().iter().sum()
    }
    fn sleep(
        &self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.0.lock().unwrap().push(duration);
        Box::pin(std::future::ready(()))
    }
}

async fn rate_limited(refusals: usize, retry_after: Option<u64>) -> (MemoryStorage, Arc<Waits>) {
    let waits = Arc::new(Waits::default());
    let storage = MemoryStorage::builder()
        .clock(waits.clone())
        .build()
        .unwrap()
        .during_setup(Arc::new(crate::SetupWaits::default()));
    storage
        .set_faults(Faults {
            fail_next: refusals,
            failure: StorageFailure::RateLimited,
            response: Some(429),
            retry_after,
            ..Faults::none()
        })
        .await;
    (storage, waits)
}

#[tokio::test]
async fn rate_limited_requests_are_sent_again_after_the_requested_wait_or_a_backoff() {
    for (retry_after, expected) in [(None, [1, 2]), (Some(5), [5, 5])] {
        let (storage, waits) = rate_limited(2, retry_after).await;
        let path = ObjectPath::file(STORE_ID, DeviceId(1), FileId(uuid::Uuid::from_u128(2)));
        check_provider(&storage, &path, b"sealed test bytes")
            .await
            .unwrap();
        assert_eq!(*waits.0.lock().unwrap(), expected.map(Duration::from_secs));
        assert_eq!(
            storage.read(&path).await.unwrap_err().failure(),
            StorageFailure::NotFound
        );
    }
}

#[tokio::test]
async fn a_provider_that_keeps_limiting_fails_the_check_once_the_bound_is_spent() {
    let (storage, waits) = rate_limited(usize::MAX, None).await;
    let path = ObjectPath::file(STORE_ID, DeviceId(1), FileId(uuid::Uuid::from_u128(3)));
    let StorageSetupError::ProviderCheck {
        check: StorageCheck::Create,
        source,
    } = check_provider(&storage, &path, b"sealed test bytes")
        .await
        .unwrap_err()
    else {
        panic!("a rate-limited check named the wrong step");
    };
    assert_eq!(source.failure(), StorageFailure::RateLimited);
    // 1 + 2 + 4 + 8 + 16 seconds; the next, 32, would pass the minute.
    assert_eq!(
        *waits.0.lock().unwrap(),
        [1, 2, 4, 8, 16].map(Duration::from_secs)
    );
    // A Retry-After longer than what is left of the bound fails at once.
    let (storage, waits) = rate_limited(usize::MAX, Some(120)).await;
    let error = check_provider(&storage, &path, b"sealed test bytes")
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StorageSetupError::ProviderCheck { check: StorageCheck::Create, source }
            if source.failure() == StorageFailure::RateLimited
    ));
    assert!(waits.0.lock().unwrap().is_empty());
}

/// A copy publication can store the object and then have its temporary's
/// deletion refused: the resent create and replacement find their own bytes.
#[tokio::test]
async fn a_resend_that_meets_its_own_stored_attempt_settles_by_its_bytes() {
    let (storage, waits) = rate_limited(0, None).await;
    storage
        .set_faults(Faults {
            refuse_after_storing: 2,
            ..Faults::none()
        })
        .await;
    let path = ObjectPath::file(STORE_ID, DeviceId(1), FileId(uuid::Uuid::from_u128(4)));
    check_provider(&storage, &path, b"sealed test bytes")
        .await
        .unwrap();
    assert_eq!(*waits.0.lock().unwrap(), [1, 2].map(Duration::from_secs));
    assert_eq!(
        storage.read(&path).await.unwrap_err().failure(),
        StorageFailure::NotFound
    );
}

#[tokio::test]
async fn a_connection_outside_setup_never_resends() {
    let storage = MemoryStorage::builder().build().unwrap();
    storage
        .set_faults(Faults {
            fail_next: 1,
            failure: StorageFailure::RateLimited,
            response: Some(429),
            ..Faults::none()
        })
        .await;
    let path = ObjectPath::file(STORE_ID, DeviceId(1), FileId(uuid::Uuid::from_u128(5)));
    assert!(matches!(
        check_provider(&storage, &path, b"sealed test bytes").await,
        Err(StorageSetupError::ProviderCheck { check: StorageCheck::Create, source })
            if source.failure() == StorageFailure::RateLimited
    ));
}
