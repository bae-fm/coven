use super::*;
use crate::{test_utils::MemoryStorage, ObjectStream, Storage};

#[tokio::test]
async fn a_lower_limit_waits_for_existing_leases_and_an_increase_wakes_waiters() {
    let budget = Arc::new(TransferBudget::new(TransferLimits {
        uploads: NonZeroUsize::MIN,
        downloads: NonZeroUsize::new(2).unwrap(),
    }));
    let first = budget.download().await;
    let second = budget.download().await;
    budget.set_limits(TransferLimits::default());
    drop(first);
    let third = budget.download();
    tokio::pin!(third);
    assert!(futures_util::poll!(&mut third).is_pending());
    budget.set_limits(TransferLimits {
        uploads: NonZeroUsize::MIN,
        downloads: NonZeroUsize::new(2).unwrap(),
    });
    let third = third.await;
    drop(second);
    drop(third);
    assert_eq!(budget.state.lock().unwrap().downloads, 0);
}

#[tokio::test]
async fn a_response_releases_its_lease_only_at_eof_failure_or_drop() {
    let budget = Arc::new(TransferBudget::new(TransferLimits::default()));
    let storage = MemoryStorage::builder()
        .build()
        .unwrap()
        .with_transfers(budget.clone());
    let path = crate::ObjectPath::floor(coven_foundation::id_source::StoreId(
        uuid::Uuid::from_u128(17),
    ));
    storage.create(&path, b"body").await.unwrap();
    let mut first = storage.stream_object(&path, None).await.unwrap();
    assert_eq!(first.read(&mut []).await.unwrap(), 0);
    let next = storage.stream_object(&path, None);
    tokio::pin!(next);
    assert!(futures_util::poll!(&mut next).is_pending());
    assert_eq!(first.read(&mut [0; 4]).await.unwrap(), 4);
    assert!(futures_util::poll!(&mut next).is_pending());
    assert_eq!(first.read(&mut [0; 4]).await.unwrap(), 0);
    drop(next.await.unwrap());
    assert_eq!(budget.state.lock().unwrap().downloads, 0);

    let metadata = storage.status(&path).await.unwrap().unwrap();
    let mut truncated = ObjectStream::new(metadata, None, std::io::Cursor::new(b"x"))
        .unwrap()
        .with_download(budget.download().await);
    assert_eq!(truncated.read(&mut [0; 4]).await.unwrap(), 1);
    assert!(truncated.read(&mut [0; 4]).await.is_err());
    assert_eq!(budget.state.lock().unwrap().downloads, 0);
}

struct WaitingBody {
    budget: Arc<TransferBudget>,
    downloads_at_drop: Arc<Mutex<Option<usize>>>,
}
impl Drop for WaitingBody {
    fn drop(&mut self) {
        *self.downloads_at_drop.lock().unwrap() = Some(self.budget.state.lock().unwrap().downloads);
    }
}
#[async_trait::async_trait]
impl crate::ObjectBody for WaitingBody {
    async fn read(&mut self, _: &mut [u8]) -> Result<usize, crate::StorageError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn cancelling_a_body_read_releases_the_lease_even_if_the_stream_is_retained() {
    let budget = Arc::new(TransferBudget::new(TransferLimits::default()));
    let storage = MemoryStorage::builder().build().unwrap();
    let path = crate::ObjectPath::floor(coven_foundation::id_source::StoreId(
        uuid::Uuid::from_u128(18),
    ));
    storage.create(&path, b"body").await.unwrap();
    let metadata = storage.status(&path).await.unwrap().unwrap();
    let downloads_at_drop = Arc::new(Mutex::new(None));
    let mut stream = ObjectStream::new(
        metadata,
        None,
        WaitingBody {
            budget: budget.clone(),
            downloads_at_drop: downloads_at_drop.clone(),
        },
    )
    .unwrap()
    .with_download(budget.download().await);
    {
        let mut buffer = [0; 4];
        let read = stream.read(&mut buffer);
        tokio::pin!(read);
        assert!(futures_util::poll!(&mut read).is_pending());
    }
    assert_eq!(*downloads_at_drop.lock().unwrap(), Some(1));
    assert_eq!(budget.state.lock().unwrap().downloads, 0);
    assert!(stream.read(&mut [0; 4]).await.is_err());
}

struct DownloadCooldown(Arc<TransferBudget>);

#[async_trait::async_trait]
impl crate::Cooldown for DownloadCooldown {
    fn remaining(&self) -> Option<std::time::Duration> {
        None
    }

    async fn record(&self, _: std::time::Duration) -> Result<(), crate::StorageError> {
        let next = self.0.download();
        tokio::pin!(next);
        assert!(
            futures_util::poll!(&mut next).is_pending(),
            "another download was admitted before the cooldown was recorded"
        );
        Ok(())
    }
}

#[tokio::test]
async fn a_throttled_response_records_its_cooldown_before_releasing_download_capacity() {
    let budget = Arc::new(TransferBudget::new(TransferLimits::default()));
    let storage = MemoryStorage::builder()
        .build()
        .unwrap()
        .with_transfers(budget.clone())
        .with_cooldown(Arc::new(DownloadCooldown(budget.clone())));
    let mut faults = crate::test_utils::Faults::none();
    faults.fail_next = 1;
    faults.failure = crate::StorageFailure::RateLimited;
    faults.retry_after = Some(60);
    storage.set_faults(faults).await;
    let path = crate::ObjectPath::floor(crate::test_utils::STORE_ID);
    assert!(matches!(
        storage.stream_object(&path, None).await,
        Err(error) if error.failure() == crate::StorageFailure::RateLimited
    ));
    assert_eq!(budget.state.lock().unwrap().downloads, 0);
}
