use super::*;
use std::sync::Arc;

/// The epoch, with every wait recorded and passed at once.
#[derive(Default)]
pub(crate) struct Waits(pub(crate) Mutex<Vec<Duration>>);
impl Clock for Waits {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH
    }
    fn elapsed(&self) -> Duration {
        self.0.lock().unwrap().iter().sum()
    }
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
        self.0.lock().unwrap().push(duration);
        Box::pin(std::future::ready(()))
    }
}

fn limited() -> StorageError {
    crate::providers::ProviderResponse::injected(
        crate::CloudProvider::S3,
        StorageFailure::RateLimited,
        429,
        None,
    )
}

#[tokio::test]
async fn one_minute_bounds_every_step_of_a_setup_call() {
    let clock = Arc::new(Waits::default());
    let waits = SetupWaits::default();
    let refusals = std::sync::atomic::AtomicUsize::new(2);
    let answer = || async {
        match refusals.fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |left| left.checked_sub(1),
        ) {
            Ok(_) => Err(limited()),
            Err(_) => Ok(()),
        }
    };
    waits.resend(clock.as_ref(), answer).await.unwrap();
    // A later step continues the same backoff and the same minute.
    let error = waits
        .resend(clock.as_ref(), || async { Err::<(), _>(limited()) })
        .await
        .unwrap_err();
    assert_eq!(error.failure(), StorageFailure::RateLimited);
    assert_eq!(
        *clock.0.lock().unwrap(),
        [1, 2, 4, 8, 16].map(Duration::from_secs)
    );
}

#[tokio::test]
async fn only_a_running_call_resends_and_never_after_a_failed_cleanup() {
    let clock = Waits::default();
    let waits = SetupWaits::default();
    let failed_cleanup = StorageError::Cleanup {
        operation: Box::new(limited()),
        cleanup: Box::new(StorageFailure::Network.into()),
    };
    assert!(!waits.wait_to_resend(&clock, &failed_cleanup).await);
    assert!(
        !waits
            .wait_to_resend(&clock, &StorageFailure::Network.into())
            .await
    );
    waits.end();
    assert!(!waits.wait_to_resend(&clock, &limited()).await);
    assert!(clock.0.lock().unwrap().is_empty());
}
