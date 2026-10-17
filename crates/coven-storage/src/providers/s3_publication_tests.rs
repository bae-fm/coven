use crate::providers::tests::{response, TestServer};
use crate::{
    ObjectPath, PublicationGate, Storage, StorageConnection, StorageError, StorageFailure,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct ExpiresAfterConnectionCheck(AtomicUsize);
impl PublicationGate for ExpiresAfterConnectionCheck {
    fn check(&self, _: &ObjectPath) -> Result<(), StorageError> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(StorageFailure::CatchUpRequired.into())
        }
    }
}

#[tokio::test]
async fn the_sdk_checks_again_at_transmission_and_preserves_the_local_refusal() {
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    let server = TestServer::new(axum::Router::new().fallback(move || {
        observed.fetch_add(1, Ordering::SeqCst);
        async { response(200, "stored") }
    }))
    .await;
    let raw = crate::providers::s3::tests::profile_provider(
        &server.url,
        crate::providers::s3::profile::PLAIN,
    );
    let gate = Arc::new(ExpiresAfterConnectionCheck(AtomicUsize::new(0)));
    let storage = StorageConnection::with_gate(
        raw.provider.clone(),
        gate.clone(),
        Arc::new(crate::NoCooldown),
        Arc::new(crate::TransferBudget::new(crate::TransferLimits::default())),
    );
    let path = ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
    assert_eq!(
        storage.create(&path, b"entry").await.unwrap_err().failure(),
        StorageFailure::CatchUpRequired
    );
    assert_eq!(gate.0.load(Ordering::SeqCst), 2);
    assert_eq!(requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn the_sdk_does_not_repeat_a_failed_publication() {
    let requests = Arc::new(AtomicUsize::new(0));
    let observed = requests.clone();
    let server = TestServer::new(axum::Router::new().fallback(move || {
        observed.fetch_add(1, Ordering::SeqCst);
        async { response(503, "<Error><Code>ServiceUnavailable</Code></Error>") }
    }))
    .await;
    let storage = crate::providers::s3::tests::profile_provider(
        &server.url,
        crate::providers::s3::profile::PLAIN,
    );
    let path = ObjectPath::slot(crate::test_utils::STORE_ID, std::num::NonZeroU64::MIN);
    assert!(storage.create(&path, b"entry").await.is_err());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}
