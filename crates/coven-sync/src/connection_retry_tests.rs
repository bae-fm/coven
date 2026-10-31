//! The app's network report and the connection's wait (§6.7; IO `WaitingNetwork`).

use super::*;
use crate::retry_waits::tests::{due, Fixture};
use coven_foundation::clock::Clock;
use coven_storage::{CloudProvider, Cooldown, StorageError, StorageFailure};
use std::time::Duration;

const SYNC: RetrySubject = RetrySubject::Sync;

/// A failed pass on a network failure the provider answered or not.
async fn fail(
    retry: &mut ConnectionRetry,
    database: &Database,
    waits: &RetryWaits,
    answered: bool,
) {
    let reason = crate::PendingReason::Storage {
        provider: CloudProvider::S3,
        failure: StorageFailure::Network,
        answered,
    };
    let error = SyncError::Storage(StorageError::Failure(StorageFailure::Network));
    retry
        .failed(
            database,
            waits,
            reason,
            waits.connection_version(),
            error,
            crate::connection_failure,
        )
        .await
        .unwrap();
}

async fn report(waits: &RetryWaits) -> bool {
    waits.network_changed();
    waits.end_reported_waits(crate::NetworkOwner::Connection).await.unwrap()
}

/// Ana's phone failed twice with no network and waits two seconds. Her
/// report ends the wait at once; the next failure continues the backoff.
#[tokio::test]
async fn a_report_ends_an_offline_wait_and_keeps_its_backoff() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let mut retry = ConnectionRetry::Automatic;
    for _ in 0..2 {
        fail(&mut retry, &database, &waits, false).await;
    }
    assert!(!due(&database, &waits, SYNC).await);
    assert!(report(&waits).await);
    assert!(due(&database, &waits, SYNC).await);
    let ended = database.retry_wait(SYNC).await.unwrap().unwrap();
    assert_eq!(
        (ended.failures, ended.not_before, ended.wait),
        (2, fixture.clock.now(), Duration::ZERO)
    );
    // Reports coalesce: none arrived since, and a second ends nothing new.
    assert!(!waits.end_reported_waits(crate::NetworkOwner::Connection).await.unwrap());
    assert!(report(&waits).await);
    assert_eq!(database.retry_wait(SYNC).await.unwrap(), Some(ended));
    fail(&mut retry, &database, &waits, false).await;
    let next = database.retry_wait(SYNC).await.unwrap().unwrap();
    assert_eq!((next.failures, next.wait), (3, Duration::from_secs(4)));
    assert!(!due(&database, &waits, SYNC).await);
    database.close().await.unwrap();
}

/// A provider that answered decided the wait, and its cooldown holds every
/// consumer: a new network changes neither (§6.7).
#[tokio::test]
async fn a_report_leaves_an_answered_wait_and_the_provider_cooldown() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let mut retry = ConnectionRetry::Automatic;
    fail(&mut retry, &database, &waits, true).await;
    let answered = database.retry_wait(SYNC).await.unwrap();
    assert!(report(&waits).await);
    assert_eq!(database.retry_wait(SYNC).await.unwrap(), answered);
    assert!(!due(&database, &waits, SYNC).await);
    // An offline wait ends, but a live cooldown still holds the pass.
    fail(&mut retry, &database, &waits, false).await;
    waits.record(Duration::from_secs(600)).await.unwrap();
    assert!(report(&waits).await);
    assert!(database.retry_wait(SYNC).await.unwrap().unwrap().wait.is_zero());
    assert!(!due(&database, &waits, SYNC).await);
    fixture.advance(599);
    assert!(!due(&database, &waits, SYNC).await);
    fixture.advance(1);
    assert!(due(&database, &waits, SYNC).await);
    database.close().await.unwrap();
}

/// The ended wait is saved with no full wait left, so reopening never arms
/// the old one again, even after the wall clock moved back (IO
/// `ended_reopens_due`, `kept_wait_returns`). A pass that then succeeds
/// clears it like any wait.
#[tokio::test]
async fn an_ended_wait_stays_ended_across_reopening() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let mut retry = ConnectionRetry::Automatic;
    for _ in 0..9 {
        fail(&mut retry, &database, &waits, false).await;
    }
    assert!(report(&waits).await);
    database.close().await.unwrap();
    fixture.clock.set(fixture.clock.now() - Duration::from_secs(100));
    let (database, waits) = fixture.open().await;
    assert!(due(&database, &waits, SYNC).await);
    assert_eq!(waits.next_due(|subject| *subject == SYNC), Some(Duration::ZERO));
    retry.succeeded(&database, &waits).await.unwrap();
    database.close().await.unwrap();
    let (database, _) = fixture.open().await;
    assert_eq!(database.retry_wait(SYNC).await.unwrap(), None);
    assert!(database.pending::<PendingRecord>().await.unwrap().is_empty());
    database.close().await.unwrap();
}

/// A wait whose save failed holds for the app (§6.7): a report is no
/// permission to retry it.
#[tokio::test]
async fn a_report_leaves_a_wait_held_for_the_app() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let mut retry = ConnectionRetry::Automatic;
    fail(&mut retry, &database, &waits, false).await;
    let saved = database.retry_wait(SYNC).await.unwrap();
    // The next attempt's wait could not be saved.
    waits.hold(SYNC);
    assert!(report(&waits).await);
    assert_eq!(database.retry_wait(SYNC).await.unwrap(), saved);
    fixture.advance(3600);
    assert!(!due(&database, &waits, SYNC).await);
    database.close().await.unwrap();
}

/// A report whose end failed to save is not used up: the connection's wait
/// keeps its backoff, and acting again ends it (§6.7).
#[tokio::test]
async fn a_report_whose_end_failed_to_save_ends_the_wait_next_time() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let mut retry = ConnectionRetry::Automatic;
    fail(&mut retry, &database, &waits, false).await;
    let saved = database.retry_wait(SYNC).await.unwrap();
    database.fail_retry_ends(1).await;
    waits.network_changed();
    assert!(waits.end_reported_waits(crate::NetworkOwner::Connection).await.is_err());
    assert_eq!(database.retry_wait(SYNC).await.unwrap(), saved);
    assert!(!due(&database, &waits, SYNC).await);
    assert!(waits.end_reported_waits(crate::NetworkOwner::Connection).await.unwrap());
    assert!(due(&database, &waits, SYNC).await);
    // Acted on: nothing is left for the next plan.
    assert!(!waits.end_reported_waits(crate::NetworkOwner::Connection).await.unwrap());
    database.close().await.unwrap();
}
