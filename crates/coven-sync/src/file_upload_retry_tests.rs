//! Upload waits: the queue's persisted retry delays, the category policy
//! and the provider cooldown shared with every consumer (§16.5, E7).

use super::*;
use coven_database::RetrySubject;
use coven_foundation::clock::{Clock, FixedClock};
use coven_storage::{test_utils::Faults, StorageFailure};

async fn fixture(clock: &Arc<FixedClock>) -> Fixture {
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
    // Only the explicit drains below attempt uploads.
    f.files.stop_background_for_counts().await;
    f.files.inner.state.lock().unwrap().paused = false;
    f
}

fn fail_next(failure: StorageFailure, retry_after: Option<u64>) -> Faults {
    Faults {
        fail_next: 1,
        failure,
        retry_after,
        ..Faults::none()
    }
}

/// Keep §10's catch-up fresh while the test clock passes retry delays.
fn advance(f: &Fixture, clock: &FixedClock, seconds: u64) {
    clock.set(clock.now() + Duration::from_secs(seconds));
    f.send.begin();
    f.send
        .complete(crate::send_gate::Binding::Device, Vec::new())
        .unwrap();
}

/// How long until the worker's next timed drain, by the waits' deadlines.
fn retry_in(f: &Fixture) -> Option<Duration> {
    let waits = &f.files.inner.waits;
    let retry = waits.next_due(|subject| matches!(subject, RetrySubject::Upload(_)));
    match (retry, waits.cooldown()) {
        (Some(retry), Some(cooldown)) => Some(retry.max(cooldown)),
        (retry, cooldown) => retry.or(cooldown),
    }
}

async fn retry(f: &Fixture) -> Option<coven_database::RetryWait> {
    f.files.inner.database.uploads().await.unwrap()[0].retry
}

fn drained(outcome: DrainOutcome) -> (usize, usize) {
    match outcome {
        DrainOutcome::Drained { uploaded, failures } => (uploaded, failures.len()),
        DrainOutcome::AllInBackoff => (0, 0),
        _ => panic!("unexpected drain outcome"),
    }
}

#[tokio::test]
async fn a_transient_failure_persists_its_delay_and_waits_it_out() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    for (attempt, wait) in [1u64, 2, 4].into_iter().enumerate() {
        f.storage
            .set_faults(fail_next(StorageFailure::Network, None))
            .await;
        assert_eq!(
            drained(f.files.inner.drain_uploads(false).await.unwrap()),
            (0, 1)
        );
        // T and W persist with the failure, before the upload waits.
        let persisted = retry(&f).await.unwrap();
        assert_eq!(persisted.failures, attempt as u32 + 1);
        assert_eq!(persisted.wait, Duration::from_secs(wait));
        assert_eq!(persisted.not_before, clock.now() + persisted.wait);
        let requests = f.storage.request_trace().len();
        assert!(matches!(
            f.files.inner.drain_uploads(false).await.unwrap(),
            DrainOutcome::AllInBackoff
        ));
        assert_eq!(f.storage.request_trace().len(), requests);
        assert_eq!(retry_in(&f), Some(Duration::from_secs(wait)));
        advance(&f, &clock, wait);
    }
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        (1, 0)
    );
    assert!(!f.queued(&file).await);
    assert_eq!(retry_in(&f), None);
    f.close().await;
}

#[tokio::test]
async fn reopening_keeps_the_remaining_delay_and_its_exponent() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let mut f = fixture(&clock).await;
    f.attach("files", "photo", b"photo".to_vec()).await;
    for _ in 0..4 {
        f.storage
            .set_faults(fail_next(StorageFailure::Network, None))
            .await;
        f.files.inner.drain_uploads(true).await.unwrap();
    }
    assert_eq!(retry(&f).await.unwrap().wait, Duration::from_secs(8));
    advance(&f, &clock, 4);
    // The device clock moves back an hour while the store is closed.
    clock.set(clock.now() - Duration::from_secs(3600));
    f.reopen().await;
    f.files.stop_background_for_counts().await;
    f.files.inner.state.lock().unwrap().paused = false;
    advance(&f, &clock, 0);
    // Restart waits min(T - now, W): the backward clock cannot stretch it.
    assert_eq!(retry_in(&f), Some(Duration::from_secs(8)));
    advance(&f, &clock, 7);
    assert!(matches!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::AllInBackoff
    ));
    advance(&f, &clock, 1);
    f.storage
        .set_faults(fail_next(StorageFailure::Network, None))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        (0, 1)
    );
    // Reopening kept the exponent: the fifth failure waits sixteen seconds.
    assert_eq!(retry(&f).await.unwrap().wait, Duration::from_secs(16));
    f.close().await;
}

#[tokio::test]
async fn failures_needing_the_app_wait_for_retry_uploads_now() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    for failure in [
        StorageFailure::QuotaExceeded,
        StorageFailure::PermissionDenied,
        StorageFailure::Authentication,
    ] {
        f.storage.set_faults(fail_next(failure, None)).await;
        assert_eq!(
            drained(f.files.inner.drain_uploads(true).await.unwrap()),
            (0, 1)
        );
        // A whole-store condition is not retried on a timer.
        assert_eq!(retry(&f).await, None);
        assert_eq!(retry_in(&f), None);
        advance(&f, &clock, 3600);
        assert!(matches!(
            f.files.inner.drain_uploads(false).await.unwrap(),
            DrainOutcome::AllInBackoff
        ));
    }
    f.files.retry_uploads_now().await.unwrap();
    assert!(!f.queued(&file).await);
    f.close().await;
}

#[tokio::test]
async fn a_provider_cooldown_holds_uploads_even_when_the_app_retries() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = fixture(&clock).await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::RateLimited, Some(600)))
        .await;
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        (0, 1)
    );
    // The provider's 600 seconds outlast the upload's own one-second delay
    // and the 300-second cap, and persist with the store.
    let cooldown = f
        .database
        .retry_wait(RetrySubject::Provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cooldown.wait, Duration::from_secs(600));
    assert_eq!(retry_in(&f), Some(Duration::from_secs(600)));
    advance(&f, &clock, 1);
    let requests = f.storage.request_trace().len();
    assert!(matches!(
        f.files.inner.drain_uploads(false).await.unwrap(),
        DrainOutcome::AllInBackoff
    ));
    let Err(refused) = f.files.retry_uploads_now().await else {
        panic!("an app retry bypassed the provider cooldown");
    };
    assert!(
        matches!(&refused, crate::SyncError::Storage(error) if error.failure() == StorageFailure::RateLimited),
        "{refused:?}"
    );
    assert_eq!(f.storage.request_trace().len(), requests);
    advance(&f, &clock, 599);
    assert_eq!(
        drained(f.files.inner.drain_uploads(false).await.unwrap()),
        (1, 0)
    );
    assert!(!f.queued(&file).await);
    f.close().await;
}

#[tokio::test]
async fn the_worker_wakes_for_a_due_retry_without_another_commit() {
    let clock = Arc::new(FixedClock::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
    ));
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
    f.storage
        .set_faults(fail_next(StorageFailure::Network, None))
        .await;
    let file = f.attach("files", "photo", b"photo".to_vec()).await;
    let mut uploads = f.files.subscribe_uploads();
    f.files.set_uploads_paused(false);
    tokio::time::timeout(Duration::from_secs(5), async {
        while retry(&f).await.is_none() {
            uploads.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    advance(&f, &clock, 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.queued(&file).await {
            uploads.next().await.unwrap();
        }
    })
    .await
    .unwrap();
    f.close().await;
}

#[tokio::test]
async fn completed_catch_up_wakes_a_worker_blocked_between_file_sends() {
    use coven_foundation::clock::FixedClock;
    use coven_storage::{ObjectPrefix, StorageFailure};
    let clock = Arc::new(FixedClock::new(SystemTime::UNIX_EPOCH));
    let f = Fixture::with_clock(Provenance::AppProvided, CacheFill::CacheLazy, clock.clone()).await;
    f.attach("files", "first", b"first".to_vec()).await;
    f.attach("files", "second", b"second".to_vec()).await;
    let (started, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    f.storage
        .hold_next_creation(ObjectPrefix::files(f.directory.id()), started, held)
        .await;
    let mut uploads = f.files.subscribe_uploads();
    f.files.set_uploads_paused(false);
    observed.await.unwrap();
    clock.set(SystemTime::UNIX_EPOCH + Duration::from_secs(300));
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let queue = uploads.next().await.unwrap();
            if queue.files.iter().any(|file| matches!(file.last_failure.as_deref(),
                Some(UploadFailure::Storage(error)) if error.failure() == StorageFailure::CatchUpRequired)) {
                break;
            }
        }
    }).await.unwrap();
    let first = f.database.file_ref("files", "first").await.unwrap();
    let second = f.database.file_ref("files", "second").await.unwrap();
    assert!(!f.queued(&first).await);
    assert!(f.queued(&second).await);
    f.send.begin();
    f.send
        .complete(crate::send_gate::Binding::Device, Vec::new())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            uploads.next().await.unwrap();
            if !f.queued(&second).await {
                break;
            }
        }
    })
    .await
    .unwrap();
    f.close().await;
}
