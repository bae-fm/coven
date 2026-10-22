//! Which pass answers the person's retry (§6.7; IO `RetryAnswer`).

use super::*;
use crate::retry_waits::tests::Fixture;
use crate::NetworkOwner;
use coven_foundation::clock::Clock;
use coven_storage::Cooldown;
use futures_util::FutureExt;
use std::time::Duration;

/// The connection's owner plans: it acts on the reports so far.
async fn plan(waits: &RetryWaits) {
    waits
        .end_reported_waits(NetworkOwner::Connection)
        .await
        .unwrap();
}

fn failure() -> PassFailure {
    PassFailure::Failed {
        error: crate::SyncFailure::Removed,
    }
}

/// Ana pulls to refresh while sync is stopped: no pass follows, so she is
/// told at once (IO `stopped_answers_at_once`).
#[tokio::test]
async fn a_retry_while_stopped_is_answered_not_syncing_at_once() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    let answer = waits.retry_requested().now_or_never();
    assert!(
        matches!(answer, Some(RetryOutcome::NotSyncing)),
        "{answer:?}"
    );
    database.close().await.unwrap();
}

/// A pass answers the requests the owner acted on before it began; Ben's
/// request, made after, waits for the next pass. Concurrent requests share
/// a pass (IO `running_pass_skips`, `concurrent_requests_share`).
#[tokio::test]
async fn a_pass_answers_only_retries_acted_on_before_it_began() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    waits.sync_started();
    let ana = waits.retry_requested();
    let dan = waits.retry_requested();
    plan(&waits).await;
    let running = waits.take_requests();
    let mut ben = waits.retry_requested();
    let finished_at = fixture.clock.now();
    running.answer(&RetryOutcome::Synced { finished_at });
    for answer in [ana, dan] {
        assert!(matches!(
            answer.now_or_never(),
            Some(RetryOutcome::Synced { finished_at: at }) if at == finished_at
        ));
    }
    assert!(
        (&mut ben).now_or_never().is_none(),
        "the running pass answered Ben"
    );
    // Taking again before the owner plans takes nothing.
    waits.take_requests().answer(&RetryOutcome::NotSyncing);
    assert!((&mut ben).now_or_never().is_none());
    plan(&waits).await;
    waits.take_requests().answer(&failure().into());
    assert!(matches!(
        ben.now_or_never(),
        Some(RetryOutcome::Failed(PassFailure::Failed {
            error: crate::SyncFailure::Removed
        }))
    ));
    database.close().await.unwrap();
}

/// A provider cooldown holds the pass: the requests the owner acted on are
/// told when it ends, at once; one made after waits (IO `held_answers`).
#[tokio::test]
async fn a_cooldown_answers_acted_on_retries_with_its_end() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    waits.sync_started();
    waits.record(Duration::from_secs(600)).await.unwrap();
    let ana = waits.retry_requested();
    plan(&waits).await;
    let mut ben = waits.retry_requested();
    waits.answer_held(None);
    let until = fixture.clock.now() + Duration::from_secs(600);
    assert!(matches!(
        ana.now_or_never(),
        Some(RetryOutcome::CoolingDown { until: at }) if at == until
    ));
    assert!((&mut ben).now_or_never().is_none());
    // Once the cooldown ends, nothing holds the pass: Ben waits for it.
    fixture.advance(600);
    plan(&waits).await;
    waits.answer_held(None);
    assert!((&mut ben).now_or_never().is_none());
    waits.sync_stopped();
    assert!(matches!(ben.now_or_never(), Some(RetryOutcome::NotSyncing)));
    database.close().await.unwrap();
}

/// A failure only new credentials, an update or a reset can clear holds the
/// pass: the request is answered with it at once.
#[tokio::test]
async fn a_held_failure_answers_failed_at_once() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    waits.sync_started();
    waits.record(Duration::from_secs(600)).await.unwrap();
    let answer = waits.retry_requested();
    plan(&waits).await;
    // The failure is the answer even during a cooldown.
    waits.answer_held(Some(&failure()));
    assert!(matches!(
        answer.now_or_never(),
        Some(RetryOutcome::Failed(PassFailure::Failed {
            error: crate::SyncFailure::Removed
        }))
    ));
    database.close().await.unwrap();
}

/// Stopping answers every waiting request `NotSyncing`; a pass already
/// running still answers its own (IO `stop_lets_pass_answer`).
#[tokio::test]
async fn stopping_answers_waiting_retries_and_leaves_a_running_pass_its_own() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    waits.sync_started();
    let taken = waits.retry_requested();
    plan(&waits).await;
    let running = waits.take_requests();
    let waiting = waits.retry_requested();
    waits.sync_stopped();
    assert!(matches!(
        waiting.now_or_never(),
        Some(RetryOutcome::NotSyncing)
    ));
    running.answer(&failure().into());
    assert!(matches!(
        taken.now_or_never(),
        Some(RetryOutcome::Failed(_))
    ));
    database.close().await.unwrap();
}

/// The connection's first attempt connected: its requests wait for the
/// loop's first pass, or hear that sync stopped meanwhile.
#[tokio::test]
async fn a_connected_attempt_hands_its_retries_to_the_first_pass() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    waits.sync_started();
    let mut handed = waits.retry_requested();
    plan(&waits).await;
    waits.return_requests(waits.take_requests());
    assert!((&mut handed).now_or_never().is_none());
    let finished_at = fixture.clock.now();
    waits
        .take_requests()
        .answer(&RetryOutcome::Synced { finished_at });
    assert!(matches!(
        handed.now_or_never(),
        Some(RetryOutcome::Synced { .. })
    ));
    let stopped = waits.retry_requested();
    plan(&waits).await;
    let taken = waits.take_requests();
    waits.sync_stopped();
    waits.return_requests(taken);
    assert!(matches!(
        stopped.now_or_never(),
        Some(RetryOutcome::NotSyncing)
    ));
    database.close().await.unwrap();
}

/// A dropped answer leaves the retry in place: it still ends every wait.
#[tokio::test]
async fn a_dropped_answer_still_retries() {
    let fixture = Fixture::new();
    let (database, waits) = fixture.open().await;
    waits.sync_started();
    drop(waits.retry_requested());
    assert!(waits
        .end_reported_waits(NetworkOwner::Connection)
        .await
        .unwrap());
    waits.take_requests().answer(&RetryOutcome::NotSyncing);
    database.close().await.unwrap();
}
