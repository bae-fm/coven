//! Which pass answers the person's retry, and what `coven_retry_effect`
//! says it would do (§6.7; IO `RetryAnswer`).

use super::*;
use crate::retry_waits::tests::Fixture;
use crate::NetworkOwner;
use coven_foundation::clock::Clock;
use coven_storage::Cooldown;
use futures_util::FutureExt;
use std::time::Duration;

async fn open(fixture: &Fixture) -> (coven_database::Database, Arc<RetryWaits>) {
    let (database, waits) = fixture.open().await;
    (database, Arc::new(waits))
}

/// The connection's owner plans: it acts on the reports so far.
async fn plan(waits: &RetryWaits) {
    waits.end_reported_waits(NetworkOwner::Connection).await.unwrap();
}

async fn ask(waits: &Arc<RetryWaits>) -> RetryAnswer {
    waits.retry_requested().await.unwrap()
}

/// A pass begins and takes the requests the owner acted on.
async fn begin(waits: &Arc<RetryWaits>) -> PassRequests {
    waits.begin_pass().await.unwrap().expect("the row lets a pass begin")
}

fn failure() -> PassFailure {
    PassFailure::Failed { error: crate::SyncFailure::Removed }
}

fn removed(answer: Option<RetryOutcome>) -> bool {
    matches!(
        answer,
        Some(RetryOutcome::Failed(PassFailure::Failed { error: crate::SyncFailure::Removed }))
    )
}

/// The end the row names for a cooldown of `seconds` from now.
fn until(fixture: &Fixture, seconds: u64) -> SystemTime {
    fixture.clock.now() + Duration::from_secs(seconds)
}

/// Ana pulls to refresh while sync is stopped: no pass follows, so she is
/// told at once (IO `stopped_answers_at_once`).
#[tokio::test]
async fn a_retry_while_stopped_is_answered_not_syncing_at_once() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    let answer = ask(&waits).await.now_or_never();
    assert!(matches!(answer, Some(RetryOutcome::NotSyncing)), "{answer:?}");
    database.close().await.unwrap();
}

/// A pass answers the requests the owner acted on before it began; Ben's
/// request, made after, waits for the next pass. Concurrent requests share
/// a pass (IO `running_pass_skips`, `concurrent_requests_share`).
#[tokio::test]
async fn a_pass_answers_only_retries_acted_on_before_it_began() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    let ana = ask(&waits).await;
    let dan = ask(&waits).await;
    plan(&waits).await;
    let running = begin(&waits).await;
    let mut ben = ask(&waits).await;
    let finished_at = fixture.clock.now();
    running.answer(&RetryOutcome::Synced { finished_at });
    for answer in [ana, dan] {
        assert!(matches!(
            answer.now_or_never(),
            Some(RetryOutcome::Synced { finished_at: at }) if at == finished_at
        ));
    }
    assert!((&mut ben).now_or_never().is_none(), "the running pass answered Ben");
    // Beginning again before the owner plans takes nothing.
    begin(&waits).await.answer(&RetryOutcome::NotSyncing);
    assert!((&mut ben).now_or_never().is_none());
    plan(&waits).await;
    begin(&waits).await.answer(&failure().into());
    assert!(removed(ben.now_or_never()));
    database.close().await.unwrap();
}

/// A retry made during a provider cooldown is told when it ends, at once,
/// as the row says. One waiting when the cooldown begins is told so by the
/// owner's next plan, and no pass begins meanwhile (IO `held_answers`,
/// `held_runs_no_pass`).
#[tokio::test]
async fn a_cooldown_answers_retries_with_its_end() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    let ana = ask(&waits).await;
    waits.record(Duration::from_secs(600)).await.unwrap();
    let ben = ask(&waits).await.now_or_never();
    let end = until(&fixture, 600);
    assert!(matches!(ben, Some(RetryOutcome::CoolingDown { until: at }) if at == end), "{ben:?}");
    assert!(waits.begin_pass().await.unwrap().is_none(), "a pass began during the cooldown");
    plan(&waits).await;
    waits.answer_held(None).await.unwrap();
    assert!(matches!(
        ana.now_or_never(),
        Some(RetryOutcome::CoolingDown { until: at }) if at == end
    ));
    database.close().await.unwrap();
}

/// A cooldown's end is a write: until a step writes it, the row says the
/// cooldown holds. The next step writes it, here the person's retry, which
/// is then answered by a pass.
#[tokio::test]
async fn a_cooldowns_end_is_written_by_the_next_step() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    let mut effect = RetryEffect::follow(&database);
    assert_eq!(effect.next().await, RetryEffect::NotSyncing);
    waits.sync_started().await.unwrap();
    assert_eq!(effect.next().await, RetryEffect::Pass);
    waits.record(Duration::from_secs(600)).await.unwrap();
    let end = until(&fixture, 600);
    assert_eq!(effect.next().await, RetryEffect::CoolingDown { until: end });
    fixture.advance(600);
    let unwritten = database.read(RetryEffect::read).await.unwrap();
    assert_eq!(unwritten, RetryEffect::CoolingDown { until: end });
    let mut ana = ask(&waits).await;
    assert_eq!(effect.next().await, RetryEffect::Pass);
    assert!((&mut ana).now_or_never().is_none(), "a pass answers Ana");
    plan(&waits).await;
    let finished_at = fixture.clock.now();
    begin(&waits).await.answer(&RetryOutcome::Synced { finished_at });
    assert!(matches!(ana.now_or_never(), Some(RetryOutcome::Synced { .. })));
    database.close().await.unwrap();
}

/// A failure only new credentials, an update or a reset can clear holds the
/// pass: a retry made then is answered with it at once, even during a
/// cooldown, and one already waiting is answered by the owner's plan.
#[tokio::test]
async fn a_held_failure_answers_failed_at_once() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    let waiting = ask(&waits).await;
    waits.record(Duration::from_secs(600)).await.unwrap();
    plan(&waits).await;
    waits.answer_held(Some(&failure())).await.unwrap();
    assert!(removed(waiting.now_or_never()));
    assert!(removed(ask(&waits).await.now_or_never()));
    database.close().await.unwrap();
}

/// Stopping answers every waiting request `NotSyncing`; a pass already
/// running still answers its own (IO `stop_lets_pass_answer`).
#[tokio::test]
async fn stopping_answers_waiting_retries_and_leaves_a_running_pass_its_own() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    let taken = ask(&waits).await;
    plan(&waits).await;
    let running = begin(&waits).await;
    let waiting = ask(&waits).await;
    waits.sync_stopped().await.unwrap();
    assert!(matches!(waiting.now_or_never(), Some(RetryOutcome::NotSyncing)));
    assert!(waits.begin_pass().await.unwrap().is_none(), "a pass began after stopping");
    running.answer(&failure().into());
    assert!(matches!(taken.now_or_never(), Some(RetryOutcome::Failed(_))));
    database.close().await.unwrap();
}

/// The connection's first attempt connected: its requests wait for the
/// loop's first pass, or hear that sync stopped meanwhile.
#[tokio::test]
async fn a_connected_attempt_hands_its_retries_to_the_first_pass() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    let mut handed = ask(&waits).await;
    plan(&waits).await;
    waits.return_requests(begin(&waits).await).await.unwrap();
    assert!((&mut handed).now_or_never().is_none());
    let finished_at = fixture.clock.now();
    begin(&waits).await.answer(&RetryOutcome::Synced { finished_at });
    assert!(matches!(handed.now_or_never(), Some(RetryOutcome::Synced { .. })));
    let stopped = ask(&waits).await;
    plan(&waits).await;
    let taken = begin(&waits).await;
    waits.sync_stopped().await.unwrap();
    waits.return_requests(taken).await.unwrap();
    assert!(matches!(stopped.now_or_never(), Some(RetryOutcome::NotSyncing)));
    database.close().await.unwrap();
}

/// A dropped answer leaves the retry in place: it still ends every wait.
#[tokio::test]
async fn a_dropped_answer_still_retries() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    drop(ask(&waits).await);
    assert!(waits.end_reported_waits(NetworkOwner::Connection).await.unwrap());
    begin(&waits).await.answer(&RetryOutcome::NotSyncing);
    database.close().await.unwrap();
}

/// What a retry would do is the row an app's live query follows, and the
/// same row answers it: stopped, free, cooling down, held, and stopped
/// again, each change in order, each answer the effect's (IO
/// `ask_answers_by_effect`).
#[tokio::test]
async fn the_rows_effect_is_how_a_retry_is_answered() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    let mut effect = RetryEffect::follow(&database);
    assert_eq!(effect.next().await, RetryEffect::NotSyncing);
    assert!(matches!(ask(&waits).await.now_or_never(), Some(RetryOutcome::NotSyncing)));
    // Nothing holds the pass: a pass runs for Ana's retry and answers it.
    waits.sync_started().await.unwrap();
    assert_eq!(effect.next().await, RetryEffect::Pass);
    let mut ana = ask(&waits).await;
    assert!((&mut ana).now_or_never().is_none(), "a pass answers Ana");
    plan(&waits).await;
    let finished_at = fixture.clock.now();
    begin(&waits).await.answer(&RetryOutcome::Synced { finished_at });
    assert!(matches!(ana.now_or_never(), Some(RetryOutcome::Synced { .. })));
    // A cooldown is written as it begins, ending when Ben's answer says.
    waits.record(Duration::from_secs(600)).await.unwrap();
    let end = until(&fixture, 600);
    assert_eq!(effect.next().await, RetryEffect::CoolingDown { until: end });
    let ben = ask(&waits).await.now_or_never();
    assert!(matches!(ben, Some(RetryOutcome::CoolingDown { until: at }) if at == end));
    // A failure the person must clear holds over the cooldown, and Carol is
    // answered with it.
    waits.holds(Some(&failure())).await.unwrap();
    assert_eq!(effect.next().await, RetryEffect::Failed);
    assert!(removed(ask(&waits).await.now_or_never()));
    // The failure clears and the cooldown ends: the owner's next plan
    // writes a pass, and Dan's retry waits for it.
    fixture.advance(600);
    plan(&waits).await;
    waits.answer_held(None).await.unwrap();
    assert_eq!(effect.next().await, RetryEffect::Pass);
    let dan = ask(&waits).await;
    waits.sync_stopped().await.unwrap();
    assert_eq!(effect.next().await, RetryEffect::NotSyncing);
    assert!(matches!(dan.now_or_never(), Some(RetryOutcome::NotSyncing)));
    database.close().await.unwrap();
}

/// One cooldown has one end. Writing the row again during it, after time
/// passes and after the wall clock moves back, names the instant it was
/// recorded with and changes nothing a live query sees; the retry's answer
/// carries that same instant.
#[tokio::test]
async fn one_cooldown_writes_one_end() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    waits.record(Duration::from_secs(600)).await.unwrap();
    let end = until(&fixture, 600);
    let mut effect = RetryEffect::follow(&database);
    assert_eq!(effect.next().await, RetryEffect::CoolingDown { until: end });
    fixture.advance(100);
    waits.holds(None).await.unwrap();
    fixture.clock.set(fixture.clock.now() - Duration::from_secs(50));
    waits.holds(None).await.unwrap();
    let answer = ask(&waits).await.now_or_never();
    assert!(matches!(answer, Some(RetryOutcome::CoolingDown { until: at }) if at == end));
    // The next change the query sees is the stop: nothing came between.
    waits.sync_stopped().await.unwrap();
    assert_eq!(effect.next().await, RetryEffect::NotSyncing);
    database.close().await.unwrap();
}

/// A step that can't write the row fails the handle, and the retries
/// waiting then hear that failure instead of waiting for a pass that won't
/// come.
#[tokio::test]
async fn a_step_that_cant_write_answers_the_waiting_retries_with_its_failure() {
    let fixture = Fixture::new();
    let (database, waits) = open(&fixture).await;
    waits.sync_started().await.unwrap();
    let waiting = ask(&waits).await;
    database.test_fail_retry_effect().await.unwrap();
    let error = waits.sync_stopped().await.unwrap_err();
    assert!(matches!(error, DbError::StatePersistence(_)), "{error:?}");
    let answer = waiting.now_or_never();
    assert!(
        matches!(
            &answer,
            Some(RetryOutcome::Failed(PassFailure::Failed { error: crate::SyncFailure::Other(_) }))
        ),
        "{answer:?}"
    );
    assert!(waits.retry_requested().await.is_err(), "a failed handle took a retry");
}
