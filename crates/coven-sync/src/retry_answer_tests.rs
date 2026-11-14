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
    PassFailure::Failed { error: crate::SyncFailure::Removed(coven_database::Removal::Device) }
}

fn removed(answer: Option<RetryOutcome>) -> bool {
    matches!(
        answer,
        Some(RetryOutcome::Failed(PassFailure::Failed {
            error: crate::SyncFailure::Removed(coven_database::Removal::Device)
        }))
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

/// The retries waiting when a step fails hear its failure as the typed
/// error the handle reports, never its text (F.5): a failed checkpoint
/// keeps its cause, and a runtime shutting down says how far it got.
#[test]
fn a_failed_steps_answer_keeps_its_typed_error() {
    let typed = |error: DbError| match failed(&error) {
        RetryOutcome::Failed(PassFailure::Failed { error: crate::SyncFailure::Other(cause) }) => {
            cause.downcast_ref::<DbError>().map(|cause| format!("{cause:?}"))
        }
        outcome => panic!("{outcome:?}"),
    };
    let checkpoint = DbError::CheckpointFailed(Arc::new(DbError::DamagedDatabase));
    assert_eq!(typed(checkpoint).as_deref(), Some("CheckpointFailed(DamagedDatabase)"));
    let shutdown = coven_foundation::task::Shutdown {
        operation: "database call",
        outcome: coven_foundation::task::ShutdownOutcome::NotRun,
    };
    assert!(
        typed(DbError::Shutdown(shutdown)).is_some_and(|cause| cause.starts_with("Shutdown(")),
        "the shutdown keeps its type"
    );
    assert!(matches!(failed(&DbError::StoreClosed), RetryOutcome::NotSyncing));
}

/// What holds the pass in the comparison below, as the test made it.
#[derive(Clone, Copy, PartialEq)]
enum Hold {
    Cooldown,
    Failure,
}

fn outcome_name(outcome: &RetryOutcome) -> &'static str {
    match outcome {
        RetryOutcome::Synced { .. } => "synced",
        RetryOutcome::Failed(_) => "failed",
        RetryOutcome::CoolingDown { .. } => "coolingDown",
        RetryOutcome::NotSyncing => "notSyncing",
    }
}

fn effect_name(effect: RetryEffect) -> &'static str {
    match effect {
        RetryEffect::Pass => "pass",
        RetryEffect::CoolingDown { .. } => "coolingDown",
        RetryEffect::Failed => "failed",
        RetryEffect::NotSyncing => "notSyncing",
    }
}

/// IO `RetryAnswer` through the Rust owner: requests, network reports,
/// starting and stopping, holds beginning and ending, the owner planning and
/// answering a held pass, passes beginning and ending, and the connection's
/// first attempt handing its requests back. After each event, whether it was
/// taken (a pass begins only when the row lets it), where each report
/// number's request stands, and the effect `coven_retry_effect` shows, against
/// the runner's `retry_answer` case.
#[tokio::test]
#[ignore = "requires COVEN_IO_LEAN"]
async fn io_differential_retry_answer() {
    use serde_json::{json, Value};
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut pick = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut inputs = Vec::new();
    let mut actual = Vec::new();
    for _ in 0..8 {
        let fixture = Fixture::new();
        let (database, waits) = open(&fixture).await;
        let mut events: Vec<Value> = Vec::new();
        let mut observed = Vec::new();
        let mut hold: Option<Hold> = None;
        let mut taken: Option<PassRequests> = None;
        let mut asked: Vec<(u64, RetryAnswer, Option<&'static str>)> = Vec::new();
        for _ in 0..60 {
            let running = taken.is_some();
            let started = waits.started();
            let mut applied = true;
            let event = match pick(16) {
                0..=2 => {
                    let answer = ask(&waits).await;
                    asked.push((waits.network_reported(), answer, None));
                    json!("ask")
                }
                3 => {
                    waits.report_network();
                    json!("report")
                }
                4 => {
                    waits.sync_started().await.unwrap();
                    json!("start")
                }
                5 => {
                    waits.sync_stopped().await.unwrap();
                    json!("stop")
                }
                6 | 7 => match hold {
                    None if pick(2) == 0 => {
                        waits.record(Duration::from_secs(600)).await.unwrap();
                        hold = Some(Hold::Cooldown);
                        json!({"held": "cooldown"})
                    }
                    None => {
                        waits.holds(Some(&failure())).await.unwrap();
                        hold = Some(Hold::Failure);
                        json!({"held": "failure"})
                    }
                    Some(ended) => {
                        if ended == Hold::Cooldown {
                            fixture.advance(600);
                        }
                        waits.holds(None).await.unwrap();
                        hold = None;
                        json!({"held": null})
                    }
                },
                8 | 9 if started && !running => {
                    plan(&waits).await;
                    json!("plan")
                }
                10 if started && !running && hold.is_some() => {
                    let failure = (hold == Some(Hold::Failure)).then(failure);
                    waits.answer_held(failure.as_ref()).await.unwrap();
                    json!("wait")
                }
                11 | 12 if !running => {
                    taken = waits.begin_pass().await.unwrap();
                    applied = taken.is_some();
                    json!("begin")
                }
                13 if running => {
                    waits.return_requests(taken.take().unwrap()).await.unwrap();
                    json!("connected")
                }
                14 | 15 if running => {
                    let (end, outcome) = match pick(3) {
                        0 => ("synced", RetryOutcome::Synced { finished_at: fixture.clock.now() }),
                        1 => ("failed", RetryOutcome::Failed(failure())),
                        _ => ("stopped", RetryOutcome::NotSyncing),
                    };
                    taken.take().unwrap().answer(&outcome);
                    json!({"finish": end})
                }
                _ => continue,
            };
            events.push(event);
            for (_, answer, answered) in &mut asked {
                if answered.is_none() {
                    *answered = answer.now_or_never().map(|outcome| outcome_name(&outcome));
                }
            }
            let waiting: Vec<u64> =
                waits.requests.lock().unwrap().waiting.iter().map(|(number, _)| *number).collect();
            let in_pass: Vec<u64> = taken
                .iter()
                .flat_map(|pass| pass.taken.iter().map(|(number, _)| *number))
                .collect();
            let requests: Vec<&str> = (1..=waits.network_reported())
                .map(|number| match asked.iter().find(|(asked, ..)| *asked == number) {
                    None => "unasked",
                    Some((_, _, Some(outcome))) => outcome,
                    Some(_) if in_pass.contains(&number) => "inPass",
                    Some(_) if waiting.contains(&number) => "waiting",
                    Some(_) => "lost",
                })
                .collect();
            let effect = effect_name(database.read(RetryEffect::read).await.unwrap());
            observed.push(json!({"applied": applied, "requests": requests, "effect": effect}));
        }
        inputs.push(json!({"check": "retry_answer", "events": events}));
        actual.push(json!(observed));
        database.close().await.unwrap();
    }
    assert_eq!(actual, crate::tests::io_runner(&inputs), "{inputs:?}");
}
