//! The answer to the person's retry (§6.7; IO `RetryAnswer`).
//!
//! What decides it is `coven_retry_effect`'s row: whether a started
//! connection will plan, whether a failure only new credentials, an update
//! or a reset can clear holds every pass, and the provider cooldown holding
//! them. Each step below changes that row and acts on the requests in one
//! step on the store's writer (`Database::change_retry_state`), so the
//! answer a request gets is the effect the row shows when it is made (IO
//! `ask_answers_by_effect`), and a request, stopping and a pass taking its
//! requests each happen at once (IO `never_stranded`). Each step writes the
//! cooldown as it stands then, and the row keeps no cooldown while sync is
//! stopped, since no owner would write its end. The cooldown's end is a
//! write too: the connection's owner wakes when it ends, and its next step
//! writes it.
//!
//! A retry request is a report, so it takes the next report number. A
//! request the row answers at once, stopped, held or cooling down, is
//! answered so; any other waits for a pass. The connection's owner, the
//! connection's first attempt and then its sync loop, acts on reports when
//! it plans between passes. A pass begins only while the row lets it, and
//! takes every waiting request the owner has acted on, answering them when
//! it ends; a request made after the owner last acted waits for a later
//! pass. When a failure or cooldown begins to hold the pass, the owner's
//! next plan answers the requests it acted on at once. Stopping sync
//! answers every waiting request `NotSyncing`. A pass that ends without
//! answering the requests it took, because it panicked or was dropped,
//! leaves them waiting again; an owner that panicked stops sync and answers
//! every waiting request with its bug, as `SyncError::WorkerFailed` (E15).
//!
//! A failed write has failed the store's handle, which every call and live
//! query then reports, and no later step can commit; the requests waiting
//! then are answered with that failure. A write that panicked fails alone:
//! its bug goes to the step's caller, or to the sink when the connection's
//! owner wrote on its own, and that owner stops (E15).

use crate::{NetworkOwner, PassFailure, RetryWaits, SyncFailure};
use coven_database::{DbError, RetryState, RetrySync};
use coven_foundation::clock::ClockRef;
use coven_foundation::locks::lock_unpoisoned;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::SystemTime,
};
use tokio::sync::oneshot;

/// How a step commits: a change of the retry state alone, or a start of
/// sync, which `coven_sync_status` counts in the same transaction.
#[derive(Clone, Copy)]
enum Step {
    Change,
    Start,
}

/// What the first sync pass after the person's retry came to (E5).
#[derive(Clone, Debug)]
pub enum RetryOutcome {
    /// The pass reached its read end.
    Synced {
        /// The store's clock when the pass finished.
        finished_at: SystemTime,
    },
    /// The pass failed, or a failure only new credentials, an update or a
    /// reset can clear holds every pass; the failure says which.
    Failed(PassFailure),
    /// A provider cooldown holds every pass until then. The pass runs once
    /// it ends, with no further request.
    CoolingDown {
        /// When the cooldown ends, by the store's clock.
        until: SystemTime,
    },
    /// Sync is stopped or the handle closed, so no pass follows the request.
    NotSyncing,
}

impl From<PassFailure> for RetryOutcome {
    fn from(failure: PassFailure) -> Self {
        Self::Failed(failure)
    }
}

/// What the person's retry would do if made now (E5), as
/// `coven_retry_effect` holds it: run a pass, or be answered at once
/// without one. An app offers a retry only while it is `Pass`; the status
/// says why it is not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryEffect {
    /// A pass runs for the retry, and its end answers it.
    Pass,
    /// A provider cooldown holds every pass until then, by the store's
    /// clock: the retry is answered `CoolingDown` at once, and the pass runs
    /// once the cooldown ends.
    CoolingDown {
        /// When the cooldown ends, by the store's clock.
        until: SystemTime,
    },
    /// A failure only new credentials, an update or a reset can clear holds
    /// every pass: the retry is answered `Failed` with it at once. The
    /// status shows the failure.
    Failed,
    /// Sync is stopped or the handle closed: the retry is answered
    /// `NotSyncing` at once.
    NotSyncing,
}

impl From<RetryState> for RetryEffect {
    /// A held failure answers before a cooldown: the cooldown's end would
    /// not let the pass run. `coven_retry_effect.effect` is computed alike.
    fn from(state: RetryState) -> Self {
        match (state.sync, state.held) {
            (RetrySync::Stopped, _) => Self::NotSyncing,
            (RetrySync::Started { .. }, true) => Self::Failed,
            (RetrySync::Started { cooling_until: Some(until) }, false) => {
                Self::CoolingDown { until }
            }
            (RetrySync::Started { cooling_until: None }, false) => Self::Pass,
        }
    }
}

impl RetryEffect {
    /// Read `coven_retry_effect`, as an app's live query does:
    /// `handle.subscribe(RetryEffect::read)`.
    pub fn read(sql: coven_database::SqlReadContext<'_>) -> Result<Self, DbError> {
        let (effect, until) =
            sql.query_row("SELECT effect,cooling_until FROM coven_retry_effect", [], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
            })?;
        Ok(match (effect.as_str(), until) {
            ("pass", None) => Self::Pass,
            ("cooling_down", Some(until)) => {
                Self::CoolingDown { until: coven_database::stored_time(until)? }
            }
            ("failed", _) => Self::Failed,
            ("not_syncing", None) => Self::NotSyncing,
            _ => return Err(DbError::Invariant { what: "the retry effect is one coven computes" }),
        })
    }

    /// Follow `coven_retry_effect` with a live query, as an app does.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn follow(
        database: &coven_database::Database,
    ) -> coven_database::test_utils::Followed<Self, DbError> {
        coven_database::test_utils::Followed::new(database.subscribe(Self::read))
    }
}

/// Resolves when the first sync pass after a retry request ends (E5).
/// Dropping it leaves the retry in place.
#[derive(Debug)]
pub struct RetryAnswer(oneshot::Receiver<RetryOutcome>);

impl Future for RetryAnswer {
    type Output = RetryOutcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<RetryOutcome> {
        // Every waiting request is answered before its owner lets it go; a
        // request is dropped unanswered only with the store's retry schedule
        // itself, once the handle is gone.
        Pin::new(&mut self.0).poll(context).map(|answer| answer.unwrap_or(RetryOutcome::NotSyncing))
    }
}

/// Retry requests by report number, each with where its answer goes.
type Waiting = Vec<(u64, oneshot::Sender<RetryOutcome>)>;

/// Answer every request in `requests` with `outcome`.
fn answer(requests: Waiting, outcome: &RetryOutcome) {
    for (_, request) in requests {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a person who stopped waiting needs no answer"
        )]
        let _ = request.send(outcome.clone());
    }
}

/// Requests a pass took when it began, answered when it ends. A pass that
/// panicked, or was dropped, before answering them leaves them waiting
/// again, for its owner's ending to answer.
#[must_use = "the pass answers its requests when it ends"]
pub struct PassRequests {
    taken: Waiting,
    /// The schedule they wait in again, unless it is gone.
    waits: Weak<RetryWaits>,
}

impl PassRequests {
    /// Answer every request with how the pass ended.
    pub fn answer(mut self, outcome: &RetryOutcome) {
        answer(std::mem::take(&mut self.taken), outcome);
    }
}

impl Drop for PassRequests {
    fn drop(&mut self) {
        if self.taken.is_empty() {
            return;
        }
        let Some(waits) = self.waits.upgrade() else {
            return;
        };
        lock_unpoisoned(&waits.requests).waiting.append(&mut self.taken);
    }
}

/// Waiting requests by report number, and the failure the row says holds
/// every pass: the error objects a `Failed` answer carries, which the row
/// can't hold. Both change only in a step on the writer, once the row they
/// go with commits.
pub(crate) struct RetryRequests {
    held: Option<PassFailure>,
    waiting: Waiting,
}

impl RetryRequests {
    /// No request waiting, and no failure held.
    pub(crate) fn new() -> Self {
        Self { held: None, waiting: Vec::new() }
    }

    /// The answer a request gets at once in `state`, none when a pass
    /// answers it: the effect's.
    fn at_once(&self, state: RetryState) -> Option<RetryOutcome> {
        match RetryEffect::from(state) {
            RetryEffect::Pass => None,
            RetryEffect::CoolingDown { until } => Some(RetryOutcome::CoolingDown { until }),
            RetryEffect::Failed => Some(RetryOutcome::Failed(
                self.held.clone().expect("the row holds a failure only with its failure"),
            )),
            RetryEffect::NotSyncing => Some(RetryOutcome::NotSyncing),
        }
    }

    /// The waiting requests the connection's owner has acted on, which
    /// wait in `waits` again if a pass drops them unanswered.
    fn acted_on(&mut self, waits: &Arc<RetryWaits>, handled: u64) -> PassRequests {
        let (taken, later) = std::mem::take(&mut self.waiting)
            .into_iter()
            .partition::<Vec<_>, _>(|(number, _)| *number <= handled);
        self.waiting = later;
        PassRequests { taken, waits: Arc::downgrade(waits) }
    }
}

/// When the live provider cooldown ends, by the store's clock as it was
/// armed, so writing one cooldown again names the same instant. `None` also
/// when the clock cannot represent that time; the pass then answers its
/// requests once the cooldown ends.
fn cooldown_until(schedule: &Mutex<super::WaitSchedule>, clock: &ClockRef) -> Option<SystemTime> {
    let elapsed = clock.elapsed();
    lock_unpoisoned(schedule)
        .armed
        .get(&coven_database::RetrySubject::Provider)
        .filter(|armed| !armed.over(elapsed))?
        .ends
}

/// The answer to the requests waiting when a step fails, carrying the typed
/// error the step's caller gets, never its text (F.5), or `NotSyncing` once
/// the store closed. A step fails only as its handle does, whose failure
/// every later call reports from one shared cause, so the answer carries that
/// same failure; as its runtime shuts down, saying how far it got; or as it
/// panics, with the bug.
fn failed(error: &DbError) -> RetryOutcome {
    let failure = match error {
        DbError::StoreClosed => return RetryOutcome::NotSyncing,
        DbError::StatePersistence(cause) => DbError::StatePersistence(cause.clone()),
        DbError::RollbackFailed => DbError::RollbackFailed,
        DbError::CheckpointFailed(cause) => DbError::CheckpointFailed(cause.clone()),
        DbError::StatisticsFailed(cause) => DbError::StatisticsFailed(cause.clone()),
        DbError::WallTimer(cause) => DbError::WallTimer(cause.clone()),
        DbError::StoreFailed(bug) => DbError::StoreFailed(bug.clone()),
        DbError::Shutdown(shutdown) => DbError::Shutdown(*shutdown),
        DbError::Bug(bug) => DbError::Bug(bug.clone()),
        _ => DbError::Invariant { what: "a retry step fails only as its handle or runtime does" },
    };
    PassFailure::Failed { error: SyncFailure::Other(Arc::new(failure)) }.into()
}

impl RetryWaits {
    /// The row with the cooldown live as the step that writes it commits,
    /// while sync is started; while it is stopped, none.
    pub(super) fn live_cooldown(&self) -> impl FnOnce(RetryState) -> RetryState + Send + 'static {
        let (schedule, clock) = (self.state.clone(), self.clock.clone());
        move |state| match state.sync {
            RetrySync::Stopped => state,
            RetrySync::Started { .. } => RetryState {
                sync: RetrySync::Started { cooling_until: cooldown_until(&schedule, &clock) },
                ..state
            },
        }
    }

    /// Change the row with `change` and the live cooldown, then run `then`
    /// with the requests and the committed state, in one step on the
    /// writer. A failed write answers every waiting request with it.
    async fn step<T: Send + 'static>(
        self: &Arc<Self>,
        change: impl FnOnce(RetryState) -> RetryState + Send + 'static,
        then: impl FnOnce(&Arc<RetryWaits>, &mut RetryRequests, RetryState) -> T + Send + 'static,
    ) -> Result<T, DbError> {
        self.step_as(Step::Change, change, then).await
    }

    /// `step`, as `kind` commits it.
    async fn step_as<T: Send + 'static>(
        self: &Arc<Self>,
        kind: Step,
        change: impl FnOnce(RetryState) -> RetryState + Send + 'static,
        then: impl FnOnce(&Arc<RetryWaits>, &mut RetryRequests, RetryState) -> T + Send + 'static,
    ) -> Result<T, DbError> {
        let Some(database) = &self.database else {
            return Err(DbError::Invariant { what: "only a handle that writes the store syncs" });
        };
        let cooling = self.live_cooldown();
        let waits = self.clone();
        let change = move |state| cooling(change(state));
        let then = move |state: RetryState| {
            waits.syncing.send_if_modified(|syncing| {
                let started = matches!(state.sync, RetrySync::Started { .. });
                std::mem::replace(syncing, started) != started
            });
            then(&waits, &mut lock_unpoisoned(&waits.requests), state)
        };
        let result = match kind {
            Step::Change => database.change_retry_state(change, then).await,
            Step::Start => database.start_retry_state(change, then).await,
        };
        if let Err(error) = &result {
            let waiting = std::mem::take(&mut lock_unpoisoned(&self.requests).waiting);
            answer(waiting, &failed(error));
        }
        result
    }

    /// The person asked to retry everything now (§6.7): each owner ends
    /// every ordinary wait it retries when it next plans, as for a network
    /// report, answered or not, and the provider cooldown holds. Stopped,
    /// held or cooling down, as the row says, answers at once; otherwise the
    /// first pass that begins after the connection's owner acted on the
    /// request answers it. A closed store answers `NotSyncing`: no pass
    /// follows.
    pub async fn retry_requested(self: &Arc<Self>) -> Result<RetryAnswer, DbError> {
        let (answer, answered) = oneshot::channel();
        let asked = self
            .step(
                |state| state,
                move |waits, requests, state| {
                    let number = waits.report(crate::retry_waits::Ending::Every);
                    match requests.at_once(state) {
                        None => requests.waiting.push((number, answer)),
                        Some(outcome) => {
                            #[expect(
                                clippy::let_underscore_must_use,
                                reason = "the receiver is still held here"
                            )]
                            let _ = answer.send(outcome);
                        }
                    }
                },
            )
            .await;
        match asked {
            Ok(()) => Ok(RetryAnswer(answered)),
            Err(DbError::StoreClosed) => {
                let (answer, answered) = oneshot::channel();
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "the receiver is still held here"
                )]
                let _ = answer.send(RetryOutcome::NotSyncing);
                Ok(RetryAnswer(answered))
            }
            Err(error) => Err(error),
        }
    }

    /// A started connection will plan: requests wait for its passes, and
    /// the failure its owner last found still holds them. Once a worker's
    /// panic has halted sync (`sync_halted`), it fails with that bug as
    /// `SyncError::WorkerFailed` until the store reopens (E15).
    pub async fn sync_started(self: &Arc<Self>) -> Result<(), crate::SyncError> {
        if let Some(bug) = self.halted() {
            return Err(crate::SyncError::WorkerFailed(bug));
        }
        Ok(self
            .step_as(
                Step::Start,
                |state| RetryState { sync: RetrySync::Started { cooling_until: None }, ..state },
                |_, _, _| (),
            )
            .await?)
    }

    /// Sync stopped, or no connection will plan: no pass follows a waiting
    /// request, so each is answered `NotSyncing`. A pass already running
    /// answers the requests it took.
    pub async fn sync_stopped(self: &Arc<Self>) -> Result<(), DbError> {
        let stopped = self
            .step(
                |state| RetryState { sync: RetrySync::Stopped, ..state },
                |_, requests, _| std::mem::take(&mut requests.waiting),
            )
            .await?;
        answer(stopped, &RetryOutcome::NotSyncing);
        Ok(())
    }

    /// The upload worker, which serves the whole store, panicked with
    /// `bug`, which the sink heard: sync halts for the rest of this store's
    /// session, as the connection owner's own panic halts it (E15). The last
    /// pass shows the bug with nothing moving, sync stops, each waiting
    /// request hears it as `SyncError::WorkerFailed`, the connection's owner
    /// ends on it, and every start fails with it until the store reopens.
    pub async fn sync_halted(
        self: &Arc<Self>,
        bug: coven_foundation::task::Bug,
    ) -> Result<(), DbError> {
        self.halted.send_replace(Some(bug.clone()));
        if let Some(database) = &self.database {
            crate::LastPass::failed(crate::SyncError::WorkerFailed(bug.clone()))
                .record(database)
                .await?;
        }
        self.sync_panicked(bug).await
    }

    /// The connection's owner panicked with `bug`, which the sink heard, so
    /// no pass follows a waiting request: sync stops, and each request,
    /// those the panicking pass took among them, is answered `Failed` with
    /// `SyncError::WorkerFailed` carrying the bug (E15).
    pub async fn sync_panicked(
        self: &Arc<Self>,
        bug: coven_foundation::task::Bug,
    ) -> Result<(), DbError> {
        let waiting = self
            .step(
                |state| RetryState { sync: RetrySync::Stopped, ..state },
                |_, requests, _| std::mem::take(&mut requests.waiting),
            )
            .await?;
        let error = crate::SyncError::WorkerFailed(bug).into();
        answer(waiting, &PassFailure::Failed { error }.into());
        Ok(())
    }

    /// A pass begins, if the row lets one: sync is started, and neither a
    /// failure nor a cooldown holds it. It takes every waiting request the
    /// connection's owner has acted on (IO `pass_answers_acted_on`). `None`
    /// when the row holds the pass, and the owner plans again.
    pub async fn begin_pass(self: &Arc<Self>) -> Result<Option<PassRequests>, DbError> {
        self.step(
            |state| state,
            |waits, requests, state| {
                (RetryEffect::from(state) == RetryEffect::Pass)
                    .then(|| requests.acted_on(waits, waits.handled(NetworkOwner::Connection)))
            },
        )
        .await
    }

    /// The connection's owner found what holds every pass: a failure only
    /// new credentials, an update or a reset can clear, or none. It says so
    /// as it opens and after each pass, before writing the status a failed
    /// pass leaves, so the effect never trails that status; each time it
    /// plans, `answer_held` says so too.
    pub async fn holds(self: &Arc<Self>, failure: Option<&PassFailure>) -> Result<(), DbError> {
        let failure = failure.cloned();
        let held = failure.is_some();
        self.step(
            move |state| RetryState { held, ..state },
            move |_, requests, _| requests.held = failure,
        )
        .await
    }

    /// The owner plans: it records what holds every pass, as `holds` does,
    /// and answers the requests it has acted on now if no pass will run for
    /// them (IO `held_answers`). A failure only new credentials, an update
    /// or a reset can clear answers `Failed`; otherwise a live provider
    /// cooldown answers `CoolingDown`, and anything else that holds the
    /// pass leaves them for it. The answer is the effect's.
    pub async fn answer_held(
        self: &Arc<Self>,
        failure: Option<&PassFailure>,
    ) -> Result<(), DbError> {
        let failure = failure.cloned();
        let held = failure.is_some();
        let answered = self
            .step(
                move |state| RetryState { held, ..state },
                move |waits, requests, state| {
                    requests.held = failure;
                    // With no answer at once, a pass runs and answers them.
                    let outcome = requests.at_once(state)?;
                    let handled = waits.handled(NetworkOwner::Connection);
                    Some((requests.acted_on(waits, handled), outcome, RetryEffect::from(state)))
                },
            )
            .await?;
        let Some((acted_on, outcome, effect)) = answered else {
            return Ok(());
        };
        // No pass runs for these requests, so nothing else says what came of
        // the person's retry.
        if !acted_on.taken.is_empty() {
            tracing::info!(
                requests = acted_on.taken.len(),
                ?effect,
                "no pass runs for the person's retry; answering it at once"
            );
        }
        acted_on.answer(&outcome);
        Ok(())
    }

    /// The connection's first attempt connected: its requests wait for the
    /// loop's first pass, or are answered `NotSyncing` if sync stopped
    /// meanwhile.
    pub async fn return_requests(self: &Arc<Self>, mut taken: PassRequests) -> Result<(), DbError> {
        let stopped = self
            .step(
                |state| state,
                move |_, requests, state| match state.sync {
                    RetrySync::Started { .. } => {
                        requests.waiting.append(&mut taken.taken);
                        None
                    }
                    RetrySync::Stopped => Some(taken),
                },
            )
            .await?;
        if let Some(stopped) = stopped {
            stopped.answer(&RetryOutcome::NotSyncing);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "retry_answer_tests.rs"]
mod tests;
