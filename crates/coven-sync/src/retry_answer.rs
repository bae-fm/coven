//! The answer to the person's retry (§6.7; IO `RetryAnswer`).
//!
//! A retry request is a report, so it takes the next report number. The
//! connection's owner, the connection's first attempt and then its sync
//! loop, acts on reports when it plans between passes. A pass that begins
//! takes every waiting request the owner has acted on, and answers them
//! when it ends; a request made after the owner last acted waits for a
//! later pass. A pass held by a provider cooldown, or by a failure only new
//! credentials, an update or a reset can clear, answers the requests the
//! owner acted on at once. Stopping sync answers every waiting request
//! `NotSyncing`, and a request made while sync is stopped is answered so at
//! once. The requests and whether a started connection will plan share one
//! lock, so a request, stopping and a pass taking its requests each happen
//! at once (IO `never_stranded`).
//!
//! The same lock keeps the failure the owner found holding every pass. One
//! decision turns it, whether sync runs and the provider cooldown into both
//! the held pass's answers and the published `RetryEffect`, what a retry
//! made now would do (IO `published_answers_at_once`,
//! `published_pass_runs`). Starting, stopping, the owner's finding and a
//! cooldown beginning publish it at once. The owner wakes when the cooldown
//! ends and publishes that as it plans; no pass runs during a cooldown
//! unless the cooldown began during it, and then the plan after that pass
//! publishes it.

use crate::{NetworkOwner, PassFailure, RetryWaits};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use tokio::sync::oneshot;

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

/// What the person's retry would do if made now (E5): run a pass, or be
/// answered at once without one. An app offers a retry only while it is
/// `Pass`; the status says why it is not.
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

/// Requests a pass took when it began, answered when it ends.
#[must_use = "the pass answers its requests when it ends"]
pub struct PassRequests(Vec<(u64, oneshot::Sender<RetryOutcome>)>);

impl PassRequests {
    /// Answer every request with how the pass ended.
    pub fn answer(self, outcome: &RetryOutcome) {
        for (_, request) in self.0 {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a person who stopped waiting needs no answer"
            )]
            let _ = request.send(outcome.clone());
        }
    }
}

/// Waiting requests by report number, whether a started connection will
/// plan, and the failure its owner last found holding every pass; the next
/// owner says what holds them as it opens.
#[derive(Default)]
pub(crate) struct Requests {
    syncing: bool,
    held: Option<PassFailure>,
    waiting: Vec<(u64, oneshot::Sender<RetryOutcome>)>,
}

impl Requests {
    /// What a retry made now would do, with the answer it gets at once when
    /// no pass runs for it. A held failure answers before a cooldown: the
    /// cooldown's end would not let the pass run.
    fn decide(&self, cooldown_until: Option<SystemTime>) -> (RetryEffect, Option<RetryOutcome>) {
        if !self.syncing {
            (RetryEffect::NotSyncing, Some(RetryOutcome::NotSyncing))
        } else if let Some(failure) = &self.held {
            (RetryEffect::Failed, Some(RetryOutcome::Failed(failure.clone())))
        } else if let Some(until) = cooldown_until {
            (RetryEffect::CoolingDown { until }, Some(RetryOutcome::CoolingDown { until }))
        } else {
            (RetryEffect::Pass, None)
        }
    }
}

impl RetryWaits {
    fn requests(&self) -> std::sync::MutexGuard<'_, Requests> {
        self.requests.lock().expect("retry requests poisoned")
    }

    /// The person asked to retry everything now (§6.7): each owner ends
    /// every ordinary wait it retries when it next plans, as for a network
    /// report, answered or not, and the provider cooldown holds. The answer
    /// comes from the first pass that begins after the connection's owner
    /// acted on the request.
    pub fn retry_requested(&self) -> RetryAnswer {
        let (answer, answered) = oneshot::channel();
        let mut requests = self.requests();
        let number = self.report(crate::retry_waits::Ending::Every);
        if requests.syncing {
            requests.waiting.push((number, answer));
        } else {
            #[expect(clippy::let_underscore_must_use, reason = "the receiver is still held here")]
            let _ = answer.send(RetryOutcome::NotSyncing);
        }
        RetryAnswer(answered)
    }

    /// What a retry made now would do, and its changes (E5).
    pub fn subscribe_retry_effect(&self) -> tokio::sync::watch::Receiver<RetryEffect> {
        self.effect.subscribe()
    }

    /// Publish what a retry made now would do, under the requests' lock so
    /// publications follow the changes in order, with the answer a retry
    /// gets at once when no pass runs for it.
    fn publish(&self, requests: &Requests) -> (RetryEffect, Option<RetryOutcome>) {
        let (effect, at_once) = requests.decide(self.cooldown_until());
        self.effect.send_if_modified(|published| std::mem::replace(published, effect) != effect);
        (effect, at_once)
    }

    /// The provider cooldown began or grew: publish what a retry would do.
    pub(crate) fn cooldown_changed(&self) {
        self.publish(&self.requests());
    }

    /// A started connection will plan: requests wait for its passes.
    pub fn sync_started(&self) {
        let mut requests = self.requests();
        requests.syncing = true;
        self.publish(&requests);
    }

    /// Sync stopped, or no connection will plan: no pass follows a waiting
    /// request, so each is answered `NotSyncing`. A pass already running
    /// answers the requests it took.
    pub fn sync_stopped(&self) {
        let waiting = {
            let mut requests = self.requests();
            requests.syncing = false;
            self.publish(&requests);
            std::mem::take(&mut requests.waiting)
        };
        PassRequests(waiting).answer(&RetryOutcome::NotSyncing);
    }

    /// The waiting requests the connection's owner has acted on.
    fn acted_on(requests: &mut Requests, handled: u64) -> PassRequests {
        let (taken, later) = std::mem::take(&mut requests.waiting)
            .into_iter()
            .partition::<Vec<_>, _>(|(number, _)| *number <= handled);
        requests.waiting = later;
        PassRequests(taken)
    }

    /// A pass begins: it takes every waiting request the connection's owner
    /// has acted on (IO `pass_answers_acted_on`).
    pub fn take_requests(&self) -> PassRequests {
        let handled = self.handled(NetworkOwner::Connection);
        Self::acted_on(&mut self.requests(), handled)
    }

    /// The connection's owner found what holds every pass: a failure only
    /// new credentials, an update or a reset can clear, or none. It says so
    /// as it opens and after each pass, before publishing the status a
    /// failed pass leaves, so the published effect never trails that
    /// status; each time it plans, `answer_held` says so too.
    pub fn holds(&self, failure: Option<&PassFailure>) {
        let mut requests = self.requests();
        requests.held = failure.cloned();
        self.publish(&requests);
    }

    /// The owner plans: it records what holds every pass, as `holds` does,
    /// and answers the requests it has acted on now if no pass will run for
    /// them (IO `held_answers`). A failure only new credentials, an update
    /// or a reset can clear answers `Failed`; otherwise a live provider
    /// cooldown answers `CoolingDown`, and anything else that holds the
    /// pass leaves them for it. The answer is the published effect's.
    pub fn answer_held(&self, failure: Option<&PassFailure>) {
        let handled = self.handled(NetworkOwner::Connection);
        let mut requests = self.requests();
        requests.held = failure.cloned();
        // With no answer at once, a pass runs and answers them.
        let (effect, Some(outcome)) = self.publish(&requests) else {
            return;
        };
        let acted_on = Self::acted_on(&mut requests, handled);
        drop(requests);
        // No pass runs for these requests, so nothing else says what came of
        // the person's retry.
        if !acted_on.0.is_empty() {
            tracing::info!(
                requests = acted_on.0.len(),
                ?effect,
                "no pass runs for the person's retry; answering it at once"
            );
        }
        acted_on.answer(&outcome);
    }

    /// The connection's first attempt connected: its requests wait for the
    /// loop's first pass, or are answered `NotSyncing` if sync stopped
    /// meanwhile.
    pub fn return_requests(&self, taken: PassRequests) {
        let mut requests = self.requests();
        if requests.syncing {
            requests.waiting.extend(taken.0);
        } else {
            drop(requests);
            taken.answer(&RetryOutcome::NotSyncing);
        }
    }

    /// When the live provider cooldown ends, by the store's clock as it
    /// was recorded, so republishing one cooldown names the same instant.
    /// `None` also when the clock cannot represent that time; the pass
    /// then answers its requests once the cooldown ends.
    fn cooldown_until(&self) -> Option<SystemTime> {
        let elapsed = self.clock.elapsed();
        self.lock()
            .armed
            .get(&coven_database::RetrySubject::Provider)
            .filter(|armed| !armed.over(elapsed))?
            .ends
    }
}

#[cfg(test)]
#[path = "retry_answer_tests.rs"]
mod tests;
