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
        Pin::new(&mut self.0)
            .poll(context)
            .map(|answer| answer.unwrap_or(RetryOutcome::NotSyncing))
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

/// Waiting requests by report number, and whether a started connection
/// will plan.
#[derive(Default)]
pub(crate) struct Requests {
    syncing: bool,
    waiting: Vec<(u64, oneshot::Sender<RetryOutcome>)>,
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
            #[expect(
                clippy::let_underscore_must_use,
                reason = "the receiver is still held here"
            )]
            let _ = answer.send(RetryOutcome::NotSyncing);
        }
        RetryAnswer(answered)
    }

    /// A started connection will plan: requests wait for its passes.
    pub fn sync_started(&self) {
        self.requests().syncing = true;
    }

    /// Sync stopped, or no connection will plan: no pass follows a waiting
    /// request, so each is answered `NotSyncing`. A pass already running
    /// answers the requests it took.
    pub fn sync_stopped(&self) {
        let waiting = {
            let mut requests = self.requests();
            requests.syncing = false;
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

    /// The pass is held: answer the requests the connection's owner has
    /// acted on now (IO `held_answers`). A failure only new credentials, an
    /// update or a reset can clear answers `Failed`; otherwise a live
    /// provider cooldown answers `CoolingDown`, and anything else that holds
    /// the pass leaves them for it.
    pub fn answer_held(&self, failure: Option<&PassFailure>) {
        let outcome = match failure {
            Some(failure) => RetryOutcome::Failed(failure.clone()),
            None => match self.cooldown_until() {
                Some(until) => RetryOutcome::CoolingDown { until },
                None => return,
            },
        };
        let handled = self.handled(NetworkOwner::Connection);
        Self::acted_on(&mut self.requests(), handled).answer(&outcome);
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

    /// When the live provider cooldown ends, by the store's clock.
    /// `None` also when the clock cannot represent that time; the pass
    /// then answers its requests once the cooldown ends.
    fn cooldown_until(&self) -> Option<SystemTime> {
        self.clock.now().checked_add(self.cooldown()?)
    }
}

#[cfg(test)]
#[path = "retry_answer_tests.rs"]
mod tests;
