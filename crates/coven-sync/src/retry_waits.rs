//! The shared retry schedule (§3.1, §16.5, SP waiting).
//!
//! Durable rows hold each wait's wall-clock deadline `T` and full wait `W`;
//! this owner keeps the monotonic timer a running session enforces: a wait
//! is over when the clock's elapsed time reaches its deadline or its timer
//! fires, whichever the clock reports first.
//! Opening arms `max(0, min(T - now, W))`, so a clock moved back cannot
//! stretch a wait and one moved forward can only shorten it. The provider
//! cooldown applies to every consumer of the store's connection: no
//! subject is due while it lasts, whatever its own wait, and an app retry
//! overrides only the subject's ordinary wait.

use crate::SyncError;
use coven_database::{Database, DbError, RetrySubject, RetryWait};
use coven_foundation::clock::ClockRef;
use coven_storage::{retry_delay, StorageError, StorageFailure};
use futures_util::FutureExt;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Mutex,
    time::{Duration, SystemTime},
};

type Timer = futures_util::future::Shared<Pin<Box<dyn Future<Output = ()> + Send>>>;

/// A wake for the work an owner retries next.
pub(crate) type Wake = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The wait a reopened store arms for a persisted `T` and `W`.
pub(crate) fn restart_wait(not_before: SystemTime, now: SystemTime, wait: Duration) -> Duration {
    not_before
        .duration_since(now)
        .unwrap_or(Duration::ZERO)
        .min(wait)
}

/// One installation's retry schedule. Construct when the store opens and
/// inject the same owner into operations, file transfers, the sync loop and
/// every provider connection.
pub struct RetryWaits {
    clock: ClockRef,
    /// Absent for a read-only handle, whose cooldowns last for its session.
    database: Option<Database>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    armed: BTreeMap<RetrySubject, Armed>,
    /// Committed evidence that can satisfy a waiting operation's
    /// prerequisites; each change makes every operation wait due once.
    evidence: u64,
    /// The evidence count when each operation was last judged due or not,
    /// which starts any attempt at it: the wait that attempt ends in begins
    /// there when other work committed while it ran.
    started: BTreeMap<RetrySubject, u64>,
}

#[derive(Clone)]
struct Armed {
    /// The persisted `T` this deadline enforces.
    not_before: SystemTime,
    /// Monotonic deadline and its timer; `None` waits for the app.
    timer: Option<(Duration, Timer)>,
    evidence: u64,
    /// Its owner has evaluated it since its wait ended, so the ended wait
    /// needs no further wake.
    seen: bool,
}

impl Armed {
    fn over(&self, elapsed: Duration) -> bool {
        self.timer
            .as_ref()
            .is_some_and(|(due, timer)| *due <= elapsed || timer.clone().now_or_never().is_some())
    }
}

impl RetryWaits {
    /// Arm every persisted wait with the restart rule (§16.5).
    pub async fn open(database: Database, clock: ClockRef) -> Result<Self, DbError> {
        let waits = database.retry_waits().await?;
        let now = clock.now();
        let owner = Self {
            clock,
            database: Some(database),
            state: Mutex::new(State::default()),
        };
        for (subject, wait) in waits {
            owner.arm_for(
                subject,
                wait.not_before,
                restart_wait(wait.not_before, now, wait.wait),
            );
        }
        Ok(owner)
    }

    /// A read-only handle cannot persist: its cooldowns hold only its own requests.
    pub fn in_memory(clock: ClockRef) -> Self {
        Self {
            clock,
            database: None,
            state: Mutex::new(State::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("retry waits poisoned")
    }

    fn cooling(state: &State, elapsed: Duration) -> bool {
        state
            .armed
            .get(&RetrySubject::Provider)
            .is_some_and(|armed| !armed.over(elapsed))
    }

    /// The next wait after an unsuccessful attempt: the backoff continues
    /// from `previous`, and `T` is persisted with `W` before waiting.
    pub(crate) fn next(&self, previous: Option<&RetryWait>) -> Result<RetryWait, SyncError> {
        let failures = previous.map_or(0, |wait| wait.failures).saturating_add(1);
        let wait = retry_delay(failures - 1);
        let not_before = self.clock.now().checked_add(wait).ok_or_else(|| {
            StorageFailure::InvalidConfiguration
                .with_source("clock cannot represent a retry deadline")
        })?;
        Ok(RetryWait {
            failures,
            not_before,
            wait,
        })
    }

    /// Enforce a persisted wait monotonically from now.
    pub(crate) fn arm(&self, subject: RetrySubject, wait: &RetryWait) {
        self.arm_for(subject, wait.not_before, wait.wait);
    }

    fn arm_for(&self, subject: RetrySubject, not_before: SystemTime, wait: Duration) {
        self.arm_since(subject, not_before, wait, None);
    }

    /// Arm a wait that began at evidence count `since`, or now.
    fn arm_since(
        &self,
        subject: RetrySubject,
        not_before: SystemTime,
        wait: Duration,
        since: Option<u64>,
    ) {
        let timer = (self.clock.elapsed() + wait, self.clock.sleep(wait).shared());
        let mut state = self.lock();
        let evidence = since.unwrap_or(state.evidence);
        state.armed.insert(
            subject,
            Armed {
                not_before,
                timer: Some(timer),
                evidence,
                seen: false,
            },
        );
    }

    /// A wait that could not be persisted holds until the app retries; it is
    /// never permission to retry at once (§16.5).
    pub(crate) fn hold(&self, subject: RetrySubject) {
        let mut state = self.lock();
        let evidence = state.evidence;
        state.armed.insert(
            subject,
            Armed {
                not_before: SystemTime::UNIX_EPOCH,
                timer: None,
                evidence,
                seen: false,
            },
        );
    }

    /// The subject's work succeeded, changed or waits for the app instead.
    pub(crate) fn forget(&self, subject: RetrySubject) {
        let mut state = self.lock();
        state.armed.remove(&subject);
        state.started.remove(&subject);
    }

    /// Forget operation waits whose operations no longer exist.
    pub(crate) fn retain_operations(
        &self,
        live: &std::collections::BTreeSet<coven_database::OperationId>,
    ) {
        let live = |subject: &RetrySubject| match subject {
            RetrySubject::Operation(id) => live.contains(id),
            _ => true,
        };
        let mut state = self.lock();
        state.armed.retain(|subject, _| live(subject));
        state.started.retain(|subject, _| live(subject));
    }

    /// Whether the subject holds an armed wait in this session.
    pub(crate) fn armed(&self, subject: RetrySubject) -> bool {
        self.lock().armed.contains_key(&subject)
    }

    /// Committed work may satisfy waiting operations' prerequisites. Each
    /// becomes due once without resetting its backoff.
    pub(crate) fn prerequisites_changed(&self) {
        self.lock().evidence += 1;
    }

    /// Whether automatic work on the subject may start now, given its
    /// persisted wait. No subject is due while the provider cooldown lasts.
    /// An absent row means the work advanced or the app retried it, unless
    /// this session holds the subject because persisting its wait failed.
    /// For an operation, an attempt starting now begins at this evidence count.
    pub(crate) fn due(&self, subject: RetrySubject, persisted: Option<&RetryWait>) -> bool {
        let elapsed = self.clock.elapsed();
        if matches!(subject, RetrySubject::Operation(_)) {
            let mut state = self.lock();
            let current = state.evidence;
            state.started.insert(subject, current);
        }
        let Some(persisted) = persisted else {
            let mut state = self.lock();
            if state
                .armed
                .get(&subject)
                .is_some_and(|armed| armed.timer.is_none())
            {
                return false;
            }
            state.armed.remove(&subject);
            return !Self::cooling(&state, elapsed);
        };
        // A row armed before this session, or replaced since, is enforced
        // from the restart rule.
        let current = self.lock().armed.get(&subject).cloned();
        if current
            .as_ref()
            .is_none_or(|armed| armed.not_before != persisted.not_before && armed.timer.is_some())
        {
            let wait = restart_wait(persisted.not_before, self.clock.now(), persisted.wait);
            self.arm_for(subject, persisted.not_before, wait);
        }
        let mut state = self.lock();
        let current = state.evidence;
        let cooling = Self::cooling(&state, elapsed);
        let armed = state.armed.get_mut(&subject).expect("armed above");
        let over = armed.over(elapsed);
        armed.seen |= over;
        let evidence = matches!(subject, RetrySubject::Operation(_))
            && armed.timer.is_some()
            && armed.evidence < current;
        !cooling && (over || evidence)
    }

    /// The owner evaluated the subject's work without running it, e.g.
    /// because its condition waits for the app: an ended wait needs no wake.
    pub(crate) fn seen(&self, subject: RetrySubject) {
        let elapsed = self.clock.elapsed();
        if let Some(armed) = self.lock().armed.get_mut(&subject) {
            armed.seen |= armed.over(elapsed);
        }
    }

    /// The live provider cooldown's remaining wait.
    pub(crate) fn cooldown(&self) -> Option<Duration> {
        let elapsed = self.clock.elapsed();
        let state = self.lock();
        let armed = state.armed.get(&RetrySubject::Provider)?;
        if armed.over(elapsed) {
            return None;
        }
        let (due, _) = armed.timer.as_ref()?;
        Some(due.saturating_sub(elapsed).max(Duration::from_nanos(1)))
    }

    /// How long until the earliest armed subject `matching` is due by its
    /// deadline, cooldown included; `None` when nothing it covers waits on a timer.
    #[cfg(test)]
    pub(crate) fn next_due(&self, matching: impl Fn(&RetrySubject) -> bool) -> Option<Duration> {
        let elapsed = self.clock.elapsed();
        let state = self.lock();
        let cooldown = state
            .armed
            .get(&RetrySubject::Provider)
            .and_then(|armed| armed.timer.as_ref())
            .map_or(Duration::ZERO, |(due, _)| *due);
        state
            .armed
            .iter()
            .filter(|(subject, _)| **subject != RetrySubject::Provider && matching(subject))
            .filter_map(|(_, armed)| armed.timer.as_ref())
            .map(|(due, _)| (*due).max(cooldown).saturating_sub(elapsed))
            .min()
    }

    /// Resolves when the earliest armed subject `matching` is due: its timer
    /// and any live provider cooldown have both fired. A wait that ended
    /// after its owner last evaluated it wakes at once; one the owner has
    /// evaluated since wakes nothing until new evidence or a new wait.
    /// `None` when nothing it covers waits on a timer.
    pub(crate) fn next_wake(&self, matching: impl Fn(&RetrySubject) -> bool) -> Option<Wake> {
        let elapsed = self.clock.elapsed();
        let state = self.lock();
        let earliest = state
            .armed
            .iter()
            .filter(|(subject, armed)| {
                **subject != RetrySubject::Provider
                    && matching(subject)
                    && !(armed.seen && armed.over(elapsed))
            })
            .filter_map(|(_, armed)| armed.timer.clone())
            .min_by_key(|(due, _)| *due)?;
        let cooldown = state
            .armed
            .get(&RetrySubject::Provider)
            .filter(|armed| !armed.over(elapsed))
            .and_then(|armed| armed.timer.clone());
        Some(Box::pin(async move {
            earliest.1.await;
            if let Some((_, cooldown)) = cooldown {
                cooldown.await;
            }
        }))
    }

    /// Resolves when the live provider cooldown ends.
    pub(crate) fn cooldown_wake(&self) -> Option<Wake> {
        let elapsed = self.clock.elapsed();
        let state = self.lock();
        let armed = state
            .armed
            .get(&RetrySubject::Provider)
            .filter(|armed| !armed.over(elapsed))?;
        let (_, timer) = armed.timer.clone()?;
        Some(Box::pin(timer))
    }

    /// Persist the subject's next wait after an unsuccessful attempt, then
    /// enforce it. A persistence failure holds the subject for the app.
    pub(crate) async fn fail(
        &self,
        subject: RetrySubject,
        previous: Option<&RetryWait>,
    ) -> Result<(), SyncError> {
        self.fail_since(subject, previous, None).await
    }

    /// An operation's attempt failed. Commits it made itself are no evidence
    /// against the wait it ends in. When other work's commits landed while
    /// it ran (`raced`), that wait begins where the attempt started, so they
    /// make it due at once (IO `racing_commit_wakes`).
    pub(crate) async fn fail_attempt(
        &self,
        subject: RetrySubject,
        previous: Option<&RetryWait>,
        raced: bool,
    ) -> Result<(), SyncError> {
        let started = self.lock().started.remove(&subject);
        self.fail_since(subject, previous, started.filter(|_| raced))
            .await
    }

    async fn fail_since(
        &self,
        subject: RetrySubject,
        previous: Option<&RetryWait>,
        since: Option<u64>,
    ) -> Result<(), SyncError> {
        let wait = self.next(previous)?;
        let Some(database) = &self.database else {
            self.arm_since(subject, wait.not_before, wait.wait, since);
            return Ok(());
        };
        match database.record_retry(subject, wait).await {
            Ok(()) => {
                self.arm_since(subject, wait.not_before, wait.wait, since);
                Ok(())
            }
            Err(error) => {
                self.hold(subject);
                Err(error.into())
            }
        }
    }

    /// Remove the subject's persisted wait once its work no longer waits.
    pub(crate) async fn clear(&self, subject: RetrySubject) -> Result<(), SyncError> {
        if !self.armed(subject) {
            return Ok(());
        }
        // The work may have ended its row already, e.g. by advancing; an
        // unchanged wait takes no writer.
        if let Some(database) = &self.database {
            if database.retry_wait(subject).await?.is_some() {
                database.clear_retry(subject).await?;
            }
        }
        self.forget(subject);
        Ok(())
    }
}

#[async_trait::async_trait]
impl coven_storage::Cooldown for RetryWaits {
    fn remaining(&self) -> Option<Duration> {
        self.cooldown()
    }

    /// Arm first, so no consumer in this session retries, then persist.
    /// A cooldown never shortens one already live.
    async fn record(&self, wait: Duration) -> Result<(), StorageError> {
        if self.cooldown().is_some_and(|remaining| remaining >= wait) {
            return Ok(());
        }
        let not_before = self.clock.now().checked_add(wait).ok_or_else(|| {
            StorageFailure::InvalidConfiguration.with_source("clock cannot represent a cooldown")
        })?;
        let cooldown = RetryWait {
            failures: 0,
            not_before,
            wait,
        };
        self.arm(RetrySubject::Provider, &cooldown);
        match &self.database {
            Some(database) => database
                .record_retry(RetrySubject::Provider, cooldown)
                .await
                .map_err(|error| StorageFailure::File.with_source(error)),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "retry_waits_tests.rs"]
pub(crate) mod tests;
