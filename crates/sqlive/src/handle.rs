//! A handle: the failure state and the subscriptions that several databases
//! share. Per-subscription dependencies: commits during a read are retained
//! until its dependencies are known; idle subscriptions retain only a changed
//! bit.

use crate::commit_scope::CommitScope;
use crate::database_changes::DatabaseChanges;
use crate::{affected, Answers, EngineFailure, Error, Lookups, ReadSet, RowChange, RowScope};
use caught::locks::lock_unpoisoned;
use caught::{Bug, BugSinkRef};
use std::{
    any::Any,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::watch;

/// Tables whose changes no write records for live queries, lowercase.
#[derive(Debug)]
pub struct UnrecordedTables(BTreeSet<String>);

impl UnrecordedTables {
    /// The tables `tables` names, in any case.
    pub fn new<'a>(tables: impl IntoIterator<Item = &'a str>) -> Self {
        Self(tables.into_iter().map(str::to_ascii_lowercase).collect())
    }

    /// Whether a write records its changes to `table` (ASCII case-insensitive).
    pub fn recorded(&self, table: &str) -> bool {
        !self.0.contains(&table.to_ascii_lowercase())
    }
}

/// The host's failure that a handle failed with, or a bug. A subscription
/// holds it too, and its type names no host failure type, so the host's
/// failure is held as `Any`: the handle, whose type names the failure type,
/// takes it back out ([`Handle::error`]).
#[derive(Clone)]
pub(crate) enum Failure {
    /// A failure that the host reported, or an engine failure that ended the
    /// handle, as the host's failure type holds it.
    Host(Arc<dyn Any + Send + Sync>),
    /// A bug, a panic or a fault a drop found, may have left the databases'
    /// state wrong (E15). It reached its one reporter as it happened, so
    /// the calls after it repeat it as `Error::HandleFailed`.
    Bug(Bug),
}

/// Why a live query's run or a change watch fails without running: the
/// handle failed or closed, or a run panicked.
pub(crate) enum Stop {
    /// The handle failed with this.
    Failed(Failure),
    /// The handle closed.
    Closed,
    /// A run panicked with this bug.
    Bug(Bug),
}

/// How a live query's or a change watch's error type `E` holds the error a
/// [`Stop`] stands for.
pub(crate) type Failed<E> = Arc<dyn Fn(Stop) -> E + Send + Sync>;

/// What a subscription has seen of its handle: whether a commit changed what
/// it waits on, and whether the handle closed or failed.
#[derive(Clone)]
pub struct CommitState {
    /// Whether a commit changed what the subscription waits on since it
    /// last began a run.
    pub changed: bool,
    closed: bool,
    failure: Option<Failure>,
}

impl CommitState {
    /// Whether the handle has closed.
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Why the subscription stopped, if it did: the handle's failure, or its
    /// closing.
    pub(crate) fn stopped_by(&self) -> Option<Stop> {
        match &self.failure {
            Some(failure) => Some(Stop::Failed(failure.clone())),
            None => self.closed.then_some(Stop::Closed),
        }
    }

    /// Whether the handle failed or closed.
    pub fn stopped(&self) -> bool {
        self.closed || self.failure.is_some()
    }
}

enum Tracking {
    /// A run whose reads aren't known until it ends: the commits made
    /// since it began, kept to judge then, and the reads of the run before
    /// it, whose scopes name the values those commits record (§4).
    Reading {
        commits: Vec<Arc<Vec<RowChange>>>,
        previous: Arc<ReadSet>,
    },
    Waiting(Arc<ReadSet>),
}

impl Tracking {
    /// The reads whose scopes a write records values for: those waited on,
    /// or, while a run reads, its previous run's, which a run with the
    /// same statements tests again.
    fn scoped(&self) -> &Arc<ReadSet> {
        match self {
            Self::Reading { previous: reads, .. } | Self::Waiting(reads) => reads,
        }
    }
}

/// A subscription's tracking, and how many times it began or finished a
/// run: a write judged against one count's reads can't stand for another's.
struct Tracked {
    tracking: Tracking,
    epoch: u64,
}

struct SubscriptionState {
    tracking: Mutex<Tracked>,
    state: watch::Sender<CommitState>,
    /// Commits made within this scope are not observed.
    outside: Option<CommitScope>,
    /// The tables whose changes no write records, which a run may not read.
    unrecorded: Arc<UnrecordedTables>,
}

/// One waiter on a handle's commits: a live query's, a change watch's, or the
/// host's own.
#[derive(Clone)]
pub struct CommitSubscription {
    shared: Arc<SubscriptionState>,
    receiver: watch::Receiver<CommitState>,
}

impl CommitSubscription {
    fn new(
        closed: bool,
        failure: Option<Failure>,
        outside: Option<CommitScope>,
        unrecorded: Arc<UnrecordedTables>,
    ) -> Self {
        let (state, receiver) = watch::channel(CommitState { changed: false, closed, failure });
        Self {
            shared: Arc::new(SubscriptionState {
                tracking: Mutex::new(Tracked {
                    tracking: Tracking::Waiting(Arc::new(Vec::new())),
                    epoch: 0,
                }),
                state,
                outside,
                unrecorded,
            }),
            receiver,
        }
    }

    /// The state now, without marking it seen.
    pub fn state(&self) -> CommitState {
        self.receiver.borrow().clone()
    }

    /// Returns the current state and marks it as seen, so `changed` waits for a
    /// later state.
    pub fn observe(&mut self) -> CommitState {
        self.receiver.borrow_and_update().clone()
    }

    /// Wait for a state this subscription hasn't seen.
    pub async fn changed(&mut self) {
        self.receiver.changed().await.expect("subscription owns its sender");
    }

    /// Start keeping the commits a run meets, until it finishes. Meanwhile
    /// a write records the values the scopes this subscription went by
    /// until now test.
    pub fn begin(&self) {
        let mut tracked = lock_unpoisoned(&self.shared.tracking);
        let previous = Arc::clone(tracked.tracking.scoped());
        tracked.tracking = Tracking::Reading { commits: Vec::new(), previous };
        tracked.epoch += 1;
        self.shared.state.send_modify(|state| state.changed = false);
    }

    /// Wait on nothing, whatever this subscription was doing: its query
    /// ended on a bug, so what its run read is unknown (E15).
    pub fn stop(&self) {
        let mut tracked = lock_unpoisoned(&self.shared.tracking);
        tracked.tracking = Tracking::Waiting(Arc::new(Vec::new()));
        tracked.epoch += 1;
    }

    /// Wait on `reads`, which join no table: no commit needs a lookup.
    pub fn finish(&self, reads: ReadSet) {
        self.finish_judging(reads, &mut |_| unreachable!("only a live query's run reads a join"))
            .expect("judging without lookups can't fail");
    }

    /// Wait on `reads`, a run's. The commits made while it ran are judged
    /// against them, with `lookups` reading the run's own snapshot: a value
    /// matches a join there exactly when the run's partners held it. A
    /// lookup that fails marks the query to run again, and its error is
    /// the run's.
    pub fn finish_judging(
        &self,
        reads: ReadSet,
        lookups: &mut Lookups<'_>,
    ) -> rusqlite::Result<()> {
        if let Some(read) = reads.iter().find(|read| !self.shared.unrecorded.recorded(&read.table))
        {
            panic!("a live query read {}, whose changes no write records", read.table);
        }
        let reads = Arc::new(reads);
        let mut changed = false;
        let mut failure = None;
        let mut answers = Answers::default();
        loop {
            // Judge without the lock, so writes go on committing; those that
            // commit meanwhile are judged on the next pass.
            let commits = {
                let mut tracked = lock_unpoisoned(&self.shared.tracking);
                let Tracking::Reading { commits, .. } = &mut tracked.tracking else {
                    panic!("read observation was not started")
                };
                if commits.is_empty() {
                    tracked.tracking = Tracking::Waiting(Arc::clone(&reads));
                    tracked.epoch += 1;
                    self.shared.state.send_modify(|state| state.changed = changed);
                    break;
                }
                std::mem::take(commits)
            };
            for changes in commits {
                if changed {
                    break;
                }
                match affected(&reads, &changes, &mut answers, lookups) {
                    Ok(found) => changed = found,
                    Err(error) => {
                        changed = true;
                        failure = Some(error);
                    }
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// A write's changes, and how each live query that waited on known reads
/// judged them in the write's transaction, before it committed.
pub struct Judged {
    changes: Arc<Vec<RowChange>>,
    /// Each subscription judged, at its epoch then, and whether the changes
    /// affect it.
    verdicts: Vec<(Weak<SubscriptionState>, u64, bool)>,
}

impl Judged {
    /// Changes no live query judged, as those that name no row: each
    /// waiting query judges them as they reach it, without lookups.
    fn unjudged(changes: Vec<RowChange>) -> Self {
        Self { changes: Arc::new(changes), verdicts: Vec::new() }
    }
}

/// Every commit made outside one attempt since it began, kept until the
/// attempt names the rows its wait depends on.
pub struct AttemptCommits {
    commits: CommitSubscription,
}

struct ObserverState {
    subscriptions: Vec<Weak<SubscriptionState>>,
    closed: bool,
    failure: Option<Failure>,
}

thread_local! {
    /// The handle's call this thread runs, if any, and the bug its own work
    /// failed that handle with (E15).
    // Clippy asks for a `const` initializer this already has where the
    // target has no native thread locals, as Android: std builds those
    // from a function, which the lint reads as the initializer.
    #[cfg_attr(target_os = "android", allow(clippy::missing_const_for_thread_local))]
    static CALL: RefCell<Option<Call>> = const { RefCell::new(None) };
}

/// One of a handle's calls running on this thread.
struct Call {
    /// The handle the call is on, by its state's address.
    handle: usize,
    /// The first bug the call's work failed the handle with, which the call
    /// returns as its own.
    bug: Option<Bug>,
}

/// The bug of `what`, a lock's state, which a panic while it was held may
/// have left half-changed.
#[track_caller]
fn half_changed(what: &'static str) -> Bug {
    Bug::new(what, "a panic while it was locked may have left it half-changed")
}

impl ObserverState {
    /// Fail every later call and subscription. The handle keeps the first
    /// failure and ignores later ones.
    fn fail(&mut self, failure: Failure) {
        let error = self.failure.get_or_insert(failure).clone();
        self.subscriptions.retain(|subscription| {
            let Some(subscription) = subscription.upgrade() else {
                return false;
            };
            subscription.state.send_modify(|state| state.failure = Some(error.clone()));
            true
        });
    }

    fn close(&mut self) {
        self.closed = true;
        self.subscriptions.retain(|subscription| {
            let Some(subscription) = subscription.upgrade() else {
                return false;
            };
            subscription.state.send_modify(|state| state.closed = true);
            true
        });
    }
}

impl Drop for ObserverState {
    fn drop(&mut self) {
        self.close();
    }
}

/// The failure state and the subscriptions that several databases share: a
/// failure in one fails every later call and live query on each, with the
/// host's failure type `F`.
#[derive(Clone)]
pub struct Handle<F> {
    state: Arc<Mutex<ObserverState>>,
    /// Where a bug that fails the handle outside its calls goes (E15).
    bugs: BugSinkRef,
    /// The tables whose changes no write records.
    unrecorded: Arc<UnrecordedTables>,
    /// How many more of the databases' upkeep threads start before the next
    /// is refused, when a test asked for that.
    #[cfg(any(test, feature = "test-utils"))]
    upkeep_threads: Arc<Mutex<Option<usize>>>,
    failure: PhantomData<fn() -> F>,
}

impl<F: Clone + Send + Sync + 'static> Handle<F> {
    /// A handle whose bugs outside its calls go to `bugs`, and whose
    /// databases' writes record no changes to the tables `unrecorded` names
    /// for live queries.
    pub fn new(bugs: BugSinkRef, unrecorded: Vec<String>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ObserverState {
                subscriptions: Vec::new(),
                closed: false,
                failure: None,
            })),
            bugs,
            unrecorded: Arc::new(UnrecordedTables::new(unrecorded.iter().map(String::as_str))),
            #[cfg(any(test, feature = "test-utils"))]
            upkeep_threads: Arc::new(Mutex::new(None)),
            failure: PhantomData,
        }
    }

    /// A test handle for which every table records its changes.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn recording_all(bugs: BugSinkRef) -> Self {
        Self::new(bugs, Vec::new())
    }

    /// The tables whose changes no write records.
    pub fn unrecorded(&self) -> &Arc<UnrecordedTables> {
        &self.unrecorded
    }

    /// Let `started` upkeep threads start, if given, and refuse the next.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn refuse_upkeep_thread_after(&self, started: Option<usize>) {
        *lock_unpoisoned(&self.upkeep_threads) = started;
    }

    /// Whether the system refuses the upkeep thread starting now.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn upkeep_thread_refused(&self) -> bool {
        let mut left = lock_unpoisoned(&self.upkeep_threads);
        match left.as_mut() {
            Some(0) => true,
            Some(left) => {
                *left -= 1;
                false
            }
            None => false,
        }
    }

    /// Observe every commit from now on.
    pub fn subscribe(&self) -> CommitSubscription {
        self.subscribe_outside(None)
    }

    /// Observe every commit except those made within `outside`. Once the
    /// database has closed, a new subscription reports that it closed, as a
    /// call to the database does, not the failure it may have closed with.
    pub fn subscribe_outside(&self, outside: Option<CommitScope>) -> CommitSubscription {
        self.with_state(|state| {
            let failure = if state.closed { None } else { state.failure.clone() };
            let subscription = CommitSubscription::new(
                state.closed,
                failure,
                outside,
                Arc::clone(&self.unrecorded),
            );
            state.subscriptions.retain(|subscription| subscription.strong_count() != 0);
            state.subscriptions.push(Arc::downgrade(&subscription.shared));
            subscription
        })
    }

    /// Watch `reads` from now on; `first` reports the state now as their
    /// first change.
    pub fn changes<R: 'static, E: 'static>(
        &self,
        reads: ReadSet,
        first: bool,
        failed: fn(Error<F, R>) -> E,
    ) -> DatabaseChanges<E> {
        let commits = self.subscribe();
        commits.begin();
        commits.finish(reads.clone());
        DatabaseChanges::new(commits, reads, first, Self::failed(failed))
    }

    /// Keep every commit made from now on outside `attempt`, until the
    /// attempt names the rows it waits on (`watch_attempt`).
    pub fn attempt(&self, attempt: CommitScope) -> AttemptCommits {
        let commits = self.subscribe_outside(Some(attempt));
        commits.begin();
        AttemptCommits { commits }
    }

    /// Watch `reads` after `attempt`: a commit since it began that changed
    /// what they read is reported at once, as evidence against the wait it
    /// ended in.
    pub fn watch_attempt<R: 'static, E: 'static>(
        &self,
        attempt: AttemptCommits,
        reads: ReadSet,
        failed: fn(Error<F, R>) -> E,
    ) -> DatabaseChanges<E> {
        attempt.commits.finish(reads.clone());
        DatabaseChanges::new(attempt.commits, reads, false, Self::failed(failed))
    }

    /// How a live query's or a change watch's error type holds what stops
    /// it, through `failed`.
    pub(crate) fn failed<R: 'static, E: 'static>(failed: fn(Error<F, R>) -> E) -> Failed<E> {
        Arc::new(move |stop| {
            failed(match stop {
                Stop::Failed(failure) => Self::failure_error(&failure),
                Stop::Closed => Error::Closed,
                Stop::Bug(bug) => Error::Bug(bug),
            })
        })
    }

    /// The error every call on a handle that failed with `failure` returns.
    fn failure_error<R>(failure: &Failure) -> Error<F, R> {
        match failure {
            Failure::Host(host) => Error::Host(
                host.downcast_ref::<F>()
                    .expect("a handle's failure has its host's failure type")
                    .clone(),
            ),
            Failure::Bug(bug) => Error::HandleFailed(bug.clone()),
        }
    }

    /// Judge a write's `changes`, made within `scope`, for every live query
    /// waiting on known reads that isn't already marked to run again, with
    /// `lookups` reading the write's transaction before it commits (§4).
    pub fn judge(
        &self,
        changes: Vec<RowChange>,
        scope: Option<CommitScope>,
        lookups: &mut Lookups<'_>,
    ) -> rusqlite::Result<Judged> {
        let waiting: Vec<(Arc<SubscriptionState>, u64, Arc<ReadSet>)> = self.with_state(|state| {
            state
                .subscriptions
                .iter()
                .filter_map(Weak::upgrade)
                .filter(|subscription| scope.is_none() || scope != subscription.outside)
                .filter(|subscription| !subscription.state.borrow().changed)
                .filter_map(|subscription| {
                    let tracked = lock_unpoisoned(&subscription.tracking);
                    let Tracking::Waiting(reads) = &tracked.tracking else {
                        return None;
                    };
                    let found = (Arc::clone(reads), tracked.epoch);
                    drop(tracked);
                    Some((subscription, found.1, found.0))
                })
                .collect()
        });
        let mut answers = Answers::default();
        let mut verdicts = Vec::with_capacity(waiting.len());
        for (subscription, epoch, reads) in waiting {
            let found = affected(&reads, &changes, &mut answers, lookups)?;
            verdicts.push((Arc::downgrade(&subscription), epoch, found));
        }
        Ok(Judged { changes: Arc::new(changes), verdicts })
    }

    /// Tell live queries that each of `columns`, a (table, column) pair of a
    /// view, changed with no write: each query that read one runs again.
    pub fn changed_without_write(&self, columns: &[(&str, &str)]) {
        let changes = columns
            .iter()
            .map(|(table, column)| RowChange {
                table: (*table).into(),
                column: (*column).into(),
                rows: None,
            })
            .collect();
        self.commit(Judged::unjudged(changes), None);
    }

    /// Report a commit made within `scope`, if any. A query still waiting on
    /// the reads it was judged against takes its verdict. One running keeps
    /// the changes, to judge against its run's snapshot as it finishes. One
    /// that finished another run since, or wasn't judged, judges the
    /// changes counting every join's value as matched: their transaction
    /// has ended, so no lookup can read it.
    pub fn commit(&self, judged: Judged, scope: Option<CommitScope>) {
        let Judged { changes, verdicts } = judged;
        self.with_state(|state| {
            state.subscriptions.retain(|subscription| {
                let Some(subscription) = subscription.upgrade() else {
                    return false;
                };
                if scope.is_some() && scope == subscription.outside {
                    return true;
                }
                let mut tracked = lock_unpoisoned(&subscription.tracking);
                let epoch = tracked.epoch;
                match &mut tracked.tracking {
                    Tracking::Reading { commits, .. } => commits.push(Arc::clone(&changes)),
                    Tracking::Waiting(reads) => {
                        let verdict = verdicts.iter().find_map(|(judged, at, found)| {
                            (*at == epoch
                                && std::ptr::eq(judged.as_ptr(), Arc::as_ptr(&subscription)))
                            .then_some(*found)
                        });
                        let found = verdict.unwrap_or_else(|| {
                            affected(reads, &changes, &mut Answers::default(), &mut |_| Ok(true))
                                .expect("judging without lookups can't fail")
                        });
                        if found {
                            subscription.state.send_modify(|state| state.changed = true);
                        }
                    }
                }
                true
            })
        });
    }

    /// The columns some subscription's scope tests, by table, all
    /// lowercase: a waiting one's, or a running one's previous run's. A
    /// write records those values of the rows it changes there. A change to
    /// any other table needs no values to wake the subscriptions that read
    /// it.
    pub fn scoped_columns(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.with_state(|state| {
            let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
            for subscription in state.subscriptions.iter().filter_map(Weak::upgrade) {
                let reads = Arc::clone(lock_unpoisoned(&subscription.tracking).tracking.scoped());
                for read in reads.iter().filter(|read| !matches!(read.scope, RowScope::All)) {
                    read.scope.columns(tables.entry(read.table.to_ascii_lowercase()).or_default());
                }
            }
            tables
        })
    }

    /// What every call on the handle fails with: its failure, or
    /// `Error::Closed` once it closed.
    pub fn error<R>(&self) -> Option<Error<F, R>> {
        self.with_state(|state| match &state.failure {
            Some(failure) => Some(Self::failure_error(failure)),
            None => state.closed.then_some(Error::Closed),
        })
    }

    /// Whether the handle failed: a handle its host only closed hasn't.
    pub fn has_failed(&self) -> bool {
        self.with_state(|state| state.failure.is_some())
    }

    /// Fail every later call and subscription with `failure`. The handle
    /// keeps the first failure and ignores later ones.
    pub fn fail(&self, failure: F) {
        self.with_state(|state| state.fail(Failure::Host(Arc::new(failure))));
    }

    /// Fail every later call and subscription with the host's failure made
    /// from `failure`, one of sqlive's own.
    pub fn fail_engine<R>(&self, failure: EngineFailure<F, R>)
    where
        F: From<EngineFailure<F, R>>,
    {
        self.fail(F::from(failure));
    }

    /// Fail the handle with `bug`, a panic in work that may have held its
    /// state or a fault a drop found in it. The handle fails with this bug,
    /// not the one a poisoned lock would name. The bug goes to one
    /// reporter: the handle's call this thread runs, which returns it as
    /// its own (`calling`), or the handle's bug sink, outside such a call or
    /// once the call has a bug of its own already (E15).
    pub fn fail_with_bug(&self, bug: Bug) {
        {
            let mut state = self.state.lock().unwrap_or_else(|poisoned| {
                self.state.clear_poison();
                poisoned.into_inner()
            });
            state.fail(Failure::Bug(bug.clone()));
        }
        let handle = self.id();
        // A drop as this thread ends finds its locals gone: no call runs.
        let unclaimed = CALL
            .try_with(|call| match call.borrow_mut().as_mut() {
                Some(call) if call.handle == handle && call.bug.is_none() => {
                    call.bug = Some(bug.clone());
                    None
                }
                _ => Some(bug.clone()),
            })
            .unwrap_or(Some(bug));
        if let Some(bug) = unclaimed {
            self.report(&bug);
        }
    }

    /// Run `call`, one of this handle's calls, on this thread. Beside its
    /// result comes the bug its own work failed the handle with, if any,
    /// which the call returns as its own (E15). A call that unwinds leaves
    /// that bug to the sink: its panic is the call's bug then. A call made
    /// inside another on the same handle is part of it, and leaves its bug
    /// to the outer one, which the app hears from.
    pub fn calling<R>(&self, call: impl FnOnce() -> R) -> (R, Option<Bug>) {
        let handle = self.id();
        if CALL
            .with(|running| running.borrow().as_ref().is_some_and(|outer| outer.handle == handle))
        {
            return (call(), None);
        }
        /// Restores the call this one runs inside, if any, as it ends.
        struct CallScope<'a, F: Clone + Send + Sync + 'static> {
            outer: Option<Call>,
            observer: &'a Handle<F>,
        }
        impl<F: Clone + Send + Sync + 'static> Drop for CallScope<'_, F> {
            fn drop(&mut self) {
                let ended = CALL.with(|call| call.replace(self.outer.take()));
                if let Some(bug) = ended.and_then(|ended| ended.bug) {
                    self.observer.report(&bug);
                }
            }
        }
        let outer = CALL.with(|running| running.replace(Some(Call { handle, bug: None })));
        let scope = CallScope { outer, observer: self };
        let result = call();
        let bug =
            CALL.with(|running| running.borrow_mut().as_mut().and_then(|call| call.bug.take()));
        drop(scope);
        (result, bug)
    }

    /// Hand `bug`, which no call returns, to the handle's bug sink (E15).
    pub fn report(&self, bug: &Bug) {
        caught::report(self.bugs.as_ref(), bug);
    }

    /// This handle, as `CALL` names it.
    fn id(&self) -> usize {
        Arc::as_ptr(&self.state) as usize
    }

    /// Close the handle: every subscription and later call reports it.
    pub fn close(&self) {
        self.with_state(ObserverState::close);
    }

    /// Panic while holding the handle's state, as a bug in judging a commit
    /// for live queries would.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn test_poison(&self) {
        std::thread::scope(|scope| {
            let holder = scope.spawn(|| {
                let _held = self.state.lock();
                panic!("a live query's judgment broke");
            });
            assert!(holder.join().is_err());
        });
    }

    /// Run `change` on the handle's state. A panic while it was held may
    /// have left some subscription untold of a commit, so the handle fails
    /// with that bug, which tells every subscription; the failure then
    /// stands in for the poisoning (E15).
    fn with_state<R>(&self, change: impl FnOnce(&mut ObserverState) -> R) -> R {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| {
            let mut state = poisoned.into_inner();
            state.fail(Failure::Bug(half_changed("the handle's live queries")));
            self.state.clear_poison();
            state
        });
        change(&mut state)
    }
}

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;
