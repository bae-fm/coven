//! Per-subscription dependencies. Commits during a read are retained until its
//! dependencies are known; idle subscriptions retain only a changed bit.

use crate::row_scope::{RowImage, RowScope};
use crate::scope_lookup::Lookups;
use crate::sql_value::SqlValue;
use crate::{CommitScope, DbError};
use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::task::{Bug, BugSinkRef};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::watch;

pub(crate) type ColumnSet = BTreeSet<(String, String)>;
pub(crate) type ReadSet = Vec<TableRead>;

/// Coven's tables no live query reads: the merge state, authorship, creation,
/// publication and claim records each write keeps beside its rows. A write records no
/// change to them, and recording one costs as much as an app row's (§4); a
/// live query that read one would be a bug coven refuses at once.
const UNOBSERVED: [&str; 21] = [
    "_coven_authored_admissions",
    "_coven_authored_changes",
    "_coven_authored_parts",
    "_coven_cells",
    "_coven_claims",
    "_coven_creations",
    "_coven_foreign_keys",
    "_coven_part_leaves",
    "_coven_part_proofs",
    "_coven_published",
    "_coven_published_creations",
    "_coven_published_dropped",
    "_coven_published_file_refs",
    "_coven_published_losses",
    "_coven_published_rows",
    "_coven_references",
    "_coven_rows",
    "_coven_write_evidence",
    "_coven_write_headers",
    "_coven_constraints",
    "_coven_written_values",
];

/// Whether a write records its changes to `table` for live queries.
pub(crate) fn observed(table: &str) -> bool {
    !UNOBSERVED.iter().any(|name| name.eq_ignore_ascii_case(table))
}

#[derive(Clone)]
pub(crate) struct TableRead {
    pub(crate) table: String,
    pub(crate) columns: BTreeSet<String>,
    /// The rows whose changes matter.
    pub(crate) scope: RowScope,
    /// Only inserted rows affect this read, not updates or deletions.
    pub(crate) inserted_only: bool,
}

pub(crate) struct RowChange {
    pub(crate) table: String,
    pub(crate) column: String,
    /// The row before and after, `None` on the side where it isn't there;
    /// `None` altogether for a change no row names, as a virtual table's or
    /// one only the update hook observed, which every scope covers.
    pub(crate) rows: Option<(Option<RowImage>, Option<RowImage>)>,
}

impl TableRead {
    /// Whether `change` could change what this read returned, running
    /// `lookups` for a join's values.
    fn affected(&self, change: &RowChange, lookups: &mut Lookups<'_>) -> rusqlite::Result<bool> {
        if self.table != change.table
            || !(change.column.is_empty() || self.columns.contains(&change.column))
            || (self.inserted_only && !inserted(change))
        {
            return Ok(false);
        }
        let Some((old, new)) = &change.rows else {
            return Ok(true);
        };
        for row in [old, new].into_iter().flatten() {
            if self.scope.matches(row, lookups)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// An insertion: a whole-row change with no row before it.
fn inserted(change: &RowChange) -> bool {
    change.column.is_empty() && change.rows.as_ref().is_some_and(|(old, _)| old.is_none())
}

/// Why this handle refuses every call until the store is reopened.
#[derive(Clone)]
pub(crate) enum HandleFailure {
    /// Saving pending state or the sync status failed, with this cause.
    State(Arc<DbError>),
    /// Rolling back a failed transaction failed, so the writer may still hold
    /// its uncommitted changes.
    Rollback,
    /// A WAL checkpoint failed with this cause, other than on a busy or
    /// locked database, and the store's checkpointing stopped.
    Checkpoint(Arc<DbError>),
    /// Checking or gathering the planner's statistics failed with this
    /// cause, other than on a busy or locked database, and keeping them
    /// stopped.
    Statistics(Arc<DbError>),
    /// The handle's clock could not wait for an invite's expiry (§13.2).
    WallTimer(coven_foundation::clock::WallTimerError),
    /// A bug, a panic or a fault a drop found, may have left this store's
    /// state wrong (E15). It reached its one reporter as it happened, so
    /// the calls after it repeat it as `DbError::StoreFailed`.
    Bug(Bug),
}

impl HandleFailure {
    fn error(&self) -> DbError {
        match self {
            Self::State(cause) => DbError::StatePersistence(cause.clone()),
            Self::Rollback => DbError::RollbackFailed,
            Self::Checkpoint(cause) => DbError::CheckpointFailed(cause.clone()),
            Self::Statistics(cause) => DbError::StatisticsFailed(cause.clone()),
            Self::WallTimer(cause) => DbError::WallTimer(cause.clone()),
            Self::Bug(bug) => DbError::StoreFailed(bug.clone()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CommitState {
    pub(crate) changed: bool,
    closed: bool,
    failure: Option<HandleFailure>,
}

impl CommitState {
    /// Whether the store has closed.
    pub(crate) fn closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn error(&self) -> Option<DbError> {
        self.failure
            .as_ref()
            .map(HandleFailure::error)
            .or_else(|| self.closed.then_some(DbError::StoreClosed))
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
}

#[derive(Clone)]
pub(crate) struct CommitSubscription {
    shared: Arc<SubscriptionState>,
    receiver: watch::Receiver<CommitState>,
}

impl CommitSubscription {
    fn new(closed: bool, failure: Option<HandleFailure>, outside: Option<CommitScope>) -> Self {
        let (state, receiver) = watch::channel(CommitState { changed: false, closed, failure });
        Self {
            shared: Arc::new(SubscriptionState {
                tracking: Mutex::new(Tracked {
                    tracking: Tracking::Waiting(Arc::new(Vec::new())),
                    epoch: 0,
                }),
                state,
                outside,
            }),
            receiver,
        }
    }

    pub(crate) fn state(&self) -> CommitState {
        self.receiver.borrow().clone()
    }

    pub(crate) async fn changed(&mut self) {
        self.receiver.changed().await.expect("subscription owns its sender");
    }

    /// Start keeping the commits a run meets, until it finishes. Meanwhile
    /// a write records the values the scopes this subscription went by
    /// until now test.
    pub(crate) fn begin(&self) {
        let mut tracked = lock_unpoisoned(&self.shared.tracking);
        let previous = Arc::clone(tracked.tracking.scoped());
        tracked.tracking = Tracking::Reading { commits: Vec::new(), previous };
        tracked.epoch += 1;
        self.shared.state.send_modify(|state| state.changed = false);
    }

    /// Wait on nothing, whatever this subscription was doing: its query
    /// ended on a bug, so what its run read is unknown (E15).
    pub(crate) fn stop(&self) {
        let mut tracked = lock_unpoisoned(&self.shared.tracking);
        tracked.tracking = Tracking::Waiting(Arc::new(Vec::new()));
        tracked.epoch += 1;
    }

    /// Wait on `reads`, which join no table: no commit needs a lookup.
    pub(crate) fn finish(&self, reads: ReadSet) {
        self.finish_judging(reads, &mut |_, _| {
            unreachable!("only a live query's run reads a join")
        })
        .expect("judging without lookups can't fail");
    }

    /// Wait on `reads`, a run's. The commits made while it ran are judged
    /// against them, with `lookups` reading the run's own snapshot: a value
    /// matches a join there exactly when the run's partners held it. A
    /// lookup that fails marks the query to run again, and its error is
    /// the run's.
    pub(crate) fn finish_judging(
        &self,
        reads: ReadSet,
        lookups: &mut Lookups<'_>,
    ) -> rusqlite::Result<()> {
        if let Some(read) = reads.iter().find(|read| !observed(&read.table)) {
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

/// Whether `changes` could change what `reads` returned. Past
/// `ROWS_PER_TABLE` row changes to one table, a join's values in that table
/// count as matched without a lookup, as a table changed whole would, so a
/// write's lookups stay bounded.
fn affected(
    reads: &ReadSet,
    changes: &[RowChange],
    answers: &mut Answers,
    lookups: &mut Lookups<'_>,
) -> rusqlite::Result<bool> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for change in changes.iter().filter(|change| change.rows.is_some()) {
        *counts.entry(&change.table).or_default() += 1;
    }
    for change in changes {
        let bounded = counts
            .get(change.table.as_str())
            .is_some_and(|count| *count > crate::sqlite::ROWS_PER_TABLE);
        for read in reads {
            let found = if bounded {
                read.affected(change, &mut |_, _| Ok(true))?
            } else {
                read.affected(change, &mut |lookup, value| answers.answer(lookup, value, lookups))?
            };
            if found {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// The lookups one judging ran, by lookup and value, so rows sharing a
/// value look it up once.
#[derive(Default)]
struct Answers(HashMap<(usize, ValueKey), bool>);

impl Answers {
    fn answer(
        &mut self,
        lookup: &crate::scope_lookup::Lookup,
        value: &SqlValue,
        lookups: &mut Lookups<'_>,
    ) -> rusqlite::Result<bool> {
        let key = (std::ptr::from_ref(lookup) as usize, ValueKey::of(value));
        if let Some(found) = self.0.get(&key) {
            return Ok(*found);
        }
        let found = lookups(lookup, value)?;
        self.0.insert(key, found);
        Ok(found)
    }
}

/// A value as a hash key, by its type and bytes.
#[derive(Hash, PartialEq, Eq)]
enum ValueKey {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl ValueKey {
    fn of(value: &SqlValue) -> Self {
        match value {
            SqlValue::Null => Self::Null,
            SqlValue::Integer(value) => Self::Integer(*value),
            SqlValue::Real(value) => Self::Real(value.to_bits()),
            SqlValue::Text(value) => Self::Text(value.clone()),
            SqlValue::Blob(value) => Self::Blob(value.clone()),
        }
    }
}

/// A write's changes, and how each live query that waited on known reads
/// judged them in the write's transaction, before it committed.
pub(crate) struct Judged {
    changes: Arc<Vec<RowChange>>,
    /// Each subscription judged, at its epoch then, and whether the changes
    /// affect it.
    verdicts: Vec<(Weak<SubscriptionState>, u64, bool)>,
}

impl Judged {
    /// Changes no live query judged, as those that name no row: each
    /// waiting query judges them as they reach it, without lookups.
    pub(crate) fn unjudged(changes: Vec<RowChange>) -> Self {
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
    failure: Option<HandleFailure>,
}

thread_local! {
    /// The store call this thread runs, if any, and the bug its own work
    /// failed that store's handle with (E15).
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
fn half_changed(what: &'static str) -> Bug {
    Bug {
        operation: what,
        message: "a panic while it was locked may have left it half-changed".into(),
    }
}

impl ObserverState {
    /// Fail every later call and subscription; the first failure is kept.
    fn fail(&mut self, failure: HandleFailure) {
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

#[derive(Clone)]
pub(crate) struct CommitObserver {
    state: Arc<Mutex<ObserverState>>,
    /// Where a bug that fails the handle outside its calls goes (E15).
    bugs: BugSinkRef,
    /// How many more of the store's upkeep threads start before the next is
    /// refused, when a test asked for that.
    #[cfg(test)]
    upkeep_threads: Arc<Mutex<Option<usize>>>,
}

impl CommitObserver {
    pub(crate) fn new(bugs: BugSinkRef) -> Self {
        Self {
            state: Arc::new(Mutex::new(ObserverState {
                subscriptions: Vec::new(),
                closed: false,
                failure: None,
            })),
            bugs,
            #[cfg(test)]
            upkeep_threads: Arc::new(Mutex::new(None)),
        }
    }

    /// Let `started` upkeep threads start, if given, and refuse the next.
    #[cfg(test)]
    pub(crate) fn refuse_upkeep_thread_after(&self, started: Option<usize>) {
        *lock_unpoisoned(&self.upkeep_threads) = started;
    }

    /// Whether the system refuses the upkeep thread starting now.
    #[cfg(test)]
    pub(crate) fn upkeep_thread_refused(&self) -> bool {
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

    pub(crate) fn subscribe(&self) -> CommitSubscription {
        self.subscribe_outside(None)
    }

    /// Observe every commit except those made within `outside`. Once the
    /// store has closed, a new subscription reports that it closed, as a
    /// call to the store does, not the failure it may have closed with.
    pub(crate) fn subscribe_outside(&self, outside: Option<CommitScope>) -> CommitSubscription {
        self.with_state(|state| {
            let failure = if state.closed { None } else { state.failure.clone() };
            let subscription = CommitSubscription::new(state.closed, failure, outside);
            state.subscriptions.retain(|subscription| subscription.strong_count() != 0);
            state.subscriptions.push(Arc::downgrade(&subscription.shared));
            subscription
        })
    }

    /// Watch `reads` from now on; `first` reports the state now as their
    /// first change.
    pub(crate) fn changes(&self, reads: ReadSet, first: bool) -> crate::DatabaseChanges {
        let commits = self.subscribe();
        commits.begin();
        commits.finish(reads.clone());
        crate::DatabaseChanges::new(commits, reads, first)
    }

    /// Keep every commit made from now on outside `attempt`, until the
    /// attempt names the rows it waits on (`watch_attempt`).
    pub(crate) fn attempt(&self, attempt: CommitScope) -> AttemptCommits {
        let commits = self.subscribe_outside(Some(attempt));
        commits.begin();
        AttemptCommits { commits }
    }

    /// Watch `rows` after `attempt`: a commit since it began that changed
    /// one is reported at once, as evidence against the wait it ended in.
    pub(crate) fn watch_attempt(
        &self,
        attempt: AttemptCommits,
        rows: &[crate::PrerequisiteRows],
    ) -> crate::DatabaseChanges {
        let reads: ReadSet = rows.iter().flat_map(crate::PrerequisiteRows::reads).collect();
        attempt.commits.finish(reads.clone());
        crate::DatabaseChanges::new(attempt.commits, reads, false)
    }

    /// Judge a write's `changes`, made within `scope`, for every live query
    /// waiting on known reads that isn't already marked to run again, with
    /// `lookups` reading the write's transaction before it commits (§4).
    pub(crate) fn judge(
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

    /// Report a commit made within `scope`, if any. A query still waiting on
    /// the reads it was judged against takes its verdict. One running keeps
    /// the changes, to judge against its run's snapshot as it finishes. One
    /// that finished another run since, or wasn't judged, judges the
    /// changes counting every join's value as matched: their transaction
    /// has ended, so no lookup can read it.
    pub(crate) fn commit(&self, judged: Judged, scope: Option<CommitScope>) {
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
                            affected(reads, &changes, &mut Answers::default(), &mut |_, _| Ok(true))
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
    pub(crate) fn scoped_columns(&self) -> BTreeMap<String, BTreeSet<String>> {
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
    /// `StoreClosed` once it closed.
    pub(crate) fn error(&self) -> Option<DbError> {
        self.with_state(|state| {
            state
                .failure
                .as_ref()
                .map(HandleFailure::error)
                .or_else(|| state.closed.then_some(DbError::StoreClosed))
        })
    }

    /// Fail every later call and subscription; the first failure is kept.
    pub(crate) fn fail(&self, failure: HandleFailure) {
        self.with_state(|state| state.fail(failure));
    }

    /// Fail the handle with `bug`, a panic in work that may have held its
    /// state or a fault a drop found in it. The handle fails with this bug,
    /// not the one a poisoned lock would name. The bug goes to one
    /// reporter: the handle's call this thread runs, which returns it as
    /// its own (`calling`), or the store's bug sink, outside such a call or
    /// once the call has a bug of its own already (E15).
    pub(crate) fn fail_with_bug(&self, bug: Bug) {
        {
            let mut state = self.state.lock().unwrap_or_else(|poisoned| {
                self.state.clear_poison();
                poisoned.into_inner()
            });
            state.fail(HandleFailure::Bug(bug.clone()));
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
    pub(crate) fn calling<R>(&self, call: impl FnOnce() -> R) -> (R, Option<Bug>) {
        let handle = self.id();
        if CALL
            .with(|running| running.borrow().as_ref().is_some_and(|outer| outer.handle == handle))
        {
            return (call(), None);
        }
        /// Restores the call this one runs inside, if any, as it ends.
        struct CallScope<'a> {
            outer: Option<Call>,
            observer: &'a CommitObserver,
        }
        impl Drop for CallScope<'_> {
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

    /// Hand `bug`, which no call returns, to the store's bug sink (E15).
    pub(crate) fn report(&self, bug: &Bug) {
        coven_foundation::task::report(self.bugs.as_ref(), bug);
    }

    /// This handle, as `CALL` names it.
    fn id(&self) -> usize {
        Arc::as_ptr(&self.state) as usize
    }

    pub(crate) fn close(&self) {
        self.with_state(ObserverState::close);
    }

    /// Panic while holding the handle's state, as a bug in judging a commit
    /// for live queries would.
    #[cfg(test)]
    pub(crate) fn test_poison(&self) {
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
            state.fail(HandleFailure::Bug(half_changed("the store's live queries")));
            self.state.clear_poison();
            state
        });
        change(&mut state)
    }
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;
