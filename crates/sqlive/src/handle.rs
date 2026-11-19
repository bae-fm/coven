//! A handle: the failure state and the subscriptions that several databases
//! share. A subscription keeps the commits made during a run of its read
//! until the run ends and its reads are known; an idle subscription keeps
//! only whether something changed.

use crate::commit_scope::CommitScope;
use crate::database_changes::DatabaseChanges;
use crate::kept_counts::KeptCounts;
use crate::read_index::{ReadIndex, SubscriptionId};
use crate::read_set::{affected_by, past_row_limit};
use crate::{affected, Answers, EngineFailure, Error, Judging, Lookups, ReadSet, RowChange};
use panic_as_error::locks::lock_unpoisoned;
use panic_as_error::{Bug, BugSinkRef};
use std::{
    any::Any,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    marker::PhantomData,
    sync::{Arc, Mutex},
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
/// holds it too, and its type names no host failure type, so `Failure` holds
/// the host's failure as `Any`: the handle, whose type names the failure
/// type, takes it back out ([`Handle::error`]).
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
    /// The reads whose values a write keeps: those waited on, or, while a
    /// run reads, its previous run's, which a run with the same statements
    /// reads and tests again.
    fn kept(&self) -> &Arc<ReadSet> {
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

impl Tracked {
    /// Track commits as `tracking` from now on, at the next epoch.
    fn set(&mut self, tracking: Tracking) {
        self.tracking = tracking;
        self.epoch += 1;
    }
}

/// What a subscription shares with its handle: its tracking, behind its own
/// lock, and what it has seen of the handle.
struct Entry {
    tracked: Mutex<Tracked>,
    state: watch::Sender<CommitState>,
    /// Commits made within this scope are not observed.
    outside: Option<CommitScope>,
}

impl Entry {
    /// Whether commits made within `scope` don't reach this subscription.
    fn outside_of(&self, scope: Option<CommitScope>) -> bool {
        scope.is_some() && scope == self.outside
    }

    /// Whether a commit has marked this subscription to run again.
    fn marked(&self) -> bool {
        self.state.borrow().changed
    }

    /// Tell this subscription of a commit of `changes`: a reading one keeps
    /// them; a waiting one not marked already is marked to run again when
    /// they affect its reads. A waiting one still at the epoch the handle filed it at,
    /// `filed`, takes the write's verdict at that epoch from `verdicts`, or
    /// judges the changes at the positions the index found it for; any
    /// other judges every change. Either judges counting every join's value
    /// as matched: the write's transaction has ended, so no lookup can read
    /// it.
    fn tell(
        &self,
        id: SubscriptionId,
        changes: &Arc<Vec<RowChange>>,
        crowded: &HashSet<&str>,
        filed: Option<(u64, &[usize])>,
        verdicts: &HashMap<SubscriptionId, (u64, bool)>,
    ) {
        let mut tracked = lock_unpoisoned(&self.tracked);
        let epoch = tracked.epoch;
        let reads = match &mut tracked.tracking {
            Tracking::Reading { commits, .. } => {
                commits.push(Arc::clone(changes));
                return;
            }
            Tracking::Waiting(_) if self.marked() => return,
            Tracking::Waiting(reads) => reads,
        };
        let mut matched = |_: Judging<'_>| Ok(true);
        let found = match filed {
            Some((at, positions)) if at == epoch => match verdicts.get(&id) {
                Some(&(judged, found)) if judged == epoch => found,
                _ => affected_by(
                    reads,
                    positions.iter().map(|&position| &changes[position]),
                    crowded,
                    &mut Answers::default(),
                    &mut matched,
                )
                .expect("judging without lookups can't fail"),
            },
            _ => affected_by(reads, changes.iter(), crowded, &mut Answers::default(), &mut matched)
                .expect("judging without lookups can't fail"),
        };
        if found {
            self.state.send_modify(|state| state.changed = true);
        }
    }
}

/// The subscriptions whose tracking changed since their handle last filed
/// them, which it files again before it next reads its index, and those
/// whose last clone dropped, which it forgets.
#[derive(Default)]
struct Refiling {
    changed: BTreeSet<SubscriptionId>,
    dropped: BTreeSet<SubscriptionId>,
}

/// What a subscription's clones share: its number in its handle, its entry,
/// and where it tells the handle to file it again.
struct SubscriptionState {
    id: SubscriptionId,
    entry: Arc<Entry>,
    refiling: Arc<Mutex<Refiling>>,
    /// The tables whose changes no write records, which a run may not read.
    unrecorded: Arc<UnrecordedTables>,
}

impl Drop for SubscriptionState {
    fn drop(&mut self) {
        lock_unpoisoned(&self.refiling).dropped.insert(self.id);
    }
}

/// One waiter on a handle's commits: a live query's, a change watch's, or the
/// host's own.
#[derive(Clone)]
pub struct CommitSubscription {
    shared: Arc<SubscriptionState>,
    receiver: watch::Receiver<CommitState>,
}

impl CommitSubscription {
    /// A new subscription to the handle whose state is `handle`, which
    /// commits made within `outside` don't reach, and whose runs may not
    /// read the tables `unrecorded` names (`Handle::subscribe_outside`).
    fn new(
        handle: &Mutex<ObserverState>,
        outside: Option<CommitScope>,
        unrecorded: Arc<UnrecordedTables>,
    ) -> Self {
        let (shared, receiver) = with_locked(handle, |state| {
            let failure = if state.closed { None } else { state.failure.clone() };
            let (sender, receiver) =
                watch::channel(CommitState { changed: false, closed: state.closed, failure });
            let entry = Arc::new(Entry {
                tracked: Mutex::new(Tracked {
                    tracking: Tracking::Waiting(Arc::new(Vec::new())),
                    epoch: 0,
                }),
                state: sender,
                outside,
            });
            let id = state.next;
            state.next += 1;
            state.entries.insert(id, Arc::clone(&entry));
            let shared =
                SubscriptionState { id, entry, refiling: Arc::clone(&state.refiling), unrecorded };
            (shared, receiver)
        });
        Self { shared: Arc::new(shared), receiver }
    }

    /// Change this subscription's tracking with `change`, and have its
    /// handle file it again before the handle next reads its index.
    fn track<R>(&self, change: impl FnOnce(&mut Tracked) -> R) -> R {
        let mut tracked = lock_unpoisoned(&self.shared.entry.tracked);
        let result = change(&mut tracked);
        lock_unpoisoned(&self.shared.refiling).changed.insert(self.shared.id);
        result
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
        self.track(|tracked| {
            let previous = Arc::clone(tracked.tracking.kept());
            tracked.set(Tracking::Reading { commits: Vec::new(), previous });
        });
        self.shared.entry.state.send_modify(|state| state.changed = false);
    }

    /// Wait on nothing, whatever this subscription was doing: its query
    /// ended on a bug, so what its run read is unknown (E15).
    pub fn stop(&self) {
        self.track(|tracked| {
            tracked.set(Tracking::Waiting(Arc::new(Vec::new())));
        });
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
            let commits = self.track(|tracked| {
                let Tracking::Reading { commits, .. } = &mut tracked.tracking else {
                    panic!("read observation was not started")
                };
                if !commits.is_empty() {
                    return Some(std::mem::take(commits));
                }
                tracked.set(Tracking::Waiting(Arc::clone(&reads)));
                self.shared.entry.state.send_modify(|state| state.changed = changed);
                None
            });
            let Some(commits) = commits else {
                break;
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
/// and that the changes could affect judged them in the write's
/// transaction, before it committed.
pub struct Judged {
    changes: Arc<Vec<RowChange>>,
    /// Each subscription judged, by its number, with its epoch then and
    /// whether the changes affect it. The changes affect no other
    /// subscription that waited on its reads then: the index found it for
    /// none of them.
    verdicts: HashMap<SubscriptionId, (u64, bool)>,
}

impl Judged {
    /// Changes no live query judged, as those that name no row: each
    /// waiting query judges them as they reach it, without lookups.
    fn unjudged(changes: Vec<RowChange>) -> Self {
        Self { changes: Arc::new(changes), verdicts: HashMap::new() }
    }

    /// How many live queries judged the changes.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn judgments(&self) -> usize {
        self.verdicts.len()
    }
}

/// Every commit made outside one attempt since it began, kept until the
/// attempt names the rows its wait depends on.
pub struct AttemptCommits {
    commits: CommitSubscription,
}

struct ObserverState {
    /// Every subscription's entry, by its number.
    entries: HashMap<SubscriptionId, Arc<Entry>>,
    /// The number the next subscription takes.
    next: SubscriptionId,
    /// The subscriptions by how they tracked commits when the handle last
    /// filed them.
    filed: Filed,
    /// The subscriptions to file again or forget, which they tell.
    refiling: Arc<Mutex<Refiling>>,
    closed: bool,
    failure: Option<Failure>,
    /// How many entries' tracking the handle has read to file them again.
    #[cfg(test)]
    entries_read: usize,
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

/// How a handle filed a subscription: waiting on reads at an epoch, or
/// reading, with the reads of its run before.
enum Filing {
    Waiting { epoch: u64, reads: Arc<ReadSet> },
    Reading { previous: Arc<ReadSet> },
}

impl Filing {
    /// The reads whose values a write keeps (`Tracking::kept`).
    fn kept(&self) -> &ReadSet {
        match self {
            Self::Waiting { reads, .. } | Self::Reading { previous: reads } => reads,
        }
    }
}

/// A handle's subscriptions by how each tracked commits when the handle last
/// filed it: those waiting on known reads by their reads, and those whose
/// runs were reading; and the columns of each table they read that a write
/// keeps.
#[derive(Default)]
struct Filed {
    filings: HashMap<SubscriptionId, Filing>,
    waiting: ReadIndex,
    reading: BTreeSet<SubscriptionId>,
    kept: KeptCounts,
}

impl Filed {
    /// File subscription `id`, which tracks commits as `tracked` says.
    fn file(&mut self, id: SubscriptionId, tracked: &Tracked) {
        self.forget(id);
        let filing = match &tracked.tracking {
            Tracking::Waiting(reads) => {
                self.waiting.insert(id, reads);
                Filing::Waiting { epoch: tracked.epoch, reads: Arc::clone(reads) }
            }
            Tracking::Reading { previous, .. } => {
                self.reading.insert(id);
                Filing::Reading { previous: Arc::clone(previous) }
            }
        };
        self.kept.add(filing.kept());
        self.filings.insert(id, filing);
    }

    /// Take subscription `id` out.
    fn forget(&mut self, id: SubscriptionId) {
        let Some(filing) = self.filings.remove(&id) else {
            return;
        };
        self.kept.remove(filing.kept());
        match filing {
            Filing::Waiting { reads, .. } => self.waiting.remove(id, &reads),
            Filing::Reading { .. } => {
                self.reading.remove(&id);
            }
        }
    }
}

/// Run `change` on a handle's `state`, once it has filed again the
/// subscriptions whose tracking changed. A panic while it was held may have
/// left some subscription untold of a commit, so the handle fails with that
/// bug, which tells every subscription; the failure then stands in for the
/// poisoning (E15).
fn with_locked<R>(state: &Mutex<ObserverState>, change: impl FnOnce(&mut ObserverState) -> R) -> R {
    let mut held = state.lock().unwrap_or_else(|poisoned| {
        let mut held = poisoned.into_inner();
        held.fail(Failure::Bug(half_changed("the handle's live queries")));
        state.clear_poison();
        held
    });
    held.refile();
    change(&mut held)
}

impl ObserverState {
    /// Fail every later call and subscription. The handle keeps the first
    /// failure and ignores later ones.
    fn fail(&mut self, failure: Failure) {
        let error = self.failure.get_or_insert(failure).clone();
        for entry in self.entries.values() {
            entry.state.send_modify(|state| state.failure = Some(error.clone()));
        }
    }

    fn close(&mut self) {
        self.closed = true;
        for entry in self.entries.values() {
            entry.state.send_modify(|state| state.closed = true);
        }
    }

    /// File again each subscription whose tracking changed since the last
    /// filing, and forget each one whose last clone dropped.
    fn refile(&mut self) {
        let Refiling { changed, dropped } = std::mem::take(&mut *lock_unpoisoned(&self.refiling));
        for id in changed.difference(&dropped) {
            if let Some(entry) = self.entries.get(id) {
                self.filed.file(*id, &lock_unpoisoned(&entry.tracked));
                #[cfg(test)]
                {
                    self.entries_read += 1;
                }
            }
        }
        for id in dropped {
            self.entries.remove(&id);
            self.filed.forget(id);
        }
    }

    /// Each subscription filed as waiting that `changes`, made within
    /// `scope`, could affect, not marked to run again already, with the
    /// epoch and reads it was filed with and the positions of those changes.
    fn candidates(
        &self,
        changes: &[RowChange],
        scope: Option<CommitScope>,
    ) -> Vec<(SubscriptionId, u64, Arc<ReadSet>, Vec<usize>)> {
        self.filed
            .waiting
            .candidates(changes)
            .into_iter()
            .filter_map(|(id, positions)| {
                let entry = &self.entries[&id];
                if entry.outside_of(scope) || entry.marked() {
                    return None;
                }
                let Some(Filing::Waiting { epoch, reads }) = self.filed.filings.get(&id) else {
                    unreachable!("the index holds only waiting subscriptions' reads")
                };
                Some((id, *epoch, Arc::clone(reads), positions))
            })
            .collect()
    }

    /// Tell each subscription a commit of `changes` made within `scope`
    /// could reach of it (`Handle::commit`), with the write's `verdicts`:
    /// those filed as reading, and those filed as waiting that the index
    /// finds for the changes. One filed as waiting that the index finds
    /// for none of them is affected by none of them; one whose tracking
    /// changed since the filing began a run after the commit, or finished
    /// one it was filed as reading for.
    fn commit(
        &self,
        changes: &Arc<Vec<RowChange>>,
        verdicts: &HashMap<SubscriptionId, (u64, bool)>,
        scope: Option<CommitScope>,
    ) {
        let crowded = past_row_limit(changes);
        for id in &self.filed.reading {
            let entry = &self.entries[id];
            if !entry.outside_of(scope) {
                entry.tell(*id, changes, &crowded, None, verdicts);
            }
        }
        for (id, epoch, _, positions) in self.candidates(changes, scope) {
            self.entries[&id].tell(id, changes, &crowded, Some((epoch, &positions)), verdicts);
        }
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
    /// How many more of the databases' upkeep threads start before the
    /// handle refuses the next, when a test asked for that.
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
                entries: HashMap::new(),
                next: 0,
                filed: Filed::default(),
                refiling: Arc::new(Mutex::new(Refiling::default())),
                closed: false,
                failure: None,
                #[cfg(test)]
                entries_read: 0,
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
        CommitSubscription::new(&self.state, outside, Arc::clone(&self.unrecorded))
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

    /// Watch `reads` after `attempt`: the returned changes report at once a
    /// commit since the attempt began that changed what they read, as
    /// evidence against the wait the attempt ended in.
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
    /// waiting on known reads that isn't already marked to run again and
    /// that the changes could affect, with `lookups` reading the write's
    /// transaction before it commits (§4).
    pub fn judge(
        &self,
        changes: Vec<RowChange>,
        scope: Option<CommitScope>,
        lookups: &mut Lookups<'_>,
    ) -> rusqlite::Result<Judged> {
        let candidates = self.with_state(|state| state.candidates(&changes, scope));
        let mut verdicts = HashMap::with_capacity(candidates.len());
        {
            let crowded = past_row_limit(&changes);
            let mut answers = Answers::default();
            for (id, epoch, reads, positions) in candidates {
                let found = affected_by(
                    &reads,
                    positions.iter().map(|&position| &changes[position]),
                    &crowded,
                    &mut answers,
                    lookups,
                )?;
                verdicts.insert(id, (epoch, found));
            }
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
    /// has ended, so no lookup can read it. A waiting query the index finds
    /// for none of the changes is affected by none of them.
    pub fn commit(&self, judged: Judged, scope: Option<CommitScope>) {
        let Judged { changes, verdicts } = judged;
        self.with_state(|state| state.commit(&changes, &verdicts, scope));
    }

    /// The columns some subscription reads or its scope tests, by table,
    /// all lowercase: a waiting one's, or a running one's previous run's.
    /// Each table such a subscription reads is here, with no column where
    /// it reads none. A write keys the rows it changes in these tables and
    /// keeps those values of them, so it can tell each row's net change and
    /// test the row against the scopes (`net_changes.rs`). A change to any
    /// other table needs no values to wake the subscriptions that read it.
    pub fn kept_columns(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.with_state(|state| state.filed.kept.columns())
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
        panic_as_error::report(self.bugs.as_ref(), bug);
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

    /// Run `change` on the handle's state (`with_locked`).
    fn with_state<R>(&self, change: impl FnOnce(&mut ObserverState) -> R) -> R {
        with_locked(&self.state, change)
    }
}

/// One of a handle's calls running on a thread (`Handle::calling`), which
/// restores the call it runs inside, if any, as it ends.
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

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;
