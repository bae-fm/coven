//! Per-subscription dependencies. Commits during a read are retained until its
//! dependencies are known; idle subscriptions retain only a changed bit.

use crate::row_scope::{RowImage, RowScope};
use crate::scope_lookup::Lookups;
use crate::sql_value::SqlValue;
use crate::{CommitScope, DbError};
use std::{
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
const UNOBSERVED: [&str; 19] = [
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
    !UNOBSERVED
        .iter()
        .any(|name| name.eq_ignore_ascii_case(table))
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
    /// Saving pending state failed, with this cause.
    Pending(Arc<DbError>),
    /// Rolling back a failed transaction failed, so the writer may still hold
    /// its uncommitted changes.
    Rollback,
}

impl HandleFailure {
    fn error(&self) -> DbError {
        match self {
            Self::Pending(cause) => DbError::PendingPersistence(cause.clone()),
            Self::Rollback => DbError::RollbackFailed,
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
    Reading(Vec<Arc<Vec<RowChange>>>),
    Waiting(Arc<ReadSet>),
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
        let (state, receiver) = watch::channel(CommitState {
            changed: false,
            closed,
            failure,
        });
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

    pub(crate) fn closed() -> Self {
        Self::new(true, None, None)
    }

    pub(crate) fn state(&self) -> CommitState {
        self.receiver.borrow().clone()
    }

    pub(crate) async fn changed(&mut self) {
        self.receiver
            .changed()
            .await
            .expect("subscription owns its sender");
    }

    pub(crate) fn begin(&self) {
        let mut tracked = self
            .shared
            .tracking
            .lock()
            .expect("observation lock poisoned");
        tracked.tracking = Tracking::Reading(Vec::new());
        tracked.epoch += 1;
        self.shared.state.send_modify(|state| state.changed = false);
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
            panic!(
                "a live query read {}, whose changes no write records",
                read.table
            );
        }
        let reads = Arc::new(reads);
        let mut changed = false;
        let mut failure = None;
        let mut answers = Answers::default();
        loop {
            // Judge without the lock, so writes go on committing; those that
            // commit meanwhile are judged on the next pass.
            let commits = {
                let mut tracked = self
                    .shared
                    .tracking
                    .lock()
                    .expect("observation lock poisoned");
                let Tracking::Reading(commits) = &mut tracked.tracking else {
                    panic!("read observation was not started")
                };
                if commits.is_empty() {
                    tracked.tracking = Tracking::Waiting(Arc::clone(&reads));
                    tracked.epoch += 1;
                    self.shared
                        .state
                        .send_modify(|state| state.changed = changed);
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
                read.affected(change, &mut |lookup, value| {
                    answers.answer(lookup, value, lookups)
                })?
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
        Self {
            changes: Arc::new(changes),
            verdicts: Vec::new(),
        }
    }
}

struct ObserverState {
    subscriptions: Vec<Weak<SubscriptionState>>,
    closed: bool,
    failure: Option<HandleFailure>,
}

impl ObserverState {
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
}

impl CommitObserver {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ObserverState {
                subscriptions: Vec::new(),
                closed: false,
                failure: None,
            })),
        }
    }

    pub(crate) fn subscribe(&self) -> CommitSubscription {
        self.subscribe_outside(None)
    }

    /// Observe every commit except those made within `outside`.
    pub(crate) fn subscribe_outside(&self, outside: Option<CommitScope>) -> CommitSubscription {
        let mut state = self.state.lock().expect("observer lock poisoned");
        let subscription = CommitSubscription::new(state.closed, state.failure.clone(), outside);
        state
            .subscriptions
            .retain(|subscription| subscription.strong_count() != 0);
        state
            .subscriptions
            .push(Arc::downgrade(&subscription.shared));
        subscription
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
        let waiting: Vec<(Arc<SubscriptionState>, u64, Arc<ReadSet>)> = self
            .state
            .lock()
            .expect("observer lock poisoned")
            .subscriptions
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|subscription| scope.is_none() || scope != subscription.outside)
            .filter(|subscription| !subscription.state.borrow().changed)
            .filter_map(|subscription| {
                let tracked = subscription
                    .tracking
                    .lock()
                    .expect("observation lock poisoned");
                let Tracking::Waiting(reads) = &tracked.tracking else {
                    return None;
                };
                let found = (Arc::clone(reads), tracked.epoch);
                drop(tracked);
                Some((subscription, found.1, found.0))
            })
            .collect();
        let mut answers = Answers::default();
        let mut verdicts = Vec::with_capacity(waiting.len());
        for (subscription, epoch, reads) in waiting {
            let found = affected(&reads, &changes, &mut answers, lookups)?;
            verdicts.push((Arc::downgrade(&subscription), epoch, found));
        }
        Ok(Judged {
            changes: Arc::new(changes),
            verdicts,
        })
    }

    /// Report a commit made within `scope`, if any. A query still waiting on
    /// the reads it was judged against takes its verdict. One running keeps
    /// the changes, to judge against its run's snapshot as it finishes. One
    /// that finished another run since, or wasn't judged, judges the
    /// changes counting every join's value as matched: their transaction
    /// has ended, so no lookup can read it.
    pub(crate) fn commit(&self, judged: Judged, scope: Option<CommitScope>) {
        let Judged { changes, verdicts } = judged;
        self.state
            .lock()
            .expect("observer lock poisoned")
            .subscriptions
            .retain(|subscription| {
                let Some(subscription) = subscription.upgrade() else {
                    return false;
                };
                if scope.is_some() && scope == subscription.outside {
                    return true;
                }
                let mut tracked = subscription
                    .tracking
                    .lock()
                    .expect("observation lock poisoned");
                let epoch = tracked.epoch;
                match &mut tracked.tracking {
                    Tracking::Reading(commits) => commits.push(Arc::clone(&changes)),
                    Tracking::Waiting(reads) => {
                        let verdict = verdicts.iter().find_map(|(judged, at, found)| {
                            (*at == epoch
                                && std::ptr::eq(judged.as_ptr(), Arc::as_ptr(&subscription)))
                            .then_some(*found)
                        });
                        let found = verdict.unwrap_or_else(|| {
                            affected(reads, &changes, &mut Answers::default(), &mut |_, _| {
                                Ok(true)
                            })
                            .expect("judging without lookups can't fail")
                        });
                        if found {
                            subscription.state.send_modify(|state| state.changed = true);
                        }
                    }
                }
                true
            });
    }

    /// The columns some waiting subscription's scope tests, by table, all
    /// lowercase: a write records those values of the rows it changes there.
    /// A change to any other table needs no values to wake the
    /// subscriptions that read it.
    pub(crate) fn scoped_columns(&self) -> BTreeMap<String, BTreeSet<String>> {
        let state = self.state.lock().expect("observer lock poisoned");
        let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for subscription in state.subscriptions.iter().filter_map(Weak::upgrade) {
            if let Tracking::Waiting(reads) = &subscription
                .tracking
                .lock()
                .expect("observation lock poisoned")
                .tracking
            {
                for read in reads
                    .iter()
                    .filter(|read| !matches!(read.scope, RowScope::All))
                {
                    read.scope
                        .columns(tables.entry(read.table.to_ascii_lowercase()).or_default());
                }
            }
        }
        tables
    }

    pub(crate) fn error(&self) -> Option<DbError> {
        self.state
            .lock()
            .expect("observer lock poisoned")
            .failure
            .as_ref()
            .map(HandleFailure::error)
    }

    /// Fail every later call and subscription; the first failure is kept.
    pub(crate) fn fail(&self, failure: HandleFailure) {
        let mut state = self.state.lock().expect("observer lock poisoned");
        let error = state.failure.get_or_insert(failure).clone();
        state.subscriptions.retain(|subscription| {
            let Some(subscription) = subscription.upgrade() else {
                return false;
            };
            subscription
                .state
                .send_modify(|state| state.failure = Some(error.clone()));
            true
        });
    }

    pub(crate) fn close(&self) {
        self.state.lock().expect("observer lock poisoned").close();
    }
}
