//! Per-subscription dependencies. Commits during a read are retained until its
//! dependencies are known; idle subscriptions retain only a changed bit.

use crate::row_scope::{RowImage, RowScope};
use crate::{CommitScope, DbError};
use std::{
    collections::{BTreeMap, BTreeSet},
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
    fn affected(&self, change: &RowChange) -> bool {
        self.table == change.table
            && (change.column.is_empty() || self.columns.contains(&change.column))
            && match &change.rows {
                Some((old, new)) => [old, new]
                    .into_iter()
                    .flatten()
                    .any(|row| self.scope.matches(row)),
                None => true,
            }
            && (!self.inserted_only || inserted(change))
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
    Waiting(ReadSet),
}

struct SubscriptionState {
    tracking: Mutex<Tracking>,
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
                tracking: Mutex::new(Tracking::Waiting(Vec::new())),
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
        let mut tracking = self
            .shared
            .tracking
            .lock()
            .expect("observation lock poisoned");
        *tracking = Tracking::Reading(Vec::new());
        self.shared.state.send_modify(|state| state.changed = false);
    }

    pub(crate) fn finish(&self, reads: ReadSet) {
        if let Some(read) = reads.iter().find(|read| !observed(&read.table)) {
            panic!(
                "a live query read {}, whose changes no write records",
                read.table
            );
        }
        let mut tracking = self
            .shared
            .tracking
            .lock()
            .expect("observation lock poisoned");
        let Tracking::Reading(commits) = &*tracking else {
            panic!("read observation was not started")
        };
        let changed = commits.iter().any(|changes| affected(&reads, changes));
        *tracking = Tracking::Waiting(reads);
        self.shared
            .state
            .send_modify(|state| state.changed = changed);
    }
}

fn affected(reads: &ReadSet, changes: &[RowChange]) -> bool {
    reads
        .iter()
        .any(|read| changes.iter().any(|change| read.affected(change)))
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

    /// Report a commit made within `scope`, if any.
    pub(crate) fn commit(&self, changes: Vec<RowChange>, scope: Option<CommitScope>) {
        let changes = Arc::new(changes);
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
                let mut tracking = subscription
                    .tracking
                    .lock()
                    .expect("observation lock poisoned");
                match &mut *tracking {
                    Tracking::Reading(commits) => commits.push(Arc::clone(&changes)),
                    Tracking::Waiting(reads) => {
                        if affected(reads, &changes) {
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
            if let Tracking::Waiting(reads) = &*subscription
                .tracking
                .lock()
                .expect("observation lock poisoned")
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
