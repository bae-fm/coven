//! Per-subscription dependencies. Commits during a read are retained until its
//! dependencies are known; idle subscriptions retain only a changed bit.

use crate::sql_value::SqlValue as Value;
use crate::{key_scope::KeyScope, DbError};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::watch;

pub(crate) type ColumnSet = BTreeSet<(String, String)>;
pub(crate) type ReadSet = Vec<TableRead>;

#[derive(Clone)]
pub(crate) struct TableRead {
    pub(crate) table: String,
    pub(crate) columns: BTreeSet<String>,
    pub(crate) keys: KeyScope,
}

pub(crate) struct RowChange {
    pub(crate) table: String,
    pub(crate) column: String,
    // None covers virtual tables and changes observed only by the update hook.
    pub(crate) keys: Option<(BTreeMap<String, Value>, BTreeMap<String, Value>)>,
}

impl TableRead {
    fn affected(&self, change: &RowChange) -> bool {
        self.table == change.table
            && (change.column.is_empty() || self.columns.contains(&change.column))
            && match &change.keys {
                Some((old, new)) => self.keys.matches(old) || self.keys.matches(new),
                None => true,
            }
    }
}

#[derive(Clone)]
pub(crate) struct CommitState {
    pub(crate) changed: bool,
    closed: bool,
}

impl CommitState {
    pub(crate) fn error(&self) -> Option<DbError> {
        self.closed.then_some(DbError::StoreClosed)
    }
}

enum Tracking {
    Reading(Vec<Arc<Vec<RowChange>>>),
    Waiting(ReadSet),
}

struct SubscriptionState {
    tracking: Mutex<Tracking>,
    state: watch::Sender<CommitState>,
}

#[derive(Clone)]
pub(crate) struct CommitSubscription {
    shared: Arc<SubscriptionState>,
    receiver: watch::Receiver<CommitState>,
}

impl CommitSubscription {
    fn new(closed: bool) -> Self {
        let (state, receiver) = watch::channel(CommitState {
            changed: false,
            closed,
        });
        Self {
            shared: Arc::new(SubscriptionState {
                tracking: Mutex::new(Tracking::Waiting(Vec::new())),
                state,
            }),
            receiver,
        }
    }

    pub(crate) fn closed() -> Self {
        Self::new(true)
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
            })),
        }
    }

    pub(crate) fn subscribe(&self) -> CommitSubscription {
        let mut state = self.state.lock().expect("observer lock poisoned");
        let subscription = CommitSubscription::new(state.closed);
        state
            .subscriptions
            .retain(|subscription| subscription.strong_count() != 0);
        state
            .subscriptions
            .push(Arc::downgrade(&subscription.shared));
        subscription
    }

    pub(crate) fn commit(&self, changes: Vec<RowChange>) {
        let changes = Arc::new(changes);
        self.state
            .lock()
            .expect("observer lock poisoned")
            .subscriptions
            .retain(|subscription| {
                let Some(subscription) = subscription.upgrade() else {
                    return false;
                };
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

    pub(crate) fn close(&self) {
        let mut state = self.state.lock().expect("observer lock poisoned");
        state.closed = true;
        state.subscriptions.retain(|subscription| {
            let Some(subscription) = subscription.upgrade() else {
                return false;
            };
            subscription.state.send_modify(|state| state.closed = true);
            true
        });
    }
}
