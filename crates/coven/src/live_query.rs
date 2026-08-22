use std::sync::{Arc, Mutex};

use crate::{CovenError, CovenResult, SqlReadContext};
use coven_database::{QueryDependencies, StoreDatabase, StoreRowWrites};

type Query<T> =
    dyn for<'connection> Fn(SqlReadContext<'connection>) -> CovenResult<T> + Send + Sync;
type RequestedQuery<Request, Value> = dyn for<'connection> Fn(&Request, SqlReadContext<'connection>) -> CovenResult<Value>
    + Send
    + Sync;

/// Identifies an absolute request accepted by a reconfigurable live query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LiveQueryRevision(u64);

impl LiveQueryRevision {
    /// Return this revision as its monotonically increasing integer value.
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the live query subscription is closed")]
pub struct LiveQueryClosed;

#[derive(Clone)]
struct RequestState<Request> {
    revision: LiveQueryRevision,
    request: Request,
}

enum PendingRun {
    Initial,
    Triggered {
        request_changed: bool,
        commits: Vec<Arc<coven_database::CommittedChanges>>,
        unknown_commit: bool,
        previous_dependencies_matched: bool,
    },
}

impl PendingRun {
    fn request_changed(&mut self) {
        match self {
            Self::Initial => {
                *self = Self::Triggered {
                    request_changed: true,
                    commits: Vec::new(),
                    unknown_commit: false,
                    previous_dependencies_matched: false,
                };
            }
            Self::Triggered {
                request_changed, ..
            } => *request_changed = true,
        }
    }

    fn committed(
        &mut self,
        changes: Arc<coven_database::CommittedChanges>,
        dependencies: &QueryDependencies,
    ) {
        if let Self::Triggered {
            commits,
            previous_dependencies_matched,
            ..
        } = self
        {
            *previous_dependencies_matched |= dependencies.is_affected_by(&changes);
            commits.push(changes);
        }
    }

    fn lagged(&mut self) {
        if let Self::Triggered { unknown_commit, .. } = self {
            *unknown_commit = true;
        }
    }

    fn cause(&self, dependencies: &QueryDependencies) -> ReconfigurableLiveQueryCause {
        let Self::Triggered {
            request_changed,
            commits,
            unknown_commit,
            previous_dependencies_matched,
        } = self
        else {
            return ReconfigurableLiveQueryCause::Initial;
        };
        let database_changed = *unknown_commit
            || commits
                .iter()
                .any(|changes| dependencies.is_affected_by(changes))
            || *previous_dependencies_matched;
        match (*request_changed, database_changed) {
            (true, true) => ReconfigurableLiveQueryCause::RequestAndDatabaseChanged,
            (true, false) => ReconfigurableLiveQueryCause::RequestChanged,
            (false, true) => ReconfigurableLiveQueryCause::DatabaseChanged,
            (false, false) => {
                unreachable!("a triggered live query must have a request or database cause")
            }
        }
    }
}

/// Changes the absolute request evaluated by a reconfigurable live query.
#[derive(Clone)]
pub struct LiveQueryRequests<Request> {
    state: Arc<Mutex<RequestState<Request>>>,
    sender: tokio::sync::watch::Sender<RequestState<Request>>,
}

/// Why a reconfigurable live query produced an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconfigurableLiveQueryCause {
    /// The subscription's first result.
    Initial,
    /// The absolute request changed.
    RequestChanged,
    /// A relevant database commit occurred.
    DatabaseChanged,
    /// The request changed and a relevant database commit occurred before the run.
    RequestAndDatabaseChanged,
}

impl<Request> LiveQueryRequests<Request>
where
    Request: Clone + PartialEq,
{
    /// Replace the requested value and return the revision that will deliver it.
    ///
    /// Repeating the current request returns its existing revision. The call
    /// fails after the subscription has been dropped.
    pub fn set(&self, request: Request) -> Result<LiveQueryRevision, LiveQueryClosed> {
        if self.sender.receiver_count() == 0 {
            return Err(LiveQueryClosed);
        }
        let mut state = self
            .state
            .lock()
            .expect("live query request mutex poisoned");
        if state.request == request {
            return Ok(state.revision);
        }
        state.revision = LiveQueryRevision(
            state
                .revision
                .0
                .checked_add(1)
                .expect("live query request revision overflow"),
        );
        state.request = request;
        self.sender
            .send(state.clone())
            .map_err(|_| LiveQueryClosed)?;
        Ok(state.revision)
    }
}

/// One query result and the exact request used to produce it.
pub struct ReconfigurableLiveQueryEvent<Request, Value> {
    cause: ReconfigurableLiveQueryCause,
    state: RequestState<Request>,
    result: CovenResult<Value>,
}

impl<Request, Value> ReconfigurableLiveQueryEvent<Request, Value> {
    /// Return why this event was produced.
    pub fn cause(&self) -> ReconfigurableLiveQueryCause {
        self.cause
    }

    /// Return the revision of the request used for this result.
    pub fn revision(&self) -> LiveQueryRevision {
        self.state.revision
    }

    /// Return the absolute request used for this result.
    pub fn request(&self) -> &Request {
        &self.state.request
    }

    /// Return the query result.
    pub fn into_result(self) -> CovenResult<Value> {
        self.result
    }
}

/// A tracked query whose absolute request can change without replacing the
/// subscription.
///
/// Construct one with
/// [`CovenHandle::subscribe_reconfigurable`](crate::CovenHandle::subscribe_reconfigurable).
/// Request changes and relevant commits are coalesced before each run. If the
/// request changes while a run is in progress, that result is discarded and
/// the latest request is evaluated before an event is returned.
///
/// A run caused only by committed changes whose value equals the last
/// delivered value is not delivered: the query's read dependencies are
/// table-and-key granular, so a paged query (`ORDER BY … LIMIT`) reruns for any
/// row change in its tables, and most of those reruns leave its window
/// unchanged. The initial run, a run after a request change, and every error
/// are always delivered; the first successful value after an error is too.
pub struct ReconfigurableLiveQuery<Request, Value> {
    _writer: StoreRowWrites,
    reader: StoreDatabase,
    changes: tokio::sync::broadcast::Receiver<Arc<coven_database::CommittedChanges>>,
    dependencies: QueryDependencies,
    pending: Option<PendingRun>,
    query: Arc<RequestedQuery<Request, Value>>,
    request_receiver: tokio::sync::watch::Receiver<RequestState<Request>>,
    requests: LiveQueryRequests<Request>,
    current: RequestState<Request>,
    /// The value most recently delivered by `next`, for skipping commit-caused
    /// reruns that produce it again. `None` before the first delivery and after
    /// a delivered error.
    last_delivered: Option<Value>,
}

impl<Request, Value> ReconfigurableLiveQuery<Request, Value>
where
    Request: Clone + PartialEq + Send + Sync + 'static,
    Value: Clone + PartialEq + Send + 'static,
{
    pub(crate) fn new<F>(
        writer: StoreRowWrites,
        reader: StoreDatabase,
        initial_request: Request,
        query: F,
    ) -> Self
    where
        F: for<'connection> Fn(&Request, SqlReadContext<'connection>) -> CovenResult<Value>
            + Send
            + Sync
            + 'static,
    {
        let changes = writer.subscribe_committed_changes();
        let current = RequestState {
            revision: LiveQueryRevision(0),
            request: initial_request,
        };
        let state = Arc::new(Mutex::new(current.clone()));
        let (sender, request_receiver) = tokio::sync::watch::channel(current.clone());
        Self {
            _writer: writer,
            reader,
            changes,
            dependencies: QueryDependencies::unknown(),
            pending: Some(PendingRun::Initial),
            query: Arc::new(query),
            request_receiver,
            requests: LiveQueryRequests { state, sender },
            current,
            last_delivered: None,
        }
    }

    /// Return a handle that can replace this subscription's absolute request.
    pub fn requests(&self) -> LiveQueryRequests<Request> {
        self.requests.clone()
    }

    /// Return the initial event, or wait for a request change or relevant
    /// committed database change and return the next event.
    ///
    /// Query errors are events and do not end the subscription. Cancelling the
    /// future preserves the pending request or database change. A commit-caused
    /// rerun whose value equals the last delivered value is not an event; the
    /// query goes back to waiting.
    pub async fn next(&mut self) -> ReconfigurableLiveQueryEvent<Request, Value> {
        loop {
            self.await_pending().await;
            self.drain_pending();
            if let Some(event) = self.run().await {
                return event;
            }
        }
    }

    /// Evaluate the pending run. `None` means the run produced the value
    /// already delivered and no event is due.
    async fn run(&mut self) -> Option<ReconfigurableLiveQueryEvent<Request, Value>> {
        loop {
            let state = self.current.clone();
            let query = self.query.clone();
            let request = state.request.clone();
            let outcome = self
                .reader
                .read_tracked(move |sql| query(&request, sql))
                .await
                .map_err(CovenError::from);

            if self.request_receiver.has_changed().unwrap_or(false) {
                self.pending
                    .as_mut()
                    .expect("live query run is pending")
                    .request_changed();
                self.accept_latest_request();
                self.drain_pending();
                continue;
            }

            let result = match outcome {
                Ok((result, dependencies)) => {
                    self.dependencies = dependencies;
                    result
                }
                Err(error) => {
                    self.dependencies = QueryDependencies::unknown();
                    Err(error)
                }
            };
            let pending = self.pending.take().expect("live query run is pending");
            let cause = pending.cause(&self.dependencies);
            if cause == ReconfigurableLiveQueryCause::DatabaseChanged
                && matches!((&result, &self.last_delivered), (Ok(value), Some(last)) if value == last)
            {
                return None;
            }
            self.last_delivered = result.as_ref().ok().cloned();
            return Some(ReconfigurableLiveQueryEvent {
                cause,
                state,
                result,
            });
        }
    }

    async fn await_pending(&mut self) {
        while self.pending.is_none() {
            tokio::select! {
                changed = self.request_receiver.changed() => {
                    changed.expect("the live query retains its request sender");
                    self.accept_latest_request();
                    self.pending = Some(PendingRun::Triggered {
                        request_changed: true,
                        commits: Vec::new(),
                        unknown_commit: false,
                        previous_dependencies_matched: false,
                    });
                }
                changes = self.changes.recv() => {
                    match changes {
                        Ok(changes) if self.dependencies.is_affected_by(&changes) => {
                            self.pending = Some(PendingRun::Triggered {
                                request_changed: false,
                                commits: vec![changes],
                                unknown_commit: false,
                                previous_dependencies_matched: true,
                            });
                        }
                        Ok(_) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            self.pending = Some(PendingRun::Triggered {
                                request_changed: false,
                                commits: Vec::new(),
                                unknown_commit: true,
                                previous_dependencies_matched: false,
                            });
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            panic!("the live query retains its committed-change sender")
                        }
                    }
                }
            }
        }
    }

    fn drain_pending(&mut self) {
        if self.request_receiver.has_changed().unwrap_or(false) {
            self.accept_latest_request();
            self.pending
                .as_mut()
                .expect("live query run is pending")
                .request_changed();
        }
        loop {
            match self.changes.try_recv() {
                Ok(changes) => self
                    .pending
                    .as_mut()
                    .expect("live query run is pending")
                    .committed(changes, &self.dependencies),
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => self
                    .pending
                    .as_mut()
                    .expect("live query run is pending")
                    .lagged(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    panic!("the live query retains its committed-change sender")
                }
            }
        }
    }

    fn accept_latest_request(&mut self) {
        self.current = self.request_receiver.borrow_and_update().clone();
    }
}

/// A query over the store's reader that runs initially and whenever a committed
/// row change can affect its result.
///
/// Construct one with [`CovenHandle::subscribe`](crate::CovenHandle::subscribe),
/// then call [`next`](Self::next) for the initial value and each later value.
/// Query errors are values in the sequence and do not end the subscription.
pub struct LiveQuery<T> {
    inner: ReconfigurableLiveQuery<(), T>,
}

impl<T> LiveQuery<T>
where
    T: Clone + PartialEq + Send + 'static,
{
    pub(crate) fn new<F>(writer: StoreRowWrites, reader: StoreDatabase, query: F) -> Self
    where
        F: for<'connection> Fn(SqlReadContext<'connection>) -> CovenResult<T>
            + Send
            + Sync
            + 'static,
    {
        let query: Arc<Query<T>> = Arc::new(query);
        Self {
            inner: ReconfigurableLiveQuery::new(writer, reader, (), move |(), sql| query(sql)),
        }
    }

    /// Return the query's initial value, or wait for a committed change that can
    /// affect it and return the value after that commit. A commit whose rerun
    /// produces the value already returned is not a value; the wait continues.
    pub async fn next(&mut self) -> CovenResult<T> {
        self.inner.next().await.into_result()
    }
}
