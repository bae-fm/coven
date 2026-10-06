//! Request revisions and cancellation-safe, demand-driven live results.

use crate::observation::CommitSubscription;
use crate::{CovenResult, Database, SqlReadContext};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

type Run<T> = Pin<Box<dyn Future<Output = CovenResult<T>> + Send>>;
type Query<Q, T> = Arc<dyn Fn(Database, Q) -> Run<T> + Send + Sync>;

/// A request revision assigned by one live query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LiveQueryRevision(pub u64);

/// The query was dropped before its request could be replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("live query is closed")]
pub struct LiveQueryClosed;

/// Why this run was requested; the first run answers the initial request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveQueryCause {
    /// A new request, including the initial one.
    Request,
    /// A commit changed a table, column and key read by the query.
    Write,
    /// A new request and a relevant commit arrived before this run.
    RequestAndWrite,
}

/// One result with the exact request and revision that produced it.
#[derive(Debug)]
pub struct ReconfigurableLiveQueryEvent<Q, T> {
    /// The request used by this run.
    pub request: Q,
    /// The revision assigned to that request.
    pub revision: LiveQueryRevision,
    /// The reason the query ran.
    pub cause: LiveQueryCause,
    /// A value or a failure; failures do not end the query.
    pub result: CovenResult<T>,
}

struct Requests<Q> {
    open: bool,
    latest: Q,
    revision: LiveQueryRevision,
    pending: Option<(Q, LiveQueryRevision)>,
}

struct RequestState<Q> {
    requests: Mutex<Requests<Q>>,
    changed: Notify,
}

/// Shared access to replacing a live query's request.
pub struct LiveQueryRequests<Q> {
    state: Arc<RequestState<Q>>,
}

impl<Q> Clone for LiveQueryRequests<Q> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<Q: Clone + PartialEq> LiveQueryRequests<Q> {
    /// Replace the request and return the revision whose result will answer it.
    /// Equal consecutive requests share their revision. A replacement before
    /// a run supersedes the pending revision; that revision is never answered.
    pub fn set(&self, request: Q) -> Result<LiveQueryRevision, LiveQueryClosed> {
        let mut state = self.state.requests.lock().expect("request lock poisoned");
        if !state.open {
            return Err(LiveQueryClosed);
        }
        if request == state.latest {
            return Ok(state.revision);
        }
        let revision = LiveQueryRevision(
            state
                .revision
                .0
                .checked_add(1)
                .expect("request revision exhausted"),
        );
        state.latest = request.clone();
        state.revision = revision;
        state.pending = Some((request, revision));
        self.state.changed.notify_one();
        Ok(revision)
    }
}

// The subscription owns this guard; request handles do not prolong its life.
struct QueryLifetime<Q>(Arc<RequestState<Q>>);

impl<Q> Drop for QueryLifetime<Q> {
    fn drop(&mut self) {
        let mut state = self.0.requests.lock().expect("request lock poisoned");
        state.open = false;
        state.pending = None;
    }
}

struct Running<Q, T> {
    request: Q,
    revision: LiveQueryRevision,
    cause: LiveQueryCause,
    run: Run<T>,
}

/// A subscription whose request may be replaced while it runs.
pub struct ReconfigurableLiveQuery<Q, T> {
    database: Database,
    lifetime: QueryLifetime<Q>,
    query: Query<Q, T>,
    commits: CommitSubscription,
    current: Option<(Q, LiveQueryRevision)>,
    running: Option<Running<Q, T>>,
    previous: Option<T>,
}

impl<Q, T> ReconfigurableLiveQuery<Q, T>
where
    Q: Clone + PartialEq + Send + Sync + 'static,
    T: Send + 'static,
{
    pub(crate) fn new<F>(
        database: Database,
        request: Q,
        query: F,
        commits: CommitSubscription,
    ) -> Self
    where
        F: Fn(&Q, SqlReadContext<'_>) -> CovenResult<T> + Send + Sync + 'static,
    {
        let completion = commits.clone();
        let query = Arc::new(query);
        let runner: Query<Q, T> = Arc::new(move |database, request| {
            let query = Arc::clone(&query);
            let completion = completion.clone();
            Box::pin(async move {
                database
                    .observed_read(move |sql| query(&request, sql), completion)
                    .await
            })
        });
        Self {
            database,
            lifetime: QueryLifetime(Arc::new(RequestState {
                requests: Mutex::new(Requests {
                    open: true,
                    latest: request.clone(),
                    revision: LiveQueryRevision(0),
                    pending: Some((request, LiveQueryRevision(0))),
                }),
                changed: Notify::new(),
            })),
            query: runner,
            commits,
            current: None,
            running: None,
            previous: None,
        }
    }

    /// Obtain a cloneable handle for replacing this query's request.
    pub fn requests(&self) -> LiveQueryRequests<Q> {
        LiveQueryRequests {
            state: Arc::clone(&self.lifetime.0),
        }
    }

    /// Return the next result and its request. Cancelling this wait retains the
    /// in-flight run, so its request revision is not lost.
    pub async fn next(&mut self) -> ReconfigurableLiveQueryEvent<Q, T>
    where
        T: Clone + PartialEq,
    {
        loop {
            if self.running.is_none() {
                let commits = self.commits.state();
                let failure = commits.error();
                let changed = commits.changed;
                let request = self
                    .lifetime
                    .0
                    .requests
                    .lock()
                    .expect("request lock poisoned")
                    .pending
                    .take();
                let next = match (request, changed) {
                    (Some(request), true) => Some((request, LiveQueryCause::RequestAndWrite)),
                    (Some(request), false) => Some((request, LiveQueryCause::Request)),
                    (None, true) => Some((
                        self.current
                            .as_ref()
                            .expect("a previous run read columns")
                            .clone(),
                        LiveQueryCause::Write,
                    )),
                    (None, false) if failure.is_some() => Some((
                        self.current
                            .as_ref()
                            .expect("initial request was answered")
                            .clone(),
                        LiveQueryCause::Write,
                    )),
                    (None, false) => None,
                };
                if let Some(((request, revision), cause)) = next {
                    let run: Run<T> = if let Some(error) = failure {
                        Box::pin(async move { Err(error.into()) })
                    } else {
                        self.commits.begin();
                        (self.query)(self.database.clone(), request.clone())
                    };
                    self.running = Some(Running {
                        request,
                        revision,
                        cause,
                        run,
                    });
                } else {
                    tokio::select! {
                        _ = self.lifetime.0.changed.notified() => {},
                        _ = self.commits.changed() => {},
                    }
                    continue;
                }
            }
            let running = self.running.as_mut().expect("run selected");
            let result = running.run.as_mut().await;
            let running = self.running.take().expect("finished run");
            self.current = Some((running.request.clone(), running.revision));
            let equal = match &result {
                Ok(value) => self.previous.as_ref() == Some(value),
                Err(_) => false,
            };
            // CovenError preserves arbitrary typed sources, which have no value
            // equality or Clone. A failed run is delivered and breaks the chain
            // of equal successful results; it never terminates the subscription.
            self.previous = match &result {
                Ok(value) => Some(value.clone()),
                Err(_) => None,
            };
            if running.cause == LiveQueryCause::Write && equal {
                continue;
            }
            return ReconfigurableLiveQueryEvent {
                request: running.request,
                revision: running.revision,
                cause: running.cause,
                result,
            };
        }
    }
}

/// A subscription's results and lifetime; dropping it ends future runs.
pub struct LiveQuery<T> {
    inner: ReconfigurableLiveQuery<(), T>,
}

impl<T: Send + 'static> LiveQuery<T> {
    pub(crate) fn new(inner: ReconfigurableLiveQuery<(), T>) -> Self {
        Self { inner }
    }

    /// The first result at once; then only changed results. Errors remain results.
    pub async fn next(&mut self) -> CovenResult<T>
    where
        T: Clone + PartialEq,
    {
        self.inner.next().await.result
    }

    /// Process each value on a worker after its connection has been released.
    /// Equality is tested on the processed value.
    pub fn process<P, U>(self, process: P) -> LiveQuery<U>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> CovenResult<U> + Send + Sync + 'static,
        U: Send + 'static,
    {
        let process = Arc::new(process);
        let ReconfigurableLiveQuery {
            database,
            lifetime,
            query,
            commits,
            current,
            running,
            previous: _,
        } = self.inner;
        let next_process = Arc::clone(&process);
        let mapped: Query<(), U> = Arc::new(move |database, request| {
            process_run(query(database, request), Arc::clone(&next_process))
        });
        let running = running.map(
            |Running {
                 request,
                 revision,
                 cause,
                 run,
             }| Running {
                request,
                revision,
                cause,
                run: process_run(run, Arc::clone(&process)),
            },
        );
        // A new output type has no previous result to compare. Rerun the current
        // request if it has already completed so the transformed query answers it.
        if running.is_none() {
            if let Some(request) = current {
                let mut requests = lifetime.0.requests.lock().expect("request lock poisoned");
                requests.pending = Some(request);
            }
        }
        LiveQuery {
            inner: ReconfigurableLiveQuery {
                database,
                lifetime,
                query: mapped,
                commits,
                current,
                running,
                previous: None,
            },
        }
    }
}

fn process_run<T: Send + 'static, U: Send + 'static, P>(run: Run<T>, process: Arc<P>) -> Run<U>
where
    P: Fn(T) -> CovenResult<U> + Send + Sync + 'static,
{
    Box::pin(async move {
        let result = run.await;
        match result {
            Ok(value) => crate::database::process(move || process(value)).await,
            Err(error) => Err(error),
        }
    })
}

#[cfg(test)]
#[path = "live_query_tests.rs"]
mod tests;
