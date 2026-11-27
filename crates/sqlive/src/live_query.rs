//! Request revisions and cancellation-safe, demand-driven live results.

use crate::handle::{CommitSubscription, Failed, Handle, Stop};
#[cfg(any(test, feature = "test-utils"))]
use crate::live_query_runs::LiveQueryRuns;
use futures_lite::future::block_on;
use panic_as_error::locks::lock_unpoisoned;
use panic_as_error::{caught_sync, BugSinkRef, CarriesBug};
use std::{
    future::{poll_fn, Future},
    pin::pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::sync::Notify;

/// One run of the query for a request: it reads on a connection it releases
/// before returning, and hands its reads to the subscription.
pub type Run<Q, T, E> = Arc<dyn Fn(&Q, &CommitSubscription) -> Result<T, E> + Send + Sync>;

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
    /// A commit, or the host's change to a host table, changed a column the
    /// query read, in a row its conditions allow.
    Write,
    /// A new request and a relevant commit arrived before this run.
    RequestAndWrite,
}

/// One result with the exact request and revision that produced it.
#[derive(Debug)]
pub struct ReconfigurableLiveQueryEvent<Q, T, E> {
    /// The request used by this run.
    pub request: Q,
    /// The revision assigned to that request.
    pub revision: LiveQueryRevision,
    /// The reason the query ran.
    pub cause: LiveQueryCause,
    /// A value or a failure, of the query's own error type; failures do
    /// not end the query.
    pub result: Result<T, E>,
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
        Self { state: Arc::clone(&self.state) }
    }
}

impl<Q: Clone + PartialEq> LiveQueryRequests<Q> {
    /// Replace the request and return the revision whose result will answer it.
    /// Equal consecutive requests share their revision. A replacement before
    /// a run supersedes the pending revision; that revision is never answered.
    pub fn set(&self, request: Q) -> Result<LiveQueryRevision, LiveQueryClosed> {
        let mut state = lock_unpoisoned(&self.state.requests);
        if !state.open {
            return Err(LiveQueryClosed);
        }
        if request == state.latest {
            return Ok(state.revision);
        }
        let revision =
            LiveQueryRevision(state.revision.0.checked_add(1).expect("request revision exhausted"));
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
        let mut state = lock_unpoisoned(&self.0.requests);
        state.open = false;
        state.pending = None;
    }
}

/// A subscription whose request may be replaced while it runs. Its results
/// fail with `E`, the query closure's own error type.
pub struct ReconfigurableLiveQuery<Q, T, E> {
    lifetime: QueryLifetime<Q>,
    run: Run<Q, T, E>,
    failed: Failed<E>,
    commits: CommitSubscription,
    /// Where a bug in a result nobody waits for any more goes (E15).
    bugs: BugSinkRef,
    current: Option<(Q, LiveQueryRevision)>,
    previous: Option<T>,
    /// The database closed, its handle failed or a run's error carried a bug,
    /// as a panic's does, and a result said so: the query has ended, and
    /// answers nothing more.
    ended: bool,
    #[cfg(any(test, feature = "test-utils"))]
    runs: LiveQueryRuns,
}

impl<Q, T, E> ReconfigurableLiveQuery<Q, T, E>
where
    Q: Clone + PartialEq + Send + Sync + 'static,
    T: Send + 'static,
    E: CarriesBug + Send + 'static,
{
    /// A query that answers `request` first, with `run`, on `handle`; its
    /// error type holds the engine's errors through `failed`.
    pub fn new<F: Clone + Send + Sync + 'static, R: 'static>(
        request: Q,
        run: Run<Q, T, E>,
        failed: fn(crate::Error<F, R>) -> E,
        handle: &Handle<F>,
        bugs: BugSinkRef,
    ) -> Self {
        let commits = handle.subscribe();
        let failed = Handle::failed(failed);
        Self {
            lifetime: QueryLifetime(Arc::new(RequestState {
                requests: Mutex::new(Requests {
                    open: true,
                    latest: request.clone(),
                    revision: LiveQueryRevision(0),
                    pending: Some((request, LiveQueryRevision(0))),
                }),
                changed: Notify::new(),
            })),
            run,
            failed,
            commits,
            bugs,
            current: None,
            previous: None,
            ended: false,
            #[cfg(any(test, feature = "test-utils"))]
            runs: LiveQueryRuns::default(),
        }
    }

    /// How often this query has run so far, its processed form's runs too.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn runs(&self) -> LiveQueryRuns {
        self.runs
    }

    /// Whether a commit since this idle query's last result changes what it
    /// reads, so asking for its next result runs it again. A commit marks the
    /// queries it changes as it commits, so this answers with no waiting.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn is_marked_for_rerun(&self) -> bool {
        assert!(self.current.is_some(), "probe requires a completed query");
        self.commits.state().changed
    }

    /// Where a bug in a result nobody waits for any more goes (E15).
    pub fn bugs(&self) -> &BugSinkRef {
        &self.bugs
    }

    /// Whether a request waits for a run to answer it.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn has_pending_request(&self) -> bool {
        lock_unpoisoned(&self.lifetime.0.requests).pending.is_some()
    }

    /// Obtain a cloneable handle for replacing this query's request.
    pub fn requests(&self) -> LiveQueryRequests<Q> {
        LiveQueryRequests { state: Arc::clone(&self.lifetime.0) }
    }

    /// Process each value after its run releases the connection.
    /// Equality compares processed values; request handles and event metadata remain.
    pub fn process<P, U>(self, process: P) -> ReconfigurableLiveQuery<Q, U, E>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> Result<U, E> + Send + Sync + 'static,
        U: Send + 'static,
    {
        let ReconfigurableLiveQuery {
            lifetime,
            run,
            failed,
            commits,
            bugs,
            current,
            previous: _,
            ended,
            #[cfg(any(test, feature = "test-utils"))]
            runs,
        } = self;
        let failing = Arc::clone(&failed);
        let mapped: Run<Q, U, E> = Arc::new(move |request, commits| {
            let value = run(request, commits)?;
            caught_sync("process a read", || process(value))
                .unwrap_or_else(|bug| Err(failing(Stop::Bug(bug))))
        });
        // A new output type has no previous result to compare. Rerun the current
        // request so the transformed query answers it.
        if let Some(request) = &current {
            let mut requests = lock_unpoisoned(&lifetime.0.requests);
            if requests.pending.is_none() {
                requests.pending = Some(request.clone());
            }
        }
        ReconfigurableLiveQuery {
            lifetime,
            run: mapped,
            failed,
            commits,
            bugs,
            current,
            previous: None,
            ended,
            #[cfg(any(test, feature = "test-utils"))]
            runs,
        }
    }

    /// Resolves once `next_blocking` has something to do: a commit changed
    /// what the last run read, a new request came, the handle failed or
    /// closed, the query ended, or no run has answered yet. Dropping it
    /// loses nothing, and it needs no runtime.
    pub fn changed(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut commits = self.commits.clone();
        let requests = Arc::clone(&self.lifetime.0);
        let settled = self.ended || self.current.is_none();
        async move {
            loop {
                let commit = commits.observe();
                if settled
                    || commit.changed
                    || commit.stopped()
                    || lock_unpoisoned(&requests.requests).pending.is_some()
                {
                    return;
                }
                let mut commit_wait = pin!(commits.changed());
                let mut request_wait = pin!(requests.changed.notified());
                poll_fn(|context| {
                    if commit_wait.as_mut().poll(context).is_ready()
                        || request_wait.as_mut().poll(context).is_ready()
                    {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
            }
        }
    }

    /// Runs the query once if a commit changed what its last run read, a new
    /// request came, the handle failed or closed, or no run has answered yet.
    /// Returns `None` when nothing changed, when a write-caused result equals
    /// the last one (the skip `LiveQueryRuns` counts), and once the query has
    /// ended. Never parks.
    pub fn run_changed(&mut self) -> Option<ReconfigurableLiveQueryEvent<Q, T, E>>
    where
        T: Clone + PartialEq,
    {
        if self.ended {
            return None;
        }
        let commits = self.commits.state();
        let failure = commits.stopped_by();
        let changed = commits.changed;
        let request = lock_unpoisoned(&self.lifetime.0.requests).pending.take();
        let next = match (request, changed) {
            (Some(request), true) => Some((request, LiveQueryCause::RequestAndWrite)),
            (Some(request), false) => Some((request, LiveQueryCause::Request)),
            (None, true) => Some((
                self.current.as_ref().expect("a previous run read columns").clone(),
                LiveQueryCause::Write,
            )),
            (None, false) if failure.is_some() => Some((
                self.current.as_ref().expect("initial request was answered").clone(),
                LiveQueryCause::Write,
            )),
            (None, false) => None,
        };
        let ((request, revision), cause) = next?;
        let result = if let Some(stop) = failure {
            Err((self.failed)(stop))
        } else {
            self.commits.begin();
            caught_sync("live query", || (self.run)(&request, &self.commits))
                .unwrap_or_else(|bug| Err((self.failed)(Stop::Bug(bug))))
        };
        self.current = Some((request.clone(), revision));
        let equal = match &result {
            Ok(value) => self.previous.as_ref() == Some(value),
            Err(_) => false,
        };
        // An error keeps its typed sources, which need have no value
        // equality or Clone. A failed run is delivered and breaks the chain
        // of equal successful results; it never terminates the subscription.
        self.previous = match &result {
            Ok(value) => Some(value.clone()),
            Err(_) => None,
        };
        #[cfg(any(test, feature = "test-utils"))]
        self.runs.count(cause, equal);
        if cause == LiveQueryCause::Write && equal {
            return None;
        }
        // A failure while the database is closed or the handle failed is the
        // last result: answering again would only repeat it. So is a bug,
        // after which the query watches nothing more.
        let bug = result.as_ref().err().and_then(CarriesBug::bug).is_some();
        if bug {
            self.commits.stop();
        }
        self.ended = bug || (result.is_err() && self.commits.state().stopped());
        Some(ReconfigurableLiveQueryEvent { request, revision, cause, result })
    }

    /// Whether the database closed, the handle failed or a run's error
    /// carried a bug, and a result said so: the query answers nothing more.
    pub fn has_ended(&self) -> bool {
        self.ended
    }

    /// Return the next result and its request, as `changed` waits for it:
    /// this thread parks until a commit or a request calls for a run, and
    /// then `run_changed` runs the query on it, again if a write-caused
    /// result equals the last. Once the database is closed, or the handle fails
    /// every call, one result reports why and every later call returns
    /// `None`. So does a run whose error carries a bug, as one that panicked
    /// does with `Error::Bug`: what it read is unknown, so no commit could
    /// tell whether to run it again (E15).
    pub fn next_blocking(&mut self) -> Option<ReconfigurableLiveQueryEvent<Q, T, E>>
    where
        T: Clone + PartialEq,
    {
        while !self.ended {
            block_on(self.changed());
            if let Some(event) = self.run_changed() {
                return Some(event);
            }
        }
        None
    }

    /// `next_blocking` for a test on an async executor: `changed().await`,
    /// then `run_changed` on the executor's thread.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn next_inline(&mut self) -> Option<ReconfigurableLiveQueryEvent<Q, T, E>>
    where
        T: Clone + PartialEq,
    {
        while !self.ended {
            self.changed().await;
            if let Some(event) = self.run_changed() {
                return Some(event);
            }
        }
        None
    }
}

/// A subscription's results and lifetime; dropping it ends future runs. Its
/// results fail with `E`, the query closure's own error type.
pub struct LiveQuery<T, E> {
    inner: ReconfigurableLiveQuery<(), T, E>,
}

impl<T: Send + 'static, E: CarriesBug + Send + 'static> LiveQuery<T, E> {
    /// The query `inner` runs, with no request to replace.
    pub fn new(inner: ReconfigurableLiveQuery<(), T, E>) -> Self {
        Self { inner }
    }

    /// Resolves once `next_blocking` has something to do: a commit changed
    /// what the last run read, the handle failed or closed, the query ended,
    /// or no run has answered yet. Dropping it loses nothing, and it needs no
    /// runtime.
    pub fn changed(&self) -> impl Future<Output = ()> + Send + 'static {
        self.inner.changed()
    }

    /// Runs the query once if a commit changed what its last run read or no
    /// run has answered yet. Returns `None` when nothing changed, when a
    /// write-caused result equals the last one, and once the query has ended.
    /// Never parks.
    pub fn run_changed(&mut self) -> Option<Result<T, E>>
    where
        T: Clone + PartialEq,
    {
        Some(self.inner.run_changed()?.result)
    }

    /// Whether the database closed, the handle failed or a run's error
    /// carried a bug, and a result said so: the query answers nothing more.
    pub fn has_ended(&self) -> bool {
        self.inner.has_ended()
    }

    /// Returns the first result at once, and after that only results that
    /// changed. The call parks this thread until `changed` would resolve,
    /// then runs the query on it.
    /// Errors remain results and don't end the query, except the one
    /// reporting that the database closed, the handle failed or a run's error
    /// carries a bug, as a panic's does: after it, `None`.
    pub fn next_blocking(&mut self) -> Option<Result<T, E>>
    where
        T: Clone + PartialEq,
    {
        Some(self.inner.next_blocking()?.result)
    }

    /// `next_blocking` for a test on an async executor.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn next_inline(&mut self) -> Option<Result<T, E>>
    where
        T: Clone + PartialEq,
    {
        Some(self.inner.next_inline().await?.result)
    }

    /// Where a bug in a result nobody waits for any more goes (E15).
    pub fn bugs(&self) -> &BugSinkRef {
        self.inner.bugs()
    }

    /// How often this query has run so far.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn runs(&self) -> LiveQueryRuns {
        self.inner.runs()
    }

    /// Whether a commit since this idle query's last result changes what it
    /// reads, so asking for its next result runs it again, with no waiting.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn is_marked_for_rerun(&self) -> bool {
        self.inner.is_marked_for_rerun()
    }

    /// Process each value after its run has released its connection.
    /// Equality is tested on the processed value.
    pub fn process<P, U>(self, process: P) -> LiveQuery<U, E>
    where
        T: Clone + PartialEq,
        P: Fn(T) -> Result<U, E> + Send + Sync + 'static,
        U: Send + 'static,
    {
        LiveQuery { inner: self.inner.process(process) }
    }
}
