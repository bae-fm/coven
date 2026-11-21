//! Work run apart from its caller — on the blocking pool, as its own task, or
//! as one message of a long-lived loop — whose panic comes back to that
//! caller as a value instead of an unwind (api.md E15).
//!
//! A panic turned into an `Err` at the nearest join lets the caller's own
//! failure path run: the rollback or cleanup it does on `Err`, which an unwind
//! would skip. The caller reports the panic as a bug. Work nobody waits for,
//! a long-lived worker or work its caller stopped waiting for, reports its
//! panic to a [`BugSink`] instead, the app's own reporter.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(any(test, feature = "test-utils"))]
#[path = "task_in_flight.rs"]
mod in_flight;

pub use ::panic_as_error::{
    catching_operation, caught, caught_sync, panic_message, report, Bug, BugKind, BugSink,
    BugSinkRef, CarriesBug, SourceLocation,
};

/// Name the step the blocking operation running on this thread has reached.
/// A test's hang report prints the latest step of each operation still
/// running, so a wait inside a long operation shows where. Nothing outside
/// tests.
pub fn operation_step(name: &'static str) {
    #[cfg(any(test, feature = "test-utils"))]
    in_flight::step(name);
    #[cfg(not(any(test, feature = "test-utils")))]
    let _ = name;
}

/// Why work apart from its caller gave no result.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TaskFailed {
    /// The work panicked: a bug, which the caller reports.
    #[error(transparent)]
    Panicked(Bug),
    /// The runtime it ran on is shutting down and dropped it before it
    /// finished. Not a bug: the process is ending the work.
    #[error(transparent)]
    Shutdown(Shutdown),
}

/// Work its runtime dropped as it shut down, before the work finished. Not a
/// bug: the process is ending the work. The caller gets it as a value and
/// returns it as its own error (E15).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{operation} {outcome}: its runtime is shutting down")]
pub struct Shutdown {
    /// What the work was doing, as its caller named it.
    pub operation: &'static str,
    /// How far the work got, so whether anything it would change changed.
    pub outcome: ShutdownOutcome,
}

/// How far work its runtime dropped got (E15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// The work never started, so nothing it would change changed. tokio
    /// can't stop a closure on the blocking pool once it has started, so a
    /// closure the pool dropped is always one it never ran.
    NotRun,
    /// The work may have stopped at any of its waits, after some of its
    /// steps: a task the runtime dropped partway. What it would change may
    /// be changed in part, so the caller reads that state again before
    /// relying on it.
    Unknown,
}

impl std::fmt::Display for ShutdownOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotRun => "did not run",
            Self::Unknown => "may have run in part",
        })
    }
}

impl TaskFailed {
    /// The failure a joined task's `error` stands for. A task the runtime
    /// dropped may have run in part: `ShutdownOutcome::Unknown`. A task
    /// whose work runs under [`caught()`] hands its panic back as a value, so
    /// its join fails only as its runtime ends it; a panic that reached the
    /// join uncaught gives a bug that can't say where it began.
    pub fn of_join(operation: &'static str, error: tokio::task::JoinError) -> Self {
        if error.is_panic() {
            Self::Panicked(panic_as_error::bug_of_unlocated(operation, error.into_panic()))
        } else {
            Self::Shutdown(Shutdown { operation, outcome: ShutdownOutcome::Unknown })
        }
    }

    /// This failure as the error of the call whose work it ended: its
    /// `Bug` for a panic, its `Shutdown` for the runtime dropping the work.
    pub fn into_error<E: From<Bug> + From<Shutdown>>(self) -> E {
        match self {
            Self::Panicked(bug) => E::from(bug),
            Self::Shutdown(shutdown) => E::from(shutdown),
        }
    }

    /// What the failed work was doing, as its caller named it.
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Panicked(Bug { operation, .. }) | Self::Shutdown(Shutdown { operation, .. }) => {
                operation
            }
        }
    }

    /// Whether the work panicked, rather than being dropped by its runtime.
    pub fn is_panic(&self) -> bool {
        matches!(self, Self::Panicked(_))
    }
}

/// Run `work` on the blocking pool. A panic in it, or the runtime dropping it
/// unstarted (`ShutdownOutcome::NotRun`), comes back as `TaskFailed`, never
/// as an unwind.
pub async fn blocking<T: Send + 'static>(
    operation: &'static str,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, TaskFailed> {
    match tokio::task::spawn_blocking(move || caught_sync(operation, work)).await {
        Ok(done) => done.map_err(TaskFailed::Panicked),
        // The closure catches its own panic, and the pool can't stop a
        // closure once it has started, so the pool dropped it unstarted.
        Err(_dropped) => {
            Err(TaskFailed::Shutdown(Shutdown { operation, outcome: ShutdownOutcome::NotRun }))
        }
    }
}

/// Wait for a task whose work runs under [`caught()`], as
/// `tokio::spawn(caught(operation, work))` starts it. Its panic comes back as
/// `TaskFailed::Panicked`, naming where it began; the runtime cancelling it,
/// as `Shutdown`.
pub async fn joined<T: Send + 'static>(
    operation: &'static str,
    task: tokio::task::JoinHandle<Result<T, Bug>>,
) -> Result<T, TaskFailed> {
    match task.await {
        Ok(caught) => caught.map_err(TaskFailed::Panicked),
        Err(error) => Err(TaskFailed::of_join(operation, error)),
    }
}

/// Logs each bug at error: the sink of an app that wants its bugs only
/// logged.
pub struct LoggedBugs;

impl BugSink for LoggedBugs {
    fn report(&self, bug: &Bug) {
        tracing::error!(
            operation = bug.operation,
            location = bug.location.as_ref().map(tracing::field::display),
            "a bug in work nobody waits for: {bug}"
        );
    }
}

/// Keeps every bug it hears of, and logs each at error, as an app's crash
/// reporter would: the sink a test gives coven. A test that expects a bug
/// takes it; one left untaken when the sink drops fails the test, so a bug
/// no test expected never passes unseen.
#[cfg(any(test, feature = "test-utils"))]
pub struct HeardBugs {
    heard: std::sync::Mutex<Vec<Bug>>,
    /// Told of each bug as it is heard.
    notice: tokio::sync::Notify,
    /// Runs as the next bug is heard, inside its report.
    next_report: std::sync::Mutex<Option<ReportHook>>,
}

/// What a test runs as [`HeardBugs`] hears its next bug.
#[cfg(any(test, feature = "test-utils"))]
type ReportHook = Box<dyn FnOnce(&Bug) + Send>;

#[cfg(any(test, feature = "test-utils"))]
impl HeardBugs {
    /// A sink that has heard nothing yet, to share with coven.
    pub fn shared() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            heard: std::sync::Mutex::new(Vec::new()),
            notice: tokio::sync::Notify::new(),
            next_report: std::sync::Mutex::new(None),
        })
    }

    /// Run `hook` as the next bug is heard, inside its report: for a test
    /// that acts at the moment the reporter hands a bug over.
    pub fn on_next_report(&self, hook: impl FnOnce(&Bug) + Send + 'static) {
        *crate::locks::lock_unpoisoned(&self.next_report) = Some(Box::new(hook));
    }

    /// Every bug heard since the last take, in the order heard.
    pub fn take(&self) -> Vec<Bug> {
        std::mem::take(&mut *crate::locks::lock_unpoisoned(&self.heard))
    }

    /// Wait for the first bug not yet taken, and take it: for a test whose
    /// bug reaches the sink after the call it made returned.
    pub async fn next(&self) -> Bug {
        loop {
            let notified = self.notice.notified();
            {
                let mut heard = crate::locks::lock_unpoisoned(&self.heard);
                if !heard.is_empty() {
                    return heard.remove(0);
                }
            }
            notified.await;
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl BugSink for HeardBugs {
    fn report(&self, bug: &Bug) {
        tracing::error!(
            operation = bug.operation,
            location = bug.location.as_ref().map(tracing::field::display),
            "a bug in work nobody waits for: {bug}"
        );
        let hook = crate::locks::lock_unpoisoned(&self.next_report).take();
        if let Some(hook) = hook {
            hook(bug);
        }
        crate::locks::lock_unpoisoned(&self.heard).push(bug.clone());
        self.notice.notify_waiters();
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Drop for HeardBugs {
    fn drop(&mut self) {
        let untaken = self.take();
        // A test already failing says why itself.
        if !untaken.is_empty() && !std::thread::panicking() {
            panic!("coven reported bugs no test took: {untaken:?}");
        }
    }
}

impl CarriesBug for TaskFailed {
    fn bug(&self) -> Option<&Bug> {
        match self {
            Self::Panicked(bug) => Some(bug),
            Self::Shutdown(_) => None,
        }
    }
}

/// Hand the bug `error` carries, if it carries one, to `bugs`: the error of
/// work nobody waits for, which no caller hears of. Whether it carried one,
/// so the work logs an error that carried none.
pub fn report_carried(bugs: &dyn BugSink, error: &impl CarriesBug) -> bool {
    let bug = error.bug();
    if let Some(bug) = bug {
        report(bugs, bug);
    }
    bug.is_some()
}

/// `error`, once the bug it carries, if any, has gone to `bugs`: the error
/// of work nobody waits for, which it records where readers see it.
pub fn reported<E: CarriesBug>(bugs: &dyn BugSink, error: E) -> E {
    report_carried(bugs, &error);
    error
}

/// Run `work`, named `operation`, as its own task that nobody waits for: a
/// worker its owner stops. A panic in it goes to `bugs`, then `ended` runs,
/// so the owner ends what the work served in a way its readers see, such as
/// recording that sync failed. A panic in `ended` goes to `bugs` too, and no
/// further.
pub fn spawn_owned<E, F>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: impl Future<Output = ()> + Send + 'static,
    ended: E,
) -> OwnedTask
where
    E: FnOnce(Bug) -> F + Send + 'static,
    F: Future<Output = ()> + Send,
{
    OwnedTask {
        operation,
        task: tokio::spawn(async move {
            let Err(bug) = caught(operation, work).await else {
                return;
            };
            report(bugs.as_ref(), &bug);
            if let Err(again) = caught(operation, ended(bug)).await {
                report(bugs.as_ref(), &again);
            }
        }),
    }
}

/// A task [`spawn_owned`] started. Dropping it leaves the task running.
#[derive(Debug)]
pub struct OwnedTask {
    operation: &'static str,
    task: tokio::task::JoinHandle<()>,
}

impl OwnedTask {
    /// End the work at its next wait. Its `ended` does not run.
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Whether the work has ended.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Wait until the work has ended: finished, panicked and reported, or
    /// dropped by its runtime as it shuts down, which ends it too.
    pub async fn ended(self) {
        // The task catches every panic in it, so only its runtime ending it
        // is left.
        if let Err(error) = self.task.await {
            tracing::debug!("{}", TaskFailed::of_join(self.operation, error));
        }
    }
}

/// Run `work`, named `operation`, as its own task, so it finishes even when
/// its caller stops waiting, as closing a store does. Its panic comes back
/// as `TaskFailed::Panicked` to a caller still waiting. Once none is, the
/// panic goes to `bugs`, as does a bug its error carries.
pub fn spawn_completing<T: Send + 'static, E: CarriesBug + Send + 'static>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: impl Future<Output = Result<T, E>> + Send + 'static,
) -> CompletingTask<Result<T, E>> {
    spawn_completing_boxed(operation, bugs, Box::pin(work))
}

/// [`spawn_completing`] with its work as one boxed type, so tokio's spawn and
/// the task's harness are built once per result type, not once per call site.
fn spawn_completing_boxed<T: Send + 'static, E: CarriesBug + Send + 'static>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
) -> CompletingTask<Result<T, E>> {
    let (done, completion) = tokio::sync::oneshot::channel();
    let heard = bugs.clone();
    #[cfg(any(test, feature = "test-utils"))]
    let flight = in_flight::Flight::begin(operation, in_flight::State::Task);
    tokio::spawn(async move {
        let answer = caught(operation, work).await;
        #[cfg(any(test, feature = "test-utils"))]
        drop(flight);
        if let Err(unheard) = done.send(answer) {
            report_unheard(heard.as_ref(), &unheard);
        }
    });
    // The runtime drops a task partway as it shuts down.
    CompletingTask {
        operation,
        dropped: ShutdownOutcome::Unknown,
        bugs,
        unread: report_unheard::<T, E>,
        completion,
    }
}

/// Run `work`, named `operation`, on the blocking pool, where it runs to its
/// end even when its caller stops waiting. Its panic comes back as
/// `TaskFailed::Panicked` to a caller still waiting, and the pool dropping it
/// unstarted as `Shutdown` with `ShutdownOutcome::NotRun`. Once no caller
/// waits, the panic goes to `bugs`, as does a bug its error carries.
pub fn blocking_completing<T: Send + 'static, E: CarriesBug + Send + 'static>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> CompletingTask<Result<T, E>> {
    blocking_completing_boxed(operation, bugs, Box::new(work))
}

/// [`blocking_completing`] with its work as one boxed type, so tokio's
/// `spawn_blocking`, the channel and the harness are built once per result
/// type, not once per call site.
fn blocking_completing_boxed<T: Send + 'static, E: CarriesBug + Send + 'static>(
    operation: &'static str,
    bugs: BugSinkRef,
    work: Box<dyn FnOnce() -> Result<T, E> + Send>,
) -> CompletingTask<Result<T, E>> {
    let (done, completion) = tokio::sync::oneshot::channel();
    let heard = bugs.clone();
    // The pool runs the closure to its end once it starts, so the closure
    // answers unless the pool dropped it unstarted.
    #[cfg(any(test, feature = "test-utils"))]
    let flight = in_flight::Flight::begin(operation, in_flight::State::Queued);
    tokio::task::spawn_blocking(move || {
        #[cfg(any(test, feature = "test-utils"))]
        flight.started();
        let answer = caught_sync(operation, work);
        #[cfg(any(test, feature = "test-utils"))]
        drop(flight);
        if let Err(unheard) = done.send(answer) {
            report_unheard(heard.as_ref(), &unheard);
        }
    });
    CompletingTask {
        operation,
        dropped: ShutdownOutcome::NotRun,
        bugs,
        unread: report_unheard::<T, E>,
        completion,
    }
}

/// Hand `bugs` the panic of work nobody waited for, or the bug its error
/// carries.
fn report_unheard<T, E: CarriesBug>(bugs: &dyn BugSink, unheard: &Result<Result<T, E>, Bug>) {
    match unheard {
        Err(bug) => report(bugs, bug),
        Ok(Err(error)) => {
            report_carried(bugs, error);
        }
        Ok(Ok(_)) => {}
    }
}

/// The result of work [`spawn_completing`] or [`blocking_completing`] runs.
/// Dropping it leaves the work running, and the work's panic, or a bug its
/// error carries, goes to the sink: once the work ends, or as it drops, for
/// an answer the work gave that no one read.
pub struct CompletingTask<T> {
    operation: &'static str,
    /// How far the work got if its runtime dropped it.
    dropped: ShutdownOutcome,
    /// Where an answer no one read reports its bug.
    bugs: BugSinkRef,
    /// Hands `bugs` the bug of an answer no one read.
    unread: fn(&dyn BugSink, &Result<T, Bug>),
    completion: tokio::sync::oneshot::Receiver<Result<T, Bug>>,
}

impl<T> std::fmt::Debug for CompletingTask<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletingTask")
            .field("operation", &self.operation)
            .field("dropped", &self.dropped)
            .finish_non_exhaustive()
    }
}

impl<T> Drop for CompletingTask<T> {
    fn drop(&mut self) {
        // Closed first, so the work either answered already, and the answer
        // is read here, or finds no one waiting and reports it itself.
        self.completion.close();
        if let Ok(answer) = self.completion.try_recv() {
            (self.unread)(self.bugs.as_ref(), &answer);
        }
    }
}

impl<T> Future for CompletingTask<T> {
    type Output = Result<T, TaskFailed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (operation, outcome) = (self.operation, self.dropped);
        Pin::new(&mut self.completion).poll(cx).map(|completed| match completed {
            Ok(result) => result.map_err(TaskFailed::Panicked),
            // The runtime dropped the work as it shut down.
            Err(_dropped) => Err(TaskFailed::Shutdown(Shutdown { operation, outcome })),
        })
    }
}

#[cfg(test)]
#[path = "task_tests.rs"]
pub(crate) mod tests;
