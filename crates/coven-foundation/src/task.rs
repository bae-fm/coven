//! Work run apart from its caller — on the blocking pool, as its own task, or
//! as one message of a long-lived loop — whose panic comes back to that
//! caller as a value instead of an unwind (api.md E15).
//!
//! A panic turned into an `Err` at the nearest join lets the caller's own
//! failure path run: the rollback or cleanup it does on `Err`, which an unwind
//! would skip. The caller reports the panic as a bug. Work nobody waits for,
//! a long-lived worker or work its caller stopped waiting for, reports its
//! panic to a [`BugSink`] instead, the app's own reporter.

use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

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

/// Work that panicked: a bug in it, not a failure its caller can act on
/// (E15). The caller reports it wherever it reports its own bugs, and the
/// call that ran the work returns it as its error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{operation} panicked: {message}")]
pub struct Bug {
    /// What the work was doing, as its caller named it.
    pub operation: &'static str,
    /// The text the panic carried ([`panic_message`]).
    pub message: String,
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
    /// The failure of `operation` whose panic carried `payload`.
    pub fn panicked(operation: &'static str, payload: &(dyn Any + Send)) -> Self {
        Self::Panicked(Bug { operation, message: panic_message(payload) })
    }

    /// The failure a joined task's `error` stands for. A task the runtime
    /// dropped may have run in part: `ShutdownOutcome::Unknown`.
    pub fn of_join(operation: &'static str, error: tokio::task::JoinError) -> Self {
        Self::of_dropped(operation, ShutdownOutcome::Unknown, error)
    }

    /// The failure a join of a closure on the blocking pool stands for. The
    /// pool can't stop a closure once it has started, so one it dropped
    /// never ran: `ShutdownOutcome::NotRun`.
    pub fn of_blocking_join(operation: &'static str, error: tokio::task::JoinError) -> Self {
        Self::of_dropped(operation, ShutdownOutcome::NotRun, error)
    }

    /// This failure as the error of the call whose work it ended: its
    /// `Bug` for a panic, its `Shutdown` for the runtime dropping the work.
    pub fn into_error<E: From<Bug> + From<Shutdown>>(self) -> E {
        match self {
            Self::Panicked(bug) => E::from(bug),
            Self::Shutdown(shutdown) => E::from(shutdown),
        }
    }

    /// `error`'s failure, where a task the runtime dropped got as far as
    /// `outcome` says.
    fn of_dropped(
        operation: &'static str,
        outcome: ShutdownOutcome,
        error: tokio::task::JoinError,
    ) -> Self {
        if error.is_panic() {
            failed_with(operation, error.into_panic())
        } else {
            Self::Shutdown(Shutdown { operation, outcome })
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

/// The text a panic carried: what `panic!` and `expect` were given, or a
/// placeholder for a payload that is not text.
pub fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&'static str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a panic whose payload is not text".to_string()
    }
}

/// Run `work` on the blocking pool. A panic in it, or the runtime dropping it
/// unstarted (`ShutdownOutcome::NotRun`), comes back as `TaskFailed`, never
/// as an unwind.
pub async fn blocking<T: Send + 'static>(
    operation: &'static str,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, TaskFailed> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| TaskFailed::of_blocking_join(operation, error))
}

/// Wait for a task already spawned. Its panic, or the runtime cancelling it,
/// comes back as `TaskFailed`.
pub async fn joined<T: Send + 'static>(
    operation: &'static str,
    task: tokio::task::JoinHandle<T>,
) -> Result<T, TaskFailed> {
    task.await.map_err(|error| TaskFailed::of_join(operation, error))
}

/// Run `work` inline, catching a panic in any of its polls. For one message of
/// a long-lived loop: the panic fails that message and the loop serves the
/// next. Run inline, the work can't be dropped by its runtime, so a panic is
/// its only failure.
///
/// After a panic `work` is dropped at once, inside a catch of its own, so a
/// drop that panics on what the first panic left behind does not unwind into
/// the caller either.
pub async fn caught<T>(operation: &'static str, work: impl Future<Output = T>) -> Result<T, Bug> {
    Caught { operation, work: Some(Box::pin(work)) }.await
}

/// [`caught`] for a synchronous call: an FFI callback, a dedicated thread's
/// body, a `Drop`. Unwinding out of an `extern "C"` function aborts the
/// process, so such a function catches here and answers with its own failure
/// value.
pub fn caught_sync<T>(operation: &'static str, work: impl FnOnce() -> T) -> Result<T, Bug> {
    catch_unwind(AssertUnwindSafe(work)).map_err(|payload| bug_of(operation, payload))
}

/// Where a bug in work nobody waits for goes (E15): a store's long-lived
/// workers, such as its sync loop and upload worker, and work whose caller
/// stopped waiting for it. The app passes its own crash reporter with
/// `CovenBuilder::bug_sink`.
pub trait BugSink: Send + Sync {
    /// Report `bug`, which no caller hears of otherwise.
    fn report(&self, bug: &Bug);
}

/// A shared [`BugSink`].
pub type BugSinkRef = std::sync::Arc<dyn BugSink>;

/// Logs each bug at error: the sink of a store whose app names none.
pub struct LoggedBugs;

impl BugSink for LoggedBugs {
    fn report(&self, bug: &Bug) {
        tracing::error!(operation = bug.operation, "a bug in work nobody waits for: {bug}");
    }
}

/// An error that may carry a bug: work its call ran panicked, and the call's
/// error says so (E15). Work nobody waits for reports the bug its error
/// carries, as it reports its own panic.
pub trait CarriesBug {
    /// The bug this error carries, itself or through the failure it keeps.
    fn bug(&self) -> Option<&Bug>;
}

impl CarriesBug for Bug {
    fn bug(&self) -> Option<&Bug> {
        Some(self)
    }
}

/// An I/O error carries the bug it was made from, if any.
impl CarriesBug for std::io::Error {
    fn bug(&self) -> Option<&Bug> {
        self.get_ref()?.downcast_ref::<Bug>()
    }
}

/// An error that can't happen carries no bug.
impl CarriesBug for std::convert::Infallible {
    fn bug(&self) -> Option<&Bug> {
        match *self {}
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

/// Hand `bug` to `bugs`. A sink that panics too is logged, and its panic
/// goes no further: it runs where nothing above it catches.
pub fn report(bugs: &dyn BugSink, bug: &Bug) {
    if let Err(again) = caught_sync("report a bug", || bugs.report(bug)) {
        tracing::error!(operation = bug.operation, "reporting {bug} panicked: {again}");
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
    let (done, completion) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        if let Err(unheard) = done.send(caught(operation, work).await) {
            report_unheard(bugs.as_ref(), &unheard);
        }
    });
    // The runtime drops a task partway as it shuts down.
    CompletingTask { operation, dropped: ShutdownOutcome::Unknown, completion }
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
    let (done, completion) = tokio::sync::oneshot::channel();
    // The pool runs the closure to its end once it starts, so the closure
    // answers unless the pool dropped it unstarted.
    tokio::task::spawn_blocking(move || {
        if let Err(unheard) = done.send(caught_sync(operation, work)) {
            report_unheard(bugs.as_ref(), &unheard);
        }
    });
    CompletingTask { operation, dropped: ShutdownOutcome::NotRun, completion }
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
/// Dropping it leaves the work running.
#[derive(Debug)]
pub struct CompletingTask<T> {
    operation: &'static str,
    /// How far the work got if its runtime dropped it.
    dropped: ShutdownOutcome,
    completion: tokio::sync::oneshot::Receiver<Result<T, Bug>>,
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

/// The failure a joined panic's `payload` stands for.
fn failed_with(operation: &'static str, payload: Box<dyn Any + Send>) -> TaskFailed {
    TaskFailed::Panicked(bug_of(operation, payload))
}

/// The bug a caught panic's `payload` stands for. The payload is dropped
/// here inside a catch of its own: a payload whose `Drop` panics would
/// otherwise unwind out of the helper that caught the first panic.
fn bug_of(operation: &'static str, payload: Box<dyn Any + Send>) -> Bug {
    let bug = Bug { operation, message: panic_message(payload.as_ref()) };
    if let Err(again) = catch_unwind(AssertUnwindSafe(move || drop(payload))) {
        tracing::error!(
            operation,
            "dropping the payload of a panic panicked again: {}",
            panic_message(again.as_ref())
        );
        // This payload's own drop may panic too; leaking it is what keeps
        // the unwind from reaching the caller.
        std::mem::forget(again);
    }
    bug
}

struct Caught<F: Future> {
    operation: &'static str,
    /// `None` once `work` has finished or panicked.
    work: Option<Pin<Box<F>>>,
}

impl<F: Future> Future for Caught<F> {
    type Output = Result<F::Output, Bug>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let operation = self.operation;
        let Some(work) = self.work.as_mut() else {
            panic!("{operation} polled after it finished");
        };
        match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => {
                self.work = None;
                Poll::Ready(Ok(value))
            }
            Err(payload) => {
                let bug = bug_of(operation, payload);
                let work = self.work.take();
                if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(work))) {
                    tracing::error!(
                        operation,
                        "dropping work that panicked panicked again: {}",
                        panic_message(payload.as_ref())
                    );
                    std::mem::forget(payload);
                }
                Poll::Ready(Err(bug))
            }
        }
    }
}

#[cfg(test)]
#[path = "task_tests.rs"]
pub(crate) mod tests;
