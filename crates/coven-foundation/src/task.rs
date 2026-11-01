//! Work run apart from its caller — on the blocking pool, as its own task, or
//! as one message of a long-lived loop — whose panic comes back to that
//! caller as a value instead of an unwind (api.md E15).
//!
//! A panic turned into an `Err` at the nearest join lets the caller's own
//! failure path run: the rollback or cleanup it does on `Err`, which an unwind
//! would skip. The caller reports the panic as a bug; this module has no bug
//! reporter of its own.

use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Why work apart from its caller gave no result.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TaskFailed {
    /// The work panicked: a bug, which the caller reports.
    #[error("{operation} panicked: {message}")]
    Panicked {
        /// What the work was doing, as its caller named it.
        operation: &'static str,
        /// The text the panic carried ([`panic_message`]).
        message: String,
    },
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
    /// The failure of `operation` whose panic carried `payload`.
    pub fn panicked(operation: &'static str, payload: &(dyn Any + Send)) -> Self {
        Self::Panicked { operation, message: panic_message(payload) }
    }

    /// The failure a joined task's `error` stands for. A task the runtime
    /// dropped may have run in part: `ShutdownOutcome::Unknown`.
    pub fn of_join(operation: &'static str, error: tokio::task::JoinError) -> Self {
        Self::of_dropped(operation, ShutdownOutcome::Unknown, error)
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
            Self::Panicked { operation, .. } | Self::Shutdown(Shutdown { operation, .. }) => {
                operation
            }
        }
    }

    /// Whether the work panicked, rather than being dropped by its runtime.
    pub fn is_panic(&self) -> bool {
        matches!(self, Self::Panicked { .. })
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
        .map_err(|error| TaskFailed::of_dropped(operation, ShutdownOutcome::NotRun, error))
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
/// next.
///
/// After a panic `work` is dropped at once, inside a catch of its own, so a
/// drop that panics on what the first panic left behind does not unwind into
/// the caller either.
pub async fn caught<T>(
    operation: &'static str,
    work: impl Future<Output = T>,
) -> Result<T, TaskFailed> {
    Caught { operation, work: Some(Box::pin(work)) }.await
}

/// [`caught`] for a synchronous call: an FFI callback, a dedicated thread's
/// body, a `Drop`. Unwinding out of an `extern "C"` function aborts the
/// process, so such a function catches here and answers with its own failure
/// value.
pub fn caught_sync<T>(operation: &'static str, work: impl FnOnce() -> T) -> Result<T, TaskFailed> {
    catch_unwind(AssertUnwindSafe(work)).map_err(|payload| failed_with(operation, payload))
}

/// The failure a caught panic's `payload` stands for. The payload is dropped
/// here inside a catch of its own: a payload whose `Drop` panics would
/// otherwise unwind out of the helper that caught the first panic.
fn failed_with(operation: &'static str, payload: Box<dyn Any + Send>) -> TaskFailed {
    let failed = TaskFailed::panicked(operation, payload.as_ref());
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
    failed
}

struct Caught<F: Future> {
    operation: &'static str,
    /// `None` once `work` has finished or panicked.
    work: Option<Pin<Box<F>>>,
}

impl<F: Future> Future for Caught<F> {
    type Output = Result<F::Output, TaskFailed>;

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
                let failed = failed_with(operation, payload);
                let work = self.work.take();
                if let Err(payload) = catch_unwind(AssertUnwindSafe(move || drop(work))) {
                    tracing::error!(
                        operation,
                        "dropping work that panicked panicked again: {}",
                        panic_message(payload.as_ref())
                    );
                    std::mem::forget(payload);
                }
                Poll::Ready(Err(failed))
            }
        }
    }
}

#[cfg(test)]
#[path = "task_tests.rs"]
mod tests;
