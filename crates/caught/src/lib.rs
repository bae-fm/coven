//! Catching a panic as a value that names where it began.
//!
//! A panic turned into an `Err` at the nearest catch lets the caller's own
//! failure path run: the rollback or cleanup it does on `Err`, which an unwind
//! would skip. [`caught`] catches a panic in a future, and [`caught_sync`] in
//! a synchronous call. Each returns a [`Bug`] that holds the text the panic
//! carried, the operation the work was doing, and the [`SourceLocation`] where
//! the panic began, which a panic's payload does not hold.
//!
//! Work nobody waits for hands its bug to a [`BugSink`] instead, through
//! [`report`], and an error that holds a bug says so through [`CarriesBug`].
//! [`locks`] holds the helpers that take a lock whose holder panicked.

use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

mod location;
pub mod locks;

pub use location::{catching_operation, SourceLocation};

/// Work that panicked, or that code found wrong without a panic: a bug in it,
/// not a failure its caller can act on. The caller reports it wherever it
/// reports its own bugs, and the call that ran the work returns it as its
/// error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bug {
    /// What the work was doing, as its caller named it.
    pub operation: &'static str,
    /// The text the panic carried ([`panic_message`]).
    pub message: String,
    /// Where the bug showed itself: where the panic began, or where the code
    /// that found the bug made it ([`Bug::new`]). `None` for a panic that began
    /// outside this crate's catches: one that reached a join uncaught
    /// ([`bug_of_unlocated`]), or one resumed with
    /// `std::panic::resume_unwind` from a panic elsewhere.
    pub location: Option<SourceLocation>,
    /// Whether the bug showed itself as a panic or code found it.
    pub kind: BugKind,
}

impl std::fmt::Display for Bug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            BugKind::Panic => write!(f, "{} panicked: {}", self.operation, self.message),
            BugKind::Found => write!(f, "{} found a bug: {}", self.operation, self.message),
        }
    }
}

impl std::error::Error for Bug {}

/// How a bug showed itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BugKind {
    /// Work panicked. The process's panic hook saw the panic begin, so an
    /// app whose own hook reports every panic has reported this one.
    Panic,
    /// Code found state its guarantee rules out, without a panic
    /// ([`Bug::new`]). No hook saw it: whoever the bug reaches reports it.
    Found,
}

impl Bug {
    /// Make the bug that code found in `operation` without a panic, such as
    /// state a `Drop` finds wrong. Its location is the caller's.
    #[track_caller]
    pub fn new(operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            operation,
            message: message.into(),
            location: Some(SourceLocation::of(std::panic::Location::caller())),
            kind: BugKind::Found,
        }
    }
}

/// Make the bug for a panic's `payload` when the panic began outside this
/// crate's catches, so the bug has no location.
pub fn bug_of_unlocated(operation: &'static str, payload: Box<dyn Any + Send>) -> Bug {
    bug_of(operation, payload, None)
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

/// Run `work` inline, catching a panic in any of its polls. For one message of
/// a long-lived loop: the panic fails that message and the loop serves the
/// next. The runtime can't drop work that runs inline, so a panic is its
/// only failure.
///
/// After a panic, `caught` drops `work` at once, inside a catch of its own, so
/// a drop that panics on what the first panic left behind does not unwind
/// into the caller either.
pub async fn caught<T>(operation: &'static str, work: impl Future<Output = T>) -> Result<T, Bug> {
    Caught { operation, work: Some(Box::pin(work)) }.await
}

/// [`caught`] for a synchronous call: an FFI callback, a dedicated thread's
/// body, a `Drop`. Unwinding out of an `extern "C"` function aborts the
/// process, so such a function catches here and answers with its own failure
/// value.
pub fn caught_sync<T>(operation: &'static str, work: impl FnOnce() -> T) -> Result<T, Bug> {
    location::catch_located(operation, work).map_err(|(payload, at)| bug_of(operation, payload, at))
}

/// Where a bug in work nobody waits for goes: a long-lived worker, or work
/// whose caller stopped waiting for it. The program passes its own crash
/// reporter.
pub trait BugSink: Send + Sync {
    /// Report `bug`, which no caller hears of otherwise.
    fn report(&self, bug: &Bug);
}

/// A shared [`BugSink`].
pub type BugSinkRef = std::sync::Arc<dyn BugSink>;

/// Hand `bug` to `bugs`. When the sink panics too, `report` logs that panic
/// and stops it there, because nothing above the sink catches it.
pub fn report(bugs: &dyn BugSink, bug: &Bug) {
    if let Err(again) = caught_sync("report a bug", || bugs.report(bug)) {
        tracing::error!(operation = bug.operation, "reporting {bug} panicked: {again}");
    }
}

/// An error that may carry a bug: work its call ran panicked, and the call's
/// error says so. Work nobody waits for reports the bug its error carries, as
/// it reports its own panic.
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

/// Make the bug for a caught panic's `payload`, which began at `location`.
/// This function drops the payload inside a catch of its own: a payload whose
/// `Drop` panics would otherwise unwind out of the helper that caught the
/// first panic.
fn bug_of(
    operation: &'static str,
    payload: Box<dyn Any + Send>,
    location: Option<SourceLocation>,
) -> Bug {
    let bug =
        Bug { operation, message: panic_message(payload.as_ref()), location, kind: BugKind::Panic };
    if bug.location.is_none() {
        tracing::warn!(
            operation,
            "{bug}: the panic began outside this crate's catches, so the bug has no location"
        );
    }
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
        match location::catch_located(operation, || work.as_mut().poll(cx)) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => {
                self.work = None;
                Poll::Ready(Ok(value))
            }
            Err((payload, at)) => {
                let bug = bug_of(operation, payload, at);
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
#[path = "lib_tests.rs"]
mod tests;
