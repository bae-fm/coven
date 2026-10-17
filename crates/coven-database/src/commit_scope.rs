//! Commits attributed to the work that made them, so an observation can leave
//! one piece of work's own commits out: an operation attempt's own commits are
//! never evidence against the wait it ends in (§6.7, §21.2; IO
//! `WaitingEvidence`).

use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

/// Commits made by database calls that a [`CommitScope::run`] future starts
/// belong to that scope. Calls started from other tasks, including tasks the
/// future spawns, are outside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitScope(u64);

tokio::task_local! {
    static TASK: CommitScope;
}

thread_local! {
    static THREAD: Cell<Option<CommitScope>> = const { Cell::new(None) };
}

impl CommitScope {
    /// A scope no other scope equals.
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// Run `work` with its database calls' commits attributed to this scope.
    pub async fn run<F: Future>(self, work: F) -> F::Output {
        TASK.scope(self, work).await
    }
}

impl Default for CommitScope {
    fn default() -> Self {
        Self::new()
    }
}

/// Bind a database call to the scope of the task starting it, before it runs
/// on a blocking thread: commits it makes report that scope.
pub(crate) fn bind<R>(
    run: impl FnOnce() -> R + Send + 'static,
) -> impl FnOnce() -> R + Send + 'static {
    let scope = TASK.try_with(|scope| *scope).ok();
    #[cfg(any(test, feature = "test-utils"))]
    let run = coven_foundation::test_work::bind(run);
    move || enter(scope, run)
}

fn enter<R>(scope: Option<CommitScope>, run: impl FnOnce() -> R) -> R {
    struct Restore(Option<CommitScope>);
    impl Drop for Restore {
        fn drop(&mut self) {
            THREAD.with(|thread| thread.set(self.0));
        }
    }
    let _restore = Restore(THREAD.with(|thread| thread.replace(scope)));
    run()
}

/// The scope of the call committing on this thread.
pub(crate) fn committing() -> Option<CommitScope> {
    THREAD.with(Cell::get)
}
