//! A commit scope that follows a task onto the blocking threads its database
//! calls run on.

use sqlive::CommitScope;
use std::future::Future;

tokio::task_local! {
    static TASK: CommitScope;
}

/// Run `work` with its database calls' commits attributed to `scope`. The
/// scope's future holds `work` once and polls it; an `async fn` here would
/// hold a second, moved copy beside it. Calls started from other tasks,
/// including tasks `work` spawns, are outside the scope.
pub fn in_commit_scope<F: Future>(scope: CommitScope, work: F) -> impl Future<Output = F::Output> {
    TASK.scope(scope, work)
}

/// Bind a database call to the scope of the task starting it, before it runs
/// on a blocking thread: commits it makes report that scope.
pub(crate) fn bind<R>(
    run: impl FnOnce() -> R + Send + 'static,
) -> impl FnOnce() -> R + Send + 'static {
    let scope = TASK.try_with(|scope| *scope).ok();
    #[cfg(any(test, feature = "test-utils"))]
    let run = coven_foundation::test_work::bind(run);
    move || sqlive::enter(scope, run)
}
