//! Commits attributed to the work that made them, so an observation can leave
//! one piece of work's own commits out: an operation attempt's own commits are
//! never evidence against the wait it ends in (§6.7, §21.2; IO
//! `WaitingEvidence`).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// The work a commit belongs to: a commit made on a thread running inside
/// `enter(scope, ..)` belongs to `scope`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitScope(u64);

thread_local! {
    // Clippy asks for a `const` initializer this already has where the
    // target has no native thread locals, as Android: std builds those
    // from a function, which the lint reads as the initializer.
    #[cfg_attr(target_os = "android", allow(clippy::missing_const_for_thread_local))]
    static THREAD: Cell<Option<CommitScope>> = const { Cell::new(None) };
}

impl CommitScope {
    /// A scope no other scope equals.
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for CommitScope {
    fn default() -> Self {
        Self::new()
    }
}

/// Run `run` with commits made on this thread attributed to `scope`, if any.
pub(crate) fn enter<R>(scope: Option<CommitScope>, run: impl FnOnce() -> R) -> R {
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
