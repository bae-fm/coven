//! Taking a lock whose holder may have panicked (api.md E15).
//!
//! A panic while a `Mutex` is held, or an `RwLock` is held for writing,
//! poisons it: std's way of saying the value may be half-changed. What that
//! means depends on what the lock guards, so coven takes each lock with the
//! helper that says it.
//!
//! - `lock_unpoisoned`: every change made under the lock is whole before
//!   anything in it can panic, so a poisoned lock holds a whole value and is
//!   used as it is.
//! - `lock_or_abort`: state the whole process shares, which a change can
//!   leave half-made. No store's handle covers it, so the process ends.
//! - coven-database's `lock_or_fail_handle`: one open store's state, which a
//!   change can leave half-made. That store's handle fails, and every later
//!   call on it says so.

use std::sync::{Mutex, MutexGuard};

pub use panic_as_error::locks::{
    into_unpoisoned, lock_unpoisoned, read_unpoisoned, wait_unpoisoned, write_unpoisoned,
};

/// Lock `mutex`, named `what`: state the whole process shares. A panic while
/// it was held may have left it half-changed, and nothing narrower than the
/// process can be failed for it, so the process ends.
pub fn lock_or_abort<'a, T: ?Sized>(mutex: &'a Mutex<T>, what: &'static str) -> MutexGuard<'a, T> {
    mutex.lock().unwrap_or_else(|_| abort_on(what))
}

/// End the process: a panic left `what`, state it shares, half-changed.
fn abort_on(what: &'static str) -> ! {
    tracing::error!(what, "a panic left process-wide state half-changed; ending the process");
    eprintln!("coven: a panic left {what} half-changed; ending the process");
    std::process::abort()
}

/// End the process: `what`, state the whole process shares, breaks `rule`.
/// Nothing narrower than the process can be failed for it, and a drop that
/// finds it, perhaps during an unwind, must not panic.
pub(crate) fn abort_broken(what: &'static str, rule: &'static str) -> ! {
    tracing::error!(what, rule, "process-wide state broke its rule; ending the process");
    eprintln!("coven: {what} broke its rule that {rule}; ending the process");
    std::process::abort()
}

#[cfg(test)]
#[path = "locks_tests.rs"]
mod tests;
