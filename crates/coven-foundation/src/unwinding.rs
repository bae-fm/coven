//! Joins of coven's own work, run on the blocking pool or as its own task,
//! where a panic in the work unwinds into the caller as if the work had run
//! inline. The runtime dropping the work as it shuts down is not a bug, so it
//! comes back as a [`Shutdown`] value the caller returns as its error (E15).
//!
//! An app's work uses [`crate::task`] instead, which turns the panic into a
//! value too.

use crate::task::{Shutdown, ShutdownOutcome};

/// What a closure on the blocking pool returned. tokio can't stop a closure
/// once it has started, so a join the runtime cancelled is one whose closure
/// never ran: `ShutdownOutcome::NotRun`, and nothing it would change changed.
pub fn blocking<T>(
    operation: &'static str,
    joined: Result<T, tokio::task::JoinError>,
) -> Result<T, Shutdown> {
    finish(operation, ShutdownOutcome::NotRun, joined)
}

/// Run `run`, the work named `operation`, on the blocking pool and wait for
/// it, as [`blocking`] joins it.
pub async fn run_blocking<T: Send + 'static>(
    operation: &'static str,
    run: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Shutdown> {
    blocking(operation, tokio::task::spawn_blocking(run).await)
}

/// What a spawned task returned. The runtime drops such a task at whichever
/// wait it is at, so what it would change may be changed in part:
/// `ShutdownOutcome::Unknown`.
pub fn task<T>(
    operation: &'static str,
    joined: Result<T, tokio::task::JoinError>,
) -> Result<T, Shutdown> {
    finish(operation, ShutdownOutcome::Unknown, joined)
}

fn finish<T>(
    operation: &'static str,
    outcome: ShutdownOutcome,
    joined: Result<T, tokio::task::JoinError>,
) -> Result<T, Shutdown> {
    match joined {
        Ok(value) => Ok(value),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(Shutdown { operation, outcome }),
    }
}

#[cfg(test)]
#[path = "unwinding_tests.rs"]
mod tests;
