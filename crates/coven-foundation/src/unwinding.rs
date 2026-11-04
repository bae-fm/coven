//! The join of a long-lived worker its owner stops, where a panic in the
//! worker unwinds into the owner as if the worker had run inline. The runtime
//! dropping the worker as it shuts down is not a bug, so it comes back as a
//! [`Shutdown`] value (E15).
//!
//! Work a caller waits for uses [`crate::task`] instead, which turns the
//! panic into a value too.

use crate::task::{Shutdown, ShutdownOutcome};

/// What a spawned task returned. The runtime drops such a task at whichever
/// wait it is at, so what it would change may be changed in part:
/// `ShutdownOutcome::Unknown`.
pub fn task<T>(
    operation: &'static str,
    joined: Result<T, tokio::task::JoinError>,
) -> Result<T, Shutdown> {
    match joined {
        Ok(value) => Ok(value),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(Shutdown { operation, outcome: ShutdownOutcome::Unknown }),
    }
}

#[cfg(test)]
#[path = "unwinding_tests.rs"]
mod tests;
