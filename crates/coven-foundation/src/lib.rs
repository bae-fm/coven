//! Coven's clock, id source, atomic file writes, store directories and locks,
//! the helpers that turn a panic in background work into a value, and the
//! joins of coven's own work that return its runtime shutting down as one.

pub mod clock;
pub mod files;
pub mod id_source;
pub mod progress;
pub mod task;
pub mod unwinding;
pub mod uuid_text;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_work;
