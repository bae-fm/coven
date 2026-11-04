//! Coven's clock, id source, atomic file writes, store directories and locks,
//! the watch of the device's network, and the helpers that run coven's own
//! work apart from its caller, turning a panic in it into a value or a report
//! to the app's bug sink, and its runtime shutting down into a value.

pub mod clock;
pub mod files;
pub mod id_source;
#[cfg(target_os = "android")]
pub mod java_exception;
pub mod network_watch;
pub mod progress;
pub mod task;
pub mod uuid_text;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_work;
