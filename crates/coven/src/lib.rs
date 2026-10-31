//! The application API for coven stores (api.md).
//!
//! Declare tables and migrations on [`Coven::builder`], then create or open
//! the store. SQL, files, keys and their capabilities remain with the
//! crates that own them. Opening resumes committed file work without starting
//! a sync loop; unfinished operations resume once storage connects.

mod application_api;
pub use application_api::*;

/// Background work whose panic comes back as a `TaskFailed` (api.md E15).
pub use coven_foundation::task;

mod builder;
mod coven;
mod error;
mod handle;
mod read_handle;
mod saved_restore;
#[cfg(any(test, feature = "test-utils"))]
mod test_utils;

mod circles;

#[cfg(feature = "dev-tools")]
mod dev_tools;

mod authentication;
mod storage;
mod store_layout;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
