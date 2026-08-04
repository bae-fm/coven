//! Blob domain workflows: locality transitions, tombstone lifecycle, retry
//! policy, and local cleanup. The blob value model — references, locators,
//! scopes, transfer limits, and the transition observer port — lives in
//! [`crate::protocol::blob`].

pub(crate) mod delete;
pub(crate) mod local_cleanup;
pub(crate) mod retry;
pub(crate) mod transition;

pub use transition::{MakeLocalError, MakeRemoteError};

#[cfg(test)]
mod upload_tests;

#[cfg(test)]
mod transition_tests;

#[cfg(test)]
mod local_store_tests;

#[cfg(test)]
mod delete_tests;
