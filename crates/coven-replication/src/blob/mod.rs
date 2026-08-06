//! Blob domain workflows: locality transitions, tombstone lifecycle, retry
//! policy, and local cleanup. The blob value model — references, locators,
//! scopes, transfer limits, and the transition observer port — lives in
//! [`coven_protocol::blob`].

pub(crate) mod delete;
pub(crate) mod retry;
pub mod transition;

pub use delete::BlobTombstoneJson;
pub use transition::{MakeLocalError, MakeRemoteError};

#[cfg(test)]
mod upload_tests;

#[cfg(test)]
mod transition_tests;

#[cfg(test)]
mod local_store_tests;

#[cfg(test)]
mod delete_tests;
