//! Verification of exact Store commits and of the Merge history they form.
//!
//! [`commit::StoreCommitVerifier`] verifies one commit and the protocol objects
//! it references; [`merge_history::MergeHistoryVerifier`] holds a commit
//! verifier and builds the verified commit graph over it. Each half names the
//! other, so neither is the module root.

pub(crate) mod commit;
pub(crate) mod merge_history;
