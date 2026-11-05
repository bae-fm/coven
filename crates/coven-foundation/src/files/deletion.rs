//! The record of a store's deletion from a device that leaves the store
//! (E1, §11.6).
//!
//! `delete_store` journals this device's removal in the store's database,
//! and the store's data can go only once that removal has applied. So just
//! before it journals the removal, the deletion records itself here, in the
//! store's directory, with what it does with unsent work. From the record
//! on, opening the store is refused and the listing shows it as being
//! deleted, until the deletion finishes and the directory goes with it.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::{atomic_file, FileError};

const RECORD: &str = "deleting.json";

/// What `delete_store` does with writes and files not yet uploaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IfUnsent {
    /// Refuse with `StoreDeletionError::Unsent`, deleting nothing.
    Refuse,
    /// Delete them with the store.
    Discard,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeletionRecord {
    unsent: IfUnsent,
}

/// The recorded deletion's choice for unsent work, when one is recorded.
pub(super) fn read(directory: &Path) -> Result<Option<IfUnsent>, FileError> {
    Ok(atomic_file::read_json::<DeletionRecord>(&directory.join(RECORD), "read deletion record")?
        .map(|record| record.unsent))
}

/// Record the deletion, durably, before its removal is journaled.
pub(super) fn record(directory: &Path, unsent: IfUnsent) -> Result<(), FileError> {
    let bytes = serde_json::to_vec(&DeletionRecord { unsent }).expect("deletion record encodes");
    atomic_file::replace(&directory.join(RECORD), &bytes)
}

/// Remove the record of a deletion that ended before its removal was
/// journaled: the store is as it was.
pub(super) fn forget(directory: &Path) -> Result<(), FileError> {
    atomic_file::remove(&directory.join(RECORD))
}

#[cfg(test)]
#[path = "deletion_tests.rs"]
mod tests;
