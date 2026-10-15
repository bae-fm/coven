//! Schema fixtures and crash checkpoints for tests of the production database.

use crate::{Migration, RowIdentity, SyncedTable};

#[path = "work_counts.rs"]
mod work_counts;
pub use work_counts::{WorkCounter, WorkCounts};

pub(crate) type WriteCheckpointObserver = Box<dyn Fn(WriteCheckpoint) + Send>;

/// Boundaries after a local write's durable commit, for process-crash tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteCheckpoint {
    /// SQLite has committed, before notifying queries and sync.
    Committed,
    /// Commit observers have been notified.
    Observed,
    /// The write has released its staged-file reservations.
    StagingReleased,
    /// An obsolete file's bytes have been removed.
    FileRemoved,
    /// The obsolete file's pending-removal row has been deleted.
    FileRemovalRecorded,
    /// Post-commit file cleanup has returned.
    Finished,
}

/// Publication facts for a write a test applies as already authenticated,
/// signed by a fixed key no fixture member binds: no member-key replacement
/// names it, so only device retirements decide its window (§10).
pub fn published(stored_at: coven_format::value::StorageTime) -> crate::Published {
    let mut secret = b"CVMK\x01".to_vec();
    secret.extend([0xA5; 64]);
    crate::Published {
        stored_at,
        signer: coven_crypto::MemberKeys::from_secret_bytes(&secret)
            .expect("fixed fixture pair")
            .public_keys()
            .signing,
    }
}

/// Declarations for a UUID-keyed notes table in the store audience.
pub fn notes_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)]
}

/// The corresponding app migration sequence.
pub fn notes_migrations() -> Vec<Migration> {
    vec![Migration::sql(1, "notes", "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '')")]
}

#[cfg(test)]
#[path = "test_utils_tests.rs"]
pub(crate) mod tests;
