//! Schema fixtures and crash checkpoints for tests of the production database.

use crate::{Migration, RowIdentity, SyncedTable};

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

/// Declarations for a UUID-keyed notes table in the store audience.
pub fn notes_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::IndependentUuid)]
}

/// The corresponding app migration sequence.
pub fn notes_migrations() -> Vec<Migration> {
    vec![Migration::sql(1, "notes", "CREATE TABLE notes (id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT '')")]
}
