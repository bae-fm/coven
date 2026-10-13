//! Test-only accounting shared by a database's writer and readers.

use std::sync::{Arc, Mutex};

/// Local database work observed at its execution boundaries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkCounts {
    /// Successful acquisitions of the writer connection, including read-only work.
    pub writer_holds: usize,
    /// Reconstructions of the saved store log, including empty logs.
    pub full_log_decodes: usize,
    /// Applied store-log entries passed to the plaintext decoder.
    pub entry_decodes: usize,
    /// Locally reserved entries decoded from the publication queue.
    pub queued_entry_decodes: usize,
    /// Successful durable transaction commits, including unchanged transactions.
    pub commits: usize,
    /// Successful atomic installations of a ready entry batch and its replay.
    pub store_log_commits: usize,
}

/// Injected counter; clones observe the same database, never other test databases.
#[derive(Clone, Default)]
pub struct WorkCounter(Arc<Mutex<WorkCounts>>);

impl WorkCounter {
    /// Start another measurement after the preceding work has completed.
    pub fn take(&self) -> WorkCounts {
        std::mem::take(&mut *self.0.lock().expect("work counter poisoned"))
    }

    pub(crate) fn record(&self, update: impl FnOnce(&mut WorkCounts)) {
        update(&mut self.0.lock().expect("work counter poisoned"));
    }
}
