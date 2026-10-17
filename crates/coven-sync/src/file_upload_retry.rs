//! When queued uploads retry (§17.7, E7): each failure is the file's pending
//! record. Recoverable source, disk and provider failures wait their persisted
//! delay; sign-in and access failures wait for a new connection. A catch-up
//! wait is woken by the sync state; a provider cooldown holds the whole queue.

use crate::{files::FilesInner, PendingReason, PendingRecord, Retry};
use coven_database::{FileUpload, RetrySubject};

impl FilesInner {
    /// Whether automatic work may attempt the upload now: it has no pending
    /// record, its delay elapsed, or a new connection can clear its failure.
    pub(super) fn retry_due(&self, item: &FileUpload, record: Option<&PendingRecord>) -> bool {
        record.is_none_or(|record| {
            record.retry() == Retry::Automatic
                || record.reason.clears() == Some(crate::pending::Clears::ByPerson)
                || record.reason == PendingReason::NoStorage
        }) && self.connection_due(RetrySubject::Upload(item.id))
            && self
                .waits
                .due(RetrySubject::Upload(item.id), item.retry.as_ref())
    }

    /// When the worker must next drain and fill without other wakes: the
    /// earliest due upload or eager download retry, or the end of a provider
    /// cooldown holding the upload queue.
    pub(crate) fn next_retry(&self) -> Option<crate::retry_waits::Wake> {
        // Resuming, connecting and a completed catch-up wake the worker themselves.
        let connected = self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_some();
        let uploads = connected && !self.paused() && self.send.files_allowed();
        self.waits
            .next_wake(|subject| match subject {
                RetrySubject::Upload(_) => uploads,
                RetrySubject::Download { .. } => connected,
                _ => false,
            })
            .or_else(|| uploads.then(|| self.waits.cooldown_wake()).flatten())
    }
}
