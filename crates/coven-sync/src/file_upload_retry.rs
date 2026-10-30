//! When queued uploads retry (§17.8, E7): each failure is the file's pending
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

    /// When the upload worker must next run without other wakes: the
    /// earliest due upload retry, or the end of a provider cooldown holding
    /// the queue. Resuming, connecting and a completed catch-up wake it
    /// themselves.
    pub(crate) fn next_upload_retry(&self) -> Option<crate::retry_waits::Wake> {
        let uploads = self
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_some()
            && !self.paused()
            && self.send.files_allowed();
        // A pin coven keeps waits for its own retry, uploads paused or not.
        let wake = self.waits.next_wake(|subject| {
            matches!(subject, RetrySubject::Pin(_))
                || (uploads && matches!(subject, RetrySubject::Upload(_)))
        });
        if !uploads {
            return wake;
        }
        wake.or_else(|| self.waits.cooldown_wake())
    }
}
