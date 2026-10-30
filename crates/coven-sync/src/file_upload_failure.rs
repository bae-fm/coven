//! Upload failures and the pending record and retry wait they commit together.

use crate::{
    files::file_error::TransferError,
    files::file_failure::{upload_subject, FileCopy, FileFailure},
    files::FilesInner,
    PendingReason, PendingRecord, Prerequisite, Retry, SyncError,
};
use coven_database::{FileUpload, RetrySubject};

impl FilesInner {
    /// Record a failed attempt as the file's pending record with its next
    /// wait, persisting T and W before the upload waits. A catch-up wait is
    /// woken by the sync state; connection waits and permanent failures keep no timer.
    /// A wait that cannot be persisted holds the upload until the app retries.
    pub(super) async fn record_failure(
        &self,
        item: &FileUpload,
        error: TransferError,
    ) -> Result<(), SyncError> {
        let subject = RetrySubject::Upload(item.id);
        let pending = upload_subject(&item.location);
        let reason = self.file_failure(&pending, FileCopy::Original, FileFailure::Held(error));
        let needs_connection = reason.clears() == Some(crate::pending::Clears::ByPerson);
        let record = PendingRecord::new(pending, reason);
        let timed = record.retry() == Retry::Automatic
            && record.reason != PendingReason::Waits(Prerequisite::SequenceCatchUp);
        let retry = if timed {
            match self.waits.next(item.retry.as_ref()) {
                Ok(wait) => Some(wait),
                Err(next) => {
                    self.waits.hold(subject);
                    return Err(next);
                }
            }
        } else {
            None
        };
        if let Err(persist) = self.database.fail_upload(item.id, record, retry).await {
            self.waits.hold(subject);
            return Err(persist.into());
        }
        if needs_connection {
            self.wait_for_connection(subject);
        }
        match &retry {
            Some(wait) => self.waits.arm(subject, wait),
            None => self.waits.forget(subject),
        }
        Ok(())
    }
}
