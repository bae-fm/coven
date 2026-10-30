//! Upload failures and the pending record and retry wait they commit together.

use crate::{
    files::file_error::TransferError, files::file_failure::FileFailure, files::FilesInner,
    PendingReason, Prerequisite, Retry, SyncError,
};
use coven_database::{FileUpload, RetrySubject};

impl FilesInner {
    /// Record a failed attempt as the file's pending record with its next
    /// wait, persisting T and W before the upload waits: a user's original
    /// that went missing on its move's record, one that changed by failing
    /// the move. A catch-up wait is
    /// woken by the sync state; connection waits and permanent failures keep no timer.
    /// A wait that cannot be persisted holds the upload until the app retries.
    pub(super) async fn record_failure(
        &self,
        item: &FileUpload,
        error: TransferError,
    ) -> Result<(), SyncError> {
        let subject = RetrySubject::Upload(item.id);
        let record = self.source_record(&item.location, item.of, FileFailure::Held(error));
        // A changed original fails its move whole (§17.3).
        if record.retry() == Retry::AppAction
            && matches!(record.reason, PendingReason::Original { .. })
        {
            self.waits.forget(subject);
            return Ok(self.database.fail_move(item.id, record).await?);
        }
        let needs_connection = record.reason.clears() == Some(crate::pending::Clears::ByPerson);
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
