//! When queued uploads retry (§16.5, E7): each failure is the file's pending
//! record; transient ones wait their persisted delay, a catch-up wait is woken
//! by the send gate, failures that need the app wait for it, and a provider
//! cooldown holds the whole queue.

use crate::{
    files::file_error::{FileReadError, UploadFailure},
    files::FilesInner,
    FileMissingReason, LocalFailure, PendingReason, PendingRecord, PendingSubject, Prerequisite,
    Retry, SyncError,
};
use coven_database::{FileUpload, RetrySubject};
use coven_format::pending::FileSourceFailure;
use coven_storage::StorageFailure;

impl FilesInner {
    /// Whether automatic work may attempt the upload now: it has no pending
    /// record, or its record retries automatically and its delay has elapsed.
    /// Other reasons wait for the app.
    pub(super) fn retry_due(&self, item: &FileUpload, record: Option<&PendingRecord>) -> bool {
        record.is_none_or(|record| record.retry() == Retry::Automatic)
            && self
                .waits
                .due(RetrySubject::Upload(item.id), item.retry.as_ref())
    }

    /// The first unmet condition a failed attempt leaves for its file (§19.1):
    /// a missing or changed source is the uploader's own `FileUnavailable`,
    /// which its positions publish.
    fn upload_reason(&self, error: &UploadFailure) -> PendingReason {
        match error {
            UploadFailure::Storage(error) | UploadFailure::File(FileReadError::Storage(error)) => {
                if error.failure() == StorageFailure::CatchUpRequired {
                    return PendingReason::Waits(Prerequisite::CatchUp);
                }
                match self.storage.read().expect("storage lock poisoned").as_ref() {
                    Some(storage) => PendingReason::Storage {
                        provider: storage.config().provider(),
                        failure: error.failure(),
                    },
                    None => PendingReason::NoStorage,
                }
            }
            UploadFailure::File(FileReadError::NoStorage) => PendingReason::NoStorage,
            UploadFailure::File(FileReadError::UserFileMissing { .. }) => {
                PendingReason::FileUnavailable(FileMissingReason::Source(
                    FileSourceFailure::Missing,
                ))
            }
            UploadFailure::File(FileReadError::UserFileChanged { .. }) => {
                PendingReason::FileUnavailable(FileMissingReason::Source(
                    FileSourceFailure::Changed,
                ))
            }
            UploadFailure::File(FileReadError::Integrity { .. }) => PendingReason::FileUnavailable(
                FileMissingReason::Source(FileSourceFailure::Integrity),
            ),
            UploadFailure::File(FileReadError::Disk(_) | FileReadError::Lock(_)) => {
                PendingReason::Failed(LocalFailure::Disk)
            }
            UploadFailure::File(_) => PendingReason::Failed(LocalFailure::Database),
        }
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
        let uploads = connected && !self.paused() && self.send.can_send();
        self.waits
            .next_wake(|subject| match subject {
                RetrySubject::Upload(_) => uploads,
                RetrySubject::Download { .. } => connected,
                _ => false,
            })
            .or_else(|| uploads.then(|| self.waits.cooldown_wake()).flatten())
    }

    /// Record a failed attempt as the file's pending record with its next
    /// wait, persisting T and W before the upload waits. A catch-up wait is
    /// woken by the send gate, and a reason needing the app keeps no timer.
    /// A wait that cannot be persisted holds the upload until the app retries.
    pub(super) async fn record_failure(
        &self,
        item: &FileUpload,
        error: &UploadFailure,
    ) -> Result<(), SyncError> {
        let subject = RetrySubject::Upload(item.id);
        let file = PendingSubject::File {
            device: item.file.uploader(),
            file: item.file.id(),
        };
        let reason = self.upload_reason(error);
        let timed = PendingRecord {
            subject: file.clone(),
            reason: reason.clone(),
            reported_by: item.file.uploader(),
        }
        .retry()
            == Retry::Automatic
            && reason != PendingReason::Waits(Prerequisite::CatchUp);
        let record = move |reported_by| PendingRecord {
            subject: file,
            reason,
            reported_by,
        };
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
        match &retry {
            Some(wait) => self.waits.arm(subject, wait),
            None => self.waits.forget(subject),
        }
        Ok(())
    }
}
