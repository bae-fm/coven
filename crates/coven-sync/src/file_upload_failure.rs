//! Upload failures and the pending record and retry wait they commit together.

use crate::FileSourceFailure;
use crate::{
    files::file_error::{FileReadError, UploadFailure},
    files::FilesInner,
    FileMissingReason, LocalFailure, PendingReason, PendingRecord, PendingSubject, Prerequisite,
    Retry, SyncError,
};
use coven_database::{FileUpload, RetrySubject};
use coven_storage::StorageFailure;

impl FilesInner {
    /// The first unmet condition a failed attempt leaves for its file (§19.1):
    /// a missing or changed source is the uploader's own `FileUnavailable`.
    fn upload_reason(&self, error: &UploadFailure) -> PendingReason {
        match error {
            UploadFailure::Refused(reason) => PendingReason::Refused(reason.clone()),
            UploadFailure::Storage(error) | UploadFailure::File(FileReadError::Storage(error)) => {
                if error.failure() == StorageFailure::CatchUpRequired {
                    return PendingReason::Waits(Prerequisite::SequenceCatchUp);
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

    /// Record a failed attempt as the file's pending record with its next
    /// wait, persisting T and W before the upload waits. A catch-up wait is
    /// woken by the sync state; connection waits and permanent failures keep no timer.
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
        let needs_connection = reason.clears() == Some(crate::pending::Clears::ByPerson);
        let record = PendingRecord {
            subject: file,
            reason,
        };
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
