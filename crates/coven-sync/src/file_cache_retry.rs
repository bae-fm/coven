//! Download failure records and their timer or connection waits.

use crate::{
    files::FilesInner, pending::Clears, FileReadError, LocalFailure, PendingReason, PendingRecord,
    PendingSubject, Prerequisite,
};
use coven_database::{FileRef, RetrySubject, RetryWait};

/// The persisted wait of an eager download of `file`.
pub(crate) fn download_subject(file: &FileRef) -> RetrySubject {
    RetrySubject::Download {
        uploader: file.uploader(),
        file: file.id(),
    }
}
impl FilesInner {
    /// Persist the failed download's record with its next wait before it
    /// waits (§16.5): recoverable conditions retry by time or a new connection;
    /// permanent failures and updates keep no timer. A wait that
    /// cannot be chosen or persisted holds the download until the app acts,
    /// and fails the fill.
    pub(super) async fn record_download_failure(
        &self,
        subject: PendingSubject,
        retry: RetrySubject,
        reason: PendingReason,
        previous: Option<&RetryWait>,
    ) -> Result<(), crate::SyncError> {
        let clears = download_clears(&reason);
        let timed = clears == Clears::ByItself;
        let wait = if timed {
            match self.waits.next(previous) {
                Ok(wait) => Some(wait),
                Err(error) => {
                    self.waits.hold(retry);
                    return Err(error);
                }
            }
        } else {
            None
        };
        let recorded = self
            .database
            .fail_download(
                move |reported_by| PendingRecord {
                    subject,
                    reason,
                    reported_by,
                },
                retry,
                wait,
            )
            .await;
        if let Err(error) = recorded {
            self.waits.hold(retry);
            return Err(error.into());
        }
        if clears == Clears::ByPerson {
            self.wait_for_connection(retry);
        }
        match wait {
            Some(wait) => self.waits.arm(retry, &wait),
            None => self.waits.forget(retry),
        }
        Ok(())
    }

    /// The first unmet condition a failed read of `file` leaves (§16.3).
    pub(super) fn read_pending(&self, file: &FileRef, error: &FileReadError) -> PendingReason {
        match error {
            FileReadError::NoStorage => PendingReason::NoStorage,
            FileReadError::Storage(error) => {
                match self.storage.read().expect("storage lock poisoned").as_ref() {
                    Some(storage) => PendingReason::Storage {
                        provider: storage.config().provider(),
                        failure: error.failure(),
                    },
                    None => PendingReason::NoStorage,
                }
            }
            FileReadError::Uploading { .. } => PendingReason::Waits(Prerequisite::Object(
                coven_storage::ObjectPath::file(self.directory.id(), file.uploader(), file.id()),
            )),
            FileReadError::Missing { reason, .. } => PendingReason::FileUnavailable(reason.clone()),
            FileReadError::Integrity { .. } => {
                PendingReason::Refused(crate::Refusal::Decryption { cause: None })
            }
            FileReadError::UpdateRequired(update) => PendingReason::UpdateRequired(*update),
            FileReadError::Disk(_) | FileReadError::Lock(_) => {
                PendingReason::Failed(LocalFailure::Disk)
            }
            FileReadError::Database(_)
            | FileReadError::UserFileMissing { .. }
            | FileReadError::UserFileChanged { .. }
            | FileReadError::RangeOutOfBounds { .. } => {
                PendingReason::Failed(LocalFailure::Database)
            }
        }
    }
}

/// Provider and local failures use the shared classification. Immutable
/// refusals, unavailable uploaders and required updates end eager work.
pub(super) fn download_clears(reason: &PendingReason) -> Clears {
    reason.clears().unwrap_or(match reason {
        PendingReason::NoStorage => Clears::ByPerson,
        PendingReason::Refused(_)
        | PendingReason::FileUnavailable(_)
        | PendingReason::UpdateRequired(_) => Clears::Never,
        _ => Clears::ByItself,
    })
}
