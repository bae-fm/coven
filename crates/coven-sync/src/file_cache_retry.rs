//! Download failure records and their timer or connection waits.

use super::file_error::TransferError;
use crate::{
    files::FilesInner, pending::Clears, FileReadError, LocalFailure, PendingReason, PendingRecord,
    PendingSubject, Prerequisite,
};
use coven_database::{DbError, FileRef, RetrySubject, RetryWait};

/// The persisted wait of an eager download of `file`.
pub(crate) fn download_subject(file: &FileRef) -> RetrySubject {
    RetrySubject::Download {
        uploader: file.uploader(),
        file: file.id(),
    }
}
impl FilesInner {
    /// The pending record and retry schedule belong to the fixed file, no
    /// matter which consumer attempted the download.
    pub(super) async fn finish_download(
        &self,
        file: &FileRef,
        reason: Option<PendingReason>,
        whole: bool,
    ) -> Result<(), crate::SyncError> {
        let subject = PendingSubject::File {
            device: file.uploader(),
            file: file.id(),
        };
        let retry = download_subject(file);
        match reason {
            Some(reason) => {
                let previous = self.database.download_wait(retry).await?;
                self.record_download_failure(subject, retry, reason, previous.as_ref())
                    .await
            }
            None => {
                let upload = self
                    .database
                    .clear_file_pending::<PendingRecord>(file, subject, move |record| {
                        whole
                            || !matches!(
                                record.reason,
                                PendingReason::Refused(_)
                                    | PendingReason::FileUnavailable(_)
                                    | PendingReason::UpdateRequired(_)
                            )
                    })
                    .await?;
                if let Some(upload) = upload {
                    self.waits.forget(upload);
                }
                self.waits.forget(retry);
                self.forget_connection_wait(retry);
                Ok(())
            }
        }
    }

    pub(super) fn download_reason(&self, file: &FileRef, error: &TransferError) -> PendingReason {
        match error {
            TransferError::File(error) => self.read_pending(file, error),
            TransferError::Refused(reason) => PendingReason::Refused(reason.clone()),
        }
    }

    /// Preserve the immediate native error even when persisting its durable
    /// condition also fails.
    pub(super) async fn complete_download<T>(
        &self,
        file: &FileRef,
        result: Result<T, TransferError>,
        whole: bool,
    ) -> Result<T, FileReadError> {
        let reason = result
            .as_ref()
            .err()
            .map(|error| self.download_reason(file, error));
        let result = result.map_err(|error| match error {
            TransferError::File(error) => error,
            TransferError::Refused(_) => FileReadError::Integrity {
                id: file.id().to_string(),
            },
        });
        if result.as_ref().is_err_and(unrecorded) {
            return result;
        }
        let recorded = self.finish_download(file, reason, whole).await;
        combine(result, recorded)
    }

    /// Local reads can settle only a local source failure, not an upload's
    /// provider condition. An already-open local stream remains usable after
    /// its store closes, when no pending transaction can run.
    pub(super) async fn complete_local_read<T>(
        &self,
        file: &FileRef,
        result: Result<T, FileReadError>,
    ) -> Result<T, FileReadError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire)
            || result.as_ref().is_err_and(unrecorded)
        {
            return result;
        }
        let subject = PendingSubject::File {
            device: file.uploader(),
            file: file.id(),
        };
        let recorded = match &result {
            Ok(_) => match self
                .database
                .clear_file_pending::<PendingRecord>(file, subject, |record| {
                    matches!(
                        record.reason,
                        PendingReason::FileUnavailable(crate::FileMissingReason::Source(_))
                    )
                })
                .await
            {
                Ok(retry) => {
                    if let Some(retry) = retry {
                        self.waits.forget(retry);
                    }
                    Ok(())
                }
                Err(error) => Err(error.into()),
            },
            Err(error) => {
                let reason = match error {
                    FileReadError::Integrity { .. } => PendingReason::FileUnavailable(
                        crate::FileMissingReason::Source(crate::FileSourceFailure::Integrity),
                    ),
                    error => self.read_pending(file, error),
                };
                let waits = self.waits.clone();
                match self
                    .database
                    .fail_local_read(file, PendingRecord { subject, reason }, move |previous| {
                        waits
                            .next(previous.as_ref())
                            .map_err(|error| DbError::SyncStream(Box::new(error)))
                    })
                    .await
                {
                    Ok(Some((subject, wait))) => {
                        self.waits.arm(subject, &wait);
                        Ok(())
                    }
                    Ok(None) => Ok(()),
                    Err(error) => Err(error.into()),
                }
            }
        };
        combine(result, recorded)
    }

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
            .fail_download(PendingRecord { subject, reason }, retry, wait)
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
            FileReadError::UserFileMissing { .. } => PendingReason::FileUnavailable(
                crate::FileMissingReason::Source(crate::FileSourceFailure::Missing),
            ),
            FileReadError::UserFileChanged { .. } => PendingReason::FileUnavailable(
                crate::FileMissingReason::Source(crate::FileSourceFailure::Changed),
            ),
            FileReadError::Database(_) | FileReadError::RangeOutOfBounds { .. } => {
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

/// Invalid arguments and a closed store do not introduce file work.
fn unrecorded(error: &FileReadError) -> bool {
    matches!(
        error,
        FileReadError::RangeOutOfBounds { .. }
            | FileReadError::Database(
                DbError::StoreClosed | DbError::FileRefChanged { .. } | DbError::FileAbsent { .. }
            )
    )
}

fn combine<T>(
    result: Result<T, FileReadError>,
    recorded: Result<(), crate::SyncError>,
) -> Result<T, FileReadError> {
    match (result, recorded) {
        (result, Ok(())) => result,
        (Ok(_), Err(crate::SyncError::Database(error))) => Err(error.into()),
        (Ok(_), Err(error)) => Err(DbError::SyncStream(Box::new(error)).into()),
        (Err(operation), Err(cleanup)) => {
            Err(DbError::SyncStream(Box::new(crate::SyncError::Cleanup {
                operation: Box::new(crate::SyncError::Failed {
                    failure: crate::LocalFailure::Operation,
                    source: std::sync::Arc::new(operation),
                }),
                cleanup: Box::new(cleanup),
            }))
            .into())
        }
    }
}
