//! Download failure records and their timer or connection waits.

use super::file_error::TransferError;
use super::file_failure::{FileCopy, FileFailure};
use crate::{
    files::FilesInner, pending::Clears, FileReadError, PendingReason, PendingRecord, PendingSubject,
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
                // A partial read settles every failure but those a whole
                // file must clear.
                let ends = move |reason: &PendingReason| {
                    whole
                        || !matches!(
                            reason,
                            PendingReason::Refused(_)
                                | PendingReason::FileUnavailable(_)
                                | PendingReason::UpdateRequired(_)
                        )
                };
                let upload = self
                    .database
                    .clear_file_pending::<PendingRecord>(file, subject.clone(), move |record| {
                        ends(&record.reason)
                    })
                    .await?;
                self.waits.causes().cleared(&subject, ends);
                if let Some(upload) = upload {
                    self.waits.forget(upload);
                }
                self.waits.forget(retry);
                self.forget_connection_wait(retry);
                Ok(())
            }
        }
    }

    /// Preserve the immediate native error even when persisting its durable
    /// condition also fails. The caller receives the error, so the record
    /// keeps none as its cause.
    pub(super) async fn complete_download<T>(
        &self,
        file: &FileRef,
        result: Result<T, TransferError>,
        whole: bool,
    ) -> Result<T, FileReadError> {
        let recorded = !matches!(&result, Err(TransferError::File(error)) if unrecorded(error));
        let reason = match &result {
            Err(error) if recorded => {
                Some(self.file_failure(file, FileCopy::Stored, FileFailure::Returned(error)))
            }
            Ok(_) | Err(_) => None,
        };
        let result = result.map_err(|error| match error {
            TransferError::File(error) => error,
            // A refused transfer keeps its authentication failure.
            TransferError::Refused(refusal) => FileReadError::Integrity {
                id: file.id().to_string(),
                cause: match refusal {
                    crate::Refusal::Decryption { cause } => cause,
                    _ => None,
                },
            },
        });
        if !recorded {
            return result;
        }
        let recorded = self.finish_download(file, reason, whole).await;
        combine(result, recorded)
    }

    /// A read-ahead has no caller: its failure is held as the record's
    /// cause, and a failure to record its outcome is logged.
    pub(super) async fn finish_read_ahead(
        &self,
        file: &FileRef,
        result: Result<Vec<u8>, TransferError>,
    ) {
        let reason = match result {
            Ok(_) => None,
            Err(TransferError::File(error)) if unrecorded(&error) => return,
            Err(error) => Some(self.file_failure(file, FileCopy::Stored, FileFailure::Held(error))),
        };
        if let Err(error) = self.finish_download(file, reason, false).await {
            tracing::warn!(file = %file.id(), "recording a read-ahead's outcome failed: {error}");
        }
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
                .clear_file_pending::<PendingRecord>(file, subject.clone(), |record| {
                    source_failure(&record.reason)
                })
                .await
            {
                Ok(retry) => {
                    self.waits.causes().cleared(&subject, source_failure);
                    if let Some(retry) = retry {
                        self.waits.forget(retry);
                    }
                    Ok(())
                }
                Err(error) => Err(error.into()),
            },
            Err(error) => {
                let reason =
                    self.file_failure(file, FileCopy::Original, FileFailure::ReturnedRead(error));
                let waits = self.waits.clone();
                match self
                    .database
                    .fail_local_read(file, PendingRecord::new(subject, reason), move |previous| {
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
    /// waits (§17.7): recoverable conditions retry by time or a new connection;
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
            .fail_download(PendingRecord::new(subject, reason), retry, wait)
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

/// A failure of the attached original's source, which only reading it
/// again settles.
fn source_failure(reason: &PendingReason) -> bool {
    matches!(
        reason,
        PendingReason::FileUnavailable(crate::FileMissingReason::Source(_))
    )
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
