//! Download failure records, and the records local reads of a staged
//! original settle. A download has no wait: only the app's next read or pin
//! tries it again (§17.6).

use super::file_error::TransferError;
use super::file_failure::{download_subject, stored, FileCopy, FileFailure};
use crate::{files::FilesInner, FileReadError, PendingReason, PendingRecord, PendingSubject};
use coven_database::{DbError, FileRef};
use coven_foundation::id_source::FileId;

impl FilesInner {
    /// The pending record belongs to the stored bytes, no matter which
    /// consumer attempted the download.
    pub(super) async fn finish_download(
        &self,
        file: &FileRef,
        reason: Option<PendingReason>,
        whole: bool,
    ) -> Result<(), crate::SyncError> {
        let (location, _) = stored(file).map_err(|_device_only| DbError::Invariant {
            what: "only a stored file is downloaded",
        })?;
        let subject = download_subject(location);
        match reason {
            Some(reason) => Ok(self
                .database
                .fail_download(PendingRecord::new(subject, reason))
                .await?),
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
                self.database
                    .clear_download_pending::<PendingRecord>(subject.clone(), move |record| {
                        ends(&record.reason)
                    })
                    .await?;
                self.waits.causes().cleared(&subject, ends);
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
        let subject = match stored(file) {
            Ok((location, _)) => download_subject(location),
            Err(error) => return Err(error),
        };
        let reason = match &result {
            Err(error) if recorded => {
                Some(self.file_failure(&subject, FileCopy::Stored, FileFailure::Returned(error)))
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
        let Ok((location, _)) = stored(file) else {
            return;
        };
        let subject = download_subject(location);
        let reason = match result {
            Ok(_) => None,
            Err(TransferError::File(error)) if unrecorded(&error) => return,
            Err(error) => {
                Some(self.file_failure(&subject, FileCopy::Stored, FileFailure::Held(error)))
            }
        };
        if let Err(error) = self.finish_download(file, reason, false).await {
            tracing::warn!(file = %file.id(), "recording a read-ahead's outcome failed: {error}");
        }
    }

    /// Local reads of a file this device uploads can settle only a local
    /// source failure of its upload's bytes, which `subject` names, not an
    /// upload's provider condition. An already-open local stream remains
    /// usable after its store closes, when no pending transaction can run.
    pub(super) async fn complete_local_read<T>(
        &self,
        file: FileId,
        subject: &PendingSubject,
        result: Result<T, FileReadError>,
    ) -> Result<T, FileReadError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire)
            || result.as_ref().is_err_and(unrecorded)
        {
            return result;
        }
        let subject = subject.clone();
        let PendingSubject::Upload {
            device,
            file: object,
        } = &subject
        else {
            return Err(DbError::Invariant {
                what: "a local read's subject is its upload's stored bytes",
            }
            .into());
        };
        let (device, object) = (*device, *object);
        let recorded = match &result {
            Ok(_) => match self
                .database
                .clear_upload_pending::<PendingRecord>(
                    (device, object),
                    subject.clone(),
                    |record| source_failure(&record.reason),
                )
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
                let reason = self.file_failure(
                    &subject,
                    FileCopy::Original,
                    FileFailure::ReturnedRead(error),
                );
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
}

/// A failure of the staged original's source, which only reading it
/// again settles.
fn source_failure(reason: &PendingReason) -> bool {
    matches!(
        reason,
        PendingReason::FileUnavailable(crate::FileMissingReason::Source(_))
    )
}

/// Invalid arguments, a closed store and a file only on another device do
/// not introduce file work.
fn unrecorded(error: &FileReadError) -> bool {
    matches!(
        error,
        FileReadError::RangeOutOfBounds { .. }
            | FileReadError::DeviceOnly { .. }
            | FileReadError::Database(DbError::StoreClosed | DbError::FileAbsent { .. })
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
