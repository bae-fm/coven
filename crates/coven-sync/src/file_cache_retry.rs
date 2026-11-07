//! Download failure records, and the records local reads of a staged
//! original settle. A download has no wait: only the app's next read or pin
//! tries it again (§17.7). Only a pin's failure is recorded: a pin is the
//! app's lasting wish for the bytes, while a read's failure goes to its
//! caller, and nothing is left pending after it.

use super::file_error::TransferError;
use super::file_failure::{download_subject, stored, upload_subject, FileCopy, FileFailure};
use super::file_status::Uploading;
use super::FilesMode;
use crate::{files::FilesInner, FileReadError, PendingReason, PendingRecord, PendingSubject};
use coven_database::{DbError, FileRef};
use coven_foundation::id_source::FileId;

impl<M: FilesMode> FilesInner<M> {
    /// Record a pin's failure as the stored bytes' pending record, or, with
    /// no failure, clear the conditions the completed download checked. The
    /// record belongs to the stored bytes, whichever call downloaded them. A
    /// read-only handle keeps no records.
    async fn finish_download(
        &self,
        file: &FileRef,
        failed: Option<PendingRecord>,
        whole: bool,
    ) -> Result<(), crate::SyncError> {
        let Some(records) = self.database.records() else {
            return Ok(());
        };
        let (location, _) = stored(file).map_err(|_device_only| DbError::Invariant {
            what: "only a stored file is downloaded",
        })?;
        let subject = download_subject(location);
        match failed {
            Some(record) => Ok(records.fail_download(record).await?),
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
                records
                    .clear_download_pending::<PendingRecord>(subject.clone(), move |record| {
                        ends(&record.reason)
                    })
                    .await?;
                self.waits.failure_cleared(&subject, ends);
                Ok(())
            }
        }
    }

    /// A pin's download outcome, as its prepare meets it: its failure stays
    /// the stored bytes' pending record until a later pin or read clears
    /// it, and a whole copy written or already kept clears every
    /// condition. The immediate native error survives a failure to persist
    /// its record too. The caller receives the error, so the record keeps
    /// none as its cause.
    pub(super) async fn complete_pin<T>(
        &self,
        file: &FileRef,
        result: Result<T, TransferError>,
    ) -> Result<T, FileReadError> {
        let recorded = !matches!(&result, Err(TransferError::File(error)) if unrecorded(error));
        let subject = match stored(file) {
            Ok((location, _)) => download_subject(location),
            Err(error) => return Err(error),
        };
        let failed = match &result {
            Err(error) if recorded => {
                Some(self.file_failure(subject, FileCopy::Stored, FileFailure::Returned(error)))
            }
            Ok(_) | Err(_) => None,
        };
        let result = result.map_err(|error| returned(file, error));
        if !recorded {
            return result;
        }
        let recorded = self.finish_download(file, failed, true).await;
        combine(result, recorded)
    }

    /// A read's outcome. Its failure returns to the caller only: nothing
    /// is pending after it, and only the caller's next read tries again. A
    /// read that succeeds settles the conditions it checked, every one when
    /// it checked the whole file.
    pub(super) async fn complete_read<T>(
        &self,
        file: &FileRef,
        result: Result<T, TransferError>,
        whole: bool,
    ) -> Result<T, FileReadError> {
        match result {
            Ok(value) => combine(Ok(value), self.finish_download(file, None, whole).await),
            Err(error) => Err(returned(file, error)),
        }
    }

    /// A read-ahead fetches for a read in order, and no caller receives its
    /// failure: the reader's next read fetches that range itself and meets
    /// it, so the failure is only logged. A success settles what a partial
    /// read does, and a failure to record that is logged.
    pub(super) async fn finish_read_ahead(
        &self,
        file: &FileRef,
        result: Result<Vec<u8>, TransferError>,
    ) {
        // Nobody waits for a read-ahead: a bug in it reaches only the
        // store's bug sink.
        let bugs = self.database.bugs();
        if let Err(error) = result {
            if !coven_foundation::task::report_carried(bugs.as_ref(), &error) {
                tracing::debug!(file = %file.id(), "a read-ahead failed: {error}");
            }
            return;
        }
        if let Err(error) = self.finish_download(file, None, false).await {
            if !coven_foundation::task::report_carried(bugs.as_ref(), &error) {
                tracing::warn!(file = %file.id(), "recording a read-ahead's outcome failed: {error}");
            }
        }
    }

    /// Local reads of a file this device uploads can settle only a local
    /// source failure: of coven's copy, on the upload's bytes, or of the
    /// user's original, on its move, not an upload's provider condition. A
    /// read that finds the original changed fails the move (§17.3). An
    /// already-open local stream remains usable after its store closes,
    /// when no pending transaction can run. Only the writer, which keeps
    /// file records, knows a file's upload.
    pub(super) async fn complete_local_read<T>(
        &self,
        file: FileId,
        uploading: &Uploading,
        result: Result<T, FileReadError>,
    ) -> Result<T, FileReadError> {
        let Some(records) = self.database.records() else {
            return result;
        };
        if self.closed.load(std::sync::atomic::Ordering::Acquire)
            || result.as_ref().is_err_and(unrecorded)
        {
            return result;
        }
        let recorded = match &result {
            Ok(_) => self.settle_source(uploading).await,
            Err(error) => {
                let record = self.source_record(
                    &uploading.location,
                    uploading.of,
                    FileFailure::ReturnedRead(error),
                );
                if record.retry() == crate::Retry::AppAction
                    && matches!(record.reason, PendingReason::Original { .. })
                {
                    records.fail_move(uploading.upload, record).await.map_err(Into::into)
                } else {
                    let waits = self.waits.clone();
                    match records
                        .fail_local_read(file, record, move |previous| {
                            waits.next(previous.as_ref())
                        })
                        .await
                    {
                        Ok(Some((subject, wait))) => {
                            self.waits.arm(subject, &wait);
                            Ok(())
                        }
                        Ok(None) => Ok(()),
                        Err(error) => Err(error),
                    }
                }
            }
        };
        combine(result, recorded)
    }

    /// A read or upload of this upload's source succeeded: the source
    /// failure recorded about its copy, on its bytes, and about its
    /// original going missing, on its move, both end.
    pub(super) async fn settle_source(
        &self,
        uploading: &Uploading,
    ) -> Result<(), crate::SyncError> {
        self.settled_source(uploading.upload, &uploading.location, uploading.of).await
    }

    /// `settle_source` for the upload `upload`, whose bytes go to
    /// `location`, of move `of`.
    pub(super) async fn settled_source(
        &self,
        upload: i64,
        location: &coven_database::FileLocation,
        of: coven_database::MoveId,
    ) -> Result<(), crate::SyncError> {
        let Some(records) = self.database.records() else {
            return Ok(());
        };
        let subject = upload_subject(location);
        let object = (location.uploader(), location.object());
        let retry = records
            .clear_upload_pending::<PendingRecord>(object, subject.clone(), |record| {
                source_failure(&record.reason)
            })
            .await?;
        self.waits.failure_cleared(&subject, source_failure);
        if let Some(retry) = retry {
            self.waits.forget(retry);
        }
        let moving = PendingSubject::Move { id: of };
        if let Some(path) = records
            .clear_original::<PendingRecord>(upload, moving.clone(), |record, path| {
                original_missing(&record.reason, path)
            })
            .await?
        {
            self.waits.forget(coven_database::RetrySubject::Upload(upload));
            self.waits.failure_cleared(&moving, move |reason| original_missing(reason, &path));
        }
        Ok(())
    }
}

/// Whether `reason` says the original at `path` is missing.
fn original_missing(reason: &PendingReason, path: &std::path::Path) -> bool {
    matches!(
        reason,
        PendingReason::Original {
            path: missing,
            failure: crate::FileSourceFailure::Missing,
        } if missing == path
    )
}

/// A failure of the staged original's source, which only reading it
/// again settles.
fn source_failure(reason: &PendingReason) -> bool {
    matches!(reason, PendingReason::FileUnavailable(crate::FileMissingReason::Source(_)))
}

/// The error a read or pin of a stored file returns. A refused transfer
/// keeps its authentication failure.
fn returned(file: &FileRef, error: TransferError) -> FileReadError {
    match error {
        TransferError::File(error) => error,
        TransferError::Refused(refusal) => FileReadError::Integrity {
            id: file.id().to_string(),
            cause: match refusal {
                crate::Refusal::Decryption { cause } => cause,
                _ => None,
            },
        },
    }
}

/// Invalid arguments, a closed store, a file only on another device and a
/// pin the app changed under its download do not introduce file work.
fn unrecorded(error: &FileReadError) -> bool {
    matches!(
        error,
        FileReadError::RangeOutOfBounds { .. }
            | FileReadError::DeviceOnly { .. }
            | FileReadError::Database(
                DbError::StoreClosed
                    | DbError::FileAbsent { .. }
                    | DbError::NoLongerStored { .. }
                    | DbError::PinChanged { .. }
            )
    )
}

fn combine<T>(
    result: Result<T, FileReadError>,
    recorded: Result<(), crate::SyncError>,
) -> Result<T, FileReadError> {
    match (result, recorded) {
        (result, Ok(())) => result,
        (Ok(_), Err(error)) => Err(error.into()),
        (Err(operation), Err(cleanup)) => Err(crate::SyncError::cleanup(
            crate::SyncError::Failed {
                failure: crate::LocalFailure::Operation,
                source: std::sync::Arc::new(operation),
            },
            cleanup,
        )
        .into()),
    }
}
