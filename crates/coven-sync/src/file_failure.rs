//! The one route from a file's failure to its pending record's reason
//! (§17.5, §17.7). Uploads, downloads and reads all classify here, and the
//! error behind the reason is logged and held as the record's cause
//! (`pending_cause.rs`).
#![deny(clippy::wildcard_enum_match_arm)]

use super::file_error::{FileReadError, TransferError};
use crate::pending_cause::{Cause, Failure};
use crate::{
    files::FilesInner, FileMissingReason, FileSourceFailure, LocalFailure, PendingReason,
    PendingSubject, Prerequisite,
};
use coven_database::FileRef;
use coven_storage::StorageFailure;
use std::sync::Arc;

/// The copy of a file the failed work used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FileCopy {
    /// The original this device attached: an upload, or a read while the
    /// upload is queued.
    Original,
    /// The stored copy: a download, a pin or a read through the cache.
    Stored,
}

/// A file's failure, and who receives its error.
pub(super) enum FileFailure<'a> {
    /// No caller awaits this attempt: the error becomes the record's cause.
    Held(TransferError),
    /// An app call returns this error.
    Returned(&'a TransferError),
    /// An app call returns this read error.
    ReturnedRead(&'a FileReadError),
}

/// The file's pending subject, which its upload and downloads share.
pub(super) fn file_subject(file: &FileRef) -> PendingSubject {
    PendingSubject::File {
        device: file.uploader(),
        file: file.id(),
    }
}

impl FilesInner {
    /// The first unmet condition `failure` leaves for `file`. Its error is
    /// logged when the file's reason appears or changes, and held as the
    /// record's cause when no caller receives it.
    pub(super) fn file_failure(
        &self,
        file: &FileRef,
        copy: FileCopy,
        failure: FileFailure<'_>,
    ) -> PendingReason {
        let reason = match &failure {
            FileFailure::Held(error) => self.transfer_reason(copy, error),
            FileFailure::Returned(error) => self.transfer_reason(copy, error),
            FileFailure::ReturnedRead(error) => self.read_reason(copy, error),
        };
        let failure = match failure {
            FileFailure::Held(error) => Failure::Held(held(error)),
            FileFailure::Returned(error) => Failure::Returned(error),
            FileFailure::ReturnedRead(error) => Failure::Returned(error),
        };
        self.waits
            .causes()
            .failed(&file_subject(file), &reason, failure);
        reason
    }

    fn transfer_reason(&self, copy: FileCopy, error: &TransferError) -> PendingReason {
        match error {
            TransferError::Refused(refusal) => PendingReason::Refused(refusal.clone()),
            TransferError::File(error) => self.read_reason(copy, error),
        }
    }

    /// Every variant is matched by name, so a new one is classified here
    /// before it can reach a record.
    fn read_reason(&self, copy: FileCopy, error: &FileReadError) -> PendingReason {
        use FileCopy::{Original, Stored};
        match (copy, error) {
            (_, FileReadError::NoStorage) => PendingReason::NoStorage,
            (_, FileReadError::Storage(error)) => {
                if error.failure() == StorageFailure::CatchUpRequired {
                    return PendingReason::Waits(Prerequisite::SequenceCatchUp);
                }
                match self.storage.read().expect("storage lock poisoned").as_ref() {
                    Some(storage) => PendingReason::Storage {
                        provider: storage.config().provider(),
                        failure: error.failure(),
                        answered: error.answered(),
                    },
                    None => PendingReason::NoStorage,
                }
            }
            (_, FileReadError::UserFileMissing { .. }) => PendingReason::FileUnavailable(
                FileMissingReason::Source(FileSourceFailure::Missing),
            ),
            (_, FileReadError::UserFileChanged { .. }) => PendingReason::FileUnavailable(
                FileMissingReason::Source(FileSourceFailure::Changed),
            ),
            (_, FileReadError::Disk(_) | FileReadError::Lock(_)) => {
                PendingReason::Failed(LocalFailure::Disk)
            }
            (_, FileReadError::Database(_) | FileReadError::RangeOutOfBounds { .. }) => {
                PendingReason::Failed(LocalFailure::Database)
            }
            // The attached original failed the checks recorded with it.
            (Original, FileReadError::Integrity { .. }) => PendingReason::FileUnavailable(
                FileMissingReason::Source(FileSourceFailure::Integrity),
            ),
            (Stored, FileReadError::Integrity { cause, .. }) => {
                PendingReason::Refused(crate::Refusal::Decryption {
                    cause: cause.clone(),
                })
            }
            (Stored, FileReadError::Missing { .. }) => {
                PendingReason::FileUnavailable(FileMissingReason::Lost)
            }
            (Stored, FileReadError::UpdateRequired(update)) => {
                PendingReason::UpdateRequired(*update)
            }
            // Only reading the stored copy meets these; on the original
            // they are a local invariant failing.
            (Original, FileReadError::Missing { .. } | FileReadError::UpdateRequired(_)) => {
                PendingReason::Failed(LocalFailure::Database)
            }
        }
    }
}

/// The error a record keeps as its cause: the app-facing read error where
/// there is one.
fn held(error: TransferError) -> Cause {
    match error {
        TransferError::File(error) => Arc::new(error),
        refused @ TransferError::Refused(_) => Arc::new(refused),
    }
}
