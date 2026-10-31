//! The one route from a file's failure to its pending record's reason
//! (§17.7, §17.9). Uploads, pins and local reads all classify here, and the
//! error behind the reason is logged and held as the record's cause
//! (`pending_cause.rs`).
#![deny(clippy::wildcard_enum_match_arm)]

use super::file_error::{FileReadError, TransferError};
use crate::pending_cause::{Cause, Failure};
use crate::{
    files::FilesInner, FileMissingReason, FileSourceFailure, LocalFailure, PendingReason,
    PendingSubject, Prerequisite,
};
use coven_database::{FileLocation, FilePlace, FileRef};
use coven_storage::StorageFailure;
use std::sync::Arc;

/// The copy of a file the failed work used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FileCopy {
    /// The original or copy this device staged: an upload, or a read while
    /// the upload is queued.
    Original,
    /// The stored copy, which a pin downloads.
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

/// The pending subject of this device's upload of stored bytes, which every
/// file over them shares.
pub(super) fn upload_subject(location: &FileLocation) -> PendingSubject {
    PendingSubject::Upload {
        device: location.uploader(),
        file: location.object(),
    }
}

/// The pending subject of this device's downloads of stored bytes, which
/// every file over them shares.
pub(super) fn download_subject(location: &FileLocation) -> PendingSubject {
    PendingSubject::Download {
        device: location.uploader(),
        file: location.object(),
    }
}

/// A stored file's location and content hash. A file only on one device has
/// neither: nothing here reads it from storage (§17.2).
pub(super) fn stored(
    file: &FileRef,
) -> Result<(&FileLocation, coven_crypto::ContentHash), FileReadError> {
    match file.place() {
        FilePlace::Cloud {
            location,
            content_hash,
            ..
        } => Ok((location, *content_hash)),
        FilePlace::DeviceOnly { device } => Err(FileReadError::DeviceOnly {
            file: file.id(),
            device,
        }),
    }
}

impl FilesInner {
    /// The first unmet condition `failure` leaves for the stored bytes
    /// `subject` names. Its error is logged when the reason appears or
    /// changes, and held as the record's cause when no caller receives it.
    pub(super) fn file_failure(
        &self,
        subject: &PendingSubject,
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
        self.waits.causes().failed(subject, &reason, failure);
        reason
    }

    /// The record a failure of this device's upload source leaves (§17.3,
    /// §17.9): a user's original that went missing or changed, on the
    /// upload's move, naming the original; any other, on the upload's
    /// bytes. Its error is logged as `file_failure` logs one.
    pub(super) fn source_record(
        &self,
        location: &FileLocation,
        of: coven_database::MoveId,
        failure: FileFailure<'_>,
    ) -> crate::PendingRecord {
        let read = match &failure {
            FileFailure::Held(TransferError::File(error))
            | FileFailure::Returned(TransferError::File(error)) => Some(error),
            FileFailure::ReturnedRead(error) => Some(*error),
            FileFailure::Held(TransferError::Refused(_))
            | FileFailure::Returned(TransferError::Refused(_)) => None,
        };
        let original = match read {
            Some(FileReadError::UserFileMissing { path, .. }) => {
                Some((path.clone(), FileSourceFailure::Missing))
            }
            Some(FileReadError::UserFileChanged { path, .. }) => {
                Some((path.clone(), FileSourceFailure::Changed))
            }
            _ => None,
        };
        let Some((path, source)) = original else {
            let subject = upload_subject(location);
            let reason = self.file_failure(&subject, FileCopy::Original, failure);
            return crate::PendingRecord::new(subject, reason);
        };
        let subject = PendingSubject::Move { id: of };
        let reason = PendingReason::Original {
            path,
            failure: source,
        };
        let logged = match failure {
            FileFailure::Held(error) => Failure::Held(held(error)),
            FileFailure::Returned(error) => Failure::Returned(error),
            FileFailure::ReturnedRead(error) => Failure::Returned(error),
        };
        self.waits.causes().failed(&subject, &reason, logged);
        crate::PendingRecord::new(subject, reason)
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
            // A file only on another device is refused before any work it
            // could record, so these name a local invariant failing.
            (
                _,
                FileReadError::Database(_)
                | FileReadError::RangeOutOfBounds { .. }
                | FileReadError::DeviceOnly { .. },
            ) => PendingReason::Failed(LocalFailure::Database),
            // The staged original failed the checks recorded with it.
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
