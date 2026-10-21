//! A file's availability, from one storage observation of the path its row
//! names, or for this device's own pending attachment from local facts alone;
//! never written to a row (§17.4).

use crate::FileSourceFailure;
use crate::{
    files::{Files, FilesInner},
    FileMissingReason, FileReadError, PendingReason, PendingRecord, PendingSubject,
};
use coven_database::FileRef;
use coven_format::file::FileHeader;
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, StoredObject};

/// A file's current availability (E8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileStatus {
    /// Storage holds the complete file at its fixed path.
    Available,
    /// This device's own pending attachment, still queued here; no other
    /// device has its reference yet (§17.2).
    Uploading,
    /// Unreadable from storage, with the reason.
    Missing {
        /// The device that uploaded, or is uploading, it.
        device: DeviceId,
        /// Why.
        reason: FileMissingReason,
    },
}

/// What this device knows locally about a file: `None` for one its row
/// names, `Some(report)` for its own pending attachment, with its source
/// failure if one is recorded (ST `Files.Local`).
pub(crate) type Local = Option<Option<FileSourceFailure>>;

/// §17.4, ST `Files.status`. This device's pending attachment is judged
/// locally. For a file a row names, presence wins; a failed observation is
/// never absence, and a confirmed absence is lost, whoever uploaded it.
pub(crate) fn status<E>(
    uploader: DeviceId,
    remote: impl FnOnce() -> Result<bool, E>,
    here: Local,
) -> Result<FileStatus, E> {
    match here {
        Some(None) => Ok(FileStatus::Uploading),
        Some(Some(failure)) => Ok(FileStatus::Missing {
            device: uploader,
            reason: FileMissingReason::Source(failure),
        }),
        None => Ok(if remote()? {
            FileStatus::Available
        } else {
            FileStatus::Missing {
                device: uploader,
                reason: FileMissingReason::Lost,
            }
        }),
    }
}

impl Files {
    /// Checks the reference against its row, then judges this device's own
    /// pending attachment locally, with no storage request, or makes one
    /// fresh single-object status call for the fixed path of a file a row
    /// names. Network failures and denied access are errors, not absence.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError> {
        self.inner.check_open()?;
        self.inner.database.validate(file).await?;
        let here = self.inner.local(file).await?;
        if here.is_some() {
            return status::<FileReadError>(file.uploader(), || Ok(false), here);
        }
        let storage = self
            .inner
            .storage
            .read()
            .expect("storage lock poisoned")
            .clone()
            .ok_or(FileReadError::NoStorage)?;
        let path = ObjectPath::file(self.inner.directory.id(), file.uploader(), file.id());
        let remote = match storage.status(&path).await? {
            Some(object) => complete(file, &object).map(|()| true),
            None => Ok(false),
        };
        status(file.uploader(), || remote, None)
    }
}

impl FilesInner {
    /// Whether the file is this device's own pending attachment, with the
    /// source failure recorded about it.
    async fn local(&self, file: &FileRef) -> Result<Local, FileReadError> {
        let (pending, record) = self
            .database
            .local_evidence::<PendingRecord>(
                file,
                PendingSubject::File {
                    device: file.uploader(),
                    file: file.id(),
                },
            )
            .await?;
        if !pending {
            return Ok(None);
        }
        Ok(Some(record.and_then(|record| match record.reason {
            PendingReason::FileUnavailable(FileMissingReason::Source(failure)) => Some(failure),
            _ => None,
        })))
    }

    /// The typed read failure for remote bytes storage confirms are absent
    /// at the path a row names: lost (§17.4).
    pub(crate) fn absence_error(&self, file: &FileRef) -> FileReadError {
        FileReadError::Missing {
            id: file.id().to_string(),
            device: file.uploader(),
        }
    }
}

/// An object at the fixed path counts as the file only at the file's complete
/// encrypted size; anything else there is damage, not absence. Status and every
/// ranged read check it, so they agree (ST `read_agrees_with_status`).
#[expect(
    clippy::map_err_ignore,
    reason = "a size whose encrypted layout overflows fails the integrity check"
)]
pub(crate) fn complete(file: &FileRef, object: &StoredObject) -> Result<(), FileReadError> {
    let expected = FileHeader::new(file.plaintext_size())
        .encrypted_size()
        .map_err(|_| FileReadError::Integrity {
            id: file.id().to_string(),
            cause: None,
        })?;
    if object.size == expected {
        Ok(())
    } else {
        Err(FileReadError::Integrity {
            id: file.id().to_string(),
            cause: None,
        })
    }
}
