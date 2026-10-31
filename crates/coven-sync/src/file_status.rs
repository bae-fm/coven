//! A file's availability: for a file this device uploads, from local facts
//! alone; for a file only on another device, from its record; for a stored
//! file, from one storage observation of its path. Never written to a record
//! (§17.6).

use crate::FileSourceFailure;
use crate::{
    files::{file_failure::upload_subject, Files, FilesInner},
    FileMissingReason, FileReadError, PendingReason, PendingRecord,
};
use coven_database::{DbError, FileLocation, FilePlace, FileRef};
use coven_format::file::FileHeader;
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, StoredObject};

/// A file's current availability (E8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileStatus {
    /// Storage holds the complete file at its fixed path.
    Available,
    /// This device is uploading it; until then it is on this device only.
    Uploading,
    /// It is on one device only, with no upload: another device, which may
    /// be gone, and then nothing here can read it, or this one, which reads
    /// it from its bytes here. Nothing waits for it (§17.3).
    DeviceOnly {
        /// The device that has it.
        device: DeviceId,
    },
    /// Unreadable from storage, with the reason.
    Missing {
        /// The device that uploaded, or is uploading, it.
        device: DeviceId,
        /// Why.
        reason: FileMissingReason,
    },
}

/// A file this device uploads: its upload, where its bytes go, its move,
/// and its source's recorded failure.
pub(crate) struct Uploading {
    pub(crate) upload: i64,
    pub(crate) location: FileLocation,
    pub(crate) of: coven_database::MoveId,
    pub(crate) failure: Option<FileSourceFailure>,
}

/// What this device knows locally about a file: `None` for a stored one,
/// `Some(report)` for one it uploads, with its source failure if one is
/// recorded (ST `Files.Local`).
pub(crate) type Local = Option<Option<FileSourceFailure>>;

/// §17.6, ST `Files.status`. A file this device uploads is judged locally.
/// For a stored file, presence wins; a failed observation is never absence,
/// and a confirmed absence is lost, whoever uploaded it.
pub(crate) fn status<E>(
    uploader: DeviceId,
    remote: impl FnOnce() -> Result<bool, E>,
    here: Local,
) -> Result<FileStatus, E> {
    match here {
        Some(None) => Ok(FileStatus::Uploading),
        Some(Some(failure)) => {
            Ok(FileStatus::Missing { device: uploader, reason: FileMissingReason::Source(failure) })
        }
        None => Ok(if remote()? {
            FileStatus::Available
        } else {
            FileStatus::Missing { device: uploader, reason: FileMissingReason::Lost }
        }),
    }
}

impl Files {
    /// The file as its record now stands: a file this device uploads is
    /// judged from local facts with no storage request, one only on another
    /// device from its record, and a stored one by one fresh single-object
    /// status call for its fixed path. Network failures and denied access are
    /// errors, not absence.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError> {
        self.inner.check_open()?;
        let file = self.inner.current(file).await?;
        let location = match file.place() {
            FilePlace::DeviceOnly { device } => {
                return match self.inner.local(&file).await? {
                    Some(here) => status::<FileReadError>(
                        here.location.uploader(),
                        || Ok(false),
                        Some(here.failure),
                    ),
                    None => match self.inner.unreadable(&file, device).await {
                        FileReadError::DeviceOnly { .. } => Ok(FileStatus::DeviceOnly { device }),
                        error => Err(error),
                    },
                };
            }
            FilePlace::Cloud { location, .. } => location,
        };
        let storage = self
            .inner
            .storage
            .read()
            .expect("storage lock poisoned")
            .clone()
            .ok_or(FileReadError::NoStorage)?;
        let path =
            ObjectPath::file(self.inner.directory.id(), location.uploader(), location.object());
        let remote = match storage.status(&path).await? {
            Some(object) => complete(&file, &object).map(|()| true),
            None => Ok(false),
        };
        status(location.uploader(), || remote, None)
    }
}

impl FilesInner {
    /// The file as its record now stands. A file never changes its bytes, so
    /// a reference taken earlier, while the file uploaded, reads the stored
    /// file once it is stored; one whose record went reads as it was named.
    pub(crate) async fn current(&self, file: &FileRef) -> Result<FileRef, FileReadError> {
        match self.database.file_ref(file.id()).await {
            Ok(current) => Ok(current),
            Err(DbError::FileAbsent { .. }) => Ok(file.clone()),
            Err(error) => Err(error.into()),
        }
    }

    /// Why a file on one device only can't be read here: this device has no
    /// record of it any more, or it is only on that device.
    pub(crate) async fn unreadable(&self, file: &FileRef, device: DeviceId) -> FileReadError {
        match self.database.file_ref(file.id()).await {
            Err(error @ DbError::FileAbsent { .. }) => error.into(),
            _ => FileReadError::DeviceOnly { file: file.id(), device },
        }
    }

    /// The upload of a file this device uploads, with the source failure
    /// recorded about it: its original's, on its move's record, or coven's
    /// copy's, on its upload's; `None` for any other file.
    pub(crate) async fn local(&self, file: &FileRef) -> Result<Option<Uploading>, FileReadError> {
        let evidence = self
            .database
            .local_evidence::<PendingRecord>(file, upload_subject, |id| {
                crate::PendingSubject::Move { id }
            })
            .await?;
        Ok(evidence.map(|evidence| {
            let original = evidence.moving.and_then(|record| match record.reason {
                PendingReason::Original { path, failure }
                    if evidence.original.as_ref() == Some(&path) =>
                {
                    Some(failure)
                }
                _ => None,
            });
            let copy = evidence.record.and_then(|record| match record.reason {
                PendingReason::FileUnavailable(FileMissingReason::Source(failure)) => Some(failure),
                _ => None,
            });
            Uploading {
                upload: evidence.upload,
                location: evidence.location,
                of: evidence.of,
                failure: original.or(copy),
            }
        }))
    }

    /// The typed read failure for stored bytes storage confirms are absent
    /// at their path: lost (§17.6).
    pub(crate) fn absence_error(&self, file: &FileRef, location: &FileLocation) -> FileReadError {
        FileReadError::Missing { id: file.id().to_string(), device: location.uploader() }
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
    let expected = FileHeader::new(file.size())
        .encrypted_size()
        .map_err(|_| FileReadError::Integrity { id: file.id().to_string(), cause: None })?;
    if object.size == expected {
        Ok(())
    } else {
        Err(FileReadError::Integrity { id: file.id().to_string(), cause: None })
    }
}
