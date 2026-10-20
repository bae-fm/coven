//! Remote file availability, derived from one storage observation and the
//! uploader's applied state and locally observed source failures; never written to a row (§17.4).

use crate::FileSourceFailure;
use crate::{
    files::{Files, FilesInner},
    FileMissingReason, FileReadError, PendingReason, PendingRecord, PendingSubject,
};
use coven_database::{DeviceState, FileRef};
use coven_format::file::FileHeader;
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, StoredObject};

/// Current remote availability of a file (E8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileStatus {
    /// Storage holds the complete file at its fixed path.
    Available,
    /// Confirmed absent, with an active uploader and no local source failure.
    Uploading {
        /// The device responsible for uploading it.
        device: DeviceId,
    },
    /// Confirmed absent, with the reason its uploader cannot supply it.
    Missing {
        /// The device responsible for uploading it.
        device: DeviceId,
        /// Why it cannot.
        reason: FileMissingReason,
    },
}

/// Why an absent file cannot arrive, if it cannot. The uploader's state comes
/// before a locally observed source failure.
fn missing_reason(
    state: DeviceState,
    report: Option<FileSourceFailure>,
) -> Option<FileMissingReason> {
    match (state, report) {
        (DeviceState::Removed, _) => Some(FileMissingReason::DeviceRemoved),
        (DeviceState::Replaced, _) => Some(FileMissingReason::DeviceReplaced),
        (DeviceState::Active, Some(failure)) => Some(FileMissingReason::Source(failure)),
        (DeviceState::Active, None) => None,
    }
}

/// §17.4, ST `Files.status`. Presence wins; a failed observation is never
/// absence, and the evidence only explains a confirmed absence.
pub(crate) fn status<E>(
    uploader: DeviceId,
    remote: Result<bool, E>,
    state: DeviceState,
    report: Option<FileSourceFailure>,
) -> Result<FileStatus, E> {
    if remote? {
        return Ok(FileStatus::Available);
    }
    Ok(match missing_reason(state, report) {
        Some(reason) => FileStatus::Missing {
            device: uploader,
            reason,
        },
        None => FileStatus::Uploading { device: uploader },
    })
}

impl Files {
    /// Make one fresh single-object status call for the file's fixed path,
    /// then combine it with the uploader's current state and locally observed
    /// source failure. Checks the reference against its row first.
    /// Network failures and denied access are errors, not absence.
    pub async fn file_status(&self, file: &FileRef) -> Result<FileStatus, FileReadError> {
        self.inner.check_open()?;
        self.inner.database.validate(file).await?;
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
        let (state, report) = self.inner.evidence(file).await?;
        status(file.uploader(), remote, state, report)
    }
}

impl FilesInner {
    /// The uploader's applied state and, on its own installation, source failure.
    async fn evidence(
        &self,
        file: &FileRef,
    ) -> Result<(DeviceState, Option<FileSourceFailure>), FileReadError> {
        let (state, record) = self
            .database
            .uploader_evidence::<PendingRecord>(
                file.uploader(),
                PendingSubject::File {
                    device: file.uploader(),
                    file: file.id(),
                },
            )
            .await?;
        let report = record.and_then(|record| match record.reason {
            PendingReason::FileUnavailable(FileMissingReason::Source(failure)) => Some(failure),
            _ => None,
        });
        Ok((state, report))
    }

    /// The typed read failure for remote bytes storage confirms are absent:
    /// still uploading, or missing with the uploader's reason (§17.4).
    pub(crate) async fn absence_error(&self, file: &FileRef) -> FileReadError {
        let (state, report) = match self.evidence(file).await {
            Ok(evidence) => evidence,
            Err(error) => return error,
        };
        let id = file.id().to_string();
        match missing_reason(state, report) {
            Some(reason) => FileReadError::Missing {
                id,
                device: file.uploader(),
                reason,
            },
            None => FileReadError::Uploading {
                id,
                device: file.uploader(),
            },
        }
    }
}

/// An object at the fixed path counts as the file only at the file's complete
/// encrypted size; anything else there is damage, not absence. Status and every
/// ranged read check it, so they agree (ST `read_agrees_with_status`).
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
