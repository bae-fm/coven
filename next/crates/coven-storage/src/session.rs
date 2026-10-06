use crate::{CloudProvider, ObjectPath, StorageConfig, StorageError};
use coven_crypto::SecretBytes;
use coven_crypto::SecretText;
use serde::{Deserialize, Serialize};

pub(crate) const GOOGLE_DRIVE_PART_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const DROPBOX_PART_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const ONEDRIVE_PART_SIZE: usize = 24 * 320 * 1024;

/// A create-once provider upload recorded by the caller's operation (§16.5, §18).
/// Posted positions cannot have recorded upload sessions.
///
/// Record it before sending parts and after each confirmed part. After a crash,
/// call `resume_upload` to discover bytes accepted before a reply was lost. The
/// bytes of this value include an upload capability: keep them secret. Dropping
/// a session never aborts it; only an explicit abort removes its pending bytes.
/// When a provider discarded the session after publishing but its reply was
/// lost, resumption resets the confirmed offset to zero. Feed the encrypted
/// bytes through `upload_part` again: they are compared to the published object,
/// without uploading them. Only a complete match confirms publication.
#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "RecordedUploadSession")]
pub struct UploadSession {
    pub(crate) location: StorageConfig,
    pub(crate) path: ObjectPath,
    pub(crate) total: u64,
    pub(crate) confirmed: u64,
    pub(crate) part_size: usize,
    pub(crate) state: SessionState,
}

// The wire boundary must construct an unchecked recording before validation.
// Keeping this private makes both Serde and `decode` return validated sessions.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedUploadSession {
    location: StorageConfig,
    path: ObjectPath,
    total: u64,
    confirmed: u64,
    part_size: usize,
    state: SessionState,
}

impl TryFrom<RecordedUploadSession> for UploadSession {
    type Error = StorageError;
    fn try_from(recorded: RecordedUploadSession) -> Result<Self, Self::Error> {
        let session = Self {
            location: recorded.location,
            path: recorded.path,
            total: recorded.total,
            confirmed: recorded.confirmed,
            part_size: recorded.part_size,
            state: recorded.state,
        };
        session.validate()?;
        Ok(session)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum SessionState {
    S3 {
        id: SecretText,
        token: SecretText,
        parts: Vec<S3Part>,
    },
    GoogleDrive {
        url: SecretText,
        file_id: SecretText,
    },
    Dropbox {
        id: SecretText,
    },
    OneDrive {
        url: SecretText,
    },
    CloudKit {
        id: SecretText,
    },
    VerifyPublished,
    Complete,
    #[cfg(any(test, feature = "test-utils"))]
    Memory {
        id: u64,
        complete: bool,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct S3Part {
    pub(crate) number: i32,
    pub(crate) size: u64,
    pub(crate) etag: String,
}

impl UploadSession {
    /// The destination encrypted-object path.
    pub fn path(&self) -> &ObjectPath {
        &self.path
    }
    /// Bytes confirmed by the provider; only these may advance the operation.
    pub fn confirmed_bytes(&self) -> u64 {
        self.confirmed
    }
    /// Total encrypted bytes promised when the session began.
    pub fn total_bytes(&self) -> u64 {
        self.total
    }
    /// Maximum part size. After resuming a partial part, send only the bytes
    /// remaining to the next part boundary (or the end of the object).
    pub fn part_size(&self) -> usize {
        self.part_size
    }
    /// Whether the destination object has been published.
    pub fn is_complete(&self) -> bool {
        match self.state {
            SessionState::Complete => true,
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::Memory { complete, .. } => complete,
            _ => false,
        }
    }
    /// Encode the complete recorded state without printing upload credentials.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        self.validate()?;
        crate::secret_json::encode(self)
    }
    /// Read a recording only if its provider, destination, state and progress agree.
    /// The adapter also checks that it names the adapter's location before any request.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let value: RecordedUploadSession = serde_json::from_slice(bytes)
            .map_err(|error| StorageError::Encoding(Box::new(error)))?;
        value.try_into()
    }
    fn validate(&self) -> Result<(), StorageError> {
        self.location.validate()?;
        if self.path.is_replaceable() {
            return Err(StorageError::InvalidPath);
        }
        if self.part_size == 0
            || self.total == 0
            || self.confirmed > self.total
            || (self.is_complete() && self.confirmed != self.total)
        {
            return Err(StorageError::InvalidPart);
        }
        let provider = self.location.provider();
        match &self.state {
            SessionState::S3 { id, token, parts } if provider == CloudProvider::S3 => {
                nonempty(id)?;
                nonempty(token)?;
                if parts.len() > 10_000 {
                    return Err(StorageError::InvalidPart);
                }
                let mut confirmed = 0u64;
                for (index, part) in parts.iter().enumerate() {
                    confirmed = confirmed
                        .checked_add(part.size)
                        .ok_or(StorageError::InvalidPart)?;
                    if part.number != index as i32 + 1
                        || part.etag.is_empty()
                        || part.size == 0
                        || part.size > self.part_size as u64
                        || (part.size != self.part_size as u64 && confirmed != self.total)
                    {
                        return Err(StorageError::InvalidPart);
                    }
                }
                if confirmed != self.confirmed {
                    return Err(StorageError::InvalidPart);
                }
            }
            SessionState::GoogleDrive { url, file_id }
                if provider == CloudProvider::GoogleDrive =>
            {
                transfer_url(url)?;
                nonempty(file_id)?;
            }
            SessionState::Dropbox { id } if provider == CloudProvider::Dropbox => nonempty(id)?,
            SessionState::OneDrive { url } if provider == CloudProvider::OneDrive => {
                transfer_url(url)?
            }
            SessionState::CloudKit { id } if provider == CloudProvider::CloudKit => nonempty(id)?,
            SessionState::VerifyPublished
                if matches!(provider, CloudProvider::Dropbox | CloudProvider::OneDrive) =>
            {
                if self.confirmed == self.total {
                    return Err(StorageError::InvalidPart);
                }
            }
            SessionState::Complete => {}
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::Memory { id, .. } => {
                return if *id != 0 && self.part_size == 4 {
                    Ok(())
                } else {
                    Err(StorageError::InvalidPart)
                };
            }
            _ => return Err(StorageError::SessionMismatch),
        }
        let valid_size = match provider {
            CloudProvider::S3 => self.part_size == s3_part_size(self.total)?,
            CloudProvider::GoogleDrive => self.part_size == GOOGLE_DRIVE_PART_SIZE,
            CloudProvider::Dropbox => self.part_size == DROPBOX_PART_SIZE,
            CloudProvider::OneDrive => self.part_size == ONEDRIVE_PART_SIZE,
            CloudProvider::CloudKit => true,
        };
        if !valid_size {
            return Err(StorageError::InvalidPart);
        }
        Ok(())
    }
    pub(crate) fn check(&self, location: &StorageConfig) -> Result<(), StorageError> {
        self.validate()?;
        if &self.location != location {
            return Err(StorageError::SessionMismatch);
        }
        Ok(())
    }
    pub(crate) fn end_of_part(&self, len: usize) -> Result<u64, StorageError> {
        let end = self
            .confirmed
            .checked_add(len as u64)
            .ok_or(StorageError::InvalidPart)?;
        if self.is_complete()
            || len == 0
            || len > self.part_size
            || end > self.total
            || (end != self.total && end % self.part_size as u64 != 0)
        {
            return Err(StorageError::InvalidPart);
        }
        Ok(end)
    }
}

pub(crate) fn s3_part_size(total: u64) -> Result<usize, StorageError> {
    let unit = 8 * 1024 * 1024;
    if total == 0 || total > 10_000 * 5 * 1024u64.pow(3) {
        return Err(StorageError::InvalidPart);
    }
    usize::try_from(total.div_ceil(10_000).div_ceil(unit) * unit)
        .map_err(|_| StorageError::InvalidPart)
}
fn nonempty(id: &SecretText) -> Result<(), StorageError> {
    if id.as_str().is_empty() {
        return Err(StorageError::SessionMismatch);
    }
    Ok(())
}
fn transfer_url(value: &SecretText) -> Result<(), StorageError> {
    let url = url::Url::parse(value.as_str()).map_err(|_| StorageError::SessionMismatch)?;
    if !matches!(url.scheme(), "https" | "http")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(StorageError::SessionMismatch);
    }
    Ok(())
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
