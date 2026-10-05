use crate::{ObjectPath, StorageConfig, StorageError};
use coven_crypto::SecretBytes;
use coven_crypto::SecretText;
use serde::{Deserialize, Serialize};

/// A provider upload recorded by the caller's operation (§16.5, §18).
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
#[serde(deny_unknown_fields)]
pub struct UploadSession {
    pub(crate) location: StorageConfig,
    pub(crate) path: ObjectPath,
    pub(crate) total: u64,
    pub(crate) confirmed: u64,
    pub(crate) part_size: usize,
    pub(crate) state: SessionState,
}

#[derive(Clone, Serialize, Deserialize)]
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
    },
}

#[derive(Clone, Serialize, Deserialize)]
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
        matches!(self.state, SessionState::Complete)
    }
    /// Encode the complete recorded state without printing upload credentials.
    pub fn encode(&self) -> Result<SecretBytes, StorageError> {
        crate::secret_json::encode(self)
    }
    /// Read a recorded session; the provider checks its location before using it.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| StorageError::Encoding(Box::new(error)))?;
        value.location.validate()?;
        if value.part_size == 0
            || value.total == 0
            || value.confirmed > value.total
            || (value.is_complete() && value.confirmed != value.total)
        {
            return Err(StorageError::InvalidPart);
        }
        Ok(value)
    }
    pub(crate) fn check(&self, location: &StorageConfig) -> Result<(), StorageError> {
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

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
