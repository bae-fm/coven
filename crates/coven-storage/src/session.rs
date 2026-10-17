use crate::{CloudProvider, ObjectPath, StorageConfig, StorageError, StorageFailure};
use coven_crypto::SecretBytes;
use coven_crypto::SecretText;
use serde::{Deserialize, Serialize};

pub(crate) const CLOUDKIT_PART_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const DROPBOX_PART_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const ONEDRIVE_PART_SIZE: usize = 24 * 320 * 1024;

/// A create-once provider upload recorded by the caller's operation (§17.7, §11).
/// The floor uses conditional replacement and cannot have a create-once session.
///
/// Record it before sending parts and after each call, including failed calls:
/// a publication conflict invalidates the native session even if abort fails.
/// After a crash,
/// call `resume_upload` to discover bytes accepted before a reply was lost. The
/// bytes of this value include an upload capability: keep them secret. Dropping
/// a session never aborts it; explicit abort abandons its pending bytes.
/// On copy profiles the recorded token names the temporary object as well as
/// identifying publication, and the session is recorded before the native
/// multipart upload exists: the first part initiates it. Abort deletes it even after native multipart completion; failed
/// copy or cleanup keeps the recording incomplete so it can be retried.
/// OneDrive records its temporary UUID through receipt, copying, verification
/// and deletion; native transfer completion alone is not publication.
/// Providers may retain closed session data until their expiry, without publishing it.
/// When recovery requires complete-byte comparison with a published object,
/// resumption resets the confirmed offset to zero. Feed the encrypted
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
    S3Restart {
        id: SecretText,
        token: SecretText,
    },
    /// A session whose upload name is recorded before the native multipart
    /// upload is initiated, so no upload exists that its recording does not
    /// name (Appendix E.10).
    S3Planned {
        token: SecretText,
        /// True only while this process knows no initiation was attempted.
        /// A restored journal cannot establish that fact.
        #[serde(skip)]
        unstarted: bool,
    },
    Dropbox {
        id: SecretText,
    },
    OneDrive {
        temporary: uuid::Uuid,
        upload: OneDriveUpload,
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

/// Native transfer and publication are separate; every phase retains the
/// temporary's identity until its deletion has succeeded.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum OneDriveUpload {
    Receiving { url: SecretText },
    Uploaded { item: String },
    Copying { monitor: SecretText },
    // The copy definitively failed; cleanup may already have removed its source.
    Refused,
    VerifyPublished,
    Published,
}

impl S3Part {
    pub(crate) fn follows(&self, index: usize, end: u64, total: u64, part_size: usize) -> bool {
        self.number as usize == index + 1
            && !self.etag.is_empty()
            && self.size != 0
            && self.size <= part_size as u64
            && end <= total
            && (self.size == part_size as u64 || end == total)
    }
}

impl UploadSession {
    /// Whether this recorded transfer belongs to the connected location.
    pub fn is_at(&self, location: &StorageConfig) -> bool {
        &self.location == location
    }
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
    /// Whether publication and provider upload cleanup have succeeded.
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
    /// Validation covers the location, state variant, nonempty identifiers, part
    /// counts and sizes, and confirmed byte offset; replaceable paths are refused.
    /// The adapter also checks that it names the adapter's location before any request.
    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let value: RecordedUploadSession = serde_json::from_slice(bytes)
            .map_err(|error| StorageFailure::Encoding.with_source(error))?;
        value.try_into()
    }
    fn validate(&self) -> Result<(), StorageError> {
        self.location.validate()?;
        if self.path.is_replaceable() {
            return Err(StorageFailure::InvalidPath.into());
        }
        if self.part_size == 0
            || self.total == 0
            || self.confirmed > self.total
            || (self.is_complete() && self.confirmed != self.total)
        {
            return Err(StorageFailure::InvalidPart.into());
        }
        let provider = self.location.provider();
        match &self.state {
            SessionState::S3 { id, token, parts } if provider == CloudProvider::S3 => {
                nonempty(id)?;
                nonempty(token)?;
                if parts.len() > 10_000 {
                    return Err(StorageFailure::InvalidPart.into());
                }
                let mut confirmed = 0u64;
                for (index, part) in parts.iter().enumerate() {
                    confirmed = confirmed
                        .checked_add(part.size)
                        .ok_or(StorageFailure::InvalidPart)?;
                    if !part.follows(index, confirmed, self.total, self.part_size) {
                        return Err(StorageFailure::InvalidPart.into());
                    }
                }
                if confirmed != self.confirmed {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::S3Restart { id, token } if provider == CloudProvider::S3 => {
                nonempty(id)?;
                nonempty(token)?;
                if self.confirmed != 0 {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::S3Planned { token, .. } if provider == CloudProvider::S3 => {
                nonempty(token)?;
                if self.confirmed != 0 {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::Dropbox { id } if provider == CloudProvider::Dropbox => nonempty(id)?,
            SessionState::OneDrive { upload, .. } if provider == CloudProvider::OneDrive => {
                match upload {
                    OneDriveUpload::Receiving { url } => {
                        transfer_url(url)?;
                        if self.confirmed == self.total {
                            return Err(StorageFailure::InvalidPart.into());
                        }
                    }
                    OneDriveUpload::Uploaded { item } => {
                        if item.is_empty() || self.confirmed != self.total {
                            return Err(StorageFailure::InvalidPart.into());
                        }
                    }
                    OneDriveUpload::Copying { monitor } => {
                        transfer_url(monitor)?;
                        if self.confirmed != self.total {
                            return Err(StorageFailure::InvalidPart.into());
                        }
                    }
                    OneDriveUpload::Published | OneDriveUpload::Refused
                        if self.confirmed != self.total =>
                    {
                        return Err(StorageFailure::InvalidPart.into());
                    }
                    OneDriveUpload::VerifyPublished if self.confirmed == self.total => {
                        return Err(StorageFailure::InvalidPart.into());
                    }
                    OneDriveUpload::VerifyPublished
                    | OneDriveUpload::Published
                    | OneDriveUpload::Refused => {}
                }
            }
            SessionState::CloudKit { id } if provider == CloudProvider::CloudKit => {
                check_cloudkit_id(id.as_str())?
            }
            SessionState::VerifyPublished if provider == CloudProvider::Dropbox => {
                if self.confirmed == self.total {
                    return Err(StorageFailure::InvalidPart.into());
                }
            }
            SessionState::Complete => {}
            #[cfg(any(test, feature = "test-utils"))]
            SessionState::Memory { id, .. } => {
                return if *id != 0 {
                    Ok(())
                } else {
                    Err(StorageFailure::InvalidPart.into())
                };
            }
            _ => return Err(StorageFailure::SessionMismatch.into()),
        }
        let valid_size = match provider {
            CloudProvider::S3 => self.part_size == s3_part_size(self.total)?,
            CloudProvider::Dropbox => self.part_size == DROPBOX_PART_SIZE,
            CloudProvider::OneDrive => self.part_size == ONEDRIVE_PART_SIZE,
            CloudProvider::CloudKit => self.part_size == CLOUDKIT_PART_SIZE,
        };
        if !valid_size {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(())
    }
    pub(crate) fn check(&self, location: &StorageConfig) -> Result<(), StorageError> {
        self.validate()?;
        if &self.location != location {
            return Err(StorageFailure::SessionMismatch.into());
        }
        Ok(())
    }
    pub(crate) fn end_of_part(&self, len: usize) -> Result<u64, StorageError> {
        let end = self
            .confirmed
            .checked_add(len as u64)
            .ok_or(StorageFailure::InvalidPart)?;
        if self.is_complete()
            || len == 0
            || len > self.part_size
            || end > self.total
            || (end != self.total && end % self.part_size as u64 != 0)
        {
            return Err(StorageFailure::InvalidPart.into());
        }
        Ok(end)
    }
}

pub(crate) fn s3_part_size(total: u64) -> Result<usize, StorageError> {
    let unit = 8 * 1024 * 1024;
    if total == 0 || total > 10_000 * 5 * 1024u64.pow(3) {
        return Err(StorageFailure::InvalidPart.into());
    }
    usize::try_from(total.div_ceil(10_000).div_ceil(unit) * unit)
        .map_err(|error| StorageFailure::InvalidPart.with_source(error))
}
pub(crate) fn check_cloudkit_id(value: &str) -> Result<(), StorageError> {
    let id = uuid::Uuid::parse_str(value)
        .map_err(|error| StorageFailure::SessionMismatch.with_source(error))?;
    if id.to_string() != value {
        return Err(StorageFailure::SessionMismatch.into());
    }
    Ok(())
}
fn nonempty(id: &SecretText) -> Result<(), StorageError> {
    if id.as_str().is_empty() {
        return Err(StorageFailure::SessionMismatch.into());
    }
    Ok(())
}
fn transfer_url(value: &SecretText) -> Result<(), StorageError> {
    let url = url::Url::parse(value.as_str())
        .map_err(|error| StorageFailure::SessionMismatch.with_source(error))?;
    if !crate::web_url::is_web_url(&url) || url.fragment().is_some() {
        return Err(StorageFailure::SessionMismatch.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
