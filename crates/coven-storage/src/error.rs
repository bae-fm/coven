use crate::{providers::OAuthError, CloudProvider};
use coven_crypto::custody::KeyError;

/// Failures the app can distinguish and act on (§20.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StorageFailure {
    /// No route to the provider, a timeout, or an interrupted response.
    Network,
    /// Credentials were refused or must be refreshed.
    Authentication,
    /// The account is signed in but cannot perform this operation.
    PermissionDenied,
    /// The requested object is absent.
    NotFound,
    /// A create-once path is occupied.
    AlreadyExists,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 endpoint or signing region is wrong.
    RegionMismatch,
    /// The provider has no space left.
    QuotaExceeded,
    /// The provider requests a later retry.
    RateLimited,
    /// The request's configuration is invalid.
    InvalidConfiguration,
    /// The provider refused the request for another reason.
    Refused,
    /// The response or recorded session is malformed.
    Protocol,
    /// An object path is outside the store's layout.
    InvalidPath,
    /// A range is empty, reversed or beyond the object's end.
    InvalidRange,
    /// Only the account holding the store may change its sharing.
    NotStoreOwner,
    /// Dropbox cannot upgrade a pending viewer without the recipient's account id.
    AccountIdUnavailable,
    /// The invite belongs to another provider location.
    InvitationMismatch,
    /// The session belongs to another provider or location.
    SessionMismatch,
    /// The provider no longer retains the recorded upload.
    SessionExpired,
    /// A part disagrees with the session's offset, size or alignment.
    InvalidPart,
    /// A posted-positions replacement exceeds this provider's single-request limit.
    SingleRequestTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The adapter's single-request limit in bytes.
        limit: u64,
    },
    /// Parsing recorded provider data failed.
    Encoding,
    /// Persisting provider settings failed.
    File,
    /// This installation has no member keys in custody.
    MemberKeysMissing,
}

/// A storage error preserves its cause without converting it into text.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// A classified failure with no underlying native error.
    #[error("{0:?}")]
    Failure(StorageFailure),
    /// A classified local failure with its diagnostic or native cause.
    #[error("{failure:?}: {source}")]
    Caused {
        /// The failure the app can act on.
        failure: StorageFailure,
        /// The diagnostic or original error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A provider failure, classified for the app, with its original cause.
    #[error("{provider:?}: {failure:?}: {source}")]
    Provider {
        /// Provider that failed.
        provider: CloudProvider,
        /// Actionable classification.
        failure: StorageFailure,
        /// Original transport, SDK or bridge error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Cleanup failed too; neither cause is hidden.
    #[error("{operation}; cleanup also failed: {cleanup}")]
    Cleanup {
        /// The original failure.
        #[source]
        operation: Box<StorageError>,
        /// Failure removing the setup test object or unfinished upload.
        cleanup: Box<StorageError>,
    },
}

impl StorageError {
    /// What the app can do about this failure.
    pub fn failure(&self) -> StorageFailure {
        match self {
            Self::Provider { failure, .. } => *failure,
            Self::Failure(failure) | Self::Caused { failure, .. } => *failure,
            Self::Cleanup { operation, .. } => operation.failure(),
        }
    }

    /// Only transport interruptions and provider throttling invite automatic retry.
    pub fn retryable(&self) -> bool {
        matches!(
            self.failure(),
            StorageFailure::Network | StorageFailure::RateLimited
        )
    }
}

/// The provider operation checked before setup commits anything (E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageCheck {
    /// Create a fresh sealed object.
    Create,
    /// Refuse a second create at the same path.
    CreateOnce,
    /// Read the original bytes in full.
    Read,
    /// Read exactly an interior byte range.
    ReadRange,
    /// Include the object and its size in a listing.
    List,
    /// Delete the object and confirm it is absent.
    Delete,
}

/// A failed setup commits no local storage settings or credentials.
#[derive(Debug, thiserror::Error)]
pub enum StorageSetupError {
    /// A provider check failed; cleanup failures retain both causes.
    #[error("storage check {check:?}: {source}")]
    ProviderCheck {
        /// The operation that failed.
        check: StorageCheck,
        /// The original failure, including cleanup if that also failed.
        #[source]
        source: StorageError,
    },
    /// The location already contains another store.
    #[error("storage location is occupied")]
    LocationOccupied,
    /// A classified storage failure.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Sign-in failed or was cancelled before setup could connect.
    #[error(transparent)]
    OAuth(#[from] OAuthError),
    /// The facade could not commit to key custody.
    #[error("secure storage: {0}")]
    SecureStorage(#[from] KeyError),
    /// A local setup step failed, retaining its cause.
    #[error("storage setup: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl StorageFailure {
    /// Retain the diagnostic or native cause alongside this classification.
    pub fn with_source(
        self,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> StorageError {
        StorageError::Caused {
            failure: self,
            source: source.into(),
        }
    }
}

impl From<StorageFailure> for StorageError {
    fn from(failure: StorageFailure) -> Self {
        Self::Failure(failure)
    }
}

impl From<coven_foundation::files::FileError> for StorageError {
    fn from(error: coven_foundation::files::FileError) -> Self {
        StorageFailure::File.with_source(error)
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;

impl From<coven_format::path::PathError> for StorageError {
    fn from(error: coven_format::path::PathError) -> Self {
        StorageFailure::InvalidPath.with_source(error)
    }
}
