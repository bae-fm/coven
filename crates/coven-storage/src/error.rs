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
}

/// A storage error preserves its cause without converting it into text.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
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
    /// Invalid location settings.
    #[error("invalid storage configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// An object path is outside the store's layout.
    #[error("invalid storage path")]
    InvalidPath,
    /// A range is empty, reversed or beyond the object's end.
    #[error("invalid byte range")]
    InvalidRange,
    /// An object does not exist.
    #[error("object not found")]
    NotFound,
    /// The path already holds an object; creation never replaces it.
    #[error("object already exists")]
    AlreadyExists,
    /// Only the account holding the store may change its sharing.
    #[error("sharing requires the store owner's account")]
    NotStoreOwner,
    /// Dropbox cannot upgrade a pending viewer without the recipient's account id.
    #[error("the provider has not supplied the account id needed to upgrade this invitation")]
    AccountIdUnavailable,
    /// The invite belongs to another provider location.
    #[error("invitation belongs to another location")]
    InvitationMismatch,
    /// The session belongs to another provider or location.
    #[error("upload session belongs to another location")]
    SessionMismatch,
    /// The provider no longer retains the recorded upload.
    #[error("upload session expired")]
    SessionExpired,
    /// A part disagrees with the session's offset, size or alignment.
    #[error("invalid upload part")]
    InvalidPart,
    /// A posted-positions replacement exceeds this provider's single-request limit.
    #[error("object of {size} bytes exceeds the single-request limit of {limit}")]
    SingleRequestTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The adapter's single-request limit in bytes.
        limit: u64,
    },
    /// A response violates the provider's protocol.
    #[error("invalid provider response: {0}")]
    Protocol(&'static str),
    /// Parsing recorded data failed.
    #[error("invalid storage data: {0}")]
    Encoding(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Persisting provider settings failed.
    #[error("storage settings: {0}")]
    File(#[from] coven_foundation::files::FileError),
    /// A test injected this classified failure.
    #[cfg(any(test, feature = "test-utils"))]
    #[error("injected storage failure: {0:?}")]
    Injected(StorageFailure),
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
            Self::InvalidConfiguration(_)
            | Self::InvalidPath
            | Self::InvalidRange
            | Self::InvalidPart
            | Self::SingleRequestTooLarge { .. } => StorageFailure::InvalidConfiguration,
            Self::NotFound | Self::SessionExpired => StorageFailure::NotFound,
            Self::AlreadyExists => StorageFailure::AlreadyExists,
            Self::NotStoreOwner => StorageFailure::PermissionDenied,
            Self::AccountIdUnavailable => StorageFailure::Refused,
            Self::InvitationMismatch
            | Self::SessionMismatch
            | Self::Protocol(_)
            | Self::Encoding(_)
            | Self::File(_) => StorageFailure::Protocol,
            Self::Cleanup { operation, .. } => operation.failure(),
            #[cfg(any(test, feature = "test-utils"))]
            Self::Injected(failure) => *failure,
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

/// Setup failures shown by the app (E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageSetupFailure {
    /// A provider check failed before setup could commit.
    ProviderCheck {
        /// The operation that failed.
        check: StorageCheck,
        /// The classified cause; contract violations are `Protocol`.
        failure: StorageFailure,
    },
    /// Provider credentials were refused.
    Authentication,
    /// The account cannot use this location.
    PermissionDenied,
    /// The bucket, folder or zone is missing.
    ContainerNotFound,
    /// The configured S3 region is wrong.
    RegionMismatch,
    /// Storage has no room left.
    QuotaExceeded,
    /// Location settings are invalid.
    InvalidConfiguration,
    /// The location holds another store.
    LocationOccupied,
    /// The provider cannot be reached.
    Network,
    /// The member's keys are absent.
    MemberKeysMissing,
    /// Key custody could not commit credentials or keys.
    SecureStorage,
    /// Another failure, with its cause in the setup error.
    Internal,
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
    /// The member's keys have not been created or restored.
    #[error("member keys are missing")]
    MemberKeysMissing,
    /// The facade could not commit to key custody.
    #[error("secure storage: {0}")]
    SecureStorage(#[from] KeyError),
    /// A local setup step failed, retaining its cause.
    #[error("storage setup: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl StorageSetupError {
    /// The exact setup classification from E5.
    pub fn failure(&self) -> StorageSetupFailure {
        use StorageSetupFailure as S;
        match self {
            Self::ProviderCheck { check, source } => S::ProviderCheck {
                check: *check,
                failure: source.failure(),
            },
            Self::LocationOccupied => S::LocationOccupied,
            Self::MemberKeysMissing => S::MemberKeysMissing,
            Self::SecureStorage(_) => S::SecureStorage,
            Self::Internal(_) => S::Internal,
            Self::OAuth(error) => match error {
                OAuthError::Unavailable(_)
                | OAuthError::RequestMismatch
                | OAuthError::InvalidRedirect => S::InvalidConfiguration,
                OAuthError::StateMismatch
                | OAuthError::Denied
                | OAuthError::MissingCode
                | OAuthError::Cancelled
                | OAuthError::Timeout
                | OAuthError::Expired
                | OAuthError::Reauthorize => S::Authentication,
                OAuthError::InvalidExpiry | OAuthError::Io(_) => S::Internal,
                OAuthError::Storage(error) => Self::storage_failure(error),
            },
            Self::Storage(error) => Self::storage_failure(error),
        }
    }

    fn storage_failure(error: &StorageError) -> StorageSetupFailure {
        use StorageFailure as F;
        use StorageSetupFailure as S;
        match error.failure() {
            F::Authentication => S::Authentication,
            F::PermissionDenied | F::Refused => S::PermissionDenied,
            F::ContainerNotFound | F::NotFound => S::ContainerNotFound,
            F::RegionMismatch => S::RegionMismatch,
            F::QuotaExceeded => S::QuotaExceeded,
            F::InvalidConfiguration => S::InvalidConfiguration,
            F::AlreadyExists => S::LocationOccupied,
            F::Network | F::RateLimited => S::Network,
            F::Protocol => S::Internal,
        }
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;

impl From<coven_format::path::PathError> for StorageError {
    fn from(_: coven_format::path::PathError) -> Self {
        Self::InvalidPath
    }
}
