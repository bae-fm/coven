use crate::{providers::OAuthError, CloudProvider};
use coven_crypto::custody::KeyError;

/// Failures the app can distinguish and act on (§20.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StorageFailure {
    /// The platform or OS version cannot use this built-in provider.
    ProviderUnavailable,
    /// The app did not register the access required by this operation.
    AccessNotRegistered {
        /// Access the app must register before this operation is available.
        required: crate::OAuthAccess,
    },
    /// The authorization did not grant the required access.
    AccessNotGranted {
        /// Access the account must authorize.
        required: crate::OAuthAccess,
    },
    /// Drive's File grant cannot see the folder; Full access is needed to inspect it.
    /// This does not establish whether the folder is absent or shared by another person.
    GoogleDriveFolderNotVisible,
    /// An App-folder store cannot be shared without relocating it.
    DropboxAppFolderNotShareable,
    /// An incremental sign-in selected a different provider account.
    AccountMismatch,
    /// The installation must complete membership and key-copy catch-up before sending.
    CatchUpRequired,
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
    /// A concurrent storage mutation defeated a conditional request.
    Conflict,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 endpoint or signing region is wrong.
    RegionMismatch,
    /// A member key coven made could list beyond its store's prefix (§4).
    KeyBeyondStore,
    /// Setting up a store that never had storage found another device's
    /// store-log entry in its folder: another installation of the store
    /// already created it at this location (§10). Restore or join it instead.
    StoreExists,
    /// The S3 service's member keys reach a whole bucket, so a store there
    /// needs a bucket of its own (§4).
    OwnBucketRequired {
        /// What the location shares.
        reason: OwnBucketReason,
    },
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
    /// The recorded upload expired or a publication conflict invalidated it.
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
    /// An S3 object published by `CopyObject` exceeds its single-copy limit,
    /// so it cannot be published atomically on this profile (§4).
    CopyTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The single-copy limit in bytes.
        limit: u64,
    },
    /// Parsing recorded provider data failed.
    Encoding,
    /// Local file access or persisting provider settings or upload progress failed.
    File,
    /// This installation has no member keys in custody.
    MemberKeysMissing,
}

/// Why an S3 location cannot hold a store that needs its own bucket (§4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OwnBucketReason {
    /// The location names a base prefix; the store occupies the bucket's root.
    Prefix,
    /// Creating the store found objects outside its own folder in the bucket.
    Occupied,
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
        /// Original transport, SDK or framework error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Cleanup failed too; neither cause is hidden.
    #[error("{operation}; cleanup also failed: {cleanup}")]
    Cleanup {
        /// The original failure.
        #[source]
        operation: Box<StorageError>,
        /// Failure removing temporary data or recording the failed upload state.
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

    /// Transport interruptions, conditional conflicts and throttling invite retry.
    pub fn retryable(&self) -> bool {
        matches!(
            self.failure(),
            StorageFailure::Network | StorageFailure::Conflict | StorageFailure::RateLimited
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
    /// Run the scoped query and check returned probe metadata; fresh objects may be omitted.
    List,
    /// Delete the object and confirm it is absent.
    Delete,
    /// S3: multipart publication refuses an occupied name with its bytes and
    /// time unchanged, and conditional completion hides unfinished uploads.
    MultipartCreateOnce,
    /// S3: multipart objects are dated by complete publication, as recorded.
    PublicationTime,
    /// S3: another setting agrees with the service's observed behavior.
    Behavior,
}

/// A failed setup commits no local storage settings or credentials.
#[derive(Debug, thiserror::Error)]
pub enum StorageSetupError {
    /// The named catalogued S3 profile failed certification's concurrent
    /// create-once races, so setup refuses it before any request (§4).
    #[error("S3 profile {profile:?} is refused: concurrent create-once publication failed certification")]
    S3ProfileRefused {
        /// The refused profile.
        profile: crate::S3ProfileName,
    },
    /// This profile has no certification with bucket versioning enabled.
    #[error("S3 bucket versioning {state:?} is not certified for this profile")]
    S3VersioningUnsupported {
        /// The bucket's observed versioning state.
        state: crate::S3BucketVersioning,
    },
    /// The bucket state is unknown and this profile has no versioning certification.
    #[error("S3 bucket versioning is unknown; this profile requires a successful GetBucketVersioning (s3:GetBucketVersioning; GCS storage.buckets.get): {source}")]
    S3VersioningUnknown {
        /// The refused versioning read, retaining its native cause.
        #[source]
        source: std::sync::Arc<StorageError>,
    },
    /// A provider check failed; cleanup failures retain both causes.
    #[error("storage check {check:?}: {source}")]
    ProviderCheck {
        /// The operation that failed.
        check: StorageCheck,
        /// The original failure, including cleanup if that also failed.
        #[source]
        source: StorageError,
    },
    /// Reconnecting cannot move an established store to another location.
    #[error("storage location change is unsupported")]
    LocationChangeUnsupported,
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
