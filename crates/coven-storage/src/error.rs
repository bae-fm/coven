use crate::{providers::OAuthError, CloudProvider};
use coven_crypto::custody::KeyError;

/// Failures the app can distinguish and act on (Appendix F.5).
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
    /// An App-folder store cannot be shared without relocating it.
    DropboxAppFolderNotShareable,
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
    /// A member key coven made could list beyond its store's prefix (§20.10).
    KeyBeyondStore,
    /// Setting up a store that never had storage found another installation's
    /// creation slot: another installation of the store claimed this
    /// location first (§10.3). Restore or join it instead.
    StoreExists,
    /// S3 rejected the device clock after one retry with the offset its
    /// response stated (§20.7).
    ClockSkew,
    /// The S3 service's member keys reach a whole bucket, so a store there
    /// needs a bucket of its own (§20.7).
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
    /// The invited account is the one holding the store (§13.2).
    InviteeIsStoreOwner,
    /// Dropbox cannot upgrade a pending viewer without the recipient's account id.
    AccountIdUnavailable,
    /// The invite belongs to another provider location.
    InvitationMismatch,
    /// The session belongs to another provider or location.
    SessionMismatch,
    /// The recorded upload expired or a publication conflict invalidated it.
    SessionExpired,
    /// The native id of an unresolved S3 initiation cannot be proven to belong to this session.
    SessionIdentityUnknown,
    /// A part disagrees with the session's offset, size or alignment.
    InvalidPart,
    /// A floor replacement exceeds this provider's single-request limit.
    SingleRequestTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The adapter's single-request limit in bytes.
        limit: u64,
    },
    /// An S3 object published by `CopyObject` exceeds its single-copy limit,
    /// so it cannot be published atomically on this profile (§20.6).
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

/// Why an S3 location cannot hold a store that needs its own bucket (§20.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OwnBucketReason {
    /// The location names a base prefix; the store occupies the bucket's root.
    Prefix,
    /// The bucket holds foreign objects, or its marker does not name this store.
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
    /// Native destination conflicts mean a failed condition during replacement.
    pub(crate) fn replacement(mut self) -> Self {
        match &mut self {
            Self::Failure(failure)
            | Self::Caused { failure, .. }
            | Self::Provider { failure, .. }
                if *failure == StorageFailure::AlreadyExists =>
            {
                *failure = StorageFailure::Conflict
            }
            _ => {}
        }
        self
    }

    /// What the app can do about this failure.
    pub fn failure(&self) -> StorageFailure {
        match self {
            Self::Provider { failure, .. } => *failure,
            Self::Failure(failure) | Self::Caused { failure, .. } => *failure,
            Self::Cleanup { operation, .. } => operation.failure(),
        }
    }

    /// Whether the provider answered the request behind this failure, even
    /// with an error (E5). `Network` alone cannot say: a server's error
    /// response is an answer; a refused connection or a timeout is not.
    pub fn answered(&self) -> bool {
        match self {
            Self::Provider { source, .. } => crate::providers::response_arrived(source.as_ref()),
            Self::Cleanup { operation, .. } => operation.answered(),
            Self::Failure(_) | Self::Caused { .. } => false,
        }
    }

    /// Whether the provider answered this request with a refusal, so it
    /// stored nothing. A timeout, or a lost, malformed or server-failure
    /// reply, leaves the outcome unknown. The clients resend a request only
    /// after a refusal (a rejected token, a skewed clock), so no earlier
    /// attempt of it stored anything either.
    pub fn refused(&self) -> bool {
        self.answered()
            && !matches!(
                self.failure(),
                StorageFailure::Network | StorageFailure::Protocol
            )
    }

    /// The wait a provider response asked every consumer to observe, from
    /// its `Retry-After`, Dropbox's `retry_after` or CloudKit's
    /// `CKErrorRetryAfterKey`.
    pub(crate) fn retry_after(&self) -> Option<crate::RetryAfter> {
        match self {
            Self::Provider {
                provider, source, ..
            } => crate::providers::retry_after(*provider, source.as_ref()),
            Self::Cleanup { operation, .. } => operation.retry_after(),
            Self::Failure(_) | Self::Caused { .. } => None,
        }
    }

    /// Transport interruptions and provider throttling invite retry.
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
    /// Replace the observed object and refuse its stale revision without changing bytes.
    ConditionalReplace,
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
