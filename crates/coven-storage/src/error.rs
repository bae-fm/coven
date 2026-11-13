use crate::CloudProvider;

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
    /// No route to the provider, a timeout, an interrupted response, or a
    /// server failure the provider answered with (§6.7).
    Network,
    /// Two complete reads of one immutable object returned different bytes:
    /// a fault on the way to the device changed one of them (§19.1).
    Corrupted,
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
    /// Setting up a store that never had storage found another installation's
    /// creation slot: another installation of the store claimed this
    /// location first (§10.3). Restore or join it instead.
    StoreExists,
    /// S3 rejected the device clock after one retry with the offset its
    /// response stated (§20.7).
    ClockSkew,
    /// Every member reaches the whole location (a Dropbox shared folder, a
    /// OneDrive folder, or an S3 bucket), so a store there needs it to
    /// itself, and it holds something besides this store (§20.3). At first
    /// setup: an object or folder outside the store's own, or a location
    /// marker that doesn't name it. At a reconnect, restore or join: a marker
    /// that is missing, malformed or names another store.
    OwnLocationRequired,
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
    /// Coven changes sharing only from the account holding the store. That is
    /// coven's rule, not every provider's limit (§13.2).
    NotStoreOwner,
    /// The invited account is the one holding the store (§13.2).
    InviteeIsStoreOwner,
    /// The provider refused to share the store for a reason only a person
    /// can fix, the invited address's or the inviting account's, so no retry
    /// changes the answer (§13.2).
    SharingRefused(coven_foundation::sharing::SharingRefusal),
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
    /// A floor pointer replacement exceeds this provider's single-request
    /// limit. The pointer is small, so it never goes up in parts.
    SingleRequestTooLarge {
        /// Encrypted body length in bytes.
        size: u64,
        /// The adapter's single-request limit in bytes.
        limit: u64,
    },
    /// An S3 object published by `CopyObject` exceeds its single-copy limit,
    /// so this profile refuses it (§20.6 says why).
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
    /// Coven's work for the request panicked: a bug, which the caller
    /// reports (E15). `StorageError::Bug` carries it.
    Bug,
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
    /// Coven's work for the request panicked: a bug, which the caller
    /// reports (E15). Its failure is `StorageFailure::Bug`.
    #[error(transparent)]
    Bug(#[from] coven_foundation::task::Bug),
}

impl coven_foundation::task::CarriesBug for StorageError {
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Cleanup { operation, cleanup } => operation.bug().or_else(|| cleanup.bug()),
            Self::Failure(_) | Self::Caused { .. } | Self::Provider { .. } => None,
        }
    }
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
            Self::Bug(_) => StorageFailure::Bug,
        }
    }

    /// The failure when the request alone failed: none when its cleanup
    /// failed too, which leaves state behind that a caller handling the
    /// failure itself, such as an occupied path or a throttled request,
    /// doesn't know of.
    pub fn own_failure(&self) -> Option<StorageFailure> {
        match self {
            Self::Cleanup { .. } => None,
            _ => Some(self.failure()),
        }
    }

    /// Whether the provider answered the request behind this failure, even
    /// with an error (E5). `Network` alone cannot say: a server's error
    /// response is an answer; a refused connection or a timeout is not.
    pub fn answered(&self) -> bool {
        match self {
            Self::Provider { source, .. } => crate::providers::response_arrived(source.as_ref()),
            Self::Cleanup { operation, .. } => operation.answered(),
            Self::Failure(_) | Self::Caused { .. } | Self::Bug(_) => false,
        }
    }

    /// Whether the provider answered this request with a refusal, so it
    /// stored nothing. A timeout, or a lost, malformed or server-failure
    /// reply, leaves the outcome unknown. The clients resend a request only
    /// after a refusal (a rejected token, a skewed clock), so no earlier
    /// attempt of it stored anything either.
    pub fn refused(&self) -> bool {
        self.answered()
            && !matches!(self.failure(), StorageFailure::Network | StorageFailure::Protocol)
    }

    /// The wait a provider response asked every consumer to observe, from
    /// its `Retry-After`, Dropbox's `retry_after` or CloudKit's
    /// `CKErrorRetryAfterKey`.
    pub(crate) fn retry_after(&self) -> Option<crate::RetryAfter> {
        match self {
            Self::Provider { provider, source, .. } => {
                crate::providers::retry_after(*provider, source.as_ref())
            }
            Self::Cleanup { operation, .. } => operation.retry_after(),
            Self::Failure(_) | Self::Caused { .. } | Self::Bug(_) => None,
        }
    }

    /// Transport interruptions and provider throttling invite retry.
    pub fn retryable(&self) -> bool {
        matches!(self.failure(), StorageFailure::Network | StorageFailure::RateLimited)
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

/// A provider or S3 settings check refused the location (§20.7, §20.8).
#[derive(Debug, thiserror::Error)]
pub enum StorageCheckError {
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
    /// A classified storage failure.
    #[error(transparent)]
    Storage(StorageError),
}

impl coven_foundation::task::CarriesBug for StorageCheckError {
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Storage(error) | Self::ProviderCheck { source: error, .. } => error.bug(),
            Self::S3VersioningUnknown { source } => source.bug(),
            Self::S3VersioningUnsupported { .. } => None,
        }
    }
}

impl From<StorageError> for StorageCheckError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl StorageFailure {
    /// Retain the diagnostic or native cause alongside this classification.
    pub fn with_source(
        self,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> StorageError {
        StorageError::Caused { failure: self, source: source.into() }
    }
}

impl From<StorageFailure> for StorageError {
    fn from(failure: StorageFailure) -> Self {
        Self::Failure(failure)
    }
}

impl From<coven_foundation::files::FileError> for StorageError {
    fn from(error: coven_foundation::files::FileError) -> Self {
        match error {
            coven_foundation::files::FileError::Bug(bug) => Self::Bug(bug),
            error => StorageFailure::File.with_source(error),
        }
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;

/// A code's storage part that the format's primitives refuse: truncated,
/// trailing bytes, an unknown tag or a length past its bound (format.md D13).
impl From<coven_format::Error> for StorageError {
    fn from(error: coven_format::Error) -> Self {
        StorageFailure::Encoding.with_source(error)
    }
}

impl From<coven_format::path::PathError> for StorageError {
    fn from(error: coven_format::path::PathError) -> Self {
        StorageFailure::InvalidPath.with_source(error)
    }
}
