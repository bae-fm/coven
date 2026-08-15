//! CloudHome: low-level cloud storage abstraction.
//!
//! Each backend (S3, R2, B2, etc.) implements `CloudHome` -- 8 methods for
//! raw bytes in/out. No encryption, no path layout knowledge, no sync
//! semantics. Higher-level concerns live in `CloudSyncConnection` which wraps any
//! `dyn CloudHome` and applies the path layout and at-rest protection.

// Pure helpers that S3-compatible backends share.
pub(crate) mod s3_common;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(feature = "oauth-providers")]
pub(crate) mod account_email;
pub mod cloudkit;
#[cfg(feature = "oauth-providers")]
pub mod dropbox;
mod factory;
#[cfg(feature = "oauth-providers")]
pub mod google_drive;
#[cfg(feature = "oauth-providers")]
mod http;
#[cfg(feature = "oauth-providers")]
mod key_encoding;
#[cfg(feature = "oauth-providers")]
mod oauth_rest;
#[cfg(feature = "oauth-providers")]
pub mod oauth_session;
#[cfg(feature = "oauth-providers")]
pub mod onedrive;
#[cfg(feature = "oauth-providers")]
mod resumable;
mod runtime;
pub mod s3;
pub mod setup;
#[cfg(feature = "oauth-providers")]
mod sharing;
#[cfg(test)]
mod test_server;

use coven_protocol::objects::{ObjectSlot, StorageBackendFailure};
pub use factory::CloudHomeFactory;
#[cfg(feature = "oauth-providers")]
pub use factory::PreparedOAuthCloudHome;
#[cfg(feature = "oauth-providers")]
pub(crate) use google_drive::{folder_search_query, supports_all_drives};
pub use runtime::CloudRuntimeError;
#[cfg(feature = "oauth-providers")]
pub use setup::SetupError;

mod blob_body;
mod exact_upload;
pub use blob_body::no_progress;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use blob_body::PROGRESS_CHUNK_SIZE;
pub(crate) use blob_body::{combine_cleanup_failure, MultipartUpload};
pub use blob_body::{BlobBody, BoxPartSink, PartSink, UploadProgress};
pub use exact_upload::{ExactUpload, ExactUploadSource};

#[cfg(test)]
pub(crate) async fn create_exact_bytes(
    storage: &dyn ExactSlotStorage,
    slot: &ObjectSlot,
    bytes: &[u8],
    progress: &UploadProgress<'_>,
) -> Result<ExactCreateOutcome, CloudHomeError> {
    let object = coven_protocol::objects::ExactObjectRef::new(
        slot.clone(),
        bytes.len() as u64,
        coven_protocol::store_commit::ObjectHash::digest(bytes),
    );
    let upload = ExactUpload::from_bytes(&object, bytes).map_err(CloudHomeError::from)?;
    storage.create_at(&upload, progress).await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExactCreateOutcome {
    Created,
    AlreadyPresent,
}

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::pin::Pin;

use futures_util::Stream;

use crate::local_file::PlaintextReader;
use coven_keys::encryption::{SealedBlobSealer, DEFAULT_BLOB_CHUNK_SIZE};

/// Errors from raw cloud storage operations.
#[derive(Debug, thiserror::Error)]
pub enum CloudHomeError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("exact slot contains different bytes: {0}")]
    SlotCollision(String),
    /// The cloud home is misconfigured or its credentials are missing or invalid:
    /// a bucket/folder/drive that isn't set, credentials absent from the keyring, a
    /// provider unsupported by this build, OAuth that needs re-authorization. The
    /// user must fix the configuration; retrying the same operation cannot succeed.
    #[error("configuration error: {0}")]
    Configuration(String),
    /// The cloud backend or the network to it failed: a request error, a non-2xx
    /// status, a malformed response. Transient — a later attempt may succeed.
    #[error("transport error: {0}")]
    Transport(String),
    #[error("cloud backend {kind:?} failure while {operation}: {source}")]
    Backend {
        kind: StorageBackendFailure,
        operation: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("{operation}; cleanup failed: {cleanup}")]
    CleanupFailed {
        #[source]
        operation: Box<CloudHomeError>,
        cleanup: Box<CloudHomeError>,
    },
    #[error("{operation}; exact response settlement failed: {settlement}")]
    UnresolvedOutcome {
        #[source]
        operation: Box<CloudHomeError>,
        settlement: Box<CloudHomeError>,
    },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("local file error: {0}")]
    Local(#[from] coven_foundation::atomic_file::FileError),
    #[error("blob source failed: {0}")]
    BlobSource(#[source] coven_protocol::objects::StorageError),
    #[error("storage protocol failed: {0}")]
    Protocol(#[source] coven_protocol::objects::StorageError),
    #[error("blob source content is invalid: {0}")]
    InvalidBlobSource(String),
}

/// Opaque provider revision for an exact cloud object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudObjectVersion(String);

impl CloudObjectVersion {
    pub fn from_provider(value: String) -> Result<Self, CloudHomeError> {
        if value.is_empty() {
            return Err(CloudHomeError::Configuration(
                "cloud object version token is empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_provider(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudVersionedObject {
    pub bytes: Vec<u8>,
    pub version: CloudObjectVersion,
}

pub type CloudObjectStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, CloudHomeError>> + Send + 'static>>;

#[derive(Debug, thiserror::Error)]
pub enum CloudFileReadError {
    #[error(transparent)]
    Source(#[from] CloudHomeError),
    #[error("{source}; local cleanup failed: {cleanup}")]
    SourceCleanup {
        #[source]
        source: CloudHomeError,
        cleanup: coven_foundation::atomic_file::FileError,
    },
    #[error("local destination failed: {0}")]
    Local(coven_foundation::atomic_file::FileError),
}

pub async fn write_cloud_object_stream(
    destination: &Path,
    stream: CloudObjectStream,
) -> Result<u64, CloudFileReadError> {
    let staged = coven_foundation::local_file::AtomicStagedFile::create(destination)
        .await
        .map_err(CloudFileReadError::Local)?;
    let (staged, written) =
        staged
            .write_byte_stream(stream)
            .await
            .map_err(|error| match error {
                coven_foundation::local_file::ByteStreamWriteError::Source(error) => {
                    CloudFileReadError::Source(error)
                }
                coven_foundation::local_file::ByteStreamWriteError::SourceCleanup {
                    source,
                    cleanup,
                } => CloudFileReadError::SourceCleanup { source, cleanup },
                coven_foundation::local_file::ByteStreamWriteError::Local(error) => {
                    CloudFileReadError::Local(error)
                }
            })?;
    staged.commit().await.map_err(CloudFileReadError::Local)?;
    Ok(written)
}

impl CloudHomeError {
    pub fn backend(
        kind: StorageBackendFailure,
        operation: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Backend {
            kind,
            operation: operation.into(),
            source: Box::new(source),
        }
    }

    pub fn configuration(
        operation: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::backend(StorageBackendFailure::Configuration, operation, source)
    }

    pub fn transport(
        operation: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::backend(StorageBackendFailure::Transport, operation, source)
    }

    /// Whether the failure is transient — worth retrying the operation unchanged —
    /// or a fault that will not resolve until the missing object appears or the user
    /// fixes the configuration. A transport or local-I/O failure is transient
    /// (`true`); a missing object, a misconfiguration, or absent/invalid credentials
    /// are not (`false`).
    pub fn is_retryable(&self) -> bool {
        match self {
            CloudHomeError::Transport(_)
            | CloudHomeError::Backend {
                kind: StorageBackendFailure::Transport,
                ..
            }
            | CloudHomeError::Io(_)
            | CloudHomeError::Local(_) => true,
            CloudHomeError::BlobSource(error) | CloudHomeError::Protocol(error) => {
                error.is_transport()
            }
            CloudHomeError::CleanupFailed { operation, .. }
            | CloudHomeError::UnresolvedOutcome { operation, .. } => operation.is_retryable(),
            CloudHomeError::NotFound(_)
            | CloudHomeError::AlreadyExists(_)
            | CloudHomeError::SlotCollision(_)
            | CloudHomeError::Configuration(_)
            | CloudHomeError::Backend { .. }
            | CloudHomeError::InvalidBlobSource(_) => false,
        }
    }

    pub fn backend_failure(&self) -> Option<StorageBackendFailure> {
        match self {
            Self::Backend { kind, .. } => Some(*kind),
            Self::Transport(_) => Some(StorageBackendFailure::Transport),
            Self::Configuration(_) => Some(StorageBackendFailure::Configuration),
            Self::CleanupFailed { operation, .. } | Self::UnresolvedOutcome { operation, .. } => {
                operation.backend_failure()
            }
            Self::BlobSource(error) | Self::Protocol(error) => error.backend_failure(),
            _ => None,
        }
    }

    pub fn cleanup_causes(&self) -> Option<(&CloudHomeError, &CloudHomeError)> {
        match self {
            Self::CleanupFailed { operation, cleanup } => Some((operation, cleanup)),
            _ => None,
        }
    }
}

/// Information needed to join a cloud home from another device.
///
/// The compact tagged shape (short `t` tags) is shared by invite codes and
/// restore codes — both wrap this same type, so a code adding neither weight
/// nor a second serde shape to carry around.
///
/// `Debug` is hand-written so the S3 `secret_key` prints as `<redacted>` —
/// `{:?}` in an error path cannot leak the storage credential.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "t")]
pub enum CloudHomeJoinInfo {
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        region: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
    #[serde(rename = "gd")]
    GoogleDrive { folder_id: String },
    /// `folder_path` matches `CloudHomeConfig.dropbox_folder_path`, whose
    /// `dropbox_` is the flat-config provider prefix (like `s3_bucket`) — one
    /// name for this value everywhere it's carried.
    #[serde(rename = "db")]
    Dropbox { folder_path: String },
    #[serde(rename = "od")]
    OneDrive { drive_id: String, folder_id: String },
    #[serde(rename = "ck")]
    CloudKit,
    #[serde(rename = "cks")]
    CloudKitShare {
        share_url: String,
        owner_name: String,
        zone_name: String,
    },
}

impl std::fmt::Debug for CloudHomeJoinInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudHomeJoinInfo::S3 {
                bucket,
                region,
                endpoint,
                access_key,
                secret_key: _,
                key_prefix,
            } => f
                .debug_struct("S3")
                .field("bucket", bucket)
                .field("region", region)
                .field("endpoint", endpoint)
                .field("access_key", access_key)
                .field("secret_key", &"<redacted>")
                .field("key_prefix", key_prefix)
                .finish(),
            CloudHomeJoinInfo::GoogleDrive { folder_id } => f
                .debug_struct("GoogleDrive")
                .field("folder_id", folder_id)
                .finish(),
            CloudHomeJoinInfo::Dropbox { folder_path } => f
                .debug_struct("Dropbox")
                .field("folder_path", folder_path)
                .finish(),
            CloudHomeJoinInfo::OneDrive {
                drive_id,
                folder_id,
            } => f
                .debug_struct("OneDrive")
                .field("drive_id", drive_id)
                .field("folder_id", folder_id)
                .finish(),
            CloudHomeJoinInfo::CloudKit => f.write_str("CloudKit"),
            CloudHomeJoinInfo::CloudKitShare {
                share_url,
                owner_name,
                zone_name,
            } => f
                .debug_struct("CloudKitShare")
                .field("share_url", share_url)
                .field("owner_name", owner_name)
                .field("zone_name", zone_name)
                .finish(),
        }
    }
}

impl CloudHomeJoinInfo {
    pub fn cloud_provider(&self) -> coven_foundation::config::CloudProvider {
        use coven_foundation::config::CloudProvider;
        match self {
            CloudHomeJoinInfo::S3 { .. } => CloudProvider::S3,
            CloudHomeJoinInfo::GoogleDrive { .. } => CloudProvider::GoogleDrive,
            CloudHomeJoinInfo::Dropbox { .. } => CloudProvider::Dropbox,
            CloudHomeJoinInfo::OneDrive { .. } => CloudProvider::OneDrive,
            CloudHomeJoinInfo::CloudKit | CloudHomeJoinInfo::CloudKitShare { .. } => {
                CloudProvider::CloudKit
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CloudAccessState {
    Present {
        member_pubkey: String,
        provider_account_email: Option<String>,
    },
    Absent {
        member_pubkey: String,
        provider_account_email: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudAccessOutcome {
    Present(CloudHomeJoinInfo),
    Absent(RevokeOutcome),
}

/// Whether a backend actually withdrew a removed member's storage credential.
///
/// Consumer clouds unshare the folder and report [`RevokeOutcome::Revoked`].
/// Shared-credential backends (S3) hand out one static bucket key that cannot be
/// withdrawn from a single member and report [`RevokeOutcome::Unsupported`].
/// Removal proceeds either way: revoking chain membership and rotating the
/// store key — not withdrawing the credential — is what protects post-removal
/// content, so `Unsupported` is a truthful outcome, not a failure to paper over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevokeOutcome {
    Revoked,
    Unsupported,
}

impl CloudAccessState {
    fn provider_account_email(&self) -> Option<&str> {
        match self {
            Self::Present {
                provider_account_email,
                ..
            }
            | Self::Absent {
                provider_account_email,
                ..
            } => provider_account_email.as_deref(),
        }
    }

    pub fn require_provider_email(&self, provider: &str) -> Result<&str, CloudHomeError> {
        require_provider_email(provider, self.provider_account_email())
    }
}

fn require_provider_email<'a>(
    provider: &str,
    email: Option<&'a str>,
) -> Result<&'a str, CloudHomeError> {
    match email {
        Some(email) if !email.is_empty() => Ok(email),
        _ => Err(CloudHomeError::Configuration(format!(
            "{provider} sharing requires the invitee's provider account email"
        ))),
    }
}

/// The HTTP `Range` header value for a ranged GET. `start` is inclusive and
/// `end` is exclusive (the `CloudHome` contract); the header is inclusive on
/// both ends, so the upper bound is `end - 1`. The one definition every backend
/// — both S3 transports and the OAuth REST backends — uses.
pub(crate) fn range_header(start: u64, end: u64) -> String {
    format!("bytes={start}-{}", end.saturating_sub(1))
}

/// Low-level cloud storage. Implementations handle a single store.
///
/// All methods deal in raw bytes. No encryption or path layout logic.
///
#[async_trait]
pub trait ExactSlotStorage: Send + Sync {
    async fn provider_binding(
        &self,
    ) -> Result<coven_protocol::objects::ResolvedProviderBinding, CloudHomeError>;

    async fn cross_principal_evidence(
        &self,
    ) -> Result<coven_protocol::provider::CrossPrincipalProviderEvidence, CloudHomeError> {
        use coven_protocol::objects::{GoogleDriveCorpus, StoreProviderBinding};
        use coven_protocol::provider::CrossPrincipalProviderEvidence;

        match self.provider_binding().await?.store {
            StoreProviderBinding::GoogleDrive {
                corpus: GoogleDriveCorpus::SharedDrive { .. },
            } => Ok(CrossPrincipalProviderEvidence::GoogleSharedDrive),
            StoreProviderBinding::Dropbox { .. } => {
                Ok(CrossPrincipalProviderEvidence::DropboxSharedNamespace)
            }
            StoreProviderBinding::OneDrive { .. } => {
                Ok(CrossPrincipalProviderEvidence::OneDriveSharedFolder)
            }
            StoreProviderBinding::CloudKit { .. } => Err(CloudHomeError::Configuration(
                "CloudKit exact-slot adapter did not supply accepted-share evidence".to_string(),
            )),
            StoreProviderBinding::GoogleDrive { .. } => Err(CloudHomeError::Configuration(
                "Google Drive cross-principal access requires a shared drive".to_string(),
            )),
            StoreProviderBinding::S3 { .. } => Err(CloudHomeError::Configuration(
                "S3 has no cross-principal provider evidence".to_string(),
            )),
        }
    }

    /// Name the slot that holds `logical_key`. A provider that addresses objects
    /// by the key itself allocates nothing; one that mints its own object id
    /// overrides this and returns an opaque locator.
    async fn allocate_slot(&self, logical_key: &str) -> Result<ObjectSlot, CloudHomeError> {
        ObjectSlot::logical(logical_key.to_string()).map_err(CloudHomeError::from)
    }

    async fn create_at(
        &self,
        upload: &ExactUpload<'_>,
        progress: &UploadProgress<'_>,
    ) -> Result<ExactCreateOutcome, CloudHomeError>;

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError>;

    async fn observe_at(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<coven_protocol::objects::ExactObjectRef>, CloudHomeError> {
        match self.read_at(slot).await {
            Ok(bytes) => Ok(Some(coven_protocol::objects::ExactObjectRef::new(
                slot.clone(),
                bytes.len() as u64,
                coven_protocol::store_commit::ObjectHash::digest(&bytes),
            ))),
            Err(CloudHomeError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn read_range_at(
        &self,
        slot: &ObjectSlot,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, CloudHomeError>;

    async fn read_at_to_file(
        &self,
        slot: &ObjectSlot,
        destination: &Path,
    ) -> Result<(), CloudFileReadError>;

    async fn delete_at(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError>;

    async fn delete_and_verify_absent(&self, slot: &ObjectSlot) -> Result<(), CloudHomeError> {
        match self.read_at(slot).await {
            Err(CloudHomeError::NotFound(_)) => Ok(()),
            Ok(_) => {
                self.delete_at(slot).await?;
                match self.read_at(slot).await {
                    Err(CloudHomeError::NotFound(_)) => Ok(()),
                    Ok(_) => Err(CloudHomeError::Configuration(format!(
                        "exact-slot adapter left {} present after deletion",
                        slot.logical_key()
                    ))),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[async_trait]
pub trait CloudHome: Send + Sync {
    /// Verify the backend is reachable with the configured credentials.
    /// Setup flows call this *before* persisting credentials, so a typo or
    /// missing bucket fails fast at setup time instead of via a delayed
    /// reconnect banner. Default implementation issues a no-op list against
    /// a sentinel prefix — backends override when a provider-specific operation
    /// verifies the capabilities sync requires.
    async fn probe(&self) -> Result<(), CloudHomeError> {
        self.list("__coven_probe__").await.map(drop)
    }

    /// One bounded single-request upload, creating or overwriting `key`. Used only
    /// for blobs at or below [`multipart_threshold`](CloudHome::multipart_threshold);
    /// large blobs stream through [`open_multipart`](CloudHome::open_multipart).
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError>;

    /// Open a streaming multipart/resumable upload for `total_len` bytes, returning
    /// the [`PartSink`] the driver pumps ordered parts into.
    async fn open_multipart<'a>(
        &'a self,
        key: &str,
        total_len: u64,
    ) -> Result<BoxPartSink<'a>, CloudHomeError>;

    /// Blobs at or below this size go via [`put_object`](CloudHome::put_object);
    /// larger ones stream via [`open_multipart`](CloudHome::open_multipart).
    fn multipart_threshold(&self) -> u64;

    /// Write a sized [`BlobBody`] to `key`. Not overridden — the central
    /// `write_blob` driver picks single-request vs multipart and pumps the
    /// parts, reporting cumulative bytes through `progress` for the per-file bar.
    async fn write(
        &self,
        key: &str,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError> {
        if body.len() <= self.multipart_threshold() {
            let data = body.collect().await?;
            let n = data.len() as u64;
            self.put_object(key, data).await?;
            progress(n);
            return Ok(());
        }
        let sink = self.open_multipart(key, body.len()).await?;
        MultipartUpload::new(key, body, sink, progress).run().await
    }

    /// Read the full contents of a key.
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;

    /// Read a byte range from a key. `start` is inclusive, `end` is exclusive.
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError>;

    /// List all keys under a prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;

    /// Delete a key. Not an error if the key does not exist.
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError>;

    /// Check whether a key exists.
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError>;

    /// Set the provider's access for one stable member principal to the absolute
    /// desired state. Implementations read the authoritative permission state,
    /// create/update/delete as required, then read it back and verify the desired
    /// state. Repeating a request after an unknown outcome is therefore
    /// idempotent. `Present` returns connection information; `Absent` returns
    /// whether this provider supports withdrawing one member's credential.
    async fn set_access(
        &self,
        desired: CloudAccessState,
    ) -> Result<CloudAccessOutcome, CloudHomeError>;
}

/// A cloud home admitted to sync: raw object operations and exact immutable
/// slots are one provider capability, so callers cannot open the home and then
/// ask it to hand back a second provider object.
pub trait ExactCloudHome: CloudHome + ExactSlotStorage {}

impl<T> ExactCloudHome for T where T: CloudHome + ExactSlotStorage {}

#[cfg(test)]
mod object_slot_tests;

#[cfg(test)]
mod join_info_tests;

#[cfg(test)]
mod retryable_tests;

#[cfg(test)]
mod streaming_tests;

impl From<CloudHomeError> for coven_protocol::objects::StorageError {
    fn from(e: CloudHomeError) -> Self {
        match e {
            CloudHomeError::NotFound(key) => coven_protocol::objects::StorageError::NotFound(key),
            CloudHomeError::AlreadyExists(key) => {
                coven_protocol::objects::StorageError::AlreadyExists(key)
            }
            CloudHomeError::SlotCollision(key) => {
                coven_protocol::objects::StorageError::SlotCollision(key)
            }
            CloudHomeError::Configuration(msg) => {
                coven_protocol::objects::StorageError::Configuration(msg)
            }
            CloudHomeError::Backend {
                kind,
                operation,
                source,
            } => coven_protocol::objects::StorageError::Backend {
                kind,
                operation,
                source,
            },
            error @ CloudHomeError::Transport(_) => coven_protocol::objects::StorageError::backend(
                StorageBackendFailure::Transport,
                "access cloud storage",
                error,
            ),
            CloudHomeError::CleanupFailed { operation, cleanup } => {
                coven_protocol::objects::StorageError::CleanupFailed {
                    operation: Box::new(Self::from(*operation)),
                    cleanup: Box::new(Self::from(*cleanup)),
                }
            }
            CloudHomeError::UnresolvedOutcome {
                operation,
                settlement,
            } => coven_protocol::objects::StorageError::UnresolvedOutcome {
                operation: Box::new(Self::from(*operation)),
                settlement: Box::new(Self::from(*settlement)),
            },
            CloudHomeError::Io(io_err) => coven_protocol::objects::StorageError::Io(io_err),
            CloudHomeError::Local(error) => {
                coven_protocol::objects::StorageError::LocalFilesystem(error)
            }
            CloudHomeError::BlobSource(error) => error,
            CloudHomeError::Protocol(error) => error,
            CloudHomeError::InvalidBlobSource(message) => {
                coven_protocol::objects::StorageError::InvalidContent(message)
            }
        }
    }
}

/// Slot and reference validation lives on the protocol values and reports
/// [`coven_protocol::objects::StorageError`]; provider code folds it into its
/// own configuration vocabulary.
impl From<coven_protocol::objects::StorageError> for CloudHomeError {
    fn from(error: coven_protocol::objects::StorageError) -> Self {
        CloudHomeError::Protocol(error)
    }
}
