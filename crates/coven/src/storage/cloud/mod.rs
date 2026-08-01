//! CloudHome: low-level cloud storage abstraction.
//!
//! Each backend (S3, R2, B2, etc.) implements `CloudHome` -- 8 methods for
//! raw bytes in/out. No encryption, no path layout knowledge, no sync
//! semantics. Higher-level concerns live in `CloudSyncStorage` which wraps any
//! `dyn CloudHome` and applies the path layout and at-rest protection.

// Pure helpers that S3-compatible backends share.
pub(crate) mod s3_common;

#[cfg(any(test, feature = "test-utils"))]
pub(crate) mod test_utils;

#[cfg(feature = "oauth-providers")]
pub(crate) mod account_email;
pub(crate) mod cloudkit;
#[cfg(feature = "oauth-providers")]
pub(crate) mod dropbox;
mod factory;
#[cfg(feature = "oauth-providers")]
pub(crate) mod google_drive;
#[cfg(feature = "oauth-providers")]
mod http;
#[cfg(feature = "oauth-providers")]
mod key_encoding;
#[cfg(feature = "oauth-providers")]
mod oauth_rest;
#[cfg(feature = "oauth-providers")]
pub(crate) mod oauth_session;
#[cfg(feature = "oauth-providers")]
pub(crate) mod onedrive;
#[cfg(feature = "oauth-providers")]
mod resumable;
pub(crate) mod s3;
pub(crate) mod setup;
#[cfg(feature = "oauth-providers")]
mod sharing;
#[cfg(test)]
mod test_server;

pub use factory::create_cloud_home;
pub(crate) use factory::CloudHomeFactory;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Deserializer, Serialize};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::Stream;

use crate::encryption::{SealedBlobSealer, DEFAULT_BLOB_CHUNK_SIZE};
use crate::storage::local_file::PlaintextReader;

/// Errors from raw cloud storage operations.
#[derive(Debug, thiserror::Error)]
pub enum CloudHomeError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
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
    #[error("{operation}; cleanup failed: {cleanup}")]
    CleanupFailed {
        #[source]
        operation: Box<CloudHomeError>,
        cleanup: Box<CloudHomeError>,
    },
    #[error("{operation}; exact response-loss readback failed: {readback}")]
    UnresolvedOutcome {
        #[source]
        operation: Box<CloudHomeError>,
        readback: Box<CloudHomeError>,
    },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Provider-specific physical address for a caller-reserved immutable slot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PhysicalObjectLocator {
    LogicalKey,
    Opaque(String),
}

/// Exact logical and physical location persisted before an immutable write.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectSlot {
    logical_key: String,
    physical: PhysicalObjectLocator,
}

impl<'de> Deserialize<'de> for ObjectSlot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            logical_key: String,
            physical: PhysicalObjectLocator,
        }

        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.logical_key, fields.physical).map_err(serde::de::Error::custom)
    }
}

impl ObjectSlot {
    pub fn logical(logical_key: String) -> Result<Self, CloudHomeError> {
        Self::new(logical_key, PhysicalObjectLocator::LogicalKey)
    }

    pub fn opaque(logical_key: String, provider_id: String) -> Result<Self, CloudHomeError> {
        Self::new(logical_key, PhysicalObjectLocator::Opaque(provider_id))
    }

    fn new(logical_key: String, physical: PhysicalObjectLocator) -> Result<Self, CloudHomeError> {
        let slot = Self {
            logical_key,
            physical,
        };
        slot.validate()?;
        Ok(slot)
    }

    pub fn validate(&self) -> Result<(), CloudHomeError> {
        if self.logical_key.is_empty() {
            return Err(CloudHomeError::Configuration(
                "object slot logical key is empty".to_string(),
            ));
        }
        if matches!(&self.physical, PhysicalObjectLocator::Opaque(value) if value.is_empty()) {
            return Err(CloudHomeError::Configuration(
                "object slot provider locator is empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn logical_key(&self) -> &str {
        &self.logical_key
    }

    pub fn physical(&self) -> &PhysicalObjectLocator {
        &self.physical
    }

    /// Reject this slot when the provider requires the logical key to be the
    /// physical object locator.
    pub(crate) fn require_logical_key_for(&self, provider: &str) -> Result<(), CloudHomeError> {
        self.validate()?;
        if self.physical != PhysicalObjectLocator::LogicalKey {
            return Err(CloudHomeError::Configuration(format!(
                "{provider} slot for {} must use its logical key",
                self.logical_key
            )));
        }
        Ok(())
    }
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
        cleanup: String,
    },
    #[error("local destination failed: {0}")]
    Local(String),
}

pub async fn write_cloud_object_stream(
    destination: &Path,
    stream: CloudObjectStream,
) -> Result<u64, CloudFileReadError> {
    let staged = crate::storage::local_file::AtomicStagedFile::create(destination)
        .await
        .map_err(CloudFileReadError::Local)?;
    let (staged, written) =
        staged
            .write_byte_stream(stream)
            .await
            .map_err(|error| match error {
                crate::storage::local_file::ByteStreamWriteError::Source(error) => {
                    CloudFileReadError::Source(error)
                }
                crate::storage::local_file::ByteStreamWriteError::SourceCleanup {
                    source,
                    cleanup,
                } => CloudFileReadError::SourceCleanup { source, cleanup },
                crate::storage::local_file::ByteStreamWriteError::Local(error) => {
                    CloudFileReadError::Local(error)
                }
            })?;
    staged.commit().await.map_err(CloudFileReadError::Local)?;
    Ok(written)
}

impl CloudHomeError {
    /// Whether the failure is transient — worth retrying the operation unchanged —
    /// or a fault that will not resolve until the missing object appears or the user
    /// fixes the configuration. A transport or local-I/O failure is transient
    /// (`true`); a missing object, a misconfiguration, or absent/invalid credentials
    /// are not (`false`).
    pub fn is_retryable(&self) -> bool {
        match self {
            CloudHomeError::Transport(_) | CloudHomeError::Io(_) => true,
            CloudHomeError::CleanupFailed { operation, .. }
            | CloudHomeError::UnresolvedOutcome { operation, .. } => operation.is_retryable(),
            CloudHomeError::NotFound(_)
            | CloudHomeError::AlreadyExists(_)
            | CloudHomeError::Configuration(_) => false,
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
    pub fn cloud_provider(&self) -> crate::config::CloudProvider {
        use crate::config::CloudProvider;
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
    pub fn member_pubkey(&self) -> &str {
        match self {
            Self::Present { member_pubkey, .. } | Self::Absent { member_pubkey, .. } => {
                member_pubkey
            }
        }
    }

    pub fn provider_account_email(&self) -> Option<&str> {
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

/// Reports how many bytes of a `write` have reached the backend so far.
/// Called with the cumulative byte count as the body uploads; backends that
/// can't observe sub-call progress call it once at the end with the full size.
/// The count is of the bytes handed to `write` (the encrypted payload).
pub type UploadProgress<'a> = dyn Fn(u64) + Send + Sync + 'a;

/// Chunk size the in-memory test backend uses to drive its `UploadProgress`
/// callback in several ticks. Real providers whose resumable API mandates a
/// specific alignment (OneDrive 320 KiB multiples, Google Drive 256 KiB
/// multiples, S3 5 MiB minimum parts) define their own constant.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) const PROGRESS_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// A progress sink that discards its reports. For `write` calls whose payload
/// is a small control file (head pointers, the snapshot) where no per-file
/// progress bar is driven — only the blob outbox surfaces progress.
pub(crate) fn no_progress() -> impl Fn(u64) + Send + Sync {
    |_| {}
}

/// A blob as a **sized stream of already-final bytes**: sealed chunks for an
/// encrypted home, plaintext for a browsable one. Encryption-agnostic and
/// concrete (no `dyn Stream`). [`next_part`](BlobBody::next_part) hands the bytes to a streaming
/// upload in bounded windows so a large blob is never held whole in memory; the
/// only [`collect`](BlobBody::collect) is the single-request path for blobs at or
/// below a provider's multipart threshold.
///
/// Built by the cipher layer (`CloudCipher::open_body`), which knows scope→key and
/// plaintext-vs-encrypted, or by [`from_bytes`](BlobBody::from_bytes) for an
/// in-memory control object / the test backend.
pub struct BlobBody {
    /// Total bytes this body will yield: the encrypted length (see
    /// [`crate::encryption::chunked_encrypted_len`]) for a sealed body, or the
    /// plaintext length for a passthrough one.
    len: u64,
    source: BlobSource,
    /// Final bytes produced by the source but not yet handed out by `next_part`.
    carry: BytesMut,
}

/// Where a [`BlobBody`]'s final bytes come from.
enum BlobSource {
    /// Already-final bytes, handed out once. A control object's sealed bytes, a
    /// small in-memory write, or the in-memory test backend's payload.
    Buffered(Bytes),
    /// Plaintext read incrementally from a local file and, for an encrypted home,
    /// sealed one header-sized chunk at a time.
    File {
        reader: PlaintextReader,
        /// Final bytes emitted before the plaintext stream — the key tag and,
        /// for an encrypted home, the sealed-blob header.
        prefix: Bytes,
        /// `Some` seals each chunk under the scope's key; `None` passes the
        /// plaintext through (a browsable home).
        sealer: Option<SealedBlobSealer>,
        /// Whether any plaintext chunk has been sealed — distinguishes a truly
        /// empty file (which still seals one tag-only chunk, so opening it
        /// authenticates its emptiness) from one that produced chunks and then
        /// drained.
        sealed_any: bool,
        eof: bool,
    },
}

impl BlobSource {
    /// The next run of final bytes, or `None` once the source is exhausted.
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, CloudHomeError> {
        match self {
            BlobSource::Buffered(b) => {
                if b.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(b)))
                }
            }
            BlobSource::File {
                reader,
                prefix,
                sealer,
                sealed_any,
                eof,
            } => {
                if !prefix.is_empty() {
                    return Ok(Some(std::mem::take(prefix)));
                }
                if *eof {
                    return Ok(None);
                }
                // Each read is exactly one chunk of the size this blob's header
                // declares, so the sealer's framing and the reader's stride are
                // the same number by construction.
                let stride = match sealer {
                    Some(s) => s.header().chunk_size().get() as usize,
                    None => DEFAULT_BLOB_CHUNK_SIZE.get() as usize,
                };
                let chunk = reader
                    .next_chunk(stride)
                    .await
                    .map_err(CloudHomeError::Transport)?;
                if chunk.is_empty() {
                    *eof = true;
                    // A sealed empty file still emits one tag-only chunk, so a
                    // reader authenticates its emptiness; a plaintext file (or a
                    // sealed one that already produced chunks) ends here.
                    if let Some(s) = sealer {
                        if !*sealed_any {
                            *sealed_any = true;
                            return Ok(Some(Bytes::from(s.seal_chunk(&[]))));
                        }
                    }
                    return Ok(None);
                }
                match sealer {
                    Some(s) => {
                        *sealed_any = true;
                        Ok(Some(Bytes::from(s.seal_chunk(&chunk))))
                    }
                    None => Ok(Some(Bytes::from(chunk))),
                }
            }
        }
    }
}

impl BlobBody {
    /// A body over already-final in-memory bytes — a sealed control object, or a
    /// test payload. `len` is the byte count.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        BlobBody {
            len: data.len() as u64,
            source: BlobSource::Buffered(Bytes::from(data)),
            carry: BytesMut::new(),
        }
    }

    pub async fn from_file(path: &Path) -> Result<Self, String> {
        let len = crate::storage::local_file::file_len(path).await?;
        let reader = crate::storage::local_file::open_reader(path).await?;
        Ok(Self::from_file_with_prefix(len, reader, None, Vec::new()))
    }

    #[cfg(test)]
    fn from_test_reader(len: u64, reader: PlaintextReader) -> Self {
        Self::from_file_with_prefix(len, reader, None, Vec::new())
    }

    pub(super) fn from_file_with_prefix(
        len: u64,
        reader: PlaintextReader,
        sealer: Option<SealedBlobSealer>,
        prefix: Vec<u8>,
    ) -> Self {
        BlobBody {
            len,
            source: BlobSource::File {
                reader,
                prefix: Bytes::from(prefix),
                sealer,
                sealed_any: false,
                eof: false,
            },
            carry: BytesMut::new(),
        }
    }

    /// Total bytes this body yields (encrypted or plaintext length).
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the body yields no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Pull bytes from the source until `carry` holds at least `min` or the source
    /// is exhausted.
    async fn fill(&mut self, min: usize) -> Result<(), CloudHomeError> {
        while self.carry.len() < min {
            match self.source.next_chunk().await? {
                Some(b) => self.carry.extend_from_slice(&b),
                None => break,
            }
        }
        Ok(())
    }

    /// Return at least `min` bytes — exactly `min` when more remain, the remainder
    /// at EOF — or `None` once fully drained. The driver calls this with the
    /// provider's part size, so every part except the last is exactly that size.
    pub async fn next_part(&mut self, min: usize) -> Result<Option<Bytes>, CloudHomeError> {
        let min = min.max(1);
        self.fill(min).await?;
        if self.carry.is_empty() {
            return Ok(None);
        }
        let take = self.carry.len().min(min);
        Ok(Some(self.carry.split_to(take).freeze()))
    }

    /// Drain the whole body into one `Vec`. Used ONLY by the single-request upload
    /// path for blobs at or below a provider's multipart threshold (bounded small).
    pub async fn collect(mut self) -> Result<Vec<u8>, CloudHomeError> {
        let mut out = Vec::with_capacity(self.len as usize);
        out.extend_from_slice(&self.carry);
        self.carry.clear();
        while let Some(b) = self.source.next_chunk().await? {
            out.extend_from_slice(&b);
        }
        Ok(out)
    }
}

/// The one per-provider streaming-upload surface: a session that accepts ordered
/// parts and commits. The central `write_blob` driver opens one of these for a
/// large blob and pumps [`BlobBody`] parts into it — no backend writes its own
/// upload loop, collect, or progress call.
#[async_trait]
pub trait PartSink: Send {
    /// Bytes per part. Every part except the last is exactly this; the last is the
    /// remainder. Encodes each provider's required part size (S3 ≥ 5 MiB, OneDrive
    /// 320 KiB multiples, Drive 256 KiB multiples, ...).
    fn part_size(&self) -> usize;

    /// Send one part. `offset` is its byte offset in the blob; `is_last` marks the
    /// final part (providers that commit on the last call use it).
    async fn send_part(
        &mut self,
        part: Bytes,
        offset: u64,
        is_last: bool,
    ) -> Result<(), CloudHomeError>;

    /// Cancel the open upload and remove its unpublished provider state. The
    /// upload owner awaits this operation and returns any cleanup failure to its
    /// caller; `Drop` must never block or terminate the process.
    async fn abort(&mut self) -> Result<(), CloudHomeError>;

    /// Commit the upload (e.g. S3 `complete_multipart_upload`); a no-op where the
    /// last `send_part` already committed.
    async fn finish(self: Box<Self>) -> Result<(), CloudHomeError>;
}

/// A boxed [`PartSink`] borrowing its home for `'a`.
pub type BoxPartSink<'a> = Box<dyn PartSink + 'a>;

/// Report an operation's failure, folding in a second failure that happened
/// while cleaning up after it. Both are kept: the cleanup failure is what left
/// remote state behind, the operation failure is why cleanup ran at all.
pub(crate) fn combine_cleanup_failure(
    operation: CloudHomeError,
    cleanup: Result<(), CloudHomeError>,
) -> CloudHomeError {
    match cleanup {
        Ok(()) => operation,
        Err(cleanup) => CloudHomeError::CleanupFailed {
            operation: Box::new(operation),
            cleanup: Box::new(cleanup),
        },
    }
}

/// One open multipart upload, including its source body, provider session, and
/// progress reporting. The operation either finishes the provider session or
/// awaits its abort and preserves both the operation and cleanup failures.
struct MultipartUpload<'sink, 'progress> {
    key: String,
    body: BlobBody,
    sink: BoxPartSink<'sink>,
    progress: &'progress UploadProgress<'progress>,
}

impl<'sink, 'progress> MultipartUpload<'sink, 'progress> {
    fn new(
        key: &str,
        body: BlobBody,
        sink: BoxPartSink<'sink>,
        progress: &'progress UploadProgress<'progress>,
    ) -> Self {
        Self {
            key: key.to_string(),
            body,
            sink,
            progress,
        }
    }

    async fn run(mut self) -> Result<(), CloudHomeError> {
        let part_size = self.sink.part_size();
        let total = self.body.len();
        let mut offset = 0u64;
        loop {
            let part = match self.body.next_part(part_size).await {
                Ok(Some(part)) => part,
                Ok(None) if offset == total => break,
                Ok(None) => {
                    let operation = CloudHomeError::Transport(format!(
                        "upload body for {} ended after {offset} of {total} bytes",
                        self.key
                    ));
                    return Err(self.abort(operation).await);
                }
                Err(operation) => return Err(self.abort(operation).await),
            };
            let n = part.len() as u64;
            let is_last = offset + n >= total;
            if let Err(operation) = self.sink.send_part(part, offset, is_last).await {
                return Err(self.abort(operation).await);
            }
            offset += n;
            (self.progress)(offset);
        }
        self.sink.finish().await
    }

    async fn abort(&mut self, operation: CloudHomeError) -> CloudHomeError {
        if matches!(&operation, CloudHomeError::CleanupFailed { .. }) {
            return operation;
        }
        combine_cleanup_failure(operation, self.sink.abort().await)
    }
}

/// Low-level cloud storage. Implementations handle a single store.
///
/// All methods deal in raw bytes. No encryption or path layout logic.
///
#[async_trait]
pub trait ExactSlotStorage: Send + Sync {
    async fn provider_binding(
        &self,
    ) -> Result<crate::storage::ResolvedProviderBinding, CloudHomeError>;

    async fn cross_principal_evidence(
        &self,
    ) -> Result<crate::protocol::provider::CrossPrincipalProviderEvidence, CloudHomeError> {
        use crate::protocol::provider::CrossPrincipalProviderEvidence;
        use crate::storage::{GoogleDriveCorpus, StoreProviderBinding};

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
        ObjectSlot::logical(logical_key.to_string())
    }

    async fn create_at(
        &self,
        slot: &ObjectSlot,
        body: BlobBody,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError>;

    async fn read_at(&self, slot: &ObjectSlot) -> Result<Vec<u8>, CloudHomeError>;

    async fn observe_at(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<crate::storage::ExactObjectRef>, CloudHomeError> {
        match self.read_at(slot).await {
            Ok(bytes) => Ok(Some(crate::storage::ExactObjectRef::new(
                slot.clone(),
                bytes.len() as u64,
                crate::protocol::store_commit::ObjectHash::digest(&bytes),
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
    fn exact_slot_storage(self: Arc<Self>) -> Option<Arc<dyn ExactSlotStorage>> {
        None
    }

    /// Verify the backend is reachable with the configured credentials.
    /// Setup flows call this *before* persisting credentials, so a typo or
    /// missing bucket fails fast at setup time instead of via a delayed
    /// reconnect banner. Default implementation issues a no-op list against
    /// a sentinel prefix — backends override with cheaper provider-specific
    /// auth checks (e.g. S3 HeadBucket) where available.
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

#[cfg(test)]
mod object_slot_tests {
    use super::*;

    #[test]
    fn deserialization_rejects_empty_slot_components() {
        assert!(serde_json::from_str::<ObjectSlot>(
            r#"{"logical_key":"","physical":{"kind":"logical_key"}}"#
        )
        .is_err());
        assert!(serde_json::from_str::<ObjectSlot>(
            r#"{"logical_key":"object","physical":{"kind":"opaque","value":""}}"#
        )
        .is_err());
    }

    #[test]
    fn logical_key_requirement_accepts_logical_slots() {
        let slot = ObjectSlot::logical("protocol/object".to_string()).expect("valid slot");

        slot.require_logical_key_for("S3")
            .expect("logical slot must be accepted");
    }

    #[test]
    fn logical_key_requirement_rejects_opaque_slots() {
        let slot = ObjectSlot::opaque(
            "protocol/object".to_string(),
            "provider-object-id".to_string(),
        )
        .expect("valid slot");

        let error = slot
            .require_logical_key_for("S3")
            .expect_err("opaque slot must be rejected");

        assert_eq!(
            error.to_string(),
            "configuration error: S3 slot for protocol/object must use its logical key"
        );
    }
}

#[cfg(test)]
mod join_info_tests {
    use super::*;

    /// The wire shape is the compact `{"t": "<short-tag>", ...}` form, not the
    /// derive default's `{"VariantName": {...}}` — invite and restore codes
    /// both wrap this type and rely on it staying compact.
    #[test]
    fn wire_shape_uses_short_t_tags() {
        let cases = [
            (
                CloudHomeJoinInfo::S3 {
                    bucket: "b".to_string(),
                    region: "r".to_string(),
                    endpoint: None,
                    access_key: "ak".to_string(),
                    secret_key: "sk".to_string(),
                    key_prefix: None,
                },
                "s3",
            ),
            (
                CloudHomeJoinInfo::GoogleDrive {
                    folder_id: "f".to_string(),
                },
                "gd",
            ),
            (
                CloudHomeJoinInfo::Dropbox {
                    folder_path: "/p".to_string(),
                },
                "db",
            ),
            (
                CloudHomeJoinInfo::OneDrive {
                    drive_id: "d".to_string(),
                    folder_id: "f".to_string(),
                },
                "od",
            ),
            (CloudHomeJoinInfo::CloudKit, "ck"),
            (
                CloudHomeJoinInfo::CloudKitShare {
                    share_url: "https://share.example".to_string(),
                    owner_name: "owner".to_string(),
                    zone_name: "zone".to_string(),
                },
                "cks",
            ),
        ];
        for (info, tag) in cases {
            let json = serde_json::to_value(&info).unwrap();
            assert_eq!(json["t"], tag, "{info:?} must tag as {tag:?}: {json}");
        }
    }

    #[test]
    fn debug_redacts_s3_secret_key() {
        let info = CloudHomeJoinInfo::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "s3-secret-value-do-not-print".to_string(),
            key_prefix: None,
        };
        let debug = format!("{info:?}");

        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(debug.contains("my-bucket"), "{debug}");
        assert!(debug.contains("AKIAIOSFODNN7EXAMPLE"), "{debug}");
        assert!(
            !debug.contains("s3-secret-value-do-not-print"),
            "S3 secret key leaked: {debug}"
        );
    }
}

#[cfg(test)]
mod retryable_tests {
    use super::*;

    #[test]
    fn transport_and_io_are_retryable_config_and_not_found_are_not() {
        assert!(CloudHomeError::Transport("timeout".to_string()).is_retryable());
        assert!(CloudHomeError::Io(std::io::Error::other("disk")).is_retryable());
        assert!(!CloudHomeError::Configuration("bucket not set".to_string()).is_retryable());
        assert!(!CloudHomeError::NotFound("key".to_string()).is_retryable());
        assert!(!CloudHomeError::AlreadyExists("key".to_string()).is_retryable());
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use crate::encryption::EncryptionService;
    const CHUNK_SIZE: usize = DEFAULT_BLOB_CHUNK_SIZE.get() as usize;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn service() -> EncryptionService {
        EncryptionService::from_key([7u8; 32])
    }

    /// Build a sealed [`BlobBody`] over a temp file holding `plaintext`. The
    /// returned `TempDir` keeps the file alive for the reader's life.
    async fn sealed_body(
        service: &EncryptionService,
        plaintext: &[u8],
    ) -> (tempfile::TempDir, BlobBody) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        std::fs::write(&path, plaintext).unwrap();
        let reader = crate::storage::local_file::open_reader(&path)
            .await
            .unwrap();
        let header = crate::encryption::SealedBlobHeader::new(
            crate::encryption::DEFAULT_BLOB_CHUNK_SIZE,
            plaintext.len() as u64,
        );
        let body = BlobBody::from_file_with_prefix(
            header.sealed_len(),
            reader,
            Some(service.blob_sealer(header, b"storage-cloud-test")),
            header.to_bytes().to_vec(),
        );
        (dir, body)
    }

    /// Drain a body via `next_part(min)`, concatenating every part.
    async fn drain(mut body: BlobBody, min: usize) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(part) = body.next_part(min).await.unwrap() {
            // Every part but the last is exactly `min` bytes.
            out.extend_from_slice(&part);
        }
        out
    }

    /// A sealed body's concatenated `next_part` output decrypts to the original
    /// plaintext, across the chunk boundaries that matter and several part sizes.
    #[tokio::test]
    async fn sealed_body_streams_then_decrypts() {
        let service = service();
        for &len in &[
            0usize,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            200_000,
        ] {
            let plaintext: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            for &min in &[1usize, 100, CHUNK_SIZE, CHUNK_SIZE + 13, 1 << 20] {
                let (_dir, body) = sealed_body(&service, &plaintext).await;
                let expected_len = body.len();
                let sealed = drain(body, min).await;
                assert_eq!(
                    sealed.len() as u64,
                    expected_len,
                    "streamed length wrong for len={len} min={min}"
                );
                let header = crate::encryption::SealedBlobHeader::parse(&sealed).unwrap();
                assert_eq!(header.plaintext_len(), len as u64);
                assert_eq!(
                    service
                        .blob_opener(header, b"storage-cloud-test")
                        .open_chunks(
                            0..header.chunk_count(),
                            &sealed[crate::encryption::SEALED_BLOB_HEADER_LEN..],
                        )
                        .unwrap(),
                    plaintext,
                    "sealed stream failed to round-trip for len={len} min={min}"
                );
            }
        }
    }

    /// Every non-final part is exactly `part_size`; the last is the remainder.
    #[tokio::test]
    async fn next_part_returns_exact_part_sizes() {
        let service = service();
        let plaintext = vec![0u8; CHUNK_SIZE * 3 + 17];
        let (_dir, mut body) = sealed_body(&service, &plaintext).await;
        let part_size = 1 << 20;
        let total = body.len();
        let mut offset = 0u64;
        while let Some(part) = body.next_part(part_size).await.unwrap() {
            offset += part.len() as u64;
            if offset < total {
                assert_eq!(
                    part.len(),
                    part_size,
                    "a non-final part must be exactly part_size"
                );
            } else {
                assert!(part.len() <= part_size, "the last part is the remainder");
            }
        }
        assert_eq!(offset, total);
    }

    /// `collect()` yields the same bytes as the concatenated `next_part` output —
    /// shown on a deterministic plaintext (passthrough) body so the two bodies
    /// produce identical bytes.
    #[tokio::test]
    async fn collect_equals_next_part_concatenation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        let plaintext: Vec<u8> = (0..200_003u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &plaintext).unwrap();

        let reader = crate::storage::local_file::open_reader(&path)
            .await
            .unwrap();
        let streamed = drain(
            BlobBody::from_file_with_prefix(plaintext.len() as u64, reader, None, Vec::new()),
            4096,
        )
        .await;
        assert_eq!(streamed, plaintext);

        let reader = crate::storage::local_file::open_reader(&path)
            .await
            .unwrap();
        let collected =
            BlobBody::from_file_with_prefix(plaintext.len() as u64, reader, None, Vec::new())
                .collect()
                .await
                .unwrap();
        assert_eq!(collected, plaintext);
        assert_eq!(collected, streamed);
    }

    /// A test home recording which upload path each write took and assembling the
    /// streamed parts so a multipart upload round-trips like a single PUT.
    struct RecordingHome {
        store: Mutex<HashMap<String, Vec<u8>>>,
        put_calls: AtomicUsize,
        multipart_calls: AtomicUsize,
        abort_calls: AtomicUsize,
        threshold: u64,
    }

    impl RecordingHome {
        fn new(threshold: u64) -> Self {
            RecordingHome {
                store: Mutex::new(HashMap::new()),
                put_calls: AtomicUsize::new(0),
                multipart_calls: AtomicUsize::new(0),
                abort_calls: AtomicUsize::new(0),
                threshold,
            }
        }
    }

    struct RecordingSink<'a> {
        home: &'a RecordingHome,
        key: String,
        buf: Vec<u8>,
    }

    #[async_trait]
    impl PartSink for RecordingSink<'_> {
        fn part_size(&self) -> usize {
            4 * 1024 * 1024
        }
        async fn send_part(
            &mut self,
            part: Bytes,
            offset: u64,
            _is_last: bool,
        ) -> Result<(), CloudHomeError> {
            assert_eq!(
                offset,
                self.buf.len() as u64,
                "parts arrive in order at the running offset"
            );
            self.buf.extend_from_slice(&part);
            Ok(())
        }
        async fn abort(&mut self) -> Result<(), CloudHomeError> {
            self.home.abort_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
            self.home.store.lock().unwrap().insert(self.key, self.buf);
            Ok(())
        }
    }

    #[async_trait]
    impl CloudHome for RecordingHome {
        async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            self.store.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }
        async fn open_multipart<'a>(
            &'a self,
            key: &str,
            _total_len: u64,
        ) -> Result<BoxPartSink<'a>, CloudHomeError> {
            self.multipart_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(RecordingSink {
                home: self,
                key: key.to_string(),
                buf: Vec::new(),
            }))
        }
        fn multipart_threshold(&self) -> u64 {
            self.threshold
        }
        async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.store
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| CloudHomeError::NotFound(key.to_string()))
        }
        async fn read_range(&self, _k: &str, _s: u64, _e: u64) -> Result<Vec<u8>, CloudHomeError> {
            unimplemented!()
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            unimplemented!()
        }
        async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
            unimplemented!()
        }
        async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
            unimplemented!()
        }
        async fn set_access(
            &self,
            _desired: CloudAccessState,
        ) -> Result<CloudAccessOutcome, CloudHomeError> {
            unimplemented!()
        }
    }

    /// A blob above the threshold streams through multipart, round-trips exactly,
    /// and reports monotonic progress reaching the full length.
    #[tokio::test]
    async fn write_blob_streams_large_blob_with_monotonic_progress() {
        let home = RecordingHome::new(8 * 1024 * 1024);
        let data: Vec<u8> = (0..20_000_003u32).map(|i| (i % 251) as u8).collect();
        let ticks = Mutex::new(Vec::<u64>::new());
        let progress = |n: u64| ticks.lock().unwrap().push(n);

        home.write("k", BlobBody::from_bytes(data.clone()), &progress)
            .await
            .unwrap();

        assert_eq!(home.multipart_calls.load(Ordering::SeqCst), 1);
        assert_eq!(home.put_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            home.read("k").await.unwrap(),
            data,
            "multipart upload round-trips"
        );

        let ticks = ticks.lock().unwrap();
        assert!(ticks.len() >= 2, "several progress ticks: {ticks:?}");
        for w in ticks.windows(2) {
            assert!(w[1] >= w[0], "progress went backwards: {ticks:?}");
        }
        assert_eq!(
            *ticks.last().unwrap(),
            data.len() as u64,
            "progress reaches the full length"
        );
    }

    #[tokio::test]
    async fn write_blob_aborts_when_the_body_ends_before_its_declared_length() {
        let home = RecordingHome::new(1);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.bin");
        std::fs::write(&path, [7; 4]).unwrap();
        let reader = crate::storage::local_file::open_reader(&path)
            .await
            .unwrap();
        let body = BlobBody::from_file_with_prefix(5, reader, None, Vec::new());

        let error = home
            .write("short", body, &no_progress())
            .await
            .expect_err("an incomplete body must not commit");

        assert!(
            error.to_string().contains("ended after 4 of 5 bytes"),
            "{error}"
        );
        assert_eq!(home.abort_calls.load(Ordering::SeqCst), 1);
        assert!(!home.store.lock().unwrap().contains_key("short"));
    }

    struct FailingPartHome {
        abort_calls: AtomicUsize,
    }

    struct FailingPartSink<'a> {
        home: &'a FailingPartHome,
    }

    #[async_trait]
    impl PartSink for FailingPartSink<'_> {
        fn part_size(&self) -> usize {
            2
        }

        async fn send_part(
            &mut self,
            _part: Bytes,
            _offset: u64,
            _is_last: bool,
        ) -> Result<(), CloudHomeError> {
            Err(CloudHomeError::Transport(
                "injected part failure".to_string(),
            ))
        }

        async fn abort(&mut self) -> Result<(), CloudHomeError> {
            self.home.abort_calls.fetch_add(1, Ordering::SeqCst);
            Err(CloudHomeError::Transport(
                "injected abort failure".to_string(),
            ))
        }

        async fn finish(self: Box<Self>) -> Result<(), CloudHomeError> {
            panic!("a failed part must not finish")
        }
    }

    #[async_trait]
    impl CloudHome for FailingPartHome {
        async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
            panic!("multipart test must not use put_object")
        }

        async fn open_multipart<'a>(
            &'a self,
            _key: &str,
            _total_len: u64,
        ) -> Result<BoxPartSink<'a>, CloudHomeError> {
            Ok(Box::new(FailingPartSink { home: self }))
        }

        fn multipart_threshold(&self) -> u64 {
            1
        }

        async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
            unimplemented!()
        }

        async fn read_range(
            &self,
            _key: &str,
            _start: u64,
            _end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            unimplemented!()
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            unimplemented!()
        }

        async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
            unimplemented!()
        }

        async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
            unimplemented!()
        }

        async fn set_access(
            &self,
            _desired: CloudAccessState,
        ) -> Result<CloudAccessOutcome, CloudHomeError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn write_blob_aborts_and_preserves_cleanup_failure_when_a_part_fails() {
        let home = FailingPartHome {
            abort_calls: AtomicUsize::new(0),
        };

        let error = home
            .write(
                "part-failure",
                BlobBody::from_bytes(vec![1, 2, 3]),
                &no_progress(),
            )
            .await
            .expect_err("a failed multipart part must abort its session");

        assert_eq!(home.abort_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(error, CloudHomeError::CleanupFailed { .. }));
        assert!(
            error.to_string().contains("injected part failure"),
            "{error}"
        );
        assert!(
            error.to_string().contains("injected abort failure"),
            "{error}"
        );
    }

    /// A blob at or below the threshold goes through `put_object` as one request.
    #[tokio::test]
    async fn write_blob_uses_put_object_below_threshold() {
        let home = RecordingHome::new(8 * 1024 * 1024);
        let data = vec![3u8; 1024];
        let total = Mutex::new(0u64);
        let progress = |n: u64| *total.lock().unwrap() = n;

        home.write("small", BlobBody::from_bytes(data.clone()), &progress)
            .await
            .unwrap();

        assert_eq!(home.put_calls.load(Ordering::SeqCst), 1);
        assert_eq!(home.multipart_calls.load(Ordering::SeqCst), 0);
        assert_eq!(home.read("small").await.unwrap(), data);
        assert_eq!(*total.lock().unwrap(), data.len() as u64);
    }
}
