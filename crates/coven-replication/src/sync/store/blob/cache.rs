//! The device-local cache for **Remote** blobs: bytes on disk, keyed by the
//! exact locator hash, with the folder the file lives in as the only retention
//! truth. This file holds the cloud access, the error type, and [`BlobStream`];
//! the cache operations themselves are `StoreBlobCache` in the parent module.
//!
//! The cache holds copies of Remote blobs only — re-fetchable from the cloud,
//! evictable to a size budget, kept-or-dropped per pin. A **Local** blob is not in
//! the cache: a user-provided Local blob is the user's own file at a path (an
//! external ref); a host-provided Local blob is in the local store
//! (`storage/local`). So `CacheEager`/`CacheLazy`/pin/budget all describe a blob
//! only while it is Remote. See the blob concept tree in
//! [`coven_protocol::blob`] for where the cache sits in the whole storage model.
//!
//! There is no cache table. A cached Remote blob is in **exactly one** of two
//! folders under the store dir, or in neither. Both are segmented by the blob's
//! namespace, so each namespace's cache evicts against its own budget without
//! touching another's:
//!
//! - `storage/pinned/<namespace>/{ab}/{cd}/<locator-hash>` — kept, budget-exempt. A Remote
//!   blob's cache copy the user pinned for offline (kept from eviction).
//! - `storage/cache/<namespace>/{ab}/{cd}/<locator-hash>` — opportunistic, evictable. A blob
//!   fetched on read (`CacheLazy`) or eagerly after pull (`CacheEager`).
//! - neither — not cached. No file; fetched from the cloud on the next read.
//!
//! Presence is the file on disk; kept-ness is which folder. Nothing the two
//! `readdir`s can't answer, so no metadata sidecar to keep in sync with the disk.
//! A corrupt occupied path fails loudly and is never replaced. Pin/unpin stage
//! a verified copy, publish it without replacing an occupied destination, then
//! remove the source.
//!
//! Reads **dispatch on the reference's authority** — they never probe every
//! store and take the first hit. The host passes a `RowBlobRef`; its authority
//! comes from the row's audience when the reference was taken, and every read
//! re-checks the reference against the live row before and after touching
//! bytes. The authority plus the blob's **provenance** pick the source:
//!
//! - **Remote** ⇒ the bytes live in the cloud fronted by the device cache:
//!   check `pinned/` then `cache/`, then fetch the exact cloud object.
//! - **PendingRemote** ⇒ the row's audience is Store or a Circle but no cloud
//!   object is recorded for it yet. Reads use the same local source as Local.
//! - **Local** ⇒ the bytes are on-device; provenance picks the copy. A
//!   **user-provided** blob is the user's own external file (`local_blob_refs`),
//!   and its ref MUST exist ([`BlobCacheError::NoExternalRef`] otherwise). A
//!   **host-provided** blob is in the **local store**, its only copy — a miss is
//!   fail-loud corruption ([`BlobCacheError::NoLocalCopy`]). Neither falls
//!   through to the cloud: a Local blob has no cloud copy.
//!
//! `read_blob` returns the entire blob in one call; `open_blob_stream` returns a
//! [`BlobStream`] a host reads ranges from while streaming or seeking. Both resolve
//! the source the same way, and each verifies what its own shape allows:
//!
//! - `read_blob` reads every byte, so it checks the plaintext's size and content
//!   hash against the row — from an external file, the local store, or a
//!   **cache hit**. A cache file is plaintext on local disk with no tags of its
//!   own, so the row's hash is the only thing that can refuse a file that
//!   rotted, was truncated by a partial write, or was edited. A cloud miss
//!   fetches the exact object once and populates `cache/`.
//! - `open_blob_stream` costs each range its own bytes, so it does not hash a
//!   local source. An external file's length is checked against its
//!   registration and the row when it opens; a local-store or cache copy is
//!   opened as it is. A **Remote uncached** blob is read from the cloud object
//!   a chunk at a time: each sealed chunk's tag covers its bytes, its index,
//!   and the header framing the blob, so a chunk that opens is authentic. A
//!   blob stored in the clear (a browsable home) has no tags to check a range
//!   against, so it is downloaded whole, hashed against the row, cached, and
//!   streamed from the cached copy.
//!
//! A local stream holds the file it opened for its whole life. That is a property,
//! not an optimization — a path can be swapped between two reads, a descriptor
//! cannot, so the stream keeps serving the file it opened even after that file is
//! evicted, renamed, or replaced.
//!
//! The cache has a **per-namespace** size budget the host sets per device (see
//! `StoreDatabase::set_cache_budget`), so a small namespace (`covers`) is never
//! wiped by pressure from a big one (`release_files`). A namespace's budget
//! counts **only** the files under `cache/<namespace>/` — `pinned/` is
//! structurally exempt, and `storage/local` (the local store) is never walked
//! at all. After a fetch or copy into `cache/<namespace>/` (not after unpin
//! moves a file there), `enforce_budget` sums that namespace's files and, if
//! their total exceeds its budget, deletes the oldest by modification time
//! until the total is back under it, never the file just written.
//! Modification time is the recency proxy — there is no `last_accessed`
//! column, the same folder-truth trade-off the whole cache makes. With a
//! namespace's budget unset eviction is off for it and its cache grows without
//! bound.

use coven_database::DbError;
use coven_foundation::atomic_file::FileError;
use coven_foundation::local_file::CommitNewFileError;
use coven_foundation::store_dir::{
    CachedLocatorRemovalError, PathTokenError, RequiredLocalBlobPathError, StoreBlobFileError,
};
use coven_protocol::objects::StorageError;
use coven_storage::CloudSyncObjectStorage;

/// Closed cloud access for one exact Remote blob. Store code resolves the
/// authority; the cache only reads bytes with the supplied protection.
pub(crate) struct RemoteBlobAccess<'a> {
    storage: &'a dyn CloudSyncObjectStorage,
    protection: RemoteBlobProtection,
}

enum RemoteBlobProtection {
    Store,
    Circle(coven_protocol::objects::BlobSpoolProtection),
}

impl<'a> RemoteBlobAccess<'a> {
    pub(crate) fn circle(
        storage: &'a dyn CloudSyncObjectStorage,
        protection: coven_protocol::objects::BlobSpoolProtection,
    ) -> Self {
        Self {
            storage,
            protection: RemoteBlobProtection::Circle(protection),
        }
    }

    pub(crate) fn store(storage: &'a dyn CloudSyncObjectStorage) -> Self {
        Self {
            storage,
            protection: RemoteBlobProtection::Store,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn key_fingerprint(
        &self,
    ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, StorageError> {
        match &self.protection {
            RemoteBlobProtection::Store => self.storage.store_blob_key_fingerprint(),
            RemoteBlobProtection::Circle(coven_protocol::objects::BlobSpoolProtection::Opaque(
                encryption,
            )) => Ok(Some(encryption.seal_key_fingerprint())),
            RemoteBlobProtection::Circle(
                coven_protocol::objects::BlobSpoolProtection::Browsable,
            ) => Ok(None),
        }
    }

    pub(super) async fn stage_verified_plaintext(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, BlobCacheError> {
        match &self.protection {
            RemoteBlobProtection::Store => {
                self.storage
                    .stage_verified_store_blob_plaintext(stored, stage, progress)
                    .await
            }
            RemoteBlobProtection::Circle(protection) => {
                self.storage
                    .stage_verified_blob_plaintext(stored, protection.clone(), stage, progress)
                    .await
            }
        }
        .map_err(Into::into)
    }

    pub(super) async fn open_range_reader(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<coven_storage::BlobRangeReader, BlobCacheError> {
        match &self.protection {
            RemoteBlobProtection::Store => self.storage.open_store_blob_range_reader(stored).await,
            RemoteBlobProtection::Circle(protection) => {
                self.storage
                    .open_blob_range_reader(stored, protection.clone())
                    .await
            }
        }
        .map_err(Into::into)
    }
}

/// Why a blob-cache operation failed.
#[derive(Debug)]
pub enum BlobCacheError {
    /// A blob `id`/`namespace`/`cloud_path` that can't form a safe path — bad data
    /// that could escape the store dir or can't be partitioned. The blob is
    /// refused before any path is built (the same gate the pull runs).
    Path(PathTokenError),
    /// A cloud read failed: the blob isn't in the cloud, or the backend errored
    /// (surfaced from the exact blob operations on `CloudSyncObjectStorage`).
    Storage(StorageError),
    /// A Remote blob has no copy on this device and no cloud home is connected
    /// to fetch it from.
    NoCloudHome,
    /// A local file operation on a blob file failed.
    File(FileError),
    /// Publishing a staged cache file failed.
    Commit(CommitNewFileError),
    /// A database read the blob operation depends on failed, or the reference
    /// no longer matches the live row: re-checking the reference, looking up
    /// its external ref or Circle protection, or reading its cache budget.
    Metadata(DbError),
    /// Building the sync storage from config failed — missing credentials or cloud
    /// configuration — when a Remote blob needed it. A configuration fault, not a
    /// disk I/O error.
    StorageSetup(coven_storage::cloud::setup::StorageSetupError),
    /// The blob's declared authority cannot open its stored representation.
    OpeningAuthority(coven_protocol::blob::BlobOpeningAuthorityError),
    /// A registered external blob ref (a user-provided Local blob's user-owned
    /// file) points at a file that is no longer there — the user moved, renamed, or
    /// deleted it. Terminal: an external blob has no cloud copy to fall back to, so
    /// this never re-fetches. The host surfaces a "files missing / moved" state
    /// whose actions are relocate (pick the new folder, re-register) or re-import.
    ExternalMissing {
        id: String,
        path: std::path::PathBuf,
        /// The underlying read failure — a missing file or a real I/O error,
        /// preserved rather than collapsed so the host sees why the read failed.
        source: FileError,
    },
    /// A registered external blob's file is present but its length no longer matches
    /// the registered `size` — the user truncated it or replaced it with a
    /// different-length file. Terminal like [`Self::ExternalMissing`]: a mismatch
    /// means this is not the exact file coven registered.
    ExternalSizeMismatch {
        id: String,
        path: std::path::PathBuf,
    },
    /// A local-store or kept blob copy has a different length from its stored
    /// declaration.
    LocalSizeMismatch {
        path: std::path::PathBuf,
        expected_size: u64,
        actual_size: u64,
    },
    /// A host-provided blob whose authority is Local or PendingRemote has no
    /// file in the local store. That file is its only copy, so this is broken
    /// state, not a cache miss, and is never answered from the cloud.
    NoLocalCopy { namespace: String, id: String },
    /// An operation that needs the blob's cloud object (a cloud fetch, pin,
    /// unpin, evict, or pinned check) got a reference that has none recorded:
    /// its authority is Local or PendingRemote.
    LocalityUnresolved { id: String },
    /// A user-provided blob whose authority is Local or PendingRemote has no
    /// `local_blob_refs` row for this row version. Its bytes live only at the
    /// registered path, so a missing ref is corruption, not a cache miss to fall
    /// through — surfaced loud so the host repairs or re-imports.
    NoExternalRef { id: String },
    /// A local plaintext file (an external file, a local-store copy, or a cache
    /// copy) differs from the row's size/hash.
    LocalIntegrity {
        path: std::path::PathBuf,
        expected_size: u64,
        actual_size: u64,
        expected_hash: coven_protocol::store_commit::ObjectHash,
        actual_hash: coven_protocol::store_commit::ObjectHash,
    },
    /// Adding the requested range length overflowed its offset.
    RangeOverflow { id: String, offset: u64, len: u64 },
    /// The requested range lies outside the opened blob.
    RangeOutOfBounds {
        id: String,
        offset: u64,
        end: u64,
        size: u64,
    },
}

/// A Remote blob read needs sync storage; if building it from config fails
/// (missing credentials or cloud configuration) the read surfaces that as a
/// configuration fault, not a disk I/O error. The cache error preserves the
/// setup failure's message at this API boundary.
impl From<coven_storage::cloud::setup::StorageSetupError> for BlobCacheError {
    fn from(e: coven_storage::cloud::setup::StorageSetupError) -> Self {
        BlobCacheError::StorageSetup(e)
    }
}

impl std::fmt::Display for BlobCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobCacheError::Path(e) => write!(f, "blob path error: {e}"),
            BlobCacheError::Storage(e) => write!(f, "blob cache storage error: {e}"),
            BlobCacheError::NoCloudHome => {
                write!(f, "no cloud home connected to read a Remote blob")
            }
            BlobCacheError::File(e) => write!(f, "blob cache file error: {e}"),
            BlobCacheError::Commit(e) => write!(f, "publish blob cache file: {e}"),
            BlobCacheError::Metadata(e) => write!(f, "blob metadata error: {e}"),
            BlobCacheError::StorageSetup(e) => write!(f, "sync storage setup failed: {e}"),
            BlobCacheError::OpeningAuthority(e) => write!(f, "blob opening authority: {e}"),
            BlobCacheError::ExternalMissing { id, path, source } => write!(
                f,
                "external blob {id} could not be read at {}: {source}",
                path.display()
            ),
            BlobCacheError::ExternalSizeMismatch { id, path } => write!(
                f,
                "external blob {id} at {} no longer matches its registered size",
                path.display()
            ),
            BlobCacheError::LocalSizeMismatch {
                path,
                expected_size,
                actual_size,
            } => write!(
                f,
                "local blob {} has {actual_size} bytes, expected {expected_size}",
                path.display()
            ),
            BlobCacheError::NoLocalCopy { namespace, id } => write!(
                f,
                "local blob {namespace}/{id} is gated Local but absent from the local store"
            ),
            BlobCacheError::LocalityUnresolved { id } => write!(
                f,
                "cannot resolve locality for blob {id}: no locality root determines where it lives"
            ),
            BlobCacheError::NoExternalRef { id } => write!(
                f,
                "user-provided Local blob {id} has no registered external ref"
            ),
            BlobCacheError::LocalIntegrity {
                path,
                expected_size,
                actual_size,
                expected_hash,
                actual_hash,
            } => write!(
                f,
                "local blob {} has size/hash {actual_size}/{actual_hash}, expected {expected_size}/{expected_hash}",
                path.display()
            ),
            BlobCacheError::RangeOverflow { id, offset, len } => write!(
                f,
                "blob range overflow for {id}: offset={offset}, len={len}"
            ),
            BlobCacheError::RangeOutOfBounds {
                id,
                offset,
                end,
                size,
            } => write!(
                f,
                "blob range {offset}..{end} for {id} exceeds blob size {size}"
            ),
        }
    }
}

impl std::error::Error for BlobCacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Path(source) => Some(source),
            Self::Storage(source) => Some(source),
            Self::File(source) => Some(source),
            Self::Commit(source) => Some(source),
            Self::Metadata(source) => Some(source),
            Self::StorageSetup(source) => Some(source),
            Self::OpeningAuthority(source) => Some(source),
            Self::ExternalMissing { source, .. } => Some(source),
            Self::NoCloudHome
            | Self::ExternalSizeMismatch { .. }
            | Self::LocalSizeMismatch { .. }
            | Self::NoLocalCopy { .. }
            | Self::LocalityUnresolved { .. }
            | Self::NoExternalRef { .. }
            | Self::LocalIntegrity { .. }
            | Self::RangeOverflow { .. }
            | Self::RangeOutOfBounds { .. } => None,
        }
    }
}

impl From<PathTokenError> for BlobCacheError {
    fn from(e: PathTokenError) -> Self {
        BlobCacheError::Path(e)
    }
}

impl From<RequiredLocalBlobPathError> for BlobCacheError {
    fn from(error: RequiredLocalBlobPathError) -> Self {
        match error {
            RequiredLocalBlobPathError::Path(error) => Self::Path(error),
            RequiredLocalBlobPathError::Missing { namespace, id } => {
                Self::NoLocalCopy { namespace, id }
            }
            RequiredLocalBlobPathError::File(error) => Self::File(error),
        }
    }
}

impl BlobCacheError {
    /// What the storage provider answered, when a provider answer is why the
    /// blob could not be read: bad credentials, a missing bucket, no permission,
    /// a transport failure. `None` for failures on this device (a missing
    /// file, a database error) and for a store with no cloud home connected.
    pub fn backend_failure(&self) -> Option<coven_protocol::objects::StorageBackendFailure> {
        match self {
            Self::Storage(error) => error.backend_failure(),
            Self::StorageSetup(coven_storage::cloud::setup::StorageSetupError::CloudHome(
                error,
            )) => error.backend_failure(),
            _ => None,
        }
    }
}

impl From<CachedLocatorRemovalError> for BlobCacheError {
    fn from(error: CachedLocatorRemovalError) -> Self {
        match error {
            CachedLocatorRemovalError::Path(error) => Self::Path(error),
            CachedLocatorRemovalError::File(error) => Self::File(error),
        }
    }
}

impl From<StoreBlobFileError> for BlobCacheError {
    fn from(error: StoreBlobFileError) -> Self {
        match error {
            StoreBlobFileError::Path(error) => Self::Path(error),
            StoreBlobFileError::File(error) => Self::File(error),
            StoreBlobFileError::Commit(error) => Self::Commit(error),
            StoreBlobFileError::SizeMismatch {
                path,
                expected_size,
                actual_size,
            } => Self::LocalSizeMismatch {
                path,
                expected_size,
                actual_size,
            },
            StoreBlobFileError::Integrity {
                path,
                expected_size,
                actual_size,
                expected_hash,
                actual_hash,
            } => Self::LocalIntegrity {
                path,
                expected_size,
                actual_size,
                expected_hash,
                actual_hash,
            },
        }
    }
}

impl From<DbError> for BlobCacheError {
    fn from(error: DbError) -> Self {
        Self::Metadata(error)
    }
}

impl From<StorageError> for BlobCacheError {
    fn from(e: StorageError) -> Self {
        BlobCacheError::Storage(e)
    }
}

impl From<coven_foundation::store_dir::LocalBlobStoreError> for BlobCacheError {
    fn from(e: coven_foundation::store_dir::LocalBlobStoreError) -> Self {
        use coven_foundation::store_dir::LocalBlobStoreError;
        match e {
            LocalBlobStoreError::Path(p) => BlobCacheError::Path(p),
            LocalBlobStoreError::File(error) => BlobCacheError::File(error),
            LocalBlobStoreError::SizeMismatch {
                path,
                expected_size,
                actual_size,
            } => BlobCacheError::LocalSizeMismatch {
                path,
                expected_size,
                actual_size,
            },
        }
    }
}

/// One opened blob, ready to serve ranges. Held by a host that is streaming or
/// seeking a blob (playback probing a codec header, then a tail, then decoding
/// forward) rather than loading it whole.
///
/// A range costs the bytes it returns, from either source, but for different
/// reasons:
///
/// - **Local** (an external file, the local store, or a cache copy) — the stream
///   holds the open file and every range is one positioned read of it. Nothing
///   is hashed against the row; opening an external file checks only its
///   length. Whole reads, materialization, pinning, and upload preparation are
///   what check a blob's bytes against the row's hash.
/// - **Remote, uncached, sealed** — the stream fetches only the sealed chunks
///   covering the range and opens them. A chunk that opens is authentic: the provider
///   holds no key and cannot forge a tag, and the tag covers the chunk's bytes,
///   its index, and the header framing the blob. So verification is per chunk,
///   which is what lets a range cost a range rather than the object. A blob in
///   a browsable home has no tags, so opening it downloads it whole, checks it
///   against the row's hash, caches it, and streams the cache copy.
///
/// Holding the local descriptor is a property, not an optimization: a path can be
/// replaced between two reads, a descriptor cannot, so the stream keeps serving
/// the file it opened even if that file is later evicted, renamed, or replaced.
/// An **in-place** rewrite of that same file does reach the stream — that is a
/// file the user owns and edits; coven's own copies are published by rename and
/// never written in place.
pub struct BlobStream {
    blob: coven_protocol::blob::BlobRef,
    source: BlobStreamSource,
}

/// Where an open stream's ranges come from.
pub(super) enum BlobStreamSource {
    /// A file on this device: the user's own external file, the local store, or
    /// a cache copy of a Remote blob.
    Local(coven_foundation::local_file::OpenFile),
    /// A sealed Remote blob with no cache copy: ranges are served from the cloud
    /// object a chunk at a time.
    Remote(coven_storage::BlobRangeReader),
}

impl BlobStream {
    pub(super) fn from_source(
        blob: coven_protocol::blob::BlobRef,
        source: BlobStreamSource,
    ) -> Self {
        Self { blob, source }
    }

    /// The blob's whole plaintext length. Every range must lie inside it.
    pub fn plaintext_size(&self) -> u64 {
        match &self.source {
            BlobStreamSource::Local(file) => file.size(),
            BlobStreamSource::Remote(reader) => reader.plaintext_size(),
        }
    }

    /// Serve `len` plaintext bytes starting at `offset`.
    ///
    /// `len == 0` is an empty result, and an `offset + len` past the blob's
    /// plaintext size (or an overflow) is an error, never a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, BlobCacheError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| BlobCacheError::RangeOverflow {
                id: self.blob.id.clone(),
                offset,
                len,
            })?;
        let source_size = self.plaintext_size();
        if end > source_size {
            return Err(BlobCacheError::RangeOutOfBounds {
                id: self.blob.id.clone(),
                offset,
                end,
                size: source_size,
            });
        }
        match &self.source {
            BlobStreamSource::Local(file) => file
                .read_at(offset, len)
                .await
                .map_err(BlobCacheError::File),
            BlobStreamSource::Remote(reader) => reader
                .read_at(offset, len)
                .await
                .map_err(BlobCacheError::Storage),
        }
    }
}
