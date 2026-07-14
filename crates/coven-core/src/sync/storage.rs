/// Storage access for immutable Store protocol objects and mutable blob/key data.
///
/// Layout:
/// ```text
/// store-v1/...                                   -- immutable protocol copies
/// {namespace}/{uploader}/{ab}/{cd}/{id}          -- blobs, hashed scheme (opaque home)
/// {namespace}/{cloud_path}                       -- blobs, plain scheme (browsable home)
/// membership/{author_pubkey}/{seq}{suffix}       -- membership entries
/// membership/{author_pubkey}/head{suffix}        -- that author's signed head
/// keys/{owner_pubkey}/{recipient_pubkey}{suffix} -- store key wrapped by an owner for a member
/// ```
///
/// The layout is aligned to one storage-access rule a provider ACL can enforce:
/// **a member writes (and deletes) only under its own public key; an owner may
/// write and delete anywhere.** Signed immutable Store objects bind each object
/// to its semantic slot; blobs and wrapped keys retain their dedicated mutable
/// paths.
///
/// A read that must span writers dispatches on where the object lives, never a
/// blind search: a member resolving its rotated store key reads
/// `keys/{owner}/{self}` across the current owners and adopts the
/// highest-generation wrap an owner's signature authenticates; a blob read keys
/// under the uploader recorded in the device-local `blob_uploaders` index (written
/// at pull from the changeset author, and at the device's own upload), falling
/// back for an unrecorded blob to a one-time listing scan that records what it
/// finds. The browsable plain scheme keeps human-readable `{namespace}/{cloud_path}`
/// keys with no uploader segment. It still has an owner-anchored membership chain;
/// plain object naming does not use the per-member encrypted-home path layout.
///
/// Blob keys follow the home's
/// [`BlobPathScheme`](crate::sync::cloud_storage::BlobPathScheme): the default
/// hashed scheme keys each blob under its uploader and shards by its id
/// (`{namespace}/{uploader}/{ab}/{cd}/{id}`); the plain scheme keys it at the
/// consumer-supplied readable path (`{namespace}/{cloud_path}`) so the bucket is
/// browsable. A device only ever writes blobs it authored, so a write keys under
/// itself; a read resolves the uploader (which may be a peer) and keys under it.
/// The blob-path scheme is independent of the at-rest cipher below.
///
/// An encrypted home seals every object under the store key before upload and
/// opens it after download; a plaintext home stores and serves objects verbatim.
/// The trait is async and mockable for testing.
use async_trait::async_trait;
use std::path::Path;

use crate::storage::cloud::{AppendedObject, ListingCoverage};

/// Runtime locator for one physical copy of a Store protocol object.
///
/// The raw provider locator is deliberately private and never serialized into a
/// signed object or database row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolObjectLocator {
    logical_key: String,
    physical: AppendedObject,
}

impl ProtocolObjectLocator {
    pub(crate) fn new(logical_key: String, physical: AppendedObject) -> Self {
        Self {
            logical_key,
            physical,
        }
    }

    pub fn logical_key(&self) -> &str {
        &self.logical_key
    }

    pub(crate) fn physical(&self) -> &AppendedObject {
        &self.physical
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolObjectListing {
    pub objects: Vec<ProtocolObjectLocator>,
    pub coverage: ListingCoverage,
}

/// Error type for storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage operation failed: {0}")]
    Storage(String),
    #[error("storage object parse failed: {0}")]
    Parse(String),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("decryption failed: {0}")]
    Decryption(String),
    /// This device has not adopted a store-key rotation the cloud already
    /// committed; see [`crate::sync::cloud_storage::RotationPending`].
    #[error("{0}")]
    RotationPending(#[from] crate::sync::cloud_storage::RotationPending),
}

impl From<crate::storage::cloud::CloudHomeError> for StorageError {
    fn from(e: crate::storage::cloud::CloudHomeError) -> Self {
        match e {
            crate::storage::cloud::CloudHomeError::NotFound(key) => StorageError::NotFound(key),
            crate::storage::cloud::CloudHomeError::Configuration(msg)
            | crate::storage::cloud::CloudHomeError::Transport(msg) => StorageError::Storage(msg),
            crate::storage::cloud::CloudHomeError::Io(io_err) => {
                StorageError::Storage(format!("I/O error: {io_err}"))
            }
        }
    }
}

impl From<crate::store_dir::PathTokenError> for StorageError {
    /// A blob id/namespace/cloud_path that can't form a safe object key is bad
    /// data, surfaced so the caller refuses the blob rather than reaching storage
    /// with a key that could escape its prefix.
    fn from(e: crate::store_dir::PathTokenError) -> Self {
        StorageError::Parse(format!("unsafe blob path: {e}"))
    }
}

#[async_trait]
pub trait SyncStorage: Send + Sync {
    /// Append one physical copy beneath a signed semantic prefix. `extension`
    /// includes the leading dot (`.json`, `.pkg`, or `.db`). The implementation
    /// injects a fresh copy id and applies its at-rest suffix below this API.
    async fn append_protocol_object(
        &self,
        semantic_prefix: &str,
        extension: &str,
        data: Vec<u8>,
    ) -> Result<ProtocolObjectLocator, StorageError> {
        let _ = (semantic_prefix, extension, data);
        Err(StorageError::Storage(
            "Store protocol append is not implemented by this storage".to_string(),
        ))
    }

    /// List all physical Store protocol copies under `prefix`, preserving
    /// duplicate provider ids.
    async fn list_protocol_objects(
        &self,
        prefix: &str,
    ) -> Result<ProtocolObjectListing, StorageError> {
        let _ = prefix;
        Err(StorageError::Storage(
            "Store protocol listing is not implemented by this storage".to_string(),
        ))
    }

    /// Read and open one exact physical Store protocol copy using the signed
    /// semantic prefix as encryption AAD.
    async fn read_protocol_object(
        &self,
        object: &ProtocolObjectLocator,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let _ = (object, semantic_prefix);
        Err(StorageError::Storage(
            "Store protocol locator read is not implemented by this storage".to_string(),
        ))
    }

    /// Delete one exact physical Store protocol copy.
    async fn delete_protocol_object(
        &self,
        object: &ProtocolObjectLocator,
    ) -> Result<(), StorageError> {
        let _ = object;
        Err(StorageError::Storage(
            "Store protocol locator delete is not implemented by this storage".to_string(),
        ))
    }

    /// Upload a blob. Under the hashed (default) scheme it is keyed
    /// `{namespace}/{uploader}/{id[0..2]}/{id[2..4]}/{id}` under this device's own
    /// public key (`cloud_path` ignored); under the plain scheme it is keyed
    /// `{namespace}/{cloud_path}` verbatim, so the bucket is browsable, and a
    /// missing `cloud_path` is an error. A device only ever uploads blobs it
    /// authored, so a write always keys under itself.
    /// On an encrypted home the plaintext is sealed with the key `scope` selects
    /// (master, or a per-scope derived key); on a plaintext home it is stored
    /// verbatim (scope ignored).
    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Upload a blob from a local plaintext file without reading the whole file
    /// into memory. Same keying, scope, and at-rest protection as [`Self::put_blob`].
    async fn put_blob_from_file(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_path: &Path,
    ) -> Result<(), StorageError>;

    /// Download and open a blob, keyed
    /// `{namespace}/{uploader}/{id[0..2]}/{id[2..4]}/{id}` under the hashed scheme
    /// or `{namespace}/{cloud_path}` under the plain one, using the key the
    /// resolved `scope` selects on an encrypted home (verbatim on a plaintext one).
    /// `uploader` is the hex public key of the device that uploaded the blob (the
    /// caller resolves it — a peer's, not necessarily this device's); it is
    /// required by the hashed scheme and ignored by the plain one (a browsable home
    /// carries no uploader segment). A plain-scheme home with no `cloud_path` is an
    /// error.
    async fn get_blob(
        &self,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError>;

    /// Check whether a blob object exists at the same key [`Self::put_blob`] and
    /// [`Self::get_blob`] use. This does not read or open the blob; publish
    /// preflights use it to prove a row about to be published will not point at a
    /// missing remote object.
    async fn blob_exists(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<bool, StorageError>;

    /// Serve `len` plaintext bytes of a blob starting at `offset`, without
    /// downloading the whole object — the ranged sibling of [`Self::get_blob`].
    /// Keyed the same way `get_blob` keys it (hashed shard or `cloud_path`), and
    /// decrypted under the key the resolved `scope` selects on an encrypted home
    /// (read verbatim on a plaintext one).
    ///
    /// `source_size` is the blob's plaintext length, which the caller knows (the
    /// host row that owns the blob carries it) and the implementation needs to
    /// validate the range and locate the covering encrypted chunks — the object's
    /// stored length alone doesn't give it (the nonce header and per-chunk
    /// authentication tags pad it). An out-of-range request (`offset + len` past
    /// `source_size`, or an overflow) errors rather than truncating; `len == 0` is
    /// an empty result. The cache layer ([`crate::blob::cache::open_blob_stream`])
    /// uses this only on a miss — a cache hit reads the local plaintext file
    /// directly.
    async fn read_blob_range(
        &self,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StorageError>;

    /// Download and open a blob into `dest` without holding the whole plaintext in
    /// memory. Same keying, scope, and validation as [`Self::read_blob_range`],
    /// writing exactly `source_size` bytes or failing.
    ///
    /// `expected_hash` is the blob's author-signed content hash (see
    /// [`crate::blob::content_hash`]); the implementation streams the decrypted
    /// plaintext through an incremental hasher and refuses to commit the file
    /// unless the whole-blob hash matches — so a tampered or rolled-back object
    /// never lands in the cache. The cheap `source_size` length check stays as the
    /// early-out; the hash is the authority.
    async fn read_blob_to_file(
        &self,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_size: u64,
        expected_hash: &str,
        dest: &Path,
    ) -> Result<(), StorageError>;

    /// The blob-path scheme this home uses. The read dispatch consults it to decide
    /// whether a blob key needs an uploader segment (hashed) or not (plain).
    fn blob_path_scheme(&self) -> crate::sync::cloud_storage::BlobPathScheme;

    /// The cloud object key this home stores `(namespace, id, cloud_path)` under — the
    /// same key [`Self::put_blob_from_file`] writes and beside which a tombstone for the
    /// blob is keyed. The inline host-provided upload path uses it to cancel a pending
    /// tombstone at the exact key its (re-)upload wrote, rather than re-deriving the
    /// scheme (which the home alone authoritatively knows).
    fn blob_cloud_key(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, StorageError>;

    /// This device's own `{uploader}` segment — the hex public key its blob uploads
    /// key under on a hashed home, or `None` on a browsable home (which carries no
    /// uploader segment). The upload path records it in the local uploader index as
    /// this device's own authoritative uploader for the blobs it introduces, so a
    /// later self-read (after a cache eviction) resolves the blob straight from that
    /// index.
    fn own_uploader(&self) -> Option<String>;

    /// Upload a wrapped store key that `owner_pubkey` sealed for `recipient_pubkey`.
    /// Writes to `keys/{owner_pubkey_hex}/{recipient_pubkey_hex}{suffix}`. An owner
    /// wraps only into its own prefix, so a recipient can hold a wrap from each
    /// owner and no two owners contend for one slot. The bytes are already a sealed
    /// box, so the home cipher stores them verbatim regardless of suffix.
    async fn put_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Download the wrapped store key `owner_pubkey` sealed for `recipient_pubkey`.
    /// Reads from `keys/{owner_pubkey_hex}/{recipient_pubkey_hex}{suffix}`.
    /// `create_invitation` reads the inviting owner's existing slot for the invitee
    /// before overwriting it, so a failed invite can restore the exact prior object
    /// rather than stripping a re-invited member's wrapped key. Returns `NotFound`
    /// when that owner has no wrapped key for the recipient yet.
    async fn get_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<Vec<u8>, StorageError>;

    /// Delete the wrapped store key `owner_pubkey` sealed for `recipient_pubkey`.
    /// Removes `keys/{owner_pubkey_hex}/{recipient_pubkey_hex}{suffix}`. An owner
    /// can delete only wraps in its own prefix; a revoked member's wraps under
    /// other owners' prefixes are pre-rotation (they wrap a key the member already
    /// held) and are reclaimed when those owners next rotate.
    async fn delete_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<(), StorageError>;
}
