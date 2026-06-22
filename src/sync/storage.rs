/// Sync storage: reads/writes to the layout used for changeset sync.
///
/// The `{suffix}` is `.enc` for an encrypted home and empty for a plaintext one,
/// so an encrypted home's keys carry `.enc` (`snapshot/{author}/0.db.enc`,
/// `heads/{device}.json.enc`, …) and a plaintext home's are bare
/// (`snapshot/{author}/0.db`, `heads/{device}.json`, …).
///
/// Layout:
/// ```text
/// changes/{device_id}/{seq}{suffix}              -- changeset envelopes
/// heads/{device_id}.json{suffix}                 -- head pointers
/// images/{ab}/{cd}/{id}                          -- library images (blobs), hashed scheme
/// images/{cloud_path}                            -- library images (blobs), plain scheme
/// snapshot/{author}/{seq}.db{suffix}             -- a generation's full DB snapshot
/// snapshot/{author}/{seq}_meta.json{suffix}      -- a generation's per-device cursors
/// snapshot/current.json{suffix}                  -- signed pointer naming the live {author, seq}
/// membership/{author_pubkey}/{seq}{suffix}       -- membership entries
/// keys/{user_pubkey}{suffix}                     -- wrapped library keys per member
/// ```
///
/// A snapshot is published as a generation under the publishing device's
/// `{author}` (its hex public key): the `{author}/{seq}.db` and then the
/// `{author}/{seq}_meta.json` object are written first, then the single
/// `current.json` pointer last. The pointer carries the live generation's
/// `{author_pubkey, seq}`, so a reader resolves the pointer, then the generation
/// it names — always a whole, self-consistent generation. Keying each device's
/// generations under its own `{author}` makes them globally unique: two devices
/// publishing at the same `seq` (each `seq` is the publisher's own `local_seq`,
/// not a global id) write distinct objects, so a publish can never overwrite a
/// peer's generation. Superseded generations are reclaimed by their author: a
/// device lists and deletes only objects under its own `{author}` prefix, so
/// ownership is structural — it never touches a peer's keyspace.
///
/// Blob keys follow the home's
/// [`BlobPathScheme`](crate::sync::cloud_storage::BlobPathScheme): the default
/// hashed scheme shards each blob by its id (`{namespace}/{ab}/{cd}/{id}`); the
/// plain scheme keys it at the consumer-supplied readable path
/// (`{namespace}/{cloud_path}`) so the bucket is browsable. The blob-path scheme
/// is independent of the at-rest cipher below.
///
/// An encrypted home seals every object under the library key before upload and
/// opens it after download; a plaintext home stores and serves objects verbatim.
/// The trait is async and mockable for testing.
use async_trait::async_trait;

/// Per-device head: the latest sequence number for a device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceHead {
    pub device_id: String,
    pub seq: u64,
    /// The seq up to which the latest snapshot covers. None if no snapshot
    /// has been created by this device.
    pub snapshot_seq: Option<u64>,
    /// RFC 3339 timestamp of when this head was last updated (i.e., when
    /// the device last synced). None for heads written before this field
    /// was added.
    pub last_sync: Option<String>,
    /// Hex-encoded Ed25519 public key the head's signature verified against, set
    /// by [`SyncStorage::list_heads`] once the embedded signature is checked. The
    /// caller uses it to decide whether the head's author is a current member (the
    /// authorization check the chain backs). Every head is signed regardless of the
    /// at-rest cipher, so a head read from storage always carries its author.
    pub author_pubkey: String,
}

/// A verified `min_schema_version`: the version plus the public key its
/// signature verified against. [`SyncStorage::get_min_schema_version`] returns
/// this only when the embedded signature checks out, so the caller can decide
/// whether the author is a current owner before honoring the floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinSchemaVersion {
    pub version: u32,
    /// Hex-encoded Ed25519 public key the signature verified against. A floor read
    /// from storage is always signed, so it always carries its author.
    pub author_pubkey: String,
}

/// Error type for storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("S3 operation failed: {0}")]
    S3(String),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("decryption failed: {0}")]
    Decryption(String),
}

impl From<crate::storage::cloud::CloudHomeError> for StorageError {
    fn from(e: crate::storage::cloud::CloudHomeError) -> Self {
        match e {
            crate::storage::cloud::CloudHomeError::NotFound(key) => StorageError::NotFound(key),
            crate::storage::cloud::CloudHomeError::Storage(msg) => StorageError::S3(msg),
            crate::storage::cloud::CloudHomeError::Io(io_err) => {
                StorageError::S3(format!("I/O error: {io_err}"))
            }
        }
    }
}

impl From<crate::library_dir::PathTokenError> for StorageError {
    /// A blob id/namespace/cloud_path that can't form a safe object key is bad
    /// data, surfaced so the caller refuses the blob rather than reaching storage
    /// with a key that could escape its prefix.
    fn from(e: crate::library_dir::PathTokenError) -> Self {
        StorageError::S3(format!("unsafe blob path: {e}"))
    }
}

/// `Send + Sync` with `Send` method futures on native; `?Send` on wasm. See
/// [`crate::MaybeThreadSafe`] for why the bound is cfg'd — the browser drives
/// every sync future on one thread.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait SyncStorage: crate::MaybeThreadSafe {
    /// List all device heads (one LIST call to `heads/`).
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError>;

    /// Fetch a single changeset by device_id and seq.
    ///
    /// Returns the **opened** envelope bytes from `changes/{device_id}/{seq}{suffix}`.
    /// Implementations download the stored blob and open it (decrypt on an
    /// encrypted home, pass through on a plaintext one) before returning. Callers
    /// receive plaintext ready for `envelope::unpack()`.
    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError>;

    /// Upload a changeset blob (plaintext — the implementation seals it).
    /// Writes to `changes/{device_id}/{seq}{suffix}`.
    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Update the head pointer for a device.
    /// Writes to `heads/{device_id}.json{suffix}`.
    /// If `snapshot_seq` is Some, the head records that a snapshot covers
    /// all changesets up to that seq. `timestamp` is the RFC 3339 time of this
    /// sync (used by the sync status UI).
    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        snapshot_seq: Option<u64>,
        timestamp: &str,
    ) -> Result<(), StorageError>;

    /// Upload a blob. Under the hashed (default) scheme it is keyed
    /// `{namespace}/{id[0..2]}/{id[2..4]}/{id}` and `cloud_path` is ignored; under
    /// the plain scheme it is keyed `{namespace}/{cloud_path}` verbatim, so the
    /// bucket is browsable, and a missing `cloud_path` is an error.
    /// On an encrypted home the plaintext is sealed with the key the resolved
    /// `scope` selects (master, a per-scope derived key, or an explicit item key);
    /// on a plaintext home it is stored verbatim (scope ignored). The caller
    /// resolves the public [`crate::blob::BlobScope`] to a
    /// [`crate::blob::ResolvedScope`] before storage sees it.
    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Download and open a blob, keyed `{namespace}/{id[0..2]}/{id[2..4]}/{id}`
    /// under the hashed scheme or `{namespace}/{cloud_path}` under the plain one,
    /// using the key the resolved `scope` selects on an encrypted home (verbatim
    /// on a plaintext one). A plain-scheme home with no `cloud_path` is an error.
    async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError>;

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
    /// an empty result. The cache layer ([`crate::blob_cache::open_blob_stream`])
    /// uses this only on a miss — a cache hit reads the local plaintext file
    /// directly.
    async fn read_blob_range(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        source_size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StorageError>;

    /// Upload one snapshot generation's DB image under its publishing device.
    /// Writes to `snapshot/{author}/{seq}.db{suffix}`. Written before the
    /// generation's metadata, and the pointer names `{author, seq}` only after
    /// both, so a reader never resolves a half-written generation. Keying under
    /// `{author}` (the publisher's hex public key) makes the object globally
    /// unique, so a publish never overwrites a peer's generation at the same `seq`.
    async fn put_snapshot(&self, author: &str, seq: u64, data: Vec<u8>)
        -> Result<(), StorageError>;

    /// Download a snapshot generation's DB image.
    /// Returns bytes from `snapshot/{author}/{seq}.db{suffix}`.
    async fn get_snapshot(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError>;

    /// Delete a single changeset from storage.
    /// Removes `changes/{device_id}/{seq}{suffix}`.
    async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError>;

    /// List all changeset keys for a device.
    /// Returns the sequence numbers that exist in `changes/{device_id}/`.
    async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError>;

    /// Get the minimum schema version required to sync with this storage, with
    /// the public key its signature verified against.
    ///
    /// Returns `None` if no minimum has been set, or if the stored object's
    /// signature is invalid (a forged floor is treated as absent, not trusted).
    /// Reads from `min_schema_version.json{suffix}`. The caller checks the
    /// returned `author_pubkey` is a current owner before honoring the version.
    async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError>;

    /// Set the minimum schema version required to sync with this storage.
    ///
    /// Writes to `min_schema_version.json{suffix}`. Used when a breaking migration
    /// bumps the schema and all devices must upgrade before syncing.
    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError>;

    /// Upload a membership entry.
    /// Writes to `membership/{author_pubkey_hex}/{seq}{suffix}`.
    async fn put_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Download a membership entry.
    /// Reads from `membership/{author_pubkey_hex}/{seq}{suffix}`.
    async fn get_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError>;

    /// List all membership entry keys.
    /// Returns tuples of (author_pubkey, seq).
    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError>;

    /// Upload a wrapped library key for a member.
    /// Writes to `keys/{user_pubkey_hex}{suffix}`. The bytes are already a sealed
    /// box, so the home cipher stores them verbatim regardless of suffix.
    async fn put_wrapped_key(&self, user_pubkey: &str, data: Vec<u8>) -> Result<(), StorageError>;

    /// Download a wrapped library key for a member.
    /// Reads from `keys/{user_pubkey_hex}{suffix}`.
    async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError>;

    /// Delete a wrapped library key.
    /// Removes `keys/{user_pubkey_hex}{suffix}`.
    async fn delete_wrapped_key(&self, user_pubkey: &str) -> Result<(), StorageError>;

    /// Upload one snapshot generation's metadata (plaintext -- the implementation
    /// seals it). Writes to `snapshot/{author}/{seq}_meta.json{suffix}`. Written
    /// *after* the DB image and *before* the pointer: the meta is what keys a
    /// generation in
    /// [`list_own_snapshot_generations`](Self::list_own_snapshot_generations), so a
    /// listed generation always has its DB image already whole. The `{author}`
    /// prefix is the publishing device's hex public key, so a device's own sweep
    /// lists only its own generations by listing under its own prefix.
    async fn put_snapshot_meta(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Download a snapshot generation's metadata (opened).
    /// Reads from `snapshot/{author}/{seq}_meta.json{suffix}`. Returns NotFound if
    /// that generation's metadata does not exist.
    async fn get_snapshot_meta(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError>;

    /// Publish the snapshot pointer (plaintext -- the implementation seals it).
    /// Writes to `snapshot/current.json{suffix}`. This is the commit of an atomic
    /// publish: it is written *last*, after the generation's metadata and DB image
    /// are fully uploaded, so it never names an incomplete generation.
    async fn put_snapshot_pointer(&self, data: Vec<u8>) -> Result<(), StorageError>;

    /// Download the snapshot pointer (opened).
    /// Reads from `snapshot/current.json{suffix}`. Returns NotFound if no snapshot
    /// has been published yet.
    async fn get_snapshot_pointer(&self) -> Result<Vec<u8>, StorageError>;

    /// List the sequences of every snapshot generation `author` has published,
    /// including superseded ones the pointer no longer names. A generation is keyed
    /// by its `snapshot/{author}/{seq}_meta.json{suffix}` object (written after the
    /// DB image), so a generation appears here only once its DB image is already
    /// whole. The sweep lists under its OWN `{author}` prefix, so ownership is
    /// structural: it only ever sees generations it published and never a peer's,
    /// which live under a different prefix.
    async fn list_own_snapshot_generations(&self, author: &str) -> Result<Vec<u64>, StorageError>;

    /// Delete one snapshot generation's objects
    /// (`snapshot/{author}/{seq}.db{suffix}` then
    /// `snapshot/{author}/{seq}_meta.json{suffix}` — the DB image first, the meta
    /// last, since the meta is what keys the generation in
    /// [`list_own_snapshot_generations`](Self::list_own_snapshot_generations), so a
    /// crash between the two leaves it still listed and re-deletable, never a
    /// meta-less db). The caller passes its own `{author}` and must have confirmed
    /// `seq` is neither the live generation nor the one it just published, so a
    /// delete never strands a generation a reader could adopt. A device can only
    /// name objects under its own prefix, so it structurally cannot delete a peer's.
    async fn delete_snapshot_generation(&self, author: &str, seq: u64) -> Result<(), StorageError>;
}
