/// Sync storage: reads/writes to the layout used for changeset sync.
///
/// Layout:
/// ```text
/// changes/{device_id}/{seq}.enc          -- encrypted changeset envelopes
/// heads/{device_id}.json.enc             -- encrypted head pointers
/// images/{ab}/{cd}/{id}                  -- encrypted library images
/// snapshot.db.enc                        -- full DB snapshot for bootstrapping
/// snapshot_meta.json.enc                 -- per-device cursors at snapshot time
/// membership/{author_pubkey}/{seq}.enc   -- encrypted membership entries
/// keys/{user_pubkey}.enc                 -- wrapped library keys per member
/// ```
///
/// All data is encrypted before upload and decrypted after download.
/// The trait is async and mockable for testing.
use std::collections::HashMap;

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
    /// This device's pull cursors: how far it has applied every OTHER device's
    /// changesets (`other_device_id -> last applied seq`). A peer reads its own
    /// id out of every head to learn how far each device has consumed it — the
    /// basis for safely deleting a blob (delete once every peer has pulled past
    /// the deletion). Empty for heads written before this field existed.
    pub cursors: HashMap<String, u64>,
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

#[async_trait]
pub trait SyncStorage: Send + Sync {
    /// List all device heads (one LIST call to `heads/`).
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError>;

    /// Fetch a single changeset by device_id and seq.
    ///
    /// Returns the **decrypted** envelope bytes from `changes/{device_id}/{seq}.enc`.
    /// Implementations must handle downloading the encrypted blob and decrypting
    /// it before returning. Callers receive plaintext ready for `envelope::unpack()`.
    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError>;

    /// Upload a changeset blob (plaintext — the implementation encrypts it).
    /// Writes to `changes/{device_id}/{seq}.enc`.
    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Update the head pointer for a device.
    /// Writes to `heads/{device_id}.json.enc`.
    /// If `snapshot_seq` is Some, the head records that a snapshot covers
    /// all changesets up to that seq. `cursors` is this device's pull cursors
    /// (how far it has applied each other device), published so peers can gate
    /// blob deletes on every peer having pulled past the deletion. `timestamp`
    /// is the RFC 3339 time of this sync (used by the sync status UI).
    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        snapshot_seq: Option<u64>,
        cursors: &HashMap<String, u64>,
        timestamp: &str,
    ) -> Result<(), StorageError>;

    /// Upload an encrypted blob to `{namespace}/{id[0..2]}/{id[2..4]}/{id}`.
    /// The plaintext is encrypted with the key the resolved `scope` selects
    /// (master, a per-scope derived key, or an explicit item key). The caller
    /// resolves the public [`crate::blob::BlobScope`] to a
    /// [`crate::blob::ResolvedScope`] before storage sees it.
    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Download and decrypt a blob from `{namespace}/{id[0..2]}/{id[2..4]}/{id}`,
    /// using the key the resolved `scope` selects.
    async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
    ) -> Result<Vec<u8>, StorageError>;

    /// Upload an encrypted snapshot.
    /// Writes to `snapshot.db.enc` (overwrites any previous snapshot).
    async fn put_snapshot(&self, data: Vec<u8>) -> Result<(), StorageError>;

    /// Download the encrypted snapshot.
    /// Returns bytes from `snapshot.db.enc`.
    async fn get_snapshot(&self) -> Result<Vec<u8>, StorageError>;

    /// Delete a single changeset from storage.
    /// Removes `changes/{device_id}/{seq}.enc`.
    async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError>;

    /// List all changeset keys for a device.
    /// Returns the sequence numbers that exist in `changes/{device_id}/`.
    async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError>;

    /// Get the minimum schema version required to sync with this storage.
    ///
    /// Returns `None` if no minimum has been set (backwards compat: any version
    /// can sync). Reads from `min_schema_version.json.enc`.
    async fn get_min_schema_version(&self) -> Result<Option<u32>, StorageError>;

    /// Set the minimum schema version required to sync with this storage.
    ///
    /// Writes to `min_schema_version.json.enc`. Used when a breaking migration
    /// bumps the schema and all devices must upgrade before syncing.
    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError>;

    /// Upload a membership entry.
    /// Writes to `membership/{author_pubkey_hex}/{seq}.enc`.
    async fn put_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError>;

    /// Download a membership entry.
    /// Reads from `membership/{author_pubkey_hex}/{seq}.enc`.
    async fn get_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError>;

    /// List all membership entry keys.
    /// Returns tuples of (author_pubkey, seq).
    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError>;

    /// Upload a wrapped library key for a member.
    /// Writes to `keys/{user_pubkey_hex}.enc`.
    async fn put_wrapped_key(&self, user_pubkey: &str, data: Vec<u8>) -> Result<(), StorageError>;

    /// Download a wrapped library key for a member.
    /// Reads from `keys/{user_pubkey_hex}.enc`.
    async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError>;

    /// Delete a wrapped library key.
    /// Removes `keys/{user_pubkey_hex}.enc`.
    async fn delete_wrapped_key(&self, user_pubkey: &str) -> Result<(), StorageError>;

    /// Upload snapshot metadata (plaintext -- the implementation encrypts it).
    /// Writes to `snapshot_meta.json.enc`.
    async fn put_snapshot_meta(&self, data: Vec<u8>) -> Result<(), StorageError>;

    /// Download snapshot metadata (decrypted).
    /// Reads from `snapshot_meta.json.enc`. Returns NotFound if no metadata exists.
    async fn get_snapshot_meta(&self) -> Result<Vec<u8>, StorageError>;
}
