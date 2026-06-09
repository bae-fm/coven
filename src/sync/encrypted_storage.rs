//! `SyncStorage` implementation backed by any `CloudHome`.
//!
//! Handles the cloud home path layout (where keys, heads, images, etc. live)
//! and encryption/decryption. The underlying `CloudHome` only deals in raw
//! bytes and flat keys.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use super::storage::{DeviceHead, StorageError, SyncStorage};
use crate::encryption::EncryptionService;
use crate::storage::cloud::CloudHome;

/// Serialized form of a device head stored in `heads/{device_id}.json.enc`.
#[derive(Serialize, Deserialize)]
struct HeadJson {
    seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_seq: Option<u64>,
    /// RFC 3339 timestamp of when this head was last written.
    #[serde(skip_serializing_if = "Option::is_none")]
    last_sync: Option<String>,
    /// This device's pull cursors (other_device_id -> last applied seq).
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    cursors: std::collections::HashMap<String, u64>,
}

/// Serialized form of `min_schema_version.json.enc`.
#[derive(Serialize, Deserialize)]
struct MinSchemaVersionJson {
    min_schema_version: u32,
}

/// `SyncStorage` that delegates raw I/O to a `CloudHome` and handles
/// the path layout and encryption layer.
pub struct EncryptedSyncStorage {
    home: Box<dyn CloudHome>,
    encryption: Arc<RwLock<EncryptionService>>,
}

impl EncryptedSyncStorage {
    pub fn new(home: Box<dyn CloudHome>, encryption: EncryptionService) -> Self {
        EncryptedSyncStorage {
            home,
            encryption: Arc::new(RwLock::new(encryption)),
        }
    }

    /// Return a shared reference to the encryption lock for external use
    /// (e.g., SyncHandle can share the same instance for snapshot creation).
    pub fn shared_encryption(&self) -> Arc<RwLock<EncryptionService>> {
        self.encryption.clone()
    }

    /// Borrow the underlying CloudHome for direct access (e.g., grant_access/revoke_access).
    pub fn cloud_home(&self) -> &dyn CloudHome {
        &*self.home
    }

    /// Convenience: read-lock the encryption service.
    fn enc(&self) -> std::sync::RwLockReadGuard<'_, EncryptionService> {
        self.encryption.read().unwrap()
    }

    /// The `EncryptionService` a blob's resolved `scope` selects, against this
    /// storage's master key. Delegates to [`encryption_for_scope`], the single
    /// mapping shared with the outbox drain.
    fn enc_for_scope(&self, scope: crate::blob::ResolvedScope) -> EncryptionService {
        encryption_for_scope(scope, &self.enc())
    }

    /// Blob key: `{namespace}/{ab}/{cd}/{id}`.
    pub fn blob_key(namespace: &str, id: &str) -> String {
        crate::library_dir::LibraryDir::hashed_path(namespace, id)
    }
}

/// The `EncryptionService` a blob's resolved `scope` selects, against `master`:
/// the library master itself, a per-scope key derived from it, or an explicit
/// key (a resolved item key). The blob storage methods and the outbox drain both
/// turn a [`crate::blob::ResolvedScope`] into a key the same way, so they share
/// this one mapping.
pub(crate) fn encryption_for_scope(
    scope: crate::blob::ResolvedScope,
    master: &EncryptionService,
) -> EncryptionService {
    match scope {
        crate::blob::ResolvedScope::Master => master.clone(),
        crate::blob::ResolvedScope::Derived(s) => master.derive_scoped(&s),
        crate::blob::ResolvedScope::Key(k) => EncryptionService::from_key(k),
    }
}

#[async_trait]
impl SyncStorage for EncryptedSyncStorage {
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
        let keys = self.home.list("heads/").await?;
        let mut heads = Vec::new();

        for key in &keys {
            // key = "heads/{device_id}.json.enc"
            let device_id = key
                .strip_prefix("heads/")
                .and_then(|s| s.strip_suffix(".json.enc"))
                .ok_or_else(|| StorageError::S3(format!("unexpected head key format: {key}")))?;

            let encrypted = self.home.read(key).await?;
            let decrypted = self
                .enc()
                .decrypt(&encrypted)
                .map_err(|e| StorageError::Decryption(format!("head {device_id}: {e}")))?;

            let head_json: HeadJson = serde_json::from_slice(&decrypted)
                .map_err(|e| StorageError::S3(format!("parse head {device_id}: {e}")))?;

            heads.push(DeviceHead {
                device_id: device_id.to_string(),
                seq: head_json.seq,
                snapshot_seq: head_json.snapshot_seq,
                last_sync: head_json.last_sync,
                cursors: head_json.cursors,
            });
        }

        Ok(heads)
    }

    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        let key = format!("changes/{device_id}/{seq}.enc");
        let encrypted = self.home.read(&key).await?;
        self.enc()
            .decrypt(&encrypted)
            .map_err(|e| StorageError::Decryption(format!("changeset {device_id}/{seq}: {e}")))
    }

    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("changes/{device_id}/{seq}.enc");
        let encrypted = self.enc().encrypt(&data);
        self.home
            .write(&key, encrypted, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        snapshot_seq: Option<u64>,
        cursors: &std::collections::HashMap<String, u64>,
        timestamp: &str,
    ) -> Result<(), StorageError> {
        let head = HeadJson {
            seq,
            snapshot_seq,
            last_sync: Some(timestamp.to_string()),
            cursors: cursors.clone(),
        };
        let json = serde_json::to_vec(&head)
            .map_err(|e| StorageError::S3(format!("serialize head: {e}")))?;
        let encrypted = self.enc().encrypt(&json);
        let key = format!("heads/{device_id}.json.enc");
        self.home
            .write(&key, encrypted, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = Self::blob_key(namespace, id);
        let enc = self.enc_for_scope(scope);
        let encrypted = enc.encrypt(&data);
        self.home
            .write(&key, encrypted, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
    ) -> Result<Vec<u8>, StorageError> {
        let key = Self::blob_key(namespace, id);
        let encrypted = self.home.read(&key).await?;
        let enc = self.enc_for_scope(scope);
        enc.decrypt(&encrypted)
            .map_err(|e| StorageError::Decryption(format!("blob {namespace}/{id}: {e}")))
    }

    async fn put_snapshot(&self, data: Vec<u8>) -> Result<(), StorageError> {
        self.home
            .write(
                "snapshot.db.enc",
                data,
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn get_snapshot(&self) -> Result<Vec<u8>, StorageError> {
        self.home
            .read("snapshot.db.enc")
            .await
            .map_err(StorageError::from)
    }

    async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError> {
        let key = format!("changes/{device_id}/{seq}.enc");
        self.home.delete(&key).await?;
        Ok(())
    }

    async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError> {
        let prefix = format!("changes/{device_id}/");
        let keys = self.home.list(&prefix).await?;

        let mut seqs: Vec<u64> = keys
            .iter()
            .filter_map(|k| {
                k.strip_prefix(&prefix)
                    .and_then(|s| s.strip_suffix(".enc"))
                    .and_then(|s| s.parse().ok())
            })
            .collect();
        seqs.sort();
        Ok(seqs)
    }

    async fn get_min_schema_version(&self) -> Result<Option<u32>, StorageError> {
        let key = "min_schema_version.json.enc";
        let encrypted = match self.home.read(key).await {
            Ok(data) => data,
            Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(StorageError::from(e)),
        };

        let decrypted = self
            .enc()
            .decrypt(&encrypted)
            .map_err(|e| StorageError::Decryption(format!("min_schema_version: {e}")))?;

        let parsed: MinSchemaVersionJson = serde_json::from_slice(&decrypted)
            .map_err(|e| StorageError::S3(format!("parse min_schema_version: {e}")))?;

        Ok(Some(parsed.min_schema_version))
    }

    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
        let payload = MinSchemaVersionJson {
            min_schema_version: version,
        };
        let json = serde_json::to_vec(&payload)
            .map_err(|e| StorageError::S3(format!("serialize min_schema_version: {e}")))?;
        let encrypted = self.enc().encrypt(&json);
        self.home
            .write(
                "min_schema_version.json.enc",
                encrypted,
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn put_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("membership/{author_pubkey}/{seq}.enc");
        let encrypted = self.enc().encrypt(&data);
        self.home
            .write(&key, encrypted, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let key = format!("membership/{author_pubkey}/{seq}.enc");
        let encrypted = self.home.read(&key).await?;
        self.enc()
            .decrypt(&encrypted)
            .map_err(|e| StorageError::Decryption(format!("membership {author_pubkey}/{seq}: {e}")))
    }

    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
        let keys = self.home.list("membership/").await?;
        let mut entries = Vec::new();

        for key in &keys {
            // key = "membership/{author_pubkey}/{seq}.enc"
            let rest = match key.strip_prefix("membership/") {
                Some(r) => r,
                None => continue,
            };
            let rest = match rest.strip_suffix(".enc") {
                Some(r) => r,
                None => continue,
            };

            // Split into author_pubkey and seq. The pubkey is hex (no slashes),
            // so the last '/' separates pubkey from seq.
            if let Some(slash_pos) = rest.rfind('/') {
                let author = &rest[..slash_pos];
                if let Ok(seq) = rest[slash_pos + 1..].parse::<u64>() {
                    entries.push((author.to_string(), seq));
                }
            }
        }

        Ok(entries)
    }

    async fn put_wrapped_key(&self, user_pubkey: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let key = format!("keys/{user_pubkey}.enc");
        // Wrapped keys are already encrypted (sealed box), store as-is.
        self.home
            .write(&key, data, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError> {
        let key = format!("keys/{user_pubkey}.enc");
        // Wrapped keys are already encrypted (sealed box), return as-is.
        self.home.read(&key).await.map_err(StorageError::from)
    }

    async fn delete_wrapped_key(&self, user_pubkey: &str) -> Result<(), StorageError> {
        let key = format!("keys/{user_pubkey}.enc");
        self.home.delete(&key).await?;
        Ok(())
    }

    async fn put_snapshot_meta(&self, data: Vec<u8>) -> Result<(), StorageError> {
        let encrypted = self.enc().encrypt(&data);
        self.home
            .write(
                "snapshot_meta.json.enc",
                encrypted,
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn get_snapshot_meta(&self) -> Result<Vec<u8>, StorageError> {
        let encrypted = self.home.read("snapshot_meta.json.enc").await?;
        self.enc()
            .decrypt(&encrypted)
            .map_err(|e| StorageError::Decryption(format!("snapshot_meta: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::ResolvedScope;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;

    /// A `ResolvedScope::Key` blob is encrypted under the explicit (item) key, not
    /// the master: it round-trips with that key and the master key cannot read
    /// it. This is what lets coven scope a blob to a per-item key (the resolved
    /// form of a `BlobScope::Item`) so it can be read — or handed to a share
    /// recipient — without exposing the whole library.
    #[tokio::test]
    async fn key_scoped_blob_round_trips_and_master_cannot_read_it() {
        let master = EncryptionService::new_with_key(&[7u8; 32]);
        let storage = EncryptedSyncStorage::new(Box::new(InMemoryCloudHome::new()), master);

        let item_key = [9u8; 32];
        let plaintext = b"per-item content bytes".to_vec();
        storage
            .put_blob(
                "images",
                "item-1",
                ResolvedScope::Key(item_key),
                plaintext.clone(),
            )
            .await
            .expect("put_blob with Key scope");

        // At rest it is ciphertext.
        let at_rest = storage
            .cloud_home()
            .read(&EncryptedSyncStorage::blob_key("images", "item-1"))
            .await
            .expect("blob present");
        assert_ne!(at_rest, plaintext, "blob is encrypted at rest");

        // The explicit key reads it back; the master key does not.
        let got = storage
            .get_blob("images", "item-1", ResolvedScope::Key(item_key))
            .await
            .expect("get_blob with the same Key");
        assert_eq!(got, plaintext);
        assert!(
            storage
                .get_blob("images", "item-1", ResolvedScope::Master)
                .await
                .is_err(),
            "the master key must not decrypt a Key-scoped blob"
        );
    }
}
