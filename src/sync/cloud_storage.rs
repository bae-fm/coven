//! `SyncStorage` implementation backed by any `CloudHome`.
//!
//! Handles the cloud home path layout (where keys, heads, images, etc. live)
//! and how objects are protected at rest. The underlying `CloudHome` only deals
//! in raw bytes and flat keys; this layer applies the [`CloudCipher`] — sealing
//! every object under the library key for an encrypted home, or storing it
//! verbatim for a plaintext one — and drives the object-key suffix off the same
//! choice (`.enc` for an encrypted home, no suffix for a plaintext one).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tracing::warn;

use super::storage::{DeviceHead, StorageError, SyncStorage};
use crate::encryption::{EncryptionError, EncryptionService};
use crate::storage::cloud::CloudHome;

/// How a cloud home protects its objects at rest. An `Encrypted` home seals
/// every object under the library key (the default); a `Plaintext` home stores
/// objects in the clear so the bucket is browsable, and drops the `.enc` suffix.
#[derive(Clone)]
pub enum CloudCipher {
    Encrypted(EncryptionService),
    Plaintext,
}

/// How a cloud home names its blob objects. This is independent of the at-rest
/// [`CloudCipher`]: a home can be encrypted or plaintext under either scheme.
#[derive(Clone, Copy)]
pub enum BlobPathScheme {
    /// Content-addressed shard `{namespace}/{ab}/{cd}/{id}` (the default).
    Hashed,
    /// The consumer's own readable path, verbatim: `{namespace}/{cloud_path}`.
    /// The consumer must supply `cloud_path` on every blob; coven errors otherwise.
    Plain,
}

impl BlobPathScheme {
    /// Map a home's `obfuscate_blob_paths` flag (config, restore code, invite
    /// code) to a scheme: obfuscated ⇒ `Hashed`, otherwise `Plain`.
    pub fn from_obfuscate(obfuscate: bool) -> Self {
        if obfuscate {
            BlobPathScheme::Hashed
        } else {
            BlobPathScheme::Plain
        }
    }
}

impl CloudCipher {
    /// Protect a control object (heads, changesets, snapshot, snapshot_meta,
    /// min_schema, membership) for storage. Encrypted seals under the library
    /// key; plaintext returns the bytes unchanged.
    pub fn seal(&self, plaintext: &[u8]) -> Vec<u8> {
        // A control object is always whole-home scoped; only blobs carry a scope.
        // This is exactly the master-scoped blob path: `encryption_for_scope`
        // maps `Master` to the library key itself.
        self.seal_scoped(crate::blob::ResolvedScope::Master, plaintext)
    }

    /// Recover a control object read from storage. Inverse of [`Self::seal`].
    pub fn open(&self, stored: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.open_scoped(crate::blob::ResolvedScope::Master, stored)
    }

    /// Protect a blob under its resolved scope. Encrypted derives the scope's key
    /// from the master via [`encryption_for_scope`]; plaintext is passthrough,
    /// ignoring the scope (a plaintext home has no per-scope keys).
    pub(crate) fn seal_scoped(
        &self,
        scope: crate::blob::ResolvedScope,
        plaintext: &[u8],
    ) -> Vec<u8> {
        match self {
            CloudCipher::Encrypted(e) => encryption_for_scope(scope, e).encrypt(plaintext),
            CloudCipher::Plaintext => plaintext.to_vec(),
        }
    }

    /// Recover a blob under its resolved scope. Inverse of [`Self::seal_scoped`].
    pub(crate) fn open_scoped(
        &self,
        scope: crate::blob::ResolvedScope,
        stored: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        match self {
            CloudCipher::Encrypted(e) => encryption_for_scope(scope, e).decrypt(stored),
            CloudCipher::Plaintext => Ok(stored.to_vec()),
        }
    }

    /// The object-key suffix this cipher implies: `.enc` for an encrypted home,
    /// empty for a plaintext one. Note `"x".strip_suffix("")` returns `Some("x")`,
    /// so the listing parsers strip an empty suffix as a clean no-op.
    pub fn suffix(&self) -> &'static str {
        match self {
            CloudCipher::Encrypted(_) => ".enc",
            CloudCipher::Plaintext => "",
        }
    }

    /// Whether this is a plaintext (unencrypted) home.
    pub fn is_plaintext(&self) -> bool {
        matches!(self, CloudCipher::Plaintext)
    }
}

/// Serialized form of a device head stored in `heads/{device_id}.json{suffix}`.
#[derive(Serialize, Deserialize)]
struct HeadJson {
    seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_seq: Option<u64>,
    /// RFC 3339 timestamp of when this head was last written.
    #[serde(skip_serializing_if = "Option::is_none")]
    last_sync: Option<String>,
}

/// Serialized form of `min_schema_version.json{suffix}`.
#[derive(Serialize, Deserialize)]
struct MinSchemaVersionJson {
    min_schema_version: u32,
}

/// `SyncStorage` that delegates raw I/O to a `CloudHome` and handles the path
/// layout and the at-rest protection (its [`CloudCipher`]).
pub struct CloudSyncStorage {
    home: Box<dyn CloudHome>,
    cipher: Arc<RwLock<CloudCipher>>,
    /// How blob objects are keyed. Unlike the cipher, the scheme does not rotate
    /// over a home's life, so it is a plain field with no lock.
    blob_paths: BlobPathScheme,
}

impl CloudSyncStorage {
    pub fn new(home: Box<dyn CloudHome>, cipher: CloudCipher, blob_paths: BlobPathScheme) -> Self {
        CloudSyncStorage {
            home,
            cipher: Arc::new(RwLock::new(cipher)),
            blob_paths,
        }
    }

    /// Return a shared reference to the cipher lock for external use (e.g.,
    /// SyncHandle shares the same instance for snapshot creation, and a member
    /// removal rotates the key in place through it).
    pub fn shared_cipher(&self) -> Arc<RwLock<CloudCipher>> {
        self.cipher.clone()
    }

    /// Borrow the underlying CloudHome for direct access (e.g., grant_access/revoke_access).
    pub fn cloud_home(&self) -> &dyn CloudHome {
        &*self.home
    }

    /// Convenience: read-lock the cipher.
    fn cipher(&self) -> std::sync::RwLockReadGuard<'_, CloudCipher> {
        self.cipher.read().unwrap()
    }

    /// The object-key suffix the current cipher implies.
    fn suffix(&self) -> &'static str {
        self.cipher().suffix()
    }

    /// The cloud object key for a blob under the home's [`BlobPathScheme`].
    ///
    /// `Hashed` ignores `cloud_path` and shards by the id:
    /// `{namespace}/{ab}/{cd}/{id}`. `Plain` uses the consumer's `cloud_path`
    /// verbatim: `{namespace}/{cloud_path}`. A `Plain` home with no `cloud_path`
    /// is an error — coven never silently falls back to the hashed layout, which
    /// would scatter readable-path blobs under unfindable shard keys.
    pub fn blob_key(
        scheme: BlobPathScheme,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, StorageError> {
        match scheme {
            BlobPathScheme::Hashed => {
                Ok(crate::library_dir::LibraryDir::hashed_path(namespace, id))
            }
            BlobPathScheme::Plain => {
                let path = cloud_path.ok_or_else(|| {
                    StorageError::S3(format!(
                        "unobfuscated blob-path home requires a cloud_path for blob {namespace}/{id}"
                    ))
                })?;
                Ok(format!("{namespace}/{path}"))
            }
        }
    }
}

/// The `EncryptionService` a blob's resolved `scope` selects, against `master`:
/// the library master itself, a per-scope key derived from it, or an explicit
/// key (a resolved item key). The blob storage methods and the outbox drain both
/// turn a [`crate::blob::ResolvedScope`] into a key the same way, so they share
/// this one mapping. Only an encrypted home has per-scope keys, so this is
/// reached only from the [`CloudCipher::Encrypted`] branches.
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl SyncStorage for CloudSyncStorage {
    async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
        let suffix = self.suffix();
        let keys = self.home.list("heads/").await?;
        let mut heads = Vec::new();

        for key in &keys {
            // key = "heads/{device_id}.json{suffix}"
            let device_id = key
                .strip_prefix("heads/")
                .and_then(|s| s.strip_suffix(suffix))
                .and_then(|s| s.strip_suffix(".json"))
                .ok_or_else(|| StorageError::S3(format!("unexpected head key format: {key}")))?;

            let stored = self.home.read(key).await?;
            // A head we can't open is not ours: a head a *different* library
            // wrote when it reused this bucket, under its own encryption key.
            // Skip it rather than abort — otherwise one foreign head wedges every
            // sync cycle for this library, so it never pulls, pushes its catalog
            // or snapshot, or publishes its own head. A transient read error
            // above still propagates (it retries next cycle); a parse failure of
            // a head we *can* open is our own corrupt data and still surfaces. In
            // a plaintext home `open` never fails, so this branch never trips.
            let decoded = match self.cipher().open(&stored) {
                Ok(d) => d,
                Err(e) => {
                    warn!("skipping head {device_id} this library cannot decrypt: {e}");
                    continue;
                }
            };

            let head_json: HeadJson = serde_json::from_slice(&decoded)
                .map_err(|e| StorageError::S3(format!("parse head {device_id}: {e}")))?;

            heads.push(DeviceHead {
                device_id: device_id.to_string(),
                seq: head_json.seq,
                snapshot_seq: head_json.snapshot_seq,
                last_sync: head_json.last_sync,
            });
        }

        Ok(heads)
    }

    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        let key = format!("changes/{device_id}/{seq}{}", self.suffix());
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open(&stored)
            .map_err(|e| StorageError::Decryption(format!("changeset {device_id}/{seq}: {e}")))
    }

    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("changes/{device_id}/{seq}{}", self.suffix());
        let stored = self.cipher().seal(&data);
        self.home
            .write(&key, stored, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn put_head(
        &self,
        device_id: &str,
        seq: u64,
        snapshot_seq: Option<u64>,
        timestamp: &str,
    ) -> Result<(), StorageError> {
        let head = HeadJson {
            seq,
            snapshot_seq,
            last_sync: Some(timestamp.to_string()),
        };
        let json = serde_json::to_vec(&head)
            .map_err(|e| StorageError::S3(format!("serialize head: {e}")))?;
        let stored = self.cipher().seal(&json);
        let key = format!("heads/{device_id}.json{}", self.suffix());
        self.home
            .write(&key, stored, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = Self::blob_key(self.blob_paths, namespace, id, cloud_path)?;
        let stored = self.cipher().seal_scoped(scope, &data);
        self.home
            .write(&key, stored, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        let key = Self::blob_key(self.blob_paths, namespace, id, cloud_path)?;
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open_scoped(scope, &stored)
            .map_err(|e| StorageError::Decryption(format!("blob {namespace}/{id}: {e}")))
    }

    async fn put_snapshot(&self, data: Vec<u8>) -> Result<(), StorageError> {
        let key = format!("snapshot.db{}", self.suffix());
        self.home
            .write(&key, data, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_snapshot(&self) -> Result<Vec<u8>, StorageError> {
        let key = format!("snapshot.db{}", self.suffix());
        self.home.read(&key).await.map_err(StorageError::from)
    }

    async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError> {
        let key = format!("changes/{device_id}/{seq}{}", self.suffix());
        self.home.delete(&key).await?;
        Ok(())
    }

    async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError> {
        let suffix = self.suffix();
        let prefix = format!("changes/{device_id}/");
        let keys = self.home.list(&prefix).await?;

        let mut seqs = Vec::new();
        for key in &keys {
            let Some(seq_str) = key
                .strip_prefix(&prefix)
                .and_then(|s| s.strip_suffix(suffix))
            else {
                warn!("skipping changeset key with unexpected format: {key}");
                continue;
            };
            match seq_str.parse::<u64>() {
                Ok(seq) => seqs.push(seq),
                Err(e) => warn!("skipping changeset key {key} with non-numeric seq: {e}"),
            }
        }
        seqs.sort();
        Ok(seqs)
    }

    async fn get_min_schema_version(&self) -> Result<Option<u32>, StorageError> {
        let key = format!("min_schema_version.json{}", self.suffix());
        let stored = match self.home.read(&key).await {
            Ok(data) => data,
            Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(StorageError::from(e)),
        };

        let decoded = self
            .cipher()
            .open(&stored)
            .map_err(|e| StorageError::Decryption(format!("min_schema_version: {e}")))?;

        let parsed: MinSchemaVersionJson = serde_json::from_slice(&decoded)
            .map_err(|e| StorageError::S3(format!("parse min_schema_version: {e}")))?;

        Ok(Some(parsed.min_schema_version))
    }

    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
        let payload = MinSchemaVersionJson {
            min_schema_version: version,
        };
        let json = serde_json::to_vec(&payload)
            .map_err(|e| StorageError::S3(format!("serialize min_schema_version: {e}")))?;
        let stored = self.cipher().seal(&json);
        let key = format!("min_schema_version.json{}", self.suffix());
        self.home
            .write(&key, stored, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn put_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("membership/{author_pubkey}/{seq}{}", self.suffix());
        let stored = self.cipher().seal(&data);
        self.home
            .write(&key, stored, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_membership_entry(
        &self,
        author_pubkey: &str,
        seq: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let key = format!("membership/{author_pubkey}/{seq}{}", self.suffix());
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open(&stored)
            .map_err(|e| StorageError::Decryption(format!("membership {author_pubkey}/{seq}: {e}")))
    }

    async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
        let suffix = self.suffix();
        let keys = self.home.list("membership/").await?;
        let mut entries = Vec::new();

        for key in &keys {
            // key = "membership/{author_pubkey}/{seq}{suffix}"
            let Some(rest) = key.strip_prefix("membership/") else {
                warn!("skipping membership key without the expected prefix: {key}");
                continue;
            };
            let Some(rest) = rest.strip_suffix(suffix) else {
                warn!("skipping membership key without the expected suffix: {key}");
                continue;
            };

            // Split into author_pubkey and seq. The pubkey is hex (no slashes),
            // so the last '/' separates pubkey from seq.
            let Some(slash_pos) = rest.rfind('/') else {
                warn!("skipping membership key with no pubkey/seq separator: {key}");
                continue;
            };
            let author = &rest[..slash_pos];
            match rest[slash_pos + 1..].parse::<u64>() {
                Ok(seq) => entries.push((author.to_string(), seq)),
                Err(e) => warn!("skipping membership key {key} with non-numeric seq: {e}"),
            }
        }

        Ok(entries)
    }

    async fn put_wrapped_key(&self, user_pubkey: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let key = format!("keys/{user_pubkey}{}", self.suffix());
        // Wrapped keys are already sealed boxes; store as-is. The suffix is kept
        // uniform with the rest of the layout, but the bytes are never sealed by
        // the home cipher — wrapping a library key is meaningful only for a
        // shared (encrypted) home.
        self.home
            .write(&key, data, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError> {
        let key = format!("keys/{user_pubkey}{}", self.suffix());
        // Wrapped keys are already sealed boxes; return as-is.
        self.home.read(&key).await.map_err(StorageError::from)
    }

    async fn delete_wrapped_key(&self, user_pubkey: &str) -> Result<(), StorageError> {
        let key = format!("keys/{user_pubkey}{}", self.suffix());
        self.home.delete(&key).await?;
        Ok(())
    }

    async fn put_snapshot_meta(&self, data: Vec<u8>) -> Result<(), StorageError> {
        let stored = self.cipher().seal(&data);
        let key = format!("snapshot_meta.json{}", self.suffix());
        self.home
            .write(&key, stored, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_snapshot_meta(&self) -> Result<Vec<u8>, StorageError> {
        let key = format!("snapshot_meta.json{}", self.suffix());
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open(&stored)
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
        let storage = CloudSyncStorage::new(
            Box::new(InMemoryCloudHome::new()),
            CloudCipher::Encrypted(master),
            BlobPathScheme::Hashed,
        );

        let item_key = [9u8; 32];
        let plaintext = b"per-item content bytes".to_vec();
        storage
            .put_blob(
                "images",
                "item-1",
                ResolvedScope::Key(item_key),
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob with Key scope");

        // At rest it is ciphertext.
        let hashed = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "images", "item-1", None)
            .expect("hashed key");
        let at_rest = storage
            .cloud_home()
            .read(&hashed)
            .await
            .expect("blob present");
        assert_ne!(at_rest, plaintext, "blob is encrypted at rest");

        // The explicit key reads it back; the master key does not.
        let got = storage
            .get_blob("images", "item-1", ResolvedScope::Key(item_key), None)
            .await
            .expect("get_blob with the same Key");
        assert_eq!(got, plaintext);
        assert!(
            storage
                .get_blob("images", "item-1", ResolvedScope::Master, None)
                .await
                .is_err(),
            "the master key must not decrypt a Key-scoped blob"
        );
    }

    /// A head this library cannot decrypt — e.g. one a *different* library wrote
    /// when it reused the same bucket (its own encryption key) — must be skipped,
    /// not abort `list_heads`. The sync cycle's pull calls `list_heads`, so an
    /// abort there wedges every cycle: the library never pulls, never pushes its
    /// catalog or snapshot, and never publishes its own head.
    #[tokio::test]
    async fn list_heads_skips_a_head_it_cannot_decrypt() {
        let storage = CloudSyncStorage::new(
            Box::new(InMemoryCloudHome::new()),
            CloudCipher::Encrypted(EncryptionService::new_with_key(&[1u8; 32])),
            BlobPathScheme::Hashed,
        );
        storage
            .put_head("ours", 5, None, "2026-01-01T00:00:00Z")
            .await
            .expect("put our head");

        // A foreign head our key can't decrypt (not our ciphertext).
        storage
            .cloud_home()
            .write(
                "heads/foreign-device.json.enc",
                b"a different library's head, encrypted with another key".to_vec(),
                &crate::storage::cloud::no_progress(),
            )
            .await
            .expect("write foreign head");

        let heads = storage
            .list_heads()
            .await
            .expect("list_heads must not abort on a head it cannot decrypt");
        let ids: Vec<&str> = heads.iter().map(|h| h.device_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["ours"],
            "the decryptable head is returned; the foreign one is skipped",
        );
    }

    /// A plaintext home stores every control object in the clear and drops the
    /// `.enc` suffix from its keys. This round-trips a head, a changeset, the
    /// snapshot, snapshot_meta, and a `Master`-scoped blob and asserts each lands
    /// as the literal bytes (not ciphertext) under a bare key, while the
    /// `.enc`-suffixed key is absent.
    #[tokio::test]
    async fn plaintext_home_stores_control_objects_in_the_clear_without_enc_suffix() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Box::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Hashed,
        );

        // Head: bare key present, `.enc` key absent, and it reads back.
        storage
            .put_head("dev1", 7, Some(3), "2026-01-01T00:00:00Z")
            .await
            .expect("put_head");
        assert!(
            home.get("heads/dev1.json").is_some(),
            "bare head key present"
        );
        assert!(
            home.get("heads/dev1.json.enc").is_none(),
            "no .enc head key"
        );
        let heads = storage.list_heads().await.expect("list_heads");
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].device_id, "dev1");
        assert_eq!(heads[0].seq, 7);

        // Changeset: at rest the bytes are the literal plaintext.
        let cs = b"changeset-plaintext-bytes".to_vec();
        storage
            .put_changeset("dev1", 1, cs.clone())
            .await
            .expect("put_changeset");
        assert_eq!(
            home.get("changes/dev1/1").as_deref(),
            Some(cs.as_slice()),
            "changeset stored verbatim under a bare key",
        );
        assert!(
            home.get("changes/dev1/1.enc").is_none(),
            "no .enc changeset key"
        );
        assert_eq!(storage.get_changeset("dev1", 1).await.expect("get"), cs);
        assert_eq!(
            storage.list_changesets("dev1").await.expect("list"),
            vec![1]
        );

        // Snapshot + snapshot_meta: literal at rest, bare keys.
        let snap = b"SQLite format 3\0 ... bytes".to_vec();
        storage
            .put_snapshot(snap.clone())
            .await
            .expect("put_snapshot");
        assert_eq!(home.get("snapshot.db").as_deref(), Some(snap.as_slice()));
        assert!(
            home.get("snapshot.db.enc").is_none(),
            "no .enc snapshot key"
        );
        assert_eq!(storage.get_snapshot().await.expect("get_snapshot"), snap);

        let meta = b"{\"cursors\":{}}".to_vec();
        storage
            .put_snapshot_meta(meta.clone())
            .await
            .expect("put_snapshot_meta");
        assert_eq!(
            home.get("snapshot_meta.json").as_deref(),
            Some(meta.as_slice())
        );
        assert!(
            home.get("snapshot_meta.json.enc").is_none(),
            "no .enc meta key"
        );
        assert_eq!(
            storage
                .get_snapshot_meta()
                .await
                .expect("get_snapshot_meta"),
            meta
        );

        // A Master-scoped blob is stored verbatim too (no per-scope key).
        let blob = b"cover-art-plaintext".to_vec();
        storage
            .put_blob(
                "photos",
                "p1cover",
                ResolvedScope::Master,
                None,
                blob.clone(),
            )
            .await
            .expect("put_blob");
        let hashed = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "photos", "p1cover", None)
            .expect("hashed key");
        assert_eq!(
            home.get(&hashed).as_deref(),
            Some(blob.as_slice()),
            "blob stored verbatim in a plaintext home",
        );
        assert_eq!(
            storage
                .get_blob("photos", "p1cover", ResolvedScope::Master, None)
                .await
                .expect("get_blob"),
            blob
        );
    }

    /// A `BlobPathScheme::Plain` home stores each blob at the consumer's readable
    /// `cloud_path` (`{namespace}/{cloud_path}`), not the content-addressed shard:
    /// the bucket is browsable. Asserts the blob lands at the exact readable key,
    /// that the hashed shard key it *would* have used is absent, and that
    /// `get_blob` with the same `cloud_path` round-trips.
    #[tokio::test]
    async fn plain_scheme_stores_blob_at_the_readable_cloud_path() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Box::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::new_with_key(&[3u8; 32])),
            BlobPathScheme::Plain,
        );

        let cloud_path = "Artist - Album/cover.jpg";
        let bytes = b"cover-art-bytes".to_vec();
        storage
            .put_blob(
                "images",
                "cover-row-id",
                ResolvedScope::Master,
                Some(cloud_path),
                bytes.clone(),
            )
            .await
            .expect("put_blob plain");

        // It lands at the bare readable key.
        assert!(
            home.get("images/Artist - Album/cover.jpg").is_some(),
            "blob stored at the readable cloud_path key",
        );
        // The hashed shard key it would otherwise have used does not exist.
        let hashed =
            CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "images", "cover-row-id", None)
                .expect("hashed key");
        assert!(
            home.get(&hashed).is_none(),
            "the hashed shard key must be absent under the plain scheme",
        );

        // Round-trips with the same cloud_path.
        let got = storage
            .get_blob(
                "images",
                "cover-row-id",
                ResolvedScope::Master,
                Some(cloud_path),
            )
            .await
            .expect("get_blob plain");
        assert_eq!(got, bytes);
    }

    /// A plain-scheme home with no `cloud_path` is a surfaced error, never a
    /// silent fall back to the hashed shard (which would scatter readable-path
    /// blobs under unfindable keys). Asserts both `blob_key` and `put_blob` error.
    #[tokio::test]
    async fn plain_scheme_without_cloud_path_errors() {
        assert!(
            CloudSyncStorage::blob_key(BlobPathScheme::Plain, "images", "id-1", None).is_err(),
            "blob_key for a plain home with no cloud_path must error",
        );

        let storage = CloudSyncStorage::new(
            Box::new(InMemoryCloudHome::new()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
        );
        assert!(
            storage
                .put_blob("images", "id-1", ResolvedScope::Master, None, b"x".to_vec())
                .await
                .is_err(),
            "put_blob for a plain home with no cloud_path must error, not silently hash",
        );
    }
}
