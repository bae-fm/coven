//! `SyncStorage` implementation backed by any `CloudHome`.
//!
//! Handles the cloud home path layout (where keys, heads, images, etc. live)
//! and how objects are protected at rest. The underlying `CloudHome` only deals
//! in raw bytes and flat keys; this layer applies the [`CloudCipher`] — sealing
//! every object under the library key for an encrypted home, or storing it
//! verbatim for a plaintext one — and drives the object-key suffix off the same
//! choice (`.enc` for an encrypted home, no suffix for a plaintext one).

use async_trait::async_trait;
use std::sync::{Arc, RwLock};
use tokio::sync::OnceCell;
use tracing::warn;

use super::signed_control::{HeadJson, MinSchemaVersionJson};
use super::storage::{DeviceHead, MinSchemaVersion, StorageError, SyncStorage};
use crate::encryption::{chunked_encrypted_len, EncryptionError, EncryptionService};
use crate::keys::UserKeypair;
use crate::storage::cloud::{BlobBody, CloudHome};

/// How a cloud home protects its objects at rest. An `Encrypted` home seals
/// every object under the library key (the default); a `Plaintext` home stores
/// objects in the clear so the bucket is browsable, and drops the `.enc` suffix.
#[derive(Clone)]
pub enum CloudCipher {
    Encrypted(EncryptionService),
    Plaintext,
}

/// How a cloud home names its blob objects. Paired with the at-rest
/// [`CloudCipher`] by the home's [`HomeStorage`](crate::config::HomeStorage): an
/// opaque home is `Hashed` + encrypted, a browsable home is `Plain` + plaintext.
#[derive(Clone, Copy)]
pub enum BlobPathScheme {
    /// Content-addressed shard `{namespace}/{ab}/{cd}/{id}` (an opaque home).
    Hashed,
    /// The consumer's own readable path, verbatim: `{namespace}/{cloud_path}`
    /// (a browsable home). The consumer must supply `cloud_path` on every blob;
    /// coven errors otherwise.
    Plain,
}

impl BlobPathScheme {
    /// The blob-path scheme a home's storage mode selects: an opaque home
    /// obfuscates (`Hashed`), a browsable home is readable (`Plain`).
    pub fn for_storage(storage: crate::config::HomeStorage) -> Self {
        if storage.is_opaque() {
            BlobPathScheme::Hashed
        } else {
            BlobPathScheme::Plain
        }
    }
}

impl CloudCipher {
    /// The at-rest cipher a home's storage mode selects: an opaque home seals
    /// under its library key (`Encrypted`), a browsable home stores in the clear
    /// (`Plaintext`). The sibling of [`BlobPathScheme::for_storage`] — together
    /// they map a [`HomeStorage`](crate::config::HomeStorage) to its
    /// (path scheme, at-rest cipher) pair.
    ///
    /// `encryption` is the library master service; it is required for (and only
    /// consulted on) an opaque home. `None` is returned only for an opaque home
    /// with no service (a locked library) — a browsable home is always
    /// `Plaintext` regardless. A host streaming a Remote blob via
    /// [`BlobRangeReader`] builds the reader with this cipher so a read applies
    /// the same protection the upload sealed under.
    pub fn for_storage(
        storage: crate::config::HomeStorage,
        encryption: Option<EncryptionService>,
    ) -> Option<Self> {
        if storage.is_opaque() {
            encryption.map(CloudCipher::Encrypted)
        } else {
            Some(CloudCipher::Plaintext)
        }
    }

    /// Protect a control object (heads, changesets, snapshot, snapshot_meta,
    /// min_schema, membership) for storage. Encrypted seals under the library
    /// key; plaintext returns the bytes unchanged.
    pub fn seal(&self, plaintext: Vec<u8>) -> Vec<u8> {
        // A control object is always whole-home scoped; only blobs carry a scope.
        // This is exactly the master-scoped blob path: `encryption_for_scope`
        // maps `Master` to the library key itself.
        self.seal_scoped(crate::blob::ResolvedScope::Master, plaintext)
    }

    /// Recover a control object read from storage. Inverse of [`Self::seal`].
    pub fn open(&self, stored: Vec<u8>) -> Result<Vec<u8>, EncryptionError> {
        self.open_scoped(crate::blob::ResolvedScope::Master, stored)
    }

    /// Protect a blob under its resolved scope. Encrypted derives the scope's key
    /// from the master via [`encryption_for_scope`]; plaintext is passthrough,
    /// ignoring the scope (a plaintext home has no per-scope keys).
    pub(crate) fn seal_scoped(
        &self,
        scope: crate::blob::ResolvedScope,
        plaintext: Vec<u8>,
    ) -> Vec<u8> {
        match self {
            CloudCipher::Encrypted(e) => encryption_for_scope(scope, e).encrypt(&plaintext),
            CloudCipher::Plaintext => plaintext,
        }
    }

    /// Recover a blob under its resolved scope. Inverse of [`Self::seal_scoped`].
    pub(crate) fn open_scoped(
        &self,
        scope: crate::blob::ResolvedScope,
        stored: Vec<u8>,
    ) -> Result<Vec<u8>, EncryptionError> {
        match self {
            CloudCipher::Encrypted(e) => encryption_for_scope(scope, e).decrypt(&stored),
            CloudCipher::Plaintext => Ok(stored),
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

    /// The final object length for a blob of `plaintext_len` bytes under this
    /// cipher: the chunked-encrypted length for an encrypted home, the plaintext
    /// length verbatim for a browsable one.
    pub fn body_len(&self, plaintext_len: u64) -> u64 {
        match self {
            CloudCipher::Encrypted(_) => chunked_encrypted_len(plaintext_len),
            CloudCipher::Plaintext => plaintext_len,
        }
    }

    /// Open a streaming [`BlobBody`] over the local plaintext file at `file_path`,
    /// sealing each chunk under `scope`'s key for an encrypted home or passing the
    /// plaintext through for a browsable one — without ever reading or sealing the
    /// whole blob into memory. The streaming sibling of [`seal_scoped`](Self::seal_scoped),
    /// used by the upload drain.
    pub(crate) async fn open_body(
        &self,
        scope: crate::blob::ResolvedScope,
        file_path: &std::path::Path,
    ) -> Result<BlobBody, String> {
        let plaintext_len = crate::local_blob::file_len(file_path).await?;
        let reader = crate::local_blob::open_reader(file_path).await?;
        let sealer = match self {
            CloudCipher::Encrypted(e) => Some(encryption_for_scope(scope, e).sealer()),
            CloudCipher::Plaintext => None,
        };
        Ok(BlobBody::from_file(
            self.body_len(plaintext_len),
            reader,
            sealer,
        ))
    }
}

/// `SyncStorage` that delegates raw I/O to a `CloudHome` and handles the path
/// layout and the at-rest protection (its [`CloudCipher`]).
pub struct CloudSyncStorage {
    /// The raw cloud backend. `Arc` (not `Box`) because a ranged read hands a
    /// clone to the [`BlobRangeReader`] it builds — the reader holds the home for
    /// the life of a stream and reads across awaits, so the home is genuinely
    /// shared between this storage and the readers it spawns, not owned by one.
    home: Arc<dyn CloudHome>,
    cipher: Arc<RwLock<CloudCipher>>,
    /// How blob objects are keyed. Unlike the cipher, the scheme does not rotate
    /// over a home's life, so it is a plain field with no lock.
    blob_paths: BlobPathScheme,
    /// The device's signing identity. The control objects this storage writes
    /// (its head, the min_schema floor) are signed with it so a reader can
    /// attribute and verify them; the at-rest cipher proves confidentiality, not
    /// authorship.
    keypair: UserKeypair,
}

impl CloudSyncStorage {
    pub fn new(
        home: Arc<dyn CloudHome>,
        cipher: CloudCipher,
        blob_paths: BlobPathScheme,
        keypair: UserKeypair,
    ) -> Self {
        CloudSyncStorage {
            home,
            cipher: Arc::new(RwLock::new(cipher)),
            blob_paths,
            keypair,
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
                Ok(crate::library_dir::LibraryDir::hashed_path(namespace, id)?)
            }
            BlobPathScheme::Plain => {
                let path = cloud_path.ok_or_else(|| {
                    StorageError::S3(format!(
                        "unobfuscated blob-path home requires a cloud_path for blob {namespace}/{id}"
                    ))
                })?;
                crate::library_dir::validate_path_token(namespace)?;
                crate::library_dir::validate_cloud_path(path)?;
                Ok(format!("{namespace}/{path}"))
            }
        }
    }
}

/// The cache namespace a blob's `cloud_key` belongs to: the key's first
/// `/`-component.
///
/// Every blob `cloud_key` [`CloudSyncStorage::blob_key`] produces is `{namespace}/…`
/// in BOTH [`BlobPathScheme`] variants (`Hashed` = `{namespace}/{ab}/{cd}/{id}`,
/// `Plain` = `{namespace}/{cloud_path}`), and a namespace is a single slash-free path
/// token (validated by [`crate::library_dir::validate_path_token`]). So the namespace
/// is always recoverable from the key with no second stored copy — the durable
/// `cloud_outbox` row carries only the key, and the upload drain recovers the
/// namespace here for the cache copy it places (`storage/cache/<namespace>/…`).
/// `split_once` returns the prefix for a real key; a slashless key (never produced by
/// `blob_key`, only by a unit-test fixture that does not exercise namespaced cache
/// placement) has no prefix and is returned whole.
pub(crate) fn namespace_from_cloud_key(cloud_key: &str) -> &str {
    cloud_key
        .split_once('/')
        .map_or(cloud_key, |(namespace, _)| namespace)
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

/// Reads plaintext byte ranges from a single stored blob without fetching the
/// whole object — the ranged analogue of [`CloudSyncStorage::get_blob`].
///
/// On an encrypted home a blob is `[nonce: 24 bytes][encrypted chunks…]` (see
/// [`EncryptionService::encrypt`]). Serving a plaintext range needs the nonce
/// plus only the chunks covering it, never the whole object, so the 24-byte
/// nonce is fetched once on the first read and reused: streaming a blob in N
/// windows issues one nonce read, not N. On a plaintext home the blob is stored
/// verbatim, so a range is read straight through with no nonce or decryption.
///
/// The blob's [`ResolvedScope`](crate::blob::ResolvedScope) is resolved to its
/// key the same way `get_blob` resolves it (see [`encryption_for_scope`]), so a
/// reader serves master-, derived-, and item-key-scoped blobs alike — not only
/// master-key ones. A host that streams a large blob (audio playback, or pinning
/// a file window by window) builds one of these instead of downloading and
/// decrypting the whole object.
pub struct BlobRangeReader {
    home: Arc<dyn CloudHome>,
    /// The scope's key for an encrypted home, resolved once at construction;
    /// `None` for a plaintext home (the blob is read verbatim).
    encryption: Option<EncryptionService>,
    /// The blob's cloud object key (see [`CloudSyncStorage::blob_key`]).
    key: String,
    /// Plaintext length of the blob. Ranges are validated against it, and the
    /// encrypted chunk range is clamped to the matching blob length.
    source_size: u64,
    /// The 24-byte base nonce of an encrypted blob, read once on first use.
    nonce: OnceCell<Vec<u8>>,
}

impl BlobRangeReader {
    /// Build a reader for the blob stored at `key` (see
    /// [`CloudSyncStorage::blob_key`]), `source_size` plaintext bytes long.
    /// `cipher` and `scope` are how the home protects this blob: an encrypted
    /// home resolves `scope` to its key once here; a plaintext home ignores
    /// `scope` and reads verbatim.
    pub fn new(
        home: Arc<dyn CloudHome>,
        cipher: &CloudCipher,
        scope: crate::blob::ResolvedScope,
        key: String,
        source_size: u64,
    ) -> Self {
        let encryption = match cipher {
            CloudCipher::Encrypted(master) => Some(encryption_for_scope(scope, master)),
            CloudCipher::Plaintext => None,
        };
        BlobRangeReader {
            home,
            encryption,
            key,
            source_size,
            nonce: OnceCell::new(),
        }
    }

    /// Read exactly `len` plaintext bytes starting at `offset`. An out-of-range
    /// request errors rather than truncating.
    pub async fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            StorageError::S3(format!("blob range overflow: offset={offset}, len={len}"))
        })?;
        if end > self.source_size {
            return Err(StorageError::S3(format!(
                "blob range {offset}..{end} exceeds blob size {}",
                self.source_size
            )));
        }

        let encryption = match &self.encryption {
            Some(encryption) => encryption,
            // Plaintext home: the blob is stored verbatim, so the plaintext range
            // is exactly the stored byte range — no nonce, no chunking.
            None => {
                return self
                    .home
                    .read_range(&self.key, offset, end)
                    .await
                    .map_err(StorageError::from);
            }
        };

        use crate::encryption::{chunked_encrypted_len, encrypted_chunk_range, CHUNK_SIZE};

        let nonce = self.nonce().await?;

        let (chunk_start, mut chunk_end) = encrypted_chunk_range(offset, end);
        chunk_end = chunk_end.min(chunked_encrypted_len(self.source_size));
        let encrypted_chunks = self
            .home
            .read_range(&self.key, chunk_start, chunk_end)
            .await
            .map_err(StorageError::from)?;

        let first_chunk_index = offset / CHUNK_SIZE as u64;
        encryption
            .decrypt_range_with_offset(nonce, &encrypted_chunks, first_chunk_index, offset, end)
            .map_err(|e| StorageError::Decryption(format!("blob range {offset}..{end}: {e}")))
    }

    /// The cached 24-byte base nonce, read from the encrypted blob's header on
    /// first use and reused for every later range read.
    async fn nonce(&self) -> Result<&[u8], StorageError> {
        use crate::encryption::NONCE_SIZE;
        let nonce = self
            .nonce
            .get_or_try_init(|| async {
                self.home
                    .read_range(&self.key, 0, NONCE_SIZE as u64)
                    .await
                    .map_err(StorageError::from)
            })
            .await?;
        Ok(nonce)
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
            let decoded = match self.cipher().open(stored) {
                Ok(d) => d,
                Err(e) => {
                    warn!("skipping head {device_id} this library cannot decrypt: {e}");
                    continue;
                }
            };

            let head_json: HeadJson = serde_json::from_slice(&decoded)
                .map_err(|e| StorageError::S3(format!("parse head {device_id}: {e}")))?;

            // A head whose signature doesn't verify against its embedded author
            // is forged (the bucket is untrusted) -- skip it like one we can't
            // decrypt, so it can't pollute sync status or drive a per-seq fetch
            // loop. The author the signature is bound to is surfaced so the
            // caller can run the membership (authorization) check.
            if !head_json.verify(device_id) {
                warn!("skipping head {device_id} with an invalid signature");
                continue;
            }

            heads.push(DeviceHead {
                device_id: device_id.to_string(),
                seq: head_json.seq,
                snapshot_seq: head_json.snapshot_seq,
                last_sync: head_json.last_sync,
                author_pubkey: head_json.author_pubkey,
            });
        }

        Ok(heads)
    }

    async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        let key = format!("changes/{device_id}/{seq}{}", self.suffix());
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open(stored)
            .map_err(|e| StorageError::Decryption(format!("changeset {device_id}/{seq}: {e}")))
    }

    async fn put_changeset(
        &self,
        device_id: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let key = format!("changes/{device_id}/{seq}{}", self.suffix());
        let stored = self.cipher().seal(data);
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
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
        let head = HeadJson::signed(
            device_id,
            seq,
            snapshot_seq,
            Some(timestamp.to_string()),
            &self.keypair,
        );
        let json = serde_json::to_vec(&head)
            .map_err(|e| StorageError::S3(format!("serialize head: {e}")))?;
        let stored = self.cipher().seal(json);
        let key = format!("heads/{device_id}.json{}", self.suffix());
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
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
        let stored = self.cipher().seal_scoped(scope, data);
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
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
            .open_scoped(scope, stored)
            .map_err(|e| StorageError::Decryption(format!("blob {namespace}/{id}: {e}")))
    }

    async fn read_blob_range(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::ResolvedScope,
        cloud_path: Option<&str>,
        source_size: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StorageError> {
        // Build the ranged reader over a clone of the shared home and the current
        // cipher (a snapshot of the lock — a key rotation between reads builds a
        // fresh reader next call). The reader owns the chunk math and decryption;
        // this just resolves the key/scope/size it needs. The cipher is cloned out
        // of the lock so the reader doesn't hold the guard across its awaits.
        let key = Self::blob_key(self.blob_paths, namespace, id, cloud_path)?;
        let cipher = self.cipher().clone();
        let reader = BlobRangeReader::new(self.home.clone(), &cipher, scope, key, source_size);
        reader.read(offset, len).await
    }

    async fn put_snapshot(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        crate::library_dir::validate_path_token(author)?;
        let key = format!("snapshot/{author}/{seq}.db{}", self.suffix());
        self.home
            .write(
                &key,
                BlobBody::from_bytes(data),
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn get_snapshot(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        crate::library_dir::validate_path_token(author)?;
        let key = format!("snapshot/{author}/{seq}.db{}", self.suffix());
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

    async fn put_ack(&self, device_id: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let stored = self.cipher().seal(data);
        let key = format!("acks/{device_id}.json{}", self.suffix());
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn get_ack(&self, device_id: &str) -> Result<Vec<u8>, StorageError> {
        let key = format!("acks/{device_id}.json{}", self.suffix());
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open(stored)
            .map_err(|e| StorageError::Decryption(format!("ack {device_id}: {e}")))
    }

    async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
        let key = format!("min_schema_version.json{}", self.suffix());
        let stored = match self.home.read(&key).await {
            Ok(data) => data,
            Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(StorageError::from(e)),
        };

        let decoded = self
            .cipher()
            .open(stored)
            .map_err(|e| StorageError::Decryption(format!("min_schema_version: {e}")))?;

        let parsed: MinSchemaVersionJson = serde_json::from_slice(&decoded)
            .map_err(|e| StorageError::S3(format!("parse min_schema_version: {e}")))?;

        // The bucket is untrusted: a floor set by anyone with the credential can
        // freeze the fleet or force a downgrade. Treat a value whose signature
        // doesn't verify as absent (None) rather than honoring it; the caller
        // separately checks the verified author is a current owner.
        if !parsed.verify() {
            warn!("ignoring min_schema_version with an invalid signature");
            return Ok(None);
        }

        Ok(Some(MinSchemaVersion {
            version: parsed.min_schema_version,
            author_pubkey: parsed.author_pubkey,
        }))
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
        let payload = MinSchemaVersionJson::signed(version, &self.keypair);
        let json = serde_json::to_vec(&payload)
            .map_err(|e| StorageError::S3(format!("serialize min_schema_version: {e}")))?;
        let stored = self.cipher().seal(json);
        let key = format!("min_schema_version.json{}", self.suffix());
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
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
        let key = format!("membership/{author_pubkey}/{seq}{}", self.suffix());
        let stored = self.cipher().seal(data);
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
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
            .open(stored)
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
            .write(
                &key,
                BlobBody::from_bytes(data),
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
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

    async fn put_snapshot_meta(
        &self,
        author: &str,
        seq: u64,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        crate::library_dir::validate_path_token(author)?;
        let stored = self.cipher().seal(data);
        let key = format!("snapshot/{author}/{seq}_meta.json{}", self.suffix());
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn get_snapshot_meta(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
        crate::library_dir::validate_path_token(author)?;
        let key = format!("snapshot/{author}/{seq}_meta.json{}", self.suffix());
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open(stored)
            .map_err(|e| StorageError::Decryption(format!("snapshot_meta {author}/{seq}: {e}")))
    }

    async fn put_snapshot_pointer(&self, data: Vec<u8>) -> Result<(), StorageError> {
        let stored = self.cipher().seal(data);
        let key = format!("snapshot/current.json{}", self.suffix());
        self.home
            .write(
                &key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn get_snapshot_pointer(&self) -> Result<Vec<u8>, StorageError> {
        let key = format!("snapshot/current.json{}", self.suffix());
        let stored = self.home.read(&key).await?;
        self.cipher()
            .open(stored)
            .map_err(|e| StorageError::Decryption(format!("snapshot_pointer: {e}")))
    }

    async fn list_own_snapshot_generations(&self, author: &str) -> Result<Vec<u64>, StorageError> {
        crate::library_dir::validate_path_token(author)?;
        let suffix = self.suffix();
        let prefix = format!("snapshot/{author}/");
        let keys = self.home.list(&prefix).await?;
        let mut seqs = Vec::new();
        for key in &keys {
            // Under this device's own prefix, match only the metadata objects:
            // `snapshot/{author}/{seq}_meta.json{suffix}`. A generation is keyed by
            // its meta (written after the DB image), so listing it means the DB
            // image is already whole. The `{seq}.db` sibling is skipped here. Only
            // this `{author}`'s generations are listed — a peer's live under a
            // different prefix — so ownership is structural, no per-object author
            // check needed.
            let Some(rest) = key
                .strip_prefix(&prefix)
                .and_then(|s| s.strip_suffix(suffix))
            else {
                warn!("snapshot listing: object {key} is not under the prefix with the expected suffix; skipping");
                continue;
            };
            match rest.strip_suffix("_meta.json") {
                Some(seq_str) => match seq_str.parse::<u64>() {
                    Ok(seq) => seqs.push(seq),
                    Err(e) => warn!("snapshot listing: meta key {key} has a non-numeric seq: {e}"),
                },
                // The `{seq}.db` sibling is each generation's expected complement,
                // listed via its meta, so skip it silently; anything else under this
                // author prefix is unexpected and surfaced rather than dropped.
                None if rest.ends_with(".db") => {}
                None => {
                    warn!(
                        "snapshot listing: unexpected object {key} under a snapshot author prefix"
                    )
                }
            }
        }
        seqs.sort_unstable();
        Ok(seqs)
    }

    async fn delete_snapshot_generation(&self, author: &str, seq: u64) -> Result<(), StorageError> {
        crate::library_dir::validate_path_token(author)?;
        let suffix = self.suffix();
        // Delete the db first, then the meta: the meta object is what
        // `list_own_snapshot_generations` keys a generation by, so removing it last
        // means a crash between the two deletes leaves the generation still listed
        // (its meta present) and re-deletable next sweep, never a meta-less db.
        self.home
            .delete(&format!("snapshot/{author}/{seq}.db{suffix}"))
            .await?;
        self.home
            .delete(&format!("snapshot/{author}/{seq}_meta.json{suffix}"))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::ResolvedScope;
    use crate::config::HomeStorage;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;

    /// A blob larger than the in-memory backend's multipart threshold, sealed by
    /// the streaming `open_body` path and written through `CloudHome::write`, lands
    /// as a byte-identical `[base_nonce][sealed chunks]` object that the unchanged
    /// decrypt reads back — proving the multipart streaming path produces the same
    /// wire format the whole-buffer encryptor does.
    #[tokio::test]
    async fn streaming_open_body_multipart_round_trips_through_write() {
        let master = EncryptionService::new_with_key(&[11u8; 32]);
        let cipher = CloudCipher::Encrypted(master.clone());
        let home = InMemoryCloudHome::new();

        // Larger than the backend's 4 MiB threshold so `write` streams it multipart.
        let plaintext: Vec<u8> = (0..9_000_003u32).map(|i| (i % 251) as u8).collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        std::fs::write(&path, &plaintext).unwrap();

        let body = cipher
            .open_body(ResolvedScope::Master, &path)
            .await
            .expect("open streaming body");
        assert!(
            body.len() > home.multipart_threshold(),
            "the body must exceed the threshold so it streams multipart",
        );
        home.write("blob-key", body, &crate::storage::cloud::no_progress())
            .await
            .expect("streaming write");

        // At rest it is the sealed wire format; the unchanged decrypt recovers the
        // plaintext.
        let stored = home.get("blob-key").expect("blob present");
        assert_eq!(
            master.decrypt(&stored).expect("decrypt streamed blob"),
            plaintext,
            "the multipart-streamed object decrypts to the original plaintext",
        );
    }

    /// `CloudCipher::for_storage` maps a home's storage mode to its at-rest
    /// cipher: an opaque home seals under the supplied library key, a browsable
    /// home is always plaintext (ignoring any key), and an opaque home with no
    /// key — a locked library — has no cipher. This is the same mapping
    /// `SyncManager::start_sync` builds the sync loop with, so reads through a
    /// `BlobRangeReader` and writes through the outbox agree on the protection.
    #[test]
    fn for_storage_maps_mode_to_cipher() {
        let master = EncryptionService::new_with_key(&[4u8; 32]);

        assert!(matches!(
            CloudCipher::for_storage(HomeStorage::Opaque, Some(master.clone())),
            Some(CloudCipher::Encrypted(_))
        ));
        // A browsable home is plaintext even if a key is on hand.
        assert!(matches!(
            CloudCipher::for_storage(HomeStorage::Browsable, Some(master)),
            Some(CloudCipher::Plaintext)
        ));
        assert!(matches!(
            CloudCipher::for_storage(HomeStorage::Browsable, None),
            Some(CloudCipher::Plaintext)
        ));
        // An opaque home with no key (a locked library) has no cipher.
        assert!(CloudCipher::for_storage(HomeStorage::Opaque, None).is_none());
    }

    #[test]
    fn plaintext_cipher_returns_owned_buffers_unchanged() {
        let cipher = CloudCipher::Plaintext;

        let control_plaintext = b"control bytes".to_vec();
        let control_plaintext_ptr = control_plaintext.as_ptr();
        let sealed = cipher.seal(control_plaintext);
        assert_eq!(sealed.as_ptr(), control_plaintext_ptr);
        assert_eq!(sealed, b"control bytes");

        let control_stored = b"stored control bytes".to_vec();
        let control_stored_ptr = control_stored.as_ptr();
        let opened = cipher.open(control_stored).expect("open plaintext control");
        assert_eq!(opened.as_ptr(), control_stored_ptr);
        assert_eq!(opened, b"stored control bytes");

        let scoped_plaintext = b"scoped blob bytes".to_vec();
        let scoped_plaintext_ptr = scoped_plaintext.as_ptr();
        let sealed = cipher.seal_scoped(ResolvedScope::Master, scoped_plaintext);
        assert_eq!(sealed.as_ptr(), scoped_plaintext_ptr);
        assert_eq!(sealed, b"scoped blob bytes");

        let scoped_stored = b"stored scoped blob bytes".to_vec();
        let scoped_stored_ptr = scoped_stored.as_ptr();
        let opened = cipher
            .open_scoped(ResolvedScope::Master, scoped_stored)
            .expect("open plaintext blob");
        assert_eq!(opened.as_ptr(), scoped_stored_ptr);
        assert_eq!(opened, b"stored scoped blob bytes");
    }

    /// A `ResolvedScope::Key` blob is encrypted under the explicit (item) key, not
    /// the master: it round-trips with that key and the master key cannot read
    /// it. This is what lets coven scope a blob to a per-item key (the resolved
    /// form of a `BlobScope::Item`) so it can be read — or handed to a share
    /// recipient — without exposing the whole library.
    #[tokio::test]
    async fn key_scoped_blob_round_trips_and_master_cannot_read_it() {
        let master = EncryptionService::new_with_key(&[7u8; 32]);
        let storage = CloudSyncStorage::new(
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Encrypted(master),
            BlobPathScheme::Hashed,
            UserKeypair::generate(),
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
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Encrypted(EncryptionService::new_with_key(&[1u8; 32])),
            BlobPathScheme::Hashed,
            UserKeypair::generate(),
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
                BlobBody::from_bytes(
                    b"a different library's head, encrypted with another key".to_vec(),
                ),
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

    /// A head with a valid signature round-trips through `put_head` / `list_heads`
    /// and surfaces its author; a head whose signature is invalid (written by
    /// anyone with the bucket credential) is skipped, not returned — the bucket is
    /// untrusted, so a forged head must not pollute sync status or drive a fetch
    /// loop.
    #[tokio::test]
    async fn list_heads_verifies_signatures_and_skips_a_forged_head() {
        let keypair = UserKeypair::generate();
        let cipher = CloudCipher::Encrypted(EncryptionService::new_with_key(&[2u8; 32]));
        let storage = CloudSyncStorage::new(
            Arc::new(InMemoryCloudHome::new()),
            cipher.clone(),
            BlobPathScheme::Hashed,
            keypair.clone(),
        );

        // Our own head is written signed by our keypair.
        storage
            .put_head("ours", 9, Some(4), "2026-01-01T00:00:00Z")
            .await
            .expect("put our head");

        // A forged head for another device: a structurally valid `HeadJson` whose
        // signature does not match its author. It is sealed under the same library
        // key (so it decrypts fine), proving the rejection is the *signature*
        // check, not the cipher.
        let forged = HeadJson {
            seq: 100,
            snapshot_seq: None,
            last_sync: None,
            author_pubkey: hex::encode(UserKeypair::generate().public_key),
            signature: hex::encode([0u8; crate::keys::SIGN_BYTES]),
        };
        let sealed = cipher.seal(serde_json::to_vec(&forged).expect("serialize forged head"));
        storage
            .cloud_home()
            .write(
                "heads/forged-device.json.enc",
                BlobBody::from_bytes(sealed),
                &crate::storage::cloud::no_progress(),
            )
            .await
            .expect("write forged head");

        let heads = storage.list_heads().await.expect("list_heads");
        assert_eq!(heads.len(), 1, "only the validly signed head is returned");
        assert_eq!(heads[0].device_id, "ours");
        assert_eq!(heads[0].seq, 9);
        assert_eq!(
            heads[0].author_pubkey,
            hex::encode(keypair.public_key),
            "the verified author is surfaced to the caller",
        );
    }

    /// A validly signed `min_schema_version` round-trips and surfaces its author;
    /// one whose signature is invalid is treated as absent (`None`), so a bucket
    /// writer can't freeze the fleet or force a downgrade by planting a forged
    /// floor.
    #[tokio::test]
    async fn get_min_schema_version_verifies_and_ignores_a_forged_floor() {
        let keypair = UserKeypair::generate();
        let cipher = CloudCipher::Encrypted(EncryptionService::new_with_key(&[3u8; 32]));
        let storage = CloudSyncStorage::new(
            Arc::new(InMemoryCloudHome::new()),
            cipher.clone(),
            BlobPathScheme::Hashed,
            keypair.clone(),
        );

        // A real floor we set verifies and carries our pubkey.
        storage.set_min_schema_version(7).await.expect("set floor");
        let got = storage.get_min_schema_version().await.expect("get floor");
        let got = got.expect("a signed floor is present");
        assert_eq!(got.version, 7);
        assert_eq!(got.author_pubkey, hex::encode(keypair.public_key));

        // Overwrite it with a forged floor (valid shape, bad signature): it is
        // treated as absent.
        let forged = MinSchemaVersionJson {
            min_schema_version: 9999,
            author_pubkey: hex::encode(UserKeypair::generate().public_key),
            signature: hex::encode([0u8; crate::keys::SIGN_BYTES]),
        };
        let sealed = cipher.seal(serde_json::to_vec(&forged).expect("serialize forged floor"));
        storage
            .cloud_home()
            .write(
                "min_schema_version.json.enc",
                BlobBody::from_bytes(sealed),
                &crate::storage::cloud::no_progress(),
            )
            .await
            .expect("write forged floor");
        assert!(
            storage
                .get_min_schema_version()
                .await
                .expect("get forged floor")
                .is_none(),
            "a floor with an invalid signature is treated as absent",
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
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Hashed,
            UserKeypair::generate(),
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

        // Snapshot generation + meta + pointer: literal at rest, bare generational
        // keys under the publishing device's `{author}` prefix.
        let author = "abc123";
        let snap = b"SQLite format 3\0 ... bytes".to_vec();
        storage
            .put_snapshot(author, 0, snap.clone())
            .await
            .expect("put_snapshot");
        assert_eq!(
            home.get("snapshot/abc123/0.db").as_deref(),
            Some(snap.as_slice())
        );
        assert!(
            home.get("snapshot/abc123/0.db.enc").is_none(),
            "no .enc snapshot key"
        );
        assert_eq!(
            storage.get_snapshot(author, 0).await.expect("get_snapshot"),
            snap
        );

        let meta = b"{\"cursors\":{}}".to_vec();
        storage
            .put_snapshot_meta(author, 0, meta.clone())
            .await
            .expect("put_snapshot_meta");
        assert_eq!(
            home.get("snapshot/abc123/0_meta.json").as_deref(),
            Some(meta.as_slice())
        );
        assert!(
            home.get("snapshot/abc123/0_meta.json.enc").is_none(),
            "no .enc meta key"
        );
        assert_eq!(
            storage
                .get_snapshot_meta(author, 0)
                .await
                .expect("get_snapshot_meta"),
            meta
        );

        let pointer = b"{\"seq\":0}".to_vec();
        storage
            .put_snapshot_pointer(pointer.clone())
            .await
            .expect("put_snapshot_pointer");
        assert_eq!(
            home.get("snapshot/current.json").as_deref(),
            Some(pointer.as_slice())
        );
        assert!(
            home.get("snapshot/current.json.enc").is_none(),
            "no .enc pointer key"
        );
        assert_eq!(
            storage
                .get_snapshot_pointer()
                .await
                .expect("get_snapshot_pointer"),
            pointer
        );

        // The generation is listable by seq under its author, and a delete removes
        // both its objects.
        assert_eq!(
            storage
                .list_own_snapshot_generations(author)
                .await
                .expect("list generations"),
            vec![0],
        );
        storage
            .delete_snapshot_generation(author, 0)
            .await
            .expect("delete generation");
        assert!(
            home.get("snapshot/abc123/0.db").is_none()
                && home.get("snapshot/abc123/0_meta.json").is_none(),
            "delete_snapshot_generation removes the generation's db and meta",
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
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::new_with_key(&[3u8; 32])),
            BlobPathScheme::Plain,
            UserKeypair::generate(),
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
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            UserKeypair::generate(),
        );
        assert!(
            storage
                .put_blob("images", "id-1", ResolvedScope::Master, None, b"x".to_vec())
                .await
                .is_err(),
            "put_blob for a plain home with no cloud_path must error, not silently hash",
        );
    }

    /// A `BlobRangeReader` over an encrypted home recovers an arbitrary plaintext
    /// sub-range — here one that straddles a 64 KB chunk boundary — by fetching
    /// only the covering chunks plus the one-time nonce, never the whole blob.
    #[tokio::test]
    async fn blob_range_reader_decrypts_a_multi_chunk_sub_range() {
        let home = InMemoryCloudHome::new();
        let master = EncryptionService::new_with_key(&[5u8; 32]);
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(master.clone()),
            BlobPathScheme::Hashed,
            UserKeypair::generate(),
        );

        // Larger than two 64 KB chunks so a window can straddle a boundary.
        let plaintext: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
        storage
            .put_blob(
                "audio",
                "track1",
                ResolvedScope::Master,
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");

        let key = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "audio", "track1", None)
            .expect("blob_key");
        let reader = BlobRangeReader::new(
            Arc::new(home.clone()) as Arc<dyn CloudHome>,
            &CloudCipher::Encrypted(master),
            ResolvedScope::Master,
            key,
            plaintext.len() as u64,
        );

        // A window straddling the 65_536-byte chunk boundary.
        let (offset, len) = (60_000u64, 20_000u64);
        let got = reader.read(offset, len).await.expect("ranged read");
        assert_eq!(
            got,
            &plaintext[offset as usize..(offset + len) as usize],
            "ranged read across a chunk boundary recovers the plaintext window",
        );

        // A second read reuses the cached nonce and still decrypts correctly.
        let tail = reader.read(149_000, 1_000).await.expect("second read");
        assert_eq!(tail, &plaintext[149_000..150_000]);
    }

    /// On a plaintext home the blob is stored verbatim, so a `BlobRangeReader`
    /// returns the requested byte range straight through, with no key.
    #[tokio::test]
    async fn blob_range_reader_reads_a_plaintext_blob_verbatim() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Hashed,
            UserKeypair::generate(),
        );
        let plaintext: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        storage
            .put_blob(
                "audio",
                "track1",
                ResolvedScope::Master,
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");

        let key = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "audio", "track1", None)
            .expect("blob_key");
        let reader = BlobRangeReader::new(
            Arc::new(home.clone()) as Arc<dyn CloudHome>,
            &CloudCipher::Plaintext,
            ResolvedScope::Master,
            key,
            plaintext.len() as u64,
        );
        let got = reader.read(40_000, 10_000).await.expect("ranged read");
        assert_eq!(got, &plaintext[40_000..50_000]);
    }

    /// The reader resolves the blob's scope to its key: a blob sealed under a
    /// derived key reads back through a reader with the same derived scope, while
    /// a reader with the wrong scope (master) cannot decrypt it.
    #[tokio::test]
    async fn blob_range_reader_resolves_the_blob_scope() {
        let home = InMemoryCloudHome::new();
        let master = EncryptionService::new_with_key(&[7u8; 32]);
        let cipher = CloudCipher::Encrypted(master);
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            cipher.clone(),
            BlobPathScheme::Hashed,
            UserKeypair::generate(),
        );

        let scope_id = "release-42".to_string();
        let plaintext: Vec<u8> = (0..80_000u32).map(|i| (i % 251) as u8).collect();
        storage
            .put_blob(
                "audio",
                "track1",
                ResolvedScope::Derived(scope_id.clone()),
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");

        let key = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "audio", "track1", None)
            .expect("blob_key");
        let home_arc = Arc::new(home.clone()) as Arc<dyn CloudHome>;

        let derived = BlobRangeReader::new(
            home_arc.clone(),
            &cipher,
            ResolvedScope::Derived(scope_id),
            key.clone(),
            plaintext.len() as u64,
        );
        assert_eq!(
            derived.read(10_000, 20_000).await.expect("derived read"),
            &plaintext[10_000..30_000],
            "the matching derived scope decrypts the range",
        );

        let wrong = BlobRangeReader::new(
            home_arc,
            &cipher,
            ResolvedScope::Master,
            key,
            plaintext.len() as u64,
        );
        assert!(
            wrong.read(10_000, 20_000).await.is_err(),
            "the master scope must not decrypt a derived-scoped blob",
        );
    }

    /// A read past the blob's plaintext length is a surfaced error, not a
    /// truncated read; a zero-length read is an empty result, not an error.
    #[tokio::test]
    async fn blob_range_reader_rejects_an_out_of_range_read() {
        let home = InMemoryCloudHome::new();
        let master = EncryptionService::new_with_key(&[9u8; 32]);
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(master.clone()),
            BlobPathScheme::Hashed,
            UserKeypair::generate(),
        );
        let plaintext = b"a short blob".to_vec();
        storage
            .put_blob(
                "audio",
                "track1",
                ResolvedScope::Master,
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");
        let key = CloudSyncStorage::blob_key(BlobPathScheme::Hashed, "audio", "track1", None)
            .expect("blob_key");
        let reader = BlobRangeReader::new(
            Arc::new(home) as Arc<dyn CloudHome>,
            &CloudCipher::Encrypted(master),
            ResolvedScope::Master,
            key,
            plaintext.len() as u64,
        );
        assert!(
            reader.read(8, 100).await.is_err(),
            "a range past the blob length must error",
        );
        assert!(reader.read(0, 0).await.expect("empty read").is_empty());
    }
}
