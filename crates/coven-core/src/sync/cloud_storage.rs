//! `SyncStorage` implementation backed by any `CloudHome`.
//!
//! Handles the cloud home path layout (where keys, heads, images, etc. live)
//! and how objects are protected at rest. The underlying `CloudHome` only deals
//! in raw bytes and flat keys; this layer applies the [`CloudCipher`] — sealing
//! every object under the store key for an encrypted home, or storing it
//! verbatim for a plaintext one — and drives the object-key suffix off the same
//! choice (`.enc` for an encrypted home, no suffix for a plaintext one).

use async_trait::async_trait;
use std::sync::{Arc, RwLock};
use tokio::sync::OnceCell;
use tracing::warn;

use super::storage::{ProtocolObjectListing, ProtocolObjectLocator, StorageError, SyncStorage};
use crate::encryption::{chunked_encrypted_len, EncryptionError, EncryptionService};
use crate::keys::UserKeypair;
use crate::storage::cloud::{BlobBody, CloudHome, CopyIdRef};

/// Every encrypted object carries this cleartext prefix naming the key it was
/// sealed under, by 8-byte fingerprint: magic, then the fingerprint. A read
/// resolves that exact key from the keyring rather than trusting a generation
/// number a fork could reuse.
const KEY_TAG_MAGIC: &[u8; 4] = b"CKF1";
const KEY_FINGERPRINT_LEN: usize = 8;
const KEY_TAG_LEN: usize = KEY_TAG_MAGIC.len() + KEY_FINGERPRINT_LEN;

/// How a cloud home protects its objects at rest. An `Encrypted` home seals
/// every object under the store key (the default); a `Plaintext` home stores
/// objects in the clear so the bucket is browsable, and drops the `.enc` suffix.
#[derive(Clone)]
pub enum CloudCipher {
    Encrypted(EncryptionService),
    Plaintext,
}

/// A sync session's fixed at-rest representation. The mode is selected once at
/// construction: plaintext has no mutable key state, while encrypted sessions
/// may merge new key generations without ever becoming plaintext.
pub struct CloudCipherState {
    mode: CloudCipherMode,
}

/// Read-only access to a session cipher snapshot. Production storage implements
/// this with [`CloudCipherState`], whose mode cannot change. The test-utils
/// implementation for a raw lock exists only for injected engine tests.
pub trait CloudCipherAccess: Send + Sync {
    fn snapshot(&self) -> CloudCipher;
    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError>;
}

enum CloudCipherMode {
    Encrypted(RwLock<EncryptionService>),
    Plaintext,
}

impl CloudCipherState {
    pub fn new(cipher: CloudCipher) -> Self {
        let mode = match cipher {
            CloudCipher::Encrypted(encryption) => {
                CloudCipherMode::Encrypted(RwLock::new(encryption))
            }
            CloudCipher::Plaintext => CloudCipherMode::Plaintext,
        };
        Self { mode }
    }

    pub fn is_plaintext(&self) -> bool {
        matches!(self.mode, CloudCipherMode::Plaintext)
    }

    pub fn encryption(&self) -> Option<EncryptionService> {
        match &self.mode {
            CloudCipherMode::Encrypted(encryption) => Some(encryption.read().unwrap().clone()),
            CloudCipherMode::Plaintext => None,
        }
    }

    pub(crate) fn snapshot(&self) -> CloudCipher {
        match &self.mode {
            CloudCipherMode::Encrypted(encryption) => {
                CloudCipher::Encrypted(encryption.read().unwrap().clone())
            }
            CloudCipherMode::Plaintext => CloudCipher::Plaintext,
        }
    }

    pub(crate) fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        let CloudCipherMode::Encrypted(live) = &self.mode else {
            return Err(crate::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            ));
        };
        let mut live = live.write().unwrap();
        let merged = live.merged_with(new_encryption);
        if merged.key_count() == live.key_count() {
            return Ok(None);
        }
        custody.persist(&crate::encryption::MasterKeyring::from(merged.clone()))?;
        *live = merged;
        Ok(Some(live.fingerprint()))
    }
}

impl CloudCipherAccess for CloudCipherState {
    fn snapshot(&self) -> CloudCipher {
        CloudCipherState::snapshot(self)
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        CloudCipherState::merge_key_rotation(self, new_encryption, custody)
    }
}

impl CloudCipherAccess for Arc<CloudCipherState> {
    fn snapshot(&self) -> CloudCipher {
        self.as_ref().snapshot()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        self.as_ref().merge_key_rotation(new_encryption, custody)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl CloudCipherAccess for RwLock<CloudCipher> {
    fn snapshot(&self) -> CloudCipher {
        self.read().unwrap().clone()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        let mut cipher = self.write().unwrap();
        let CloudCipher::Encrypted(live) = &mut *cipher else {
            return Err(crate::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            ));
        };
        let merged = live.merged_with(new_encryption);
        if merged.key_count() == live.key_count() {
            return Ok(None);
        }
        custody.persist(&crate::encryption::MasterKeyring::from(merged.clone()))?;
        *live = merged;
        Ok(Some(live.fingerprint()))
    }
}

/// The cloud has committed the store key to `committed_generation`, and this
/// device's live cipher is still sealing under `live_generation`. A member
/// removal rotates the store key on the cloud before this device necessarily
/// folds the new generation into its own cipher (custody can fail to persist it
/// locally even though the cloud rotation is durable), so the two can
/// transiently disagree. Every seal for the cloud refuses while this holds,
/// rather than sealing new data under a generation the store has already
/// superseded — the removed member still holds a key for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "the store key rotated to generation {committed_generation}, which this device has not \
     adopted (still sealing under generation {live_generation}); refusing to seal for the \
     cloud until adoption completes"
)]
pub struct RotationPending {
    pub committed_generation: u64,
    pub live_generation: u64,
}

/// Whether the cloud has committed the store key to a generation this device's
/// live cipher has not adopted. Set when a member removal commits its rotation
/// but this device's own custody fails to persist the new key locally, or when a
/// peer's rotation is discovered on refresh and can't be adopted the same way;
/// cleared only alongside the cipher swap that adopts it (see
/// [`crate::sync::membership_ops::apply_key_rotation`]). `None` — the default —
/// means the live cipher already holds everything the store has committed.
///
/// Shared (behind one `Arc`, via [`CloudSyncStorage::shared_pending_rotation`])
/// across every path that seals data for the cloud — changesets, heads, blobs,
/// tombstones, snapshots — so a rotation this device can't adopt blocks all of
/// them the same way, not just the removal call that discovered it. This is the
/// structural half of the invariant: this device must never seal under a
/// generation the store has already superseded.
pub struct PendingRotation(std::sync::RwLock<Option<u64>>);

impl Default for PendingRotation {
    fn default() -> Self {
        Self(std::sync::RwLock::new(None))
    }
}

impl PendingRotation {
    pub fn none() -> Self {
        Self::default()
    }

    /// Record that the cloud has committed `generation` and this device has not
    /// folded it into its live cipher. Forward-only: a generation not newer than
    /// one already recorded leaves the recorded value untouched, so an older
    /// rediscovery (e.g. a decoy wrap from a non-rotating owner) can never erase
    /// a genuinely newer generation already known to be pending.
    pub fn mark_committed(&self, generation: u64) {
        let mut recorded = self.0.write().unwrap();
        if recorded.is_none_or(|g| generation > g) {
            *recorded = Some(generation);
        }
    }

    /// Clear the marker: the live cipher now holds everything committed.
    pub fn clear(&self) {
        *self.0.write().unwrap() = None;
    }

    /// Clear the mark only if `cipher`'s live seal key now covers the committed
    /// generation; a higher generation still pending stays marked. The adoption
    /// counterpart of [`Self::mark_committed`] — a merge that adopts a same- or
    /// higher-generation key resolves the pause, but one that leaves a strictly
    /// newer committed generation unadopted does not.
    pub fn resolve(&self, cipher: &CloudCipher) {
        if self.check(cipher).is_ok() {
            self.clear();
        }
    }

    /// The recorded committed generation, if any is pending — for status
    /// reporting independent of a specific cipher snapshot.
    pub fn pending_generation(&self) -> Option<u64> {
        *self.0.read().unwrap()
    }

    /// Check `cipher` against the committed generation, if one is pending. A
    /// plaintext home never rotates a store key (sharing, and hence removal,
    /// requires an encrypted home), so it is never blocked.
    pub fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        let live_generation = match cipher {
            CloudCipher::Encrypted(enc) => enc.current_generation(),
            CloudCipher::Plaintext => return Ok(()),
        };
        if let Some(committed_generation) = self.pending_generation() {
            if committed_generation > live_generation {
                return Err(RotationPending {
                    committed_generation,
                    live_generation,
                });
            }
        }
        Ok(())
    }
}

/// The `protocol_state` key under which a device durably records that a committed
/// store-key rotation is outstanding (the committed generation as decimal). Set
/// when [`PendingRotation`] is marked, deleted when the mark resolves. Restored
/// into the in-memory marker at open so a restart cannot forget an unadopted
/// rotation and resume sealing under the superseded generation — the removed
/// member still holds a key for it — even if a fresh cloud scan, lagging, does
/// not re-surface the rotation.
pub const PENDING_ROTATION_STATE_KEY: &str = "pending_rotation_generation";

/// Restore the in-memory [`PendingRotation`] from its durable `protocol_state`
/// record, if one is set. Called at open, before the first cycle seals anything.
pub async fn restore_pending_rotation(
    db: &crate::database::Database,
    pending_rotation: &PendingRotation,
) -> Result<(), crate::database::DbError> {
    if let Some(value) = db.get_protocol_state(PENDING_ROTATION_STATE_KEY).await? {
        match value.parse::<u64>() {
            Ok(generation) => pending_rotation.mark_committed(generation),
            Err(_) => warn!("ignoring malformed persisted pending-rotation generation {value:?}"),
        }
    }
    Ok(())
}

/// Write the in-memory [`PendingRotation`]'s current state to its durable
/// `protocol_state` record: the committed generation while a rotation is pending, or
/// a delete once it has resolved.
pub async fn persist_pending_rotation(
    db: &crate::database::Database,
    pending_rotation: &PendingRotation,
) -> Result<(), crate::database::DbError> {
    match pending_rotation.pending_generation() {
        Some(generation) => {
            db.set_protocol_state(PENDING_ROTATION_STATE_KEY, &generation.to_string())
                .await
        }
        None => db.delete_protocol_state(PENDING_ROTATION_STATE_KEY).await,
    }
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
    /// under its store key (`Encrypted`), a browsable home stores in the clear
    /// (`Plaintext`). The sibling of [`BlobPathScheme::for_storage`] — together
    /// they map a [`HomeStorage`](crate::config::HomeStorage) to its
    /// (path scheme, at-rest cipher) pair.
    ///
    /// `encryption` is the store master service; it is required for (and only
    /// consulted on) an opaque home. `None` is returned only for an opaque home
    /// with no service (a locked store) — a browsable home is always
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

    /// Protect an immutable Store object or mutable membership/key object for
    /// storage. Encrypted homes seal under the current store-key generation and
    /// prefix that generation in cleartext; plaintext homes return the bytes
    /// unchanged.
    pub fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        // A control object is always whole-home scoped; only blobs carry a scope.
        // This is exactly the master-scoped blob path: `encryption_for_scope`
        // maps `Master` to the store key itself.
        self.seal_scoped(crate::blob::BlobScope::Master, plaintext, aad_context)
    }

    /// Recover a control object read from storage. Inverse of [`Self::seal`].
    pub fn open(&self, stored: Vec<u8>, aad_context: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.open_scoped(crate::blob::BlobScope::Master, stored, aad_context)
    }

    /// Protect a blob under its scope. Encrypted blobs carry the current
    /// store-key generation in cleartext, so a later read knows which
    /// generation to open with.
    pub(crate) fn seal_scoped(
        &self,
        scope: crate::blob::BlobScope,
        plaintext: Vec<u8>,
        aad_context: &[u8],
    ) -> Vec<u8> {
        match self {
            CloudCipher::Encrypted(e) => seal_scoped_encrypted(scope, e, &plaintext, aad_context),
            CloudCipher::Plaintext => plaintext,
        }
    }

    /// Recover a blob under its resolved scope. Inverse of [`Self::seal_scoped`].
    pub(crate) fn open_scoped(
        &self,
        scope: crate::blob::BlobScope,
        stored: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        match self {
            CloudCipher::Encrypted(e) => open_scoped_encrypted(scope, e, &stored, aad_context),
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
    /// cipher: the generation tag plus the chunked-encrypted length for an
    /// encrypted home, the plaintext length verbatim for a browsable one.
    pub fn body_len(&self, plaintext_len: u64) -> u64 {
        match self {
            CloudCipher::Encrypted(_) => chunked_encrypted_len(plaintext_len) + KEY_TAG_LEN as u64,
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
        scope: crate::blob::BlobScope,
        file_path: &std::path::Path,
        aad_context: &[u8],
    ) -> Result<BlobBody, String> {
        let plaintext_len = crate::local_blob::file_len(file_path).await?;
        let reader = crate::local_blob::open_reader(file_path).await?;
        let (sealer, prefix) = match self {
            CloudCipher::Encrypted(e) => {
                let (encryption, prefix) = sealing_encryption_for_scope(scope, e);
                (Some(encryption.sealer(plaintext_len, aad_context)), prefix)
            }
            CloudCipher::Plaintext => (None, Vec::new()),
        };
        Ok(BlobBody::from_file_with_prefix(
            self.body_len(plaintext_len),
            reader,
            sealer,
            prefix,
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
    cipher: Arc<CloudCipherState>,
    /// Whether a committed rotation is outstanding — see [`PendingRotation`].
    /// Shared the same way `cipher` is, so a member removal or a refresh cycle
    /// that discovers a rotation this device can't adopt blocks every seal path,
    /// not just the one that discovered it.
    pending_rotation: Arc<PendingRotation>,
    /// How blob objects are keyed. Unlike the cipher, the scheme does not rotate
    /// over a home's life, so it is a plain field with no lock.
    blob_paths: BlobPathScheme,
    store_id: String,
    copy_ids: Option<CopyIdRef>,
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
        store_id: impl Into<String>,
        keypair: UserKeypair,
    ) -> Self {
        CloudSyncStorage {
            home,
            cipher: Arc::new(CloudCipherState::new(cipher)),
            pending_rotation: Arc::new(PendingRotation::none()),
            blob_paths,
            store_id: store_id.into(),
            keypair,
            copy_ids: None,
        }
    }

    /// Install the composition root's physical-copy id source. Store protocol
    /// appends fail loudly until this is supplied.
    pub fn with_copy_ids(mut self, copy_ids: CopyIdRef) -> Self {
        self.copy_ids = Some(copy_ids);
        self
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_paths
    }

    pub(crate) fn store_id(&self) -> &str {
        &self.store_id
    }

    pub(crate) fn user_keypair(&self) -> &UserKeypair {
        &self.keypair
    }

    /// The session's fixed-mode cipher state. The state exposes key-generation
    /// merging but no operation that can replace encrypted mode with plaintext.
    pub(crate) fn cipher_state(&self) -> &Arc<CloudCipherState> {
        &self.cipher
    }

    /// Return a shared reference to the rotation-pending marker for external use
    /// — the same instance a member removal (or a refresh cycle) marks when it
    /// commits a rotation this device has not adopted, so every seal path (this
    /// storage's own, plus the blob upload/tombstone drains, which seal directly
    /// against a `CloudCipher` rather than through this trait) refuses together.
    pub fn shared_pending_rotation(&self) -> Arc<PendingRotation> {
        self.pending_rotation.clone()
    }

    /// Borrow the underlying CloudHome for direct access (e.g., grant_access/revoke_access).
    pub fn cloud_home(&self) -> &dyn CloudHome {
        &*self.home
    }

    fn cipher(&self) -> CloudCipher {
        self.cipher.snapshot()
    }

    /// The object-key suffix the current cipher implies.
    fn suffix(&self) -> &'static str {
        self.cipher().suffix()
    }

    /// This device's hex public key — the `{uploader}` segment its own blob
    /// uploads are keyed under. A device only ever writes blobs it authored, so a
    /// write always keys under itself; a read resolves the uploader of the blob it
    /// wants (which may be a peer) and passes it in.
    pub(crate) fn self_uploader(&self) -> String {
        hex::encode(self.keypair.public_key())
    }

    /// The cipher to seal new data under — refuses while the cloud has committed
    /// a rotation this device has not adopted, rather than sealing under the
    /// generation the store has superseded. Every write that protects data under
    /// the store key calls this instead of reading `self.cipher()` directly;
    /// reads/opens are unaffected (they resolve their own generation from the
    /// ciphertext's tag) and keep reading the cipher plainly.
    fn cipher_for_seal(&self) -> Result<CloudCipher, StorageError> {
        let cipher = self.cipher();
        self.pending_rotation.check(&cipher)?;
        Ok(cipher)
    }

    fn aad_context(&self, key: &str) -> Vec<u8> {
        cloud_aad_context(&self.store_id, key)
    }

    async fn write_blob_sealed(
        &self,
        key: &str,
        scope: crate::blob::BlobScope,
        plaintext: Vec<u8>,
    ) -> Result<(), StorageError> {
        let stored = self
            .cipher_for_seal()?
            .seal_scoped(scope, plaintext, &self.aad_context(key));
        self.home
            .write(
                key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(())
    }

    async fn read_blob_sealed(
        &self,
        key: &str,
        scope: crate::blob::BlobScope,
        label: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let stored = self.home.read(key).await?;
        self.cipher()
            .open_scoped(scope, stored, &self.aad_context(key))
            .map_err(|e| StorageError::Decryption(format!("{label}: {e}")))
    }

    /// The cloud object key for a blob under the home's [`BlobPathScheme`].
    ///
    /// **A cloud object is never rewritten with different bytes, so no two blobs ever
    /// share a key.** `Hashed` gets that from the key itself; `Plain` gets it from the
    /// blob's declared [`BlobReplacement`](crate::blob::BlobReplacement), which coven
    /// enforces where a blob is derived from its row ([`crate::blob::decl::BlobDecls`]) —
    /// a replaceable blob's readable path must name it, and a write-once blob's row can
    /// never be repointed. Either way, an object's *presence* at a blob's key is proof of
    /// its *content*, which is what lets the push skip an upload without asking a sealed
    /// object what it holds.
    ///
    /// `Hashed` ignores `cloud_path` and shards by the id under the uploading
    /// device: `{namespace}/{uploader}/{ab}/{cd}/{id}` — the id is right there, and the
    /// `{uploader}` segment aligns the keyspace to the storage-access rule (a member
    /// writes only under its own public key), so `uploader` is required and a missing one
    /// is an error.
    ///
    /// `Plain` uses the consumer's `cloud_path` verbatim: `{namespace}/{cloud_path}`,
    /// keeping the bucket browsable. Plain blob naming carries no uploader segment
    /// and ignores `uploader`; the store still has membership authorization. A
    /// `Plain` home with no `cloud_path` is an error — coven never silently falls
    /// back to the hashed layout, which would scatter readable-path blobs under
    /// unfindable shard keys.
    pub fn blob_key(
        scheme: BlobPathScheme,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, StorageError> {
        match scheme {
            BlobPathScheme::Hashed => {
                let uploader = uploader.ok_or_else(|| {
                    StorageError::Parse(format!(
                        "an opaque-home blob requires an uploader for {namespace}/{id}"
                    ))
                })?;
                Ok(crate::store_dir::StoreDir::uploader_hashed_key(
                    namespace, uploader, id,
                )?)
            }
            BlobPathScheme::Plain => {
                let path = cloud_path.ok_or_else(|| {
                    StorageError::Parse(format!(
                        "unobfuscated blob-path home requires a cloud_path for blob {namespace}/{id}"
                    ))
                })?;
                crate::store_dir::validate_path_token(namespace)?;
                crate::store_dir::validate_cloud_path(path)?;
                Ok(format!("{namespace}/{path}"))
            }
        }
    }
}

/// The cache namespace a blob's `cloud_key` belongs to: the key's first
/// `/`-component.
///
/// The namespace prefix of a blob `cloud_key` — the segment before the first `/`.
///
/// Every blob `cloud_key` [`CloudSyncStorage::blob_key`] produces is `{namespace}/…`
/// in BOTH [`BlobPathScheme`] variants (`Hashed` = `{namespace}/{ab}/{cd}/{id}`,
/// `Plain` = `{namespace}/{cloud_path}`), and a namespace is a single slash-free path
/// token (validated by [`crate::store_dir::validate_path_token`]). So the namespace
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

/// The `EncryptionService` a blob's `scope` selects, against `master`: the
/// store master itself, or a per-scope key derived from it. The blob storage
/// methods and the outbox drain both turn a [`crate::blob::BlobScope`] into a
/// key the same way, so they share this one mapping. Only an encrypted home has
/// per-scope keys, so this is reached only from the [`CloudCipher::Encrypted`]
/// branches.
pub(crate) fn encryption_for_scope(
    scope: crate::blob::BlobScope,
    master: &EncryptionService,
) -> EncryptionService {
    match scope {
        crate::blob::BlobScope::Master => master.clone(),
        crate::blob::BlobScope::Derived(s) => master.derive_scoped(&s),
    }
}

pub(crate) fn cloud_aad_context(store_id: &str, cloud_key: &str) -> Vec<u8> {
    let mut context =
        Vec::with_capacity(std::mem::size_of::<u64>() * 2 + store_id.len() + cloud_key.len());
    context.extend_from_slice(&(store_id.len() as u64).to_le_bytes());
    context.extend_from_slice(store_id.as_bytes());
    context.extend_from_slice(&(cloud_key.len() as u64).to_le_bytes());
    context.extend_from_slice(cloud_key.as_bytes());
    context
}

fn key_tag(fingerprint: &[u8; KEY_FINGERPRINT_LEN]) -> Vec<u8> {
    let mut tag = Vec::with_capacity(KEY_TAG_LEN);
    tag.extend_from_slice(KEY_TAG_MAGIC);
    tag.extend_from_slice(fingerprint);
    tag
}

fn read_key_tag(stored: &[u8]) -> Result<([u8; KEY_FINGERPRINT_LEN], &[u8]), EncryptionError> {
    if stored.len() < KEY_TAG_LEN {
        return Err(EncryptionError::Decryption(
            "ciphertext too short for key tag".to_string(),
        ));
    }
    if &stored[..KEY_TAG_MAGIC.len()] != KEY_TAG_MAGIC {
        return Err(EncryptionError::Decryption(
            "ciphertext missing key tag".to_string(),
        ));
    }
    let mut fingerprint = [0u8; KEY_FINGERPRINT_LEN];
    fingerprint.copy_from_slice(&stored[KEY_TAG_MAGIC.len()..KEY_TAG_LEN]);
    Ok((fingerprint, &stored[KEY_TAG_LEN..]))
}

/// The key `scope` seals under plus the cleartext key-tag prefix every encrypted
/// object carries (the master seal key's fingerprint, so a later read resolves
/// the exact key to open with — for a derived scope it re-derives from that
/// master key).
fn sealing_encryption_for_scope(
    scope: crate::blob::BlobScope,
    master: &EncryptionService,
) -> (EncryptionService, Vec<u8>) {
    (
        encryption_for_scope(scope, master),
        key_tag(&master.seal_fingerprint()),
    )
}

fn opening_encryption_for_scope(
    scope: crate::blob::BlobScope,
    master: &EncryptionService,
    fingerprint: &[u8; KEY_FINGERPRINT_LEN],
) -> Result<EncryptionService, EncryptionError> {
    match scope {
        crate::blob::BlobScope::Master => master.service_for_fingerprint(fingerprint),
        crate::blob::BlobScope::Derived(scope_id) => {
            master.derive_scoped_for_fingerprint(fingerprint, &scope_id)
        }
    }
}

fn seal_scoped_encrypted(
    scope: crate::blob::BlobScope,
    master: &EncryptionService,
    plaintext: &[u8],
    aad_context: &[u8],
) -> Vec<u8> {
    let (encryption, mut prefix) = sealing_encryption_for_scope(scope, master);
    prefix.extend(encryption.encrypt(plaintext, aad_context));
    prefix
}

fn open_scoped_encrypted(
    scope: crate::blob::BlobScope,
    master: &EncryptionService,
    stored: &[u8],
    aad_context: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    let (fingerprint, ciphertext) = read_key_tag(stored)?;
    opening_encryption_for_scope(scope, master, &fingerprint)?.decrypt(ciphertext, aad_context)
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
/// The blob's [`BlobScope`](crate::blob::BlobScope) is resolved to its
/// key the same way `get_blob` resolves it (see [`encryption_for_scope`]), so a
/// reader serves master- and derived-scoped blobs alike. A host that streams a
/// large blob (audio playback, or pinning a file window by window) builds one of
/// these instead of downloading and decrypting the whole object.
pub struct BlobRangeReader {
    home: Arc<dyn CloudHome>,
    /// The scope's key for an encrypted home, resolved once at construction;
    /// `None` for a plaintext home (the blob is read verbatim).
    encryption: Option<RangeEncryption>,
    /// The blob's cloud object key (see [`CloudSyncStorage::blob_key`]).
    key: String,
    /// Plaintext length of the blob. Ranges are validated against it, and the
    /// encrypted chunk range is clamped to the matching blob length.
    source_size: u64,
    /// The encrypted blob header, read once on first use.
    header: OnceCell<RangeHeader>,
}

/// A whole-blob download reader that streams the plaintext front-to-back while
/// folding each chunk into a content hasher, and — on reaching the end — refuses
/// to hand the terminal "done" signal to the atomic writer unless the whole-blob
/// hash matches the blob's author-signed `expected_hash`. Because the writer
/// commits the file only after this reader returns the empty terminal chunk, a
/// hash mismatch aborts before the temp file is renamed, so a tampered or
/// rolled-back object never becomes a cache file a later (hash-unchecked) cache
/// hit would serve. The per-chunk AEAD already authenticates each chunk's bytes
/// under the store key; this adds that the *whole* object is the one the row's
/// author signed.
struct HashVerifyingPlaintextReader {
    reader: BlobRangeReader,
    offset: u64,
    remaining: u64,
    hasher: crate::blob::ContentHasher,
    expected_hash: String,
    key: String,
}

#[async_trait]
impl crate::local_blob::PlaintextChunkReader for HashVerifyingPlaintextReader {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        if max == 0 {
            return Ok(Vec::new());
        }
        if self.remaining == 0 {
            // End of the plaintext: finalize the running hash and require it to
            // match the row's signed hash before signalling "done" (an empty
            // chunk), which is what lets the atomic writer commit the file. A
            // mismatch returns an error instead, so the write aborts before the
            // rename and nothing is cached.
            let hasher = std::mem::take(&mut self.hasher);
            let actual = hasher.finish();
            if actual != self.expected_hash {
                return Err(format!(
                    "blob {} content hash mismatch: expected {}, got {actual}",
                    self.key, self.expected_hash
                ));
            }
            return Ok(Vec::new());
        }
        let len = self.remaining.min(max as u64);
        let chunk = self
            .reader
            .read(self.offset, len)
            .await
            .map_err(|e| e.to_string())?;
        if chunk.len() as u64 != len {
            return Err(format!(
                "short blob range read at {}: got {} of {len} bytes",
                self.offset,
                chunk.len()
            ));
        }
        self.hasher.update(&chunk);
        self.offset += chunk.len() as u64;
        self.remaining = self.remaining.saturating_sub(chunk.len() as u64);
        Ok(chunk)
    }
}

/// What an encrypted home needs to open a blob's ranged reads: the master
/// service (which generation-resolves once the header's tag is read), the
/// blob's scope, and the AAD context.
struct RangeEncryption {
    master: EncryptionService,
    scope: crate::blob::BlobScope,
    aad_context: Vec<u8>,
}

struct RangeHeader {
    encryption: EncryptionService,
    nonce: Vec<u8>,
    chunk_base: u64,
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
        scope: crate::blob::BlobScope,
        key: String,
        source_size: u64,
        aad_context: Vec<u8>,
    ) -> Self {
        let encryption = match cipher {
            CloudCipher::Encrypted(master) => Some(RangeEncryption {
                master: master.clone(),
                scope,
                aad_context,
            }),
            CloudCipher::Plaintext => None,
        };
        BlobRangeReader {
            home,
            encryption,
            key,
            source_size,
            header: OnceCell::new(),
        }
    }

    /// Read exactly `len` plaintext bytes starting at `offset`. An out-of-range
    /// request errors rather than truncating.
    pub async fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            StorageError::Storage(format!("blob range overflow: offset={offset}, len={len}"))
        })?;
        if end > self.source_size {
            return Err(StorageError::Storage(format!(
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

        let header = self.header(encryption).await?;

        let (chunk_start, mut chunk_end) = encrypted_chunk_range(offset, end);
        chunk_end = chunk_end.min(chunked_encrypted_len(self.source_size));
        let stored_chunk_start =
            header.chunk_base + (chunk_start - crate::encryption::NONCE_SIZE as u64);
        let stored_chunk_end =
            header.chunk_base + (chunk_end - crate::encryption::NONCE_SIZE as u64);
        let encrypted_chunks = self
            .home
            .read_range(&self.key, stored_chunk_start, stored_chunk_end)
            .await
            .map_err(StorageError::from)?;

        let first_chunk_index = offset / CHUNK_SIZE as u64;
        header
            .encryption
            .decrypt_range_with_offset(
                &header.nonce,
                &encrypted_chunks,
                first_chunk_index,
                offset,
                end,
                self.source_size,
                &encryption.aad_context,
            )
            .map_err(|e| StorageError::Decryption(format!("blob range {offset}..{end}: {e}")))
    }

    /// The cached encrypted blob header, read once and reused for later range reads.
    async fn header(&self, encryption: &RangeEncryption) -> Result<&RangeHeader, StorageError> {
        use crate::encryption::NONCE_SIZE;
        self.header
            .get_or_try_init(|| async {
                let header = self
                    .home
                    .read_range(&self.key, 0, (KEY_TAG_LEN + NONCE_SIZE) as u64)
                    .await
                    .map_err(StorageError::from)?;
                if header.len() < KEY_TAG_LEN + NONCE_SIZE {
                    return Err(StorageError::Decryption(format!(
                        "blob header too short: expected {}, got {}",
                        KEY_TAG_LEN + NONCE_SIZE,
                        header.len()
                    )));
                }
                let (fingerprint, nonce_and_chunks) = read_key_tag(&header)
                    .map_err(|e| StorageError::Decryption(format!("blob key tag: {e}")))?;
                let service = opening_encryption_for_scope(
                    encryption.scope.clone(),
                    &encryption.master,
                    &fingerprint,
                )
                .map_err(|e| {
                    StorageError::Decryption(format!("blob key {}: {e}", hex::encode(fingerprint)))
                })?;
                Ok(RangeHeader {
                    encryption: service,
                    nonce: nonce_and_chunks[..NONCE_SIZE].to_vec(),
                    chunk_base: (KEY_TAG_LEN + NONCE_SIZE) as u64,
                })
            })
            .await
    }
}

#[async_trait]
impl SyncStorage for CloudSyncStorage {
    async fn append_protocol_object(
        &self,
        semantic_prefix: &str,
        extension: &str,
        data: Vec<u8>,
    ) -> Result<ProtocolObjectLocator, StorageError> {
        if !semantic_prefix.starts_with(crate::sync::store_commit::protocol_prefix())
            || semantic_prefix.contains("/copies/")
            || !matches!(extension, ".json" | ".pkg" | ".db")
        {
            return Err(StorageError::Parse(format!(
                "invalid Store append path {semantic_prefix:?} with extension {extension:?}"
            )));
        }
        let copy_id = self
            .copy_ids
            .as_ref()
            .ok_or_else(|| {
                StorageError::Storage(
                    "Store protocol append has no injected CopyIdGenerator".to_string(),
                )
            })?
            .next_copy_id();
        let logical_key = format!("{semantic_prefix}/copies/{copy_id}{extension}");
        let physical_key = format!("{logical_key}{}", self.suffix());
        let stored = self
            .cipher_for_seal()?
            .seal(data, &self.aad_context(semantic_prefix));
        let physical = self
            .home
            .append_object(
                &physical_key,
                BlobBody::from_bytes(stored),
                &crate::storage::cloud::no_progress(),
            )
            .await?;
        Ok(ProtocolObjectLocator::new(logical_key, physical))
    }

    async fn list_protocol_objects(
        &self,
        prefix: &str,
    ) -> Result<ProtocolObjectListing, StorageError> {
        if !prefix.starts_with(crate::sync::store_commit::protocol_prefix()) {
            return Err(StorageError::Parse(format!(
                "invalid Store listing prefix {prefix:?}"
            )));
        }
        let suffix = self.suffix();
        let listing = self.home.list_appended(prefix).await?;
        let mut objects = Vec::with_capacity(listing.objects.len());
        for physical in listing.objects {
            let logical_key = physical
                .logical_key()
                .strip_suffix(suffix)
                .ok_or_else(|| {
                    StorageError::Parse(format!(
                        "Store append key {:?} lacks expected storage suffix {suffix:?}",
                        physical.logical_key()
                    ))
                })?
                .to_string();
            objects.push(ProtocolObjectLocator::new(logical_key, physical));
        }
        Ok(ProtocolObjectListing {
            objects,
            coverage: listing.coverage,
        })
    }

    async fn read_protocol_object(
        &self,
        object: &ProtocolObjectLocator,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let expected_physical_key = format!("{}{}", object.logical_key(), self.suffix());
        if object.physical().logical_key() != expected_physical_key {
            return Err(StorageError::Parse(format!(
                "Store locator key {:?} does not match physical key {:?}",
                object.logical_key(),
                object.physical().logical_key()
            )));
        }
        let stored = self.home.read_appended(object.physical()).await?;
        self.cipher()
            .open(stored, &self.aad_context(semantic_prefix))
            .map_err(|error| {
                StorageError::Decryption(format!("Store object {}: {error}", object.logical_key()))
            })
    }

    async fn delete_protocol_object(
        &self,
        object: &ProtocolObjectLocator,
    ) -> Result<(), StorageError> {
        self.home.delete_appended(object.physical()).await?;
        Ok(())
    }

    async fn put_blob(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let uploader = self.self_uploader();
        let key = Self::blob_key(self.blob_paths, namespace, Some(&uploader), id, cloud_path)?;
        self.write_blob_sealed(&key, scope, data).await
    }

    async fn put_blob_from_file(
        &self,
        namespace: &str,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_path: &std::path::Path,
    ) -> Result<(), StorageError> {
        let uploader = self.self_uploader();
        let key = Self::blob_key(self.blob_paths, namespace, Some(&uploader), id, cloud_path)?;
        let cipher = self.cipher_for_seal()?;
        let body = cipher
            .open_body(scope, source_path, &self.aad_context(&key))
            .await
            .map_err(StorageError::Storage)?;
        self.home
            .write(&key, body, &crate::storage::cloud::no_progress())
            .await?;
        Ok(())
    }

    async fn get_blob(
        &self,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
    ) -> Result<Vec<u8>, StorageError> {
        let key = Self::blob_key(self.blob_paths, namespace, uploader, id, cloud_path)?;
        self.read_blob_sealed(&key, scope, &format!("blob {namespace}/{id}"))
            .await
    }

    async fn blob_exists(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<bool, StorageError> {
        // Whether an object stands at the key this device would write the blob to, so
        // it keys under itself. A blob's key names the blob under both schemes — the
        // hashed key carries its id, and a browsable home's readable path must name it
        // ([`crate::blob::decl::cloud_path_names_blob`]) — so no two blobs share a key
        // and the object standing at one is that blob's bytes. Presence IS content.
        let uploader = self.self_uploader();
        let key = Self::blob_key(self.blob_paths, namespace, Some(&uploader), id, cloud_path)?;
        self.home.exists(&key).await.map_err(StorageError::from)
    }

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
    ) -> Result<Vec<u8>, StorageError> {
        // Build the ranged reader over a clone of the shared home and the current
        // cipher (a snapshot of the lock — a key rotation between reads builds a
        // fresh reader next call). The reader owns the chunk math and decryption;
        // this just resolves the key/scope/size it needs. The cipher is cloned out
        // of the lock so the reader doesn't hold the guard across its awaits.
        let key = Self::blob_key(self.blob_paths, namespace, uploader, id, cloud_path)?;
        let cipher = self.cipher();
        let aad_context = self.aad_context(&key);
        let reader = BlobRangeReader::new(
            self.home.clone(),
            &cipher,
            scope,
            key,
            source_size,
            aad_context,
        );
        reader.read(offset, len).await
    }

    async fn read_blob_to_file(
        &self,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        scope: crate::blob::BlobScope,
        cloud_path: Option<&str>,
        source_size: u64,
        expected_hash: &str,
        dest: &std::path::Path,
    ) -> Result<(), StorageError> {
        let key = Self::blob_key(self.blob_paths, namespace, uploader, id, cloud_path)?;
        let cipher = self.cipher();
        let aad_context = self.aad_context(&key);
        let reader = BlobRangeReader::new(
            self.home.clone(),
            &cipher,
            scope,
            key.clone(),
            source_size,
            aad_context,
        );
        // Stream the plaintext through a content hasher; the reader refuses to
        // signal "done" (so the writer never commits the file) unless the
        // whole-blob hash matches the row's signed one — a tampered or rolled-back
        // object aborts before the rename and is not cached.
        let mut source = HashVerifyingPlaintextReader {
            reader,
            offset: 0,
            remaining: source_size,
            hasher: crate::blob::ContentHasher::new(),
            expected_hash: expected_hash.to_string(),
            key,
        };
        let written = crate::local_blob::write_stream_atomic(dest, &mut source)
            .await
            .map_err(StorageError::Storage)?;
        if written != source_size {
            return Err(StorageError::Storage(format!(
                "downloaded blob {namespace}/{id} wrote {written} bytes, expected {source_size}"
            )));
        }
        Ok(())
    }

    fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_paths
    }

    fn blob_cloud_key(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, StorageError> {
        Self::blob_key(
            self.blob_paths,
            namespace,
            self.own_uploader().as_deref(),
            id,
            cloud_path,
        )
    }

    fn own_uploader(&self) -> Option<String> {
        match self.blob_paths {
            BlobPathScheme::Hashed => Some(self.self_uploader()),
            BlobPathScheme::Plain => None,
        }
    }

    async fn put_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        crate::store_dir::validate_path_token(owner_pubkey)?;
        let key = format!("keys/{owner_pubkey}/{recipient_pubkey}{}", self.suffix());
        // Wrapped keys are already sealed boxes; store as-is. The suffix is kept
        // uniform with the rest of the layout, but the bytes are never sealed by
        // the home cipher — wrapping a store key is meaningful only for a
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

    async fn get_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<Vec<u8>, StorageError> {
        crate::store_dir::validate_path_token(owner_pubkey)?;
        let key = format!("keys/{owner_pubkey}/{recipient_pubkey}{}", self.suffix());
        // Wrapped keys are already sealed boxes; return as-is.
        self.home.read(&key).await.map_err(StorageError::from)
    }

    async fn delete_wrapped_key(
        &self,
        owner_pubkey: &str,
        recipient_pubkey: &str,
    ) -> Result<(), StorageError> {
        crate::store_dir::validate_path_token(owner_pubkey)?;
        let key = format!("keys/{owner_pubkey}/{recipient_pubkey}{}", self.suffix());
        self.home.delete(&key).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::BlobScope;
    use crate::config::HomeStorage;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{
        BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHomeError,
        SequentialCopyIdGenerator,
    };
    use crate::sync::test_helpers::{open_test_db, TestCustody};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A committed rotation this device has not adopted survives a restart. A
    /// prior run marks it and persists to `protocol_state`; a fresh run restores it
    /// into a new in-memory marker, so sealing stays paused rather than resuming
    /// under the superseded generation a removed member still holds — even though
    /// the fresh marker started empty. Adopting the rotation and re-persisting
    /// clears the durable record so a later restart no longer pauses.
    #[tokio::test]
    async fn persisted_pending_rotation_pauses_sealing_across_restart() {
        let db = open_test_db();

        // Prior run: generation 2 is committed but unadopted; record it durably.
        let marked = PendingRotation::none();
        marked.mark_committed(2);
        persist_pending_rotation(&db, &marked).await.unwrap();

        // Fresh run: a brand-new marker restores the pause from protocol_state.
        let restored = PendingRotation::none();
        restore_pending_rotation(&db, &restored).await.unwrap();
        let live_gen_1 = CloudCipher::Encrypted(EncryptionService::from_key([1u8; 32]));
        assert!(
            matches!(
                restored.check(&live_gen_1),
                Err(RotationPending {
                    committed_generation: 2,
                    live_generation: 1,
                })
            ),
            "a restored pause must still refuse sealing under the superseded generation",
        );

        // Adopt the rotation and re-persist: the durable record clears.
        let adopted = CloudCipher::Encrypted(
            EncryptionService::from_key([1u8; 32])
                .with_appended_generation(2, [2u8; 32])
                .unwrap(),
        );
        restored.resolve(&adopted);
        persist_pending_rotation(&db, &restored).await.unwrap();

        let after_restart = PendingRotation::none();
        restore_pending_rotation(&db, &after_restart).await.unwrap();
        assert!(
            after_restart.check(&live_gen_1).is_ok(),
            "a resolved rotation leaves no durable pause behind",
        );
    }

    #[derive(Clone)]
    struct RecordingCloudHome {
        inner: InMemoryCloudHome,
        full_reads: Arc<AtomicUsize>,
        range_reads: Arc<AtomicUsize>,
    }

    impl RecordingCloudHome {
        fn new(inner: InMemoryCloudHome) -> Self {
            Self {
                inner,
                full_reads: Arc::new(AtomicUsize::new(0)),
                range_reads: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn full_reads(&self) -> usize {
            self.full_reads.load(Ordering::SeqCst)
        }

        fn range_reads(&self) -> usize {
            self.range_reads.load(Ordering::SeqCst)
        }

        fn get(&self, key: &str) -> Option<Vec<u8>> {
            self.inner.get(key)
        }
    }

    #[async_trait]
    impl CloudHome for RecordingCloudHome {
        async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
            self.inner.put_object(key, data).await
        }

        async fn open_multipart<'a>(
            &'a self,
            key: &str,
            total_len: u64,
        ) -> Result<BoxPartSink<'a>, CloudHomeError> {
            self.inner.open_multipart(key, total_len).await
        }

        fn multipart_threshold(&self) -> u64 {
            self.inner.multipart_threshold()
        }

        async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.full_reads.fetch_add(1, Ordering::SeqCst);
            self.inner.read(key).await
        }

        async fn read_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            self.range_reads.fetch_add(1, Ordering::SeqCst);
            self.inner.read_range(key, start, end).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
            self.inner.delete(key).await
        }

        async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
            self.inner.exists(key).await
        }

        async fn set_access(
            &self,
            desired: CloudAccessState,
        ) -> Result<CloudAccessOutcome, CloudHomeError> {
            self.inner.set_access(desired).await
        }
    }

    /// A blob larger than the in-memory backend's multipart threshold, sealed by
    /// the streaming `open_body` path and written through `CloudHome::write`, lands
    /// as a byte-identical `[base_nonce][sealed chunks]` object that the unchanged
    /// decrypt reads back — proving the multipart streaming path produces the same
    /// wire format the whole-buffer encryptor does.
    #[tokio::test]
    async fn streaming_open_body_multipart_round_trips_through_write() {
        let master = EncryptionService::from_key([11u8; 32]);
        let cipher = CloudCipher::Encrypted(master.clone());
        let home = InMemoryCloudHome::new();

        // Larger than the backend's 4 MiB threshold so `write` streams it multipart.
        let plaintext: Vec<u8> = (0..9_000_003u32).map(|i| (i % 251) as u8).collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        std::fs::write(&path, &plaintext).unwrap();

        let body = cipher
            .open_body(
                BlobScope::Master,
                &path,
                &cloud_aad_context("test-lib", "blob-key"),
            )
            .await
            .expect("open streaming body");
        assert!(
            body.len() > home.multipart_threshold(),
            "the body must exceed the threshold so it streams multipart",
        );
        home.write("blob-key", body, &crate::storage::cloud::no_progress())
            .await
            .expect("streaming write");

        // At rest it is the generation-tagged sealed wire format; the cipher
        // reads the tag and recovers the plaintext.
        let stored = home.get("blob-key").expect("blob present");
        assert_eq!(
            cipher
                .open(stored, &cloud_aad_context("test-lib", "blob-key"))
                .expect("decrypt streamed blob"),
            plaintext,
            "the multipart-streamed object decrypts to the original plaintext",
        );
    }

    #[tokio::test]
    async fn cloud_sync_storage_put_blob_from_file_uploads_the_file_body() {
        let home = RecordingCloudHome::new(InMemoryCloudHome::new());
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Hashed,
            "test-lib",
            UserKeypair::generate(),
        );
        let plaintext: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        std::fs::write(&path, &plaintext).unwrap();

        storage
            .put_blob_from_file("audio", "track1", BlobScope::Master, None, &path)
            .await
            .expect("upload file body");

        let key = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "audio",
            Some(&storage.self_uploader()),
            "track1",
            None,
        )
        .expect("blob key");
        assert_eq!(
            home.get(&key).expect("stored blob"),
            plaintext,
            "the file-backed upload stores the file plaintext on a plaintext home",
        );
        assert_eq!(
            home.full_reads(),
            0,
            "uploading a file must not read the cloud object back",
        );
    }

    #[tokio::test]
    async fn cloud_sync_storage_read_blob_to_file_uses_ranges() {
        let home = RecordingCloudHome::new(InMemoryCloudHome::new());
        let master = EncryptionService::from_key([17u8; 32]);
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(master),
            BlobPathScheme::Hashed,
            "test-lib",
            UserKeypair::generate(),
        );
        let plaintext: Vec<u8> = (0..180_000u32).map(|i| (i % 251) as u8).collect();
        storage
            .put_blob(
                "audio",
                "track1",
                BlobScope::Master,
                None,
                plaintext.clone(),
            )
            .await
            .expect("seed encrypted blob");

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("download.bin");
        storage
            .read_blob_to_file(
                "audio",
                Some(&storage.self_uploader()),
                "track1",
                BlobScope::Master,
                None,
                plaintext.len() as u64,
                &crate::blob::content_hash(&plaintext),
                &dest,
            )
            .await
            .expect("download to file");

        assert_eq!(
            std::fs::read(&dest).expect("read downloaded file"),
            plaintext,
            "the file-backed download writes the plaintext to the destination",
        );
        assert_eq!(
            home.full_reads(),
            0,
            "download-to-file must not fetch the whole cloud object",
        );
        assert!(
            home.range_reads() > 1,
            "download-to-file reads the encrypted object through ranges",
        );
    }

    /// `CloudCipher::for_storage` maps a home's storage mode to its at-rest
    /// cipher: an opaque home seals under the supplied store key, a browsable
    /// home is always plaintext (ignoring any key), and an opaque home with no
    /// key — a locked store — has no cipher. This is the same mapping
    /// `SyncManager::start_sync` builds the sync loop with, so reads through a
    /// `BlobRangeReader` and writes through the outbox agree on the protection.
    #[test]
    fn for_storage_maps_mode_to_cipher() {
        let master = EncryptionService::from_key([4u8; 32]);

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
        // An opaque home with no key (a locked store) has no cipher.
        assert!(CloudCipher::for_storage(HomeStorage::Opaque, None).is_none());
    }

    #[tokio::test]
    async fn membership_entry_survives_key_generation_rotation() {
        let home = InMemoryCloudHome::new();
        let owner = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::from_key_at_generation(1, [1u8; 32])),
            BlobPathScheme::Hashed,
            "test-lib",
            owner.clone(),
        )
        .with_copy_ids(Arc::new(
            crate::storage::cloud::SequentialCopyIdGenerator::new("membership-rotation"),
        ));

        let first =
            crate::sync::membership::founder_entry("test-lib", &owner, "0000000000001-0000-owner");
        let owner_pubkey = crate::keys::public_key_hex(&owner);
        let first_coord = first.coord();
        let mut chain = crate::sync::membership::MembershipChain::from_entries(vec![first.clone()])
            .expect("found membership");
        crate::sync::store_objects::append_membership_entry_object(&storage, &first_coord, &first)
            .await
            .expect("write generation one membership");

        let keyring = EncryptionService::from_keyring([(1, [1u8; 32]), (2, [2u8; 32])]).unwrap();
        crate::sync::membership_ops::apply_key_rotation(
            keyring,
            &TestCustody::default(),
            storage.cipher_state(),
            &storage.shared_pending_rotation(),
        )
        .expect("adopt generation two");

        assert_eq!(
            crate::sync::store_objects::load_membership_entry_slot(
                &storage,
                &owner_pubkey,
                &first_coord.author_owner_grant,
                1,
            )
            .await
            .expect("read generation one membership after rotation")
            .expect("generation one membership exists")
            .value,
            first,
        );

        let member = UserKeypair::generate();
        let second = chain
            .signed_set_member(
                &owner,
                crate::keys::public_key_hex(&member),
                None,
                crate::sync::membership::MemberRole::Member,
                "0000000000002-0000-owner".to_string(),
            )
            .expect("owner adds member");
        chain.add_entry(second.clone()).expect("valid member add");
        let second_coord = second.coord();
        crate::sync::store_objects::append_membership_entry_object(
            &storage,
            &second_coord,
            &second,
        )
        .await
        .expect("write generation two membership");
        let semantic_prefix = crate::sync::store_commit::membership_entry_semantic_prefix(
            &owner_pubkey,
            &second_coord.author_owner_grant,
            2,
            second_coord.entry_hash,
        );
        let generation_two_key = home
            .appended_keys()
            .into_iter()
            .find(|key| key.starts_with(&semantic_prefix))
            .expect("generation two object exists");
        let generation_two = home
            .get_appended(&generation_two_key)
            .expect("generation two object exists");
        assert!(
            CloudCipher::Encrypted(EncryptionService::from_key_at_generation(1, [1u8; 32]))
                .open(
                    generation_two,
                    &cloud_aad_context("test-lib", &semantic_prefix),
                )
                .is_err(),
            "a generation one key must not open a generation two object",
        );
    }

    #[tokio::test]
    async fn blob_moved_to_a_different_cloud_key_fails_to_open() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Hashed,
            "test-lib",
            UserKeypair::generate(),
        );

        storage
            .put_blob(
                "images",
                "blob-a",
                BlobScope::Master,
                None,
                b"secret-a".to_vec(),
            )
            .await
            .expect("put blob a");
        storage
            .put_blob(
                "images",
                "blob-b",
                BlobScope::Master,
                None,
                b"secret-b".to_vec(),
            )
            .await
            .expect("put blob b");

        let uploader = storage.self_uploader();
        let key_a = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "images",
            Some(&uploader),
            "blob-a",
            None,
        )
        .expect("blob a key");
        let key_b = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "images",
            Some(&uploader),
            "blob-b",
            None,
        )
        .expect("blob b key");
        let a_bytes = home.get(&key_a).expect("blob a at rest");
        home.write(
            &key_b,
            BlobBody::from_bytes(a_bytes),
            &crate::storage::cloud::no_progress(),
        )
        .await
        .expect("overwrite blob b");

        assert!(
            storage
                .get_blob("images", Some(&uploader), "blob-b", BlobScope::Master, None)
                .await
                .is_err(),
            "a blob substituted at another key must fail authentication",
        );
    }
    /// A plaintext home stores Store protocol objects and blobs in the clear
    /// without adding an `.enc` suffix to their keys.
    #[tokio::test]
    async fn plaintext_home_stores_protocol_objects_and_blobs_without_encryption_suffix() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Hashed,
            "test-lib",
            UserKeypair::generate(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("plaintext-copy")));

        let semantic_prefix = "store-v1/packages/dev1/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let package = b"changeset-plaintext-bytes".to_vec();
        let object = storage
            .append_protocol_object(semantic_prefix, ".pkg", package.clone())
            .await
            .expect("append Store package");
        assert_eq!(
            home.get_appended(object.physical().logical_key())
                .as_deref(),
            Some(package.as_slice()),
        );
        assert!(!object.physical().logical_key().ends_with(".enc"));
        assert_eq!(
            storage
                .read_protocol_object(&object, semantic_prefix)
                .await
                .expect("read Store package"),
            package,
        );

        let blob = b"cover-art-plaintext".to_vec();
        storage
            .put_blob("photos", "p1cover", BlobScope::Master, None, blob.clone())
            .await
            .expect("put blob");
        let uploader = storage.self_uploader();
        let hashed = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "photos",
            Some(&uploader),
            "p1cover",
            None,
        )
        .expect("hashed key");
        assert_eq!(home.get(&hashed).as_deref(), Some(blob.as_slice()));
        assert_eq!(
            storage
                .get_blob(
                    "photos",
                    Some(&uploader),
                    "p1cover",
                    BlobScope::Master,
                    None,
                )
                .await
                .expect("get blob"),
            blob,
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
            CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
            BlobPathScheme::Plain,
            "test-lib",
            UserKeypair::generate(),
        );

        let cloud_path = "Artist - Album/cover-cover-row-id.jpg";
        let bytes = b"cover-art-bytes".to_vec();
        storage
            .put_blob(
                "images",
                "cover-row-id",
                BlobScope::Master,
                Some(cloud_path),
                bytes.clone(),
            )
            .await
            .expect("put_blob plain");

        // It lands at the bare readable key.
        assert!(
            home.get("images/Artist - Album/cover-cover-row-id.jpg")
                .is_some(),
            "blob stored at the readable cloud_path key",
        );
        // The hashed shard key it would otherwise have used does not exist.
        let hashed = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "images",
            Some(&storage.self_uploader()),
            "cover-row-id",
            None,
        )
        .expect("hashed key");
        assert!(
            home.get(&hashed).is_none(),
            "the hashed shard key must be absent under the plain scheme",
        );

        // Round-trips with the same cloud_path (a plain home carries no uploader).
        let got = storage
            .get_blob(
                "images",
                None,
                "cover-row-id",
                BlobScope::Master,
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
            CloudSyncStorage::blob_key(BlobPathScheme::Plain, "images", None, "id-1", None)
                .is_err(),
            "blob_key for a plain home with no cloud_path must error",
        );

        let storage = CloudSyncStorage::new(
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "test-lib",
            UserKeypair::generate(),
        );
        assert!(
            storage
                .put_blob("images", "id-1", BlobScope::Master, None, b"x".to_vec())
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
        let master = EncryptionService::from_key([5u8; 32]);
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(master.clone()),
            BlobPathScheme::Hashed,
            "test-lib",
            UserKeypair::generate(),
        );

        // Larger than two 64 KB chunks so a window can straddle a boundary.
        let plaintext: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
        storage
            .put_blob(
                "audio",
                "track1",
                BlobScope::Master,
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");

        let key = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "audio",
            Some(&storage.self_uploader()),
            "track1",
            None,
        )
        .expect("blob_key");
        let reader = BlobRangeReader::new(
            Arc::new(home.clone()) as Arc<dyn CloudHome>,
            &CloudCipher::Encrypted(master),
            BlobScope::Master,
            key.clone(),
            plaintext.len() as u64,
            cloud_aad_context("test-lib", &key),
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
            "test-lib",
            UserKeypair::generate(),
        );
        let plaintext: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        storage
            .put_blob(
                "audio",
                "track1",
                BlobScope::Master,
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");

        let key = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "audio",
            Some(&storage.self_uploader()),
            "track1",
            None,
        )
        .expect("blob_key");
        let reader = BlobRangeReader::new(
            Arc::new(home.clone()) as Arc<dyn CloudHome>,
            &CloudCipher::Plaintext,
            BlobScope::Master,
            key.clone(),
            plaintext.len() as u64,
            cloud_aad_context("test-lib", &key),
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
        let master = EncryptionService::from_key([7u8; 32]);
        let cipher = CloudCipher::Encrypted(master);
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            cipher.clone(),
            BlobPathScheme::Hashed,
            "test-lib",
            UserKeypair::generate(),
        );

        let scope_id = "release-42".to_string();
        let plaintext: Vec<u8> = (0..80_000u32).map(|i| (i % 251) as u8).collect();
        storage
            .put_blob(
                "audio",
                "track1",
                BlobScope::Derived(scope_id.clone()),
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");

        let key = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "audio",
            Some(&storage.self_uploader()),
            "track1",
            None,
        )
        .expect("blob_key");
        let home_arc = Arc::new(home.clone()) as Arc<dyn CloudHome>;

        let derived = BlobRangeReader::new(
            home_arc.clone(),
            &cipher,
            BlobScope::Derived(scope_id),
            key.clone(),
            plaintext.len() as u64,
            cloud_aad_context("test-lib", &key),
        );
        assert_eq!(
            derived.read(10_000, 20_000).await.expect("derived read"),
            &plaintext[10_000..30_000],
            "the matching derived scope decrypts the range",
        );

        let wrong = BlobRangeReader::new(
            home_arc,
            &cipher,
            BlobScope::Master,
            key.clone(),
            plaintext.len() as u64,
            cloud_aad_context("test-lib", &key),
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
        let master = EncryptionService::from_key([9u8; 32]);
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(master.clone()),
            BlobPathScheme::Hashed,
            "test-lib",
            UserKeypair::generate(),
        );
        let plaintext = b"a short blob".to_vec();
        storage
            .put_blob(
                "audio",
                "track1",
                BlobScope::Master,
                None,
                plaintext.clone(),
            )
            .await
            .expect("put_blob");
        let key = CloudSyncStorage::blob_key(
            BlobPathScheme::Hashed,
            "audio",
            Some(&storage.self_uploader()),
            "track1",
            None,
        )
        .expect("blob_key");
        let reader = BlobRangeReader::new(
            Arc::new(home) as Arc<dyn CloudHome>,
            &CloudCipher::Encrypted(master),
            BlobScope::Master,
            key.clone(),
            plaintext.len() as u64,
            cloud_aad_context("test-lib", &key),
        );
        assert!(
            reader.read(8, 100).await.is_err(),
            "a range past the blob length must error",
        );
        assert!(reader.read(0, 0).await.expect("empty read").is_empty());
    }

    #[tokio::test]
    async fn encrypted_append_retries_keep_distinct_copies_with_one_semantic_aad() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Hashed,
            "append-store",
            UserKeypair::generate(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new("append-copy")));
        let semantic = format!(
            "store-v1/store-protocol-root/{}",
            crate::sync::store_commit::ObjectHash::digest(b"store protocol root")
        );
        let first = storage
            .append_protocol_object(&semantic, ".json", b"same signed bytes".to_vec())
            .await
            .expect("first append");
        let second = storage
            .append_protocol_object(&semantic, ".json", b"same signed bytes".to_vec())
            .await
            .expect("retry append");

        assert_ne!(first.logical_key(), second.logical_key());
        assert_eq!(
            storage
                .read_protocol_object(&first, &semantic)
                .await
                .unwrap(),
            b"same signed bytes"
        );
        assert_eq!(
            storage
                .read_protocol_object(&second, &semantic)
                .await
                .unwrap(),
            b"same signed bytes"
        );
        let listing = storage
            .list_protocol_objects(&semantic)
            .await
            .expect("list both copies");
        assert_eq!(
            listing.coverage,
            crate::storage::cloud::ListingCoverage::CompleteAtScan
        );
        assert_eq!(listing.objects.len(), 2);
    }
}
