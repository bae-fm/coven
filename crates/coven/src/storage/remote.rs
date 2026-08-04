//! `SyncStorage` implementation backed by any `CloudHome`.
//!
//! Handles the cloud home path layout (where keys, heads, images, etc. live)
//! and how objects are protected at rest. The underlying `CloudHome` only deals
//! in raw bytes and flat keys; this layer applies the [`CloudCipher`] — sealing
//! every object under the store key for an encrypted home, or storing it
//! verbatim for a plaintext one — and drives the object-key suffix off the same
//! choice (`.enc` for encrypted data-plane objects, no suffix for signed
//! control-plane and recipient-sealed objects).

use async_trait::async_trait;
use std::path::Path;
use std::sync::{Arc, RwLock};

use super::provider_probe::ProviderProbeStorage;
use super::SyncStorage;
use crate::encryption::{
    EncryptionError, EncryptionService, SealedBlobHeader, SEALED_BLOB_HEADER_LEN,
};
use crate::keys::UserKeypair;
use crate::protocol::objects::ObjectSlot;
#[cfg(test)]
use crate::protocol::objects::ProtocolObjectDomain;
use crate::protocol::objects::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectProtection,
    ResolvedProviderBinding, RotationGate, RotationPending, StorageError,
};
use crate::protocol::store_commit::ObjectHash;
use crate::storage::cloud::{BlobBody, CloudFileReadError, CloudHome, ExactSlotStorage};

/// Every encrypted object carries this cleartext prefix naming the key it was
/// sealed under: magic, then the key's full SHA-256 fingerprint. A read resolves
/// that exact key from the keyring rather than trusting a generation number a
/// fork could reuse.
const KEY_TAG_MAGIC: &[u8; 4] = b"CKF1";
const KEY_FINGERPRINT_LEN: usize = 32;

/// How many bytes of key tag a sealed object carries before its payload — for a
/// blob, before the [`SealedBlobHeader`] that names its chunk size. The public
/// sibling of [`SEALED_BLOB_HEADER_LEN`], so a reader outside this crate can
/// locate a stored blob's header in the object's bytes.
pub(crate) const KEY_TAG_LEN: usize = KEY_TAG_MAGIC.len() + KEY_FINGERPRINT_LEN;

/// How a cloud home protects its objects at rest. An `Encrypted` home seals
/// every object under the store key (the default); a `Plaintext` home stores
/// objects in the clear so the bucket is browsable, and drops the `.enc` suffix.
macro_rules! define_cloud_cipher {
    ($visibility:vis) => {
        #[derive(Clone)]
        $visibility enum CloudCipher {
            Encrypted(EncryptionService),
            Plaintext,
        }
    };
}

#[cfg(any(test, feature = "test-utils"))]
define_cloud_cipher!(pub);
#[cfg(not(any(test, feature = "test-utils")))]
define_cloud_cipher!(pub(crate));

/// A sync session's fixed at-rest representation. The mode is selected once at
/// construction: plaintext has no mutable key state, while encrypted sessions
/// may merge new key generations without ever becoming plaintext.
pub(crate) struct CloudCipherState {
    mode: CloudCipherMode,
}

/// Read-only access to a session cipher snapshot. Production storage implements
/// this with [`CloudCipherState`], whose mode cannot change. The test-utils
/// implementation for a raw lock exists only for injected engine tests.
pub(crate) trait CloudCipherAccess: Send + Sync {
    fn snapshot(&self) -> CloudCipher;
    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError>;

    fn adopt_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<String, crate::keys::KeyError> {
        if let Some(fingerprint) = self.merge_key_rotation(new_encryption, custody)? {
            return Ok(fingerprint);
        }
        let CloudCipher::Encrypted(live) = self.snapshot() else {
            return Err(crate::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            ));
        };
        let retained = live
            .merged_with(new_encryption)
            .map_err(|error| crate::keys::KeyError::Crypto(error.to_string()))?;
        if retained.key_count() != live.key_count() {
            return Err(crate::keys::KeyError::Crypto(
                "live keyring changed without retaining an adopted rotation".to_string(),
            ));
        }
        Ok(live.fingerprint())
    }
}

enum CloudCipherMode {
    Encrypted(RwLock<EncryptionService>),
    Plaintext,
}

impl CloudCipherState {
    pub(crate) fn new(cipher: CloudCipher) -> Self {
        let mode = match cipher {
            CloudCipher::Encrypted(encryption) => {
                CloudCipherMode::Encrypted(RwLock::new(encryption))
            }
            CloudCipher::Plaintext => CloudCipherMode::Plaintext,
        };
        Self { mode }
    }

    pub(crate) fn is_plaintext(&self) -> bool {
        matches!(self.mode, CloudCipherMode::Plaintext)
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
        let merged = live
            .merged_with(new_encryption)
            .map_err(|error| crate::keys::KeyError::Crypto(error.to_string()))?;
        if merged.key_count() == live.key_count() {
            return Ok(None);
        }
        custody.persist(&crate::encryption::MasterKeyring::from(merged.clone()))?;
        *live = merged;
        Ok(Some(live.fingerprint()))
    }

    #[cfg(test)]
    pub(crate) fn encryption(&self) -> Option<EncryptionService> {
        match &self.mode {
            CloudCipherMode::Encrypted(encryption) => Some(encryption.read().unwrap().clone()),
            CloudCipherMode::Plaintext => None,
        }
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

#[cfg(test)]
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
        let merged = live
            .merged_with(new_encryption)
            .map_err(|error| crate::keys::KeyError::Crypto(error.to_string()))?;
        if merged.key_count() == live.key_count() {
            return Ok(None);
        }
        custody.persist(&crate::encryption::MasterKeyring::from(merged.clone()))?;
        *live = merged;
        Ok(Some(live.fingerprint()))
    }
}

pub(crate) struct PendingRotation(std::sync::RwLock<Option<RotationGate>>);

pub(crate) trait CloudRotationAccess: Send + Sync {
    fn mark_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String>;
    fn mark_committed_mutation(&self, generation: u64, mutation: ObjectHash) -> Result<(), String>;
    fn remove_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String>;
    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), String>;
    fn gate(&self) -> Option<RotationGate>;
    fn install_durable_gate(&self, gate: Option<RotationGate>);
    fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending>;
}

impl Default for PendingRotation {
    fn default() -> Self {
        Self(std::sync::RwLock::new(None))
    }
}

impl PendingRotation {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    pub(crate) fn mark_candidate(
        &self,
        generation: u64,
        mutation: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        *recorded = Some(RotationGate::with_candidate(
            recorded.clone(),
            generation,
            mutation,
        )?);
        Ok(())
    }

    pub(crate) fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        *recorded = Some(RotationGate::commit_candidate(
            recorded.clone(),
            generation,
            mutation,
        )?);
        Ok(())
    }

    pub(crate) fn remove_candidate(
        &self,
        generation: u64,
        mutation: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.clone().ok_or_else(|| {
            "rotation candidate gate is absent during proven nonactivation".to_string()
        })?;
        *recorded = gate.remove_candidate(generation, mutation)?;
        Ok(())
    }

    pub(crate) fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: crate::protocol::store_commit::ObjectHash,
        replacement: crate::protocol::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.clone().ok_or_else(|| {
            "rotation candidate gate is absent during candidate replacement".to_string()
        })?;
        *recorded = Some(gate.replace_candidate_mutation(generation, previous, replacement)?);
        Ok(())
    }

    pub(crate) fn gate(&self) -> Option<RotationGate> {
        self.0.read().unwrap().clone()
    }

    pub(crate) fn install_durable_gate(&self, gate: Option<RotationGate>) {
        *self.0.write().unwrap() = gate;
    }

    /// Check `cipher` against the committed generation, if one is pending. A
    /// plaintext home never rotates a store key (sharing, and hence removal,
    /// requires an encrypted home), so it is never blocked.
    pub(crate) fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        let live_generation = match cipher {
            CloudCipher::Encrypted(enc) => enc.current_generation(),
            CloudCipher::Plaintext => return Ok(()),
        };
        if let Some(gate) = self.gate() {
            return Err(RotationPending {
                state: gate.pending_state(),
                live_generation,
            });
        }
        Ok(())
    }

    /// Record that the cloud has committed `generation` and this device has not
    /// folded it into its live cipher. Forward-only: a generation not newer than
    /// one already recorded leaves the recorded value untouched, so an older
    /// rediscovery (e.g. a decoy wrap from a non-rotating owner) can never erase
    /// a genuinely newer generation already known to be pending.
    #[cfg(test)]
    pub(crate) fn mark_committed(&self, generation: u64) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        *recorded = Some(RotationGate::merge_peer_commit(
            recorded.clone(),
            generation,
        )?);
        Ok(())
    }

    /// The recorded committed generation, if any is pending — for status
    /// reporting independent of a specific cipher snapshot.
    #[cfg(test)]
    pub(crate) fn pending_generation(&self) -> Option<u64> {
        self.0
            .read()
            .unwrap()
            .as_ref()
            .map(|gate| gate.generation().get())
    }
}

impl CloudRotationAccess for PendingRotation {
    fn mark_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        PendingRotation::mark_candidate(self, generation, mutation)
    }

    fn mark_committed_mutation(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        PendingRotation::mark_committed_mutation(self, generation, mutation)
    }

    fn remove_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        PendingRotation::remove_candidate(self, generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), String> {
        PendingRotation::replace_candidate_mutation(self, generation, previous, replacement)
    }

    fn gate(&self) -> Option<RotationGate> {
        PendingRotation::gate(self)
    }

    fn install_durable_gate(&self, gate: Option<RotationGate>) {
        PendingRotation::install_durable_gate(self, gate);
    }

    fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        PendingRotation::check(self, cipher)
    }
}

/// How a cloud home names its blob objects. Paired with the at-rest
/// [`CloudCipher`] by the home's [`HomeStorage`](crate::config::HomeStorage): an
/// opaque home is `Hashed` + encrypted, a browsable home is `Plain` + plaintext.
#[derive(Clone, Copy)]
pub(crate) enum BlobPathScheme {
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
    pub(crate) fn for_storage(storage: crate::config::HomeStorage) -> Self {
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
    /// `Plaintext` regardless. A host streaming a Remote blob opens a
    /// [`BlobRangeReader`] under this cipher, so a read applies the same
    /// protection the upload sealed under.
    pub(crate) fn for_storage(
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
    pub(crate) fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        // A control object is always whole-home scoped; only blobs carry a scope.
        // This is exactly the master-scoped blob path: `encryption_for_scope`
        // maps `Master` to the store key itself.
        self.seal_scoped(
            crate::protocol::blob::BlobScope::Master,
            plaintext,
            aad_context,
        )
    }

    /// Recover a control object read from storage. Inverse of [`Self::seal`].
    pub(crate) fn open(
        &self,
        stored: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        self.open_scoped(
            crate::protocol::blob::BlobScope::Master,
            stored,
            aad_context,
        )
    }

    /// Protect a blob under its scope. Encrypted blobs carry the current
    /// store-key generation in cleartext, so a later read knows which
    /// generation to open with.
    pub(crate) fn seal_scoped(
        &self,
        scope: crate::protocol::blob::BlobScope,
        plaintext: Vec<u8>,
        aad_context: &[u8],
    ) -> Vec<u8> {
        match self {
            CloudCipher::Encrypted(master) => {
                ScopedBlobSealing::new(scope, master).seal(plaintext, aad_context)
            }
            CloudCipher::Plaintext => plaintext,
        }
    }

    /// Recover a blob under its resolved scope. Inverse of [`Self::seal_scoped`].
    pub(crate) fn open_scoped(
        &self,
        scope: crate::protocol::blob::BlobScope,
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
    pub(crate) fn suffix(&self) -> &'static str {
        match self {
            CloudCipher::Encrypted(_) => ".enc",
            CloudCipher::Plaintext => "",
        }
    }

    /// Whether this is a plaintext (unencrypted) home.
    pub(crate) fn is_plaintext(&self) -> bool {
        matches!(self, CloudCipher::Plaintext)
    }

    /// The final object length for a blob framed by `header` under this cipher:
    /// the key tag plus the sealed body for an encrypted home, the plaintext
    /// length verbatim for a browsable one. Known before a byte is sealed, so a
    /// streaming upload can declare its length up front.
    pub(crate) fn body_len(&self, header: SealedBlobHeader) -> u64 {
        match self {
            CloudCipher::Encrypted(_) => KEY_TAG_LEN as u64 + header.sealed_len(),
            CloudCipher::Plaintext => header.plaintext_len(),
        }
    }

    /// Open a streaming [`BlobBody`] over the local plaintext file at `file_path`,
    /// sealing each chunk under `scope`'s key for an encrypted home or passing the
    /// plaintext through for a browsable one — without ever reading or sealing the
    /// whole blob into memory. The streaming sibling of [`seal_scoped`](Self::seal_scoped),
    /// used by the upload drain.
    pub(crate) async fn open_body(
        &self,
        scope: crate::protocol::blob::BlobScope,
        file_path: &std::path::Path,
        aad_context: &[u8],
        chunk_size: std::num::NonZeroU32,
    ) -> Result<BlobBody, String> {
        let plaintext_len = crate::local_file::file_len(file_path).await?;
        let header = SealedBlobHeader::new(chunk_size, plaintext_len);
        let reader = crate::storage::local_file::open_reader(file_path).await?;
        Ok(match self {
            CloudCipher::Encrypted(encryption) => {
                ScopedBlobSealing::new(scope, encryption).into_body(header, reader, aad_context)
            }
            CloudCipher::Plaintext => {
                BlobBody::from_file_with_prefix(self.body_len(header), reader, None, Vec::new())
            }
        })
    }
}

/// The two numbers that decide what a blob transfer costs. They are independent
/// on purpose: the chunk is fixed when a blob is sealed and bounds how little a
/// read can fetch, so it sets how long a seek waits for its first byte; the
/// window is a live reader-side choice about how much one request carries, so it
/// sets how many round-trips a long read costs. Neither can be derived from the
/// other, and changing the window never touches a stored blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobChunking {
    chunk: std::num::NonZeroU32,
    window: std::num::NonZeroU64,
}

impl BlobChunking {
    /// 64 KiB chunks read one mebibyte of stored bytes at a time.
    pub const DEFAULT: Self = Self {
        chunk: crate::encryption::DEFAULT_BLOB_CHUNK_SIZE,
        window: match std::num::NonZeroU64::new(1 << 20) {
            Some(window) => window,
            None => unreachable!(),
        },
    };

    pub fn new(chunk: std::num::NonZeroU32, window: std::num::NonZeroU64) -> Self {
        Self { chunk, window }
    }

    pub fn chunk(self) -> std::num::NonZeroU32 {
        self.chunk
    }

    pub fn window(self) -> std::num::NonZeroU64 {
        self.window
    }
}

/// Serves plaintext ranges of one stored blob by fetching only the sealed chunks
/// that cover them. A read costs the chunks it touches and nothing else — never
/// the whole object, however many ranges the stream asks for.
///
/// Opening a sealed blob reads its `[key tag][header]` prefix once, which is
/// what names the key and the chunk size; every later range is arithmetic over
/// that header plus one ranged request per
/// [window](BlobChunking::window)-worth of chunks. A chunk that opens is
/// authentic — its tag covers its bytes, its index, and the header — so there is
/// nothing else to check and no whole-object pass to amortize.
pub(crate) struct BlobRangeReader {
    exact: Arc<dyn ExactSlotStorage>,
    slot: crate::protocol::objects::ObjectSlot,
    opener: crate::encryption::SealedBlobOpener,
    plaintext_size: u64,
    window: std::num::NonZeroU64,
}

impl BlobRangeReader {
    /// The blob's whole plaintext length, as its row declares it.
    pub(crate) fn plaintext_size(&self) -> u64 {
        self.plaintext_size
    }

    /// Read exactly `len` plaintext bytes at `offset`. A range past the blob's
    /// end is an error, never a short read.
    pub(crate) async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, StorageError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            StorageError::Storage(format!("blob range overflow: offset={offset}, len={len}"))
        })?;
        if end > self.plaintext_size {
            return Err(StorageError::Storage(format!(
                "blob range {offset}..{end} exceeds blob size {}",
                self.plaintext_size
            )));
        }
        let header = self.opener.header();
        let chunks = header.covering_chunks(offset, end).map_err(|error| {
            StorageError::InvalidContent(format!("blob range {offset}..{end}: {error}"))
        })?;
        let mut plaintext = Vec::with_capacity(len as usize);
        for run in header.request_runs(chunks, self.window) {
            let span = header.sealed_span(run.clone());
            let sealed = self
                .read_stored(
                    KEY_TAG_LEN as u64 + span.start,
                    KEY_TAG_LEN as u64 + span.end,
                )
                .await?;
            let covered = header.plaintext_span(run.clone());
            let opened = self.opener.open_chunks(run, &sealed).map_err(|error| {
                StorageError::Decryption(format!("blob range {offset}..{end}: {error}"))
            })?;
            let from = (offset.max(covered.start) - covered.start) as usize;
            let to = (end.min(covered.end) - covered.start) as usize;
            plaintext.extend_from_slice(&opened[from..to]);
        }
        Ok(plaintext)
    }

    /// One ranged request against the stored object.
    async fn read_stored(&self, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        let bytes = self
            .exact
            .read_range_at(&self.slot, start, end)
            .await
            .map_err(StorageError::from)?;
        // A provider that ignored the range and answered with more (or less)
        // than was asked for has not served this range; splicing its answer
        // would silently read the wrong bytes.
        if bytes.len() as u64 != end - start {
            return Err(StorageError::InvalidContent(format!(
                "ranged read of {} returned {} bytes for {start}..{end}",
                self.slot.logical_key(),
                bytes.len()
            )));
        }
        Ok(bytes)
    }
}

/// `SyncStorage` that delegates raw I/O to a `CloudHome` and handles the path
/// layout and the at-rest protection (its [`CloudCipher`]).
pub(crate) struct CloudSyncStorage {
    home: Arc<dyn CloudHome>,
    /// `Arc` (not `Box`) because a ranged read hands a clone to the
    /// [`BlobRangeReader`] it opens — the reader holds this client for the life
    /// of a stream and reads across awaits, so it is genuinely shared between
    /// this storage and the readers it opens, not owned by one.
    exact: Arc<dyn ExactSlotStorage>,
    provider_probes: ProviderProbeStorage,
    cipher: Arc<CloudCipherState>,
    /// Whether a committed rotation is outstanding — see [`PendingRotation`].
    /// Shared the same way `cipher` is, so a member removal or a refresh cycle
    /// that discovers a rotation this device can't adopt blocks every seal path,
    /// not just the one that discovered it.
    pending_rotation: Arc<PendingRotation>,
    /// How blob objects are keyed. Unlike the cipher, the scheme does not rotate
    /// over a home's life, so it is a plain field with no lock.
    blob_paths: BlobPathScheme,
    /// How this installation chunks blobs and how wide its range requests are.
    blob_chunking: BlobChunking,
    store_id: String,
    /// The device's signing identity. The control objects this storage writes
    /// (its head, the min_schema floor) are signed with it so a reader can
    /// attribute and verify them; the at-rest cipher proves confidentiality, not
    /// authorship.
    keypair: UserKeypair,
}

impl CloudSyncStorage {
    pub(crate) fn new(
        home: Arc<dyn CloudHome>,
        cipher: CloudCipher,
        blob_paths: BlobPathScheme,
        store_id: impl Into<String>,
        keypair: UserKeypair,
    ) -> Result<Self, crate::storage::cloud::CloudHomeError> {
        let exact = home.clone().exact_slot_storage().ok_or_else(|| {
            crate::storage::cloud::CloudHomeError::Configuration(
                "CloudSyncStorage requires exact-slot storage".to_string(),
            )
        })?;
        let exact_probe_peer = home.clone().exact_slot_storage().ok_or_else(|| {
            crate::storage::cloud::CloudHomeError::Configuration(
                "CloudSyncStorage requires a second exact-slot probe client".to_string(),
            )
        })?;
        let provider_probes = ProviderProbeStorage::new(exact.clone(), exact_probe_peer);
        Ok(CloudSyncStorage {
            home,
            exact,
            provider_probes,
            cipher: Arc::new(CloudCipherState::new(cipher)),
            pending_rotation: Arc::new(PendingRotation::none()),
            blob_paths,
            blob_chunking: BlobChunking::DEFAULT,
            store_id: store_id.into(),
            keypair,
        })
    }

    /// Seal and read blobs with `chunking` instead of [`BlobChunking::DEFAULT`].
    /// The chunk size applies to blobs this storage seals from now on; already
    /// stored blobs keep the size their own headers record, so installations
    /// with different settings read each other's blobs unchanged.
    pub(crate) fn with_blob_chunking(mut self, chunking: BlobChunking) -> Self {
        self.blob_chunking = chunking;
        self
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_paths
    }

    pub(crate) fn store_id(&self) -> &str {
        &self.store_id
    }

    fn validate_blob_locator_home(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
    ) -> Result<(), StorageError> {
        let valid = matches!(
            (locator, self.blob_paths, self.cipher.is_plaintext()),
            (
                crate::protocol::blob::locator::BlobLocator::Opaque { .. },
                BlobPathScheme::Hashed,
                false
            ) | (
                crate::protocol::blob::locator::BlobLocator::Browsable { .. },
                BlobPathScheme::Plain,
                true
            )
        );
        if !valid {
            return Err(StorageError::InvalidContent(
                "blob locator protection does not match the cloud home's fixed storage mode"
                    .to_string(),
            ));
        }
        Ok(())
    }

    async fn validate_blob_append_authority(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<(), StorageError> {
        authority
            .reference
            .verify_registration(authority.registration)
            .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
        if locator.uploader() != authority.reference {
            return Err(StorageError::InvalidContent(format!(
                "blob locator uploader {:?} differs from its exact write authority",
                locator.uploader()
            )));
        }
        if authority.registration.author_pubkey != hex::encode(self.keypair.public_key()) {
            return Err(StorageError::InvalidContent(
                "blob write authority is not this device's identity key".to_string(),
            ));
        }
        let live = self
            .exact
            .provider_binding()
            .await
            .map_err(StorageError::from)?;
        if live.device != authority.registration.provider {
            return Err(StorageError::InvalidContent(
                "blob write authority differs from the authenticated provider principal"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn uses_identity(&self, identity: &UserKeypair) -> bool {
        self.keypair.public_key() == identity.public_key()
    }

    pub(crate) fn is_plaintext(&self) -> bool {
        self.cipher.is_plaintext()
    }

    fn cipher(&self) -> CloudCipher {
        self.cipher.snapshot()
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

    fn protocol_cipher_for_seal(
        &self,
        context: &ProtocolObjectContext,
    ) -> Result<CloudCipher, StorageError> {
        match context.protection() {
            ProtocolObjectProtection::StoreEncrypted => self.cipher_for_seal(),
            ProtocolObjectProtection::SignedPlaintext => Ok(CloudCipher::Plaintext),
            ProtocolObjectProtection::Circle(encryption) => {
                Ok(CloudCipher::Encrypted(encryption.clone()))
            }
            ProtocolObjectProtection::RecipientSealed => Ok(CloudCipher::Plaintext),
        }
    }

    fn protocol_cipher_for_open(&self, context: &ProtocolObjectContext) -> CloudCipher {
        match context.protection() {
            ProtocolObjectProtection::StoreEncrypted => self.cipher(),
            ProtocolObjectProtection::SignedPlaintext => CloudCipher::Plaintext,
            ProtocolObjectProtection::Circle(encryption) => {
                CloudCipher::Encrypted(encryption.clone())
            }
            ProtocolObjectProtection::RecipientSealed => CloudCipher::Plaintext,
        }
    }

    /// The cloud object key for a blob under the home's [`BlobPathScheme`].
    ///
    /// **A cloud object is never rewritten with different bytes, so no two blobs ever
    /// share a key.** `Hashed` gets that from the key itself; `Plain` gets it from the
    /// blob's declared [`BlobReplacement`](crate::protocol::blob::BlobReplacement), which coven
    /// enforces where a blob is derived from its row ([`crate::database::BlobDecls`]) —
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
    pub(crate) fn blob_key(
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

    #[cfg(test)]
    async fn blob_write_registration(
        &self,
        label: &str,
    ) -> crate::protocol::store_commit::ReferencedStoreDeviceRegistration {
        use crate::protocol::store_commit::{
            DeviceStreamAnchor, StoreCreationId, StoreDeviceRegistration,
            StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreRootRef,
        };

        let root_bytes = format!("{label} Store root").into_bytes();
        let root = StoreRootRef {
            store_root_id: ObjectHash::digest(format!("{label} root id").as_bytes()),
            store_root_hash: ObjectHash::digest(&root_bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical(format!("store-v1/store-protocol-root/{label}.json")).unwrap(),
                root_bytes.len() as u64,
                ObjectHash::digest(&root_bytes),
            ),
        };
        let anchor_slot = |stream: &str| {
            ObjectSlot::logical(format!(
                "store-v1/test-device-streams/{label}/{stream}.json"
            ))
            .unwrap()
        };
        let provider = SyncStorage::provider_binding(self).await.unwrap().device;
        let registration = StoreDeviceRegistration::signed(
            root,
            StoreDeviceRegistrationOrigin::Founder {
                creation_id: StoreCreationId::from_nonce(label),
            },
            provider,
            DeviceStreamAnchor::StoreAnnouncements {
                first_slot: anchor_slot("announcements"),
            },
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: anchor_slot("acknowledgements"),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: anchor_slot("snapshots"),
            },
            &self.keypair,
        )
        .unwrap();
        let bytes = registration.to_bytes();
        let reference = StoreDeviceRegistrationRef::from_registration(
            &registration,
            ExactObjectRef::new(
                ObjectSlot::logical(format!(
                    "store-v1/devices/{}/registration.json",
                    registration.device_id
                ))
                .unwrap(),
                bytes.len() as u64,
                ObjectHash::digest(&bytes),
            ),
        );
        crate::protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
            reference,
            registration,
        )
        .expect("construct test blob write registration")
    }

    #[cfg(test)]
    pub(crate) fn current_encryption(&self) -> Option<EncryptionService> {
        self.cipher.encryption()
    }

    #[cfg(test)]
    pub(crate) fn cipher_snapshot(&self) -> CloudCipher {
        self.cipher.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation_for_test(
        &self,
        encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<String, crate::keys::KeyError> {
        CloudCipherAccess::adopt_key_rotation(self, encryption, custody)
    }

    #[cfg(test)]
    pub(crate) fn mark_rotation_committed_for_test(&self, generation: u64) -> Result<(), String> {
        self.pending_rotation.mark_committed(generation)
    }

    #[cfg(test)]
    pub(crate) fn pending_rotation_generation_for_test(&self) -> Option<u64> {
        self.pending_rotation.pending_generation()
    }

    #[cfg(test)]
    pub(crate) fn clear_rotation_gate_for_test(&self) {
        self.pending_rotation.install_durable_gate(None);
    }
}

impl CloudCipherAccess for CloudSyncStorage {
    fn snapshot(&self) -> CloudCipher {
        self.cipher.snapshot()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        self.cipher.merge_key_rotation(new_encryption, custody)
    }
}

impl CloudRotationAccess for CloudSyncStorage {
    fn mark_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        self.pending_rotation.mark_candidate(generation, mutation)
    }

    fn mark_committed_mutation(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        self.pending_rotation
            .mark_committed_mutation(generation, mutation)
    }

    fn remove_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        self.pending_rotation.remove_candidate(generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), String> {
        self.pending_rotation
            .replace_candidate_mutation(generation, previous, replacement)
    }

    fn gate(&self) -> Option<RotationGate> {
        self.pending_rotation.gate()
    }

    fn install_durable_gate(&self, gate: Option<RotationGate>) {
        self.pending_rotation.install_durable_gate(gate);
    }

    fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        self.pending_rotation.check(cipher)
    }
}

/// The `EncryptionService` a blob's `scope` selects, against `master`: the
/// store master itself, or a per-scope key derived from it. The blob storage
/// methods and the outbox drain both turn a [`crate::protocol::blob::BlobScope`] into a
/// key the same way, so they share this one mapping. Only an encrypted home has
/// per-scope keys, so this is reached only from the [`CloudCipher::Encrypted`]
/// branches.
pub(crate) fn encryption_for_scope(
    scope: crate::protocol::blob::BlobScope,
    master: &EncryptionService,
) -> EncryptionService {
    match scope {
        crate::protocol::blob::BlobScope::Master => master.clone(),
        crate::protocol::blob::BlobScope::Derived(s) => master.derive_scoped(&s),
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

fn protocol_object_aad_context(context: &ProtocolObjectContext, semantic_prefix: &str) -> Vec<u8> {
    let domain = context.domain().aad_label();
    let mut aad = Vec::with_capacity(
        context.store_root_hash().as_bytes().len()
            + std::mem::size_of::<u64>() * 2
            + domain.len()
            + semantic_prefix.len(),
    );
    aad.extend_from_slice(context.store_root_hash().as_bytes());
    aad.extend_from_slice(&(domain.len() as u64).to_le_bytes());
    aad.extend_from_slice(domain);
    aad.extend_from_slice(&(semantic_prefix.len() as u64).to_le_bytes());
    aad.extend_from_slice(semantic_prefix.as_bytes());
    aad
}

async fn run_storage_cpu<T>(
    operation: &'static str,
    work: Box<dyn FnOnce() -> Result<T, StorageError> + Send>,
) -> Result<T, StorageError>
where
    T: Send + 'static,
{
    crate::blocking::run(work)
        .await
        .map_err(|error| StorageError::Storage(format!("{operation}: {error}")))?
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
struct ScopedBlobSealing {
    encryption: EncryptionService,
    key_tag: Vec<u8>,
}

impl ScopedBlobSealing {
    fn new(scope: crate::protocol::blob::BlobScope, master: &EncryptionService) -> Self {
        Self {
            encryption: encryption_for_scope(scope, master),
            key_tag: key_tag(&master.seal_fingerprint()),
        }
    }

    fn seal(self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        let mut stored = self.key_tag;
        stored.extend(self.encryption.encrypt(&plaintext, aad_context));
        stored
    }

    fn into_body(
        self,
        header: SealedBlobHeader,
        reader: crate::storage::local_file::PlaintextReader,
        aad_context: &[u8],
    ) -> BlobBody {
        let mut prefix = self.key_tag;
        prefix.extend_from_slice(&header.to_bytes());
        BlobBody::from_file_with_prefix(
            KEY_TAG_LEN as u64 + header.sealed_len(),
            reader,
            Some(self.encryption.blob_sealer(header, aad_context)),
            prefix,
        )
    }
}

fn opening_encryption_for_scope(
    scope: crate::protocol::blob::BlobScope,
    master: &EncryptionService,
    fingerprint: &[u8; KEY_FINGERPRINT_LEN],
) -> Result<EncryptionService, EncryptionError> {
    match scope {
        crate::protocol::blob::BlobScope::Master => master.service_for_fingerprint(fingerprint),
        crate::protocol::blob::BlobScope::Derived(scope_id) => {
            master.derive_scoped_for_fingerprint(fingerprint, &scope_id)
        }
    }
}

fn open_scoped_encrypted(
    scope: crate::protocol::blob::BlobScope,
    master: &EncryptionService,
    stored: &[u8],
    aad_context: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    let (fingerprint, ciphertext) = read_key_tag(stored)?;
    opening_encryption_for_scope(scope, master, &fingerprint)?.decrypt(ciphertext, aad_context)
}

enum ExactBlobOpening {
    Browsable,
    Opaque {
        opener: crate::encryption::SealedBlobOpener,
        next_chunk: u64,
    },
}

/// Opens one already exact-verified stored blob and withholds EOF until the
/// complete plaintext size and hash match the signed locator.
struct ExactBlobPlaintextReader {
    source: crate::storage::local_file::PlaintextReader,
    opening: ExactBlobOpening,
    remaining: u64,
    hasher: Option<crate::protocol::blob::ContentHasher>,
    expected_hash: ObjectHash,
    locator_hash: ObjectHash,
    pending: Vec<u8>,
    pending_offset: usize,
}

impl ExactBlobPlaintextReader {
    async fn new(
        stored_file: &Path,
        store_id: &str,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
    ) -> Result<Self, StorageError> {
        let locator = blob.locator();
        let mut source = crate::storage::local_file::open_reader(stored_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;

        let opening = match (locator, protection) {
            (
                crate::protocol::blob::locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                },
                crate::protocol::objects::BlobSpoolProtection::Opaque(master),
            ) => {
                let prefix = read_source_exact(
                    &mut source,
                    KEY_TAG_LEN + SEALED_BLOB_HEADER_LEN,
                    locator.locator_hash(),
                )
                .await?;
                let opener = verified_sealed_blob_opener(
                    &prefix,
                    blob,
                    key_fingerprint,
                    scope,
                    &master,
                    &cloud_aad_context(store_id, &locator.semantic_key()),
                )?;
                ExactBlobOpening::Opaque {
                    opener,
                    next_chunk: 0,
                }
            }
            (
                crate::protocol::blob::locator::BlobLocator::Browsable { .. },
                crate::protocol::objects::BlobSpoolProtection::Browsable,
            ) => {
                check_stored_blob_length(blob, locator.plaintext_size())?;
                ExactBlobOpening::Browsable
            }
            (crate::protocol::blob::locator::BlobLocator::Opaque { .. }, _) => {
                return Err(StorageError::Configuration(
                    "opaque blob locator requires audience encryption".to_string(),
                ));
            }
            (crate::protocol::blob::locator::BlobLocator::Browsable { .. }, _) => {
                return Err(StorageError::Configuration(
                    "browsable blob locator cannot use audience encryption".to_string(),
                ));
            }
        };

        Ok(Self {
            // A sealed blob is verified by opening it: every chunk's tag covers
            // its bytes, its index, and the header that frames them, so nothing
            // the provider can serve opens as this blob's plaintext. A browsable
            // home stores the plaintext in the clear and has no tags, so there
            // the row's content hash is the only thing that can refuse the
            // provider's bytes — the two homes verify by different means, not by
            // one mechanism plus a spare.
            hasher: match opening {
                ExactBlobOpening::Browsable => {
                    Some(crate::protocol::blob::ContentHasher::default())
                }
                ExactBlobOpening::Opaque { .. } => None,
            },
            source,
            opening,
            remaining: locator.plaintext_size(),
            expected_hash: locator.plaintext_hash(),
            locator_hash: locator.locator_hash(),
            pending: Vec::new(),
            pending_offset: 0,
        })
    }

    fn take_pending(&mut self, max: usize) -> Vec<u8> {
        let end = (self.pending_offset + max).min(self.pending.len());
        let result = self.pending[self.pending_offset..end].to_vec();
        self.pending_offset = end;
        if self.pending_offset == self.pending.len() {
            self.pending.clear();
            self.pending_offset = 0;
        }
        result
    }

    fn verify_complete(&mut self) -> Result<(), crate::storage::local_file::PlaintextChunkError> {
        let Some(hasher) = self.hasher.take() else {
            return Ok(());
        };
        let actual = hasher.finish();
        if actual != self.expected_hash.to_string() {
            return Err(
                crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                    "blob {} plaintext hash mismatch: expected {}, got {actual}",
                    self.locator_hash, self.expected_hash
                )),
            );
        }
        Ok(())
    }
}

/// Split a stored sealed blob into the three things its bytes declare: the key
/// fingerprint naming what sealed it, the header framing its chunks, and the
/// sealed chunks themselves.
///
/// The layout is `[CKF1][fingerprint: 32][version: 1][chunk_size: 4][plaintext_len: 8][chunks…]`.
/// Everything before the chunks is cleartext — a reader must know the key and the
/// chunk size before it can open anything — and all of it is bound into every
/// chunk's AAD, so a rewritten prefix fails the first open rather than re-framing
/// the object.
pub(crate) fn split_sealed_blob(
    stored: &[u8],
) -> Result<(crate::encryption::KeyFingerprint, SealedBlobHeader, &[u8]), EncryptionError> {
    let (fingerprint, rest) = read_key_tag(stored)?;
    let header = SealedBlobHeader::parse(rest)
        .map_err(|error| EncryptionError::Decryption(error.to_string()))?;
    Ok((
        crate::encryption::KeyFingerprint::from_bytes(fingerprint),
        header,
        &rest[SEALED_BLOB_HEADER_LEN..],
    ))
}

/// Resolve a sealed blob's `[key tag][header]` prefix into the key that sealed
/// it and the layout it declares. The fingerprint must be the one the row's
/// locator names — a blob sealed under any other key is not this row's blob,
/// whatever it decrypts to.
fn verified_sealed_blob_opener(
    prefix: &[u8],
    blob: &crate::protocol::blob::locator::StoredBlobRef,
    key_fingerprint: &crate::encryption::KeyFingerprint,
    scope: &crate::protocol::blob::BlobScope,
    master: &EncryptionService,
    aad_context: &[u8],
) -> Result<crate::encryption::SealedBlobOpener, StorageError> {
    let locator = blob.locator();
    let (fingerprint, header, _) = split_sealed_blob(prefix).map_err(|error| {
        StorageError::Decryption(format!("blob {}: {error}", locator.locator_hash()))
    })?;
    if fingerprint != *key_fingerprint {
        return Err(StorageError::InvalidContent(format!(
            "blob {} stored key fingerprint differs from its locator",
            locator.locator_hash()
        )));
    }
    let encryption = opening_encryption_for_scope(scope.clone(), master, fingerprint.as_bytes())
        .map_err(|error| {
            StorageError::Decryption(format!(
                "blob {} audience key: {error}",
                locator.locator_hash()
            ))
        })?;
    if header.plaintext_len() != locator.plaintext_size() {
        return Err(StorageError::InvalidContent(format!(
            "blob {} header declares {} plaintext bytes, its locator declares {}",
            locator.locator_hash(),
            header.plaintext_len(),
            locator.plaintext_size()
        )));
    }
    check_stored_blob_length(blob, KEY_TAG_LEN as u64 + header.sealed_len())?;
    Ok(encryption.blob_opener(header, aad_context))
}

/// Check a stored blob's length against what its own framing implies. The row
/// pins the stored object's exact size, so a length the framing cannot produce
/// means the object is not the one the row names.
fn check_stored_blob_length(
    blob: &crate::protocol::blob::locator::StoredBlobRef,
    expected: u64,
) -> Result<(), StorageError> {
    if blob.object().stored_size() != expected {
        return Err(StorageError::InvalidContent(format!(
            "blob {} stored length is {}, expected {expected} for its locator",
            blob.locator().locator_hash(),
            blob.object().stored_size()
        )));
    }
    Ok(())
}

async fn read_source_exact(
    source: &mut crate::storage::local_file::PlaintextReader,
    len: usize,
    locator_hash: ObjectHash,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        let chunk = source
            .next_chunk(len - bytes.len())
            .await
            .map_err(StorageError::LocalFilesystem)?;
        if chunk.is_empty() {
            return Err(StorageError::InvalidContent(format!(
                "blob {locator_hash} stored body ended after {} of {len} required bytes",
                bytes.len()
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[async_trait]
impl crate::local_file::PlaintextChunkReader for ExactBlobPlaintextReader {
    type Error = crate::storage::local_file::PlaintextChunkError;

    async fn next_chunk(
        &mut self,
        max: usize,
    ) -> Result<Vec<u8>, crate::storage::local_file::PlaintextChunkError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        if !self.pending.is_empty() {
            return Ok(self.take_pending(max));
        }
        if self.remaining == 0 {
            self.verify_complete()?;
            return Ok(Vec::new());
        }

        let plaintext = match &mut self.opening {
            ExactBlobOpening::Browsable => {
                let wanted = usize::try_from(self.remaining.min(max as u64)).map_err(|_| {
                    crate::storage::local_file::PlaintextChunkError::InvalidContent(
                        "blob plaintext read length does not fit this platform".to_string(),
                    )
                })?;
                let chunk = self.source.next_chunk(wanted).await.map_err(|error| {
                    crate::storage::local_file::PlaintextChunkError::Local(error.to_string())
                })?;
                if chunk.is_empty() {
                    return Err(
                        crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                            "blob {} plaintext ended early",
                            self.locator_hash
                        )),
                    );
                }
                chunk
            }
            ExactBlobOpening::Opaque { opener, next_chunk } => {
                let index = *next_chunk;
                let sealed_len =
                    usize::try_from(opener.header().sealed_chunk_len(index)).map_err(|_| {
                        crate::storage::local_file::PlaintextChunkError::InvalidContent(
                            "one sealed blob chunk does not fit this platform".to_string(),
                        )
                    })?;
                let sealed = read_source_exact(&mut self.source, sealed_len, self.locator_hash)
                    .await
                    .map_err(crate::storage::local_file::PlaintextChunkError::Remote)?;
                let plaintext = opener.open_chunk(index, &sealed).map_err(|error| {
                    crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                        "blob {}: {error}",
                        self.locator_hash
                    ))
                })?;
                *next_chunk += 1;
                plaintext
            }
        };
        if plaintext.len() as u64 > self.remaining {
            return Err(
                crate::storage::local_file::PlaintextChunkError::InvalidContent(format!(
                    "blob {} produced excess plaintext",
                    self.locator_hash
                )),
            );
        }
        // Present only for a browsable home, where the content hash is what
        // refuses the provider's bytes; a sealed blob is refused by its tags.
        if let Some(hasher) = self.hasher.as_mut() {
            hasher.update(&plaintext);
        }
        self.remaining -= plaintext.len() as u64;
        self.pending = plaintext;
        Ok(self.take_pending(max))
    }
}

#[async_trait]
impl SyncStorage for CloudSyncStorage {
    fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_path_scheme()
    }

    fn self_uploader(&self) -> String {
        self.self_uploader()
    }

    async fn probe_provider(&self) -> Result<(), StorageError> {
        self.home.probe().await.map_err(Into::into)
    }

    async fn set_member_access(
        &self,
        state: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, StorageError> {
        self.home.set_access(state).await.map_err(Into::into)
    }

    async fn read_blob_tombstone(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        self.home.read(key).await.map_err(Into::into)
    }

    async fn write_blob_tombstone(
        &self,
        key: &str,
        stored_bytes: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.home
            .write(
                key,
                BlobBody::from_bytes(stored_bytes),
                &crate::storage::cloud::no_progress(),
            )
            .await
            .map_err(Into::into)
    }

    async fn list_blob_tombstones(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.home.list(prefix).await.map_err(Into::into)
    }

    async fn blob_tombstone_exists(&self, key: &str) -> Result<bool, StorageError> {
        self.home.exists(key).await.map_err(Into::into)
    }

    async fn delete_blob_tombstone(&self, key: &str) -> Result<(), StorageError> {
        self.home.delete(key).await.map_err(Into::into)
    }

    async fn probe_exact_slots(
        &self,
        journal: &dyn crate::protocol::provider::ProviderProbeJournal,
        probe_id: crate::protocol::provider::ProviderProbeId,
        binding: &ResolvedProviderBinding,
    ) -> Result<
        crate::protocol::provider::ExactSlotProbeReceipt,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.provider_probes
            .probe_exact_slots(journal, probe_id, binding)
            .await
    }

    async fn reserve_cross_principal_response_slot(
        &self,
        probe_id: crate::protocol::provider::ProviderProbeId,
    ) -> Result<ObjectSlot, crate::protocol::provider::ProviderProbeError> {
        self.provider_probes
            .reserve_cross_principal_response_slot(probe_id)
            .await
    }

    async fn observe_exact_slot(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<ExactObjectRef>, StorageError> {
        self.exact.observe_at(slot).await.map_err(Into::into)
    }

    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &ObjectSlot,
    ) -> Result<(), StorageError> {
        self.exact
            .delete_and_verify_absent(slot)
            .await
            .map_err(Into::into)
    }

    async fn prepare_cross_principal_challenge(
        &self,
        publication_journal: &dyn crate::protocol::provider::DeviceJoinChallengePublicationJournal,
        probe_id: crate::protocol::provider::ProviderProbeId,
        store: &crate::protocol::objects::StoreProviderBinding,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeChallenge,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.provider_probes
            .prepare_cross_principal_challenge(
                publication_journal,
                probe_id,
                store,
                context,
                administrator_signer,
            )
            .await
    }

    async fn settle_cross_principal_challenge(
        &self,
        publication_journal: &dyn crate::protocol::provider::DeviceJoinChallengePublicationJournal,
        authorization: &crate::protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalChallengeContext,
        store: &crate::protocol::objects::StoreProviderBinding,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeChallenge,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.provider_probes
            .settle_cross_principal_challenge(
                publication_journal,
                authorization,
                challenge,
                context,
                store,
            )
            .await
    }

    async fn create_cross_principal_response(
        &self,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        context: &crate::protocol::provider::CrossPrincipalResponseContext,
        store: &crate::protocol::objects::StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signer: &UserKeypair,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeResponse,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.provider_probes
            .create_cross_principal_response(
                challenge,
                context,
                store,
                administrator_signing_pubkey,
                peer_signer,
            )
            .await
    }

    async fn complete_cross_principal_probe(
        &self,
        journal: &dyn crate::protocol::provider::ProviderProbeJournal,
        challenge: &crate::protocol::provider::CrossPrincipalProbeChallenge,
        response: &crate::protocol::provider::CrossPrincipalProbeResponse,
        context: &crate::protocol::provider::CrossPrincipalResponseContext,
        store: &crate::protocol::objects::StoreProviderBinding,
        administrator_signer: &dyn crate::keys::DeviceSigningAuthority,
        peer_signing_pubkey: &str,
    ) -> Result<
        crate::protocol::provider::CrossPrincipalProbeReceipt,
        crate::protocol::provider::ProviderProbeError,
    > {
        self.provider_probes
            .complete_cross_principal_probe(
                journal,
                challenge,
                response,
                context,
                store,
                administrator_signer,
                peer_signing_pubkey,
            )
            .await
    }

    fn store_blob_protection(
        &self,
    ) -> Result<crate::protocol::objects::BlobSpoolProtection, StorageError> {
        Ok(match self.cipher_for_seal()? {
            CloudCipher::Encrypted(encryption) => {
                crate::protocol::objects::BlobSpoolProtection::Opaque(encryption)
            }
            CloudCipher::Plaintext => crate::protocol::objects::BlobSpoolProtection::Browsable,
        })
    }

    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, StorageError> {
        self.exact.provider_binding().await.map_err(Into::into)
    }

    async fn allocate_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<ObjectSlot, StorageError> {
        context.validate_path(semantic_prefix)?;
        context.validate_extension(extension)?;
        Ok(self
            .exact
            .allocate_slot(&format!("{semantic_prefix}{extension}"))
            .await?)
    }

    fn prepare_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        slot: ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<PreparedExactObject, StorageError> {
        context.validate_slot(&slot, semantic_prefix)?;
        let aad = protocol_object_aad_context(context, semantic_prefix);
        let stored = self.protocol_cipher_for_seal(context)?.seal(data, &aad);
        let reference = ExactObjectRef::new(
            slot,
            stored.len() as u64,
            crate::protocol::store_commit::ObjectHash::digest(&stored),
        );
        PreparedExactObject::new(reference, stored)
    }

    async fn create_protocol_object(
        &self,
        prepared: &PreparedExactObject,
    ) -> Result<(), StorageError> {
        let create_error = self
            .exact
            .create_at(
                prepared.reference().slot(),
                BlobBody::from_bytes(prepared.stored_bytes().to_vec()),
                &crate::storage::cloud::no_progress(),
            )
            .await
            .err();
        if let Some(error) = &create_error {
            if !matches!(
                error,
                crate::storage::cloud::CloudHomeError::AlreadyExists(_)
            ) && !error.is_retryable()
            {
                return Err(create_error.expect("create error exists").into());
            }
        }
        let observed = match self.exact.read_at(prepared.reference().slot()).await {
            Ok(observed) => observed,
            Err(crate::storage::cloud::CloudHomeError::NotFound(_)) if create_error.is_some() => {
                return Err(create_error.expect("create error exists").into())
            }
            Err(readback) => {
                return match create_error {
                    Some(operation) => Err(StorageError::UnresolvedOutcome {
                        operation: Box::new(operation.into()),
                        readback: Box::new(readback.into()),
                    }),
                    None => Err(readback.into()),
                }
            }
        };
        if observed != prepared.stored_bytes() {
            return Err(StorageError::SlotCollision(
                prepared.reference().slot().logical_key().to_string(),
            ));
        }
        prepared.reference().verify(&observed)?;
        Ok(())
    }

    async fn read_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        context.validate_reference(object, semantic_prefix)?;
        let stored = self.exact.read_at(object.slot()).await?;
        let aad = protocol_object_aad_context(context, semantic_prefix);
        let cipher = self.protocol_cipher_for_open(context);
        let object = object.clone();
        run_storage_cpu(
            "verify and open protocol object",
            Box::new(move || {
                object.verify(&stored)?;
                cipher.open(stored, &aad).map_err(|error| {
                    StorageError::Decryption(format!(
                        "protocol object {}: {error}",
                        object.slot().logical_key()
                    ))
                })
            }),
        )
        .await
    }

    async fn read_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, ExactObjectRef), StorageError> {
        let (opened, prepared) = self
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await?;
        Ok((opened, prepared.reference().clone()))
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, PreparedExactObject), StorageError> {
        context.validate_slot(slot, semantic_prefix)?;
        let stored = self.exact.read_at(slot).await?;
        let aad = protocol_object_aad_context(context, semantic_prefix);
        let cipher = self.protocol_cipher_for_open(context);
        let slot = slot.clone();
        run_storage_cpu(
            "identify and open protocol slot",
            Box::new(move || {
                let object = ExactObjectRef::new(
                    slot.clone(),
                    stored.len() as u64,
                    crate::protocol::store_commit::ObjectHash::digest(&stored),
                );
                let prepared = PreparedExactObject::new(object, stored.clone())?;
                let opened = cipher.open(stored, &aad).map_err(|error| {
                    StorageError::Decryption(format!(
                        "protocol object {}: {error}",
                        slot.logical_key()
                    ))
                })?;
                Ok((opened, prepared))
            }),
        )
        .await
    }

    async fn delete_protocol_object(&self, object: &ExactObjectRef) -> Result<(), StorageError> {
        match self.exact.read_at(object.slot()).await {
            Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
            Ok(stored)
                if stored.len() as u64 != object.stored_size()
                    || crate::protocol::store_commit::ObjectHash::digest(&stored)
                        != object.stored_hash() =>
            {
                return Err(StorageError::SlotCollision(format!(
                    "exact delete target {} contains different bytes",
                    object.slot().logical_key()
                )));
            }
            Ok(_) => {}
        }
        let delete_error = self.exact.delete_at(object.slot()).await.err();
        if delete_error
            .as_ref()
            .is_some_and(|error| !error.is_retryable())
        {
            return Err(delete_error.expect("delete error exists").into());
        }
        match self.exact.read_at(object.slot()).await {
            Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => Ok(()),
            Err(readback) => match delete_error {
                Some(operation) => Err(StorageError::UnresolvedOutcome {
                    operation: Box::new(operation.into()),
                    readback: Box::new(readback.into()),
                }),
                None => Err(readback.into()),
            },
            Ok(_) => match delete_error {
                Some(error) => Err(error.into()),
                None => Err(StorageError::Storage(format!(
                    "exact object remains after delete: {}",
                    object.slot().logical_key()
                ))),
            },
        }
    }

    async fn allocate_blob_slot(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<ObjectSlot, StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        Ok(self.exact.allocate_slot(&locator.semantic_key()).await?)
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        protection: crate::protocol::objects::BlobSpoolProtection,
        plaintext_file: &Path,
        spool_file: &Path,
    ) -> Result<crate::protocol::objects::BlobSpoolWrite, StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        let (plaintext_size, plaintext_hash) =
            crate::storage::local_file::exact_file_facts(plaintext_file)
                .await
                .map_err(StorageError::LocalFilesystem)?;
        if plaintext_size != locator.plaintext_size() || plaintext_hash != locator.plaintext_hash()
        {
            return Err(StorageError::InvalidContent(format!(
                "blob plaintext {}/{} does not match its locator size/hash",
                locator.namespace(),
                locator.blob_id()
            )));
        }

        match tokio::fs::metadata(spool_file).await {
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(StorageError::LocalFilesystem(format!(
                        "blob spool path is not a file: {}",
                        spool_file.display()
                    )));
                }
                let (stored_size, stored_hash) =
                    crate::storage::local_file::exact_file_facts(spool_file)
                        .await
                        .map_err(StorageError::LocalFilesystem)?;
                let object = ExactObjectRef::new(
                    ObjectSlot::logical(locator.semantic_key())?,
                    stored_size,
                    stored_hash,
                );
                let blob =
                    crate::protocol::blob::locator::StoredBlobRef::new(locator.clone(), object)
                        .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                let mut reader =
                    ExactBlobPlaintextReader::new(spool_file, &self.store_id, &blob, protection)
                        .await?;
                loop {
                    let chunk =
                        crate::local_file::PlaintextChunkReader::next_chunk(&mut reader, 1 << 20)
                            .await
                            .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                    if chunk.is_empty() {
                        break;
                    }
                }
                return Ok(crate::protocol::objects::BlobSpoolWrite::Reused);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageError::LocalFilesystem(format!(
                    "inspect blob spool {}: {error}",
                    spool_file.display()
                )));
            }
        }

        let retry_protection = protection.clone();
        let body = match (locator, protection) {
            (
                crate::protocol::blob::locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                },
                crate::protocol::objects::BlobSpoolProtection::Opaque(encryption),
            ) => {
                if encryption.seal_key_fingerprint() != *key_fingerprint {
                    return Err(StorageError::InvalidContent(format!(
                        "blob locator key fingerprint {key_fingerprint} differs from the supplied audience key {}",
                        encryption.seal_key_fingerprint()
                    )));
                }
                let aad = cloud_aad_context(&self.store_id, &locator.semantic_key());
                CloudCipher::Encrypted(encryption)
                    .open_body(
                        scope.clone(),
                        plaintext_file,
                        &aad,
                        self.blob_chunking.chunk(),
                    )
                    .await
                    .map_err(StorageError::LocalFilesystem)?
            }
            (
                crate::protocol::blob::locator::BlobLocator::Browsable { .. },
                crate::protocol::objects::BlobSpoolProtection::Browsable,
            ) => BlobBody::from_file(plaintext_file)
                .await
                .map_err(StorageError::LocalFilesystem)?,
            (crate::protocol::blob::locator::BlobLocator::Opaque { .. }, _) => {
                return Err(StorageError::Configuration(
                    "opaque blob locator requires audience encryption".to_string(),
                ));
            }
            (crate::protocol::blob::locator::BlobLocator::Browsable { .. }, _) => {
                return Err(StorageError::Configuration(
                    "browsable blob locator cannot use audience encryption".to_string(),
                ));
            }
        };
        let expected_size = body.len();
        let stream = futures_util::stream::try_unfold(body, |mut body| async move {
            match body.next_part(1 << 20).await? {
                Some(chunk) => Ok::<_, crate::storage::cloud::CloudHomeError>(Some((chunk, body))),
                None => Ok::<_, crate::storage::cloud::CloudHomeError>(None),
            }
        });
        let staged = crate::local_file::AtomicStagedFile::create(spool_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let (staged, written) =
            staged
                .write_byte_stream(Box::pin(stream))
                .await
                .map_err(|error| match error {
                    crate::local_file::ByteStreamWriteError::Source(error) => error.into(),
                    crate::local_file::ByteStreamWriteError::SourceCleanup { source, cleanup } => {
                        StorageError::CleanupFailed {
                            operation: Box::new(source.into()),
                            cleanup: Box::new(StorageError::LocalFilesystem(cleanup)),
                        }
                    }
                    crate::local_file::ByteStreamWriteError::Local(error) => {
                        StorageError::LocalFilesystem(error)
                    }
                })?;
        if written != expected_size {
            return Err(StorageError::InvalidContent(format!(
                "blob spool {} contains {written} stored bytes, expected {expected_size}",
                spool_file.display()
            )));
        }
        match staged.commit_new().await {
            Ok(()) => Ok(crate::protocol::objects::BlobSpoolWrite::Created),
            Err(crate::local_file::CommitNewFileError::DestinationExists(_)) => {
                let (stored_size, stored_hash) =
                    crate::storage::local_file::exact_file_facts(spool_file)
                        .await
                        .map_err(StorageError::LocalFilesystem)?;
                let object = ExactObjectRef::new(
                    ObjectSlot::logical(locator.semantic_key())?,
                    stored_size,
                    stored_hash,
                );
                let blob =
                    crate::protocol::blob::locator::StoredBlobRef::new(locator.clone(), object)
                        .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                let mut reader = ExactBlobPlaintextReader::new(
                    spool_file,
                    &self.store_id,
                    &blob,
                    retry_protection,
                )
                .await?;
                loop {
                    let chunk =
                        crate::local_file::PlaintextChunkReader::next_chunk(&mut reader, 1 << 20)
                            .await
                            .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                    if chunk.is_empty() {
                        break;
                    }
                }
                Ok(crate::protocol::objects::BlobSpoolWrite::Reused)
            }
            Err(error) => Err(StorageError::LocalFilesystem(error.to_string())),
        }
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        slot: ObjectSlot,
        stored_file: &Path,
    ) -> Result<crate::protocol::blob::locator::StoredBlobRef, StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        let expected = locator.semantic_key();
        if slot.logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob slot {:?} does not match locator key {expected:?}",
                slot.logical_key()
            )));
        }
        let (stored_size, stored_hash) = crate::storage::local_file::exact_file_facts(stored_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        crate::protocol::blob::locator::StoredBlobRef::new(
            locator.clone(),
            ExactObjectRef::new(slot, stored_size, stored_hash),
        )
        .map_err(|error| StorageError::InvalidContent(error.to_string()))
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
        stored_file: &Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), StorageError> {
        let locator = blob.locator();
        let object = blob.object();
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        let expected = locator.semantic_key();
        if object.slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                object.slot().logical_key()
            )));
        }
        {
            let (size, digest) = crate::local_file::file_facts(stored_file)
                .await
                .map_err(|error| StorageError::LocalFilesystem(error.to_string()))?;
            object.verify_stored_facts(
                stored_file,
                size,
                crate::protocol::store_commit::ObjectHash::from_digest(digest),
            )?;
        }
        let body = BlobBody::from_file(stored_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let create_error = self
            .exact
            .create_at(object.slot(), body, progress)
            .await
            .err();
        if let Some(error) = &create_error {
            if !matches!(
                error,
                crate::storage::cloud::CloudHomeError::AlreadyExists(_)
            ) && !error.is_retryable()
            {
                return Err(create_error.expect("create error exists").into());
            }
        }
        match self.exact.read_at(object.slot()).await {
            Ok(stored) => object
                .verify(&stored)
                .map_err(|_| StorageError::SlotCollision(object.slot().logical_key().to_string())),
            Err(crate::storage::cloud::CloudHomeError::NotFound(_)) if create_error.is_some() => {
                Err(create_error.expect("create error exists").into())
            }
            Err(readback) => match create_error {
                Some(operation) => Err(StorageError::UnresolvedOutcome {
                    operation: Box::new(operation.into()),
                    readback: Box::new(readback.into()),
                }),
                None => Err(readback.into()),
            },
        }
    }

    async fn verify_blob_object(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        self.validate_blob_locator_home(blob.locator())?;
        let expected = blob.locator().semantic_key();
        if blob.object().slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                blob.object().slot().logical_key()
            )));
        }
        let stored = self.exact.read_at(blob.object().slot()).await?;
        blob.object().verify(&stored)
    }

    async fn stage_exact_blob_download(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        dest: &Path,
    ) -> Result<crate::local_file::AtomicStagedFile, StorageError> {
        let locator = blob.locator();
        let object = blob.object();
        self.validate_blob_locator_home(locator)?;
        let expected = locator.semantic_key();
        if object.slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                object.slot().logical_key()
            )));
        }
        let mut staged = crate::local_file::AtomicStagedFile::create(dest)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        self.exact
            .read_at_to_file(object.slot(), staged.path_for_atomic_replacement())
            .await
            .map_err(|error| match error {
                CloudFileReadError::Source(error) => StorageError::from(error),
                CloudFileReadError::SourceCleanup { source, cleanup } => {
                    StorageError::CleanupFailed {
                        operation: Box::new(StorageError::from(source)),
                        cleanup: Box::new(StorageError::LocalFilesystem(cleanup)),
                    }
                }
                CloudFileReadError::Local(error) => StorageError::LocalFilesystem(error),
            })?;
        {
            let (size, digest) = crate::local_file::file_facts(staged.path())
                .await
                .map_err(|error| StorageError::LocalFilesystem(error.to_string()))?;
            object.verify_stored_facts(
                staged.path(),
                size,
                crate::protocol::store_commit::ObjectHash::from_digest(digest),
            )?;
        }
        Ok(staged)
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
        dest: &Path,
    ) -> Result<crate::local_file::AtomicStagedFile, StorageError> {
        let stored_destination = dest.with_extension("coven-stored-download");
        let stored = self
            .stage_exact_blob_download(blob, &stored_destination)
            .await?;
        let mut plaintext = crate::local_file::AtomicStagedFile::create(dest)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let mut reader =
            ExactBlobPlaintextReader::new(stored.path(), &self.store_id, blob, protection).await?;
        let written =
            plaintext
                .write_plaintext(&mut reader)
                .await
                .map_err(|error| match error {
                    crate::local_file::StreamWriteError::Source(
                        crate::storage::local_file::PlaintextChunkError::Remote(error),
                    ) => error,
                    crate::local_file::StreamWriteError::Source(
                        crate::storage::local_file::PlaintextChunkError::InvalidContent(error),
                    ) => StorageError::InvalidContent(error),
                    crate::local_file::StreamWriteError::Source(
                        crate::storage::local_file::PlaintextChunkError::Local(error),
                    )
                    | crate::local_file::StreamWriteError::Local(error) => {
                        StorageError::LocalFilesystem(error)
                    }
                })?;
        if written != blob.locator().plaintext_size() {
            return Err(StorageError::InvalidContent(format!(
                "blob {} plaintext stage contains {written} bytes, expected {}",
                blob.locator().locator_hash(),
                blob.locator().plaintext_size()
            )));
        }
        Ok(plaintext)
    }

    async fn open_blob_range_reader(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
        protection: crate::protocol::objects::BlobSpoolProtection,
    ) -> Result<BlobRangeReader, StorageError> {
        let locator = blob.locator();
        self.validate_blob_locator_home(locator)?;
        let slot = blob.object().slot().clone();
        let (scope, key_fingerprint) = match locator {
            crate::protocol::blob::locator::BlobLocator::Opaque {
                scope,
                key_fingerprint,
                ..
            } => (scope, key_fingerprint),
            // A browsable home stores the plaintext in the clear, so its objects
            // carry no tags and a range read has nothing to check the provider's
            // answer against. Ranged reading is refused rather than served
            // unverified; the caller materializes the whole blob, where the row's
            // content hash can refuse it.
            crate::protocol::blob::locator::BlobLocator::Browsable { .. } => {
                return Err(StorageError::Configuration(format!(
                    "blob {} is stored in the clear, which has no per-range verification",
                    locator.locator_hash()
                )));
            }
        };
        let crate::protocol::objects::BlobSpoolProtection::Opaque(master) = protection else {
            return Err(StorageError::Configuration(
                "opaque blob locator requires audience encryption".to_string(),
            ));
        };
        // One ranged read of the prefix names the key and the chunk size; every
        // later range is arithmetic over the header it carries, so this is the
        // only request a range does not pay for.
        let prefix = self
            .exact
            .read_range_at(&slot, 0, (KEY_TAG_LEN + SEALED_BLOB_HEADER_LEN) as u64)
            .await
            .map_err(StorageError::from)?;
        let opener = verified_sealed_blob_opener(
            &prefix,
            blob,
            key_fingerprint,
            scope,
            &master,
            &cloud_aad_context(&self.store_id, &locator.semantic_key()),
        )?;
        Ok(BlobRangeReader {
            exact: self.exact.clone(),
            slot,
            opener,
            plaintext_size: locator.plaintext_size(),
            window: self.blob_chunking.window(),
        })
    }

    async fn delete_blob_object(
        &self,
        blob: &crate::protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        let locator = blob.locator();
        let object = blob.object();
        self.validate_blob_locator_home(locator)?;
        let expected = locator.semantic_key();
        if object.slot().logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "blob object {:?} does not match locator key {expected:?}",
                object.slot().logical_key()
            )));
        }
        self.delete_protocol_object(object).await?;
        Ok(())
    }

    #[cfg(test)]
    async fn list_provider_objects_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, StorageError> {
        self.home.list(prefix).await.map_err(Into::into)
    }

    #[cfg(test)]
    async fn read_provider_object_for_test(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        self.home.read(key).await.map_err(Into::into)
    }

    #[cfg(test)]
    async fn provider_object_exists_for_test(&self, key: &str) -> Result<bool, StorageError> {
        self.home.exists(key).await.map_err(Into::into)
    }
}

#[cfg(test)]
#[path = "remote_tests.rs"]
mod tests;
