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
use tokio::sync::OnceCell;

use super::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectProtection,
    ResolvedProviderBinding, StorageError, SyncStorage,
};
use crate::encryption::{chunked_encrypted_len, EncryptionError, EncryptionService};
use crate::keys::UserKeypair;
use crate::storage::cloud::{
    BlobBody, CloudFileReadError, CloudHome, ExactSlotStorage, ObjectSlot,
};
#[cfg(test)]
use crate::sync::storage::ProtocolObjectDomain;
use crate::sync::store_commit::ObjectHash;

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

/// Store-key work is in flight or committed but not fully adopted. Every cloud
/// seal refuses while this holds, including while a local removal candidate may
/// still publish and after a committed rotation whose key is not locally
/// adopted or whose exact operation journal remains open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "store-key rotation is pending ({state:?}) while this device is sealing under generation \
     {live_generation}; refusing to seal for the cloud until the pending state is completed"
)]
pub struct RotationPending {
    pub state: RotationPendingState,
    pub live_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationPendingState {
    Candidate {
        generation: u64,
    },
    LocalCommitted {
        generation: u64,
    },
    PeerCommitted {
        generation: u64,
    },
    CandidateAndPeer {
        candidate_generation: u64,
        peer_generation: u64,
    },
    LocalCommittedAndPeer {
        local_generation: u64,
        peer_generation: u64,
    },
}

/// The exact store-key work that blocks sealing: a local candidate, an activated
/// local removal awaiting adoption, a peer's committed generation awaiting
/// adoption, or a local fact together with a peer fact. Durable database
/// transitions and this in-memory copy move together at operation boundaries.
///
/// Shared (behind one `Arc`, via [`CloudSyncStorage::shared_pending_rotation`])
/// across every path that seals data for the cloud — changesets, heads, blobs,
/// tombstones, snapshots — so a rotation this device can't adopt blocks all of
/// them the same way, not just the removal call that discovered it. This is the
/// structural half of the invariant: this device must never seal under a
/// generation the store has already superseded.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RotationGate {
    candidate: Option<RotationCandidateGate>,
    local_committed: Option<RotationLocalCommittedGate>,
    peer_committed_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RotationCandidateGate {
    generation: u64,
    mutation: crate::sync::store_commit::ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RotationLocalCommittedGate {
    generation: u64,
    mutation: crate::sync::store_commit::ObjectHash,
}

impl RotationGate {
    pub(crate) fn empty() -> Self {
        Self {
            candidate: None,
            local_committed: None,
            peer_committed_generation: None,
        }
    }

    pub(crate) fn generation(&self) -> Option<u64> {
        self.candidate
            .as_ref()
            .map(|gate| gate.generation)
            .into_iter()
            .chain(self.local_committed.as_ref().map(|gate| gate.generation))
            .chain(self.peer_committed_generation)
            .max()
    }

    fn pending_state(&self) -> Result<RotationPendingState, String> {
        self.validate()?;
        match (
            self.candidate.as_ref(),
            self.local_committed.as_ref(),
            self.peer_committed_generation,
        ) {
            (Some(candidate), None, None) => Ok(RotationPendingState::Candidate {
                generation: candidate.generation,
            }),
            (None, Some(local), None) => Ok(RotationPendingState::LocalCommitted {
                generation: local.generation,
            }),
            (None, None, Some(generation)) => {
                Ok(RotationPendingState::PeerCommitted { generation })
            }
            (Some(candidate), None, Some(peer_generation)) => {
                Ok(RotationPendingState::CandidateAndPeer {
                    candidate_generation: candidate.generation,
                    peer_generation,
                })
            }
            (None, Some(local), Some(peer_generation)) => {
                Ok(RotationPendingState::LocalCommittedAndPeer {
                    local_generation: local.generation,
                    peer_generation,
                })
            }
            _ => Err("rotation gate has an impossible combination of states".to_string()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.generation().is_none()
            || self
                .candidate
                .as_ref()
                .is_some_and(|gate| gate.generation == 0)
            || self
                .local_committed
                .as_ref()
                .is_some_and(|gate| gate.generation == 0)
            || self.peer_committed_generation == Some(0)
            || (self.candidate.is_some() && self.local_committed.is_some())
        {
            return Err("rotation gate is empty or names generation zero".to_string());
        }
        Ok(())
    }

    pub(crate) fn with_candidate(
        mut self,
        generation: u64,
        mutation: crate::sync::store_commit::ObjectHash,
    ) -> Result<Self, String> {
        let candidate = RotationCandidateGate {
            generation,
            mutation,
        };
        if generation == 0 {
            return Err("rotation candidate names generation zero".to_string());
        }
        if self.local_committed.is_some() {
            return Err("a committed local rotation already owns the gate".to_string());
        }
        match &self.candidate {
            Some(existing) if existing != &candidate => {
                return Err("another rotation candidate already owns the gate".to_string())
            }
            Some(_) => {}
            None => self.candidate = Some(candidate),
        }
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn commit_candidate(
        mut self,
        generation: u64,
        mutation: crate::sync::store_commit::ObjectHash,
    ) -> Result<Self, String> {
        if self.candidate.is_none() {
            match &self.local_committed {
                Some(committed)
                    if committed.generation == generation && committed.mutation == mutation =>
                {
                    return Ok(self);
                }
                _ => {}
            }
        }
        if self.candidate
            != Some(RotationCandidateGate {
                generation,
                mutation,
            })
        {
            return Err("rotation commit does not own the pending candidate gate".to_string());
        }
        self.candidate = None;
        let committed = RotationLocalCommittedGate {
            generation,
            mutation,
        };
        self.local_committed = Some(committed);
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn merge_peer_commit(mut self, generation: u64) -> Result<Self, String> {
        if generation == 0 {
            return Err("committed rotation names generation zero".to_string());
        }
        if self
            .peer_committed_generation
            .is_none_or(|existing| generation > existing)
        {
            self.peer_committed_generation = Some(generation);
        }
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn remove_candidate(
        mut self,
        generation: u64,
        mutation: crate::sync::store_commit::ObjectHash,
    ) -> Result<Option<Self>, String> {
        if self.candidate
            != Some(RotationCandidateGate {
                generation,
                mutation,
            })
        {
            return Err("rotation loss does not own the pending candidate gate".to_string());
        }
        self.candidate = None;
        if self.local_committed.is_none() && self.peer_committed_generation.is_none() {
            return Ok(None);
        }
        self.validate()?;
        Ok(Some(self))
    }

    pub(crate) fn replace_candidate_mutation(
        mut self,
        generation: u64,
        previous: crate::sync::store_commit::ObjectHash,
        replacement: crate::sync::store_commit::ObjectHash,
    ) -> Result<Self, String> {
        if self.candidate
            != Some(RotationCandidateGate {
                generation,
                mutation: previous,
            })
        {
            return Err("rotation candidate replacement lost its exact owner".to_string());
        }
        self.candidate = Some(RotationCandidateGate {
            generation,
            mutation: replacement,
        });
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn complete_local_adoption(
        mut self,
        generation: u64,
        mutation: crate::sync::store_commit::ObjectHash,
    ) -> Result<Option<Self>, String> {
        if self.candidate.is_some() {
            return Err("rotation adoption cannot close while a candidate is pending".to_string());
        }
        match &self.local_committed {
            Some(committed)
                if committed.generation == generation && committed.mutation == mutation =>
            {
                self.local_committed = None;
            }
            _ => return Err("rotation adoption does not own the committed gate".to_string()),
        }
        if self
            .peer_committed_generation
            .is_some_and(|peer| peer <= generation)
        {
            self.peer_committed_generation = None;
        }
        if self.peer_committed_generation.is_none() {
            return Ok(None);
        }
        self.validate()?;
        Ok(Some(self))
    }

    pub(crate) fn complete_peer_adoption(
        mut self,
        adopted_generation: u64,
    ) -> Result<Option<Self>, String> {
        if adopted_generation == 0 {
            return Err("adopted rotation names generation zero".to_string());
        }
        if self
            .peer_committed_generation
            .is_some_and(|generation| generation <= adopted_generation)
        {
            self.peer_committed_generation = None;
        }
        if self.candidate.is_none()
            && self.local_committed.is_none()
            && self.peer_committed_generation.is_none()
        {
            return Ok(None);
        }
        self.validate()?;
        Ok(Some(self))
    }
}

pub struct PendingRotation(std::sync::RwLock<Option<RotationGate>>);

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
    #[cfg(any(test, feature = "test-utils"))]
    pub fn mark_committed(&self, generation: u64) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.take().unwrap_or_else(RotationGate::empty);
        match gate.clone().merge_peer_commit(generation) {
            Ok(next) => {
                *recorded = Some(next);
                Ok(())
            }
            Err(error) => {
                *recorded = Some(gate);
                Err(error)
            }
        }
    }

    pub(crate) fn mark_candidate(
        &self,
        generation: u64,
        mutation: crate::sync::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.take().unwrap_or_else(RotationGate::empty);
        match gate.clone().with_candidate(generation, mutation) {
            Ok(next) => {
                *recorded = Some(next);
                Ok(())
            }
            Err(error) => {
                *recorded = Some(gate);
                Err(error)
            }
        }
    }

    pub(crate) fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: crate::sync::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.take().unwrap_or_else(RotationGate::empty);
        match gate.clone().commit_candidate(generation, mutation) {
            Ok(next) => {
                *recorded = Some(next);
                Ok(())
            }
            Err(error) => {
                *recorded = Some(gate);
                Err(error)
            }
        }
    }

    pub(crate) fn remove_candidate(
        &self,
        generation: u64,
        mutation: crate::sync::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.take().ok_or_else(|| {
            "rotation candidate gate is absent during proven nonactivation".to_string()
        })?;
        match gate.clone().remove_candidate(generation, mutation) {
            Ok(next) => {
                *recorded = next;
                Ok(())
            }
            Err(error) => {
                *recorded = Some(gate);
                Err(error)
            }
        }
    }

    pub(crate) fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: crate::sync::store_commit::ObjectHash,
        replacement: crate::sync::store_commit::ObjectHash,
    ) -> Result<(), String> {
        let mut recorded = self.0.write().unwrap();
        let gate = recorded.take().ok_or_else(|| {
            "rotation candidate gate is absent during candidate replacement".to_string()
        })?;
        match gate
            .clone()
            .replace_candidate_mutation(generation, previous, replacement)
        {
            Ok(next) => {
                *recorded = Some(next);
                Ok(())
            }
            Err(error) => {
                *recorded = Some(gate);
                Err(error)
            }
        }
    }

    /// The recorded committed generation, if any is pending — for status
    /// reporting independent of a specific cipher snapshot.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn pending_generation(&self) -> Option<u64> {
        self.0
            .read()
            .unwrap()
            .as_ref()
            .and_then(RotationGate::generation)
    }

    pub(crate) fn gate(&self) -> Option<RotationGate> {
        self.0.read().unwrap().clone()
    }

    pub(crate) fn install_durable_gate(&self, gate: Option<RotationGate>) -> Result<(), String> {
        if let Some(gate) = &gate {
            gate.validate()?;
        }
        *self.0.write().unwrap() = gate;
        Ok(())
    }

    /// Check `cipher` against the committed generation, if one is pending. A
    /// plaintext home never rotates a store key (sharing, and hence removal,
    /// requires an encrypted home), so it is never blocked.
    pub fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        let live_generation = match cipher {
            CloudCipher::Encrypted(enc) => enc.current_generation(),
            CloudCipher::Plaintext => return Ok(()),
        };
        if let Some(gate) = self.gate() {
            let state = gate
                .pending_state()
                .expect("in-memory rotation gate must be validated before installation");
            return Err(RotationPending {
                state,
                live_generation,
            });
        }
        Ok(())
    }
}

/// The `protocol_state` key for the serialized [`RotationGate`]. Restored before
/// the first cycle so a restart cannot forget an unfinished candidate or an
/// unadopted committed rotation and resume sealing under an unauthorized key.
pub const ROTATION_GATE_STATE_KEY: &str = "rotation_gate";

/// Restore the in-memory [`PendingRotation`] from its durable `protocol_state`
/// record, if one is set. Called at open, before the first cycle seals anything.
pub async fn restore_pending_rotation(
    db: &crate::database::Database,
    pending_rotation: &PendingRotation,
) -> Result<(), crate::database::DbError> {
    if let Some(value) = db.get_protocol_state(ROTATION_GATE_STATE_KEY).await? {
        let gate: RotationGate = serde_json::from_str(&value).map_err(|error| {
            crate::database::DbError::Message(format!(
                "persisted rotation gate is invalid: {error}"
            ))
        })?;
        pending_rotation
            .install_durable_gate(Some(gate))
            .map_err(crate::database::DbError::Message)?;
    }
    Ok(())
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
    exact: Arc<dyn ExactSlotStorage>,
    exact_probe_peer: Arc<dyn ExactSlotStorage>,
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
        Ok(CloudSyncStorage {
            home,
            exact,
            exact_probe_peer,
            cipher: Arc::new(CloudCipherState::new(cipher)),
            pending_rotation: Arc::new(PendingRotation::none()),
            blob_paths,
            store_id: store_id.into(),
            keypair,
        })
    }

    pub(crate) fn exact_slot_probe_clients(
        &self,
    ) -> (&dyn ExactSlotStorage, &dyn ExactSlotStorage) {
        (self.exact.as_ref(), self.exact_probe_peer.as_ref())
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_paths
    }

    pub(crate) fn store_id(&self) -> &str {
        &self.store_id
    }

    fn validate_blob_locator_home(
        &self,
        locator: &crate::blob::locator::BlobLocator,
    ) -> Result<(), StorageError> {
        let valid = matches!(
            (locator, self.blob_paths, self.cipher.is_plaintext()),
            (
                crate::blob::locator::BlobLocator::Opaque { .. },
                BlobPathScheme::Hashed,
                false
            ) | (
                crate::blob::locator::BlobLocator::Browsable { .. },
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
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
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
    super::blocking::run(work)
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

enum ExactBlobOpening {
    Browsable,
    Opaque {
        encryption: EncryptionService,
        nonce: Vec<u8>,
        next_chunk: u64,
        aad_context: Vec<u8>,
    },
}

/// Opens one already exact-verified stored blob and withholds EOF until the
/// complete plaintext size and hash match the signed locator.
struct ExactBlobPlaintextReader {
    source: crate::local_blob::PlaintextReader,
    opening: ExactBlobOpening,
    remaining: u64,
    total_size: u64,
    hasher: Option<crate::blob::ContentHasher>,
    expected_hash: ObjectHash,
    locator_hash: ObjectHash,
    pending: Vec<u8>,
    pending_offset: usize,
}

impl ExactBlobPlaintextReader {
    async fn new(
        stored_file: &Path,
        store_id: &str,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: crate::sync::storage::BlobSpoolProtection,
    ) -> Result<Self, StorageError> {
        let locator = blob.locator();
        let mut source = crate::local_blob::open_reader(stored_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let expected_stored_size = match locator {
            crate::blob::locator::BlobLocator::Opaque { .. } => {
                KEY_TAG_LEN as u64 + chunked_encrypted_len(locator.plaintext_size())
            }
            crate::blob::locator::BlobLocator::Browsable { .. } => locator.plaintext_size(),
        };
        if blob.object().stored_size() != expected_stored_size {
            return Err(StorageError::InvalidContent(format!(
                "blob {} stored length is {}, expected {expected_stored_size} for its locator",
                locator.locator_hash(),
                blob.object().stored_size()
            )));
        }

        let opening = match (locator, protection) {
            (
                crate::blob::locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                },
                crate::sync::storage::BlobSpoolProtection::Opaque(master),
            ) => {
                let header = read_source_exact(
                    &mut source,
                    KEY_TAG_LEN + crate::encryption::NONCE_SIZE,
                    locator.locator_hash(),
                )
                .await?;
                let (fingerprint, nonce_and_chunks) = read_key_tag(&header).map_err(|error| {
                    StorageError::Decryption(format!(
                        "blob {} key tag: {error}",
                        locator.locator_hash()
                    ))
                })?;
                if crate::encryption::KeyFingerprint::from_bytes(fingerprint) != *key_fingerprint {
                    return Err(StorageError::InvalidContent(format!(
                        "blob {} stored key fingerprint differs from its locator",
                        locator.locator_hash()
                    )));
                }
                let encryption = opening_encryption_for_scope(scope.clone(), &master, &fingerprint)
                    .map_err(|error| {
                        StorageError::Decryption(format!(
                            "blob {} audience key: {error}",
                            locator.locator_hash()
                        ))
                    })?;
                ExactBlobOpening::Opaque {
                    encryption,
                    nonce: nonce_and_chunks.to_vec(),
                    next_chunk: 0,
                    aad_context: cloud_aad_context(store_id, &locator.semantic_key()),
                }
            }
            (
                crate::blob::locator::BlobLocator::Browsable { .. },
                crate::sync::storage::BlobSpoolProtection::Browsable,
            ) => ExactBlobOpening::Browsable,
            (crate::blob::locator::BlobLocator::Opaque { .. }, _) => {
                return Err(StorageError::Configuration(
                    "opaque blob locator requires audience encryption".to_string(),
                ));
            }
            (crate::blob::locator::BlobLocator::Browsable { .. }, _) => {
                return Err(StorageError::Configuration(
                    "browsable blob locator cannot use audience encryption".to_string(),
                ));
            }
        };

        Ok(Self {
            source,
            opening,
            remaining: locator.plaintext_size(),
            total_size: locator.plaintext_size(),
            hasher: Some(crate::blob::ContentHasher::default()),
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

    fn verify_complete(&mut self) -> Result<(), crate::local_blob::PlaintextChunkError> {
        let Some(hasher) = self.hasher.take() else {
            return Ok(());
        };
        let actual = hasher.finish();
        if actual != self.expected_hash.to_string() {
            return Err(crate::local_blob::PlaintextChunkError::InvalidContent(
                format!(
                    "blob {} plaintext hash mismatch: expected {}, got {actual}",
                    self.locator_hash, self.expected_hash
                ),
            ));
        }
        Ok(())
    }
}

async fn read_source_exact(
    source: &mut crate::local_blob::PlaintextReader,
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
impl crate::local_blob::PlaintextChunkReader for ExactBlobPlaintextReader {
    async fn next_chunk(
        &mut self,
        max: usize,
    ) -> Result<Vec<u8>, crate::local_blob::PlaintextChunkError> {
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
                    crate::local_blob::PlaintextChunkError::InvalidContent(
                        "blob plaintext read length does not fit this platform".to_string(),
                    )
                })?;
                let chunk = self.source.next_chunk(wanted).await.map_err(|error| {
                    crate::local_blob::PlaintextChunkError::Local(error.to_string())
                })?;
                if chunk.is_empty() {
                    return Err(crate::local_blob::PlaintextChunkError::InvalidContent(
                        format!("blob {} plaintext ended early", self.locator_hash),
                    ));
                }
                chunk
            }
            ExactBlobOpening::Opaque {
                encryption,
                nonce,
                next_chunk,
                aad_context,
            } => {
                let plaintext_len = self.remaining.min(crate::encryption::CHUNK_SIZE as u64);
                let encrypted_len = usize::try_from(plaintext_len)
                    .expect("one encryption chunk fits usize")
                    + crate::encryption::TAG_SIZE;
                let encrypted =
                    read_source_exact(&mut self.source, encrypted_len, self.locator_hash)
                        .await
                        .map_err(crate::local_blob::PlaintextChunkError::Remote)?;
                let start = *next_chunk * crate::encryption::CHUNK_SIZE as u64;
                let end = start + plaintext_len;
                let plaintext = encryption
                    .decrypt_range_with_offset(
                        nonce,
                        &encrypted,
                        *next_chunk,
                        start,
                        end,
                        self.total_size,
                        aad_context,
                    )
                    .map_err(|error| {
                        crate::local_blob::PlaintextChunkError::InvalidContent(format!(
                            "blob {} chunk {}: {error}",
                            self.locator_hash, *next_chunk
                        ))
                    })?;
                *next_chunk += 1;
                plaintext
            }
        };
        if plaintext.len() as u64 > self.remaining {
            return Err(crate::local_blob::PlaintextChunkError::InvalidContent(
                format!("blob {} produced excess plaintext", self.locator_hash),
            ));
        }
        self.hasher
            .as_mut()
            .expect("hash verification remains active until EOF")
            .update(&plaintext);
        self.remaining -= plaintext.len() as u64;
        self.pending = plaintext;
        Ok(self.take_pending(max))
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
    fn store_blob_protection(
        &self,
    ) -> Result<crate::sync::storage::BlobSpoolProtection, StorageError> {
        Ok(match self.cipher_for_seal()? {
            CloudCipher::Encrypted(encryption) => {
                crate::sync::storage::BlobSpoolProtection::Opaque(encryption)
            }
            CloudCipher::Plaintext => crate::sync::storage::BlobSpoolProtection::Browsable,
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
            crate::sync::store_commit::ObjectHash::digest(&stored),
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
                    crate::sync::store_commit::ObjectHash::digest(&stored),
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
                    || crate::sync::store_commit::ObjectHash::digest(&stored)
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
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
    ) -> Result<ObjectSlot, StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        Ok(self.exact.allocate_slot(&locator.semantic_key()).await?)
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        protection: crate::sync::storage::BlobSpoolProtection,
        plaintext_file: &Path,
        spool_file: &Path,
    ) -> Result<(), StorageError> {
        self.validate_blob_locator_home(locator)?;
        self.validate_blob_append_authority(locator, authority)
            .await?;
        let (plaintext_size, plaintext_hash) = crate::local_blob::exact_file_facts(plaintext_file)
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
                let (stored_size, stored_hash) = crate::local_blob::exact_file_facts(spool_file)
                    .await
                    .map_err(StorageError::LocalFilesystem)?;
                let object = ExactObjectRef::new(
                    ObjectSlot::logical(locator.semantic_key())?,
                    stored_size,
                    stored_hash,
                );
                let blob = crate::blob::locator::StoredBlobRef::new(locator.clone(), object)
                    .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                let mut reader =
                    ExactBlobPlaintextReader::new(spool_file, &self.store_id, &blob, protection)
                        .await?;
                loop {
                    let chunk =
                        crate::local_blob::PlaintextChunkReader::next_chunk(&mut reader, 1 << 20)
                            .await
                            .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                    if chunk.is_empty() {
                        break;
                    }
                }
                return Ok(());
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
                crate::blob::locator::BlobLocator::Opaque {
                    scope,
                    key_fingerprint,
                    ..
                },
                crate::sync::storage::BlobSpoolProtection::Opaque(encryption),
            ) => {
                if encryption.seal_key_fingerprint() != *key_fingerprint {
                    return Err(StorageError::InvalidContent(format!(
                        "blob locator key fingerprint {key_fingerprint} differs from the supplied audience key {}",
                        encryption.seal_key_fingerprint()
                    )));
                }
                let aad = cloud_aad_context(&self.store_id, &locator.semantic_key());
                CloudCipher::Encrypted(encryption)
                    .open_body(scope.clone(), plaintext_file, &aad)
                    .await
                    .map_err(StorageError::LocalFilesystem)?
            }
            (
                crate::blob::locator::BlobLocator::Browsable { .. },
                crate::sync::storage::BlobSpoolProtection::Browsable,
            ) => BlobBody::from_file(plaintext_file)
                .await
                .map_err(StorageError::LocalFilesystem)?,
            (crate::blob::locator::BlobLocator::Opaque { .. }, _) => {
                return Err(StorageError::Configuration(
                    "opaque blob locator requires audience encryption".to_string(),
                ));
            }
            (crate::blob::locator::BlobLocator::Browsable { .. }, _) => {
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
        let staged = crate::local_blob::stage_atomic_destination(spool_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let written = crate::local_blob::write_byte_stream_atomic(staged.path(), Box::pin(stream))
            .await
            .map_err(|error| match error {
                crate::local_blob::ByteStreamWriteError::Source(error) => error.into(),
                crate::local_blob::ByteStreamWriteError::Local(error) => {
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
            Ok(()) => Ok(()),
            Err(crate::local_blob::CommitNewFileError::DestinationExists(_)) => {
                let (stored_size, stored_hash) = crate::local_blob::exact_file_facts(spool_file)
                    .await
                    .map_err(StorageError::LocalFilesystem)?;
                let object = ExactObjectRef::new(
                    ObjectSlot::logical(locator.semantic_key())?,
                    stored_size,
                    stored_hash,
                );
                let blob = crate::blob::locator::StoredBlobRef::new(locator.clone(), object)
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
                        crate::local_blob::PlaintextChunkReader::next_chunk(&mut reader, 1 << 20)
                            .await
                            .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
                    if chunk.is_empty() {
                        break;
                    }
                }
                Ok(())
            }
            Err(error) => Err(StorageError::LocalFilesystem(error.to_string())),
        }
    }

    async fn prepare_blob_object(
        &self,
        locator: &crate::blob::locator::BlobLocator,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
        slot: ObjectSlot,
        stored_file: &Path,
    ) -> Result<crate::blob::locator::StoredBlobRef, StorageError> {
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
        let (stored_size, stored_hash) = crate::local_blob::exact_file_facts(stored_file)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        crate::blob::locator::StoredBlobRef::new(
            locator.clone(),
            ExactObjectRef::new(slot, stored_size, stored_hash),
        )
        .map_err(|error| StorageError::InvalidContent(error.to_string()))
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        authority: &crate::sync::storage::BlobWriteAuthority<'_>,
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
        crate::local_blob::verify_exact_file(object, stored_file)
            .await
            .map_err(|error| match error {
                crate::local_blob::ExactFileVerificationError::Filesystem(error) => {
                    StorageError::LocalFilesystem(error)
                }
                crate::local_blob::ExactFileVerificationError::IdentityMismatch(error) => {
                    StorageError::InvalidContent(error)
                }
            })?;
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
        blob: &crate::blob::locator::StoredBlobRef,
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
        blob: &crate::blob::locator::StoredBlobRef,
        dest: &Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, StorageError> {
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
        let staged = crate::local_blob::stage_atomic_destination(dest)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        self.exact
            .read_at_to_file(object.slot(), staged.path())
            .await
            .map_err(|error| match error {
                CloudFileReadError::Source(error) => StorageError::from(error),
                CloudFileReadError::Local(error) => StorageError::LocalFilesystem(error),
            })?;
        crate::local_blob::verify_exact_file(object, staged.path())
            .await
            .map_err(|error| match error {
                crate::local_blob::ExactFileVerificationError::Filesystem(error) => {
                    StorageError::LocalFilesystem(error)
                }
                crate::local_blob::ExactFileVerificationError::IdentityMismatch(error) => {
                    StorageError::InvalidContent(error)
                }
            })?;
        Ok(staged)
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
        protection: crate::sync::storage::BlobSpoolProtection,
        dest: &Path,
    ) -> Result<crate::local_blob::AtomicStagedFile, StorageError> {
        let stored_destination = dest.with_extension("coven-stored-download");
        let stored = self
            .stage_exact_blob_download(blob, &stored_destination)
            .await?;
        let plaintext = crate::local_blob::stage_atomic_destination(dest)
            .await
            .map_err(StorageError::LocalFilesystem)?;
        let mut reader =
            ExactBlobPlaintextReader::new(stored.path(), &self.store_id, blob, protection).await?;
        let written = crate::local_blob::write_stream_to_stage(&plaintext, &mut reader)
            .await
            .map_err(|error| match error {
                crate::local_blob::StreamWriteError::Source(
                    crate::local_blob::PlaintextChunkError::Remote(error),
                ) => error,
                crate::local_blob::StreamWriteError::Source(
                    crate::local_blob::PlaintextChunkError::InvalidContent(error),
                ) => StorageError::InvalidContent(error),
                crate::local_blob::StreamWriteError::Source(
                    crate::local_blob::PlaintextChunkError::Local(error),
                )
                | crate::local_blob::StreamWriteError::Local(error) => {
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

    async fn delete_blob_object(
        &self,
        blob: &crate::blob::locator::StoredBlobRef,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::locator::{BlobLocator, RemoteAudience};
    use crate::blob::BlobScope;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::storage::{BlobWriteAuthority, ExactObjectRef};
    use crate::sync::store_commit::{
        DeviceStreamAnchor, ObjectHash, StoreCreationId, StoreDeviceRegistration,
        StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreRootRef,
    };

    #[test]
    fn peer_rotation_cannot_stand_in_for_the_exact_local_candidate() {
        let mutation = ObjectHash::digest(b"local rotation mutation");
        let gate = RotationGate::empty()
            .merge_peer_commit(2)
            .expect("record peer rotation");

        assert!(gate.commit_candidate(2, mutation).is_err());
    }

    #[test]
    fn local_adoption_cannot_close_another_local_rotation() {
        let adopted = ObjectHash::digest(b"adopted local rotation");
        let other = ObjectHash::digest(b"other local rotation");
        let gate = RotationGate {
            candidate: None,
            local_committed: Some(RotationLocalCommittedGate {
                generation: 3,
                mutation: other,
            }),
            peer_committed_generation: None,
        };

        assert!(gate.complete_local_adoption(2, adopted).is_err());
    }

    #[test]
    fn rotation_gate_rejects_empty_zero_and_two_local_owners() {
        assert!(RotationGate::empty().validate().is_err());
        assert!(RotationGate {
            candidate: None,
            local_committed: None,
            peer_committed_generation: Some(0),
        }
        .validate()
        .is_err());
        let mutation = ObjectHash::digest(b"rotation owner");
        assert!(RotationGate {
            candidate: Some(RotationCandidateGate {
                generation: 2,
                mutation,
            }),
            local_committed: Some(RotationLocalCommittedGate {
                generation: 2,
                mutation,
            }),
            peer_committed_generation: None,
        }
        .validate()
        .is_err());
    }

    #[test]
    fn local_adoption_clears_the_same_peer_fact_but_preserves_a_newer_one() {
        let mutation = ObjectHash::digest(b"local removal");
        let committed = RotationGate::empty()
            .with_candidate(2, mutation)
            .unwrap()
            .commit_candidate(2, mutation)
            .unwrap();
        assert_eq!(
            committed
                .clone()
                .merge_peer_commit(2)
                .unwrap()
                .complete_local_adoption(2, mutation)
                .unwrap(),
            None
        );
        assert_eq!(
            committed
                .merge_peer_commit(3)
                .unwrap()
                .complete_local_adoption(2, mutation)
                .unwrap()
                .unwrap()
                .pending_state()
                .unwrap(),
            RotationPendingState::PeerCommitted { generation: 3 }
        );
    }

    async fn blob_write_registration(
        storage: &CloudSyncStorage,
        label: &str,
    ) -> (StoreDeviceRegistrationRef, StoreDeviceRegistration) {
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
        let provider = SyncStorage::provider_binding(storage).await.unwrap().device;
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
            storage.user_keypair(),
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
        (reference, registration)
    }

    #[tokio::test]
    async fn circle_blob_spool_uses_the_supplied_audience_key() {
        let home = InMemoryCloudHome::new();
        let identity = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
            BlobPathScheme::Hashed,
            "circle-blob-spool",
            identity,
        )
        .expect("test cloud storage supports exact slots");
        let (uploader, registration) = blob_write_registration(&storage, "circle-blob-spool").await;
        let authority = BlobWriteAuthority::new(&uploader, &registration).unwrap();
        let circle_key = EncryptionService::from_key([9u8; 32]);
        let plaintext = b"circle audience blob";
        let locator = BlobLocator::opaque(
            "covers",
            "circle-cover",
            uploader.clone(),
            RemoteAudience::Circle(crate::sync::circle::CircleId::from_bytes([8; 16])),
            BlobScope::Master,
            circle_key.seal_key_fingerprint(),
            plaintext.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(plaintext),
        )
        .expect("build Circle locator");
        let temp = tempfile::tempdir().expect("temporary blob directory");
        let source = temp.path().join("plaintext");
        let spool = temp.path().join("spool");
        tokio::fs::write(&source, plaintext)
            .await
            .expect("write plaintext source");

        storage
            .seal_blob_to_spool(
                &locator,
                &authority,
                crate::sync::storage::BlobSpoolProtection::Opaque(circle_key.clone()),
                &source,
                &spool,
            )
            .await
            .expect("seal Circle blob spool");

        let stored = tokio::fs::read(&spool).await.expect("read exact spool");
        let opened = CloudCipher::Encrypted(circle_key)
            .open_scoped(
                BlobScope::Master,
                stored,
                &cloud_aad_context("circle-blob-spool", &locator.semantic_key()),
            )
            .expect("open Circle blob with supplied key");
        assert_eq!(opened, plaintext);
    }

    #[tokio::test]
    async fn blob_spool_rejects_a_key_that_differs_from_the_locator() {
        let home = InMemoryCloudHome::new();
        let identity = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
            BlobPathScheme::Hashed,
            "blob-spool-key-mismatch",
            identity,
        )
        .expect("test cloud storage supports exact slots");
        let (uploader, registration) =
            blob_write_registration(&storage, "blob-spool-key-mismatch").await;
        let authority = BlobWriteAuthority::new(&uploader, &registration).unwrap();
        let declared_key = EncryptionService::from_key([9u8; 32]);
        let plaintext = b"audience blob";
        let locator = BlobLocator::opaque(
            "covers",
            "mismatched-cover",
            uploader.clone(),
            RemoteAudience::Store,
            BlobScope::Master,
            declared_key.seal_key_fingerprint(),
            plaintext.len() as u64,
            crate::sync::store_commit::ObjectHash::digest(plaintext),
        )
        .expect("build locator");
        let temp = tempfile::tempdir().expect("temporary blob directory");
        let source = temp.path().join("plaintext");
        let spool = temp.path().join("spool");
        tokio::fs::write(&source, plaintext)
            .await
            .expect("write plaintext source");

        assert!(matches!(
            storage
                .seal_blob_to_spool(
                    &locator,
                    &authority,
                    crate::sync::storage::BlobSpoolProtection::Opaque(EncryptionService::from_key(
                        [10u8; 32]
                    ),),
                    &source,
                    &spool,
                )
                .await,
            Err(StorageError::InvalidContent(_))
        ));
        assert!(!spool.exists());
    }

    #[tokio::test]
    async fn exact_blob_plaintext_is_published_only_after_both_verifications() {
        let home = InMemoryCloudHome::new();
        let identity = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
            BlobPathScheme::Hashed,
            "verified-blob-download",
            identity,
        )
        .expect("test cloud storage supports exact slots");
        let (uploader, registration) =
            blob_write_registration(&storage, "verified-blob-download").await;
        let authority = BlobWriteAuthority::new(&uploader, &registration).unwrap();
        let audience_key = EncryptionService::from_key([9u8; 32]);
        let plaintext: Vec<u8> = (0..150_000u32).map(|value| (value % 251) as u8).collect();
        let locator = BlobLocator::opaque(
            "audio",
            "verified-track",
            uploader.clone(),
            RemoteAudience::Store,
            BlobScope::Derived("album-a".to_string()),
            audience_key.seal_key_fingerprint(),
            plaintext.len() as u64,
            ObjectHash::digest(&plaintext),
        )
        .expect("build locator");
        let temp = tempfile::tempdir().expect("temporary blob directory");
        let source = temp.path().join("plaintext");
        let spool = temp.path().join("spool");
        let destination = temp.path().join("materialized");
        tokio::fs::write(&source, &plaintext)
            .await
            .expect("write plaintext source");
        storage
            .seal_blob_to_spool(
                &locator,
                &authority,
                crate::sync::storage::BlobSpoolProtection::Opaque(audience_key.clone()),
                &source,
                &spool,
            )
            .await
            .expect("seal exact spool");
        let slot = storage
            .allocate_blob_slot(&locator, &authority)
            .await
            .expect("allocate exact blob slot");
        let blob = storage
            .prepare_blob_object(&locator, &authority, slot, &spool)
            .await
            .expect("prepare exact blob");
        storage
            .create_blob_object_from_file(
                &blob,
                &authority,
                &spool,
                &crate::storage::cloud::no_progress(),
            )
            .await
            .expect("create exact blob");

        let staged = storage
            .stage_verified_blob_plaintext(
                &blob,
                crate::sync::storage::BlobSpoolProtection::Opaque(audience_key),
                &destination,
            )
            .await
            .expect("stage verified plaintext");
        assert!(!destination.exists());
        assert_eq!(tokio::fs::read(staged.path()).await.unwrap(), plaintext);
        staged.commit().await.expect("publish verified plaintext");
        assert_eq!(tokio::fs::read(destination).await.unwrap(), plaintext);
    }

    #[tokio::test]
    async fn stored_blob_corruption_never_creates_a_plaintext_stage() {
        let home = InMemoryCloudHome::new();
        let identity = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::from_key([3u8; 32])),
            BlobPathScheme::Hashed,
            "corrupt-blob-download",
            identity,
        )
        .expect("test cloud storage supports exact slots");
        let (uploader, registration) =
            blob_write_registration(&storage, "corrupt-blob-download").await;
        let authority = BlobWriteAuthority::new(&uploader, &registration).unwrap();
        let audience_key = EncryptionService::from_key([9u8; 32]);
        let plaintext = b"signed blob plaintext";
        let locator = BlobLocator::opaque(
            "covers",
            "corrupt-cover",
            uploader.clone(),
            RemoteAudience::Store,
            BlobScope::Master,
            audience_key.seal_key_fingerprint(),
            plaintext.len() as u64,
            ObjectHash::digest(plaintext),
        )
        .expect("build locator");
        let temp = tempfile::tempdir().expect("temporary blob directory");
        let source = temp.path().join("plaintext");
        let spool = temp.path().join("spool");
        let destination = temp.path().join("materialized");
        tokio::fs::write(&source, plaintext)
            .await
            .expect("write plaintext source");
        storage
            .seal_blob_to_spool(
                &locator,
                &authority,
                crate::sync::storage::BlobSpoolProtection::Opaque(audience_key.clone()),
                &source,
                &spool,
            )
            .await
            .expect("seal exact spool");
        let slot = storage
            .allocate_blob_slot(&locator, &authority)
            .await
            .unwrap();
        let blob = storage
            .prepare_blob_object(&locator, &authority, slot, &spool)
            .await
            .unwrap();
        storage
            .create_blob_object_from_file(
                &blob,
                &authority,
                &spool,
                &crate::storage::cloud::no_progress(),
            )
            .await
            .unwrap();
        home.replace_exact_object(blob.object().slot(), b"corrupt".to_vec());

        assert!(matches!(
            storage
                .stage_verified_blob_plaintext(
                    &blob,
                    crate::sync::storage::BlobSpoolProtection::Opaque(audience_key),
                    &destination,
                )
                .await,
            Err(StorageError::InvalidContent(_))
        ));
        assert!(!destination.exists());
    }

    #[tokio::test]
    async fn reserved_protocol_slot_read_returns_its_completed_exact_reference() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Hashed,
            "reserved-slot-read",
            UserKeypair::generate(),
        )
        .expect("test cloud storage supports exact slots");
        let root = crate::sync::store_commit::ObjectHash::digest(b"reserved slot root");
        let semantic = "store-v1/heads/device-a/1".to_string();
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root,
            ProtocolObjectDomain::StoreHead,
        );
        let slot = storage
            .allocate_protocol_slot(&context, &semantic, ".json")
            .await
            .expect("reserve successor slot");
        let prepared = storage
            .prepare_protocol_object(
                &context,
                slot.clone(),
                &semantic,
                b"signed successor bytes".to_vec(),
            )
            .expect("prepare successor bytes");
        storage
            .create_protocol_object(&prepared)
            .await
            .expect("create successor");

        let (opened, completed) = storage
            .read_protocol_slot(&context, &slot, &semantic)
            .await
            .expect("read reserved successor slot");

        assert_eq!(opened, b"signed successor bytes");
        assert_eq!(&completed, prepared.reference());
    }

    #[test]
    fn protocol_object_prepare_rejects_a_path_outside_its_domain() {
        let storage = CloudSyncStorage::new(
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Hashed,
            "prepare-domain-path",
            UserKeypair::generate(),
        )
        .expect("test cloud storage supports exact slots");
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            ObjectHash::digest(b"prepare domain root"),
            ProtocolObjectDomain::StoreHead,
        );
        let invalid_semantic = "store-v1/commits/device-a/1";
        let slot = ObjectSlot::logical(format!("{invalid_semantic}.json"))
            .expect("valid logical object slot");

        assert!(matches!(
            storage.prepare_protocol_object(
                &context,
                slot,
                invalid_semantic,
                b"signed bytes".to_vec(),
            ),
            Err(StorageError::Parse(_))
        ));
    }

    #[tokio::test]
    async fn exact_delete_refuses_to_remove_different_bytes_in_the_same_slot() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Hashed,
            "exact-delete-identity",
            UserKeypair::generate(),
        )
        .expect("test cloud storage supports exact slots");
        let root = ObjectHash::digest(b"exact delete root");
        let semantic = "store-v1/heads/device-a/1";
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root,
            ProtocolObjectDomain::StoreHead,
        );
        let slot = storage
            .allocate_protocol_slot(&context, semantic, ".json")
            .await
            .expect("allocate exact slot");
        let prepared = storage
            .prepare_protocol_object(&context, slot.clone(), semantic, b"original".to_vec())
            .expect("prepare exact object");
        storage
            .create_protocol_object(&prepared)
            .await
            .expect("create exact object");
        home.replace_exact_object(&slot, b"competing stored bytes".to_vec());

        assert!(matches!(
            storage.delete_protocol_object(prepared.reference()).await,
            Err(StorageError::SlotCollision(_))
        ));
        assert_eq!(
            home.get(slot.logical_key()),
            Some(b"competing stored bytes".to_vec())
        );
    }

    #[tokio::test]
    async fn reserved_protocol_slot_rejects_a_mismatched_semantic_path_before_read() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(EncryptionService::from_key([7u8; 32])),
            BlobPathScheme::Hashed,
            "reserved-slot-relocation",
            UserKeypair::generate(),
        )
        .expect("test cloud storage supports exact slots");
        let root = crate::sync::store_commit::ObjectHash::digest(b"reserved slot root");
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root,
            ProtocolObjectDomain::StoreHead,
        );
        let original = "store-v1/heads/device-a/1".to_string();
        let relocated = "store-v1/heads/device-b/1".to_string();
        let slot = storage
            .allocate_protocol_slot(&context, &original, ".json")
            .await
            .expect("reserve successor slot");

        assert!(matches!(
            storage
                .read_protocol_slot(&context, &slot, &relocated)
                .await,
            Err(StorageError::Parse(_))
        ));
    }

    #[tokio::test]
    async fn protocol_object_read_rejects_domain_and_path_substitution() {
        let home = InMemoryCloudHome::new();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(EncryptionService::from_key([8u8; 32])),
            BlobPathScheme::Hashed,
            "aad-store",
            UserKeypair::generate(),
        )
        .expect("test cloud storage supports immutable copies");
        let root = crate::sync::store_commit::ObjectHash::digest(b"root-a");
        let other_root = crate::sync::store_commit::ObjectHash::digest(b"root-b");
        let commit_hash = crate::sync::store_commit::ObjectHash::digest(b"commit");
        let family = crate::sync::store_commit::CandidateFamilyId::from_hash(
            crate::sync::store_commit::ObjectHash::digest(b"cloud test family"),
        );
        let semantic =
            crate::sync::store_commit::commit_semantic_prefix(family, "device", 1, commit_hash);
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root,
            ProtocolObjectDomain::StoreCommit,
        );
        let slot = storage
            .allocate_protocol_slot(&context, &semantic, ".json")
            .await
            .expect("allocate root-bound Store commit slot");
        let prepared = storage
            .prepare_protocol_object(&context, slot, &semantic, b"signed commit".to_vec())
            .expect("prepare root-bound Store commit");
        storage
            .create_protocol_object(&prepared)
            .await
            .expect("create root-bound Store commit");
        let object = prepared.reference().clone();

        assert_eq!(
            storage
                .read_protocol_object(&context, &object, &semantic)
                .await
                .expect("read with the exact authenticated context"),
            b"signed commit",
        );
        let other_root_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            other_root,
            ProtocolObjectDomain::StoreCommit,
        );
        assert_eq!(
            storage
                .read_protocol_object(&other_root_context, &object, &semantic)
                .await
                .expect("signed plaintext bytes are opened before their root signature is parsed"),
            b"signed commit",
        );

        let other_semantic =
            crate::sync::store_commit::commit_semantic_prefix(family, "device", 2, commit_hash);
        assert!(matches!(
            storage
                .read_protocol_object(&context, &object, &other_semantic)
                .await,
            Err(crate::sync::storage::StorageError::Parse(_))
        ));

        let other_domain_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root,
            ProtocolObjectDomain::StoreHead,
        );
        assert!(matches!(
            storage
                .read_protocol_object(&other_domain_context, &object, &semantic)
                .await,
            Err(crate::sync::storage::StorageError::Parse(_))
        ));
    }

    #[tokio::test]
    async fn signed_control_is_readable_across_store_key_rotations_but_packages_are_not() {
        let home = Arc::new(InMemoryCloudHome::new());
        let writer = CloudSyncStorage::new(
            home.clone(),
            CloudCipher::Encrypted(EncryptionService::from_key([8u8; 32])),
            BlobPathScheme::Hashed,
            "control-plane-rotation",
            UserKeypair::generate(),
        )
        .expect("writer storage");
        let stale_reader = CloudSyncStorage::new(
            home,
            CloudCipher::Encrypted(EncryptionService::from_key([9u8; 32])),
            BlobPathScheme::Hashed,
            "control-plane-rotation",
            UserKeypair::generate(),
        )
        .expect("stale reader storage");
        let root = ObjectHash::digest(b"control plane root");
        let head_semantic = "store-v1/heads/device-a/1";
        let head_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            root,
            ProtocolObjectDomain::StoreHead,
        );
        let head_slot = writer
            .allocate_protocol_slot(&head_context, head_semantic, ".json")
            .await
            .expect("allocate signed head");
        let head = writer
            .prepare_protocol_object(
                &head_context,
                head_slot,
                head_semantic,
                b"signed control bytes".to_vec(),
            )
            .expect("prepare signed head");
        writer
            .create_protocol_object(&head)
            .await
            .expect("create signed head");
        assert_eq!(
            stale_reader
                .read_protocol_object(&head_context, head.reference(), head_semantic)
                .await
                .expect("read signed control with a different Store key"),
            b"signed control bytes",
        );

        let family = crate::sync::store_commit::CandidateFamilyId::from_hash(ObjectHash::digest(
            b"control plane package family",
        ));
        let package_hash = ObjectHash::digest(b"encrypted package");
        let package_semantic = format!(
            "store-v1/candidates/{}/packages/device-a/1/{package_hash}",
            family.as_hash()
        );
        let package_context = crate::sync::storage::ProtocolObjectContext::store_encrypted(
            root,
            ProtocolObjectDomain::StorePackage,
        );
        let package_slot = writer
            .allocate_protocol_slot(&package_context, &package_semantic, ".pkg")
            .await
            .expect("allocate encrypted package");
        let package = writer
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_semantic,
                b"encrypted package".to_vec(),
            )
            .expect("prepare encrypted package");
        writer
            .create_protocol_object(&package)
            .await
            .expect("create encrypted package");
        assert!(matches!(
            stale_reader
                .read_protocol_object(&package_context, package.reference(), &package_semantic,)
                .await,
            Err(StorageError::Decryption(_))
        ));
    }

    #[tokio::test]
    async fn malformed_durable_pending_rotation_blocks_session_reopen() {
        let directory = tempfile::tempdir().expect("pending-rotation database directory");
        let path = directory.path().join("store.sqlite3");
        let open = || {
            crate::database::Database::open(
                &path,
                crate::sync::test_helpers::test_synced_tables(),
                crate::blob::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                "pending-rotation-reopen-device".to_string(),
                &crate::sync::test_helpers::test_migrations(),
            )
            .expect("open pending-rotation database")
            .0
        };
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let encryption = EncryptionService::from_key([17; 32]);
        let db = open();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(encryption.clone()),
            BlobPathScheme::Hashed,
            "pending-rotation-reopen",
            signer.clone(),
        )
        .expect("construct pending-rotation storage");
        let components = crate::sync::cycle::init_sync_over_storage(
            &crate::sync::store::StoreDatabase::new(&db),
            storage,
            crate::sync::cycle::StoreInitialization::CreateStore,
            None,
        )
        .await
        .expect("initialize pending-rotation Store");
        let root = crate::sync::store::database::StoreDatabase::new(&db)
            .local_store_root_ref()
            .await
            .expect("read pending-rotation Store root")
            .expect("pending-rotation Store root exists");
        db.set_protocol_state(ROTATION_GATE_STATE_KEY, "not-a-rotation-gate")
            .await
            .expect("persist malformed pending rotation");
        drop(components);
        drop(db);

        let reopened = open();
        let storage = CloudSyncStorage::new(
            Arc::new(home),
            CloudCipher::Encrypted(encryption),
            BlobPathScheme::Hashed,
            "pending-rotation-reopen",
            signer,
        )
        .expect("reconstruct pending-rotation storage");
        let result = crate::sync::cycle::init_sync_over_storage(
            &crate::sync::store::StoreDatabase::new(&reopened),
            storage,
            crate::sync::cycle::StoreInitialization::OpenStore {
                expected_store_root: root,
            },
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(crate::sync::cycle::InitSyncError::PendingRotationRestore(_))
        ));
    }
}
