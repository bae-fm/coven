use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::keys::KeyError;
use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use hkdf::Hkdf;
use rand::RngCore;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::info;

/// XChaCha20-Poly1305 nonce size (24 bytes).
pub const NONCE_SIZE: usize = 24;

/// Poly1305 auth tag size (16 bytes).
pub const TAG_SIZE: usize = 16;

/// 64KB plaintext chunks
pub const CHUNK_SIZE: usize = 65536;
/// Each encrypted chunk: plaintext + 16-byte auth tag
pub const ENCRYPTED_CHUNK_SIZE: usize = CHUNK_SIZE + TAG_SIZE;
pub const INITIAL_KEY_GENERATION: u64 = 1;
const AEAD_V2_LABEL: &[u8] = b"coven-aead-v2";

/// The sealed app-data format version this build writes, and the only one it
/// reads. The leading byte of every payload [`EncryptionService::seal_app_data`]
/// produces; a payload naming any other version is refused
/// ([`SealError::UnknownVersion`]) rather than guessed at.
pub const APP_DATA_SEAL_VERSION: u8 = 1;

/// The fixed header every sealed app-data payload carries ahead of its
/// ciphertext: the version byte, then the sealing key's 8-byte fingerprint.
const APP_DATA_FINGERPRINT_SIZE: usize = 8;
const APP_DATA_HEADER_SIZE: usize = 1 + APP_DATA_FINGERPRINT_SIZE;

/// Stable wire identity of one 32-byte encryption key: the first eight bytes
/// of its SHA-256 digest, serialized as exactly sixteen lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyFingerprint([u8; 8]);

impl KeyFingerprint {
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }
}

impl fmt::Debug for KeyFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for KeyFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for KeyFingerprint {
    type Err = KeyFingerprintParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 16
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(KeyFingerprintParseError(value.to_string()));
        }
        let bytes: [u8; 8] = hex::decode(value)
            .map_err(|_| KeyFingerprintParseError(value.to_string()))?
            .try_into()
            .map_err(|_| KeyFingerprintParseError(value.to_string()))?;
        Ok(Self(bytes))
    }
}

impl serde::Serialize for KeyFingerprint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for KeyFingerprint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <String as serde::Deserialize>::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("key fingerprint must be exactly sixteen lowercase hexadecimal characters: {0:?}")]
pub struct KeyFingerprintParseError(String);

/// Generate a random 32-byte key.
pub fn generate_random_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    key
}

/// The length of the encrypted blob [`EncryptionService::encrypt`] produces for
/// a plaintext of `plaintext_len` bytes: the base nonce, the plaintext itself,
/// and one 16-byte tag per chunk. An empty plaintext still produces one
/// (tag-only) chunk. Lets a streaming upload know the final object size up
/// front, before a byte is sealed.
pub fn chunked_encrypted_len(plaintext_len: u64) -> u64 {
    NONCE_SIZE as u64
        + plaintext_len
        + chunk_count_for_plaintext_len(plaintext_len) * TAG_SIZE as u64
}

/// Incremental encryptor that emits the same `[base_nonce][chunk_0][chunk_1]...`
/// bytes as [`EncryptionService::encrypt`], one chunk at a time, so a large blob
/// is sealed and uploaded without ever holding the whole plaintext or ciphertext
/// in memory. `encrypt` is itself implemented on top of this, so the streaming
/// and whole-buffer forms cannot drift.
pub struct ChunkSealer {
    cipher: XChaCha20Poly1305,
    base_nonce: [u8; NONCE_SIZE],
    aad_context: Vec<u8>,
    total_chunks: u64,
    next_index: u64,
}

impl ChunkSealer {
    /// Start a sealer with a fresh random base nonce.
    fn new(key: &[u8; 32], plaintext_len: u64, aad_context: &[u8]) -> Self {
        let mut base_nonce = [0u8; NONCE_SIZE];
        rand::rng().fill_bytes(&mut base_nonce);
        Self {
            cipher: XChaCha20Poly1305::new(GenericArray::from_slice(key)),
            base_nonce,
            aad_context: aad_context.to_vec(),
            total_chunks: chunk_count_for_plaintext_len(plaintext_len),
            next_index: 0,
        }
    }

    /// The base nonce — the first [`NONCE_SIZE`] bytes of the encrypted blob,
    /// emitted before any chunk.
    pub fn base_nonce(&self) -> [u8; NONCE_SIZE] {
        self.base_nonce
    }

    /// Seal one plaintext chunk (at most [`CHUNK_SIZE`] bytes) into its
    /// ciphertext-plus-tag, advancing the chunk counter. A chunk longer than
    /// `CHUNK_SIZE` would desync the framing the decryptor expects, so the caller
    /// must split the plaintext on `CHUNK_SIZE` boundaries.
    pub fn seal_chunk(&mut self, plaintext: &[u8]) -> Vec<u8> {
        debug_assert!(
            plaintext.len() <= CHUNK_SIZE,
            "a sealed chunk must be at most CHUNK_SIZE bytes"
        );
        let nonce = chunk_nonce(&self.base_nonce, self.next_index);
        let aad = chunk_aad(&self.aad_context, self.next_index, self.total_chunks);
        self.next_index += 1;
        self.cipher
            .encrypt(
                GenericArray::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .expect("encryption should not fail")
    }
}

#[derive(Error, Debug)]
pub enum EncryptionError {
    #[error("Encryption failed: {0}")]
    Encryption(String),
    #[error("Decryption failed: {0}")]
    Decryption(String),
    #[error("Key management error: {0}")]
    KeyManagement(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Why sealing or opening a host's app-data failed.
///
/// Sealing can only fail before the cipher runs — the store has no master key
/// to seal under. Opening adds the failures a stored payload can carry: a
/// version this build does not read, a generation this keyring holds no key
/// for, or an AEAD rejection (a wrong `aad`, a tampered or truncated payload).
#[derive(Debug, Error)]
pub enum SealError {
    /// Custody unlocked no keyring: the store is locked, or a master key was
    /// never established. The app-data counterpart of the sync engine's
    /// master-key gate — `unlock` returning `None` is refused here, never
    /// treated as an empty keyring to seal under.
    #[error("no master key is established for this store (locked, or never initialized)")]
    Locked,
    /// Custody could not produce the keyring — a wrong passphrase, an
    /// unreadable backing store. Distinct from [`Self::Locked`], which is a
    /// legitimate absence rather than a failure.
    #[error("custody error: {0}")]
    Custody(#[from] KeyError),
    /// The payload's leading version byte is not one this build seals or reads.
    #[error("unsupported sealed app-data version {0}")]
    UnknownVersion(u8),
    /// The payload names a key (by fingerprint) this keyring does not hold: the
    /// keyring predates the payload, or the payload was sealed under a foreign one.
    #[error("sealed app-data names key {0}, which this keyring does not hold")]
    UnknownKey(String),
    /// The AEAD rejected the payload — a wrong `aad`, or a tampered or
    /// truncated ciphertext. Surfaced as it happened, never masked.
    #[error("app-data cryptography failed: {0}")]
    Crypto(#[from] EncryptionError),
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredKeyring {
    keys: Vec<StoredKeyringGeneration>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredKeyringGeneration {
    generation: u64,
    key_hex: String,
}

/// One key a keyring holds: its 32 bytes and the generation number that orders
/// it. The key's fingerprint (not the generation) is its identity — two entries
/// can share a generation number without colliding.
#[derive(Clone)]
struct KeyEntry {
    generation: u64,
    key: [u8; 32],
}

/// A store's master key material: every key it holds. This is the value custody
/// implementations store, unlock, and re-protect — never a cipher. coven builds
/// the [`EncryptionService`] cipher from it internally; custody never touches
/// cipher machinery.
#[derive(Clone)]
pub struct MasterKeyring {
    keys: BTreeMap<[u8; 8], KeyEntry>,
}

impl std::fmt::Debug for MasterKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasterKeyring")
            .field("keys", &"<redacted>")
            .finish()
    }
}

impl MasterKeyring {
    /// One fresh generation-1 key.
    pub fn generate() -> Self {
        Self::from(EncryptionService::from_key(generate_random_key()))
    }

    /// Serialize to the stored keyring JSON — the same format
    /// [`EncryptionService::to_keyring_string`] produces, since every
    /// generation this type holds came from (or feeds) that cipher.
    pub fn to_serialized(&self) -> String {
        EncryptionService::from(self.clone())
            .to_keyring_string()
            .expect("a MasterKeyring always holds at least one generation")
    }

    /// Parse the stored master-key format [`Self::to_serialized`] produces.
    pub fn from_serialized(s: &str) -> Result<Self, EncryptionError> {
        EncryptionService::new(s).map(Self::from)
    }

    /// SHA-256 fingerprint of the seal key (the deterministically selected
    /// key this keyring seals new data under), first 8 bytes hex-encoded. Short
    /// enough to display in UI, long enough to detect wrong keys.
    pub fn fingerprint(&self) -> String {
        EncryptionService::from(self.clone()).fingerprint()
    }
}

impl From<EncryptionService> for MasterKeyring {
    fn from(service: EncryptionService) -> Self {
        Self { keys: service.keys }
    }
}

impl From<MasterKeyring> for EncryptionService {
    fn from(keyring: MasterKeyring) -> Self {
        EncryptionService { keys: keyring.keys }
    }
}

/// The 8-byte fingerprint of a key: the first 8 bytes of its SHA-256. A keyring
/// entry's identity, and what a sealed object names to say which key sealed it.
fn key_fingerprint_bytes(key: &[u8; 32]) -> [u8; 8] {
    let hash = Sha256::digest(key);
    let mut fingerprint = [0u8; 8];
    fingerprint.copy_from_slice(&hash[..8]);
    fingerprint
}

/// Manages encryption keys and provides XChaCha20-Poly1305 encryption/decryption
///
/// This implements the security model described in the README:
/// - Files are encrypted using XChaCha20-Poly1305 for authenticated encryption
/// - Chunked format enables random-access decryption for efficient range reads
#[derive(Clone)]
pub struct EncryptionService {
    // Keyed by key fingerprint, so two keys sharing a generation number (a fork
    // from two owners rotating at once) coexist as distinct entries rather than
    // one silently overwriting the other. The seal key is chosen deterministically
    // (highest generation, then greatest fingerprint), so once every device holds
    // the union of both, they all converge on one seal key. A sealed object names
    // the key it was sealed under by fingerprint, so anything sealed under any key
    // this keyring holds stays decryptable regardless of which key is current.
    keys: BTreeMap<[u8; 8], KeyEntry>,
}
impl std::fmt::Debug for EncryptionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionService")
            .field("keys", &"<redacted>")
            .finish()
    }
}
impl EncryptionService {
    /// Create an encryption service from a serialized keyring.
    pub fn new(stored_key: &str) -> Result<Self, EncryptionError> {
        info!("Loading master key...");
        EncryptionService::from_keyring_json(stored_key)
    }

    /// Create a new encryption service from a raw 32-byte key.
    pub fn from_key(key: [u8; 32]) -> Self {
        Self::from_key_at_generation(INITIAL_KEY_GENERATION, key)
    }

    pub fn from_key_at_generation(generation: u64, key: [u8; 32]) -> Self {
        let mut keys = BTreeMap::new();
        keys.insert(key_fingerprint_bytes(&key), KeyEntry { generation, key });
        EncryptionService { keys }
    }

    pub fn from_keyring(
        keys: impl IntoIterator<Item = (u64, [u8; 32])>,
    ) -> Result<Self, EncryptionError> {
        let keys: BTreeMap<[u8; 8], KeyEntry> = keys
            .into_iter()
            .map(|(generation, key)| (key_fingerprint_bytes(&key), KeyEntry { generation, key }))
            .collect();
        if keys.is_empty() {
            return Err(EncryptionError::KeyManagement(
                "keyring has no keys".to_string(),
            ));
        }
        Ok(EncryptionService { keys })
    }

    /// The keyring entry this device seals new data under, chosen
    /// deterministically fleet-wide: the highest generation number, and among
    /// keys sharing that generation, the greatest fingerprint. Once the wraps of
    /// a fork propagate so every device holds both keys, they all pick the same
    /// one here — a fork converges instead of partitioning.
    fn seal_entry(&self) -> (&[u8; 8], &KeyEntry) {
        self.keys
            .iter()
            .max_by(|(fingerprint_a, a), (fingerprint_b, b)| {
                a.generation
                    .cmp(&b.generation)
                    .then_with(|| fingerprint_a.cmp(fingerprint_b))
            })
            .expect("a keyring always holds at least one key")
    }

    pub fn current_generation(&self) -> u64 {
        self.seal_entry().1.generation
    }

    /// How many keys this keyring holds. Two keys at the same generation count
    /// as two — the count grows only when a genuinely new key is folded in.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    pub fn keyring_entries(&self) -> Vec<(u64, [u8; 32])> {
        self.keys
            .values()
            .map(|entry| (entry.generation, entry.key))
            .collect()
    }

    /// Union this keyring with `other`: every key either holds. A key already
    /// present (same fingerprint) keeps its existing entry, so a merge never
    /// drops a key already adopted and never rewrites one. This is how adoption
    /// folds an incoming keyring in — keyrings merge, they never replace.
    pub fn merged_with(&self, other: &EncryptionService) -> EncryptionService {
        let mut keys = self.keys.clone();
        for (fingerprint, entry) in &other.keys {
            keys.entry(*fingerprint).or_insert_with(|| entry.clone());
        }
        EncryptionService { keys }
    }

    pub fn to_keyring_string(&self) -> Result<String, EncryptionError> {
        let payload = StoredKeyring {
            keys: self
                .keys
                .values()
                .map(|entry| StoredKeyringGeneration {
                    generation: entry.generation,
                    key_hex: hex::encode(entry.key),
                })
                .collect(),
        };
        serde_json::to_string(&payload)
            .map_err(|e| EncryptionError::KeyManagement(format!("serialize keyring: {e}")))
    }

    pub fn to_keyring_payload(&self) -> Result<Vec<u8>, EncryptionError> {
        self.to_keyring_string().map(String::into_bytes)
    }

    pub fn from_keyring_payload(plaintext: Vec<u8>) -> Result<Self, EncryptionError> {
        let keyring = String::from_utf8(plaintext).map_err(|e| {
            EncryptionError::KeyManagement(format!("keyring payload is not UTF-8: {e}"))
        })?;
        EncryptionService::from_keyring_json(&keyring)
    }

    fn from_keyring_json(keyring: &str) -> Result<Self, EncryptionError> {
        let payload: StoredKeyring = serde_json::from_str(keyring).map_err(|e| {
            EncryptionError::KeyManagement(format!("keyring JSON is malformed: {e}"))
        })?;
        let mut keys = Vec::with_capacity(payload.keys.len());
        for entry in payload.keys {
            let key: [u8; 32] = hex::decode(&entry.key_hex)
                .map_err(|e| {
                    EncryptionError::KeyManagement(format!("keyring key is not hex: {e}"))
                })?
                .try_into()
                .map_err(|_| {
                    EncryptionError::KeyManagement("keyring key is not 32 bytes".to_string())
                })?;
            keys.push((entry.generation, key));
        }
        EncryptionService::from_keyring(keys)
    }

    /// The key with fingerprint `fingerprint`, if this keyring holds it. A sealed
    /// object names its sealing key this way, so decryption resolves the key by
    /// identity rather than by a generation number that a fork could reuse.
    pub fn key_for_fingerprint(&self, fingerprint: &[u8; 8]) -> Result<[u8; 32], EncryptionError> {
        self.keys.get(fingerprint).map(|e| e.key).ok_or_else(|| {
            EncryptionError::KeyManagement(format!(
                "no key with fingerprint {}",
                hex::encode(fingerprint)
            ))
        })
    }

    pub fn service_for_fingerprint(
        &self,
        fingerprint: &[u8; 8],
    ) -> Result<EncryptionService, EncryptionError> {
        let key = self.key_for_fingerprint(fingerprint)?;
        // The single-key service keeps the source key's generation so its own
        // seal choices and any re-serialization stay consistent with the keyring
        // it came from.
        let generation = self
            .keys
            .get(fingerprint)
            .expect("just resolved")
            .generation;
        Ok(EncryptionService::from_key_at_generation(generation, key))
    }

    pub fn with_appended_generation(
        &self,
        generation: u64,
        key: [u8; 32],
    ) -> Result<EncryptionService, EncryptionError> {
        if generation <= self.current_generation() {
            return Err(EncryptionError::KeyManagement(format!(
                "new generation {generation} must be greater than current generation {}",
                self.current_generation()
            )));
        }
        let mut keys = self.keys.clone();
        keys.insert(key_fingerprint_bytes(&key), KeyEntry { generation, key });
        Ok(EncryptionService { keys })
    }

    /// SHA-256 fingerprint of the seal key, first 8 bytes hex-encoded (16 hex
    /// chars). Short enough to display in UI, long enough to detect wrong keys.
    pub fn fingerprint(&self) -> String {
        hex::encode(self.seal_fingerprint())
    }

    /// The seal key's 8-byte fingerprint — what a sealed object records so a
    /// later read resolves the exact key, whatever the keyring has become since.
    pub fn seal_fingerprint(&self) -> [u8; 8] {
        *self.seal_entry().0
    }

    pub fn seal_key_fingerprint(&self) -> KeyFingerprint {
        KeyFingerprint::from_bytes(self.seal_fingerprint())
    }

    /// Return the raw 32-byte seal key.
    pub fn key_bytes(&self) -> [u8; 32] {
        self.seal_entry().1.key
    }

    /// Encrypt data using chunked XChaCha20-Poly1305 format.
    /// Returns: [base_nonce: 24 bytes][ciphertext with auth tags]
    /// For small data (single chunk), this is equivalent to standard AEAD.
    /// For large data, each chunk is independently encrypted for random-access.
    pub fn encrypt(&self, plaintext: &[u8], aad_context: &[u8]) -> Vec<u8> {
        let mut sealer = self.sealer(plaintext.len() as u64, aad_context);
        let mut output = sealer.base_nonce().to_vec();

        // Empty plaintext still produces one chunk holding just the auth tag.
        if plaintext.is_empty() {
            output.extend(sealer.seal_chunk(&[]));
            return output;
        }

        for chunk in plaintext.chunks(CHUNK_SIZE) {
            output.extend(sealer.seal_chunk(chunk));
        }

        output
    }

    /// Decrypt data in chunked format: [nonce (24 bytes)][ciphertext chunks...]
    pub fn decrypt(
        &self,
        encrypted_data: &[u8],
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        let base_nonce = read_base_nonce(encrypted_data)?;
        let layout = encrypted_chunk_layout(encrypted_data.len())?;
        let key = self.key_bytes();
        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&key));

        let mut result = Vec::with_capacity(decrypted_len_upper_bound(layout.data_len));
        for chunk_index in 0..layout.total_chunks {
            let (chunk_start, chunk_end) =
                layout.chunk_bounds(encrypted_data.len(), chunk_index)?;
            let chunk_data = &encrypted_data[chunk_start..chunk_end];
            let decrypted = decrypt_chunk_with_cipher(
                &cipher,
                &base_nonce,
                aad_context,
                chunk_index as u64,
                layout.total_chunks as u64,
                chunk_data,
            )
            .map_err(|_| {
                EncryptionError::Decryption(format!(
                    "Authentication failed for chunk {}",
                    chunk_index
                ))
            })?;
            result.extend(decrypted);
        }

        Ok(result)
    }

    /// A streaming sealer over this service's key, for encrypting a blob
    /// chunk-by-chunk straight into an upload. See [`ChunkSealer`].
    pub fn sealer(&self, plaintext_len: u64, aad_context: &[u8]) -> ChunkSealer {
        ChunkSealer::new(&self.key_bytes(), plaintext_len, aad_context)
    }

    /// Decrypt a specific chunk from chunked encrypted data.
    /// Enables random-access decryption without reading preceding chunks.
    pub fn decrypt_chunk(
        &self,
        ciphertext: &[u8],
        chunk_index: usize,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        let base_nonce = read_base_nonce(ciphertext)?;
        let layout = encrypted_chunk_layout(ciphertext.len())?;
        let (chunk_start, chunk_end) = layout.chunk_bounds(ciphertext.len(), chunk_index)?;
        let chunk_data = &ciphertext[chunk_start..chunk_end];

        let key = self.key_bytes();
        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&key));
        decrypt_chunk_with_cipher(
            &cipher,
            &base_nonce,
            aad_context,
            chunk_index as u64,
            layout.total_chunks as u64,
            chunk_data,
        )
        .map_err(|_| EncryptionError::Decryption("Authentication failed".to_string()))
    }

    /// Decrypt a plaintext byte range using nonce from DB and partial chunk data.
    ///
    /// This is the efficient method for encrypted range requests:
    /// - `nonce`: 24-byte nonce stored in DB at import time
    /// - `encrypted_chunks`: Raw encrypted chunk bytes (NO nonce prefix)
    /// - `first_chunk_index`: Which chunk index the encrypted_chunks starts at
    /// - `plaintext_start`, `plaintext_end`: Absolute byte positions in original file
    ///
    /// Example: To read plaintext bytes 500,000-600,000:
    /// 1. Calculate needed chunks: `encrypted_chunk_range(500000, 600000)` -> chunks 7-9
    /// 2. Fetch encrypted bytes from cloud at those positions
    /// 3. Call `decrypt_range_with_offset(nonce, chunks, 7, 500000, 600000, source_size, aad_context)`
    pub fn decrypt_range_with_offset(
        &self,
        nonce: &[u8],
        encrypted_chunks: &[u8],
        first_chunk_index: u64,
        plaintext_start: u64,
        plaintext_end: u64,
        source_size: u64,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        if nonce.len() != NONCE_SIZE {
            return Err(EncryptionError::Decryption(format!(
                "Invalid nonce length: expected {}, got {}",
                NONCE_SIZE,
                nonce.len()
            )));
        }

        if plaintext_start >= plaintext_end {
            return Err(EncryptionError::Decryption(format!(
                "Invalid range: start ({}) >= end ({})",
                plaintext_start, plaintext_end
            )));
        }
        if plaintext_end > source_size {
            return Err(EncryptionError::Decryption(format!(
                "Invalid range: end ({plaintext_end}) > source size ({source_size})"
            )));
        }

        let base_nonce: [u8; NONCE_SIZE] = nonce
            .try_into()
            .map_err(|_| EncryptionError::Decryption("Invalid nonce".to_string()))?;

        let key = self.key_bytes();
        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&key));

        let start_chunk = plaintext_start / CHUNK_SIZE as u64;
        let end_chunk = (plaintext_end.saturating_sub(1)) / CHUNK_SIZE as u64;
        let total_chunks = chunk_count_for_plaintext_len(source_size);

        let mut plaintext = Vec::new();

        for absolute_chunk_idx in start_chunk..=end_chunk {
            // Convert absolute chunk index to position in encrypted_chunks
            let relative_idx = absolute_chunk_idx - first_chunk_index;
            let chunk_start = (relative_idx as usize) * ENCRYPTED_CHUNK_SIZE;

            // Handle last chunk which may be smaller
            let chunk_end = if chunk_start + ENCRYPTED_CHUNK_SIZE > encrypted_chunks.len() {
                encrypted_chunks.len()
            } else {
                chunk_start + ENCRYPTED_CHUNK_SIZE
            };

            if chunk_start >= encrypted_chunks.len() {
                return Err(EncryptionError::Decryption(format!(
                    "Chunk {} not in provided data (first_chunk_index={})",
                    absolute_chunk_idx, first_chunk_index
                )));
            }

            let chunk_data = &encrypted_chunks[chunk_start..chunk_end];
            let decrypted = decrypt_chunk_with_cipher(
                &cipher,
                &base_nonce,
                aad_context,
                absolute_chunk_idx,
                total_chunks,
                chunk_data,
            )
            .map_err(|_| {
                EncryptionError::Decryption(format!(
                    "Authentication failed for chunk {}",
                    absolute_chunk_idx
                ))
            })?;

            plaintext.extend(decrypted);
        }

        // Slice to exact range within the decrypted chunks
        let offset_in_first_chunk = (plaintext_start % CHUNK_SIZE as u64) as usize;
        let len = (plaintext_end - plaintext_start) as usize;
        let end = offset_in_first_chunk + len;

        if end > plaintext.len() {
            return Err(EncryptionError::Decryption(format!(
                "Decrypted data too short: need {} bytes, got {}",
                end,
                plaintext.len()
            )));
        }

        Ok(plaintext[offset_in_first_chunk..end].to_vec())
    }

    /// Derive a scoped encryption service.
    ///
    /// Uses HKDF: master_key + "coven-scope-v1:{scope_id}" -> 32-byte key.
    /// Deterministic: same master + scope_id always gives the same key.
    pub fn derive_scoped(&self, scope_id: &str) -> EncryptionService {
        let derived = self.derive_key(&format!("coven-scope-v1:{scope_id}"));
        EncryptionService::from_key_at_generation(self.current_generation(), derived)
    }

    pub fn derive_scoped_for_fingerprint(
        &self,
        fingerprint: &[u8; 8],
        scope_id: &str,
    ) -> Result<EncryptionService, EncryptionError> {
        let key = self.key_for_fingerprint(fingerprint)?;
        let generation = self
            .keys
            .get(fingerprint)
            .expect("just resolved")
            .generation;
        let derived = derive_key_from(&key, &format!("coven-scope-v1:{scope_id}"));
        Ok(EncryptionService::from_key_at_generation(
            generation, derived,
        ))
    }

    /// Derive a 32-byte key using HKDF-SHA256 with the given info label.
    ///
    /// The derivation is deterministic: same master key + same info string always
    /// produces the same derived key.
    ///
    /// - Salt: the constant `"coven-hkdf-salt-v1"` (RFC 5869 permits a fixed,
    ///   non-secret salt)
    /// - IKM: master key
    /// - Info: caller-provided label
    pub fn derive_key(&self, info: &str) -> [u8; 32] {
        derive_key_from(&self.key_bytes(), info)
    }

    /// Seal `plaintext` for storage in a host's own rows, under this keyring's
    /// seal key:
    ///
    /// ```text
    /// [0]     version = APP_DATA_SEAL_VERSION
    /// [1..9]  the seal key's 8-byte fingerprint
    /// [9..]   the chunked ciphertext `encrypt` produces under that key
    /// ```
    ///
    /// Naming the key by fingerprint is what keeps the payload openable across
    /// any number of later rotations and forks — [`Self::open_app_data`] resolves
    /// whichever key the payload names, not the current one, and a key once held
    /// is never dropped. `aad` binds the ciphertext to its context (the owning
    /// row's primary key, say) and must be presented unchanged to open it.
    ///
    /// The body is the existing chunked format, so a large payload streams the
    /// same way a blob does; there is no size cliff and no second cipher.
    pub fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
        let fingerprint = self.seal_fingerprint();
        let mut sealed = Vec::with_capacity(
            APP_DATA_HEADER_SIZE + chunked_encrypted_len(plaintext.len() as u64) as usize,
        );
        sealed.push(APP_DATA_SEAL_VERSION);
        sealed.extend_from_slice(&fingerprint);
        sealed.extend(self.encrypt(plaintext, aad));
        sealed
    }

    /// Open a payload [`Self::seal_app_data`] produced, under whichever key it
    /// names — so a keyring that has rotated or merged a fork since still opens
    /// everything it sealed before. A version this build does not read, or a key
    /// this keyring does not hold, is a typed error; a wrong `aad` or a tampered
    /// payload surfaces the AEAD failure through [`SealError::Crypto`].
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        let (fingerprint, ciphertext) = split_sealed_app_data(sealed)?;
        self.service_for_fingerprint(&fingerprint)
            // `service_for_fingerprint` fails only when the keyring holds no key
            // with that fingerprint, so this names the cause exactly.
            .map_err(|_| SealError::UnknownKey(hex::encode(fingerprint)))?
            .decrypt(ciphertext, aad)
            .map_err(SealError::Crypto)
    }
}

/// Split a sealed app-data payload into the key fingerprint it names and its
/// ciphertext body, refusing a version this build does not read.
///
/// A payload too short to hold the fixed header cannot name a version or a
/// fingerprint, so it is a corrupt envelope — reported as a decryption failure
/// rather than guessed at or padded.
fn split_sealed_app_data(sealed: &[u8]) -> Result<([u8; 8], &[u8]), SealError> {
    let (&version, rest) = sealed.split_first().ok_or_else(|| {
        SealError::Crypto(EncryptionError::Decryption(
            "sealed app-data payload is empty".to_string(),
        ))
    })?;
    if version != APP_DATA_SEAL_VERSION {
        return Err(SealError::UnknownVersion(version));
    }
    if rest.len() < APP_DATA_FINGERPRINT_SIZE {
        return Err(SealError::Crypto(EncryptionError::Decryption(
            "sealed app-data payload is truncated before its key fingerprint".to_string(),
        )));
    }
    let (fingerprint, ciphertext) = rest.split_at(APP_DATA_FINGERPRINT_SIZE);
    let fingerprint: [u8; 8] = fingerprint
        .try_into()
        .expect("split_at yields exactly APP_DATA_FINGERPRINT_SIZE bytes");
    Ok((fingerprint, ciphertext))
}

fn derive_key_from(key: &[u8; 32], info: &str) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(b"coven-hkdf-salt-v1"), key);
    let mut okm = [0u8; 32];
    hk.expand(info.as_bytes(), &mut okm)
        .expect("32 bytes is a valid HKDF output length");
    okm
}

#[derive(Clone, Copy)]
struct EncryptedChunkLayout {
    data_len: usize,
    total_chunks: usize,
    has_partial: bool,
}

impl EncryptedChunkLayout {
    fn chunk_bounds(
        self,
        ciphertext_len: usize,
        chunk_index: usize,
    ) -> Result<(usize, usize), EncryptionError> {
        if chunk_index >= self.total_chunks {
            return Err(EncryptionError::Decryption(format!(
                "Chunk index {} out of range (total chunks: {})",
                chunk_index, self.total_chunks
            )));
        }

        let chunk_start = NONCE_SIZE + chunk_index * ENCRYPTED_CHUNK_SIZE;
        let chunk_end = if chunk_index == self.total_chunks - 1 && self.has_partial {
            ciphertext_len
        } else {
            chunk_start + ENCRYPTED_CHUNK_SIZE
        };
        Ok((chunk_start, chunk_end))
    }
}

fn read_base_nonce(ciphertext: &[u8]) -> Result<[u8; NONCE_SIZE], EncryptionError> {
    if ciphertext.len() < NONCE_SIZE {
        return Err(EncryptionError::Decryption(
            "Ciphertext too short for nonce".to_string(),
        ));
    }

    let mut base_nonce = [0u8; NONCE_SIZE];
    base_nonce.copy_from_slice(&ciphertext[..NONCE_SIZE]);
    Ok(base_nonce)
}

fn encrypted_chunk_layout(ciphertext_len: usize) -> Result<EncryptedChunkLayout, EncryptionError> {
    if ciphertext_len < NONCE_SIZE {
        return Err(EncryptionError::Decryption(
            "Ciphertext too short for nonce".to_string(),
        ));
    }

    let data_len = ciphertext_len - NONCE_SIZE;
    let num_full_chunks = data_len / ENCRYPTED_CHUNK_SIZE;
    let has_partial = !data_len.is_multiple_of(ENCRYPTED_CHUNK_SIZE);
    let total_chunks = num_full_chunks + usize::from(has_partial);
    Ok(EncryptedChunkLayout {
        data_len,
        total_chunks,
        has_partial,
    })
}

fn decrypted_len_upper_bound(encrypted_data_len: usize) -> usize {
    let chunk_count = encrypted_data_len.div_ceil(ENCRYPTED_CHUNK_SIZE);
    encrypted_data_len.saturating_sub(chunk_count * TAG_SIZE)
}

fn chunk_count_for_plaintext_len(plaintext_len: u64) -> u64 {
    plaintext_len.div_ceil(CHUNK_SIZE as u64).max(1)
}

fn chunk_aad(aad_context: &[u8], chunk_index: u64, total_chunks: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AEAD_V2_LABEL.len() + 8 + aad_context.len() + 16);
    aad.extend_from_slice(AEAD_V2_LABEL);
    aad.extend_from_slice(&(aad_context.len() as u64).to_le_bytes());
    aad.extend_from_slice(aad_context);
    aad.extend_from_slice(&chunk_index.to_le_bytes());
    aad.extend_from_slice(&total_chunks.to_le_bytes());
    aad
}

fn decrypt_chunk_with_cipher(
    cipher: &XChaCha20Poly1305,
    base_nonce: &[u8; NONCE_SIZE],
    aad_context: &[u8],
    chunk_index: u64,
    total_chunks: u64,
    chunk_data: &[u8],
) -> Result<Vec<u8>, ()> {
    let nonce = chunk_nonce(base_nonce, chunk_index);
    let nonce_arr = GenericArray::from_slice(&nonce);
    let aad = chunk_aad(aad_context, chunk_index, total_chunks);
    cipher
        .decrypt(
            nonce_arr,
            Payload {
                msg: chunk_data,
                aad: &aad,
            },
        )
        .map_err(|_| ())
}

/// Derive nonce for chunk i: base_nonce XOR i (little-endian)
fn chunk_nonce(base_nonce: &[u8; NONCE_SIZE], chunk_index: u64) -> [u8; NONCE_SIZE] {
    let mut nonce = *base_nonce;
    let index_bytes = chunk_index.to_le_bytes();
    for i in 0..8 {
        nonce[i] ^= index_bytes[i];
    }
    nonce
}

/// Calculate the encrypted byte range for a plaintext byte range.
///
/// Returns `(chunk_start, chunk_end)` - the byte positions in the encrypted file
/// where the needed chunks are located. Does NOT include the nonce (first 24 bytes).
///
/// Use this for efficient range requests: fetch nonce separately (or from DB),
/// then fetch just `chunk_start..chunk_end` from storage.
pub fn encrypted_chunk_range(plaintext_start: u64, plaintext_end: u64) -> (u64, u64) {
    let start_chunk = plaintext_start / CHUNK_SIZE as u64;
    let end_chunk = (plaintext_end.saturating_sub(1)) / CHUNK_SIZE as u64;

    let chunk_start = NONCE_SIZE as u64 + start_chunk * ENCRYPTED_CHUNK_SIZE as u64;
    let chunk_end = NONCE_SIZE as u64 + (end_chunk + 1) * ENCRYPTED_CHUNK_SIZE as u64;

    (chunk_start, chunk_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_AAD: &[u8] = b"encryption-test-context";

    fn test_key() -> [u8; 32] {
        // Fixed test key for reproducibility
        [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]
    }

    fn create_test_service() -> EncryptionService {
        EncryptionService::from_key(test_key())
    }

    fn decrypt_plaintext_range(
        service: &EncryptionService,
        full_ciphertext: &[u8],
        source_size: u64,
        plaintext_start: u64,
        plaintext_end: u64,
    ) -> Vec<u8> {
        let nonce = &full_ciphertext[..NONCE_SIZE];
        let (chunk_start, chunk_end) = encrypted_chunk_range(plaintext_start, plaintext_end);
        let chunks_only = &full_ciphertext[chunk_start as usize..chunk_end as usize];
        let first_chunk_index = (chunk_start - NONCE_SIZE as u64) / ENCRYPTED_CHUNK_SIZE as u64;
        service
            .decrypt_range_with_offset(
                nonce,
                chunks_only,
                first_chunk_index,
                plaintext_start,
                plaintext_end,
                source_size,
                TEST_AAD,
            )
            .unwrap()
    }

    #[test]
    fn test_roundtrip_small() {
        let service = create_test_service();
        let plaintext = b"Hello, world!";

        let ciphertext = service.encrypt(plaintext, TEST_AAD);
        let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    /// The streaming sealer (base nonce + per-chunk `seal_chunk`) produces a blob
    /// the existing whole-buffer decryptor reads back unchanged, across the
    /// boundaries that matter: empty, sub-chunk, exact chunk, and several
    /// non-aligned chunks. `encrypt` is built on the sealer, so this also
    /// guards the streaming form against drifting from the stored format.
    #[test]
    fn streaming_sealer_matches_whole_buffer_format() {
        let service = create_test_service();
        for len in [
            0usize,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            200_000,
        ] {
            let plaintext: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();

            // Seal incrementally, exactly as a streaming upload would.
            let mut sealer = service.sealer(plaintext.len() as u64, TEST_AAD);
            let mut streamed = sealer.base_nonce().to_vec();
            if plaintext.is_empty() {
                streamed.extend(sealer.seal_chunk(&[]));
            } else {
                for chunk in plaintext.chunks(CHUNK_SIZE) {
                    streamed.extend(sealer.seal_chunk(chunk));
                }
            }

            assert_eq!(
                streamed.len() as u64,
                chunked_encrypted_len(len as u64),
                "predicted length wrong for len={len}"
            );
            assert_eq!(
                service.decrypt(&streamed, TEST_AAD).unwrap(),
                plaintext,
                "streamed ciphertext failed to round-trip for len={len}"
            );
        }
    }

    /// `chunked_encrypted_len` predicts the exact byte length `encrypt`
    /// produces, across the chunk boundaries that matter — so a streaming upload
    /// can announce the final object size before sealing a byte.
    #[test]
    fn chunked_encrypted_len_matches_encrypt() {
        let service = create_test_service();
        for n in [
            0usize,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            200_000,
        ] {
            let produced = service.encrypt(&vec![0u8; n], TEST_AAD).len() as u64;
            assert_eq!(
                chunked_encrypted_len(n as u64),
                produced,
                "predicted length wrong for n={n}"
            );
        }
    }

    #[test]
    fn test_roundtrip_exact_chunk() {
        let service = create_test_service();
        let plaintext = vec![0x42u8; CHUNK_SIZE];

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);
        let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_roundtrip_multiple_chunks() {
        let service = create_test_service();
        // 2.5 chunks worth of data
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 2 + CHUNK_SIZE / 2)
            .map(|i| (i % 256) as u8)
            .collect();

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);
        let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_random_access_chunk() {
        let service = create_test_service();
        // 3 chunks: chunk 0 = 0x00, chunk 1 = 0x11, chunk 2 = 0x22
        let mut plaintext = vec![0x00u8; CHUNK_SIZE];
        plaintext.extend(vec![0x11u8; CHUNK_SIZE]);
        plaintext.extend(vec![0x22u8; CHUNK_SIZE]);

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);

        // Decrypt only chunk 1 (middle chunk)
        let chunk1 = service.decrypt_chunk(&ciphertext, 1, TEST_AAD).unwrap();
        assert_eq!(chunk1, vec![0x11u8; CHUNK_SIZE]);

        // Decrypt chunk 0
        let chunk0 = service.decrypt_chunk(&ciphertext, 0, TEST_AAD).unwrap();
        assert_eq!(chunk0, vec![0x00u8; CHUNK_SIZE]);

        // Decrypt chunk 2
        let chunk2 = service.decrypt_chunk(&ciphertext, 2, TEST_AAD).unwrap();
        assert_eq!(chunk2, vec![0x22u8; CHUNK_SIZE]);
    }

    #[test]
    fn test_random_access_partial_last_chunk() {
        let service = create_test_service();
        // 1 full chunk + partial chunk
        let mut plaintext = vec![0xAAu8; CHUNK_SIZE];
        plaintext.extend(vec![0xBBu8; 100]);

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);

        let chunk0 = service.decrypt_chunk(&ciphertext, 0, TEST_AAD).unwrap();
        assert_eq!(chunk0, vec![0xAAu8; CHUNK_SIZE]);

        let chunk1 = service.decrypt_chunk(&ciphertext, 1, TEST_AAD).unwrap();
        assert_eq!(chunk1, vec![0xBBu8; 100]);
    }

    #[test]
    fn test_tamper_detection() {
        let service = create_test_service();
        let plaintext = b"Secret data";

        let mut ciphertext = service.encrypt(plaintext, TEST_AAD);

        // Tamper with the ciphertext (after nonce)
        let tamper_pos = NONCE_SIZE + 5;
        ciphertext[tamper_pos] ^= 0xFF;

        let result = service.decrypt(&ciphertext, TEST_AAD);
        assert!(result.is_err());
    }

    #[test]
    fn truncating_trailing_chunks_fails_to_decrypt() {
        let service = create_test_service();
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 3).map(|i| (i % 251) as u8).collect();
        let ciphertext = service.encrypt(&plaintext, TEST_AAD);
        let truncated = &ciphertext[..ciphertext.len() - ENCRYPTED_CHUNK_SIZE];

        assert!(
            service.decrypt(truncated, TEST_AAD).is_err(),
            "a truncated multi-chunk object must fail, not return a short plaintext",
        );
    }

    #[test]
    fn test_empty_plaintext() {
        let service = create_test_service();
        let plaintext = b"";

        let ciphertext = service.encrypt(plaintext, TEST_AAD);

        // Should just be nonce + auth tag
        assert_eq!(ciphertext.len(), NONCE_SIZE + TAG_SIZE);

        let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_single_byte() {
        let service = create_test_service();
        let plaintext = b"x";

        let ciphertext = service.encrypt(plaintext, TEST_AAD);
        let decrypted = service.decrypt(&ciphertext, TEST_AAD).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypted_range_single_chunk() {
        // Plaintext bytes 0-100 are in chunk 0
        let (start, end) = encrypted_chunk_range(0, 100);

        assert_eq!(start, NONCE_SIZE as u64);
        assert_eq!(end, NONCE_SIZE as u64 + ENCRYPTED_CHUNK_SIZE as u64);
    }

    #[test]
    fn test_encrypted_range_spans_chunks() {
        // Plaintext bytes spanning chunk 0 and chunk 1
        let (start, end) = encrypted_chunk_range(CHUNK_SIZE as u64 - 10, CHUNK_SIZE as u64 + 10);

        assert_eq!(start, NONCE_SIZE as u64);
        assert_eq!(end, NONCE_SIZE as u64 + 2 * ENCRYPTED_CHUNK_SIZE as u64);
    }

    #[test]
    fn test_encrypted_range_middle_chunk() {
        // Plaintext bytes entirely within chunk 2
        let chunk2_start = CHUNK_SIZE as u64 * 2;
        let (start, end) = encrypted_chunk_range(chunk2_start + 10, chunk2_start + 100);

        assert_eq!(start, NONCE_SIZE as u64 + 2 * ENCRYPTED_CHUNK_SIZE as u64);
        assert_eq!(end, NONCE_SIZE as u64 + 3 * ENCRYPTED_CHUNK_SIZE as u64);
    }

    #[test]
    fn test_different_encryptions_different_ciphertext() {
        let service = create_test_service();
        let plaintext = b"Same message";

        let ciphertext1 = service.encrypt(plaintext, TEST_AAD);
        let ciphertext2 = service.encrypt(plaintext, TEST_AAD);

        // Different nonces = different ciphertext
        assert_ne!(ciphertext1, ciphertext2);

        // Both decrypt to same plaintext
        assert_eq!(service.decrypt(&ciphertext1, TEST_AAD).unwrap(), plaintext);
        assert_eq!(service.decrypt(&ciphertext2, TEST_AAD).unwrap(), plaintext);
    }

    #[test]
    fn test_chunk_index_out_of_range() {
        let service = create_test_service();
        let plaintext = vec![0u8; CHUNK_SIZE]; // Exactly 1 chunk

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);

        // Chunk 0 should work
        assert!(service.decrypt_chunk(&ciphertext, 0, TEST_AAD).is_ok());

        // Chunk 1 should fail
        assert!(service.decrypt_chunk(&ciphertext, 1, TEST_AAD).is_err());
    }

    #[test]
    fn test_decrypt_range_within_single_chunk() {
        let service = create_test_service();
        // Create plaintext with recognizable pattern
        let plaintext: Vec<u8> = (0..CHUNK_SIZE).map(|i| (i % 256) as u8).collect();

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);

        let decrypted =
            decrypt_plaintext_range(&service, &ciphertext, plaintext.len() as u64, 100, 200);

        assert_eq!(decrypted.len(), 100);
        assert_eq!(decrypted, plaintext[100..200]);
    }

    #[test]
    fn test_decrypt_range_spanning_chunks() {
        let service = create_test_service();
        // 3 chunks of data
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 3).map(|i| (i % 256) as u8).collect();

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);

        // Range spanning from end of chunk 0 into chunk 1
        let start = CHUNK_SIZE as u64 - 100;
        let end = CHUNK_SIZE as u64 + 100;
        let decrypted =
            decrypt_plaintext_range(&service, &ciphertext, plaintext.len() as u64, start, end);

        assert_eq!(decrypted.len(), 200);
        assert_eq!(decrypted, &plaintext[start as usize..end as usize]);
    }

    #[test]
    fn test_decrypt_range_entire_middle_chunk() {
        let service = create_test_service();
        // 3 chunks, middle chunk filled with 0xBB
        let mut plaintext = vec![0xAAu8; CHUNK_SIZE];
        plaintext.extend(vec![0xBBu8; CHUNK_SIZE]);
        plaintext.extend(vec![0xCCu8; CHUNK_SIZE]);

        let ciphertext = service.encrypt(&plaintext, TEST_AAD);

        // Decrypt just the middle chunk
        let start = CHUNK_SIZE as u64;
        let end = (CHUNK_SIZE * 2) as u64;
        let decrypted =
            decrypt_plaintext_range(&service, &ciphertext, plaintext.len() as u64, start, end);

        assert_eq!(decrypted, vec![0xBBu8; CHUNK_SIZE]);
    }

    #[test]
    fn test_decrypt_range_with_partial_encrypted_data() {
        let service = create_test_service();
        // Create 3-chunk plaintext
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let full_ciphertext = service.encrypt(&plaintext, TEST_AAD);

        // Calculate encrypted range for plaintext bytes in chunk 1
        let plaintext_start = CHUNK_SIZE as u64 + 100;
        let plaintext_end = CHUNK_SIZE as u64 + 200;
        let nonce = &full_ciphertext[..NONCE_SIZE];
        let (chunk_start, chunk_end) = encrypted_chunk_range(plaintext_start, plaintext_end);
        let chunks_only = &full_ciphertext[chunk_start as usize..chunk_end as usize];
        let first_chunk_index = (chunk_start - NONCE_SIZE as u64) / ENCRYPTED_CHUNK_SIZE as u64;
        let decrypted = service
            .decrypt_range_with_offset(
                nonce,
                chunks_only,
                first_chunk_index,
                plaintext_start,
                plaintext_end,
                plaintext.len() as u64,
                TEST_AAD,
            )
            .unwrap();

        assert_eq!(decrypted.len(), 100);
        assert_eq!(
            decrypted,
            &plaintext[plaintext_start as usize..plaintext_end as usize]
        );
    }

    #[test]
    fn test_encrypted_chunk_range_returns_actual_bounds() {
        // For plaintext in chunk 5, should return just chunk 5's encrypted bytes
        // NOT starting from 0
        let chunk5_start = CHUNK_SIZE as u64 * 5;
        let chunk5_end = chunk5_start + 1000;

        let (enc_start, enc_end) = encrypted_chunk_range(chunk5_start, chunk5_end);

        // Should start at chunk 5's position, not 0
        let expected_start = NONCE_SIZE as u64 + 5 * ENCRYPTED_CHUNK_SIZE as u64;
        let expected_end = NONCE_SIZE as u64 + 6 * ENCRYPTED_CHUNK_SIZE as u64;

        assert_eq!(
            enc_start, expected_start,
            "encrypted_chunk_range should return actual chunk start, not 0"
        );
        assert_eq!(enc_end, expected_end);
    }

    #[test]
    fn test_encrypted_chunk_range_spanning_multiple_chunks() {
        // Range spanning chunks 3-5
        let start = CHUNK_SIZE as u64 * 3 + 100;
        let end = CHUNK_SIZE as u64 * 5 + 500;

        let (enc_start, enc_end) = encrypted_chunk_range(start, end);

        let expected_start = NONCE_SIZE as u64 + 3 * ENCRYPTED_CHUNK_SIZE as u64;
        let expected_end = NONCE_SIZE as u64 + 6 * ENCRYPTED_CHUNK_SIZE as u64;

        assert_eq!(enc_start, expected_start);
        assert_eq!(enc_end, expected_end);
    }

    #[test]
    fn test_decrypt_range_with_separate_nonce() {
        // This simulates production flow: nonce from DB + chunks from range request
        let service = create_test_service();

        // Create 10-chunk plaintext with recognizable pattern
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 10).map(|i| (i % 256) as u8).collect();
        let full_ciphertext = service.encrypt(&plaintext, TEST_AAD);

        // Extract nonce (this would come from DB in production)
        let nonce = &full_ciphertext[..NONCE_SIZE];

        // We want plaintext bytes in chunk 7
        let plaintext_start = CHUNK_SIZE as u64 * 7 + 100;
        let plaintext_end = CHUNK_SIZE as u64 * 7 + 500;

        // Get the encrypted chunk range (NOT starting from 0)
        let (chunk_start, chunk_end) = encrypted_chunk_range(plaintext_start, plaintext_end);

        // Fetch just the needed chunks (simulating range request)
        let chunks_only = &full_ciphertext[chunk_start as usize..chunk_end as usize];

        // First chunk index is 7 (the chunk our range starts in)
        let first_chunk_index = plaintext_start / CHUNK_SIZE as u64;

        // Use the new method that handles offset chunks
        let decrypted = service
            .decrypt_range_with_offset(
                nonce,
                chunks_only,
                first_chunk_index,
                plaintext_start,
                plaintext_end,
                plaintext.len() as u64,
                TEST_AAD,
            )
            .unwrap();

        assert_eq!(decrypted.len(), 400);
        assert_eq!(
            decrypted,
            &plaintext[plaintext_start as usize..plaintext_end as usize]
        );
    }

    #[test]
    fn test_decrypt_range_with_offset_spanning_chunks() {
        // Test decrypting a range that spans multiple chunks
        let service = create_test_service();

        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 10).map(|i| (i % 256) as u8).collect();
        let full_ciphertext = service.encrypt(&plaintext, TEST_AAD);
        let nonce = &full_ciphertext[..NONCE_SIZE];

        // Range spanning chunks 3, 4, 5
        let plaintext_start = CHUNK_SIZE as u64 * 3 + 1000;
        let plaintext_end = CHUNK_SIZE as u64 * 5 + 2000;

        let (chunk_start, chunk_end) = encrypted_chunk_range(plaintext_start, plaintext_end);
        let chunks_only = &full_ciphertext[chunk_start as usize..chunk_end as usize];
        let first_chunk_index = plaintext_start / CHUNK_SIZE as u64;

        let decrypted = service
            .decrypt_range_with_offset(
                nonce,
                chunks_only,
                first_chunk_index,
                plaintext_start,
                plaintext_end,
                plaintext.len() as u64,
                TEST_AAD,
            )
            .unwrap();

        let expected_len = (plaintext_end - plaintext_start) as usize;
        assert_eq!(decrypted.len(), expected_len);
        assert_eq!(
            decrypted,
            &plaintext[plaintext_start as usize..plaintext_end as usize]
        );
    }

    #[test]
    fn test_fingerprint_deterministic() {
        let service = create_test_service();
        assert_eq!(service.fingerprint(), service.fingerprint());
    }

    #[test]
    fn test_fingerprint_different_keys() {
        let service1 = EncryptionService::from_key([0u8; 32]);
        let service2 = EncryptionService::from_key([1u8; 32]);
        assert_ne!(service1.fingerprint(), service2.fingerprint());
    }

    #[test]
    fn key_fingerprint_wire_form_is_strict_lowercase_hex() {
        let fingerprint = create_test_service().seal_key_fingerprint();
        let serialized = serde_json::to_string(&fingerprint).expect("serialize fingerprint");
        assert_eq!(
            serde_json::from_str::<KeyFingerprint>(&serialized).unwrap(),
            fingerprint
        );
        assert!(fingerprint
            .to_string()
            .to_uppercase()
            .parse::<KeyFingerprint>()
            .is_err());
    }

    #[test]
    fn derive_scoped_deterministic() {
        let service = create_test_service();
        let derived1 = service.derive_scoped("rel-123");
        let derived2 = service.derive_scoped("rel-123");
        assert_eq!(derived1.key_bytes(), derived2.key_bytes());
    }

    #[test]
    fn derive_scoped_different_releases() {
        let service = create_test_service();
        let key_a = service.derive_scoped("rel-aaa").key_bytes();
        let key_b = service.derive_scoped("rel-bbb").key_bytes();
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn derive_scoped_different_master_keys() {
        let svc1 = EncryptionService::from_key([0u8; 32]);
        let svc2 = EncryptionService::from_key([1u8; 32]);
        let key1 = svc1.derive_scoped("rel-123").key_bytes();
        let key2 = svc2.derive_scoped("rel-123").key_bytes();
        assert_ne!(key1, key2);
    }

    #[test]
    fn derive_scoped_roundtrip() {
        let master = create_test_service();
        let release_enc = master.derive_scoped("rel-456");
        let plaintext = b"test audio data for this release";

        let encrypted = release_enc.encrypt(plaintext, TEST_AAD);
        let decrypted = release_enc.decrypt(&encrypted, TEST_AAD).unwrap();
        assert_eq!(decrypted, plaintext);

        // Cannot decrypt with master key
        assert!(master.decrypt(&encrypted, TEST_AAD).is_err());

        // Cannot decrypt with wrong release key
        let wrong_enc = master.derive_scoped("rel-999");
        assert!(wrong_enc.decrypt(&encrypted, TEST_AAD).is_err());
    }

    #[test]
    fn master_keyring_from_serialized_accepts_the_current_keyring_format() {
        let keyring = MasterKeyring::generate();
        let serialized = keyring.to_serialized();
        let parsed =
            MasterKeyring::from_serialized(&serialized).expect("parse a generated keyring");
        assert_eq!(parsed.to_serialized(), serialized);
        assert_eq!(parsed.fingerprint(), keyring.fingerprint());
    }

    #[test]
    fn master_keyring_from_serialized_rejects_raw_hex() {
        let raw_hex = hex::encode(test_key());
        assert!(MasterKeyring::from_serialized(&raw_hex).is_err());
    }

    #[test]
    fn keyring_payload_requires_the_current_json_format() {
        let service = create_test_service()
            .with_appended_generation(2, [9u8; 32])
            .expect("append a generation");
        let payload = service
            .to_keyring_payload()
            .expect("serialize the current keyring payload");
        let parsed = EncryptionService::from_keyring_payload(payload)
            .expect("parse the current keyring payload");

        assert_eq!(parsed.keyring_entries(), service.keyring_entries());
        assert!(EncryptionService::from_keyring_payload(test_key().to_vec()).is_err());
    }

    #[test]
    fn master_keyring_and_encryption_service_convert_without_losing_generations() {
        let service = EncryptionService::from_key(test_key())
            .with_appended_generation(2, [9u8; 32])
            .expect("append a generation");
        let keyring: MasterKeyring = service.clone().into();
        assert_eq!(keyring.fingerprint(), service.fingerprint());
        assert_eq!(
            keyring.to_serialized(),
            service.to_keyring_string().unwrap()
        );

        let round_tripped: EncryptionService = keyring.into();
        assert_eq!(round_tripped.current_generation(), 2);
        assert_eq!(round_tripped.keyring_entries(), service.keyring_entries(),);
    }

    /// Two owners rotating at once mint two distinct keys at the SAME generation
    /// number. A keyring keyed on the generation number would keep only one of
    /// them; keyed on fingerprint, both coexist. Every device that folds in the
    /// union then selects the same seal key (highest generation, then greatest
    /// fingerprint), so a fork converges instead of partitioning — and because
    /// merge keeps every key, each side still opens data sealed under the other's.
    #[test]
    fn same_generation_fork_converges_on_one_seal_key_and_keeps_both() {
        let base = EncryptionService::from_key([1u8; 32]);
        let fork_a = base.with_appended_generation(2, [0xA0u8; 32]).unwrap();
        let fork_b = base.with_appended_generation(2, [0xB0u8; 32]).unwrap();

        let a_then_b = fork_a.merged_with(&fork_b);
        let b_then_a = fork_b.merged_with(&fork_a);
        assert_eq!(
            a_then_b.fingerprint(),
            b_then_a.fingerprint(),
            "seal selection is order-independent, so both sides converge on one key",
        );
        assert_eq!(
            a_then_b.key_count(),
            3,
            "the base key and both forks are held"
        );
        assert_eq!(a_then_b.current_generation(), 2);

        let sealed_a = fork_a.seal_app_data(b"from owner A", b"ctx");
        let sealed_b = fork_b.seal_app_data(b"from owner B", b"ctx");
        assert_eq!(
            a_then_b.open_app_data(&sealed_a, b"ctx").unwrap(),
            b"from owner A",
        );
        assert_eq!(
            a_then_b.open_app_data(&sealed_b, b"ctx").unwrap(),
            b"from owner B",
        );
    }

    #[test]
    fn master_keyring_debug_redacts_keys() {
        let keyring = MasterKeyring::generate();
        let debug = format!("{keyring:?}");
        assert!(debug.contains("<redacted>"), "{debug}");
    }

    // =========================================================================
    // App-data sealing
    // =========================================================================

    /// What the pinned v1 fixture wraps: this payload sealed under [`test_key`]
    /// with this `aad`. The bytes are
    /// `[01][63 0d cd 29 66 c4 33 66][24-byte nonce][ciphertext ++ tag]` — the
    /// version, [`test_key`]'s 8-byte fingerprint, then the chunked ciphertext.
    const APP_DATA_V1_FIXTURE_PLAINTEXT: &[u8] = b"pinned app-data payload";
    const APP_DATA_V1_FIXTURE_AAD: &[u8] = b"pinned-app-data-context";
    const APP_DATA_V1_FIXTURE_HEX: &str = concat!(
        "01",
        "630dcd2966c43366",
        "2bdfe10d13cb397b648c2eb352bbadd92a19eafd8499b5c5",
        "b0d1e8eb56f757621ec41a78488c937427aac5df38b5e8af",
        "2b2b8c9155ead15242e0c87b00bbe8",
    );

    /// The key fingerprint a sealed payload names, read straight out of its
    /// header — so the tests below assert the recorded key rather than trusting
    /// `open_app_data` to have picked the right one silently.
    fn sealed_fingerprint(sealed: &[u8]) -> [u8; 8] {
        sealed[1..9].try_into().expect("a sealed header")
    }

    #[test]
    fn seal_app_data_round_trips_and_records_its_version_and_key() {
        let service = create_test_service();
        for payload in [b"".as_slice(), b"x", b"a longer app-data secret value"] {
            let sealed = service.seal_app_data(payload, TEST_AAD);

            assert_eq!(sealed[0], APP_DATA_SEAL_VERSION, "the version byte leads");
            assert_eq!(
                sealed_fingerprint(&sealed),
                service.seal_fingerprint(),
                "the header names the key it sealed under",
            );
            assert_eq!(
                sealed.len(),
                APP_DATA_HEADER_SIZE + chunked_encrypted_len(payload.len() as u64) as usize,
                "the body is exactly the chunked ciphertext, behind the fixed header",
            );
            assert_eq!(service.open_app_data(&sealed, TEST_AAD).unwrap(), payload);
        }
    }

    /// `aad` binds a payload to its context. Opening with a different one must
    /// fail, so a payload lifted into another row does not silently open there.
    #[test]
    fn open_app_data_rejects_a_different_aad() {
        let service = create_test_service();
        let sealed = service.seal_app_data(b"bound to row 42", b"row-42");

        let error = service
            .open_app_data(&sealed, b"row-99")
            .expect_err("a different aad must not open the payload");

        assert!(matches!(error, SealError::Crypto(_)), "{error:?}");
    }

    #[test]
    fn open_app_data_rejects_a_flipped_ciphertext_byte() {
        let service = create_test_service();
        let mut sealed = service.seal_app_data(b"tamper with me", TEST_AAD);
        let last = sealed.len() - 1;
        sealed[last] ^= 0xFF;

        let error = service
            .open_app_data(&sealed, TEST_AAD)
            .expect_err("a tampered payload must fail authentication");

        assert!(matches!(error, SealError::Crypto(_)), "{error:?}");
    }

    /// A version this build does not read is refused by name, never guessed at
    /// — the payload was written by a format we have no decoder for.
    #[test]
    fn open_app_data_rejects_an_unknown_version() {
        let service = create_test_service();
        let mut sealed = service.seal_app_data(b"a version-1 payload", TEST_AAD);
        sealed[0] = 2;

        let error = service
            .open_app_data(&sealed, TEST_AAD)
            .expect_err("version 2 must be refused");

        assert!(matches!(error, SealError::UnknownVersion(2)), "{error:?}");
    }

    /// Rotation does not orphan already-sealed payloads. Each records the key it
    /// was sealed under by fingerprint, and a rotated keyring retains every
    /// earlier key, so it opens what it sealed before and after.
    #[test]
    fn open_app_data_survives_rotation_and_each_payload_names_its_key() {
        let before_rotation = create_test_service();
        let sealed_under_1 = before_rotation.seal_app_data(b"sealed before rotating", TEST_AAD);

        let after_rotation = before_rotation
            .with_appended_generation(2, [9u8; 32])
            .expect("rotate the keyring");
        let sealed_under_2 = after_rotation.seal_app_data(b"sealed after rotating", TEST_AAD);

        assert_eq!(
            sealed_fingerprint(&sealed_under_1),
            before_rotation.seal_fingerprint(),
        );
        assert_eq!(
            sealed_fingerprint(&sealed_under_2),
            after_rotation.seal_fingerprint(),
            "sealing after a rotation records the new seal key",
        );

        assert_eq!(
            after_rotation
                .open_app_data(&sealed_under_1, TEST_AAD)
                .unwrap(),
            b"sealed before rotating",
            "the rotated keyring still opens what the old generation sealed",
        );
        assert_eq!(
            after_rotation
                .open_app_data(&sealed_under_2, TEST_AAD)
                .unwrap(),
            b"sealed after rotating",
        );
    }

    /// A keyring that does not hold the key a payload names — it predates the
    /// payload, or the payload is foreign — is a typed error, not a panic and
    /// not a decrypt attempt under the wrong key.
    #[test]
    fn open_app_data_rejects_a_key_the_keyring_lacks() {
        let rotated = create_test_service()
            .with_appended_generation(2, [9u8; 32])
            .expect("rotate the keyring");
        let sealed_under_2 = rotated.seal_app_data(b"sealed under the rotated key", TEST_AAD);

        let fresh_single_key = EncryptionService::from_key([7u8; 32]);
        let error = fresh_single_key
            .open_app_data(&sealed_under_2, TEST_AAD)
            .expect_err("a keyring without the sealing key must not open it");

        assert!(matches!(error, SealError::UnknownKey(_)), "{error:?}");
    }

    /// The sealed app-data format is a durable storage contract: a host's rows
    /// hold these bytes, so a build that stopped opening them would strand the
    /// data. This pins one payload sealed under [`test_key`] — if the version
    /// byte, the generation encoding, the chunk framing, or the AAD derivation
    /// ever changes, this stops opening and says so.
    ///
    /// Generated once from `seal_app_data` itself, then frozen. It is not
    /// re-derived at test time on purpose: a fixture that regenerates would
    /// still pass against a changed format and pin nothing.
    #[test]
    fn sealed_app_data_v1_fixture_still_opens() {
        let sealed = hex::decode(APP_DATA_V1_FIXTURE_HEX).expect("the fixture is valid hex");

        assert_eq!(sealed[0], APP_DATA_SEAL_VERSION, "a version-1 payload");
        assert_eq!(
            sealed_fingerprint(&sealed),
            EncryptionService::from_key(test_key()).seal_fingerprint(),
        );

        let opened = EncryptionService::from_key(test_key())
            .open_app_data(&sealed, APP_DATA_V1_FIXTURE_AAD)
            .expect("the pinned v1 payload must keep opening");

        assert_eq!(opened, APP_DATA_V1_FIXTURE_PLAINTEXT);
    }
}
