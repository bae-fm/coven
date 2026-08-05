use std::collections::BTreeMap;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
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
pub(crate) const NONCE_SIZE: usize = 24;

/// Poly1305 auth tag size (16 bytes).
pub(crate) const TAG_SIZE: usize = 16;

/// 64KB plaintext chunks
pub const CHUNK_SIZE: usize = 65536;
/// Each encrypted chunk: plaintext + 16-byte auth tag
pub(crate) const ENCRYPTED_CHUNK_SIZE: usize = CHUNK_SIZE + TAG_SIZE;
pub(crate) const INITIAL_KEY_GENERATION: u64 = 1;
const AEAD_V2_LABEL: &[u8] = b"coven-aead-v2";

/// The sealed app-data format version this build writes, and the only one it
/// reads. The leading byte of every payload [`EncryptionService::seal_app_data`]
/// produces; a payload naming any other version is refused
/// ([`SealError::UnknownVersion`]) rather than guessed at.
pub(crate) const APP_DATA_SEAL_VERSION: u8 = 1;

/// The fixed header every sealed app-data payload carries ahead of its
/// ciphertext: the version byte, then the sealing key's full SHA-256 digest.
const APP_DATA_FINGERPRINT_SIZE: usize = 32;
const APP_DATA_HEADER_SIZE: usize = 1 + APP_DATA_FINGERPRINT_SIZE;

/// Stable wire identity of one 32-byte encryption key: its full SHA-256 digest,
/// serialized as exactly 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyFingerprint([u8; 32]);

impl KeyFingerprint {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
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
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(KeyFingerprintParseError(value.to_string()));
        }
        let bytes: [u8; 32] = hex::decode(value)
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
#[error("key fingerprint must be exactly 64 lowercase hexadecimal characters: {0:?}")]
pub struct KeyFingerprintParseError(String);

/// Generate a random 32-byte key.
pub(crate) fn generate_random_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    key
}

/// The length of the encrypted blob [`EncryptionService::encrypt`] produces for
/// a plaintext of `plaintext_len` bytes: the base nonce, the plaintext itself,
/// and one 16-byte tag per chunk. An empty plaintext still produces one
/// (tag-only) chunk. Lets a streaming upload know the final object size up
/// front, before a byte is sealed.
pub(crate) fn chunked_encrypted_len(plaintext_len: u64) -> u64 {
    NONCE_SIZE as u64
        + plaintext_len
        + chunk_count_for_plaintext_len(plaintext_len) * TAG_SIZE as u64
}

/// Incremental encryptor for the whole-object format [`EncryptionService::encrypt`]
/// produces — a protocol object or a host's sealed app data. Emits
/// `[base_nonce][chunk_0][chunk_1]...` one chunk at a time so a large payload is
/// never held whole twice.
///
/// This format is read whole, always: it carries no header describing its own
/// framing, so a reader needs every chunk before it knows anything. Blobs, which
/// are read by range, use [`SealedBlobSealer`] instead.
struct ChunkSealer {
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

    /// The base nonce — the first [`NONCE_SIZE`] bytes of the payload, emitted
    /// before any chunk.
    fn base_nonce(&self) -> [u8; NONCE_SIZE] {
        self.base_nonce
    }

    /// Seal one plaintext chunk (at most [`CHUNK_SIZE`] bytes) into its
    /// ciphertext-plus-tag, advancing the chunk counter. A chunk longer than
    /// `CHUNK_SIZE` would desync the framing the decryptor expects, so the caller
    /// must split the plaintext on `CHUNK_SIZE` boundaries.
    fn seal_chunk(&mut self, plaintext: &[u8]) -> Vec<u8> {
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

/// The sealed-blob format version this build writes, and the only one it reads.
/// The leading byte of every blob header; a blob naming any other version is
/// refused rather than guessed at.
pub(crate) const SEALED_BLOB_VERSION: u8 = 1;

/// The chunk size a blob is sealed at when the host configures none. A read
/// honors whatever its own header records, so this is only ever the *writer's*
/// choice and can change without touching a blob already stored.
pub(crate) const DEFAULT_BLOB_CHUNK_SIZE: NonZeroU32 = NonZeroU32::new(64 * 1024).expect("64 KiB");

/// `[version: 1][chunk_size: 4 LE][plaintext_len: 8 LE]` — the fixed header
/// every sealed blob carries ahead of its first chunk.
pub(crate) const SEALED_BLOB_HEADER_LEN: usize = 1 + 4 + 8;

const BLOB_AEAD_LABEL: &[u8] = b"coven-blob-aead-v1";
const BLOB_NONCE_INFO: &[u8] = b"coven-blob-nonce-v1";

/// What a sealed blob's header says about its own layout: the chunk size it was
/// sealed at and the plaintext length it covers. Every other offset in the
/// object is arithmetic over these two numbers, so a blob describes its own
/// shape and nothing per-chunk is stored.
///
/// The header travels in the clear (a reader must know the chunk size before it
/// can open anything) but is bound into every chunk's AAD, so altering it makes
/// the first chunk fail to open rather than silently re-framing the object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealedBlobHeader {
    chunk_size: NonZeroU32,
    plaintext_len: u64,
}

/// Why a sealed blob's header or one of its chunks could not be opened.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum SealedBlobError {
    #[error("sealed blob header names version {0}, which this build does not read")]
    UnknownVersion(u8),
    #[error("sealed blob header is {0} bytes, expected {SEALED_BLOB_HEADER_LEN}")]
    ShortHeader(usize),
    #[error("sealed blob header names chunk size 0")]
    ZeroChunkSize,
    #[error("sealed blob chunk {index} is {actual} bytes, expected {expected}")]
    ChunkLength {
        index: u64,
        expected: usize,
        actual: usize,
    },
    #[error("sealed blob chunk {index} failed authentication")]
    ChunkAuthentication { index: u64 },
    #[error("sealed blob range {start}..{end} lies outside its {plaintext_len}-byte plaintext")]
    RangeOutOfBounds {
        start: u64,
        end: u64,
        plaintext_len: u64,
    },
}

impl SealedBlobHeader {
    /// Describe a blob of `plaintext_len` bytes sealed at `chunk_size`.
    pub fn new(chunk_size: NonZeroU32, plaintext_len: u64) -> Self {
        Self {
            chunk_size,
            plaintext_len,
        }
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, SealedBlobError> {
        if bytes.len() < SEALED_BLOB_HEADER_LEN {
            return Err(SealedBlobError::ShortHeader(bytes.len()));
        }
        if bytes[0] != SEALED_BLOB_VERSION {
            return Err(SealedBlobError::UnknownVersion(bytes[0]));
        }
        let chunk_size = NonZeroU32::new(u32::from_le_bytes(
            bytes[1..5].try_into().expect("four header bytes"),
        ))
        .ok_or(SealedBlobError::ZeroChunkSize)?;
        let plaintext_len =
            u64::from_le_bytes(bytes[5..13].try_into().expect("eight header bytes"));
        Ok(Self::new(chunk_size, plaintext_len))
    }

    pub fn to_bytes(self) -> [u8; SEALED_BLOB_HEADER_LEN] {
        let mut bytes = [0u8; SEALED_BLOB_HEADER_LEN];
        bytes[0] = SEALED_BLOB_VERSION;
        bytes[1..5].copy_from_slice(&self.chunk_size.get().to_le_bytes());
        bytes[5..13].copy_from_slice(&self.plaintext_len.to_le_bytes());
        bytes
    }

    pub fn chunk_size(self) -> NonZeroU32 {
        self.chunk_size
    }

    pub fn plaintext_len(self) -> u64 {
        self.plaintext_len
    }

    /// Chunk `index`'s AAD: the format label, this complete header, the blob's
    /// context, and the index. Binding the header makes a rewritten chunk size
    /// or plaintext length fail the first open; binding the context and index
    /// makes a chunk refuse to open as a different blob's chunk, or as a
    /// different position in its own.
    fn chunk_aad(self, aad_context: &[u8], index: u64) -> Vec<u8> {
        let mut aad = Vec::with_capacity(
            BLOB_AEAD_LABEL.len() + SEALED_BLOB_HEADER_LEN + 16 + aad_context.len(),
        );
        aad.extend_from_slice(BLOB_AEAD_LABEL);
        aad.extend_from_slice(&self.to_bytes());
        aad.extend_from_slice(&(aad_context.len() as u64).to_le_bytes());
        aad.extend_from_slice(aad_context);
        aad.extend_from_slice(&index.to_le_bytes());
        aad
    }

    /// How many chunks the plaintext occupies. An empty blob still seals one
    /// tag-only chunk, so opening it authenticates its emptiness rather than
    /// trusting a zero-length object.
    pub fn chunk_count(self) -> u64 {
        self.plaintext_len
            .div_ceil(u64::from(self.chunk_size.get()))
            .max(1)
    }

    /// The plaintext length chunk `index` carries — the chunk size for every
    /// chunk but the last, which holds the remainder.
    pub(crate) fn chunk_plaintext_len(self, index: u64) -> u64 {
        let start = index.saturating_mul(u64::from(self.chunk_size.get()));
        self.plaintext_len
            .saturating_sub(start)
            .min(u64::from(self.chunk_size.get()))
    }

    /// The sealed length of chunk `index`: its plaintext plus one tag.
    pub fn sealed_chunk_len(self, index: u64) -> u64 {
        self.chunk_plaintext_len(index) + TAG_SIZE as u64
    }

    /// The whole sealed body: the header followed by every chunk. What a
    /// streaming upload declares as its length before a byte is sealed.
    pub fn sealed_len(self) -> u64 {
        SEALED_BLOB_HEADER_LEN as u64 + self.plaintext_len + self.chunk_count() * TAG_SIZE as u64
    }

    /// The chunks covering plaintext `start..end`. A caller reads exactly these
    /// and no others; the range must lie inside the plaintext.
    pub fn covering_chunks(
        self,
        start: u64,
        end: u64,
    ) -> Result<std::ops::Range<u64>, SealedBlobError> {
        if start > end || end > self.plaintext_len {
            return Err(SealedBlobError::RangeOutOfBounds {
                start,
                end,
                plaintext_len: self.plaintext_len,
            });
        }
        if start == end {
            return Ok(0..0);
        }
        let chunk_size = u64::from(self.chunk_size.get());
        Ok((start / chunk_size)..((end - 1) / chunk_size + 1))
    }

    /// Where `chunks` sit in the sealed object, as an offset span measured from
    /// the object's first byte. The header is included in the offset, so this is
    /// what a ranged cloud read asks for verbatim.
    pub fn sealed_span(self, chunks: std::ops::Range<u64>) -> std::ops::Range<u64> {
        let full = u64::from(self.chunk_size.get()) + TAG_SIZE as u64;
        let start = SEALED_BLOB_HEADER_LEN as u64 + chunks.start * full;
        let mut end = start;
        for index in chunks {
            end += self.sealed_chunk_len(index);
        }
        start..end
    }

    /// Split `chunks` into the runs one ranged read each should fetch: as many
    /// chunks as fit in `window` stored bytes, and never fewer than one (a chunk
    /// wider than the window still takes exactly one request). The window is how
    /// a reader trades round-trips against per-request size without changing what
    /// bytes it asks for — the union of the runs is always exactly `chunks`.
    pub fn request_runs(
        self,
        chunks: std::ops::Range<u64>,
        window: NonZeroU64,
    ) -> Vec<std::ops::Range<u64>> {
        let mut runs = Vec::new();
        let mut start = chunks.start;
        while start < chunks.end {
            let mut end = start;
            let mut span = 0u64;
            while end < chunks.end {
                let next = span.saturating_add(self.sealed_chunk_len(end));
                if end > start && next > window.get() {
                    break;
                }
                span = next;
                end += 1;
            }
            runs.push(start..end);
            start = end;
        }
        runs
    }

    /// Where `chunks` sit in the plaintext.
    pub fn plaintext_span(self, chunks: std::ops::Range<u64>) -> std::ops::Range<u64> {
        let chunk_size = u64::from(self.chunk_size.get());
        let start = (chunks.start * chunk_size).min(self.plaintext_len);
        let end = (chunks.end * chunk_size).min(self.plaintext_len);
        start..end
    }
}

/// The per-blob nonce base: HKDF over the sealing key, bound to the blob's
/// context. Chunk `n` uses this base XOR `n`, so no nonce is ever stored.
///
/// # Invariant: blob addressing must keep covering blob content
///
/// Nonce separation rests entirely on `aad_context` differing whenever the
/// plaintext does. It holds today because the context is minted from the blob's
/// semantic key, which for an opaque blob is `{namespace}/opaque/{locator_hash}`,
/// and the locator hash covers the plaintext hash — so two different plaintexts
/// cannot share a base without a SHA-256 collision. Re-sealing identical bytes
/// under an identical locator reproduces an identical base, which is fine: it
/// reproduces identical ciphertext, not a second message under one nonce.
///
/// **Any change to how a blob is addressed must preserve that.** A locator that
/// stopped folding in the plaintext hash, or a context minted from something
/// that outlives a blob's content (a bare row id, a stable path), would let one
/// key seal two different plaintexts under one nonce — which for
/// XChaCha20-Poly1305 leaks the plaintexts' XOR and forfeits authentication.
/// That failure is silent: everything still encrypts, decrypts, and round-trips.
fn blob_nonce_base(key: &[u8; 32], aad_context: &[u8]) -> [u8; NONCE_SIZE] {
    let mut info = Vec::with_capacity(BLOB_NONCE_INFO.len() + aad_context.len());
    info.extend_from_slice(BLOB_NONCE_INFO);
    info.extend_from_slice(aad_context);
    let hk = Hkdf::<Sha256>::new(Some(b"coven-hkdf-salt-v1"), key);
    let mut base = [0u8; NONCE_SIZE];
    hk.expand(&info, &mut base)
        .expect("24 bytes is a valid HKDF output length");
    base
}

/// Seals one blob's chunks in order, so an upload streams without ever holding
/// the whole plaintext or ciphertext. The header it emits first is what a later
/// read needs to compute every chunk offset.
pub struct SealedBlobSealer {
    cipher: XChaCha20Poly1305,
    base_nonce: [u8; NONCE_SIZE],
    header: SealedBlobHeader,
    aad_context: Vec<u8>,
    next_index: u64,
}

impl SealedBlobSealer {
    fn new(key: &[u8; 32], header: SealedBlobHeader, aad_context: &[u8]) -> Self {
        Self {
            cipher: XChaCha20Poly1305::new(GenericArray::from_slice(key)),
            base_nonce: blob_nonce_base(key, aad_context),
            header,
            aad_context: aad_context.to_vec(),
            next_index: 0,
        }
    }

    pub fn header(&self) -> SealedBlobHeader {
        self.header
    }

    /// Seal the next chunk. The caller splits the plaintext on the header's
    /// chunk boundaries; a chunk of any other length would desync the framing
    /// every reader computes from the header.
    pub fn seal_chunk(&mut self, plaintext: &[u8]) -> Vec<u8> {
        let index = self.next_index;
        debug_assert_eq!(
            plaintext.len() as u64,
            self.header.chunk_plaintext_len(index),
            "a sealed chunk carries exactly the plaintext its header assigns it",
        );
        self.next_index += 1;
        let nonce = chunk_nonce(&self.base_nonce, index);
        let aad = self.header.chunk_aad(&self.aad_context, index);
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

/// Opens a sealed blob's chunks in any order. A chunk that opens is authentic —
/// the tag covers its bytes, its position, and the header that framed it — so
/// decryption is the whole verification and no separate hash is read.
pub struct SealedBlobOpener {
    cipher: XChaCha20Poly1305,
    base_nonce: [u8; NONCE_SIZE],
    header: SealedBlobHeader,
    aad_context: Vec<u8>,
}

impl SealedBlobOpener {
    fn new(key: &[u8; 32], header: SealedBlobHeader, aad_context: &[u8]) -> Self {
        Self {
            cipher: XChaCha20Poly1305::new(GenericArray::from_slice(key)),
            base_nonce: blob_nonce_base(key, aad_context),
            header,
            aad_context: aad_context.to_vec(),
        }
    }

    pub fn header(&self) -> SealedBlobHeader {
        self.header
    }

    /// Open chunk `index` from exactly its sealed bytes. A length the header
    /// does not assign that index is refused before the cipher runs — the bytes
    /// are not that chunk, whatever they authenticate as.
    pub fn open_chunk(&self, index: u64, sealed: &[u8]) -> Result<Vec<u8>, SealedBlobError> {
        let expected = self.header.sealed_chunk_len(index);
        if sealed.len() as u64 != expected {
            return Err(SealedBlobError::ChunkLength {
                index,
                expected: expected as usize,
                actual: sealed.len(),
            });
        }
        let nonce = chunk_nonce(&self.base_nonce, index);
        let aad = self.header.chunk_aad(&self.aad_context, index);
        self.cipher
            .decrypt(
                GenericArray::from_slice(&nonce),
                Payload {
                    msg: sealed,
                    aad: &aad,
                },
            )
            .map_err(|_| SealedBlobError::ChunkAuthentication { index })
    }

    /// Open every chunk in `chunks` from the contiguous sealed bytes covering
    /// them — the span [`SealedBlobHeader::sealed_span`] names for the same
    /// range — and return their whole plaintext. Chunks are opened one at a
    /// time, so a tampered chunk fails only the reads that touch it.
    pub fn open_chunks(
        &self,
        chunks: std::ops::Range<u64>,
        sealed: &[u8],
    ) -> Result<Vec<u8>, SealedBlobError> {
        let window = self.header.plaintext_span(chunks.clone());
        let mut plaintext = Vec::with_capacity((window.end - window.start) as usize);
        let mut offset = 0usize;
        for index in chunks {
            let len = self.header.sealed_chunk_len(index) as usize;
            let sealed_chunk = sealed.get(offset..offset + len).ok_or({
                SealedBlobError::ChunkLength {
                    index,
                    expected: len,
                    actual: sealed.len().saturating_sub(offset),
                }
            })?;
            offset += len;
            plaintext.extend(self.open_chunk(index, sealed_chunk)?);
        }
        Ok(plaintext)
    }
}

#[derive(Error, Debug)]
pub enum EncryptionError {
    #[error("Decryption failed: {0}")]
    Decryption(String),
    #[error("Key management error: {0}")]
    KeyManagement(String),
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
#[derive(Clone, PartialEq, Eq)]
struct KeyEntry {
    generation: u64,
    key: [u8; 32],
}

/// A store's master key material: every key it holds. This is the value custody
/// implementations store, unlock, and re-protect — never a cipher. coven builds
/// the [`EncryptionService`] cipher from it internally; custody never touches
/// cipher machinery.
#[derive(Clone)]
pub struct MasterKeyring(EncryptionService);

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
        Self(EncryptionService::from_key(generate_random_key()))
    }

    /// Serialize to the stored keyring JSON — the same format
    /// [`EncryptionService::to_keyring_string`] produces, since every
    /// generation this type holds came from (or feeds) that cipher.
    pub fn to_serialized(&self) -> String {
        self.0
            .to_keyring_string()
            .expect("a MasterKeyring always holds at least one generation")
    }

    /// Parse the stored master-key format [`Self::to_serialized`] produces.
    pub fn from_serialized(s: &str) -> Result<Self, EncryptionError> {
        EncryptionService::new(s).map(Self)
    }

    /// SHA-256 fingerprint of the seal key (the deterministically selected
    /// key this keyring seals new data under), hex-encoded in full.
    pub fn fingerprint(&self) -> String {
        self.0.fingerprint()
    }
}

impl From<EncryptionService> for MasterKeyring {
    fn from(service: EncryptionService) -> Self {
        Self(service)
    }
}

impl From<MasterKeyring> for EncryptionService {
    fn from(keyring: MasterKeyring) -> Self {
        keyring.0
    }
}

/// The full SHA-256 digest of a key. A keyring entry's identity, and what a
/// sealed object names to say which key sealed it.
fn key_fingerprint(key: &[u8; 32]) -> KeyFingerprint {
    KeyFingerprint::from_bytes(Sha256::digest(key).into())
}

fn insert_key_entry(
    keys: &mut BTreeMap<KeyFingerprint, KeyEntry>,
    fingerprint: KeyFingerprint,
    entry: KeyEntry,
) -> Result<(), EncryptionError> {
    match keys.get(&fingerprint) {
        None => {
            keys.insert(fingerprint, entry);
            Ok(())
        }
        Some(existing) if existing == &entry => Ok(()),
        Some(existing) => Err(EncryptionError::KeyManagement(format!(
            "key fingerprint {fingerprint} identifies conflicting entries at generations {} and {}",
            existing.generation, entry.generation,
        ))),
    }
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
    keys: BTreeMap<KeyFingerprint, KeyEntry>,
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
        keys.insert(key_fingerprint(&key), KeyEntry { generation, key });
        EncryptionService { keys }
    }

    pub fn from_keyring(
        keys: impl IntoIterator<Item = (u64, [u8; 32])>,
    ) -> Result<Self, EncryptionError> {
        let mut keyring = BTreeMap::new();
        for (generation, key) in keys {
            insert_key_entry(
                &mut keyring,
                key_fingerprint(&key),
                KeyEntry { generation, key },
            )?;
        }
        if keyring.is_empty() {
            return Err(EncryptionError::KeyManagement(
                "keyring has no keys".to_string(),
            ));
        }
        Ok(EncryptionService { keys: keyring })
    }

    /// The keyring entry this device seals new data under, chosen
    /// deterministically fleet-wide: the highest generation number, and among
    /// keys sharing that generation, the greatest fingerprint. Once the wraps of
    /// a fork propagate so every device holds both keys, they all pick the same
    /// one here — a fork converges instead of partitioning.
    fn seal_entry(&self) -> (&KeyFingerprint, &KeyEntry) {
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

    /// Union this keyring with `other`: every distinct key either holds.
    /// Identical entries deduplicate. The same fingerprint naming different key
    /// bytes or generations is invalid rather than silently choosing one entry.
    pub fn merged_with(
        &self,
        other: &EncryptionService,
    ) -> Result<EncryptionService, EncryptionError> {
        let mut keys = self.keys.clone();
        for (fingerprint, entry) in &other.keys {
            insert_key_entry(&mut keys, *fingerprint, entry.clone())?;
        }
        Ok(EncryptionService { keys })
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
    pub(crate) fn key_for_fingerprint(
        &self,
        fingerprint: &[u8; 32],
    ) -> Result<[u8; 32], EncryptionError> {
        let fingerprint = KeyFingerprint::from_bytes(*fingerprint);
        self.keys.get(&fingerprint).map(|e| e.key).ok_or_else(|| {
            EncryptionError::KeyManagement(format!("no key with fingerprint {fingerprint}"))
        })
    }

    pub fn service_for_fingerprint(
        &self,
        fingerprint: &[u8; 32],
    ) -> Result<EncryptionService, EncryptionError> {
        let key = self.key_for_fingerprint(fingerprint)?;
        // The single-key service keeps the source key's generation so its own
        // seal choices and any re-serialization stay consistent with the keyring
        // it came from.
        let generation = self
            .keys
            .get(&KeyFingerprint::from_bytes(*fingerprint))
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
        insert_key_entry(
            &mut keys,
            key_fingerprint(&key),
            KeyEntry { generation, key },
        )?;
        Ok(EncryptionService { keys })
    }

    /// Full SHA-256 fingerprint of the seal key, hex-encoded.
    pub fn fingerprint(&self) -> String {
        hex::encode(self.seal_fingerprint())
    }

    /// The seal key's full SHA-256 fingerprint — what a sealed object records
    /// so a later read resolves the exact key, whatever the keyring has become.
    pub fn seal_fingerprint(&self) -> [u8; 32] {
        *self.seal_entry().0.as_bytes()
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
            let nonce = chunk_nonce(&base_nonce, chunk_index as u64);
            let aad = chunk_aad(aad_context, chunk_index as u64, layout.total_chunks as u64);
            let decrypted = cipher
                .decrypt(
                    GenericArray::from_slice(&nonce),
                    Payload {
                        msg: chunk_data,
                        aad: &aad,
                    },
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

    /// A streaming sealer over this service's key. Only [`Self::encrypt`] uses
    /// it: a whole-object payload is sealed chunk by chunk so a large one never
    /// sits in memory twice, but it is always read whole.
    fn sealer(&self, plaintext_len: u64, aad_context: &[u8]) -> ChunkSealer {
        ChunkSealer::new(&self.key_bytes(), plaintext_len, aad_context)
    }

    /// A sealer for one blob, framed by `header`. The blob namespace's format:
    /// the header travels in the clear ahead of the chunks, every chunk's nonce
    /// is derived rather than stored, and the AAD binds the header, the blob's
    /// context, and the chunk index.
    pub fn blob_sealer(&self, header: SealedBlobHeader, aad_context: &[u8]) -> SealedBlobSealer {
        SealedBlobSealer::new(&self.key_bytes(), header, aad_context)
    }

    /// The opener for a blob whose header has been read. Random access: any
    /// chunk opens without the ones before it.
    pub fn blob_opener(&self, header: SealedBlobHeader, aad_context: &[u8]) -> SealedBlobOpener {
        SealedBlobOpener::new(&self.key_bytes(), header, aad_context)
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
        fingerprint: &[u8; 32],
        scope_id: &str,
    ) -> Result<EncryptionService, EncryptionError> {
        let key = self.key_for_fingerprint(fingerprint)?;
        let generation = self
            .keys
            .get(&KeyFingerprint::from_bytes(*fingerprint))
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
    /// [1..33] the seal key's full SHA-256 fingerprint
    /// [33..]  the chunked ciphertext `encrypt` produces under that key
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
fn split_sealed_app_data(sealed: &[u8]) -> Result<([u8; 32], &[u8]), SealError> {
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
    let fingerprint: [u8; 32] = fingerprint
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

/// Derive nonce for chunk i: base_nonce XOR i (little-endian)
fn chunk_nonce(base_nonce: &[u8; NONCE_SIZE], chunk_index: u64) -> [u8; NONCE_SIZE] {
    let mut nonce = *base_nonce;
    let index_bytes = chunk_index.to_le_bytes();
    for i in 0..8 {
        nonce[i] ^= index_bytes[i];
    }
    nonce
}

#[cfg(test)]
#[path = "encryption_tests.rs"]
mod tests;
