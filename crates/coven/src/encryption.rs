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
pub struct MasterKeyring {
    keys: BTreeMap<KeyFingerprint, KeyEntry>,
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
    /// key this keyring seals new data under), hex-encoded in full.
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
    fn key_fingerprint_is_the_full_sha256_digest() {
        let fingerprint = create_test_service().seal_key_fingerprint();
        let expected: [u8; 32] = Sha256::digest(test_key()).into();

        assert_eq!(fingerprint.as_bytes().as_slice(), expected.as_slice());
        assert_eq!(fingerprint.to_string(), hex::encode(expected));
        assert_eq!(fingerprint.to_string().len(), 64);
        assert!("630dcd2966c43366".parse::<KeyFingerprint>().is_err());
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

        let a_then_b = fork_a.merged_with(&fork_b).unwrap();
        let b_then_a = fork_b.merged_with(&fork_a).unwrap();
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
    fn keyring_construction_rejects_one_key_at_conflicting_generations() {
        let key = [0x44u8; 32];

        assert!(EncryptionService::from_keyring([(1, key), (2, key)]).is_err());
    }

    #[test]
    fn appending_a_generation_rejects_an_existing_key_fingerprint() {
        let key = [0x55u8; 32];
        let keyring = EncryptionService::from_key_at_generation(1, key);

        assert!(keyring.with_appended_generation(2, key).is_err());
    }

    #[test]
    fn merging_rejects_one_key_at_conflicting_generations() {
        let key = [0x66u8; 32];
        let generation_one = EncryptionService::from_key_at_generation(1, key);
        let generation_two = EncryptionService::from_key_at_generation(2, key);

        assert!(generation_one.merged_with(&generation_two).is_err());
    }

    #[test]
    fn identical_duplicate_keyring_entries_are_deduplicated() {
        let key = [0x77u8; 32];
        let keyring =
            EncryptionService::from_keyring([(1, key), (1, key)]).expect("identical entries");

        assert_eq!(keyring.key_count(), 1);
    }

    #[test]
    fn merging_identical_keyring_entries_deduplicates_them() {
        let key = [0x88u8; 32];
        let left = EncryptionService::from_key_at_generation(1, key);
        let right = EncryptionService::from_key_at_generation(1, key);
        let merged = left.merged_with(&right).expect("identical entries");

        assert_eq!(merged.key_count(), 1);
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
    /// `[01][32-byte SHA-256 digest][24-byte nonce][ciphertext ++ tag]` — the
    /// version, [`test_key`]'s full fingerprint, then the chunked ciphertext.
    const APP_DATA_V1_FIXTURE_PLAINTEXT: &[u8] = b"pinned app-data payload";
    const APP_DATA_V1_FIXTURE_AAD: &[u8] = b"pinned-app-data-context";
    const APP_DATA_V1_FIXTURE_HEX: &str = concat!(
        "01",
        "630dcd2966c4336691125448bbb25b4ff412a49c732db2c8abc1b8581bd710dd",
        "2bdfe10d13cb397b648c2eb352bbadd92a19eafd8499b5c5",
        "b0d1e8eb56f757621ec41a78488c937427aac5df38b5e8af",
        "2b2b8c9155ead15242e0c87b00bbe8",
    );

    /// The key fingerprint a sealed payload names, read straight out of its
    /// header — so the tests below assert the recorded key rather than trusting
    /// `open_app_data` to have picked the right one silently.
    fn sealed_fingerprint(sealed: &[u8]) -> [u8; 32] {
        sealed[1..33].try_into().expect("a sealed header")
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

    #[test]
    fn sealed_app_data_header_carries_the_full_key_digest() {
        let service = create_test_service();
        let plaintext = b"full fingerprint header";
        let sealed = service.seal_app_data(plaintext, TEST_AAD);
        let expected: [u8; 32] = Sha256::digest(test_key()).into();

        assert_eq!(&sealed[1..33], expected.as_slice());
        assert_eq!(
            sealed.len(),
            33 + chunked_encrypted_len(plaintext.len() as u64) as usize
        );
        assert_eq!(service.open_app_data(&sealed, TEST_AAD).unwrap(), plaintext);
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
    fn sealed_app_data_v1_fixture_opens() {
        let sealed = hex::decode(APP_DATA_V1_FIXTURE_HEX).expect("the fixture is valid hex");

        assert_eq!(sealed[0], APP_DATA_SEAL_VERSION, "a version-1 payload");
        assert_eq!(
            sealed_fingerprint(&sealed),
            EncryptionService::from_key(test_key()).seal_fingerprint(),
        );

        let opened = EncryptionService::from_key(test_key())
            .open_app_data(&sealed, APP_DATA_V1_FIXTURE_AAD)
            .expect("the pinned v1 payload opens");

        assert_eq!(opened, APP_DATA_V1_FIXTURE_PLAINTEXT);
    }
}
