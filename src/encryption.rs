use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::{aead::Aead, KeyInit, XChaCha20Poly1305};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
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

/// Generate a random 32-byte key.
pub fn generate_random_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    key
}

/// The length of the encrypted blob [`EncryptionService::encrypt_chunked`]
/// produces for a plaintext of `plaintext_len` bytes: the base nonce, the
/// plaintext itself, and one 16-byte tag per chunk. An empty plaintext still
/// produces one (tag-only) chunk. Lets a streaming upload know the final object
/// size up front, before a byte is sealed.
pub fn chunked_encrypted_len(plaintext_len: u64) -> u64 {
    let chunks = plaintext_len.div_ceil(CHUNK_SIZE as u64).max(1);
    NONCE_SIZE as u64 + plaintext_len + chunks * TAG_SIZE as u64
}

/// Incremental encryptor that emits the same `[base_nonce][chunk_0][chunk_1]...`
/// bytes as [`EncryptionService::encrypt_chunked`], one chunk at a time, so a
/// large blob is sealed and uploaded without ever holding the whole plaintext or
/// ciphertext in memory. `encrypt_chunked` is itself implemented on top of this,
/// so the streaming and whole-buffer forms cannot drift.
pub struct ChunkSealer {
    cipher: XChaCha20Poly1305,
    base_nonce: [u8; NONCE_SIZE],
    next_index: u64,
}

impl ChunkSealer {
    /// Start a sealer with a fresh random base nonce.
    fn new(key: &[u8; 32]) -> Self {
        let mut base_nonce = [0u8; NONCE_SIZE];
        rand::rng().fill_bytes(&mut base_nonce);
        Self {
            cipher: XChaCha20Poly1305::new(GenericArray::from_slice(key)),
            base_nonce,
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
        self.next_index += 1;
        self.cipher
            .encrypt(GenericArray::from_slice(&nonce), plaintext)
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
/// Manages encryption keys and provides XChaCha20-Poly1305 encryption/decryption
///
/// This implements the security model described in the README:
/// - Files are encrypted using XChaCha20-Poly1305 for authenticated encryption
/// - Chunked format enables random-access decryption for efficient range reads
#[derive(Clone)]
pub struct EncryptionService {
    key: [u8; 32],
}
impl std::fmt::Debug for EncryptionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionService")
            .field("cipher", &"<initialized>")
            .finish()
    }
}
impl EncryptionService {
    /// Create a new encryption service from a hex-encoded key string
    pub fn new(key_hex: &str) -> Result<Self, EncryptionError> {
        info!("Loading master key...");
        let key_bytes = hex::decode(key_hex)
            .map_err(|e| EncryptionError::KeyManagement(format!("Invalid key format: {}", e)))?;
        if key_bytes.len() != 32 {
            return Err(EncryptionError::KeyManagement(
                "Invalid key length, expected 32 bytes".to_string(),
            ));
        }
        let key: [u8; 32] = key_bytes.try_into().map_err(|_| {
            EncryptionError::KeyManagement("Failed to convert key bytes to array".to_string())
        })?;
        Ok(EncryptionService { key })
    }

    /// Create a new encryption service from a raw 32-byte key.
    pub fn from_key(key: [u8; 32]) -> Self {
        EncryptionService { key }
    }

    /// SHA-256 fingerprint of the key, first 8 bytes hex-encoded (16 hex chars).
    /// Short enough to display in UI, long enough to detect wrong keys.
    pub fn fingerprint(&self) -> String {
        let hash = Sha256::digest(self.key);
        hex::encode(&hash[..8])
    }

    /// Create an encryption service with a raw key (for testing)
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_with_key(key_bytes: &[u8]) -> Self {
        if key_bytes.len() != 32 {
            panic!("Invalid key length, expected 32 bytes");
        }
        let key: [u8; 32] = key_bytes.try_into().unwrap();
        EncryptionService { key }
    }

    /// Return the raw 32-byte key.
    pub fn key_bytes(&self) -> [u8; 32] {
        self.key
    }

    /// Encrypt data using chunked XChaCha20-Poly1305 format.
    /// Returns: [base_nonce: 24 bytes][ciphertext with auth tags]
    /// For small data (single chunk), this is equivalent to standard AEAD.
    /// For large data, each chunk is independently encrypted for random-access.
    pub fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> {
        self.encrypt_chunked(plaintext)
    }

    /// Decrypt data in chunked format: [nonce (24 bytes)][ciphertext chunks...]
    pub fn decrypt(&self, encrypted_data: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.decrypt_chunked(encrypted_data)
    }

    /// Encrypt data using chunked XChaCha20-Poly1305 format.
    /// Returns: `[base_nonce: 24 bytes][chunk_0][chunk_1]...`
    /// Each chunk is independently encrypted, enabling random-access decryption.
    pub fn encrypt_chunked(&self, plaintext: &[u8]) -> Vec<u8> {
        let mut sealer = self.sealer();
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

    /// A streaming sealer over this service's key, for encrypting a blob
    /// chunk-by-chunk straight into an upload. See [`ChunkSealer`].
    pub fn sealer(&self) -> ChunkSealer {
        ChunkSealer::new(&self.key)
    }

    /// Decrypt a specific chunk from chunked encrypted data.
    /// Enables random-access decryption without reading preceding chunks.
    pub fn decrypt_chunk(
        &self,
        ciphertext: &[u8],
        chunk_index: usize,
    ) -> Result<Vec<u8>, EncryptionError> {
        if ciphertext.len() < NONCE_SIZE {
            return Err(EncryptionError::Decryption(
                "Ciphertext too short for nonce".to_string(),
            ));
        }

        let base_nonce: [u8; NONCE_SIZE] = ciphertext[..NONCE_SIZE]
            .try_into()
            .map_err(|_| EncryptionError::Decryption("Invalid nonce".to_string()))?;

        let data_start = NONCE_SIZE;
        let total_data_len = ciphertext.len() - data_start;

        // Calculate chunk boundaries
        let num_full_chunks = total_data_len / ENCRYPTED_CHUNK_SIZE;
        let has_partial = !total_data_len.is_multiple_of(ENCRYPTED_CHUNK_SIZE);
        let total_chunks = num_full_chunks + if has_partial { 1 } else { 0 };

        if chunk_index >= total_chunks {
            return Err(EncryptionError::Decryption(format!(
                "Chunk index {} out of range (total chunks: {})",
                chunk_index, total_chunks
            )));
        }

        let chunk_start = data_start + chunk_index * ENCRYPTED_CHUNK_SIZE;
        let chunk_end = if chunk_index == total_chunks - 1 && has_partial {
            ciphertext.len()
        } else {
            chunk_start + ENCRYPTED_CHUNK_SIZE
        };

        let chunk_data = &ciphertext[chunk_start..chunk_end];
        let nonce = chunk_nonce(&base_nonce, chunk_index as u64);
        let nonce_arr = GenericArray::from_slice(&nonce);

        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&self.key));
        cipher
            .decrypt(nonce_arr, chunk_data)
            .map_err(|_| EncryptionError::Decryption("Authentication failed".to_string()))
    }

    /// Decrypt all chunks from chunked encrypted data.
    pub fn decrypt_chunked(&self, ciphertext: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if ciphertext.len() < NONCE_SIZE {
            return Err(EncryptionError::Decryption(
                "Ciphertext too short for nonce".to_string(),
            ));
        }

        let data_start = NONCE_SIZE;
        let total_data_len = ciphertext.len() - data_start;

        let num_full_chunks = total_data_len / ENCRYPTED_CHUNK_SIZE;
        let has_partial = !total_data_len.is_multiple_of(ENCRYPTED_CHUNK_SIZE);
        let total_chunks = num_full_chunks + if has_partial { 1 } else { 0 };

        let mut result = Vec::new();
        for i in 0..total_chunks {
            let chunk = self.decrypt_chunk(ciphertext, i)?;
            result.extend(chunk);
        }

        Ok(result)
    }

    /// Decrypt a specific plaintext byte range from encrypted data.
    ///
    /// The ciphertext must start with the nonce (first 24 bytes) but may be
    /// truncated after the chunks needed for the requested range.
    ///
    /// Returns exactly the plaintext bytes from `plaintext_start` to `plaintext_end`.
    pub fn decrypt_range(
        &self,
        ciphertext: &[u8],
        plaintext_start: u64,
        plaintext_end: u64,
    ) -> Result<Vec<u8>, EncryptionError> {
        if plaintext_start >= plaintext_end {
            return Err(EncryptionError::Decryption(format!(
                "Invalid range: start ({}) >= end ({})",
                plaintext_start, plaintext_end
            )));
        }

        let start_chunk = plaintext_start / CHUNK_SIZE as u64;
        let end_chunk = (plaintext_end.saturating_sub(1)) / CHUNK_SIZE as u64;

        let mut plaintext = Vec::new();
        for chunk_idx in start_chunk..=end_chunk {
            let chunk = self.decrypt_chunk(ciphertext, chunk_idx as usize)?;
            plaintext.extend(chunk);
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
    /// 3. Call `decrypt_range_with_offset(nonce, chunks, 7, 500000, 600000)`
    pub fn decrypt_range_with_offset(
        &self,
        nonce: &[u8],
        encrypted_chunks: &[u8],
        first_chunk_index: u64,
        plaintext_start: u64,
        plaintext_end: u64,
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

        let base_nonce: [u8; NONCE_SIZE] = nonce
            .try_into()
            .map_err(|_| EncryptionError::Decryption("Invalid nonce".to_string()))?;

        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&self.key));

        let start_chunk = plaintext_start / CHUNK_SIZE as u64;
        let end_chunk = (plaintext_end.saturating_sub(1)) / CHUNK_SIZE as u64;

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
            let nonce = chunk_nonce(&base_nonce, absolute_chunk_idx);
            let nonce_arr = GenericArray::from_slice(&nonce);

            let decrypted = cipher.decrypt(nonce_arr, chunk_data).map_err(|_| {
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
        EncryptionService::from_key(derived)
    }

    /// Derive a 32-byte key using HKDF-SHA256 with the given info label.
    ///
    /// The derivation is deterministic: same master key + same info string always
    /// produces the same derived key.
    ///
    /// - Salt: `HMAC-SHA256(master_key, "coven-hkdf-salt-v1")`
    /// - IKM: master key
    /// - Info: caller-provided label
    pub fn derive_key(&self, info: &str) -> [u8; 32] {
        let mut mac =
            <Hmac<Sha256> as Mac>::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(b"coven-hkdf-salt-v1");
        let salt = mac.finalize().into_bytes();

        let hk = Hkdf::<Sha256>::new(Some(&salt), &self.key);
        let mut okm = [0u8; 32];
        hk.expand(info.as_bytes(), &mut okm)
            .expect("32 bytes is a valid HKDF output length");
        okm
    }
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

/// Calculate the total encrypted blob length for a chunked plaintext size.
pub fn encrypted_blob_len_for_plaintext(plaintext_size: u64) -> u64 {
    if plaintext_size == 0 {
        return (NONCE_SIZE + TAG_SIZE) as u64;
    }

    let full_chunks = plaintext_size / CHUNK_SIZE as u64;
    let final_chunk_size = plaintext_size % CHUNK_SIZE as u64;
    let encrypted_chunks = if final_chunk_size == 0 {
        full_chunks * ENCRYPTED_CHUNK_SIZE as u64
    } else {
        full_chunks * ENCRYPTED_CHUNK_SIZE as u64 + final_chunk_size + TAG_SIZE as u64
    };

    NONCE_SIZE as u64 + encrypted_chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Calculate the encrypted byte range needed for a plaintext byte range.
    /// Returns (encrypted_start, encrypted_end) including the nonce header.
    fn encrypted_range_for_plaintext(start: u64, end: u64) -> (u64, u64) {
        let start_chunk = start / CHUNK_SIZE as u64;
        let end_chunk = (end.saturating_sub(1)) / CHUNK_SIZE as u64;

        let enc_start = NONCE_SIZE as u64 + start_chunk * ENCRYPTED_CHUNK_SIZE as u64;
        let enc_end = NONCE_SIZE as u64 + (end_chunk + 1) * ENCRYPTED_CHUNK_SIZE as u64;

        // Always need the nonce header
        (0, enc_end.max(enc_start))
    }

    fn test_key() -> [u8; 32] {
        // Fixed test key for reproducibility
        [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]
    }

    fn create_test_service() -> EncryptionService {
        EncryptionService::new_with_key(&test_key())
    }

    #[test]
    fn test_roundtrip_small() {
        let service = create_test_service();
        let plaintext = b"Hello, world!";

        let ciphertext = service.encrypt(plaintext);
        let decrypted = service.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    /// The streaming sealer (base nonce + per-chunk `seal_chunk`) produces a blob
    /// the existing whole-buffer decryptor reads back unchanged, across the
    /// boundaries that matter: empty, sub-chunk, exact chunk, and several
    /// non-aligned chunks. `encrypt_chunked` is built on the sealer, so this also
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
            let mut sealer = service.sealer();
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
                service.decrypt(&streamed).unwrap(),
                plaintext,
                "streamed ciphertext failed to round-trip for len={len}"
            );
        }
    }

    /// `chunked_encrypted_len` predicts the exact byte length `encrypt_chunked`
    /// produces, across the chunk boundaries that matter — so a streaming upload
    /// can announce the final object size before sealing a byte.
    #[test]
    fn chunked_encrypted_len_matches_encrypt_chunked() {
        let service = create_test_service();
        for n in [
            0usize,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            200_000,
        ] {
            let produced = service.encrypt_chunked(&vec![0u8; n]).len() as u64;
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

        let ciphertext = service.encrypt(&plaintext);
        let decrypted = service.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_roundtrip_multiple_chunks() {
        let service = create_test_service();
        // 2.5 chunks worth of data
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 2 + CHUNK_SIZE / 2)
            .map(|i| (i % 256) as u8)
            .collect();

        let ciphertext = service.encrypt(&plaintext);
        let decrypted = service.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_random_access_chunk() {
        let service = create_test_service();
        // 3 chunks: chunk 0 = 0x00, chunk 1 = 0x11, chunk 2 = 0x22
        let mut plaintext = vec![0x00u8; CHUNK_SIZE];
        plaintext.extend(vec![0x11u8; CHUNK_SIZE]);
        plaintext.extend(vec![0x22u8; CHUNK_SIZE]);

        let ciphertext = service.encrypt(&plaintext);

        // Decrypt only chunk 1 (middle chunk)
        let chunk1 = service.decrypt_chunk(&ciphertext, 1).unwrap();
        assert_eq!(chunk1, vec![0x11u8; CHUNK_SIZE]);

        // Decrypt chunk 0
        let chunk0 = service.decrypt_chunk(&ciphertext, 0).unwrap();
        assert_eq!(chunk0, vec![0x00u8; CHUNK_SIZE]);

        // Decrypt chunk 2
        let chunk2 = service.decrypt_chunk(&ciphertext, 2).unwrap();
        assert_eq!(chunk2, vec![0x22u8; CHUNK_SIZE]);
    }

    #[test]
    fn test_random_access_partial_last_chunk() {
        let service = create_test_service();
        // 1 full chunk + partial chunk
        let mut plaintext = vec![0xAAu8; CHUNK_SIZE];
        plaintext.extend(vec![0xBBu8; 100]);

        let ciphertext = service.encrypt(&plaintext);

        let chunk0 = service.decrypt_chunk(&ciphertext, 0).unwrap();
        assert_eq!(chunk0, vec![0xAAu8; CHUNK_SIZE]);

        let chunk1 = service.decrypt_chunk(&ciphertext, 1).unwrap();
        assert_eq!(chunk1, vec![0xBBu8; 100]);
    }

    #[test]
    fn test_tamper_detection() {
        let service = create_test_service();
        let plaintext = b"Secret data";

        let mut ciphertext = service.encrypt(plaintext);

        // Tamper with the ciphertext (after nonce)
        let tamper_pos = NONCE_SIZE + 5;
        ciphertext[tamper_pos] ^= 0xFF;

        let result = service.decrypt(&ciphertext);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_plaintext() {
        let service = create_test_service();
        let plaintext = b"";

        let ciphertext = service.encrypt(plaintext);

        // Should just be nonce + auth tag
        assert_eq!(ciphertext.len(), NONCE_SIZE + TAG_SIZE);

        let decrypted = service.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypted_blob_len_for_plaintext_matches_chunked_encryption() {
        let service = create_test_service();
        for plaintext_size in [
            0,
            1,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            CHUNK_SIZE * 2 + 99,
        ] {
            let plaintext = vec![0x42u8; plaintext_size];
            let ciphertext = service.encrypt(&plaintext);

            assert_eq!(
                encrypted_blob_len_for_plaintext(plaintext_size as u64),
                ciphertext.len() as u64
            );
        }
    }

    #[test]
    fn test_single_byte() {
        let service = create_test_service();
        let plaintext = b"x";

        let ciphertext = service.encrypt(plaintext);
        let decrypted = service.decrypt(&ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypted_range_single_chunk() {
        // Plaintext bytes 0-100 are in chunk 0
        let (start, end) = encrypted_range_for_plaintext(0, 100);

        assert_eq!(start, 0); // Always need nonce
        assert_eq!(end, NONCE_SIZE as u64 + ENCRYPTED_CHUNK_SIZE as u64);
    }

    #[test]
    fn test_encrypted_range_spans_chunks() {
        // Plaintext bytes spanning chunk 0 and chunk 1
        let (start, end) =
            encrypted_range_for_plaintext(CHUNK_SIZE as u64 - 10, CHUNK_SIZE as u64 + 10);

        assert_eq!(start, 0); // Always need nonce
        assert_eq!(end, NONCE_SIZE as u64 + 2 * ENCRYPTED_CHUNK_SIZE as u64);
    }

    #[test]
    fn test_encrypted_range_middle_chunk() {
        // Plaintext bytes entirely within chunk 2
        let chunk2_start = CHUNK_SIZE as u64 * 2;
        let (start, end) = encrypted_range_for_plaintext(chunk2_start + 10, chunk2_start + 100);

        assert_eq!(start, 0); // Always need nonce
        assert_eq!(end, NONCE_SIZE as u64 + 3 * ENCRYPTED_CHUNK_SIZE as u64);
    }

    #[test]
    fn test_different_encryptions_different_ciphertext() {
        let service = create_test_service();
        let plaintext = b"Same message";

        let ciphertext1 = service.encrypt(plaintext);
        let ciphertext2 = service.encrypt(plaintext);

        // Different nonces = different ciphertext
        assert_ne!(ciphertext1, ciphertext2);

        // Both decrypt to same plaintext
        assert_eq!(service.decrypt(&ciphertext1).unwrap(), plaintext);
        assert_eq!(service.decrypt(&ciphertext2).unwrap(), plaintext);
    }

    #[test]
    fn test_chunk_index_out_of_range() {
        let service = create_test_service();
        let plaintext = vec![0u8; CHUNK_SIZE]; // Exactly 1 chunk

        let ciphertext = service.encrypt(&plaintext);

        // Chunk 0 should work
        assert!(service.decrypt_chunk(&ciphertext, 0).is_ok());

        // Chunk 1 should fail
        assert!(service.decrypt_chunk(&ciphertext, 1).is_err());
    }

    #[test]
    fn test_decrypt_range_within_single_chunk() {
        let service = create_test_service();
        // Create plaintext with recognizable pattern
        let plaintext: Vec<u8> = (0..CHUNK_SIZE).map(|i| (i % 256) as u8).collect();

        let ciphertext = service.encrypt(&plaintext);

        // Decrypt range [100, 200) within first chunk
        let decrypted = service.decrypt_range(&ciphertext, 100, 200).unwrap();

        assert_eq!(decrypted.len(), 100);
        assert_eq!(decrypted, plaintext[100..200]);
    }

    #[test]
    fn test_decrypt_range_spanning_chunks() {
        let service = create_test_service();
        // 3 chunks of data
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 3).map(|i| (i % 256) as u8).collect();

        let ciphertext = service.encrypt(&plaintext);

        // Range spanning from end of chunk 0 into chunk 1
        let start = CHUNK_SIZE as u64 - 100;
        let end = CHUNK_SIZE as u64 + 100;
        let decrypted = service.decrypt_range(&ciphertext, start, end).unwrap();

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

        let ciphertext = service.encrypt(&plaintext);

        // Decrypt just the middle chunk
        let start = CHUNK_SIZE as u64;
        let end = (CHUNK_SIZE * 2) as u64;
        let decrypted = service.decrypt_range(&ciphertext, start, end).unwrap();

        assert_eq!(decrypted, vec![0xBBu8; CHUNK_SIZE]);
    }

    #[test]
    fn test_decrypt_range_with_partial_encrypted_data() {
        let service = create_test_service();
        // Create 3-chunk plaintext
        let plaintext: Vec<u8> = (0..CHUNK_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let full_ciphertext = service.encrypt(&plaintext);

        // Calculate encrypted range for plaintext bytes in chunk 1
        let plaintext_start = CHUNK_SIZE as u64 + 100;
        let plaintext_end = CHUNK_SIZE as u64 + 200;
        let (enc_start, enc_end) = encrypted_range_for_plaintext(plaintext_start, plaintext_end);

        // Fetch only the needed encrypted bytes (simulating range read)
        let partial_ciphertext = full_ciphertext[enc_start as usize..enc_end as usize].to_vec();

        // Decrypt range from partial data
        let decrypted = service
            .decrypt_range(&partial_ciphertext, plaintext_start, plaintext_end)
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
        let full_ciphertext = service.encrypt(&plaintext);

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
        let full_ciphertext = service.encrypt(&plaintext);
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
        let service1 = EncryptionService::new_with_key(&[0u8; 32]);
        let service2 = EncryptionService::new_with_key(&[1u8; 32]);
        assert_ne!(service1.fingerprint(), service2.fingerprint());
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
        let svc1 = EncryptionService::new_with_key(&[0u8; 32]);
        let svc2 = EncryptionService::new_with_key(&[1u8; 32]);
        let key1 = svc1.derive_scoped("rel-123").key_bytes();
        let key2 = svc2.derive_scoped("rel-123").key_bytes();
        assert_ne!(key1, key2);
    }

    #[test]
    fn derive_scoped_roundtrip() {
        let master = create_test_service();
        let release_enc = master.derive_scoped("rel-456");
        let plaintext = b"test audio data for this release";

        let encrypted = release_enc.encrypt(plaintext);
        let decrypted = release_enc.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);

        // Cannot decrypt with master key
        assert!(master.decrypt(&encrypted).is_err());

        // Cannot decrypt with wrong release key
        let wrong_enc = master.derive_scoped("rel-999");
        assert!(wrong_enc.decrypt(&encrypted).is_err());
    }
}
