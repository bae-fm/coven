//! A file's independent key and indexed XChaCha20-Poly1305 chunks.

use crate::{cipher, randomness, CryptoError, SecretBytes};
use zeroize::Zeroizing;

/// A random 32-byte key for one immutable file, erased on drop.
/// Never reuse this key and a chunk index with different plaintext.
pub struct FileKey(Zeroizing<[u8; 32]>);

impl FileKey {
    /// Generate a file key independently of store and circle keys.
    pub fn generate() -> Result<Self, CryptoError> {
        let mut bytes = Zeroizing::new([0; 32]);
        randomness::fill(bytes.as_mut())?;
        Ok(Self(bytes))
    }

    /// Import the key carried in a row's where-column.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Export the key for its where-column; the returned bytes erase on drop.
    pub fn to_secret_bytes(&self) -> SecretBytes {
        SecretBytes::new(self.0.to_vec())
    }

    /// Seal a chunk, binding the path, whole cleartext header and index.
    /// The output is ciphertext and tag; its nonce is not stored.
    /// Panics if the storage path is empty.
    pub fn seal_chunk(&self, path: &str, header: &[u8], index: u64, plaintext: &[u8]) -> Vec<u8> {
        cipher::seal(&self.0, &nonce(index), &aad(path, header, index), plaintext)
    }

    /// Open only at the authenticated file path, header and chunk index.
    /// Panics if the storage path is empty.
    pub fn open_chunk(
        &self,
        path: &str,
        header: &[u8],
        index: u64,
        sealed: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        cipher::open(&self.0, &nonce(index), &aad(path, header, index), sealed)
    }
}

impl std::fmt::Debug for FileKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FileKey([REDACTED])")
    }
}

fn nonce(index: u64) -> [u8; 24] {
    let mut nonce = [0; 24];
    nonce[16..].copy_from_slice(&index.to_be_bytes());
    nonce
}

fn aad(path: &str, header: &[u8], index: u64) -> Vec<u8> {
    cipher::context(&[
        b"coven/file-chunk/v1",
        cipher::storage_path(path),
        header,
        &index.to_be_bytes(),
    ])
}

#[cfg(test)]
#[path = "file_key_tests.rs"]
mod tests;
