//! HKDF labels are the durable separation between cryptographic purposes (§11.1).

use hkdf::Hkdf;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use crate::{cipher, ContentHash, CryptoError, FingerprintHasher, StoredFileName};

pub(crate) const ENCRYPTION: &[u8] = b"coven/encryption/v1";
pub(crate) const APP_DATA: &[u8] = b"coven/app-data/v1";
pub(crate) const NAMING: &[u8] = b"coven/naming/v1";
pub(crate) const FILE_NONCES: &[u8] = b"coven/file-nonces/v1";
pub(crate) const FINGERPRINTS: &[u8] = b"coven/fingerprints/v1";
pub(crate) const JOIN_REQUEST: &[u8] = b"coven/join-request/v1";
pub(crate) const SEALED_BOX: &[u8] = b"coven/sealed-box/v1";

pub(crate) fn derive(key: &[u8; 32], context: &[u8]) -> Zeroizing<[u8; 32]> {
    let mut output = Zeroizing::new([0; 32]);
    let (mut extracted, hkdf) = Hkdf::<Sha256>::extract(None, key);
    extracted.as_mut_slice().zeroize();
    hkdf.expand(context, output.as_mut())
        .expect("32-byte derived keys fit HKDF-SHA256's output limit");
    output
}

pub(crate) fn derive_label(key: &[u8; 32], label: &[u8]) -> Zeroizing<[u8; 32]> {
    derive(key, &cipher::context(&[label]))
}

pub(crate) fn mac(key: &[u8; 32]) -> Hmac<Sha256> {
    Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts a 32-byte key")
}

/// A purpose-derived key for objects, including an invite's join request (§11.1).
pub struct EncryptionKey(pub(crate) Zeroizing<[u8; 32]>);

impl EncryptionKey {
    /// Seal an object with a random nonce, authenticating its storage path.
    /// Panics if the storage path is empty.
    pub fn seal_object(&self, path: &str, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        cipher::seal_random(
            &self.0,
            &cipher::context(&[b"coven/object/v1", cipher::storage_path(path)]),
            plaintext,
        )
    }

    /// Open an object only at the storage path it was sealed for.
    /// Panics if the storage path is empty.
    pub fn open_object(&self, path: &str, sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
        cipher::open_random(
            &self.0,
            &cipher::context(&[b"coven/object/v1", cipher::storage_path(path)]),
            sealed,
        )
    }

    /// Seal one write or snapshot chunk with a stored random nonce (§11.1).
    /// Section 0 holds a write's header; part i uses section i + 1. A snapshot
    /// uses section 0. Indices start at zero within each section.
    /// Panics if the storage path is empty.
    pub fn seal_object_chunk(
        &self,
        path: &str,
        section: u64,
        index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        cipher::seal_random(&self.0, &object_chunk_aad(path, section, index), plaintext)
    }

    /// Open a chunk only at its authenticated path, section and index (§11.1).
    /// Panics if the storage path is empty.
    pub fn open_object_chunk(
        &self,
        path: &str,
        section: u64,
        index: u64,
        sealed: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        cipher::open_random(&self.0, &object_chunk_aad(path, section, index), sealed)
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptionKey([REDACTED])")
    }
}

/// Separate encryption, naming, file-nonce and fingerprint keys for one audience.
pub struct DerivedKeys {
    encryption: EncryptionKey,
    naming: Zeroizing<[u8; 32]>,
    file_nonces: Zeroizing<[u8; 32]>,
    fingerprints: Zeroizing<[u8; 32]>,
}

impl DerivedKeys {
    pub(crate) fn new(key: &[u8; 32]) -> Self {
        Self {
            encryption: EncryptionKey(derive_label(key, ENCRYPTION)),
            naming: derive_label(key, NAMING),
            file_nonces: derive_label(key, FILE_NONCES),
            fingerprints: derive_label(key, FINGERPRINTS),
        }
    }

    /// Seal an object with a random nonce, authenticating its storage path (§11.1).
    /// Panics if the storage path is empty.
    pub fn seal_object(&self, path: &str, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.encryption.seal_object(path, plaintext)
    }

    /// Open an object only at its authenticated storage path (§11.1).
    /// Panics if the storage path is empty.
    pub fn open_object(&self, path: &str, sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.encryption.open_object(path, sealed)
    }

    /// Seal one chunk with this audience's derived encryption key (§11.1).
    /// See [`EncryptionKey::seal_object_chunk`] for section and index numbering.
    pub fn seal_object_chunk(
        &self,
        path: &str,
        section: u64,
        index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        self.encryption
            .seal_object_chunk(path, section, index, plaintext)
    }

    /// Open a chunk only at its authenticated path, section and index (§11.1).
    /// Panics if the storage path is empty.
    pub fn open_object_chunk(
        &self,
        path: &str,
        section: u64,
        index: u64,
        sealed: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        self.encryption
            .open_object_chunk(path, section, index, sealed)
    }

    /// HMAC-SHA256 of a file's content hash with the naming key (§16.2).
    pub fn file_name(&self, hash: &ContentHash) -> StoredFileName {
        let mut mac = mac(&self.naming);
        mac.update(hash.as_bytes());
        StoredFileName::from_bytes(mac.finalize().into_bytes().into())
    }

    /// Start an incremental hash of the audience's agreed data (§19.1).
    pub fn fingerprint_hasher(&self) -> FingerprintHasher {
        FingerprintHasher::new(&self.fingerprints)
    }

    /// Seal one file chunk, binding its keyed name and index (§11.1).
    /// Storage maps each name to one path, so binding the name binds that path.
    /// A name must identify immutable content with one fixed chunk partition:
    /// never reuse the same name and index for different plaintext bytes.
    pub fn seal_chunk(&self, name: &StoredFileName, index: u64, plaintext: &[u8]) -> Vec<u8> {
        cipher::seal(
            &self.encryption.0,
            &self.chunk_nonce(name, index),
            &chunk_aad(name, index),
            plaintext,
        )
    }

    /// Open one chunk only under its file's name and chunk index (§16.2).
    pub fn open_chunk(
        &self,
        name: &StoredFileName,
        index: u64,
        sealed: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        cipher::open(
            &self.encryption.0,
            &self.chunk_nonce(name, index),
            &chunk_aad(name, index),
            sealed,
        )
    }

    fn chunk_nonce(&self, name: &StoredFileName, index: u64) -> [u8; 24] {
        let mut mac = mac(&self.file_nonces);
        mac.update(name.as_bytes());
        let digest = mac.finalize().into_bytes();
        let mut nonce = [0; 24];
        nonce[..16].copy_from_slice(&digest[..16]);
        nonce[16..].copy_from_slice(&index.to_le_bytes());
        nonce
    }
}

impl std::fmt::Debug for DerivedKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DerivedKeys([REDACTED])")
    }
}

fn chunk_aad(name: &StoredFileName, index: u64) -> Vec<u8> {
    cipher::context(&[b"coven/chunk/v1", name.as_bytes(), &index.to_le_bytes()])
}

fn object_chunk_aad(path: &str, section: u64, index: u64) -> Vec<u8> {
    cipher::context(&[
        b"coven/object-chunk/v1",
        cipher::storage_path(path),
        &section.to_le_bytes(),
        &index.to_le_bytes(),
    ])
}

#[cfg(test)]
#[path = "derivation_tests.rs"]
mod tests;
