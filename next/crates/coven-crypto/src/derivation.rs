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

pub(crate) fn derive(key: &[u8; 32], label: &[u8]) -> Result<Zeroizing<[u8; 32]>, CryptoError> {
    let mut output = Zeroizing::new([0; 32]);
    let (mut extracted, hkdf) = Hkdf::<Sha256>::extract(None, key);
    extracted.as_mut_slice().zeroize();
    hkdf.expand(label, output.as_mut())
        .map_err(|_| CryptoError::Derivation)?;
    Ok(output)
}

pub(crate) fn mac(key: &[u8; 32]) -> Result<Hmac<Sha256>, CryptoError> {
    Hmac::<Sha256>::new_from_slice(key).map_err(|_| CryptoError::Derivation)
}

/// A purpose-derived key for objects, including an invite's join request (§11.1).
pub struct EncryptionKey(pub(crate) Zeroizing<[u8; 32]>);

impl EncryptionKey {
    /// Seal an object with a random nonce, authenticating its storage path.
    pub fn seal_object(&self, path: &str, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        cipher::seal_random(
            &self.0,
            &cipher::context(&[b"coven/object/v1", cipher::storage_path(path)?]),
            plaintext,
        )
    }

    /// Open an object only at the storage path it was sealed for.
    pub fn open_object(&self, path: &str, sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
        cipher::open_random(
            &self.0,
            &cipher::context(&[b"coven/object/v1", cipher::storage_path(path)?]),
            sealed,
        )
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
    pub(crate) fn new(key: &[u8; 32]) -> Result<Self, CryptoError> {
        Ok(Self {
            encryption: EncryptionKey(derive(key, ENCRYPTION)?),
            naming: derive(key, NAMING)?,
            file_nonces: derive(key, FILE_NONCES)?,
            fingerprints: derive(key, FINGERPRINTS)?,
        })
    }

    /// Seal an object with a random nonce, authenticating its storage path (§11.1).
    pub fn seal_object(&self, path: &str, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.encryption.seal_object(path, plaintext)
    }

    /// Open an object only at its authenticated storage path (§11.1).
    pub fn open_object(&self, path: &str, sealed: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.encryption.open_object(path, sealed)
    }

    /// HMAC-SHA256 of a file's content hash with the naming key (§16.2).
    pub fn file_name(&self, hash: &ContentHash) -> Result<StoredFileName, CryptoError> {
        let mut mac = mac(&self.naming)?;
        mac.update(hash.as_bytes());
        Ok(StoredFileName::from_bytes(
            mac.finalize().into_bytes().into(),
        ))
    }

    /// Start an incremental hash of the audience's agreed data (§19.1).
    pub fn fingerprint_hasher(&self) -> Result<FingerprintHasher, CryptoError> {
        FingerprintHasher::new(&self.fingerprints)
    }

    /// Seal one file chunk, binding its keyed name and index (§11.1).
    /// Storage maps each name to one path, so binding the name binds that path.
    /// A name must identify immutable content with one fixed chunk partition:
    /// never reuse the same name and index for different plaintext bytes.
    pub fn seal_chunk(
        &self,
        name: &StoredFileName,
        index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        cipher::seal(
            &self.encryption.0,
            &self.chunk_nonce(name, index)?,
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
            &self.chunk_nonce(name, index)?,
            &chunk_aad(name, index),
            sealed,
        )
    }

    fn chunk_nonce(&self, name: &StoredFileName, index: u64) -> Result<[u8; 24], CryptoError> {
        let mut mac = mac(&self.file_nonces)?;
        mac.update(name.as_bytes());
        let digest = mac.finalize().into_bytes();
        let mut nonce = [0; 24];
        nonce[..16].copy_from_slice(&digest[..16]);
        nonce[16..].copy_from_slice(&index.to_le_bytes());
        Ok(nonce)
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

#[cfg(test)]
#[path = "derivation_tests.rs"]
mod tests;
