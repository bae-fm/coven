//! HKDF labels are the durable separation between cryptographic purposes (§11.1).

use hkdf::Hkdf;
use hmac::{Hmac, KeyInit};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use crate::{cipher, CryptoError, FingerprintHasher};

pub(crate) const ENCRYPTION: &[u8] = b"coven/encryption/v1";
pub(crate) const APP_DATA: &[u8] = b"coven/app-data/v1";
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
    /// Seal an object chunk with a random nonce, binding its whole cleartext prefix.
    /// Section 0 holds a write's header; part i uses section i + 1. A snapshot
    /// uses section 0. Indices start at zero within each section.
    /// Panics if the storage path is empty.
    pub fn seal_object_chunk(
        &self,
        path: &str,
        prefix: &[u8],
        section: u64,
        index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        cipher::seal_random(
            &self.0,
            &object_chunk_aad(path, prefix, section, index),
            plaintext,
        )
    }

    /// Open a chunk only at its authenticated path, prefix, section and index.
    /// Panics if the storage path is empty.
    pub fn open_object_chunk(
        &self,
        path: &str,
        prefix: &[u8],
        section: u64,
        index: u64,
        sealed: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        cipher::open_random(
            &self.0,
            &object_chunk_aad(path, prefix, section, index),
            sealed,
        )
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptionKey([REDACTED])")
    }
}

/// Separate encryption and fingerprint keys for one audience.
pub struct DerivedKeys {
    encryption: EncryptionKey,
    fingerprints: Zeroizing<[u8; 32]>,
}

impl DerivedKeys {
    pub(crate) fn new(key: &[u8; 32]) -> Self {
        Self {
            encryption: EncryptionKey(derive_label(key, ENCRYPTION)),
            fingerprints: derive_label(key, FINGERPRINTS),
        }
    }

    /// Seal one chunk with this audience's derived encryption key (§11.1).
    /// See [`EncryptionKey::seal_object_chunk`] for section and index numbering.
    pub fn seal_object_chunk(
        &self,
        path: &str,
        prefix: &[u8],
        section: u64,
        index: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        self.encryption
            .seal_object_chunk(path, prefix, section, index, plaintext)
    }

    /// Open a chunk only at its authenticated path, prefix, section and index.
    /// Panics if the storage path is empty.
    pub fn open_object_chunk(
        &self,
        path: &str,
        prefix: &[u8],
        section: u64,
        index: u64,
        sealed: &[u8],
    ) -> Result<Vec<u8>, CryptoError> {
        self.encryption
            .open_object_chunk(path, prefix, section, index, sealed)
    }

    /// Start an incremental hash of the audience's agreed data (§19.1).
    pub fn fingerprint_hasher(&self) -> FingerprintHasher {
        FingerprintHasher::new(&self.fingerprints)
    }
}

impl std::fmt::Debug for DerivedKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DerivedKeys([REDACTED])")
    }
}

fn object_chunk_aad(path: &str, prefix: &[u8], section: u64, index: u64) -> Vec<u8> {
    cipher::context(&[
        b"coven/object-chunk/v1",
        cipher::storage_path(path),
        prefix,
        &section.to_be_bytes(),
        &index.to_be_bytes(),
    ])
}

#[cfg(test)]
#[path = "derivation_tests.rs"]
mod tests;
