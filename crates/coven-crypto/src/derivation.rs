//! HKDF labels are the durable separation between cryptographic purposes (Appendix D).

use hkdf::Hkdf;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

use crate::{cipher, AuthorshipHasher, CryptoError};

pub(crate) const ENCRYPTION: &[u8] = b"coven/encryption/v1";
pub(crate) const JOIN_REQUEST: &[u8] = b"coven/join-request/v1";
pub(crate) const CONFIRMATION_NUMBER: &[u8] = b"coven/confirmation-number/v1";
pub(crate) const SEALED_BOX: &[u8] = b"coven/sealed-box/v1";
pub(crate) const AUTHORSHIP: &[u8] = b"coven/row-authorship/v1";

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

/// A purpose-derived key for objects, including an invite's join request (Appendix D).
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

/// Separate encryption and authorship keys for one audience.
pub struct DerivedKeys {
    encryption: EncryptionKey,
    authorship: Zeroizing<[u8; 32]>,
}

impl DerivedKeys {
    pub(crate) fn new(key: &[u8; 32]) -> Self {
        Self {
            encryption: EncryptionKey(derive_label(key, ENCRYPTION)),
            authorship: derive_label(key, AUTHORSHIP),
        }
    }

    /// Hash a write part's authorship leaves and branches under this
    /// audience's key, so only its readers can test a row guess (D9, D11).
    pub fn authorship_hasher(&self) -> AuthorshipHasher {
        AuthorshipHasher::new(&self.authorship)
    }

    /// Seal one chunk with this audience's derived encryption key (Appendix D).
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

    /// Re-seal an inner-write or sequence-slot chunk identically on every
    /// attempt (D9/D11).
    /// The nonce binds the path, complete cleartext prefix, section, index and
    /// SHA-256 of the complete chunk plaintext, including any frame bytes.
    /// Different content at a reused path has different nonce inputs. Retries
    /// fix plaintext, prefix and key choices to reproduce the same bytes.
    /// Snapshots, floors and join requests use `seal_object_chunk` instead.
    pub fn reseal_object_chunk(
        &self,
        path: &str,
        prefix: &[u8],
        section: u64,
        index: u64,
        plaintext: &[u8],
    ) -> Vec<u8> {
        let key = &self.encryption.0;
        let mut nonce = mac(key);
        nonce.update(&cipher::context(&[
            b"coven/object-nonce/v2",
            cipher::storage_path(path),
            prefix,
            &section.to_be_bytes(),
            &index.to_be_bytes(),
            &Sha256::digest(plaintext),
        ]));
        let nonce = nonce.finalize().into_bytes();
        let nonce: &[u8; 24] = nonce[..24].try_into().expect("24-byte nonce");
        let mut sealed = nonce.to_vec();
        sealed.extend(cipher::seal(
            key,
            nonce,
            &object_chunk_aad(path, prefix, section, index),
            plaintext,
        ));
        sealed
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

    /// Open a header whose authentication identity is inside its plaintext.
    /// `identity` receives only the fixed routing prefix as an untrusted hint;
    /// it must only construct the identity, never install or report that hint.
    /// No plaintext is returned until the ordinary AEAD check succeeds with
    /// the selected identity, exact prefix, and header coordinates (0, 0).
    ///
    /// D9 inner writes use this because their stable write number is sealed.
    /// ChaCha's keystream does not depend on associated data (RFC 8439 §2.8).
    /// The tentative bytes and cipher state are erased after routing.
    pub fn open_identified_header<const N: usize>(
        &self,
        prefix: &[u8],
        sealed: &[u8],
        identity: impl FnOnce(&[u8; N]) -> Result<String, CryptoError>,
    ) -> Result<Vec<u8>, CryptoError> {
        use chacha20::{
            cipher::{StreamCipher, StreamCipherSeek},
            KeyIvInit, XChaCha20,
        };
        let (nonce, ciphertext) = sealed.split_at_checked(24).ok_or(CryptoError::Malformed)?;
        if ciphertext
            .len()
            .checked_sub(cipher::FILE_CHUNK_TAG_LEN)
            .is_none_or(|length| length < N)
        {
            return Err(CryptoError::Malformed);
        }
        let mut tentative = Zeroizing::new([0; N]);
        tentative.copy_from_slice(&ciphertext[..N]);
        let key: &[u8; 32] = &self.encryption.0;
        let nonce: &[u8; 24] = nonce.try_into().expect("24-byte split");
        let mut stream = XChaCha20::new(key.into(), nonce.into());
        // Block zero supplies Poly1305's key; payload encryption starts at one.
        stream.seek(64u64);
        stream.apply_keystream(tentative.as_mut());
        let path = identity(&tentative)?;
        self.open_object_chunk(&path, prefix, 0, 0, sealed)
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
