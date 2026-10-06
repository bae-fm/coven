//! One XChaCha20-Poly1305 implementation for objects, chunks and custody.

use crate::{randomness, CryptoError};
use chacha20poly1305::{
    aead::{array::typenum::Unsigned, consts::U24, Aead, AeadCore, Payload},
    KeyInit, XChaCha20Poly1305,
};

/// Bytes added to each sealed object chunk: its stored nonce and authentication tag.
pub const SEALED_OBJECT_CHUNK_OVERHEAD: usize = <XChaCha20Poly1305 as AeadCore>::NonceSize::USIZE
    + <XChaCha20Poly1305 as AeadCore>::TagSize::USIZE;

pub(crate) fn storage_path(path: &str) -> &[u8] {
    assert!(!path.is_empty(), "storage paths must be nonempty");
    path.as_bytes()
}
pub(crate) fn context(parts: &[&[u8]]) -> Vec<u8> {
    let mut aad = Vec::new();
    for part in parts {
        aad.extend_from_slice(&(part.len() as u64).to_le_bytes());
        aad.extend_from_slice(part);
    }
    aad
}

pub(crate) fn seal(key: &[u8; 32], nonce: &[u8; 24], aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
    encrypt(&XChaCha20Poly1305::new(key.into()), nonce, aad, plaintext)
}

fn encrypt(
    cipher: &impl Aead<NonceSize = U24>,
    nonce: &[u8; 24],
    aad: &[u8],
    plaintext: &[u8],
) -> Vec<u8> {
    cipher
        .encrypt(
            nonce.into(),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .expect("XChaCha20-Poly1305 plaintext must fit its block counter and lengths must fit u64")
}

pub(crate) fn open(
    key: &[u8; 32],
    nonce: &[u8; 24],
    aad: &[u8],
    sealed: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    XChaCha20Poly1305::new(key.into())
        .decrypt(nonce.into(), Payload { msg: sealed, aad })
        .map_err(|_| CryptoError::Authentication)
}

pub(crate) fn seal_random(
    key: &[u8; 32],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let mut nonce = [0; 24];
    randomness::fill(&mut nonce)?;
    let mut sealed = nonce.to_vec();
    sealed.extend(seal(key, &nonce, aad, plaintext));
    Ok(sealed)
}

pub(crate) fn open_random(
    key: &[u8; 32],
    aad: &[u8],
    sealed: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let (nonce, ciphertext) = sealed.split_at_checked(24).ok_or(CryptoError::Malformed)?;
    let nonce = nonce.try_into().map_err(|_| CryptoError::Malformed)?;
    open(key, nonce, aad, ciphertext)
}

#[cfg(test)]
#[path = "cipher_tests.rs"]
mod tests;
