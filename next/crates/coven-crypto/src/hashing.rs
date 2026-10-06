//! Incremental content and fingerprint hashing (§16, §19.1).

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// SHA-256 of a file's bytes, kept inside encrypted writes (§16).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    /// Decode the content hash recorded in an encrypted row.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// The hash bytes; never place these in provider-visible paths or metadata.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A stream's incremental SHA-256 state; the file never has to fit in memory.
pub struct ContentHasher(Sha256);

impl ContentHasher {
    /// Start hashing a file's bytes (§16).
    pub fn new() -> Self {
        Self(Sha256::new())
    }
    /// Feed the next bytes of the stream in order, with any buffer size.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
    /// Finish the content hash once the stream has ended.
    pub fn finish(self) -> ContentHash {
        ContentHash(self.0.finalize().into())
    }
}

impl std::fmt::Debug for ContentHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ContentHasher(..)")
    }
}

/// A keyed hash of the data every device must agree on in an audience (§19.1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    /// Decode a fingerprint posted with another device's positions.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    /// The keyed hash bytes posted with this device's positions.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Incremental HMAC-SHA256 with an audience's derived fingerprint key (§19.1).
pub struct FingerprintHasher(Hmac<Sha256>);

impl FingerprintHasher {
    pub(crate) fn new(key: &[u8; 32]) -> Self {
        Self(crate::derivation::mac(key))
    }
    /// Feed the next canonical bytes of rows, generations, writers or lost values.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
    /// Finish the fingerprint for comparison at matching positions (§19.1).
    pub fn finish(self) -> Fingerprint {
        Fingerprint(self.0.finalize().into_bytes().into())
    }
}

impl std::fmt::Debug for FingerprintHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FingerprintHasher([REDACTED])")
    }
}

#[cfg(test)]
#[path = "hashing_tests.rs"]
mod tests;
