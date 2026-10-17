//! Content hashing (§16) and keyed original-write authorship (D11).

use hmac::Mac;
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

#[cfg(test)]
#[path = "hashing_tests.rs"]
mod tests;

/// Keyed hashing of one write part's authorship leaves (D11). Each leaf is
/// keyed with the part's audience key, so a member who cannot read the
/// audience cannot confirm a guessed row from a published root. Branches and
/// the empty tree are unkeyed: a device holding a part's leaf hashes can open
/// any of its leaves again without the key.
#[derive(Clone)]
pub struct AuthorshipHasher(zeroize::Zeroizing<[u8; 32]>);

impl AuthorshipHasher {
    pub(crate) fn new(key: &[u8; 32]) -> Self {
        Self(zeroize::Zeroizing::new(*key))
    }

    /// One leaf's encoded bytes, as D11 defines them.
    pub fn leaf(&self, leaf: &[u8]) -> [u8; 32] {
        let mut mac = crate::derivation::mac(&self.0);
        mac.update(&[0]);
        mac.update(leaf);
        mac.finalize().into_bytes().into()
    }
}

/// A branch of an authorship tree over its ordered children (D11).
pub fn authorship_node(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update([1]);
    hash.update(left);
    hash.update(right);
    hash.finalize().into()
}

/// The root of an authorship tree with no leaves (D11).
pub fn authorship_empty() -> [u8; 32] {
    Sha256::digest([2]).into()
}

impl std::fmt::Debug for AuthorshipHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthorshipHasher([REDACTED])")
    }
}
