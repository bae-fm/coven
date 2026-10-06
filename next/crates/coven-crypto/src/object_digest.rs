//! Incremental hashing of the bytes covered by an object's signature (§14.4).

use sha2::{Digest, Sha256};

/// SHA-256 of every object byte before its signature, including its sealed chunks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectDigest([u8; 32]);

impl ObjectDigest {
    /// The digest bytes authenticated with the object's path and signature domain.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Incremental SHA-256 state for an object's bytes in storage order.
pub struct ObjectHasher(Sha256);

impl ObjectHasher {
    /// Start hashing the object before reading or writing its prefix.
    pub fn new() -> Self {
        Self(Sha256::new())
    }

    /// Feed the next prefix, length field or sealed chunk bytes in order.
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Finish the digest after the last sealed chunk, before the signature.
    pub fn finish(self) -> ObjectDigest {
        ObjectDigest(self.0.finalize().into())
    }
}

impl std::fmt::Debug for ObjectHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ObjectHasher(..)")
    }
}

#[cfg(test)]
#[path = "object_digest_tests.rs"]
mod tests;
