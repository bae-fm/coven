//! Failures callers distinguish without parsing error text.

/// A cryptographic operation could not produce or authenticate its result.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    /// The device cannot provide a cryptographic service needed by this call.
    #[error("cryptographic service unavailable: {0}")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The ciphertext, key, path, index or associated data did not authenticate.
    #[error("ciphertext authentication failed")]
    Authentication,
    /// A sealed value was truncated or had invalid framing.
    #[error("invalid sealed value")]
    Malformed,
    /// X25519 produced a non-contributory shared secret.
    #[error("X25519 public key has low order")]
    WeakSealingKey,
    /// The Ed25519 public key does not identify a member.
    #[error("invalid or weak Ed25519 public key")]
    InvalidMemberId,
    /// The member's signature did not verify.
    #[error("signature verification failed")]
    Signature,
    /// Authenticated key material did not have the expected shape.
    #[error("invalid key material: {0}")]
    Material(#[from] MaterialError),
}

/// A numbered key or serialized secret violates its representation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MaterialError {
    /// The encoded secret or sealed value is malformed.
    #[error("invalid key material encoding")]
    Encoding,
    /// A different key already has this store key number.
    #[error("conflicting store key number {0}")]
    StoreKeyConflict(u64),
    /// A different key already has this circle id and key number.
    #[error("conflicting key number {number} for circle {circle}")]
    CircleKeyConflict {
        /// The circle whose number was reused.
        circle: crate::CircleId,
        /// The conflicting key number.
        number: u64,
    },
    /// The keyring does not hold this store key.
    #[error("unknown store key number {0}")]
    UnknownStoreKey(u64),
    /// The keyring does not hold this circle key.
    #[error("unknown key number {number} for circle {circle}")]
    UnknownCircleKey {
        /// The circle whose key is missing.
        circle: crate::CircleId,
        /// The missing key number.
        number: u64,
    },
}

/// App data could not be sealed or opened with the store key it names (§20.11).
#[derive(Debug, thiserror::Error)]
pub enum SealError {
    /// The keyring lacks the named key or the encoded material is invalid.
    #[error("store key unavailable: {0}")]
    Key(#[from] MaterialError),
    /// The cipher refused the value or its app-supplied context.
    #[error("app data cryptography failed: {0}")]
    Crypto(#[from] CryptoError),
}
