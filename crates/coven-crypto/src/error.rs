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
    /// The stored object has an unrecognized kind.
    #[error("unknown sealed object kind {0}")]
    UnknownKind(u8),
    /// The stored object uses a format version this reader does not support.
    #[error("unsupported sealed object format version {0}")]
    UnsupportedVersion(u16),
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

/// A key or serialized secret violates its representation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MaterialError {
    /// The encoded secret or sealed value is malformed.
    #[error("invalid key material encoding")]
    Encoding,
    /// A different key already has this store key id.
    #[error("conflicting store key id {0}")]
    StoreKeyConflict(coven_foundation::id_source::KeyId),
    /// A different key already has this circle id and key id.
    #[error("conflicting key id {key} for circle {circle}")]
    CircleKeyConflict {
        /// The circle whose key id was reused.
        circle: crate::CircleId,
        /// The conflicting key id.
        key: coven_foundation::id_source::KeyId,
    },
    /// The keyring does not hold this store key.
    #[error("unknown store key id {0}")]
    UnknownStoreKey(coven_foundation::id_source::KeyId),
    /// The keyring does not hold this circle key.
    #[error("unknown key id {key} for circle {circle}")]
    UnknownCircleKey {
        /// The circle whose key is missing.
        circle: crate::CircleId,
        /// The missing key id.
        key: coven_foundation::id_source::KeyId,
    },
}
