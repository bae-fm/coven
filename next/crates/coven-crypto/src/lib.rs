//! Store keys, circle keys, member keys, cryptography and key custody (§11.1).
//!
//! Objects authenticate their storage path; file chunks authenticate their keyed
//! name and index, with the name uniquely identifying their storage path.
//! Secret material is erased on drop and redacted in diagnostic output.

mod cipher;
mod derivation;
mod error;
mod hashing;
mod keys;
mod member;
mod object_digest;
mod randomness;
mod secret;
mod wire;

pub mod custody;

pub use cipher::SEALED_OBJECT_CHUNK_OVERHEAD;
pub use coven_foundation::id_source::CircleId;
pub use derivation::{DerivedKeys, EncryptionKey};
pub use error::{CryptoError, MaterialError, SealError};
pub use hashing::{ContentHash, ContentHasher, Fingerprint, FingerprintHasher, StoredFileName};
pub use keys::{CircleKey, InviteSecret, StoreKey, StoreKeyring};
pub use member::{
    seal_circle_key, seal_store_key, MemberId, MemberKeys, SealingPublicKey, Signature,
};
pub use object_digest::{ObjectDigest, ObjectHasher};
pub use secret::{SecretBytes, SecretText};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
