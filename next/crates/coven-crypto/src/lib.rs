//! Store keys, circle keys, member keys, cryptography and key custody (§11.1).
//!
//! Object and file chunks authenticate their storage path, cleartext prefix and
//! coordinates. Every file has an independent key and uses its index as nonce.
//! Secret material is erased on drop and redacted in diagnostic output.

mod cipher;
mod derivation;
mod error;
mod file_key;
mod hashing;
mod keys;
mod member;
mod object_digest;
mod randomness;
mod secret;
mod wire;

pub mod custody;

pub use cipher::{FILE_CHUNK_TAG_LEN, SEALED_OBJECT_CHUNK_OVERHEAD};
pub use coven_foundation::id_source::CircleId;
pub use derivation::{DerivedKeys, EncryptionKey};
pub use error::{CryptoError, MaterialError, SealError};
pub use file_key::FileKey;
pub use hashing::{ContentHash, ContentHasher, Fingerprint, FingerprintHasher, StoredFileName};
pub use keys::{CircleKey, InviteSecret, StoreKey, StoreKeyring};
pub use member::{
    seal_circle_key, seal_store_key, MemberId, MemberKeys, SealedKey, SealingPublicKey, Signature,
};
pub use object_digest::{ObjectDigest, ObjectHasher};
pub use secret::{SecretBytes, SecretText};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
