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
pub use randomness::random_secret;
mod s3_signing;
mod secret;
mod wire;

pub mod custody;

pub use cipher::{FILE_CHUNK_TAG_LEN, SEALED_OBJECT_CHUNK_OVERHEAD};
pub use coven_foundation::id_source::CircleId;
pub use derivation::{DerivedKeys, EncryptionKey};
pub use error::{CryptoError, MaterialError};
pub use file_key::FileKey;
pub use hashing::{
    authorship_empty, authorship_node, AuthorshipHasher, ContentHash, ContentHasher,
};
pub use keys::{CircleKey, InviteSecret, StoreKey, StoreKeyring};
pub use member::{
    seal_access_key, seal_circle_key, seal_store_key, MemberIdentity, MemberKeys, MemberPublicKeys,
    SealingKey, Signature, SigningKey, MAX_ACCESS_KEY_BOX,
};
pub use object_digest::{ObjectDigest, ObjectHasher};
pub use s3_signing::{aws_request_signature, gcs_request_signature, s3_request_hash};
pub use secret::{SecretBytes, SecretText};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
