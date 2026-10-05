//! Store keys, circle keys, member keys, cryptography and key custody (§11.1).
//!
//! Objects authenticate their storage path; file chunks also authenticate their
//! index. Secret material is erased on drop and redacted in diagnostic output.

mod cipher;
mod derivation;
mod error;
mod hashing;
mod keys;
mod member;
mod randomness;
mod secret;
mod wire;

pub mod custody;

pub use derivation::{DerivedKeys, EncryptionKey};
pub use error::{CryptoError, MaterialError, SealError};
pub use hashing::{ContentHash, ContentHasher, FileName, Fingerprint, FingerprintHasher};
pub use keys::{CircleId, CircleKey, InviteSecret, StoreKey, StoreKeyring};
pub use member::{
    seal_circle_key, seal_store_key, MemberId, MemberKeys, SealingPublicKey, Signature,
};
pub use secret::SecretBytes;
