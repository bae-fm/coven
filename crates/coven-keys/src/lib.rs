//! Keys: device and master key custody, the sealed-secret files that hold
//! them, and the cipher every encrypted Coven object is sealed with.
//!
//! This crate owns the key-bearing primitives — signing keys, sealed-box keys,
//! the AEAD cipher, the passphrase KDF, and the platform keyring — so the
//! layers above it hold custody objects rather than raw key material.
//!
//! `envelope` and `keyring_backend` are private: the passphrase vault and the
//! keyring-store installer are how custody and the key service do their work,
//! not something a caller composes.

pub mod custody;
pub mod encryption;
pub mod identity_custody;
pub mod keys;

mod envelope;
mod keyring_backend;
