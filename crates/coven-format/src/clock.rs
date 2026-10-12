//! Authentication of a device's signed storage-time probe (§9, D8–D10).
//!
//! Crate-to-crate API: callers supply the store key and the member the applied
//! store log names for the path's device. An unknown device waits for registration.
//! Time probes use provider status metadata; opening a body never supplies a time.

use crate::error::Rule;
use crate::objects::ClockObservation;
use crate::path::ObjectPath;
use crate::sealed_single::SingleChunkObject;
use crate::{Error, Object};
use coven_crypto::{CryptoError, MemberId, ObjectHasher, StoreKey};

/// Authenticate one complete clock object and require its payload to name its path.
/// Missing storage objects are handled by the caller; empty or malformed bytes
/// are errors, never an absent observation.
pub fn open(
    bytes: &[u8],
    path: &ObjectPath,
    key: &StoreKey,
    author: &MemberId,
) -> Result<ClockObservation, ClockError> {
    let object = SingleChunkObject::decode(bytes)?;
    let SingleChunkObject::ClockObservation { key: declared, .. } = &object else {
        return Err(Error::Invalid {
            field: "clock object",
            rule: Rule::Kind,
        }
        .into());
    };
    if *declared != key.id() {
        return Err(ClockError::WrongIdentity);
    }
    let mut hash = ObjectHasher::new();
    hash.update(&bytes[..bytes.len() - 64]);
    author.verify_object(path.as_str(), &hash.finish(), object.signature())?;
    let plain = key.derive().open_object_chunk(
        path.as_str(),
        &object.prefix().encode()?,
        0,
        0,
        object.chunk(),
    )?;
    let Object::ClockObservation(clock) = Object::decode(&plain)? else {
        return Err(Error::Invalid {
            field: "clock frame",
            rule: Rule::Kind,
        }
        .into());
    };
    if *path != ObjectPath::clock(path.store(), clock.device) {
        return Err(ClockError::WrongIdentity);
    }
    Ok(clock)
}

/// A clock object failed framing, authentication or its declared identity checks.
#[derive(Debug)]
pub enum ClockError {
    /// The envelope or plaintext frame is malformed or has the wrong kind.
    Format(Error),
    /// The signature or ciphertext could not be authenticated.
    Crypto(CryptoError),
    /// The key id or device/path disagrees with the supplied identity.
    WrongIdentity,
}

impl From<Error> for ClockError {
    fn from(error: Error) -> Self {
        Self::Format(error)
    }
}

impl From<CryptoError> for ClockError {
    fn from(error: CryptoError) -> Self {
        Self::Crypto(error)
    }
}

impl std::fmt::Display for ClockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(error) => error.fmt(f),
            Self::Crypto(error) => error.fmt(f),
            Self::WrongIdentity => f.write_str("clock object identity mismatch"),
        }
    }
}

impl std::error::Error for ClockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Format(error) => Some(error),
            Self::Crypto(error) => Some(error),
            Self::WrongIdentity => None,
        }
    }
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;
