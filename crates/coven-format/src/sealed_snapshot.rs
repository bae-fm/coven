//! Authenticated snapshot routing and coverage (D9).

use crate::error::Error;
use crate::sealed;
use crate::sequence::Purpose;
use crate::wire::{Decoder, Encoder, Wire};
use coven_crypto::Signature;
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;

/// Clear snapshot routing. Verify the prefix signature before using coverage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotObjectPrefix {
    /// The audience of its rows and header.
    pub audience: Audience,
    /// The audience key sealing section zero.
    pub key: KeyId,
    /// Last included slot, including consumed refusals.
    pub covers: u64,
    /// Ordinary snapshot or prepared admin state.
    pub purpose: Purpose,
}
impl SnapshotObjectPrefix {
    /// Greatest signed prefix end, including a circle and raised version.
    pub const MAX_SIGNED_PREFIX: usize = 3 + 17 + 16 + 8 + 5 + 64;
    /// Bytes through the prefix signature.
    pub fn signed_length(bytes: &[u8]) -> Result<usize, Error> {
        Ok(Self::length(bytes)? + 64)
    }
    /// Plaintext size from the complete encrypted object size.
    pub fn plaintext_length(&self, object_length: u64) -> Result<u64, Error> {
        crate::sealed_stream::plaintext_length(
            object_length
                .checked_sub(self.encode()?.len() as u64 + 128)
                .ok_or(Error::Truncated)?,
        )
    }
    /// Encode kind, format, routing, coverage and purpose.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        34u8.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        self.audience.put(&mut out)?;
        self.key.put(&mut out)?;
        self.covers.put(&mut out)?;
        self.purpose.put(&mut out)?;
        Ok(out.bytes)
    }
    /// Determine the bounded prefix length without allocation.
    pub fn length(bytes: &[u8]) -> Result<usize, Error> {
        sealed::prefix(bytes, 34)?;
        let purpose = match *bytes.get(3).ok_or(Error::Truncated)? {
            0 => 28,
            1 => 44,
            tag => {
                return Err(Error::UnknownTag {
                    field: "audience",
                    tag,
                })
            }
        };
        match *bytes.get(purpose).ok_or(Error::Truncated)? {
            0 | 2 => Ok(purpose + 1),
            1 => Ok(purpose + 5),
            tag => Err(Error::UnknownTag {
                field: "snapshot purpose",
                tag,
            }),
        }
    }
    /// Decode exactly the prefix and its signature, without authenticating them.
    pub fn decode_signed(bytes: &[u8]) -> Result<(Self, Signature), Error> {
        let length = Self::length(bytes)?;
        let prefix = Self::decode(bytes.get(..length).ok_or(Error::Truncated)?)?;
        Ok((prefix, sealed::signature(&bytes[length..])?))
    }
    /// Decode exactly the clear prefix.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let length = Self::length(bytes)?;
        if bytes.len() < length {
            return Err(Error::Truncated);
        }
        if bytes.len() > length {
            return Err(Error::TrailingBytes);
        }
        let mut input = Decoder::new(&bytes[3..])?;
        let prefix = Self {
            audience: Wire::get(&mut input)?,
            key: Wire::get(&mut input)?,
            covers: Wire::get(&mut input)?,
            purpose: Wire::get(&mut input)?,
        };
        input.finish()?;
        Ok(prefix)
    }
}

#[cfg(test)]
#[path = "sealed_snapshot_tests.rs"]
mod tests;
