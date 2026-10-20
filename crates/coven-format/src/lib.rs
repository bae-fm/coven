//! Versioned plaintext frames and streaming sealed-object layouts (Appendix F.1).
//!
//! No I/O, clock, randomness or signing occurs here. File chunks compose
//! crypto with validated headers and ranges. Plaintext frames
//! are bounded; writes and snapshots stream without an object-size bound.
//! `spec/format.md` specifies stored bytes.

pub mod access_key_delivery;
pub mod admin;
pub mod authorship;
pub mod chunks;
pub mod codes;
pub mod dismissal;
pub mod error;
pub mod file;
pub mod file_reference;
pub mod floor;
pub mod key;
pub mod loss;
pub mod merge_fields;
mod merge_wire;
pub mod objects;
pub mod path;
mod sealed;
pub mod sealed_floor;
pub mod sealed_key;
pub mod sealed_single;
pub mod sealed_slot;
pub mod sealed_snapshot;
pub mod sealed_stream;
pub mod sealed_write;
pub mod sequence;
pub mod snapshot;
pub mod snapshot_rows;
pub mod snapshot_stream;
pub mod value;
mod version;
mod wire;
pub mod write;
pub mod write_sources;
pub mod write_stream;
pub mod writes_slot;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use error::Error;
pub use version::{FormatVersion, RequiredUpdate};

use admin::StoreChange;
use error::bound;
use objects::JoinRequest;
use wire::{decode_frame, Encoder, Wire, MAX_OBJECT};

/// The object format written and read by coven (format.md).
pub const FORMAT_VERSION: u16 = FormatVersion::CURRENT.number();
/// The fixed prefix length: kind, format version, payload length.
pub const FRAME_PREFIX_LEN: usize = 7;

/// Independently decoded plaintext objects. Writes use [`write_stream::WriteEncoder`];
/// snapshots use [`snapshot::SnapshotDecoder`]
/// with merge metadata; restore and invite codes use their zeroizing byte APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Object {
    /// One membership, device, circle, version or reset entry (§10).
    Admin(StoreChange),
    /// A new person's public keys and device name (§13.2).
    JoinRequest(JoinRequest),
}

impl Object {
    /// Checks structure and returns the one canonical byte string for this value.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.encode_in(FormatVersion::CURRENT)
    }

    /// Encode in the format recorded for an attempted object, preserving its bytes.
    pub fn encode_in(&self, format: FormatVersion) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let FormatVersion::V1 = format;
        match self {
            Self::Admin(v) => {
                let bytes = encode_frame_in(4, format, |out| v.put(out))?;
                Ok(bytes)
            }
            Self::JoinRequest(v) => encode_frame_in(9, format, |out| v.put(out)),
        }
    }
    /// Decodes exactly one bounded frame, refusing unknown tags/versions, trailing
    /// bytes, noncanonical sets and structurally invalid records. Authentication
    /// and checks requiring other writes or a database belong to their owners.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        let object = match kind {
            4 => Self::Admin(Wire::get(&mut input)?),
            9 => Self::JoinRequest(Wire::get(&mut input)?),
            tag => {
                return Err(Error::UnknownTag {
                    field: "object kind",
                    tag,
                })
            }
        };
        input.finish()?;
        object.validate()?;
        Ok(object)
    }
    fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Admin(v) => v.validate(),
            Self::JoinRequest(v) => v.validate(),
        }
    }
}

/// Checks the seven-byte prefix and returns the full frame length, capped at
/// 16 MiB, before a caller allocates or fetches its payload. Extra supplied bytes
/// are ignored here; [`Object::decode`] requires exactly one complete frame.
pub fn frame_length(prefix: &[u8]) -> Result<usize, Error> {
    let bytes = prefix.get(..3).ok_or(Error::Truncated)?;
    if !matches!(bytes[0], 1..=7 | 9..=17) {
        return Err(Error::UnknownTag {
            field: "object kind",
            tag: bytes[0],
        });
    }
    let FormatVersion::V1 = FormatVersion::decode(bytes)?;
    let bytes = prefix.get(..FRAME_PREFIX_LEN).ok_or(Error::Truncated)?;
    let size = u32::from_be_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]) as usize;
    bound(size, MAX_OBJECT - FRAME_PREFIX_LEN, "frame payload")?;
    Ok(size + FRAME_PREFIX_LEN)
}

pub(crate) fn encode_frame<T: Wire>(kind: u8, value: &T) -> Result<Vec<u8>, Error> {
    encode_frame_with(kind, |out| value.put(out))
}

pub(crate) fn encode_frame_with(
    kind: u8,
    put: impl FnOnce(&mut Encoder) -> Result<(), Error>,
) -> Result<Vec<u8>, Error> {
    encode_frame_in(kind, FormatVersion::CURRENT, put)
}

fn encode_frame_in(
    kind: u8,
    format: FormatVersion,
    put: impl FnOnce(&mut Encoder) -> Result<(), Error>,
) -> Result<Vec<u8>, Error> {
    let mut out = Encoder::new();
    kind.put(&mut out)?;
    format.number().put(&mut out)?;
    0u32.put(&mut out)?;
    put(&mut out)?;
    let size = (out.bytes.len() - FRAME_PREFIX_LEN) as u32;
    out.bytes[3..7].copy_from_slice(&size.to_be_bytes());
    Ok(out.bytes)
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

mod member_access;
pub use member_access::{IamPolicy, MemberAccess, S3Principal};
