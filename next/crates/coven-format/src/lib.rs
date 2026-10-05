//! Deterministic, versioned plaintext encodings of coven's storage objects (§21.1).
//!
//! No I/O, clock, randomness, sealing or signing occurs here. Every object starts
//! with a kind byte, a big-endian u16 format version, and a big-endian u32 payload
//! length. See the crate's `FORMAT.md` for every layout, bound and ordering rule.

pub mod codes;
pub mod error;
pub mod key;
mod merge_wire;
pub mod objects;
pub mod snapshot;
pub mod snapshot_rows;
pub mod store_log;
pub mod value;
mod wire;
pub mod write;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use error::Error;

use error::bound;
use objects::{FileChunk, FileHeader, JoinRequest, PostedPositions};
use store_log::StoreLogEntry;
use wire::{decode_frame, Encoder, Wire, MAX_OBJECT};
use write::WriteRecord;

/// The single supported plaintext format version (§17.2).
pub const FORMAT_VERSION: u16 = 1;
/// The fixed prefix length: kind, format version, payload length.
pub const FRAME_PREFIX_LEN: usize = 7;

/// Independently decoded plaintext objects. Snapshots use [`snapshot::SnapshotDecoder`]
/// with merge metadata; restore and invite codes use their zeroizing byte APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Object {
    /// One transaction, with a header and a part per audience (§5, §14.4).
    Write(WriteRecord),
    /// One membership, device, circle, version or reset entry (§9).
    StoreLog(StoreLogEntry),
    /// A file's chunk size and total size (§16.2).
    FileHeader(FileHeader),
    /// A new person's public keys and device name (§12.2).
    JoinRequest(JoinRequest),
    /// A device's positions and keyed fingerprints (§6, §19.1).
    PostedPositions(PostedPositions),
    /// One file chunk's index and plaintext (§16.2).
    FileChunk(FileChunk),
}

impl Object {
    /// Checks structure and returns the one canonical byte string for this value.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        match self {
            Self::Write(v) => encode_frame(1, v),
            Self::StoreLog(v) => encode_frame(2, v),
            Self::FileHeader(v) => encode_frame(7, v),
            Self::JoinRequest(v) => encode_frame(10, v),
            Self::PostedPositions(v) => encode_frame(11, v),
            Self::FileChunk(v) => encode_frame(12, v),
        }
    }
    /// Decodes exactly one bounded frame, refusing unknown tags/versions, trailing
    /// bytes, noncanonical sets and structurally invalid records. Authentication
    /// and checks requiring other writes or a database belong to their owners.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        let object = match kind {
            1 => Self::Write(Wire::get(&mut input)?),
            2 => Self::StoreLog(Wire::get(&mut input)?),
            7 => Self::FileHeader(Wire::get(&mut input)?),
            10 => Self::JoinRequest(Wire::get(&mut input)?),
            11 => Self::PostedPositions(Wire::get(&mut input)?),
            12 => Self::FileChunk(Wire::get(&mut input)?),
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
            Self::Write(v) => v.validate(),
            Self::StoreLog(v) => v.validate(),
            Self::FileHeader(v) => v.validate(),
            Self::JoinRequest(v) => v.validate(),
            Self::PostedPositions(v) => v.validate(),
            Self::FileChunk(v) => v.validate(),
        }
    }
}

/// Checks the seven-byte prefix and returns the full frame length, capped at
/// 16 MiB, before a caller allocates or fetches its payload. Extra supplied bytes
/// are ignored here; [`Object::decode`] requires exactly one complete frame.
pub fn frame_length(prefix: &[u8]) -> Result<usize, Error> {
    let bytes = prefix.get(..FRAME_PREFIX_LEN).ok_or(Error::Truncated)?;
    if !matches!(bytes[0], 1..=5 | 7..=12) {
        return Err(Error::UnknownTag {
            field: "object kind",
            tag: bytes[0],
        });
    }
    let version = u16::from_be_bytes([bytes[1], bytes[2]]);
    if version != FORMAT_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }
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
    let mut out = Encoder::new();
    kind.put(&mut out)?;
    FORMAT_VERSION.put(&mut out)?;
    0u32.put(&mut out)?;
    put(&mut out)?;
    let size = (out.bytes.len() - FRAME_PREFIX_LEN) as u32;
    out.bytes[3..7].copy_from_slice(&size.to_be_bytes());
    Ok(out.bytes)
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
