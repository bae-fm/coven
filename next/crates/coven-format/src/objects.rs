//! File chunks, join requests and posted positions (§6, §11, §12, §16, §19).

use crate::error::{bound, require, Error, Rule};
use crate::store_log::MemberPublicKeys;
use crate::value::{name, EntryPositions, WritePositions};
use crate::wire::{get_blob, put_blob, wire_struct, Decoder, Encoder, Wire, MAX_BYTES};
use coven_foundation::id_source::{DeviceId, InviteId};
use coven_merge::Audience;

/// A file's plaintext header, followed by separately authenticated chunks (§16.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileHeader {
    /// Plaintext bytes per full chunk; 65,536 is the product default.
    pub chunk_size: u32,
    /// The complete plaintext file length, including its last partial chunk.
    pub total_size: u64,
}
wire_struct!(FileHeader, chunk_size, total_size);
impl FileHeader {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(self.chunk_size > 0, "file chunk size", Rule::Required)?;
        bound(self.chunk_size as usize, MAX_BYTES, "file chunk size")
    }
    /// Checks a chunk's index and exact plaintext size against this file (§16.2).
    pub fn validate_chunk(&self, chunk: &FileChunk) -> Result<(), Error> {
        self.validate()?;
        chunk.validate()?;
        let offset = chunk
            .index
            .checked_mul(u64::from(self.chunk_size))
            .ok_or(Error::Invalid {
                field: "chunk offset",
                rule: Rule::Chunk,
            })?;
        require(offset < self.total_size, "chunk index", Rule::Chunk)?;
        let expected = (self.total_size - offset).min(u64::from(self.chunk_size));
        require(
            chunk.bytes.len() as u64 == expected,
            "chunk length",
            Rule::Chunk,
        )
    }
}

/// One file chunk, whose index must also be bound into its authentication (§16.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileChunk {
    /// The zero-based chunk index.
    pub index: u64,
    /// The plaintext bytes of this chunk.
    pub bytes: Vec<u8>,
}
impl Wire for FileChunk {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.index.put(out)?;
        put_blob(&self.bytes, out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self {
            index: u64::get(input)?,
            bytes: get_blob(input)?,
        })
    }
}
impl FileChunk {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(!self.bytes.is_empty(), "file chunk", Rule::Required)
    }
}

/// A new person's request, encrypted with the invite-derived key and signed (§12.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinRequest {
    /// The invite this request answers.
    pub invite: InviteId,
    /// The new member's signing and sealed-box public keys.
    pub keys: MemberPublicKeys,
    /// The new device's name shown to the approving admin.
    pub device_name: String,
}
wire_struct!(JoinRequest, invite, keys, device_name);
impl JoinRequest {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        name(&self.device_name)
    }
}

/// An audience's keyed fingerprint, supplied by the cryptography layer (§19.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    /// The rows this fingerprint describes.
    pub audience: Audience,
    /// The audience key number used to derive the fingerprint key.
    pub key_number: u64,
    /// The 256-bit fingerprint; this crate neither computes nor authenticates it.
    pub bytes: coven_crypto::Fingerprint,
}
wire_struct!(Fingerprint, audience, key_number, bytes);

/// A device's posted positions and fingerprints, made after uploading its writes (§15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostedPositions {
    /// The device publishing this record.
    pub device: DeviceId,
    /// Applied positions in device write logs.
    pub writes: WritePositions,
    /// Applied positions in the store log, which also affect the merged state.
    pub store_log: EntryPositions,
    /// One fingerprint per readable audience, in increasing audience order.
    pub fingerprints: Vec<Fingerprint>,
}
wire_struct!(PostedPositions, device, writes, store_log, fingerprints);
impl PostedPositions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.writes.validate()?;
        self.store_log.validate()?;
        require(
            self.fingerprints
                .first()
                .is_some_and(|f| f.audience == Audience::Store),
            "store fingerprint",
            Rule::Required,
        )?;
        require(
            self.fingerprints
                .windows(2)
                .all(|f| f[0].audience < f[1].audience),
            "fingerprints",
            Rule::Order,
        )?;
        for f in &self.fingerprints {
            require(f.key_number > 0, "fingerprint key number", Rule::Required)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "objects_tests.rs"]
mod tests;
