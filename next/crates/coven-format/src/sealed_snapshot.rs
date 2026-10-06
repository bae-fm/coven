//! Snapshots have one audience key and one stream of sealed chunks (§15).

use crate::chunks::CHUNK_SIZE;
use crate::error::{require, Error, Rule};
use crate::sealed;
use crate::wire::{Decoder, Encoder, Wire};
use coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD;
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;

/// Cleartext routing information for the snapshot's single encrypted section.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotObjectPrefix {
    /// The snapshot's audience; the opened header must agree.
    pub audience: Audience,
    /// The audience key sealing every chunk.
    pub key: KeyId,
}
impl SnapshotObjectPrefix {
    /// Encode kind 15, version, audience and key id.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        15u8.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        self.audience.put(&mut out)?;
        self.key.put(&mut out)?;
        Ok(out.bytes)
    }

    /// Determine the prefix length from its first four bytes before allocation.
    pub fn length(bytes: &[u8]) -> Result<usize, Error> {
        sealed::prefix(bytes, 15)?;
        match *bytes.get(3).ok_or(Error::Truncated)? {
            0 => Ok(20),
            1 => Ok(36),
            tag => Err(Error::UnknownTag {
                field: "audience",
                tag,
            }),
        }
    }

    /// Decode exactly the prefix; no sealed chunk bytes are accepted here.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let length = Self::length(bytes)?;
        if bytes.len() < length {
            return Err(Error::Truncated);
        }
        if bytes.len() > length {
            return Err(Error::TrailingBytes);
        }
        let mut input = Decoder::new(&bytes[3..])?;
        Ok(Self {
            audience: Audience::get(&mut input)?,
            key: KeyId::get(&mut input)?,
        })
    }
}

/// Delimits sealed snapshot chunks without buffering the object. EOF is valid
/// only after the opened plaintext's snapshot decoder accepts its end marker.
pub struct SnapshotObjectLayout {
    index: u64,
    partial: bool,
}
impl SnapshotObjectLayout {
    /// Start the snapshot's section 0 at chunk index 0.
    pub fn new() -> Self {
        Self {
            index: 0,
            partial: false,
        }
    }

    /// The index to authenticate when sealing or opening the next chunk.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Bound a length prefix before fetching or allocating its chunk.
    pub fn chunk_length(&self, prefix: &[u8]) -> Result<usize, Error> {
        require(
            !self.partial,
            "chunk after snapshot's final partial chunk",
            Rule::Chunk,
        )?;
        sealed::chunk_length(prefix, CHUNK_SIZE)
    }

    /// Encode one sealed chunk, checking the canonical chunk partition.
    pub fn encode_chunk(&mut self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        require(
            !self.partial
                && (SEALED_OBJECT_CHUNK_OVERHEAD + 1..=CHUNK_SIZE + SEALED_OBJECT_CHUNK_OVERHEAD)
                    .contains(&bytes.len()),
            "snapshot chunk length",
            Rule::Chunk,
        )?;
        let piece = sealed::encode_chunk(bytes)?;
        self.advance(bytes.len())?;
        Ok(piece)
    }

    /// Read a complete length-prefixed chunk without copying its sealed bytes.
    pub fn decode_chunk<'a>(&mut self, piece: &'a [u8]) -> Result<&'a [u8], Error> {
        self.chunk_length(piece)?;
        let bytes = sealed::chunk(piece, CHUNK_SIZE)?;
        self.advance(bytes.len())?;
        Ok(bytes)
    }

    fn advance(&mut self, length: usize) -> Result<(), Error> {
        self.index = self.index.checked_add(1).ok_or(Error::Invalid {
            field: "snapshot chunk index",
            rule: Rule::Chunk,
        })?;
        self.partial = length < CHUNK_SIZE + SEALED_OBJECT_CHUNK_OVERHEAD;
        Ok(())
    }
}

#[cfg(test)]
#[path = "sealed_snapshot_tests.rs"]
mod tests;
