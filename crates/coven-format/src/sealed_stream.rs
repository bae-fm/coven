//! Canonical 64-KiB sealed streams used by slots, snapshots and floors.

use crate::chunks::CHUNK_SIZE;
use crate::error::{require, Error, Rule};
use crate::sealed;
use coven_crypto::{Signature, SEALED_OBJECT_CHUNK_OVERHEAD};

/// Delimits sealed stream chunks without buffering the object. The caller
/// reserves the final 64 bytes for the whole-object signature. Acceptance also
/// requires that signature and the plaintext decoder's end-marker check.
pub struct StreamObjectLayout {
    index: u64,
    partial: bool,
}
impl Default for StreamObjectLayout {
    fn default() -> Self {
        Self::new()
    }
}
impl StreamObjectLayout {
    /// Start the stream's section 0 at chunk index 0.
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
            "chunk after stream's final partial chunk",
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
            "stream chunk length",
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

    /// Consume the layout at its final signature, requiring at least one chunk.
    /// The caller verifies it against the digest of every preceding object byte,
    /// including the prefix signature, before applying any plaintext.
    pub fn read_signature(self, bytes: &[u8]) -> Result<Signature, Error> {
        require(self.index > 0, "stream chunks", Rule::Required)?;
        sealed::signature(bytes)
    }

    fn advance(&mut self, length: usize) -> Result<(), Error> {
        self.index = self.index.checked_add(1).ok_or(Error::Invalid {
            field: "stream chunk index",
            rule: Rule::Chunk,
        })?;
        self.partial = length < CHUNK_SIZE + SEALED_OBJECT_CHUNK_OVERHEAD;
        Ok(())
    }
}

/// Recover plaintext size from a complete stream's encrypted size.
pub fn plaintext_length(sealed: u64) -> Result<u64, Error> {
    let overhead = (4 + SEALED_OBJECT_CHUNK_OVERHEAD) as u64;
    let full = CHUNK_SIZE as u64 + overhead;
    require(sealed > 0, "stream chunks", Rule::Required)?;
    let chunks = sealed.div_ceil(full);
    require(
        sealed - (chunks - 1) * full > overhead,
        "stream final chunk",
        Rule::Chunk,
    )?;
    Ok(sealed - chunks * overhead)
}
