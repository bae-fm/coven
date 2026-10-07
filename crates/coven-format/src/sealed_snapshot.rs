//! Snapshots have one audience key and one stream of sealed chunks (§15).

use crate::chunks::CHUNK_SIZE;
use crate::error::{bound, require, Error, Rule};
use crate::sealed;
use crate::value::{EntryPositions, WritePositions};
use crate::wire::{Decoder, Encoder, Wire, MAX_ITEMS};
use coven_crypto::{Signature, SEALED_OBJECT_CHUNK_OVERHEAD};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;

/// Cleartext routing information for the snapshot's single encrypted section.
/// Decoding alone does not authenticate it: verify its prefix signature before
/// using any field, and its whole-object signature before applying its data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotObjectPrefix {
    /// The snapshot's audience; the opened header must agree.
    pub audience: Audience,
    /// The audience key sealing every chunk.
    pub key: KeyId,
    /// Applied write positions, strictly increasing by device.
    pub writes: WritePositions,
    /// Applied store-log positions, strictly increasing by device.
    pub store_log: EntryPositions,
}
impl SnapshotObjectPrefix {
    /// Next exclusive read boundary while fetching a signed prefix. Start with
    /// no bytes and append exactly the requested range until this equals the
    /// buffer length. Counts are bounded before reading their lists; these
    /// unverified routing fields only delimit reads and confer no authority.
    pub fn read_length(bytes: &[u8]) -> Result<usize, Error> {
        if bytes.len() < 4 {
            return Ok(4);
        }
        let writes = Self::routing_length(bytes)?;
        if bytes.len() < writes + 4 {
            return Ok(writes + 4);
        }
        let entries = positions_end(bytes, writes)?;
        if bytes.len() < entries + 4 {
            return Ok(entries + 4);
        }
        Ok(positions_end(bytes, entries)? + 64)
    }

    /// Encoded plaintext size from a listing, excluding the prefix, both
    /// signatures and per-chunk overhead. This checks the possible partition;
    /// it does not authenticate the body or validate its plaintext frames.
    pub fn plaintext_length(&self, object_length: u64) -> Result<u64, Error> {
        let sealed = object_length
            .checked_sub(self.encode()?.len() as u64 + 128)
            .ok_or(Error::Truncated)?;
        let overhead = (4 + SEALED_OBJECT_CHUNK_OVERHEAD) as u64;
        let full = CHUNK_SIZE as u64 + overhead;
        require(sealed > 0, "snapshot chunks", Rule::Required)?;
        let chunks = sealed.div_ceil(full);
        let last = sealed - (chunks - 1) * full;
        require(last > overhead, "snapshot final chunk length", Rule::Chunk)?;
        Ok(sealed - chunks * overhead)
    }

    /// Encode kind 34, version, audience, key and both counted position lists.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.writes.validate()?;
        self.store_log.validate()?;
        let mut out = Encoder::new();
        34u8.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        self.audience.put(&mut out)?;
        self.key.put(&mut out)?;
        // This unframed prefix has two independently bounded collections.
        let mut writes = Encoder::new();
        self.writes.put(&mut writes)?;
        out.bytes(&writes.bytes)?;
        let mut entries = Encoder::new();
        self.store_log.put(&mut entries)?;
        out.bytes(&entries.bytes)?;
        Ok(out.bytes)
    }

    /// Bound the prefix from its routing fields and both list counts. The second
    /// count follows the first list, so those bytes must already be available.
    /// No position values are allocated or decoded here.
    pub fn length(bytes: &[u8]) -> Result<usize, Error> {
        let writes = Self::routing_length(bytes)?;
        let entries = positions_end(bytes, writes)?;
        positions_end(bytes, entries)
    }

    /// Decode exactly a cleartext prefix followed by its 64-byte signature.
    /// The caller verifies the signature with the expected member and path.
    pub fn decode_signed(bytes: &[u8]) -> Result<(Self, Signature), Error> {
        let length = Self::length(bytes)?;
        let prefix = Self::decode(bytes.get(..length).ok_or(Error::Truncated)?)?;
        let signature = sealed::signature(&bytes[length..])?;
        Ok((prefix, signature))
    }

    /// Decode exactly the prefix, checking both lists fit before allocation.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let length = Self::length(bytes)?;
        if bytes.len() < length {
            return Err(Error::Truncated);
        }
        if bytes.len() > length {
            return Err(Error::TrailingBytes);
        }
        let writes_start = Self::routing_length(bytes)?;
        let entries_start = positions_end(bytes, writes_start)?;
        let mut routing = Decoder::new(&bytes[3..writes_start])?;
        let audience = Audience::get(&mut routing)?;
        let key = KeyId::get(&mut routing)?;
        routing.finish()?;
        let mut input = Decoder::new(&bytes[writes_start..entries_start])?;
        let writes = WritePositions::get(&mut input)?;
        input.finish()?;
        let mut input = Decoder::new(&bytes[entries_start..])?;
        let store_log = EntryPositions::get(&mut input)?;
        input.finish()?;
        writes.validate()?;
        store_log.validate()?;
        Ok(Self {
            audience,
            key,
            writes,
            store_log,
        })
    }

    fn routing_length(bytes: &[u8]) -> Result<usize, Error> {
        sealed::prefix(bytes, 34)?;
        match *bytes.get(3).ok_or(Error::Truncated)? {
            0 => Ok(20),
            1 => Ok(36),
            tag => Err(Error::UnknownTag {
                field: "audience",
                tag,
            }),
        }
    }
}

fn positions_end(bytes: &[u8], offset: usize) -> Result<usize, Error> {
    let count = bytes.get(offset..offset + 4).ok_or(Error::Truncated)?;
    let count = u32::from_be_bytes(count.try_into().expect("four bytes")) as usize;
    bound(count, MAX_ITEMS, "snapshot positions")?;
    Ok(offset + 4 + count * 16)
}

/// Delimits sealed snapshot chunks without buffering the object. The caller
/// reserves the final 64 bytes for the whole-object signature. Acceptance also
/// requires that signature and the plaintext decoder's end-marker check.
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

    /// Consume the layout at its final signature, requiring at least one chunk.
    /// The caller verifies it against the digest of every preceding object byte,
    /// including the prefix signature, before applying any plaintext.
    pub fn read_signature(self, bytes: &[u8]) -> Result<Signature, Error> {
        require(self.index > 0, "snapshot chunks", Rule::Required)?;
        sealed::signature(bytes)
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
