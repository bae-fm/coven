//! Piecewise sealed write layout. Crypto supplies and authenticates the bytes.

use crate::chunks::CHUNK_SIZE;
use crate::error::{bound, require, Error, Rule};
use crate::sealed;
use crate::wire::{Decoder, Encoder, Wire, MAX_ITEMS, MAX_OBJECT};
use coven_crypto::{Signature, SEALED_OBJECT_CHUNK_OVERHEAD};
use coven_foundation::id_source::KeyId;

/// The exact stored size, independent of key identities, nonces and signature
/// contents. Header and part lengths come from the validated plaintext codec.
pub fn sealed_length(header_length: usize, part_lengths: &[u64]) -> Result<u64, Error> {
    validate_lengths(header_length, part_lengths)?;
    let overhead = (4 + SEALED_OBJECT_CHUNK_OVERHEAD) as u64;
    let mut length = WriteObjectPrefix::encoded_length(part_lengths.len())? as u64
        + header_length as u64
        + overhead
        + 64;
    for part in part_lengths {
        length = part
            .div_ceil(CHUNK_SIZE as u64)
            .checked_mul(overhead)
            .and_then(|n| n.checked_add(*part))
            .and_then(|n| n.checked_add(length))
            .ok_or(Error::Invalid {
                field: "sealed write length",
                rule: Rule::StreamLength,
            })?;
    }
    Ok(length)
}

fn validate_lengths(header_length: usize, part_lengths: &[u64]) -> Result<(), Error> {
    bound(header_length, MAX_OBJECT, "write header")?;
    require(
        header_length >= crate::FRAME_PREFIX_LEN,
        "write header",
        Rule::StreamLength,
    )?;
    bound(part_lengths.len(), MAX_ITEMS, "write parts")?;
    for length in part_lengths {
        require(
            *length >= crate::FRAME_PREFIX_LEN as u64,
            "write part length",
            Rule::StreamLength,
        )?;
    }
    Ok(())
}

/// Cleartext key identities, authenticated by the object's final signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteObjectPrefix {
    /// The format fixed with these keys before the first upload attempt.
    pub format: crate::FormatVersion,
    /// The store key sealing section 0, the header frame.
    pub store_key: KeyId,
    /// Keys of the audience parts in header order; section i + 1 uses entry i.
    pub part_keys: Vec<KeyId>,
}
impl WriteObjectPrefix {
    /// Exact prefix size for this many parts, before their key ids are selected.
    pub fn encoded_length(parts: usize) -> Result<usize, Error> {
        bound(parts, MAX_ITEMS, "write section keys")?;
        Ok(23 + 16 * parts)
    }

    /// Encode kind, version, header key and the counted list of part keys.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let crate::FormatVersion::V1 = self.format;
        let mut out = Encoder::new();
        32u8.put(&mut out)?;
        self.format.number().put(&mut out)?;
        self.store_key.put(&mut out)?;
        self.part_keys.put(&mut out)?;
        Ok(out.bytes)
    }

    /// Determine the complete prefix size from its first 23 bytes, before
    /// allocating the counted key list. This is not a plaintext frame prefix.
    pub fn length(bytes: &[u8]) -> Result<usize, Error> {
        sealed::prefix(bytes, 32)?;
        let count = bytes.get(19..23).ok_or(Error::Truncated)?;
        let count = u32::from_be_bytes(count.try_into().expect("four bytes")) as usize;
        Self::encoded_length(count)
    }

    /// Decode exactly the complete prefix, refusing trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let length = Self::length(bytes)?;
        if bytes.len() < length {
            return Err(Error::Truncated);
        }
        if bytes.len() > length {
            return Err(Error::TrailingBytes);
        }
        let mut input = Decoder::new(&bytes[3..])?;
        let result = Self {
            format: crate::FormatVersion::decode(bytes)?,
            store_key: KeyId::get(&mut input)?,
            part_keys: Vec::get(&mut input)?,
        };
        input.finish()?;
        Ok(result)
    }

    /// Bound the following header's length prefix before fetching or allocating.
    pub fn header_chunk_length(prefix: &[u8]) -> Result<usize, Error> {
        let length = sealed::chunk_length(prefix, MAX_OBJECT)?;
        require(
            length >= crate::FRAME_PREFIX_LEN + 4 + SEALED_OBJECT_CHUNK_OVERHEAD,
            "sealed header frame",
            Rule::Chunk,
        )?;
        Ok(length)
    }

    /// Read the length-prefixed sealed header. Open it with `store_key`, the
    /// object's path, section 0 and index 0, then call [`Self::opened_header`].
    pub fn header_chunk(piece: &[u8]) -> Result<&[u8], Error> {
        Self::header_chunk_length(piece)?;
        sealed::chunk(piece, MAX_OBJECT)
    }

    /// Begin part chunks after the caller authenticates and decodes the header.
    /// `part_lengths` must come from that header, in part order. This layer
    /// treats frame bytes as opaque; the final signature still must be checked.
    pub fn opened_header(
        self,
        bytes: &[u8],
        part_lengths: Vec<u64>,
    ) -> Result<WriteObjectLayout, Error> {
        let mut layout = WriteObjectLayout::new(self, bytes, part_lengths)?;
        layout.advance();
        Ok(layout)
    }
}

/// Coordinates, selected key and exact plaintext length of the next chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectChunk {
    /// The audience key to select by id.
    pub key: KeyId,
    /// Header 0, or part i + 1.
    pub section: u64,
    /// Zero-based position within that section.
    pub index: u64,
    /// Exact plaintext size before adding the stored nonce and tag.
    pub plaintext_length: usize,
}

enum Position {
    Chunk { section: usize, index: u64 },
    Signature,
    End,
}

/// Sealed layout cursor, retaining header metadata rather than object bytes.
/// Writers start at the header. Readers start after opening the header with
/// [`WriteObjectPrefix::opened_header`]. Hash each encoded piece, including the
/// prefix and length fields, before the final signature.
pub struct WriteObjectLayout {
    prefix: WriteObjectPrefix,
    part_lengths: Vec<u64>,
    header_length: usize,
    position: Position,
}
impl WriteObjectLayout {
    /// Begin producing a write from its header bytes and declared part lengths.
    /// Exactly one key is required for every part; lengths come from the header's
    /// plaintext codec, which remains responsible for validating its fields.
    pub fn new(
        prefix: WriteObjectPrefix,
        header: &[u8],
        part_lengths: Vec<u64>,
    ) -> Result<Self, Error> {
        let header_length = header.len();
        validate_lengths(header_length, &part_lengths)?;
        require(
            prefix.part_keys.len() == part_lengths.len(),
            "write section key count",
            Rule::StreamLength,
        )?;
        Ok(Self {
            prefix,
            part_lengths,
            header_length,
            position: Position::Chunk {
                section: 0,
                index: 0,
            },
        })
    }

    /// Encode the cleartext prefix before supplying sealed chunks.
    pub fn prefix(&self) -> Result<Vec<u8>, Error> {
        self.prefix.encode()
    }

    /// Coordinates for the next sealed chunk, or `None` when only the signature remains.
    pub fn next_chunk(&self) -> Option<ObjectChunk> {
        let Position::Chunk { section, index } = self.position else {
            return None;
        };
        if section == 0 {
            Some(ObjectChunk {
                key: self.prefix.store_key,
                section: 0,
                index: 0,
                plaintext_length: self.header_length,
            })
        } else {
            let left = self.part_lengths[section - 1] - index * CHUNK_SIZE as u64;
            Some(ObjectChunk {
                key: self.prefix.part_keys[section - 1],
                section: section as u64,
                index,
                plaintext_length: left.min(CHUNK_SIZE as u64) as usize,
            })
        }
    }

    /// Validate a chunk length prefix against the next exact boundary before allocating.
    pub fn chunk_length(&self, prefix: &[u8]) -> Result<usize, Error> {
        let chunk = self.next_chunk().ok_or(Error::TrailingBytes)?;
        let length = sealed::chunk_length(prefix, chunk.plaintext_length)?;
        require(
            length == 4 + SEALED_OBJECT_CHUNK_OVERHEAD + chunk.plaintext_length,
            "sealed write chunk length",
            Rule::Chunk,
        )?;
        Ok(length)
    }

    /// Write the next length-prefixed sealed chunk, advancing only on success.
    pub fn encode_chunk(&mut self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        let chunk = self.next_chunk().ok_or(Error::TrailingBytes)?;
        require(
            bytes.len() == SEALED_OBJECT_CHUNK_OVERHEAD + chunk.plaintext_length,
            "sealed write chunk length",
            Rule::Chunk,
        )?;
        let encoded = sealed::encode_chunk(bytes)?;
        self.advance();
        Ok(encoded)
    }

    /// Read the next complete piece without opening or copying it. A reader
    /// lacking this part's key can skip the returned bytes while hashing them.
    pub fn decode_chunk<'a>(&mut self, piece: &'a [u8]) -> Result<(ObjectChunk, &'a [u8]), Error> {
        let chunk = self.next_chunk().ok_or(Error::TrailingBytes)?;
        let length = self.chunk_length(piece)?;
        if piece.len() < length {
            return Err(Error::Truncated);
        }
        if piece.len() > length {
            return Err(Error::TrailingBytes);
        }
        self.advance();
        Ok((chunk, &piece[4..]))
    }

    /// Write a signature only after every declared chunk.
    pub fn signature(&mut self, signature: &Signature) -> Result<[u8; 64], Error> {
        require(
            matches!(self.position, Position::Signature),
            "write signature position",
            Rule::StreamLength,
        )?;
        self.position = Position::End;
        Ok(*signature.as_bytes())
    }

    /// Read the final 64-byte signature. Crypto verifies it against the author,
    /// path and accumulated digest; this method only checks layout.
    pub fn read_signature(&mut self, bytes: &[u8]) -> Result<Signature, Error> {
        let signature = sealed::signature(bytes)?;
        self.signature(&signature)?;
        Ok(signature)
    }

    /// Check EOF after the signature. No additional bytes may follow it.
    pub fn finish(&self, trailing: &[u8]) -> Result<(), Error> {
        if !trailing.is_empty() {
            return Err(Error::TrailingBytes);
        }
        require(
            matches!(self.position, Position::End),
            "write signature",
            Rule::StreamLength,
        )
    }

    fn advance(&mut self) {
        let Position::Chunk { section, index } = self.position else {
            unreachable!("a chunk was checked");
        };
        if section > 0 && index + 1 < self.part_lengths[section - 1].div_ceil(CHUNK_SIZE as u64) {
            self.position = Position::Chunk {
                section,
                index: index + 1,
            };
        } else if section < self.part_lengths.len() {
            self.position = Position::Chunk {
                section: section + 1,
                index: 0,
            };
        } else {
            self.position = Position::Signature;
        }
    }
}

#[cfg(test)]
#[path = "sealed_write_tests.rs"]
mod tests;
