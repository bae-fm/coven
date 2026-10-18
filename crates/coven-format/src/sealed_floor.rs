//! Streamed floor routing: a key id, first kept slot and original key copies (D9).
//! The clear prefix has no total-size bound. Each returned piece is bounded.

use crate::error::{bound, require, Error, Rule};
use crate::floor::OriginalSlot;
use crate::value::Slot;
use crate::wire::{Decoder, Encoder, Wire, MAX_ITEMS};
use coven_foundation::id_source::KeyId;

/// Incremental clear-prefix encoder. Retains only the count and previous slot.
pub struct FloorPrefixEncoder {
    remaining: u32,
    previous: Option<Slot>,
}
impl FloorPrefixEncoder {
    /// Emit kind 39, format, store key, first kept slot and copy count.
    pub fn start(key: KeyId, first: Slot, copies: u32) -> Result<(Self, Vec<u8>), Error> {
        bound(copies as usize, MAX_ITEMS, "floor key copies")?;
        let mut out = Encoder::new();
        39u8.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        key.put(&mut out)?;
        first.put(&mut out)?;
        copies.put(&mut out)?;
        Ok((
            Self {
                remaining: copies,
                previous: None,
            },
            out.bytes,
        ))
    }
    /// Emit one exact original key-copy slot in increasing slot order.
    pub fn copy(&mut self, copy: &OriginalSlot) -> Result<Vec<u8>, Error> {
        check_copy(self.remaining, self.previous, copy)?;
        let mut out = Encoder::new();
        copy.put(&mut out)?;
        self.remaining -= 1;
        self.previous = Some(copy.slot);
        Ok(out.bytes)
    }
    /// Require all declared copies before emitting encrypted floor chunks.
    pub fn finish(self) -> Result<(), Error> {
        require(self.remaining == 0, "floor key copies", Rule::StreamLength)
    }
}

/// Incremental clear-prefix decoder. Supply one bounded copy piece at a time.
pub struct FloorPrefixDecoder {
    /// Store key sealing the floor stream.
    pub key: KeyId,
    /// First kept slot; guides key discovery until the floor authenticates.
    pub first: Slot,
    remaining: u32,
    previous: Option<Slot>,
}
impl FloorPrefixDecoder {
    /// Exact initial routing length, before copy pieces.
    pub const HEADER_LENGTH: usize = 31;
    /// Read exactly the initial routing and bounded copy count.
    pub fn start(bytes: &[u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, 39)?;
        let mut input = Decoder::new(&bytes[3..])?;
        let key = KeyId::get(&mut input)?;
        let first = Slot::get(&mut input)?;
        let remaining = u32::get(&mut input)?;
        bound(remaining as usize, MAX_ITEMS, "floor key copies")?;
        input.finish()?;
        Ok(Self {
            key,
            first,
            remaining,
            previous: None,
        })
    }
    /// Number of copy pieces still needed, before the encrypted body begins.
    pub fn remaining(&self) -> u32 {
        self.remaining
    }
    /// Bound one copy's slot and length fields before allocating its body.
    pub fn copy_length(&self, bytes: &[u8]) -> Result<usize, Error> {
        require(
            self.remaining > 0,
            "floor key copy count",
            Rule::StreamLength,
        )?;
        let length = bytes.get(8..12).ok_or(Error::Truncated)?;
        let length = u32::from_be_bytes(length.try_into().expect("four bytes")) as usize;
        bound(length, crate::sequence::MAX_BOUNDED_SLOT, "floor key copy")?;
        Ok(12 + length)
    }
    /// Decode one complete original slot, keeping its signed bytes intact.
    pub fn copy(&mut self, bytes: &[u8]) -> Result<OriginalSlot, Error> {
        let length = self.copy_length(bytes)?;
        if bytes.len() < length {
            return Err(Error::Truncated);
        }
        if bytes.len() > length {
            return Err(Error::TrailingBytes);
        }
        let mut input = Decoder::new(bytes)?;
        let copy = OriginalSlot::get(&mut input)?;
        input.finish()?;
        check_copy(self.remaining, self.previous, &copy)?;
        self.remaining -= 1;
        self.previous = Some(copy.slot);
        Ok(copy)
    }
    /// Refuse a prefix missing any declared copy.
    pub fn finish(self) -> Result<(), Error> {
        require(self.remaining == 0, "floor key copies", Rule::StreamLength)
    }
}
fn check_copy(remaining: u32, previous: Option<Slot>, copy: &OriginalSlot) -> Result<(), Error> {
    require(
        remaining > 0 && previous.is_none_or(|slot| slot < copy.slot),
        "floor key-copy order",
        Rule::Order,
    )?;
    copy.validate_kind(2)
}
