//! The floor's encrypted header and original announcement frames (D8).

use crate::error::{require, Error, Rule};
use crate::sequence::MAX_BOUNDED_SLOT;
use crate::value::Slot;
use crate::wire::{decode_frame, get_blob, put_blob, wire_struct, Decoder, Encoder, Wire};
use coven_foundation::id_source::DeviceId;
use std::collections::BTreeSet;

/// Exact original slot evidence, verified at its original slot path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginalSlot {
    /// The original slot, even if retention has since deleted its path.
    pub slot: Slot,
    /// Complete original sealed bytes and signature.
    pub object: Vec<u8>,
}
impl Wire for OriginalSlot {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.slot.put(out)?;
        put_blob(&self.object, out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self {
            slot: Wire::get(input)?,
            object: get_blob(input)?,
        })
    }
}
impl OriginalSlot {
    /// Check the expected clear slot kind and bounded envelope. Authentication
    /// and the encrypted header's path binding are checked when opening it.
    pub fn validate_kind(&self, kind: u8) -> Result<(), Error> {
        crate::value::positive(self.slot.0)?;
        crate::error::bound(self.object.len(), MAX_BOUNDED_SLOT, "retained slot")?;
        let object = crate::sealed_slot::BoundedSlot::decode(&self.object)?;
        require(
            object.prefix.kind.tag() == kind,
            "retained slot kind",
            Rule::Kind,
        )
    }
}

/// Header of the signed floor stream. Announcements follow in this order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FloorHeader {
    /// Device signing the floor in the state at `at`.
    pub author: DeviceId,
    /// Signer's applied slot, covering every named announcement.
    pub at: u64,
    /// First slot the sequence retains.
    pub first: Slot,
    /// One announcement per required audience, in audience order, store first.
    pub snapshots: Vec<Slot>,
}
wire_struct!(FloorHeader, author, at, first, snapshots);
impl FloorHeader {
    fn validate(&self) -> Result<(), Error> {
        crate::value::positive(self.first.0)?;
        require(
            !self.snapshots.is_empty(),
            "floor snapshots",
            Rule::Required,
        )?;
        require(
            self.first.0 - 1 <= self.at
                && self
                    .snapshots
                    .iter()
                    .all(|slot| slot.0 > 0 && slot.0 <= self.at),
            "floor coverage",
            Rule::Coverage,
        )?;
        let unique: BTreeSet<_> = self.snapshots.iter().collect();
        require(
            unique.len() == self.snapshots.len(),
            "floor announcements",
            Rule::Order,
        )
    }
    /// Encode the kind-12 header frame.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        crate::encode_frame(12, self)
    }
    /// Decode exactly the kind-12 header frame.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 12, "floor header", Rule::Kind)?;
        let header = Self::get(&mut input)?;
        input.finish()?;
        header.validate()?;
        Ok(header)
    }
}

/// Incremental floor frame codec; no bound is imposed on the complete stream.
pub struct FloorFrames {
    header: FloorHeader,
    next: usize,
    ended: bool,
}
impl FloorFrames {
    /// Begin encoding with the validated floor header.
    pub fn start(header: FloorHeader) -> Result<(Self, Vec<u8>), Error> {
        let frame = header.encode()?;
        Ok((
            Self {
                header,
                next: 0,
                ended: false,
            },
            frame,
        ))
    }
    /// Start decoding from its initial header frame, checking the clear prefix.
    pub fn decode_header(bytes: &[u8], first: Slot) -> Result<Self, Error> {
        let header = FloorHeader::decode(bytes)?;
        require(header.first == first, "floor first", Rule::Coverage)?;
        Ok(Self {
            header,
            next: 0,
            ended: false,
        })
    }
    /// Unauthenticated header; verify the floor before granting authority.
    pub fn header(&self) -> &FloorHeader {
        &self.header
    }
    fn accept(&mut self, slot: &OriginalSlot) -> Result<(), Error> {
        slot.validate_kind(3)?;
        require(
            !self.ended && self.header.snapshots.get(self.next) == Some(&slot.slot),
            "floor announcement order",
            Rule::Order,
        )?;
        self.next += 1;
        Ok(())
    }
    /// Encode the next original announcement, checking header order.
    pub fn announcement(&mut self, slot: &OriginalSlot) -> Result<Vec<u8>, Error> {
        let bytes = crate::encode_frame(6, slot)?;
        self.accept(slot)?;
        Ok(bytes)
    }
    /// Encode the required end frame after all announcements.
    pub fn end(&mut self) -> Result<Vec<u8>, Error> {
        require(
            !self.ended && self.next == self.header.snapshots.len(),
            "floor end",
            Rule::SnapshotSequence,
        )?;
        self.ended = true;
        crate::encode_frame_with(7, |_| Ok(()))
    }
    /// Decode one announcement or the end marker, preserving exact evidence.
    pub fn frame(&mut self, bytes: &[u8]) -> Result<Option<OriginalSlot>, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        if kind == 7 {
            input.finish()?;
            require(
                !self.ended && self.next == self.header.snapshots.len(),
                "floor end",
                Rule::SnapshotSequence,
            )?;
            self.ended = true;
            return Ok(None);
        }
        require(kind == 6, "floor announcement", Rule::Kind)?;
        let slot = OriginalSlot::get(&mut input)?;
        input.finish()?;
        self.accept(&slot)?;
        Ok(Some(slot))
    }
    /// Require the end marker before accepting the stream.
    pub fn finish(&self) -> Result<(), Error> {
        if self.ended {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
}

#[cfg(test)]
#[path = "floor_tests.rs"]
mod tests;
