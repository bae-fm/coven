//! Clear slot routing and bounded slot envelopes (D9). Writes stream separately.

use crate::error::{bound, require, Error, Rule};
use crate::sealed;
use crate::sealed_key::{CopyBox, KeyCopyPrefix};
use crate::sequence::{SlotFrame, MAX_BOUNDED_SLOT};
use crate::wire::{Decoder, Encoder, Wire};
use coven_crypto::{Signature, SEALED_OBJECT_CHUNK_OVERHEAD};
use coven_foundation::id_source::KeyId;

/// Slot kind and the routing which exists only for key-copy slots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotKind {
    /// One admin operation.
    Admin,
    /// One device's queued inner writes.
    Writes,
    /// A recipient's anonymous box.
    KeyCopy(CopyBox),
    /// An immutable snapshot's announcement.
    Announcement,
}
impl SlotKind {
    /// The D9 kind byte.
    pub fn tag(&self) -> u8 {
        match self {
            Self::Admin => 0,
            Self::Writes => 1,
            Self::KeyCopy(_) => 2,
            Self::Announcement => 3,
        }
    }
    /// Check the opened second frame against the clear kind.
    pub fn check_frame(&self, frame: &SlotFrame) -> Result<(), Error> {
        require(
            matches!(
                (self, frame),
                (Self::Admin, SlotFrame::Admin(_))
                    | (Self::KeyCopy(_), SlotFrame::KeyCopy)
                    | (Self::Announcement, SlotFrame::Announcement(_))
            ),
            "slot kind",
            Rule::Kind,
        )
    }
}

/// The complete clear routing, without any device, claim number or part audience.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotObjectPrefix {
    /// The recorded format of this claim attempt.
    pub format: crate::FormatVersion,
    /// The store key sealing the body.
    pub key: KeyId,
    /// Kind and optional key-copy box.
    pub kind: SlotKind,
}
impl SlotObjectPrefix {
    /// Longest prefix, including a circle key copy.
    pub const MAX_LENGTH: usize = 21 + 81 + 56 + 4 + 64 + 16;
    /// Encode the exact associated-data prefix, including any anonymous box.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = self.start()?;
        match &self.kind {
            SlotKind::KeyCopy(copy) => {
                1u8.put(&mut out)?;
                copy.put(&mut out)?;
            }
            _ => 0u8.put(&mut out)?,
        }
        Ok(out.bytes)
    }
    fn start(&self) -> Result<Encoder, Error> {
        let mut out = Encoder::new();
        33u8.put(&mut out)?;
        self.format.number().put(&mut out)?;
        self.kind.tag().put(&mut out)?;
        self.key.put(&mut out)?;
        Ok(out)
    }
    /// Box context through the recipient's sealing key, before generating its
    /// ephemeral key and ciphertext. This context binds the slot path in crypto.
    pub fn copy_context(
        format: crate::FormatVersion,
        key: KeyId,
        routing: &KeyCopyPrefix,
    ) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        33u8.put(&mut out)?;
        format.number().put(&mut out)?;
        2u8.put(&mut out)?;
        key.put(&mut out)?;
        1u8.put(&mut out)?;
        routing.put(&mut out)?;
        Ok(out.bytes)
    }
    /// Bound the clear prefix before reading its box. No length supplied by a
    /// peer can enlarge the fixed store/circle box sizes.
    pub fn length(bytes: &[u8]) -> Result<usize, Error> {
        sealed::prefix(bytes, 33)?;
        let kind = *bytes.get(3).ok_or(Error::Truncated)?;
        require(kind <= 3, "slot kind", Rule::Kind)?;
        let copy = *bytes.get(20).ok_or(Error::Truncated)?;
        require(copy == u8::from(kind == 2), "slot copy routing", Rule::Kind)?;
        if kind != 2 {
            return Ok(21);
        }
        let (routing, ciphertext) = match *bytes.get(21).ok_or(Error::Truncated)? {
            0 => (65, 48),
            1 => (81, 64),
            tag => return Err(Error::UnknownTag { field: "audience", tag }),
        };
        let offset = 21 + routing + 56;
        let declared = bytes.get(offset..offset + 4).ok_or(Error::Truncated)?;
        require(
            u32::from_be_bytes(declared.try_into().expect("four bytes")) == ciphertext,
            "key box ciphertext",
            Rule::StreamLength,
        )?;
        Ok(offset + 4 + ciphertext as usize + 16)
    }
    /// Decode exactly the prefix, without authenticating routing.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let length = Self::length(bytes)?;
        if bytes.len() < length {
            return Err(Error::Truncated);
        }
        if bytes.len() > length {
            return Err(Error::TrailingBytes);
        }
        let mut input = Decoder::new(&bytes[4..])?;
        let key = KeyId::get(&mut input)?;
        let copy = Option::<CopyBox>::get(&mut input)?;
        let kind = match bytes[3] {
            0 => SlotKind::Admin,
            1 => SlotKind::Writes,
            2 => SlotKind::KeyCopy(copy.expect("copy tag checked")),
            3 => SlotKind::Announcement,
            _ => unreachable!("kind checked"),
        };
        input.finish()?;
        Ok(Self { format: crate::FormatVersion::decode(bytes)?, key, kind })
    }
    /// Frame the sole sealed chunk of an admin, copy or announcement slot.
    /// Checks the complete 4-MiB limit including the eventual signature.
    pub fn encode_chunk(&self, chunk: &[u8]) -> Result<Vec<u8>, Error> {
        require(!matches!(self.kind, SlotKind::Writes), "bounded slot kind", Rule::Kind)?;
        let mut bytes = self.encode()?;
        let length =
            chunk.len().checked_sub(SEALED_OBJECT_CHUNK_OVERHEAD).ok_or(Error::Truncated)?;
        require(length >= 2 * crate::FRAME_PREFIX_LEN, "slot frames", Rule::Chunk)?;
        bound(bytes.len() + 4 + chunk.len() + 64, MAX_BOUNDED_SLOT, "slot object")?;
        bytes.extend_from_slice(&sealed::encode_chunk(chunk)?);
        Ok(bytes)
    }
}

/// One complete bounded slot envelope; cryptographic verification is separate.
#[derive(Debug)]
pub struct BoundedSlot<'a> {
    /// Clear routing covered by both chunk authentication and final signature.
    pub prefix: SlotObjectPrefix,
    /// Stored nonce, ciphertext and tag.
    pub chunk: &'a [u8],
    /// Signature over every preceding byte, bound to the slot path.
    pub signature: Signature,
}
impl<'a> BoundedSlot<'a> {
    /// Decode without allocating the body.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, Error> {
        bound(bytes.len(), MAX_BOUNDED_SLOT, "slot object")?;
        let prefix_length = SlotObjectPrefix::length(bytes)?;
        let prefix = SlotObjectPrefix::decode(bytes.get(..prefix_length).ok_or(Error::Truncated)?)?;
        require(!matches!(prefix.kind, SlotKind::Writes), "bounded slot kind", Rule::Kind)?;
        let body = &bytes[prefix_length..];
        let length = sealed::chunk_length(body, MAX_BOUNDED_SLOT)?;
        let chunk = body.get(4..length).ok_or(Error::Truncated)?;
        require(
            chunk.len() >= SEALED_OBJECT_CHUNK_OVERHEAD + 2 * crate::FRAME_PREFIX_LEN,
            "slot frames",
            Rule::Chunk,
        )?;
        let signature = sealed::signature(body.get(length..).ok_or(Error::Truncated)?)?;
        Ok(Self { prefix, chunk, signature })
    }
    /// Reproduce the original sealed envelope.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.prefix.encode_chunk(self.chunk)?;
        bytes.extend_from_slice(self.signature.as_bytes());
        Ok(bytes)
    }
}

#[cfg(test)]
#[path = "sealed_slot_tests.rs"]
mod tests;
