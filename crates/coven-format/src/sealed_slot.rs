//! Clear slot routing and bounded slot envelopes (D9). Writes stream separately.

use crate::admin::StoreChange;
use crate::error::{bound, exact_length, require, Error, Rule};
use crate::sealed;
use crate::sealed_key::{CopyBox, KeyCopyPrefix};
use crate::sequence::{SlotFrame, MAX_BOUNDED_SLOT};
use crate::wire::{Decoder, Encoder, Wire};
use coven_crypto::{
    CircleKey, Signature, StoreKey, KEY_BOX_HEAD_LEN, KEY_BOX_TAG_LEN,
    SEALED_OBJECT_CHUNK_OVERHEAD, SIGNATURE_LEN,
};
use coven_foundation::id_source::KeyId;

/// Where a key box sits in its slot's clear prefix, which its context binds
/// (D9, D11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoxPlace {
    /// The one box of a key-copy slot.
    KeyCopy,
    /// The box at this index of an admission's copies.
    Admission(u32),
}

/// Slot kind and the routing which exists only for key-copy slots and
/// admissions.
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
    /// The entry admitting a member and registering their first device, with
    /// that member's boxes, all to one recipient and sealing key, in
    /// increasing `(audience, key)` order (§13.2).
    Admission(Vec<CopyBox>),
}
impl SlotKind {
    /// The D9 kind byte.
    pub fn tag(&self) -> u8 {
        match self {
            Self::Admin => 0,
            Self::Writes => 1,
            Self::KeyCopy(_) => 2,
            Self::Announcement => 3,
            Self::Admission(_) => 4,
        }
    }
    /// The key boxes the clear prefix carries, each with its place.
    pub fn boxes(&self) -> impl Iterator<Item = (BoxPlace, &CopyBox)> {
        let (single, many) = match self {
            Self::KeyCopy(copy) => (Some(copy), &[][..]),
            Self::Admission(copies) => (None, &copies[..]),
            Self::Admin | Self::Writes | Self::Announcement => (None, &[][..]),
        };
        single.map(|copy| (BoxPlace::KeyCopy, copy)).into_iter().chain(
            many.iter().enumerate().map(|(index, copy)| (BoxPlace::Admission(index as u32), copy)),
        )
    }
    /// Check the opened second frame against the clear kind. Only an
    /// admission holds an admit-member or recover-member change, and it
    /// holds nothing else (D6).
    pub fn check_frame(&self, frame: &SlotFrame) -> Result<(), Error> {
        let admits = matches!(
            frame,
            SlotFrame::Admin(StoreChange::AdmitMember { .. } | StoreChange::RecoverMember { .. })
        );
        require(
            match (self, frame) {
                (Self::Admin, SlotFrame::Admin(_)) => !admits,
                (Self::Admission(_), SlotFrame::Admin(_)) => admits,
                (Self::KeyCopy(_), SlotFrame::KeyCopy)
                | (Self::Announcement, SlotFrame::Announcement(_)) => true,
                _ => false,
            },
            "slot kind",
            Rule::Kind,
        )
    }
}

/// An admission's boxes: at least one, all to one recipient and sealing key,
/// in strictly increasing `(audience, key)` order (D9).
fn check_copies(copies: &[CopyBox]) -> Result<(), Error> {
    require(!copies.is_empty(), "admission copies", Rule::Required)?;
    let first = &copies[0];
    require(
        copies.iter().all(|copy| {
            copy.routing.member == first.routing.member
                && copy.routing.sealing == first.routing.sealing
        }),
        "admission copy recipient",
        Rule::Recipient,
    )?;
    require(
        copies.windows(2).all(|pair| {
            (&pair[0].routing.audience, pair[0].routing.key)
                < (&pair[1].routing.audience, pair[1].routing.key)
        }),
        "admission copy order",
        Rule::Order,
    )
}

/// The complete clear routing, without any device, claim number or part audience.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotObjectPrefix {
    /// The recorded format of this claim attempt.
    pub format: crate::FormatVersion,
    /// The store key sealing the body.
    pub key: KeyId,
    /// Kind and any key boxes.
    pub kind: SlotKind,
}
impl SlotObjectPrefix {
    /// Longest prefix of any slot but an admission, including a circle key
    /// copy. An admission's prefix is bounded only by its slot's 4 MiB.
    pub const MAX_LENGTH: usize = Self::FIXED_LEN
        + KeyCopyPrefix::CIRCLE_LEN
        + KEY_BOX_HEAD_LEN
        + 4
        + CircleKey::ENCODED_LEN
        + KEY_BOX_TAG_LEN;
    /// Kind, version, slot kind, key and the copy flag, before any box.
    const FIXED_LEN: usize = 1 + 2 + 1 + 16 + 1;
    /// Encode the exact associated-data prefix, including any anonymous box.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = self.start()?;
        match &self.kind {
            SlotKind::KeyCopy(copy) => {
                1u8.put(&mut out)?;
                copy.put(&mut out)?;
            }
            SlotKind::Admission(copies) => {
                check_copies(copies)?;
                copies.put(&mut out)?;
            }
            SlotKind::Admin | SlotKind::Writes | SlotKind::Announcement => 0u8.put(&mut out)?,
        }
        Ok(out.bytes)
    }
    fn start(&self) -> Result<Encoder, Error> {
        let mut out = Encoder::new();
        out.bytes(&self.format.prefix(crate::kinds::SEQUENCE_SLOT))?;
        self.kind.tag().put(&mut out)?;
        self.key.put(&mut out)?;
        Ok(out)
    }
    /// Box context through the recipient's sealing key, before generating its
    /// ephemeral key and ciphertext: the slot's fixed fields, the box's place
    /// and its routing. This context binds the slot path in crypto.
    pub fn copy_context(
        format: crate::FormatVersion,
        key: KeyId,
        place: BoxPlace,
        routing: &KeyCopyPrefix,
    ) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        out.bytes(&format.prefix(crate::kinds::SEQUENCE_SLOT))?;
        match place {
            BoxPlace::KeyCopy => {
                2u8.put(&mut out)?;
                key.put(&mut out)?;
                1u8.put(&mut out)?;
            }
            BoxPlace::Admission(index) => {
                4u8.put(&mut out)?;
                key.put(&mut out)?;
                index.put(&mut out)?;
            }
        }
        routing.put(&mut out)?;
        Ok(out.bytes)
    }
    /// Whether the prefix `bytes` begins is an admission's, whose length only
    /// its whole bounded slot bounds. It reads the first four bytes.
    pub fn admission(bytes: &[u8]) -> Result<bool, Error> {
        sealed::prefix(bytes, crate::kinds::SEQUENCE_SLOT)?;
        Ok(*bytes.get(3).ok_or(Error::Truncated)? == 4)
    }
    /// Bound the clear prefix before reading its boxes. No length supplied by
    /// a peer can enlarge the fixed store/circle box sizes.
    pub fn length(bytes: &[u8]) -> Result<usize, Error> {
        sealed::prefix(bytes, crate::kinds::SEQUENCE_SLOT)?;
        let kind = *bytes.get(3).ok_or(Error::Truncated)?;
        require(kind <= 4, "slot kind", Rule::Kind)?;
        if kind == 4 {
            let start = Self::FIXED_LEN - 1;
            let count = bytes.get(start..start + 4).ok_or(Error::Truncated)?;
            let count = u32::from_be_bytes(count.try_into().expect("four bytes")) as usize;
            require(count > 0, "admission copies", Rule::Required)?;
            bound(count, crate::wire::MAX_ITEMS, "admission copies")?;
            let mut end = start + 4;
            for _ in 0..count {
                end = box_end(bytes, end)?;
            }
            return Ok(end);
        }
        let copy = *bytes.get(Self::FIXED_LEN - 1).ok_or(Error::Truncated)?;
        require(copy == u8::from(kind == 2), "slot copy routing", Rule::Kind)?;
        if kind != 2 {
            return Ok(Self::FIXED_LEN);
        }
        box_end(bytes, Self::FIXED_LEN)
    }
    /// Decode exactly the prefix, without authenticating routing.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        exact_length(bytes.len(), Self::length(bytes)?)?;
        let mut input = Decoder::new(&bytes[4..])?;
        let key = KeyId::get(&mut input)?;
        let kind = match bytes[3] {
            4 => {
                let copies = Vec::<CopyBox>::get(&mut input)?;
                check_copies(&copies)?;
                SlotKind::Admission(copies)
            }
            tag => {
                let copy = Option::<CopyBox>::get(&mut input)?;
                match tag {
                    0 => SlotKind::Admin,
                    1 => SlotKind::Writes,
                    2 => SlotKind::KeyCopy(copy.expect("copy tag checked")),
                    3 => SlotKind::Announcement,
                    _ => unreachable!("kind checked"),
                }
            }
        };
        input.finish()?;
        Ok(Self { format: crate::FormatVersion::decode(bytes)?, key, kind })
    }
    /// Whether the complete bounded slot with this prefix and `plaintext`
    /// bytes of frames stays within its 4-MiB limit, signature included (D2).
    /// An operation checks it before it claims anything.
    pub fn fits(&self, plaintext: usize) -> Result<bool, Error> {
        Ok(bound(
            Self::bounded_size(self.encode()?.len(), plaintext + SEALED_OBJECT_CHUNK_OVERHEAD),
            MAX_BOUNDED_SLOT,
            "slot object",
        )
        .is_ok())
    }
    /// The complete bounded slot's size: its prefix, the chunk's length
    /// field and bytes, and the signature.
    fn bounded_size(prefix: usize, chunk: usize) -> usize {
        prefix + 4 + chunk + SIGNATURE_LEN
    }
    /// Frame the sole sealed chunk of an admin, admission, copy or
    /// announcement slot.
    /// Checks the complete 4-MiB limit including the eventual signature.
    pub fn encode_chunk(&self, chunk: &[u8]) -> Result<Vec<u8>, Error> {
        require(!matches!(self.kind, SlotKind::Writes), "bounded slot kind", Rule::Kind)?;
        let mut bytes = self.encode()?;
        let length =
            chunk.len().checked_sub(SEALED_OBJECT_CHUNK_OVERHEAD).ok_or(Error::Truncated)?;
        require(length >= 2 * crate::FRAME_PREFIX_LEN, "slot frames", Rule::Chunk)?;
        bound(Self::bounded_size(bytes.len(), chunk.len()), MAX_BOUNDED_SLOT, "slot object")?;
        bytes.extend_from_slice(&sealed::encode_chunk(chunk)?);
        Ok(bytes)
    }
}

/// Where the box that starts at `start` ends: its routing, whose audience
/// fixes the key material's length, the ephemeral key and nonce, the declared
/// ciphertext length, the ciphertext and the tag.
fn box_end(bytes: &[u8], start: usize) -> Result<usize, Error> {
    let (routing, ciphertext) = match *bytes.get(start).ok_or(Error::Truncated)? {
        0 => (KeyCopyPrefix::STORE_LEN, StoreKey::ENCODED_LEN),
        1 => (KeyCopyPrefix::CIRCLE_LEN, CircleKey::ENCODED_LEN),
        tag => return Err(Error::UnknownTag { field: "audience", tag }),
    };
    let offset = start + routing + KEY_BOX_HEAD_LEN;
    let declared = bytes.get(offset..offset + 4).ok_or(Error::Truncated)?;
    require(
        u32::from_be_bytes(declared.try_into().expect("four bytes")) as usize == ciphertext,
        "key box ciphertext",
        Rule::StreamLength,
    )?;
    Ok(offset + 4 + ciphertext + KEY_BOX_TAG_LEN)
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
