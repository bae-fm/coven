//! The floor pointer: the small record at `<store>/floor` that names the newest
//! floor body (D8, D9). Its clear prefix carries the store key, the first kept
//! slot and the body's writer, number and digest, so a joiner holding no key
//! can find the body and read its key copies. Its encrypted header carries the
//! signer.

use crate::error::{exact_length, Error};
use crate::path::ObjectPath;
use crate::value::Slot;
use crate::wire::{decode_frame_value, wire_struct, Decoder, Encoder, Wire};
use coven_foundation::id_source::{DeviceId, KeyId, StoreId};

/// The clear routing of a floor pointer: kind 42, format, then
/// `key | first | device | number | digest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloorPointerPrefix {
    /// Store key sealing the pointer's header.
    pub key: KeyId,
    /// First kept slot; guides key discovery until the floor authenticates.
    pub first: Slot,
    /// The device that wrote the body.
    pub device: DeviceId,
    /// The body's number among that device's floor bodies.
    pub number: Slot,
    /// The digest of the body's complete bytes, signature included.
    pub body: [u8; 32],
}

impl FloorPointerPrefix {
    /// The exact length of the prefix.
    pub const LENGTH: usize = 3 + 16 + 8 + 8 + 8 + 32;

    /// Encode the exact associated-data prefix.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        out.bytes(&crate::FormatVersion::CURRENT.prefix(crate::kinds::FLOOR_POINTER))?;
        self.key.put(&mut out)?;
        self.first.put(&mut out)?;
        self.device.put(&mut out)?;
        self.number.put(&mut out)?;
        self.body.put(&mut out)?;
        Ok(out.bytes)
    }

    /// Read exactly the prefix.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, crate::kinds::FLOOR_POINTER)?;
        exact_length(bytes.len(), Self::LENGTH)?;
        let mut input = Decoder::new(&bytes[3..])?;
        let prefix = Self {
            key: KeyId::get(&mut input)?,
            first: <Slot as Wire>::get(&mut input)?,
            device: DeviceId::get(&mut input)?,
            number: <Slot as Wire>::get(&mut input)?,
            body: <[u8; 32]>::get(&mut input)?,
        };
        input.finish()?;
        Ok(prefix)
    }

    /// The path of the body this pointer names in `store`.
    pub fn body_path(&self, store: StoreId) -> ObjectPath {
        ObjectPath::floor_body(store, self.device, self.number.into())
    }
}

/// The pointer's encrypted header: the signer and the slot its history is
/// applied through. Verify the signer against the membership at `at`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloorPointerHeader {
    /// Device signing the pointer in the state at `at`.
    pub author: DeviceId,
    /// Signer's applied slot, covering the body's announcements.
    pub at: u64,
}
wire_struct!(FloorPointerHeader, author, at);

impl FloorPointerHeader {
    /// Encode the kind-19 header frame.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        crate::encode_frame(crate::kinds::FLOOR_POINTER_HEADER, self)
    }
    /// Decode exactly the kind-19 header frame.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        decode_frame_value(bytes, crate::kinds::FLOOR_POINTER_HEADER, "floor pointer header")
    }
}

#[cfg(test)]
#[path = "floor_pointer_tests.rs"]
mod tests;
