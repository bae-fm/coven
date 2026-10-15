//! A store's creation marker (D9, D10 `<store>/created`): the first object a
//! setup of a never-connected store writes, create-once, so at most one
//! installation creates the store at a location (§10).
//!
//! ```text
//! kind:u8 (41) | version:u16 | store:uuid | device:DeviceId
//! ```
//!
//! It is neither sealed nor signed: a setup compares the stored bytes with its
//! own encoding, and storage that forges a marker can only refuse setups.

use crate::wire::{Decoder, Encoder, Wire};
use crate::Error;
use coven_foundation::id_source::{DeviceId, StoreId};

const KIND: u8 = 41;

/// The store and the device whose setup claimed the location.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreationMarker {
    /// The store being created, also named by the marker's path.
    pub store: StoreId,
    /// The installation that claimed the creation.
    pub device: DeviceId,
}

impl CreationMarker {
    /// The marker's exact stored bytes.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = Encoder::new();
        KIND.put(&mut out)?;
        crate::FORMAT_VERSION.put(&mut out)?;
        self.store.put(&mut out)?;
        self.device.put(&mut out)?;
        Ok(out.bytes)
    }

    /// Decode stored marker bytes, refusing any other kind, version or length.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, KIND)?;
        let mut input = Decoder::new(&bytes[3..])?;
        let marker = Self {
            store: StoreId::get(&mut input)?,
            device: DeviceId::get(&mut input)?,
        };
        input.finish()?;
        Ok(marker)
    }
}

#[cfg(test)]
#[path = "creation_marker_tests.rs"]
mod tests;
