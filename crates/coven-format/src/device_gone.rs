//! The marker a device gone for good leaves among its join requests (D8, D10).

use crate::error::{exact_length, Error};

/// What an admin's device creates at `<store>/join-requests/<device>/gone`
/// once it applies the entry that stopped `<device>` for good, before it
/// deletes the requests waiting there (§13.2). A joining device that finds
/// it, before sending its request or while its request waits, ends its join
/// as declined. It is never deleted: a request can arrive at any time, and
/// the device it waits for never will answer. It holds nothing but its kind
/// and format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceGone;

const KIND: u8 = crate::kinds::GONE_DEVICE;
const LENGTH: usize = 3;

impl DeviceGone {
    /// The marker's complete bytes: its kind and format.
    pub fn encode(&self) -> Vec<u8> {
        crate::FormatVersion::CURRENT.prefix(KIND).to_vec()
    }

    /// Read a marker. Bytes of another kind, such as a join request or an
    /// ended invite's marker, fail with `UnknownTag`.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        crate::sealed::prefix(bytes, KIND)?;
        exact_length(bytes.len(), LENGTH)?;
        Ok(Self)
    }
}

#[cfg(test)]
#[path = "device_gone_tests.rs"]
mod tests;
