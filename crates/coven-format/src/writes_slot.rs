//! A writes slot's body codec, inner-write identities and ordering checks
//! (D9).

use crate::error::{exact_length, require, Error, Rule};
use crate::path::ObjectPath;
use crate::sequence::SlotHeader;
use crate::value::positive;
use crate::write::WriteHeader;

/// The body's head: the slot header frame, then how many inner writes follow.
pub const HEAD_LEN: usize = SlotHeader::ENCODED_LEN + 8;

/// Encode the head of a body holding `count` inner writes, at least one.
pub fn encode_head(header: &SlotHeader, path: &ObjectPath, count: u64) -> Result<Vec<u8>, Error> {
    positive(count)?;
    let mut bytes = header.encode(path)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    Ok(bytes)
}

/// Decode exactly a body's head: its slot header and positive write count.
pub fn decode_head(bytes: &[u8], path: &ObjectPath) -> Result<(SlotHeader, u64), Error> {
    exact_length(bytes.len(), HEAD_LEN)?;
    let (header, count) = bytes.split_at(SlotHeader::ENCODED_LEN);
    let header = SlotHeader::decode(header, path)?;
    let count = u64::from_be_bytes(count.try_into().expect("eight bytes"));
    positive(count)?;
    Ok((header, count))
}

/// The length field before each inner write.
pub fn encode_write_length(length: u64) -> [u8; 8] {
    length.to_be_bytes()
}

/// Decode an inner write's length field, refusing an empty write or one
/// longer than the `remaining` body bytes after the field.
pub fn decode_write_length(bytes: [u8; 8], remaining: u64) -> Result<u64, Error> {
    let length = u64::from_be_bytes(bytes);
    require(length > 0 && length <= remaining, "inner write length", Rule::StreamLength)?;
    Ok(length)
}

/// Exact crypto identity of an inner write, independent of its containing slot.
pub fn identity(
    store: coven_foundation::id_source::StoreId,
    write: coven_merge::WriteId,
) -> Result<String, Error> {
    crate::value::positive(write.number)?;
    Ok(format!("coven/write/{store}/{}/{}", write.device.0, write.number))
}

/// Check one authenticated inner header's device, causal prefix and increasing
/// number within a writes slot.
pub fn check_write(
    slot: &SlotHeader,
    previous: Option<u64>,
    header: &WriteHeader,
) -> Result<(), Error> {
    slot.check_write(header)?;
    require(
        previous.is_none_or(|number| number < header.position.number),
        "batch write order",
        Rule::Order,
    )
}

#[cfg(test)]
#[path = "writes_slot_tests.rs"]
mod tests;
