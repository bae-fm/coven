//! Inner-write identities and ordering checks for sequence slots (D9).

use crate::error::{require, Error, Rule};
use crate::sequence::SlotHeader;
use crate::write::WriteHeader;

/// Exact crypto identity of an inner write, independent of its containing slot.
pub fn identity(
    store: coven_foundation::id_source::StoreId,
    write: coven_merge::WriteId,
) -> Result<String, Error> {
    crate::value::positive(write.number)?;
    Ok(format!(
        "coven/write/{store}/{}/{}",
        write.device.0, write.number
    ))
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
