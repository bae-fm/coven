//! Authored causality and first-claim ordering in the store sequence.

use crate::{Slot, WriteId, WritePast};

/// Publication order of a unique value's input setter (§8.5).
/// Local writes follow every published claim, in their original number order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClaimOrder {
    /// A counted write in the shared sequence.
    Published {
        /// Its containing slot.
        slot: Slot,
        /// Its author's write number within the slot.
        number: u64,
    },
    /// An original still in this installation's queue.
    Local {
        /// Its local write number.
        number: u64,
    },
}

/// The authored prefix plus the author's earlier originals (§7.1).
/// A later publication never enlarges that prefix.
pub fn had_read(reader: WriteId, read_through: u64, earlier: WriteId, slot: Option<Slot>) -> bool {
    (reader.device == earlier.device && earlier.number < reader.number)
        || slot.is_some_and(|slot| slot.0 > 0 && slot.0 <= read_through)
}

/// A sequence past backed by the consumer's write-to-slot lookup.
/// `latest` names the greatest timestamp in the authored prefix and own past,
/// not necessarily the write in its last slot.
pub struct SlotPast<F> {
    reader: WriteId,
    read_through: u64,
    slot: F,
    latest: Option<WriteId>,
}

impl<F: Fn(&WriteId) -> Option<Slot>> SlotPast<F> {
    /// Construct the past from authored metadata and its timestamp maximum.
    pub fn new(reader: WriteId, read_through: u64, slot: F, latest: Option<WriteId>) -> Self {
        Self {
            reader,
            read_through,
            slot,
            latest,
        }
    }
}

impl<F: Fn(&WriteId) -> Option<Slot>> WritePast for SlotPast<F> {
    fn contains(&self, write: &WriteId) -> bool {
        had_read(self.reader, self.read_through, *write, (self.slot)(write))
    }

    fn frontier(&self) -> impl Iterator<Item = &WriteId> {
        self.latest.iter()
    }
}

#[cfg(test)]
#[path = "sequence_tests.rs"]
mod tests;
