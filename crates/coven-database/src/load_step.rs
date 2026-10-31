//! The steps of loading a store from storage onto a new installation, which
//! a host draws a bar for (§13.4, api.md E10).

use coven_merge::Audience;

/// A step of loading a store from storage (§13.4), in the order they run. A
/// step runs only when it has work to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadStep {
    /// Reading slots for copies of this member's keys: from the floor's
    /// first kept slot, or from slot 1 without a floor, or, for a join, from
    /// its invite, to the end of the sequence. It runs only when the floor
    /// holds no copy for this member. Done counts the slots passed and total
    /// the end found once the first slot is read; slots that land while it
    /// reads raise the total.
    FindingKeys,
    /// Streaming one of the floor's snapshots from storage, staging its rows
    /// as they arrive: the store's first, then each circle's this member
    /// reads. Done and total count bytes of the snapshot's object.
    Streaming(Audience),
    /// Installing the staged snapshots' rows. Done and total count tables:
    /// installing passes over each snapshot's tables six times, to copy
    /// their rows, check them three ways, write their merge records and
    /// check that every row has one, and each table counts once in each pass.
    Installing,
    /// Applying the slots from the snapshots' coverage to the load's target,
    /// in the same transaction. Done and total count slots.
    InstallingSlots,
    /// Reading the slots after that target, to the end of the sequence.
    /// Done and total count slots; the total is the end found as the step
    /// begins, and grows if slots land while it reads.
    Reading,
    /// Applying the slots it read, one transaction each. Done and total
    /// count slots.
    Applying,
}

#[cfg(test)]
#[path = "load_step_tests.rs"]
mod tests;
