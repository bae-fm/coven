//! The two flows a host drives end to end rather than a single call: joining a
//! device to an existing store, and restoring a store onto a new device.
//!
//! Both compose the layers below — cloud home, keys, protocol, database,
//! replication — into an operation with its own journal and its own resumable
//! terminal states, so neither belongs to any one of them. Module paths are the
//! API: `coven_domain::joining::join_with_device_pairing`,
//! `coven_domain::restoration::restore_from_code`.

pub mod joining;
pub mod restoration;

#[cfg(test)]
mod test_snapshots;
