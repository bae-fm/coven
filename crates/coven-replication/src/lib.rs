//! Replication: the sync loop, the Store authority spine over a store's
//! history, and the blob locality and tombstone machinery its cycles execute.
//!
//! Two subsystems sit at the root, and they name each other. [`sync`] runs
//! cycles: it pulls the remote history, verifies it, publishes this device's
//! commits, and drives membership, circle, and device-join operations against
//! the authority ladder. [`blob`] moves a row's blob between the local store,
//! the cache, and the cloud home, and retires the copies a retraction orphans —
//! work a sync cycle schedules and a host also drives directly.
//!
//! Neither contains the other, so neither is the crate root: a cycle stages and
//! uploads blobs, and a blob transition reads the locality the replicated rows
//! carry.

pub mod blob;
pub mod sync;
