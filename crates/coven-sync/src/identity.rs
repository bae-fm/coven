//! The check that this installation still owns its counters before it sends,
//! and the comparison of an occupied immutable path with this copy's bytes (§10).

use crate::{SendGate, SyncError};
use coven_crypto::{ObjectDigest, ObjectHasher};
use coven_database::{DeviceState, IdentityReservations, StoreLog};
use coven_foundation::id_source::DeviceId;
use coven_storage::{ObjectPath, Storage, StorageFailure};

/// What showed that another copy uses this device's id (§10, E5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceIdEvidence {
    /// Storage holds an own object numbered beyond this copy's reservations.
    StorageAhead,
    /// One of this device's own immutable paths holds different bytes.
    DifferentBytes,
    /// The store log keeps a replacement of this device's id.
    Replaced,
}

/// The greatest own write and entry numbers one kind of evidence shows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Ends {
    pub(crate) writes: u64,
    pub(crate) entries: u64,
}

impl Ends {
    fn combine(self, other: Self) -> Self {
        Self {
            writes: self.writes.max(other.writes),
            entries: self.entries.max(other.entries),
        }
    }
}

/// Own numbers found in storage by next-number reads and listings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counters {
    pub(crate) logs: Ends,
    pub(crate) key_copies: u64,
    pub(crate) snapshots: u64,
}

/// Initialization evidence kept for the running session: signed snapshot
/// coverage, the checked own post and own snapshot numbers. Reopening,
/// reconnecting or a reset discards it with the send gate's session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SessionEvidence {
    pub(crate) covered: Ends,
    pub(crate) posted: Ends,
    pub(crate) snapshots: u64,
}

/// One complete observation of everything §10 compares with the reservations.
/// A failed read or listing produces no evidence at all.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Evidence {
    pub(crate) listed: Counters,
    pub(crate) covered: Ends,
    pub(crate) posted: Ends,
    pub(crate) replaced: bool,
    pub(crate) removed: bool,
}

impl Evidence {
    pub(crate) fn observed(&self) -> Counters {
        Counters {
            logs: self.listed.logs.combine(self.covered).combine(self.posted),
            ..self.listed
        }
    }
}

/// The identity verdict before any send (Lean `CovenStorage.checkIdentity`
/// with custody naming this device: opening refuses any other custody).
#[derive(Debug)]
pub(crate) enum Gate {
    Send,
    Pending(SyncError),
    InUse(DeviceIdEvidence),
    Stopped,
}

/// Storage proves a counter beyond this database's reservations. Equality,
/// including an upload confirmed just before a crash, is not stale.
pub(crate) fn ahead(observed: &Counters, reserved: &IdentityReservations) -> bool {
    observed.logs.writes > reserved.writes
        || observed.logs.entries > reserved.entries
        || observed.key_copies > reserved.key_copies
        || observed.snapshots > reserved.snapshots
}

pub(crate) fn check(reserved: &IdentityReservations, scan: Result<Evidence, SyncError>) -> Gate {
    match scan {
        Err(error) => Gate::Pending(error),
        Ok(evidence) if evidence.removed => Gate::Stopped,
        Ok(evidence) if evidence.replaced => Gate::InUse(DeviceIdEvidence::Replaced),
        Ok(evidence) if ahead(&evidence.observed(), reserved) => {
            Gate::InUse(DeviceIdEvidence::StorageAhead)
        }
        Ok(_) => Gate::Send,
    }
}

/// Any kept registration naming `device` as the id it replaces stops it,
/// whichever new id it registered (D6, §10). A removal stays a removal.
pub(crate) fn replaced(log: &StoreLog, device: DeviceId) -> bool {
    log.replay
        .state
        .devices
        .get(&device)
        .is_some_and(|d| d.state == DeviceState::Replaced)
}

/// Length and SHA-256 of bytes this copy would send.
pub(crate) fn digest(bytes: &[u8]) -> (u64, ObjectDigest) {
    let mut hasher = ObjectHasher::new();
    hasher.update(bytes);
    (bytes.len() as u64, hasher.finish())
}

/// Read the complete occupant of an immutable path. A failed read keeps the
/// attempt pending; occupation alone proves nothing (§10).
pub(crate) async fn stored_digest(
    storage: &dyn Storage,
    path: &ObjectPath,
) -> Result<(u64, ObjectDigest), SyncError> {
    let mut stream = storage.stream_object(path, None).await?;
    if &stream.metadata().path != path {
        return Err(StorageFailure::Protocol
            .with_source("occupant response names another path")
            .into());
    }
    let mut hasher = ObjectHasher::new();
    let mut length = 0u64;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length += read as u64;
    }
    Ok((length, hasher.finish()))
}

/// Equal complete bytes count as stored. Different bytes on this device's own
/// path mean another copy uses its id: stop every send (§10).
pub(crate) async fn settle_occupied(
    storage: &dyn Storage,
    send: &SendGate,
    path: &ObjectPath,
    expected: (u64, ObjectDigest),
) -> Result<(), SyncError> {
    if stored_digest(storage, path).await? == expected {
        return Ok(());
    }
    tracing::warn!(
        path = path.as_str(),
        "own immutable path holds another copy's bytes"
    );
    Err(send.stop(DeviceIdEvidence::DifferentBytes))
}

#[cfg(test)]
#[path = "identity_tests.rs"]
pub(crate) mod tests;
