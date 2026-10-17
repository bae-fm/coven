//! Identity evidence from authenticated sequence slots (§10).

use crate::SyncError;
use coven_crypto::{ObjectDigest, ObjectHasher};
use coven_merge::Slot;
use coven_storage::{ObjectPath, Storage, StorageFailure};

/// Authenticated evidence that another installation uses this device id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceIdEvidence {
    /// This copy never attempted the bytes signed by its device at this slot.
    UnattemptedSlot {
        /// The occupied slot.
        slot: Slot,
    },
    /// A counted registration replaces this installation.
    Replaced,
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

/// A non-slot immutable artifact with other bytes fails its own attempt. Only
/// authenticated sequence claims prove another installation uses this device id.
pub(crate) async fn settle_occupied(
    storage: &dyn Storage,
    path: &ObjectPath,
    expected: (u64, ObjectDigest),
) -> Result<(), SyncError> {
    if stored_digest(storage, path).await? == expected {
        return Ok(());
    }
    Err(crate::write_object::damaged(
        path,
        crate::Refusal::WrongIdentity { cause: None },
    ))
}
