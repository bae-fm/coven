//! The installation's actual progress, shared with transfer workers.

use crate::{DeviceIdEvidence, PendingRecord, SyncError, SyncFailure};
use coven_storage::{ObjectPath, PublicationGate, StorageError, StorageFailure};
use std::sync::Mutex;
use tokio::sync::watch;

/// Current sync activity. Reaching the read end says nothing about pending files.
#[derive(Clone, Debug)]
pub enum SyncStatus {
    /// No pass has established this session's applied end.
    Opening,
    /// The pass reached the read end at this slot.
    Synced {
        /// Last applied slot.
        through: u64,
    },
    /// A required slot prevents all later reading, reloading and sending.
    Blocked(Box<PendingRecord>),
    /// This installation cannot resume from its current identity.
    Stopped(SyncFailure),
}

/// Shared state of this open installation. It carries no time-based permission.
pub struct SyncState {
    status: watch::Sender<SyncStatus>,
    registered: Mutex<bool>,
}

impl SyncState {
    /// Start before a read pass has established the local binding.
    pub fn new() -> Self {
        Self {
            status: watch::channel(SyncStatus::Opening).0,
            registered: Mutex::new(false),
        }
    }
    /// Observe progress and blocked causes.
    pub fn subscribe(&self) -> watch::Receiver<SyncStatus> {
        self.status.subscribe()
    }
    pub(crate) fn set(&self, status: SyncStatus) {
        self.status.send_replace(status);
    }
    pub(crate) fn registered(&self, registered: bool) {
        *self.registered.lock().expect("sync state poisoned") = registered;
    }
    pub(crate) fn files_allowed(&self) -> bool {
        matches!(&*self.status.borrow(), SyncStatus::Synced { .. })
            && *self.registered.lock().expect("sync state poisoned")
    }
    pub(crate) fn check(&self) -> Result<(), SyncError> {
        match &*self.status.borrow() {
            SyncStatus::Synced { .. } => Ok(()),
            SyncStatus::Blocked(record) => Err(SyncError::Pending(record.clone())),
            SyncStatus::Stopped(SyncFailure::DeviceIdInUse(evidence)) => {
                Err(SyncError::DeviceIdInUse(*evidence))
            }
            SyncStatus::Stopped(SyncFailure::Removed) => Err(SyncError::Removed),
            SyncStatus::Stopped(failure) => Err(SyncError::Failed {
                failure: crate::LocalFailure::Operation,
                source: std::sync::Arc::new(failure.clone()),
            }),
            SyncStatus::Opening => Err(StorageFailure::CatchUpRequired.into()),
        }
    }
    pub(crate) fn stop_copy(&self, evidence: DeviceIdEvidence) -> SyncError {
        self.set(SyncStatus::Stopped(SyncFailure::DeviceIdInUse(evidence)));
        SyncError::DeviceIdInUse(evidence)
    }
}
impl Default for SyncState {
    fn default() -> Self {
        Self::new()
    }
}
impl PublicationGate for SyncState {
    fn check(&self, path: &ObjectPath) -> Result<(), StorageError> {
        if self.check().is_err()
            || (path.file_id().is_some() && !*self.registered.lock().expect("sync state poisoned"))
        {
            Err(StorageFailure::CatchUpRequired.into())
        } else {
            Ok(())
        }
    }
}
