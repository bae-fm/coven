//! The installation's actual progress, shared with transfer workers.

use crate::{DeviceIdEvidence, PendingRecord, SyncError, SyncFailure};
use coven_storage::{ObjectPath, PublicationGate, StorageError, StorageFailure};
use std::sync::Mutex;
use tokio::sync::watch;

/// Current sync activity. Reaching the read end says nothing about pending files.
#[derive(Clone, Debug)]
pub enum SequenceStatus {
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

/// What the app sees of this installation's sync (E5). It outlives each
/// connection: the open store publishes `Disconnected` and `Stopped`, and the
/// running loop publishes the rest.
#[derive(Clone, Debug)]
pub enum SyncStatus {
    /// No storage is set up on this device.
    Disconnected,
    /// Storage is set up and syncing is stopped; no provider client is required.
    Stopped,
    /// The last attempt could not reach storage, regardless of earlier success.
    /// The request preventing the pass received no usable provider response.
    Offline {
        /// The network failure and its original cause.
        error: std::sync::Arc<StorageError>,
    },
    /// The initial sync is queued or a sync is running.
    Syncing,
    /// The pass reached its observed end; other subjects may still be pending.
    Synced {
        /// The store's clock when the pass finished.
        finished_at: std::time::SystemTime,
    },
    /// This slot invokes §5.4's stuck-device rule.
    Blocked {
        /// The slot that cannot apply.
        slot: coven_merge::Slot,
        /// Why it cannot apply.
        reason: crate::PendingReason,
        /// The error that blocked the slot, with every cause below it, while
        /// this process holds it. A device that opens already blocked has
        /// none: only the reason is saved.
        error: Option<std::sync::Arc<dyn std::error::Error + Send + Sync>>,
    },
    /// Storage refused the whole pass, or coven's own operation failed.
    Failed {
        /// The failure and its original cause.
        error: SyncFailure,
    },
}

/// Why a pass, or the connection attempt that begins one, failed: the
/// status it leaves (E5).
#[derive(Clone, Debug)]
pub enum PassFailure {
    /// The request preventing the pass received no usable provider response.
    Offline {
        /// The network failure and its original cause.
        error: std::sync::Arc<StorageError>,
    },
    /// This slot invokes §5.4's stuck-device rule.
    Blocked {
        /// The slot that cannot apply.
        slot: coven_merge::Slot,
        /// Why it cannot apply.
        reason: crate::PendingReason,
        /// The error that blocked the slot, with every cause below it, while
        /// this process holds it.
        error: Option<std::sync::Arc<dyn std::error::Error + Send + Sync>>,
    },
    /// Storage refused the whole pass, or coven's own operation failed.
    Failed {
        /// The failure and its original cause.
        error: SyncFailure,
    },
}

impl From<PassFailure> for SyncStatus {
    fn from(failure: PassFailure) -> Self {
        match failure {
            PassFailure::Offline { error } => Self::Offline { error },
            PassFailure::Blocked {
                slot,
                reason,
                error,
            } => Self::Blocked {
                slot,
                reason,
                error,
            },
            PassFailure::Failed { error } => Self::Failed { error },
        }
    }
}

/// Shared state of this open installation. It carries no time-based permission.
pub struct SyncState {
    status: watch::Sender<SequenceStatus>,
    sync: watch::Sender<SyncStatus>,
    invites: watch::Sender<Vec<crate::Invite>>,
    registered: Mutex<bool>,
    /// App work outside the database that the next pass serves.
    wake: tokio::sync::Notify,
}

impl SyncState {
    /// Start before a read pass has established the local binding.
    pub fn new() -> Self {
        Self {
            status: watch::channel(SequenceStatus::Opening).0,
            sync: watch::channel(SyncStatus::Disconnected).0,
            invites: watch::channel(Vec::new()).0,
            registered: Mutex::new(false),
            wake: tokio::sync::Notify::new(),
        }
    }
    /// Observe progress and blocked causes.
    pub fn subscribe(&self) -> watch::Receiver<SequenceStatus> {
        self.status.subscribe()
    }
    /// Observe what the app sees of sync, starting with the current status.
    pub fn subscribe_sync(&self) -> watch::Receiver<SyncStatus> {
        self.sync.subscribe()
    }
    /// Publish the app's view of sync.
    pub fn publish(&self, status: SyncStatus) {
        self.sync.send_replace(status);
    }
    /// This device's open invites, each with its request once one has
    /// arrived, starting with the current list (§13.2).
    pub fn subscribe_invites(&self) -> watch::Receiver<Vec<crate::Invite>> {
        self.invites.subscribe()
    }
    /// Ask the running loop for a pass, retained until it waits.
    pub(crate) fn request_pass(&self) {
        self.wake.notify_one();
    }
    /// A pass was requested for app work outside the database.
    pub(crate) async fn pass_requested(&self) {
        self.wake.notified().await;
    }
    pub(crate) fn set_invites(&self, invites: Vec<crate::Invite>) {
        self.invites.send_if_modified(|current| {
            let changed = *current != invites;
            *current = invites;
            changed
        });
    }
    pub(crate) fn set(&self, status: SequenceStatus) {
        self.status.send_replace(status);
    }
    pub(crate) fn registered(&self, registered: bool) {
        *self.registered.lock().expect("sync state poisoned") = registered;
    }
    pub(crate) fn files_allowed(&self) -> bool {
        matches!(&*self.status.borrow(), SequenceStatus::Synced { .. })
            && *self.registered.lock().expect("sync state poisoned")
    }
    pub(crate) fn check(&self) -> Result<(), SyncError> {
        match &*self.status.borrow() {
            SequenceStatus::Synced { .. } => Ok(()),
            SequenceStatus::Blocked(record) => Err(SyncError::Pending(record.clone())),
            SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(evidence)) => {
                Err(SyncError::DeviceIdInUse(*evidence))
            }
            SequenceStatus::Stopped(SyncFailure::Removed) => Err(SyncError::Removed),
            SequenceStatus::Stopped(failure) => Err(SyncError::Failed {
                failure: crate::LocalFailure::Operation,
                source: std::sync::Arc::new(failure.clone()),
            }),
            SequenceStatus::Opening => Err(StorageFailure::CatchUpRequired.into()),
        }
    }
    pub(crate) fn stop_copy(&self, evidence: DeviceIdEvidence) -> SyncError {
        self.set(SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(
            evidence,
        )));
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
        // A join request is the bootstrap step before any sequence state
        // exists; the stuck-device rule governs only sequence-derived sends.
        if path.join_request_invite().is_some() {
            return Ok(());
        }
        if self.check().is_err()
            || (path.file_id().is_some() && !*self.registered.lock().expect("sync state poisoned"))
        {
            Err(StorageFailure::CatchUpRequired.into())
        } else {
            Ok(())
        }
    }
}
