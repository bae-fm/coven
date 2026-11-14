//! The installation's actual progress, shared with transfer workers.

use crate::{DeviceIdEvidence, PendingRecord, SyncError, SyncFailure};
use coven_database::PassMove;
use coven_foundation::locks::lock_unpoisoned;
use coven_storage::{ObjectPath, PublicationGate, StorageError, StorageFailure};
use std::collections::BTreeSet;
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

/// Hears each change of this installation's sync activity (`SequenceState::watch`).
pub(crate) struct SequenceStatusWatch(watch::Receiver<SequenceStatus>);

impl SequenceStatusWatch {
    /// Resolves at the next change; an error once the sequence state is gone.
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.0.changed().await
    }
}

/// Hears what the running pass moves: each time the last thing it moved
/// changes between a slot it read that it hadn't and one it claimed
/// (`SequenceState::pass_begins`).
pub(crate) struct PassMoves(watch::Receiver<Option<PassMove>>);

impl PassMoves {
    /// Resolves with what the pass moved, at each move the loop hasn't heard.
    /// The loop waiting holds the sequence state the pass moves.
    pub(crate) async fn moved(&mut self) -> PassMove {
        loop {
            self.0.changed().await.expect("the sequence state outlives its pass");
            if let Some(moved) = *self.0.borrow_and_update() {
                return moved;
            }
        }
    }
}

/// A provider check's object while permitted (`SequenceState::permit_provider_check`):
/// removes it when dropped, a dropped future included.
#[must_use = "dropping the permit ends it at once"]
pub struct ProviderCheckPermit<'a> {
    checks: &'a Mutex<BTreeSet<ObjectPath>>,
    path: &'a ObjectPath,
}

impl Drop for ProviderCheckPermit<'_> {
    fn drop(&mut self) {
        lock_unpoisoned(self.checks).remove(self.path);
    }
}

/// Shared state of this open installation. It carries no time-based permission.
pub struct SequenceState {
    status: watch::Sender<SequenceStatus>,
    /// What the running pass moved last, a slot it read that it hadn't or one
    /// it claimed: what the sync state shows as syncing (§19.1).
    moving: watch::Sender<Option<PassMove>>,
    registered: Mutex<bool>,
    /// App work outside the database that the next pass serves.
    wake: tokio::sync::Notify,
    /// The intervals apps hold for idle passes, across connections.
    often: crate::sync_often::SyncOften,
    /// Fresh objects a storage setup's provider check writes while it runs,
    /// which a connection may publish whatever the sync state (§20.8, E5).
    checks: Mutex<BTreeSet<ObjectPath>>,
}

impl SequenceState {
    /// Start before a read pass has established the local binding.
    pub fn new() -> Self {
        Self {
            status: watch::channel(SequenceStatus::Opening).0,
            moving: watch::channel(None).0,
            registered: Mutex::new(false),
            wake: tokio::sync::Notify::new(),
            often: crate::sync_often::SyncOften::new(),
            checks: Mutex::new(BTreeSet::new()),
        }
    }
    /// Permit the provider check's own object until the permit drops, however
    /// the check ends.
    pub fn permit_provider_check<'a>(&'a self, path: &'a ObjectPath) -> ProviderCheckPermit<'a> {
        lock_unpoisoned(&self.checks).insert(path.clone());
        ProviderCheckPermit { checks: &self.checks, path }
    }
    /// The sync activity now: progress, or what blocks or stopped it.
    pub(crate) fn status(&self) -> SequenceStatus {
        self.status.borrow().clone()
    }
    /// Hear each change of the sync activity, for work that waits until sync
    /// allows it.
    pub(crate) fn watch(&self) -> SequenceStatusWatch {
        SequenceStatusWatch(self.status.subscribe())
    }
    /// Observe progress and blocked causes, for a test to wait on.
    #[cfg(test)]
    pub(crate) fn subscribe(&self) -> watch::Receiver<SequenceStatus> {
        self.status.subscribe()
    }
    /// A pass begins, having moved nothing yet: hear its moves.
    pub(crate) fn pass_begins(&self) -> PassMoves {
        self.moving.send_replace(None);
        PassMoves(self.moving.subscribe())
    }
    /// The running pass read a slot it hadn't, or claimed one.
    pub(crate) fn moved(&self, moved: PassMove) {
        self.moving.send_if_modified(|moving| moving.replace(moved) != Some(moved));
    }
    /// Ask the running loop for a pass, retained until it waits.
    pub(crate) fn request_pass(&self) {
        self.wake.notify_one();
    }
    /// A pass was requested for app work outside the database.
    pub(crate) async fn pass_requested(&self) {
        self.wake.notified().await;
    }
    /// Run idle passes at most `interval` apart, instead of every 30
    /// seconds, until the guard drops. With several held, the shortest wins.
    /// Connection backoff and provider cooldowns still hold every pass.
    pub fn sync_often(&self, interval: std::time::Duration) -> crate::SyncOftenGuard {
        self.often.hold(interval)
    }
    /// The wait between idle passes, and its changes.
    pub(crate) fn idle_interval(&self) -> crate::HeldIntervals {
        self.often.subscribe()
    }
    pub(crate) fn set(&self, status: SequenceStatus) {
        self.status.send_replace(status);
    }
    pub(crate) fn registered(&self, registered: bool) {
        *lock_unpoisoned(&self.registered) = registered;
    }
    pub(crate) fn files_allowed(&self) -> bool {
        matches!(&*self.status.borrow(), SequenceStatus::Synced { .. })
            && *lock_unpoisoned(&self.registered)
    }
    pub(crate) fn check(&self) -> Result<(), SyncError> {
        match &*self.status.borrow() {
            SequenceStatus::Synced { .. } => Ok(()),
            SequenceStatus::Blocked(record) => Err(SyncError::Pending(record.clone())),
            SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(evidence)) => {
                Err(SyncError::DeviceIdInUse(*evidence))
            }
            SequenceStatus::Stopped(SyncFailure::Removed) => Err(SyncError::Removed),
            SequenceStatus::Stopped(SyncFailure::NotAdmitted) => Err(SyncError::NotAdmitted),
            SequenceStatus::Stopped(failure) => Err(SyncError::Failed {
                failure: crate::LocalFailure::Operation,
                source: std::sync::Arc::new(failure.clone()),
            }),
            SequenceStatus::Opening => Err(StorageFailure::CatchUpRequired.into()),
        }
    }
    pub(crate) fn stop_copy(&self, evidence: DeviceIdEvidence) -> SyncError {
        self.set(SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(evidence)));
        SyncError::DeviceIdInUse(evidence)
    }
}
impl PublicationGate for SequenceState {
    fn check(&self, path: &ObjectPath) -> Result<(), StorageError> {
        if lock_unpoisoned(&self.checks).contains(path) {
            return Ok(());
        }
        // A join request is the bootstrap step before any sequence state
        // exists; the stuck-device rule governs only sequence-derived sends.
        if path.join_request_invite().is_some() {
            return Ok(());
        }
        if self.check().is_err()
            || (path.object_id().is_some() && !*lock_unpoisoned(&self.registered))
        {
            Err(StorageFailure::CatchUpRequired.into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "sequence_state_tests.rs"]
mod tests;
