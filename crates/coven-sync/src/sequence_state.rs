//! The installation's actual progress, shared with transfer workers.

use crate::{DeviceIdEvidence, PendingRecord, SyncError, SyncFailure};
use coven_database::DbError;
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

/// What the app sees of this installation's sync (E5), as the one row of
/// `coven_sync_status` holds it. It outlives each connection: opening writes
/// `Disconnected` or `Stopped`, and the running loop writes the rest, each in
/// the step that changes it. An error is kept as its text, which names every
/// cause below it with secrets and URLs already cut, so a status read after
/// the process that wrote it ended still says why.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SyncStatus {
    /// No storage is set up on this device.
    Disconnected,
    /// Storage is set up and syncing is stopped; no provider client is required.
    Stopped,
    /// The last attempt could not reach storage, regardless of earlier success.
    /// The request preventing the pass received no usable provider response.
    Offline {
        /// The network failure, as the provider adapter classified it.
        failure: StorageFailure,
        /// The failure's text.
        error: String,
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
        #[serde(with = "crate::pending_encoding::slot")]
        slot: coven_merge::Slot,
        /// Why it cannot apply.
        reason: crate::PendingReason,
        /// The text of the error that blocked the slot: the cause the
        /// slot's pending record keeps. A device that opens already blocked
        /// has none until a pass of its own meets the error.
        error: Option<String>,
    },
    /// Storage refused the whole pass, or coven's own operation failed.
    Failed {
        /// What failed.
        failure: StatusFailure,
        /// The failure's text.
        error: String,
    },
}

/// What stopped a pass as a whole, as the status keeps it: a `SyncFailure`
/// without the error objects, whose text the status holds beside it (E5).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StatusFailure {
    /// A schema or stored object format is newer than this implementation.
    UpdateRequired(crate::RequiredUpdate),
    /// This device or its member has been removed.
    Removed,
    /// Another copy uses this device's id; this copy sends nothing more (§19.3).
    DeviceIdInUse(DeviceIdEvidence),
    /// The provider refused or failed a request, as its adapter classified it.
    Storage(StorageFailure),
    /// The pass finished, but this work of coven's own failed its last
    /// attempt; it retries under backoff.
    Operation(PendingRecord),
    /// Any other failure.
    Other,
}

impl SyncStatus {
    /// The status a whole-pass failure leaves.
    pub fn failed(failure: impl Into<SyncFailure>) -> Self {
        let failure = failure.into();
        let error = failure.to_string();
        let failure = match failure {
            SyncFailure::UpdateRequired(update) => StatusFailure::UpdateRequired(update),
            SyncFailure::Removed => StatusFailure::Removed,
            SyncFailure::DeviceIdInUse(evidence) => StatusFailure::DeviceIdInUse(evidence),
            SyncFailure::Storage(storage) => StatusFailure::Storage(storage.failure()),
            SyncFailure::Operation(record) => StatusFailure::Operation(*record),
            SyncFailure::Other(_) => StatusFailure::Other,
        };
        Self::Failed { failure, error }
    }

    /// The name `coven_sync_status.state` holds.
    fn state(&self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Stopped => "stopped",
            Self::Offline { .. } => "offline",
            Self::Syncing => "syncing",
            Self::Synced { .. } => "synced",
            Self::Blocked { .. } => "blocked",
            Self::Failed { .. } => "failed",
        }
    }

    /// Write this status as `coven_sync_status`'s row. A failure fails the
    /// store's handle with its cause (`DbError::StatePersistence`), so the
    /// caller stops the work that changed the status.
    pub async fn record(&self, database: &coven_database::Database) -> Result<(), DbError> {
        let status = serde_json::to_vec(self).map_err(DbError::StateEncoding)?;
        database.record_sync_status(self.state(), status).await
    }

    /// Follow `coven_sync_status` with a live query, as an app does.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn follow(
        database: &coven_database::Database,
    ) -> coven_database::test_utils::Followed<Self> {
        coven_database::test_utils::Followed::new(database.subscribe(|sql| {
            Ok(sql.query_row("SELECT status FROM coven_sync_status", [], |row| row.get(0))?)
        }))
    }
}

/// Reads `coven_sync_status.status`.
impl coven_database::types::FromSql for SyncStatus {
    fn column_result(
        value: coven_database::types::ValueRef<'_>,
    ) -> coven_database::types::FromSqlResult<Self> {
        serde_json::from_slice(value.as_blob()?)
            .map_err(|error| coven_database::types::FromSqlError::Other(Box::new(error)))
    }
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
        /// The text of the error that blocked the slot: the cause the slot's
        /// pending record keeps.
        error: Option<String>,
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
            PassFailure::Offline { error } => {
                Self::Offline { failure: error.failure(), error: error.to_string() }
            }
            PassFailure::Blocked { slot, reason, error } => Self::Blocked { slot, reason, error },
            PassFailure::Failed { error } => Self::failed(error),
        }
    }
}

/// Hears each change of this installation's sync activity (`SyncState::watch`).
pub(crate) struct SyncStatusWatch(watch::Receiver<SequenceStatus>);

impl SyncStatusWatch {
    /// Resolves at the next change; an error once the sync state is gone.
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.0.changed().await
    }
}

/// Shared state of this open installation. It carries no time-based permission.
pub struct SyncState {
    status: watch::Sender<SequenceStatus>,
    registered: Mutex<bool>,
    /// App work outside the database that the next pass serves.
    wake: tokio::sync::Notify,
    /// The intervals apps hold for idle passes, across connections.
    often: crate::sync_often::SyncOften,
}

impl SyncState {
    /// Start before a read pass has established the local binding.
    pub fn new() -> Self {
        Self {
            status: watch::channel(SequenceStatus::Opening).0,
            registered: Mutex::new(false),
            wake: tokio::sync::Notify::new(),
            often: crate::sync_often::SyncOften::new(),
        }
    }
    /// The sync activity now: progress, or what blocks or stopped it.
    pub(crate) fn status(&self) -> SequenceStatus {
        self.status.borrow().clone()
    }
    /// Hear each change of the sync activity, for work that waits until sync
    /// allows it.
    pub(crate) fn watch(&self) -> SyncStatusWatch {
        SyncStatusWatch(self.status.subscribe())
    }
    /// Observe progress and blocked causes, for a test to wait on.
    #[cfg(test)]
    pub(crate) fn subscribe(&self) -> watch::Receiver<SequenceStatus> {
        self.status.subscribe()
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
    pub(crate) fn idle_interval(&self) -> crate::sync_often::IdleInterval {
        self.often.subscribe()
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
        self.set(SequenceStatus::Stopped(SyncFailure::DeviceIdInUse(evidence)));
        SyncError::DeviceIdInUse(evidence)
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
