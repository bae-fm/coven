//! The sync state a status screen shows (§19.1, api.md E5), computed in one
//! read from what coven knows and never stored: whether a location is
//! recorded and its credentials held, how the last pass ended, what the
//! running pass moved, each bound upload in the queue, whether the
//! send queue holds a write, and the pending records the state reads. Each
//! fact names one candidate state or none, and when several hold the one of
//! highest rank shows, as `state` of the Lean model in
//! `spec/proofs/io/CovenIO/SyncState.lean` computes it; the differential
//! test checks this against that model's runner.
//!
//! A trailing upload never reaches the state. What it is doing is its
//! phase in the upload queue, computed from the same facts by
//! `Facts::upload_phase` as the model's `Upload.phase` in `Transfers.lean`
//! computes it.

use crate::{LastPass, LocalFailure, PendingReason, PendingRecord, StatusFailure};
use coven_database::{DbError, RecordedLocation, RetrySync};
use coven_storage::{CloudProvider, StorageFailure};
use std::time::SystemTime;

/// What a status screen shows (E5, §19.1): the sync state and when sync
/// last finished, as `sync_status()` reads it and the handle's live query
/// follows it.
#[derive(Clone, Debug, PartialEq)]
pub struct SyncStatus {
    /// The sync state.
    pub state: SyncState,
    /// When a pass last reached the end of the sequence, by the store's
    /// clock; kept across every state and never earlier than before.
    pub last_synced: Option<SystemTime>,
}

/// The sync state (§19.1). With several candidates, the first of `Removed`,
/// `NeedsReset`, `UpdateNeeded`, `SignedOut`, `NoAccess`, `FolderMissing`,
/// `ClockWrong`, `StorageFull`, `OfflineWithUnsentChanges`, `Offline`,
/// `Failed`, `Syncing`, `WaitingToSync`, `Synced` shows; `NotSynced` comes
/// before them all.
#[derive(Clone, Debug, PartialEq)]
pub enum SyncState {
    /// No storage location is recorded on this device.
    NotSynced,
    /// Work that clears by itself is under way or next in line, with
    /// nothing holding it: what moves now, else a bound file, else a write,
    /// else the next pass.
    Syncing(SyncActivity),
    /// Nothing moves, and something holds the work for a while: the last
    /// pass was held back, at a slot that can't apply yet or by a provider's
    /// cooldown, or it or a bound file waits out a retry, or a write in the
    /// send queue waits for the pass: what the pass waits for, else what the
    /// first such file waits for, else what the write waits for.
    WaitingToSync(SyncWait),
    /// The last pass reached the end of the sequence, every bound file this
    /// device uploads is stored, the send queue is empty, and nothing below
    /// holds: nothing waits. A trailing file never holds it.
    Synced,
    /// The last pass got no provider response.
    Offline(SyncCause),
    /// Offline, and the send queue holds a write or the upload queue a bound
    /// file with bytes to send.
    OfflineWithUnsentChanges(SyncCause),
    /// A location is recorded, but this device holds no credentials for it
    /// (no cause), or storage refused them.
    SignedOut {
        /// The recorded location's provider.
        provider: CloudProvider,
        /// What refused the credentials; none when this device holds none.
        cause: Option<SyncCause>,
    },
    /// Signed in, but the account isn't allowed.
    NoAccess(SyncCause),
    /// The provider's quota is used up.
    StorageFull(SyncCause),
    /// The storage folder or bucket is gone.
    FolderMissing(SyncCause),
    /// The device's clock is too far from the provider's.
    ClockWrong(SyncCause),
    /// A newer app or coven is needed.
    UpdateNeeded(SyncCause),
    /// This device or its member was removed, or a joined store's admission
    /// was refused (`NotAdmitted`, §13.2).
    Removed(SyncCause),
    /// Another copy uses this device's id, or its member keys were replaced.
    NeedsReset(SyncCause),
    /// Any other failure, or a bound file that can't be uploaded or read,
    /// whose pending record is the cause.
    Failed(SyncCause),
}

/// What Syncing is doing, as `Syncing` names it (§19.1): the first of what the
/// running pass moved last, a bound file moving or next in line, a write next
/// in line, and the next pass's turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncActivity {
    /// The running pass reads slots it hadn't, which are changes from other
    /// devices; or the next pass's turn has come, after a cooldown or a
    /// newer floor.
    Receiving,
    /// A write goes out: the pass claimed a slot for one, or one waits in the
    /// send queue with nothing holding it.
    Sending,
    /// A bound file uploads or waits its turn in the bound lane; the writes
    /// it holds go out once it is stored.
    UploadingBoundFile,
}

/// What waiting to sync waits for, as `WaitingToSync` names it: what holds
/// every transfer, the pass's and each file's alike, first; then, for a
/// bound file waiting in the upload queue, what holds that file (§19.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncWait {
    /// Sync isn't running (`start_sync`, `stop_sync`).
    NotSyncing,
    /// The provider asked every request to wait until then (§6.7).
    CoolingDown {
        /// When the cooldown ends, by the store's clock.
        until: SystemTime,
    },
    /// A slot can't apply yet; nothing goes out or comes in until it does
    /// (§5.4).
    Blocked,
    /// A failure that clears by itself and names no state of its own, on the
    /// pass, a slot's application, coven's own operation or a bound file's
    /// attempt, has failed, and the next try is then, or at the person's
    /// retry (§6.7); the pending list says why.
    Retrying {
        /// When its backoff ends, by the store's clock.
        at: SystemTime,
    },
}

/// What names a state: the last pass, or the first pending record, in the
/// order they arose, that names it, with the cause `coven_pending` keeps.
#[derive(Clone, Debug, PartialEq)]
pub enum SyncCause {
    /// How the last pass ended.
    Pass(LastPass),
    /// A pending record.
    Pending(PendingRecord),
}

/// The states that name a cause, as the model's `State.named` lists them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Named {
    SignedOut,
    NoAccess,
    StorageFull,
    FolderMissing,
    ClockWrong,
    UpdateNeeded,
    Removed,
    NeedsReset,
    Failed,
}

impl Named {
    /// Its place among the states, as `SyncState::rank` orders them.
    fn rank(self) -> u8 {
        match self {
            Self::Failed => 3,
            Self::StorageFull => 6,
            Self::ClockWrong => 7,
            Self::FolderMissing => 8,
            Self::NoAccess => 9,
            Self::SignedOut => 10,
            Self::UpdateNeeded => 11,
            Self::NeedsReset => 12,
            Self::Removed => 13,
        }
    }

    /// The state, named by `cause`, at a location of `provider`.
    fn state(self, cause: SyncCause, provider: CloudProvider) -> SyncState {
        match self {
            Self::SignedOut => SyncState::SignedOut { provider, cause: Some(cause) },
            Self::NoAccess => SyncState::NoAccess(cause),
            Self::StorageFull => SyncState::StorageFull(cause),
            Self::FolderMissing => SyncState::FolderMissing(cause),
            Self::ClockWrong => SyncState::ClockWrong(cause),
            Self::UpdateNeeded => SyncState::UpdateNeeded(cause),
            Self::Removed => SyncState::Removed(cause),
            Self::NeedsReset => SyncState::NeedsReset(cause),
            Self::Failed => SyncState::Failed(cause),
        }
    }
}

impl SyncState {
    /// Which state shows when several hold: the highest. The device's own
    /// standing first, then signing in, then what storage said of the
    /// account or location, then reachability, then the catch-all failure,
    /// then activity: syncing, then waiting.
    fn rank(&self) -> u8 {
        match self {
            Self::Synced => 0,
            Self::WaitingToSync(_) => 1,
            Self::Syncing(_) => 2,
            Self::Failed(_) => Named::Failed.rank(),
            Self::Offline(_) => 4,
            Self::OfflineWithUnsentChanges(_) => 5,
            Self::StorageFull(_) => Named::StorageFull.rank(),
            Self::ClockWrong(_) => Named::ClockWrong.rank(),
            Self::FolderMissing(_) => Named::FolderMissing.rank(),
            Self::NoAccess(_) => Named::NoAccess.rank(),
            Self::SignedOut { .. } => Named::SignedOut.rank(),
            Self::UpdateNeeded(_) => Named::UpdateNeeded.rank(),
            Self::NeedsReset(_) => Named::NeedsReset.rank(),
            Self::Removed(_) => Named::Removed.rank(),
            Self::NotSynced => 14,
        }
    }
}

/// The state a provider failure names: what storage said of this account or
/// location that a person must see, or coven's own bug (the model's
/// `Failure.named`).
#[deny(clippy::wildcard_enum_match_arm)]
fn storage_names(failure: StorageFailure) -> Option<Named> {
    use StorageFailure::*;
    match failure {
        Authentication => Some(Named::SignedOut),
        PermissionDenied | AccessNotGranted { .. } => Some(Named::NoAccess),
        QuotaExceeded => Some(Named::StorageFull),
        ContainerNotFound => Some(Named::FolderMissing),
        ClockSkew => Some(Named::ClockWrong),
        // Its work retries, as a local bug's does, and shows failed meanwhile.
        Bug => Some(Named::Failed),
        ProviderUnavailable
        | AccessNotRegistered { .. }
        | DropboxAppFolderNotShareable
        | CatchUpRequired
        | Network
        | Corrupted
        | NotFound
        | AlreadyExists
        | Conflict
        | RegionMismatch
        | StoreExists
        | OwnLocationRequired
        | RateLimited
        | InvalidConfiguration
        | Refused
        | Protocol
        | InvalidPath
        | InvalidRange
        | NotStoreOwner
        | InviteeIsStoreOwner
        | SharingRefused(_)
        | AccountIdUnavailable
        | InvitationMismatch
        | SessionMismatch
        | SessionExpired
        | InvalidPart
        | SingleRequestTooLarge { .. }
        | CopyTooLarge { .. }
        | Encoding
        | File
        | MemberKeysMissing => None,
    }
}

impl PendingReason {
    /// The state a pending record names, by its reason (the model's
    /// `Reason.state`). A wait names none, nor does a verdict on an admin
    /// operation or on another member's writes, which the app shows with
    /// that operation. A provider failure that clears by itself names none
    /// either: it retries on its own, and the pass says whether storage is
    /// reachable. A file that can't be uploaded or read names failed: it
    /// holds back what waits for it and is never skipped, so it shows until
    /// the file uploads or the person acts. Any other failure names the
    /// state its cause does, or failed. A cleanup names what its cleanup
    /// waits on, as it retries that.
    #[deny(clippy::wildcard_enum_match_arm)]
    fn names(&self) -> Option<Named> {
        match self {
            Self::Storage { failure, .. } => storage_names(*failure).or_else(|| {
                (crate::pending::storage_clears(*failure) != crate::pending::Clears::ByItself)
                    .then_some(Named::Failed)
            }),
            Self::UpdateRequired(_) => Some(Named::UpdateNeeded),
            Self::Removed | Self::NotAdmitted => Some(Named::Removed),
            Self::MemberKeysReplaced { .. } => Some(Named::NeedsReset),
            Self::Refused(_) | Self::FileUnavailable(_) | Self::Original { .. } => {
                Some(Named::Failed)
            }
            Self::Failed(LocalFailure::Shutdown) => None,
            Self::Failed(
                LocalFailure::Database
                | LocalFailure::DiskFull
                | LocalFailure::DiskDenied
                | LocalFailure::DiskOther
                | LocalFailure::KeyCustody
                | LocalFailure::Crypto
                | LocalFailure::Operation
                | LocalFailure::Bug,
            ) => Some(Named::Failed),
            Self::Cleanup { cleanup, .. } => cleanup.names(),
            Self::Missing { .. }
            | Self::Waits(_)
            | Self::KeyUnavailable { .. }
            | Self::NoStorage
            | Self::WritesRefused { .. }
            | Self::ProviderPending { .. }
            | Self::AccountInUse { .. }
            | Self::AccessRemains { .. }
            | Self::Rejected(_)
            | Self::Paused => None,
        }
    }

    /// Whether the record names a state of its own, which fails the
    /// upload it is about (the model's `Reason.state` is some).
    pub(crate) fn names_a_state(&self) -> bool {
        self.names().is_some()
    }

    /// Whether the reason clears by itself (the model's `Reason.byItself`).
    fn clears_by_itself(&self) -> bool {
        self.clears() == Some(crate::pending::Clears::ByItself)
    }

    /// Whether a pass that met this reason retries under the sync scope's
    /// backoff: it names no state and clears by itself (the model's
    /// `PassFailure.retries`).
    fn retries(&self) -> bool {
        self.names().is_none() && self.clears_by_itself()
    }
}

impl StatusFailure {
    /// The state a pass that failed as a whole names (the model's
    /// `PassFailure.state` but for offline); none for a failure that clears
    /// by itself and names no state of its own, a provider's cooldown
    /// included, on the pass or on coven's own operation.
    #[deny(clippy::wildcard_enum_match_arm)]
    fn names(&self) -> Option<Named> {
        match self {
            Self::UpdateRequired(_) => Some(Named::UpdateNeeded),
            Self::Removed | Self::NotAdmitted => Some(Named::Removed),
            Self::DeviceIdInUse(_) => Some(Named::NeedsReset),
            Self::Storage(failure) => storage_names(*failure).or_else(|| {
                (crate::pending::storage_clears(*failure) != crate::pending::Clears::ByItself)
                    .then_some(Named::Failed)
            }),
            Self::Operation(record) => record
                .reason
                .names()
                .or_else(|| (!record.reason.clears_by_itself()).then_some(Named::Failed)),
            Self::Other => Some(Named::Failed),
        }
    }

    /// Whether the pass retries under the sync scope's backoff (the model's
    /// `PassFailure.retries`).
    fn retries(&self) -> bool {
        match self {
            Self::Storage(failure) => {
                storage_names(*failure).is_none()
                    && crate::pending::storage_clears(*failure) == crate::pending::Clears::ByItself
            }
            Self::Operation(record) => record.reason.retries(),
            Self::UpdateRequired(_)
            | Self::Removed
            | Self::NotAdmitted
            | Self::DeviceIdInUse(_)
            | Self::Other => false,
        }
    }
}

impl LastPass {
    /// Whether the pass ended on a failure that retries under the sync
    /// scope's backoff (the model's `Facts.lastRetries`).
    fn retries(&self) -> bool {
        match self {
            Self::Synced | Self::Offline { .. } => false,
            Self::Blocked { reason, .. } => reason.retries(),
            Self::Failed { failure, .. } => failure.retries(),
        }
    }

    /// What this pass says, with no slot moving (the model's `Facts.ended`):
    /// a pass held back by a wait, at a slot or by a provider's cooldown, or
    /// one that failed with a failure that clears by itself, waits to sync,
    /// for what `wait` says the next pass waits for; with `wait` none its
    /// turn has come, and that is syncing, receiving; offline is offline with
    /// unsent changes while a write or a file waits to go out.
    fn ended(&self, unsent: bool, wait: Option<SyncWait>, provider: CloudProvider) -> SyncState {
        let cause = || SyncCause::Pass(self.clone());
        let waiting =
            || wait.map_or(SyncState::Syncing(SyncActivity::Receiving), SyncState::WaitingToSync);
        match self {
            Self::Synced => SyncState::Synced,
            Self::Offline { .. } if unsent => SyncState::OfflineWithUnsentChanges(cause()),
            Self::Offline { .. } => SyncState::Offline(cause()),
            Self::Blocked { reason, .. } => {
                reason.names().map_or_else(waiting, |named| named.state(cause(), provider))
            }
            Self::Failed { failure, .. } => {
                failure.names().map_or_else(waiting, |named| named.state(cause(), provider))
            }
        }
    }
}

impl SyncStatus {
    /// The sync state and when sync last finished, from what one committed
    /// snapshot of coven's rows holds (§19.1): what `Database::sync_facts`
    /// reads once and `Database::subscribe_sync_facts` follows.
    pub fn computed(facts: coven_database::SyncFacts<PendingRecord>) -> Result<Self, DbError> {
        let (facts, last_synced) = Facts::read(facts)?;
        Ok(Self { state: facts.state(), last_synced })
    }

    /// A closed store's sync status (api.md E1, E5): the state opening shows
    /// before sync starts, from the facts its database holds, with what
    /// opening writes over them: `location`, read from the keychain as
    /// opening reads it; no pass moving, whatever a process that ended
    /// mid-pass left; and sync not running. The facts read with nothing sent,
    /// as nothing moves while the store is closed.
    pub fn closed(
        facts: coven_database::SyncFacts<PendingRecord>,
        location: RecordedLocation<CloudProvider>,
    ) -> Result<Self, DbError> {
        let (facts, last_synced) = Facts::read(facts)?;
        let facts = Facts { location, moved: None, sync: RetrySync::Stopped, ..facts };
        Ok(Self { state: facts.state(), last_synced })
    }
}

/// The sync state and when sync last finished, computed from what coven
/// knows in one committed snapshot (§19.1).
pub async fn sync_status(database: &coven_database::Database) -> Result<SyncStatus, DbError> {
    database.sync_facts(SyncStatus::computed).await
}

/// The name `coven_sync_status.provider` gives a provider: its own.
fn provider_name(provider: CloudProvider) -> String {
    match provider {
        CloudProvider::S3 => "S3",
        CloudProvider::Dropbox => "Dropbox",
        CloudProvider::OneDrive => "OneDrive",
        CloudProvider::CloudKit => "CloudKit",
    }
    .into()
}

/// The provider `coven_sync_status.provider` names.
fn named_provider(name: &str) -> Result<CloudProvider, DbError> {
    match name {
        "S3" => Ok(CloudProvider::S3),
        "Dropbox" => Ok(CloudProvider::Dropbox),
        "OneDrive" => Ok(CloudProvider::OneDrive),
        "CloudKit" => Ok(CloudProvider::CloudKit),
        _ => Err(DbError::DamagedDatabase),
    }
}

/// Write, as the store opens, whether a storage location is recorded here
/// and its credentials held, and that no pass moves; `finished`, for a store
/// joining or restoring read to its end, is when (E5). A failure fails the
/// store's handle with its cause.
pub async fn record_opened(
    database: &coven_database::Database,
    location: RecordedLocation<CloudProvider>,
    finished: Option<SystemTime>,
) -> Result<(), DbError> {
    database.record_opened(location.map(provider_name), finished).await
}

/// Write whether a storage location is recorded here and its credentials
/// held, as setup, a change of credentials or releasing a connection finds
/// it; `finished`, for setup, is when it read the store to its end. A
/// failure fails the store's handle with its cause.
pub async fn record_location(
    database: &coven_database::Database,
    location: RecordedLocation<CloudProvider>,
    finished: Option<SystemTime>,
) -> Result<(), DbError> {
    database.record_location(location.map(provider_name), finished).await
}

#[path = "sync_status_facts.rs"]
mod facts;
pub(crate) use facts::{Facts, Progress, Upload};

#[cfg(test)]
#[path = "sync_status_tests.rs"]
mod tests;
