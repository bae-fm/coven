//! The sync state a status screen shows (§19.1, api.md E5), computed in one
//! read from what coven knows and never stored: whether a location is
//! recorded and its credentials held, how the last pass ended, whether the
//! running pass moved a slot, the files moving, each upload in the queue,
//! whether the send queue holds a write, and the pending records. Each fact
//! names one candidate state or none, and when several hold the one of
//! highest rank shows, as `state` of the Lean model in
//! `spec/proofs/io/CovenIO/SyncState.lean` computes it; the differential
//! test checks this against that model's runner.

use crate::{LastPass, LocalFailure, PendingReason, PendingRecord, PendingSubject, StatusFailure};
use coven_database::{CovenResult, DbError, RecordedLocation, RetrySync};
use coven_storage::{CloudProvider, StorageFailure};
use std::time::SystemTime;

/// What a status screen shows (E5, §19.1): the sync state and when sync
/// last finished, as `sync_status()` reads it and `subscribe_sync_status()`
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
    /// A running pass has read or claimed a slot, or a file is being sent
    /// or downloaded.
    Syncing,
    /// A file waits in the upload queue and nothing moves: what the first
    /// such file waits for.
    WaitingToSync(UploadWait),
    /// The last pass reached storage, every file this device uploads is
    /// stored, and nothing below holds.
    Synced,
    /// The last pass got no provider response.
    Offline(SyncCause),
    /// Offline, and the send queue holds a write or the upload queue a file.
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
    /// This device or its member was removed.
    Removed(SyncCause),
    /// Another copy uses this device's id, or its member keys were replaced.
    NeedsReset(SyncCause),
    /// Any other failure, or a file that can't be uploaded or read, whose
    /// pending record is the cause.
    Failed(SyncCause),
}

/// What a file waiting in the upload queue waits for, as `WaitingToSync`
/// names it for the first such file: what holds every upload first, then
/// what holds that file (§19.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadWait {
    /// Sync isn't running (`start_sync`, `stop_sync`).
    NotSyncing,
    /// The provider asked every request to wait until then (§6.7).
    CoolingDown {
        /// When the cooldown ends, by the store's clock.
        until: SystemTime,
    },
    /// The last pass is blocked at a slot, and no file goes out until it
    /// applies (§5.4).
    Blocked,
    /// The app paused uploads, and the file's reference trails
    /// (`set_uploads_paused`, §17.4).
    Paused,
    /// The network is metered, or may be, since the platform stopped
    /// answering after opening (E5); the app moves files only on unmetered
    /// networks (`set_unmetered_only`, §17.9), and the file's reference
    /// trails.
    Metered,
    /// Its last attempt failed, and it retries then, or at the person's
    /// retry (§6.7); `coven_uploads.pending` says why.
    Retrying {
        /// When its backoff ends, by the store's clock.
        at: SystemTime,
    },
    /// Its turn hasn't come.
    Queued,
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
    /// then activity: moving, then waiting.
    fn rank(&self) -> u8 {
        match self {
            Self::Synced => 0,
            Self::WaitingToSync(_) => 1,
            Self::Syncing => 2,
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
/// location that a person must see (the model's `Failure.named`).
#[deny(clippy::wildcard_enum_match_arm)]
fn storage_names(failure: StorageFailure) -> Option<Named> {
    use StorageFailure::*;
    match failure {
        Authentication => Some(Named::SignedOut),
        PermissionDenied | AccessNotGranted { .. } => Some(Named::NoAccess),
        QuotaExceeded => Some(Named::StorageFull),
        ContainerNotFound => Some(Named::FolderMissing),
        ClockSkew => Some(Named::ClockWrong),
        ProviderUnavailable
        | AccessNotRegistered { .. }
        | DropboxAppFolderNotShareable
        | CatchUpRequired
        | Network
        | NotFound
        | AlreadyExists
        | Conflict
        | RegionMismatch
        | StoreExists
        | OwnBucketRequired { .. }
        | RateLimited
        | InvalidConfiguration
        | Refused
        | Protocol
        | InvalidPath
        | InvalidRange
        | NotStoreOwner
        | InviteeIsStoreOwner
        | AccountIdUnavailable
        | InvitationMismatch
        | SessionMismatch
        | SessionExpired
        | SessionIdentityUnknown
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
            Self::Removed => Some(Named::Removed),
            Self::MemberKeysReplaced { .. } => Some(Named::NeedsReset),
            Self::Refused(_) | Self::FileUnavailable(_) | Self::Original { .. } => {
                Some(Named::Failed)
            }
            Self::Failed(LocalFailure::Shutdown) => None,
            Self::Failed(
                LocalFailure::Database
                | LocalFailure::Disk
                | LocalFailure::KeyCustody
                | LocalFailure::Crypto
                | LocalFailure::Operation,
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
}

impl StatusFailure {
    /// The state a pass that failed as a whole names (the model's
    /// `PassFailure.state` but for offline); none for a provider's cooldown.
    #[deny(clippy::wildcard_enum_match_arm)]
    fn names(&self) -> Option<Named> {
        match self {
            Self::UpdateRequired(_) => Some(Named::UpdateNeeded),
            Self::Removed => Some(Named::Removed),
            Self::DeviceIdInUse(_) => Some(Named::NeedsReset),
            Self::Storage(failure) => storage_names(*failure)
                .or_else(|| (*failure != StorageFailure::RateLimited).then_some(Named::Failed)),
            Self::Operation(record) => Some(record.reason.names().unwrap_or(Named::Failed)),
            Self::Other => Some(Named::Failed),
        }
    }
}

impl LastPass {
    /// What this pass says, with no slot moving (the model's `Facts.ended`):
    /// a wait counts as synced, since the pass reached storage; offline is
    /// offline with unsent changes while a write or a file waits to go out.
    fn ended(&self, unsent: bool, provider: CloudProvider) -> SyncState {
        let cause = || SyncCause::Pass(self.clone());
        match self {
            Self::Synced => SyncState::Synced,
            Self::Offline { .. } if unsent => SyncState::OfflineWithUnsentChanges(cause()),
            Self::Offline { .. } => SyncState::Offline(cause()),
            Self::Blocked { reason, .. } => {
                reason.names().map_or(SyncState::Synced, |named| named.state(cause(), provider))
            }
            Self::Failed { failure, .. } => {
                failure.names().map_or(SyncState::Synced, |named| named.state(cause(), provider))
            }
        }
    }
}

/// Where one upload in this device's queue stands (§17.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    /// An attempt is sending it now.
    Moving,
    /// Storage holds its bytes; it waits for the rest of its move.
    Stored,
    /// Queued, and no attempt is sending it.
    Queued,
}

/// One upload in this device's queue.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Upload {
    pub(crate) progress: Progress,
    /// A bound reference's, which the pause doesn't hold (§17.4).
    pub(crate) bound: bool,
    /// Its last attempt failed, and it retries then (§6.7).
    pub(crate) retry_at: Option<SystemTime>,
    /// Its own pending record, if it has one.
    pub(crate) record: Option<PendingRecord>,
}

/// What the state is computed from, as the model's `Facts` names it; the
/// time of the last finished sync is read beside it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Facts {
    pub(crate) location: RecordedLocation<CloudProvider>,
    /// How the last pass ended; none until a pass ends here, which counts
    /// as synced, since setup, joining and restoring read the store to its
    /// end.
    pub(crate) last: Option<LastPass>,
    /// The running pass has read a slot it hadn't, or claimed one.
    pub(crate) moving: bool,
    /// A file is being downloaded.
    pub(crate) downloading: bool,
    /// Each upload a file waits for, in queue order.
    pub(crate) uploads: Vec<Upload>,
    /// The send queue holds a write.
    pub(crate) unsent: bool,
    /// The pending records, in the order they arose, but the connection's,
    /// which is the last pass's failure kept for its retry.
    pub(crate) records: Vec<PendingRecord>,
    /// Whether a started connection will plan, and the cooldown holding
    /// every request.
    pub(crate) sync: RetrySync,
    /// The app paused trailing uploads.
    pub(crate) paused: bool,
    /// The app moves files only on unmetered networks.
    pub(crate) unmetered_only: bool,
    /// The store's watch says the device's network is metered.
    pub(crate) metered: bool,
}

/// A state that may show, its cause made only for the one that does.
enum Candidate<'a> {
    State(SyncState),
    Named(Named, &'a PendingRecord),
}

impl Candidate<'_> {
    fn rank(&self) -> u8 {
        match self {
            Self::State(state) => state.rank(),
            Self::Named(named, _) => named.rank(),
        }
    }

    fn state(self, provider: CloudProvider) -> SyncState {
        match self {
            Self::State(state) => state,
            Self::Named(named, record) => named.state(SyncCause::Pending(record.clone()), provider),
        }
    }
}

impl Facts {
    /// A write or an upload waits to go out.
    fn unsent_work(&self) -> bool {
        self.unsent || !self.uploads.is_empty()
    }

    /// What one queued upload waits for: what holds every upload first, then
    /// what holds this one (the model's `Facts.uploadWait`).
    fn upload_wait(&self, upload: &Upload) -> UploadWait {
        match self.sync {
            RetrySync::Stopped => UploadWait::NotSyncing,
            RetrySync::Started { cooling_until: Some(until) } => UploadWait::CoolingDown { until },
            RetrySync::Started { cooling_until: None } => {
                if matches!(self.last, Some(LastPass::Blocked { .. })) {
                    UploadWait::Blocked
                } else if self.paused && !upload.bound {
                    UploadWait::Paused
                } else if self.unmetered_only
                    && self.metered
                    && crate::files::held_on_metered(upload.bound)
                {
                    UploadWait::Metered
                } else if let Some(at) = upload.retry_at {
                    UploadWait::Retrying { at }
                } else {
                    UploadWait::Queued
                }
            }
        }
    }

    /// What an upload names: syncing while it moves; once queued, what its
    /// record names, else waiting to sync; nothing once stored.
    fn upload<'a>(&self, upload: &'a Upload) -> Option<Candidate<'a>> {
        match upload.progress {
            Progress::Moving => Some(Candidate::State(SyncState::Syncing)),
            Progress::Stored => None,
            Progress::Queued => Some(
                match upload
                    .record
                    .as_ref()
                    .and_then(|record| Some((record.reason.names()?, record)))
                {
                    Some((named, record)) => Candidate::Named(named, record),
                    None => Candidate::State(SyncState::WaitingToSync(self.upload_wait(upload))),
                },
            ),
        }
    }

    /// The state shown: not synced with no location recorded, else the
    /// highest-ranked candidate. Among candidates of one state the first
    /// shows, so the cause is the last pass, or else the first record, in
    /// the order they arose, that names the state, and waiting to sync names
    /// the first waiting upload's wait. Signed out with no credentials comes
    /// first of all, with no cause.
    pub(crate) fn state(&self) -> SyncState {
        let (provider, credentials) = match self.location {
            RecordedLocation::None => return SyncState::NotSynced,
            RecordedLocation::SignedOut(provider) => (provider, false),
            RecordedLocation::Connected(provider) => (provider, true),
        };
        let signed_out = (!credentials)
            .then_some(Candidate::State(SyncState::SignedOut { provider, cause: None }));
        let looping = Candidate::State(if self.moving {
            SyncState::Syncing
        } else {
            match &self.last {
                None => SyncState::Synced,
                Some(last) => last.ended(self.unsent_work(), provider),
            }
        });
        let records = self
            .records
            .iter()
            .filter_map(|record| Some(Candidate::Named(record.reason.names()?, record)));
        let downloading = self.downloading.then_some(Candidate::State(SyncState::Syncing));
        let uploads = self.uploads.iter().filter_map(|upload| self.upload(upload));
        signed_out
            .into_iter()
            .chain([looping])
            .chain(records)
            .chain(downloading)
            .chain(uploads)
            .reduce(|shown, next| if next.rank() > shown.rank() { next } else { shown })
            .expect("the loop's candidate is always one")
            .state(provider)
    }

    /// The facts as the database read them, and when sync last finished.
    fn read(
        facts: coven_database::SyncFacts<PendingRecord>,
    ) -> Result<(Self, Option<SystemTime>), DbError> {
        let row = facts.row;
        let location = match row.location {
            RecordedLocation::None => RecordedLocation::None,
            RecordedLocation::SignedOut(name) => {
                RecordedLocation::SignedOut(named_provider(&name)?)
            }
            RecordedLocation::Connected(name) => {
                RecordedLocation::Connected(named_provider(&name)?)
            }
        };
        let last = row
            .last_pass
            .map(|bytes| serde_json::from_slice(&bytes).map_err(DbError::StateEncoding))
            .transpose()?;
        let uploads = facts
            .uploads
            .into_iter()
            .map(|upload| Upload {
                progress: if upload.sending {
                    Progress::Moving
                } else if upload.stored {
                    Progress::Stored
                } else {
                    Progress::Queued
                },
                bound: upload.bound,
                retry_at: upload.retry_at,
                record: upload.record,
            })
            .collect();
        let facts = Self {
            location,
            last,
            moving: row.moving,
            downloading: facts.downloading,
            uploads,
            unsent: facts.unsent,
            records: facts
                .records
                .into_iter()
                .filter(|record| record.subject != PendingSubject::Connection)
                .collect(),
            sync: facts.retry.sync,
            paused: facts.paused,
            unmetered_only: facts.unmetered_only,
            metered: facts.metered,
        };
        Ok((facts, row.last_synced))
    }
}

impl SyncStatus {
    fn computed(facts: coven_database::SyncFacts<PendingRecord>) -> CovenResult<Self> {
        let (facts, last_synced) = Facts::read(facts)?;
        Ok(Self { state: facts.state(), last_synced })
    }
}

/// The sync state and when sync last finished, computed from what coven
/// knows in one committed snapshot (§19.1).
pub async fn sync_status(database: &coven_database::Database) -> CovenResult<SyncStatus> {
    database.sync_facts(SyncStatus::computed).await
}

/// The same pre-built query, live: its first value is the current state.
pub fn subscribe_sync_status(
    database: &coven_database::Database,
) -> coven_database::LiveQuery<SyncStatus> {
    database.subscribe_sync_facts(SyncStatus::computed)
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

#[cfg(test)]
#[path = "sync_status_tests.rs"]
mod tests;
