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
//!
//! The database registers `SyncStatus::row` as the SQL function
//! `_coven_sync_state` (`sync_functions`), which the `coven_sync_state`
//! view calls over the facts it reads: an app's screen reads the state in
//! its own live query, joined to its rows, and this code stays the one
//! definition of the state that the Lean model checks. `sync_status` reads
//! the view.

use crate::{
    DeviceIdEvidence, KeyHolder, LastPass, LocalFailure, PendingReason, PendingRecord,
    Prerequisite, StatusFailure, SyncError,
};
use coven_database::{
    DbError, RecordedLocation, Removal, RetrySync, SqlReadContext, SyncFacts, SyncStateRow,
    UnsentWork,
};
use coven_foundation::id_source::{KeyId, MemberId};
use coven_merge::Audience;
use coven_storage::{CloudProvider, StorageFailure};
use std::time::SystemTime;

/// What a status screen shows (E5, §19.1): the sync state and when sync
/// last finished, as `coven_sync_state` shows them and `sync_status()` reads
/// them.
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
/// before them all. `coven_sync_state.detail` holds it as JSON, which this
/// type reads.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
    /// This device isn't in the store, and won't be again. What was written
    /// here and not sent never will be.
    Removed {
        /// Why this device isn't in the store.
        removal: Removal,
        /// What this device committed and will not send.
        unsent: UnsentWork,
    },
    /// This installation can't send under its keys or id. What was written
    /// here and not sent never will be.
    NeedsReset {
        /// Why it can't send.
        reason: ResetReason,
        /// What this device committed and will not send.
        unsent: UnsentWork,
    },
    /// Any other failure, or a bound file that can't be uploaded or read,
    /// whose pending record is the cause.
    Failed(SyncCause),
}

/// Why this installation can't send under its keys or id, as `NeedsReset`
/// names it (§19.3, §12.3).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResetReason {
    /// A reset or restore registered another installation in this one's place
    /// (§19.3).
    Replaced,
    /// Another installation uses this device's id: this copy is the one
    /// behind, which the app resets without asking, and the reset registers a
    /// new device (§19.3).
    DeviceIdInUse,
    /// This member's keys were replaced with a pair this device doesn't hold
    /// (§12.3).
    MemberKeysReplaced {
        /// The member whose held keys were replaced.
        member: MemberId,
        /// The device that wrote the replacement, once this installation has
        /// applied it: none when the error came before, as an old restore code
        /// opening a store whose history it has not read.
        by: Option<crate::NamedDevice>,
    },
}

/// What Syncing is doing, as `Syncing` names it (§19.1): the first of what the
/// running pass moved last, a bound file moving or next in line, a write next
/// in line, and the next pass's turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
/// bound file waiting in the upload queue, what holds that file, then a
/// write's own key (§19.1).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SyncWait {
    /// Sync isn't running (`start_sync`, `stop_sync`).
    NotSyncing,
    /// The provider asked every request to wait until then (§6.7).
    CoolingDown {
        /// When the cooldown ends, by the store's clock.
        until: SystemTime,
    },
    /// A slot, or the first write in the send queue, needs a key this device
    /// holds no copy of; another device of a member holding it sends one
    /// (§12.1). A slot waiting on it holds everything: nothing goes out or
    /// comes in until the key arrives (§5.4).
    KeyCopy {
        /// The key's audience.
        #[serde(with = "crate::pending_encoding::audience")]
        audience: Audience,
        /// The key.
        key: KeyId,
        /// The members that hold the key, as the history this device applied
        /// names them: their devices to open, or, for a holder that no longer
        /// reads the audience, a member of the audience to add it back (§12.1).
        /// Empty only when the record was made without the history; a key no
        /// member holds is never sent, and shows Failed.
        holders: Vec<KeyHolder>,
    },
    /// A slot holds a raise or reset of `audience`, and no snapshot announced
    /// after it is named yet; another device takes one (§16.5). Nothing goes
    /// out or comes in until then.
    Snapshot {
        /// The audience whose snapshot is awaited.
        #[serde(with = "crate::pending_encoding::audience")]
        audience: Audience,
    },
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
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SyncCause {
    /// How the last pass ended.
    Pass(LastPass),
    /// A pending record.
    Pending(#[serde(with = "with_cause")] PendingRecord),
}

/// Encodes a pending record that names a state together with its cause,
/// because the record's own encoding leaves the cause to
/// `coven_pending.cause`.
mod with_cause {
    use crate::PendingRecord;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Recorded {
        record: PendingRecord,
        cause: Option<String>,
    }

    pub(super) fn serialize<S: serde::Serializer>(
        record: &PendingRecord,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(
            &Recorded { record: record.clone(), cause: record.cause.clone() },
            serializer,
        )
    }

    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<PendingRecord, D::Error> {
        let Recorded { mut record, cause } = serde::Deserialize::deserialize(deserializer)?;
        record.cause = cause;
        Ok(record)
    }
}

/// The states that name a cause, as the model's `State.named` lists them;
/// removed and needs reset name the removal or reset reason instead.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Named {
    SignedOut,
    NoAccess,
    StorageFull,
    FolderMissing,
    ClockWrong,
    UpdateNeeded,
    Removed(Removal),
    NeedsReset(ResetReason),
    Failed,
}

impl Named {
    /// Its place among the states, as `SyncState::rank` orders them.
    fn rank(&self) -> u8 {
        match self {
            Self::Failed => 3,
            Self::StorageFull => 6,
            Self::ClockWrong => 7,
            Self::FolderMissing => 8,
            Self::NoAccess => 9,
            Self::SignedOut => 10,
            Self::UpdateNeeded => 11,
            Self::NeedsReset(_) => 12,
            Self::Removed(_) => 13,
        }
    }

    /// The state, named by `cause`, at a location of `provider`; the
    /// standing states say what is unsent instead.
    fn state(self, cause: SyncCause, provider: CloudProvider, unsent: UnsentWork) -> SyncState {
        match self {
            Self::SignedOut => SyncState::SignedOut { provider, cause: Some(cause) },
            Self::NoAccess => SyncState::NoAccess(cause),
            Self::StorageFull => SyncState::StorageFull(cause),
            Self::FolderMissing => SyncState::FolderMissing(cause),
            Self::ClockWrong => SyncState::ClockWrong(cause),
            Self::UpdateNeeded => SyncState::UpdateNeeded(cause),
            Self::Removed(removal) => SyncState::Removed { removal, unsent },
            Self::NeedsReset(reason) => SyncState::NeedsReset { reason, unsent },
            Self::Failed => SyncState::Failed(cause),
        }
    }
}

impl SyncState {
    /// The state's kind, as `coven_sync_state.state` names it.
    fn kind(&self) -> &'static str {
        match self {
            Self::NotSynced => "not_synced",
            Self::Syncing(_) => "syncing",
            Self::WaitingToSync(_) => "waiting_to_sync",
            Self::Synced => "synced",
            Self::Offline(_) => "offline",
            Self::OfflineWithUnsentChanges(_) => "offline_with_unsent_changes",
            Self::SignedOut { .. } => "signed_out",
            Self::NoAccess(_) => "no_access",
            Self::StorageFull(_) => "storage_full",
            Self::FolderMissing(_) => "folder_missing",
            Self::ClockWrong(_) => "clock_wrong",
            Self::UpdateNeeded(_) => "update_needed",
            Self::Removed { .. } => "removed",
            Self::NeedsReset { .. } => "needs_reset",
            Self::Failed(_) => "failed",
        }
    }

    /// What names the state, where something does.
    fn cause(&self) -> Option<&SyncCause> {
        match self {
            Self::Offline(cause)
            | Self::OfflineWithUnsentChanges(cause)
            | Self::NoAccess(cause)
            | Self::StorageFull(cause)
            | Self::FolderMissing(cause)
            | Self::ClockWrong(cause)
            | Self::UpdateNeeded(cause)
            | Self::Failed(cause) => Some(cause),
            Self::SignedOut { cause, .. } => cause.as_ref(),
            Self::NotSynced
            | Self::Syncing(_)
            | Self::WaitingToSync(_)
            | Self::Synced
            | Self::Removed { .. }
            | Self::NeedsReset { .. } => None,
        }
    }

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
            Self::NeedsReset { reason, .. } => Named::NeedsReset(reason.clone()).rank(),
            Self::Removed { removal, .. } => Named::Removed(*removal).rank(),
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
            Self::Removed(removal) => Some(Named::Removed(*removal)),
            Self::MemberKeysReplaced { member, by } => {
                Some(Named::NeedsReset(ResetReason::MemberKeysReplaced {
                    member: *member,
                    by: by.clone(),
                }))
            }
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
            // A key no member holds is never sent: that is no wait.
            Self::KeyUnavailable { holders, .. } => {
                holders.as_ref().is_some_and(Vec::is_empty).then_some(Named::Failed)
            }
            Self::Missing { .. }
            | Self::Waits(_)
            | Self::NoStorage
            | Self::WritesRefused { .. }
            | Self::ProviderPending { .. }
            | Self::AccountInUse { .. }
            | Self::AccessRemains { .. }
            | Self::Rejected(_)
            | Self::Paused => None,
        }
    }

    /// The wait a slot blocked with this reason names (the model's
    /// `Reason.slotWait`): the key it needs, or the snapshot it waits for. A
    /// newer floor names none: the next pass retries it.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub(super) fn slot_wait(&self) -> Option<SyncWait> {
        match self {
            Self::KeyUnavailable { holders: Some(holders), .. } if holders.is_empty() => None,
            Self::KeyUnavailable { audience, key, holders } => Some(SyncWait::KeyCopy {
                audience: audience.clone(),
                key: *key,
                holders: holders.clone().unwrap_or_default(),
            }),
            Self::Waits(Prerequisite::Reload(audience)) => {
                Some(SyncWait::Snapshot { audience: audience.clone() })
            }
            Self::Waits(Prerequisite::Floor | Prerequisite::SequenceCatchUp)
            | Self::Missing { .. }
            | Self::UpdateRequired(_)
            | Self::NoStorage
            | Self::Storage { .. }
            | Self::Refused(_)
            | Self::WritesRefused { .. }
            | Self::ProviderPending { .. }
            | Self::AccountInUse { .. }
            | Self::AccessRemains { .. }
            | Self::FileUnavailable(_)
            | Self::Original { .. }
            | Self::Failed(_)
            | Self::Rejected(_)
            | Self::Cleanup { .. }
            | Self::Paused
            | Self::Removed(_)
            | Self::MemberKeysReplaced { .. } => None,
        }
    }

    /// What a slot blocked with this reason names (the model's
    /// `Reason.slotState`). The reasons that wait (`slot_wait`, a newer
    /// floor), the failures that clear by themselves and those that name a
    /// state keep what `names` says. A slot blocks with no other reason, so
    /// one that did would be coven's own bug: it names failed.
    #[deny(clippy::wildcard_enum_match_arm)]
    fn slot_names(&self) -> Option<Named> {
        match self {
            Self::Waits(Prerequisite::Reload(_) | Prerequisite::Floor) => None,
            // It waits for the copy, or, with no holder to send one, is failed.
            Self::KeyUnavailable { .. } => self.names(),
            Self::Waits(Prerequisite::SequenceCatchUp)
            | Self::Missing { .. }
            | Self::UpdateRequired(_)
            | Self::NoStorage
            | Self::Storage { .. }
            | Self::Refused(_)
            | Self::WritesRefused { .. }
            | Self::ProviderPending { .. }
            | Self::AccountInUse { .. }
            | Self::AccessRemains { .. }
            | Self::FileUnavailable(_)
            | Self::Original { .. }
            | Self::Failed(_)
            | Self::Rejected(_)
            | Self::Cleanup { .. }
            | Self::Paused
            | Self::Removed(_)
            | Self::MemberKeysReplaced { .. } => {
                self.names().or_else(|| (!self.clears_by_itself()).then_some(Named::Failed))
            }
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
            Self::Removed(removal) => Some(Named::Removed(*removal)),
            Self::DeviceIdInUse(DeviceIdEvidence::Replaced) => {
                Some(Named::NeedsReset(ResetReason::Replaced))
            }
            Self::DeviceIdInUse(DeviceIdEvidence::UnattemptedSlot { .. }) => {
                Some(Named::NeedsReset(ResetReason::DeviceIdInUse))
            }
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
            Self::UpdateRequired(_) | Self::Removed(_) | Self::DeviceIdInUse(_) | Self::Other => {
                false
            }
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
    fn ended(
        &self,
        unsent_work: bool,
        unsent: UnsentWork,
        wait: Option<SyncWait>,
        provider: CloudProvider,
    ) -> SyncState {
        let cause = || SyncCause::Pass(self.clone());
        let waiting =
            || wait.map_or(SyncState::Syncing(SyncActivity::Receiving), SyncState::WaitingToSync);
        match self {
            Self::Synced => SyncState::Synced,
            Self::Offline { .. } if unsent_work => SyncState::OfflineWithUnsentChanges(cause()),
            Self::Offline { .. } => SyncState::Offline(cause()),
            Self::Blocked { reason, .. } => reason
                .slot_names()
                .map_or_else(waiting, |named| named.state(cause(), provider, unsent)),
            Self::Failed { failure, .. } => {
                failure.names().map_or_else(waiting, |named| named.state(cause(), provider, unsent))
            }
        }
    }
}

/// Reads `coven_sync_state.detail`.
impl coven_database::types::FromSql for SyncState {
    fn column_result(
        value: coven_database::types::ValueRef<'_>,
    ) -> coven_database::types::FromSqlResult<Self> {
        serde_json::from_str(value.as_str()?)
            .map_err(|error| coven_database::types::FromSqlError::Other(Box::new(error)))
    }
}

impl SyncStatus {
    /// The state as `coven_sync_state` shows it, from the facts its subquery
    /// reads: what `_coven_sync_state` returns. The view reads when sync last
    /// finished from the status row itself.
    pub(crate) fn row(facts: SyncFacts<PendingRecord>) -> Result<SyncStateRow, DbError> {
        let positions: Vec<(i64, crate::PendingSubject)> = facts
            .records
            .iter()
            .map(|(position, record)| (*position, record.subject.clone()))
            .collect();
        let (facts, _last_synced) = Facts::read(facts)?;
        let state = facts.state();
        // The cause's record is one of the facts' records, and its subject
        // is the key `coven_pending` holds it under.
        let pending = match state.cause() {
            Some(SyncCause::Pending(record)) => Some(
                positions
                    .iter()
                    .find_map(|(position, subject)| {
                        (*subject == record.subject).then_some(*position)
                    })
                    .ok_or(DbError::Invariant {
                        what: "the state's pending record is among the facts' records",
                    })?,
            ),
            Some(SyncCause::Pass(_)) | None => None,
        };
        Ok(SyncStateRow {
            state: state.kind(),
            pending,
            detail: serde_json::to_value(&state).map_err(DbError::StateEncoding)?,
        })
    }

    /// The sync state and when sync last finished, as `coven_sync_state`
    /// shows them in the caller's read.
    pub fn read(sql: SqlReadContext<'_>) -> Result<Self, DbError> {
        let (state, last_synced) =
            sql.query_row("SELECT detail,last_synced FROM coven_sync_state", [], |row| {
                Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?))
            })?;
        Ok(Self { state, last_synced: last_synced.map(coven_database::stored_time).transpose()? })
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

/// The sync state and when sync last finished, as `coven_sync_state` shows
/// them in one committed snapshot (§19.1).
pub async fn sync_status(database: &coven_database::StoreDatabase) -> Result<SyncStatus, DbError> {
    database.read(SyncStatus::read).await
}

/// The provider that `coven_sync_status.provider` names, or
/// `DamagedDatabase` when the name matches no provider.
fn named_provider(name: &str) -> Result<CloudProvider, DbError> {
    name.parse().map_err(|_unknown| DbError::DamagedDatabase)
}

/// Write, as the store opens, whether a storage location is recorded here
/// and its credentials held, with the location and its labels, and that no
/// pass moves; `finished`, for a store joining or restoring read to its end,
/// is when (E5). A failure fails the store's handle with its cause.
pub async fn record_opened(
    database: &coven_database::StoreDatabase,
    location: RecordedLocation<coven_storage::StorageConfig>,
    finished: Option<SystemTime>,
) -> Result<(), SyncError> {
    Ok(database.record_opened(stored(location)?, finished).await?)
}

/// Write whether a storage location is recorded here and its credentials
/// held, with the location and its labels, as setup, a change of
/// credentials, a connection's new label or releasing a connection finds
/// it; `finished`, for setup, is when it read the store to its end. A
/// failure fails the store's handle with its cause.
pub async fn record_location(
    database: &coven_database::StoreDatabase,
    location: RecordedLocation<coven_storage::StorageConfig>,
    finished: Option<SystemTime>,
) -> Result<(), SyncError> {
    Ok(database.record_location(stored(location)?, finished).await?)
}

/// `location` as `coven_sync_status` keeps it: its provider's name and the
/// location's settings text.
fn stored(
    location: RecordedLocation<coven_storage::StorageConfig>,
) -> Result<RecordedLocation<coven_database::StoredLocation>, SyncError> {
    location.try_map(|location| {
        Ok(coven_database::StoredLocation {
            provider: location.provider().name().to_owned(),
            location: location.recorded_text()?,
        })
    })
}

/// Write that `location` is recorded on `connection`, a test's connection
/// outside any store, with this device holding its credentials, as
/// `record_opened` writes it.
#[cfg(any(test, feature = "test-utils"))]
pub fn record_connected_outside_store(
    connection: &coven_database::test_utils::Connection,
    location: coven_storage::StorageConfig,
) -> Result<(), SyncError> {
    coven_database::test_utils::save_location_outside_store(
        connection,
        &stored(RecordedLocation::Connected(location))?,
    )?;
    Ok(())
}

#[path = "sync_status_facts.rs"]
mod facts;
pub(crate) use facts::{held_all, last_pass, Facts, Progress, Upload};

#[cfg(test)]
#[path = "sync_status_tests.rs"]
mod tests;
