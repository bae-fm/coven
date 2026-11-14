//! Current unmet conditions shared by calls, persistence and the application query.

use super::pending_encoding as encoding;
use crate::{OperationId, OperationKind, Refusal};
use coven_database::PendingValue;
use coven_format::{admin::SnapshotId, path::ObjectPath};
use coven_foundation::id_source::{DeviceId, FileId, KeyId, MemberId, ObjectId};
use coven_merge::{Audience, Slot, WriteId};
use coven_storage::{CloudProvider, RetainedAccess, StorageFailure};
use serde::{Deserialize, Serialize};

pub use coven_format::RequiredUpdate;

/// The staging device cannot supply the file's original bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileSourceFailure {
    /// The original file is absent.
    Missing,
    /// The original changed after it was staged.
    Changed,
    /// The original fails its retained content checks.
    Integrity,
}

/// A member of the store that holds a key a stopped device waits for: a copy
/// of the key is sealed to the member's current sealing key, so every device of
/// the member with that pair opens it, and a device of the member that reads the
/// key's audience sends the copy on its next pass (§12.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyHolder {
    /// The member.
    pub member: MemberId,
    /// The member reads the key's audience now, so its devices send the copy;
    /// otherwise it holds the key from before it left the audience, and a
    /// member of the audience has to add it back first.
    pub reads_audience: bool,
    /// The member's active devices, as the applied history names them.
    pub devices: Vec<HolderDevice>,
}

/// An active device of a `KeyHolder`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderDevice {
    /// The device.
    pub id: DeviceId,
    /// The name it was added under.
    pub name: String,
}

/// One local first unmet condition, identified by its subject (§19.1, E5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingRecord {
    /// The work that cannot advance.
    pub subject: PendingSubject,
    /// The first condition in its processing order.
    pub reason: PendingReason,
    /// The text of the error behind `reason`: the provider, disk or
    /// database failure, naming every cause below it. `coven_pending.cause`
    /// keeps it beside the record, written in the same step. Only the run
    /// that met the error writes one: opening clears every cause, and a
    /// record whose last failure went back to an app call has none. It
    /// names the failure and is no part of the record's identity: equality
    /// ignores it, and `record` leaves it out.
    #[serde(skip)]
    pub cause: Option<String>,
}

impl PartialEq for PendingRecord {
    fn eq(&self, other: &Self) -> bool {
        self.subject == other.subject && self.reason == other.reason
    }
}
impl Eq for PendingRecord {}

/// The subject and its reason, then the error behind it when the record
/// keeps one, such as `Operation { .. }: Storage { .. }: S3: Network: …`.
impl std::fmt::Display for PendingRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {:?}", self.subject, self.reason)?;
        match &self.cause {
            Some(cause) => write!(f, ": {cause}"),
            None => Ok(()),
        }
    }
}

/// Stable identities of work, independent of attempts and wall-clock time.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PendingSubject {
    /// A numbered write.
    Write(#[serde(with = "encoding::write")] WriteId),
    /// A position in the store sequence.
    Slot(#[serde(with = "coven_database::slot_json")] Slot),
    /// A sealed key addressed to a member.
    KeyCopy {
        /// The key's audience.
        #[serde(with = "crate::pending_encoding::audience")]
        audience: Audience,
        /// The key identity.
        key: KeyId,
        /// The recipient.
        member: MemberId,
        /// The recipient's exact sealing credential (D9, Key copies).
        #[serde(with = "encoding::sealing")]
        sealing: coven_crypto::SealingKey,
    },
    /// A numbered snapshot.
    Snapshot(#[serde(with = "crate::pending_encoding::snapshot_id")] SnapshotId),
    /// This device's upload of a stored object, and its reads of its staged
    /// original while the upload waits.
    Upload {
        /// This device, which uploads it.
        device: DeviceId,
        /// The stored object's id.
        object: ObjectId,
    },
    /// This device's downloads of a stored object, for an app's pins: a
    /// read's failure goes to its caller and leaves no record (§17.7).
    Download {
        /// The device that uploaded it.
        device: DeviceId,
        /// The stored object's id.
        object: ObjectId,
    },
    /// A file a switch to the cloud asked this device to keep whole, which
    /// coven downloads itself: its pin is work coven drives, not an app
    /// call's (§17.8).
    Keep {
        /// The file.
        file: FileId,
    },
    /// A move of this device's files to the cloud: the files one write
    /// staged for the cloud, or the files the app moved (§17.3).
    Move {
        /// The move's identity on this device.
        id: coven_database::MoveId,
    },
    /// An unfinished journal row.
    Operation {
        /// The never reused local identity.
        id: OperationId,
        /// The work's purpose.
        kind: OperationKind,
    },
    /// A physical object waiting for safe deletion.
    Retention {
        /// The storage path.
        path: ObjectPath,
    },
    /// An audience that requires dependent work.
    Audience(#[serde(with = "crate::pending_encoding::audience")] Audience),
    /// The local storage connection.
    Connection,
}

/// A fact required before processing can continue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Prerequisite {
    /// A completed read and apply through the current sequence end, which a
    /// file read or a member-key replacement waits for. A slot never blocks
    /// on it.
    SequenceCatchUp,
    /// This audience must reload: a slot holds a raise or reset of it, and no
    /// snapshot announced after it is named yet.
    Reload(#[serde(with = "crate::pending_encoding::audience")] Audience),
    /// A slot's reload met a newer floor, which retention moved past the slots
    /// it needed; the next pass retries it from the new floor.
    Floor,
}

/// A local invariant or resource failure; immediate calls retain the native cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalFailure {
    /// Database state or persistence failed.
    Database,
    /// Local file access failed because the disk is full or its quota is
    /// spent.
    DiskFull,
    /// Local file access failed because the operating system refused this
    /// process the file or folder, or the volume is read-only.
    DiskDenied,
    /// Local file access failed for a reason that is neither: the file is
    /// gone, the device failed, or the cause names no operating system
    /// error.
    DiskOther,
    /// Key custody failed.
    KeyCustody,
    /// Cryptographic work failed.
    Crypto,
    /// Operation state failed.
    Operation,
    /// The work's runtime is shutting down and dropped it: the process is
    /// ending, and the next one runs the work again (E15).
    Shutdown,
    /// Coven's own code broke its rule or panicked: a bug. The work retries
    /// under backoff, as a database failure does, and fails again until the
    /// bug is fixed (E15).
    Bug,
}

impl LocalFailure {
    /// The disk failure `error` is, read from the operating system error
    /// first in its chain of causes: a full disk or spent quota, a refusal
    /// of access or a read-only volume, or anything else.
    pub(crate) fn of_disk(error: &(dyn std::error::Error + 'static)) -> Self {
        let mut current = Some(error);
        while let Some(cause) = current {
            if let Some(os) = cause.downcast_ref::<std::io::Error>() {
                return match os.kind() {
                    std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded => {
                        Self::DiskFull
                    }
                    std::io::ErrorKind::PermissionDenied
                    | std::io::ErrorKind::ReadOnlyFilesystem => Self::DiskDenied,
                    _ => Self::DiskOther,
                };
            }
            current = cause.source();
        }
        Self::DiskOther
    }
}

/// The provider's intended sharing change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderAccessAction {
    /// Share the location.
    Grant,
    /// Take sharing back.
    Revoke,
}

/// Why a file can't be read (§17.6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileMissingReason {
    /// Storage confirms nothing at the path a file's record names: the
    /// provider lost it. Nothing brings it back on its own; the app can stage
    /// it again.
    Lost,
    /// A file this device uploads, on it only until then, whose source
    /// failed: its original is missing, or coven's copy fails its check; its
    /// upload retries (§17.3, §17.9). A changed original fails its move.
    Source(FileSourceFailure),
}

/// A typed current condition, never an error's display string.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingReason {
    /// The named object has not arrived.
    Missing {
        /// The storage path.
        path: ObjectPath,
    },
    /// An earlier prerequisite remains unmet.
    Waits(Prerequisite),
    /// No usable copy of a required key has arrived.
    KeyUnavailable {
        /// The key's audience.
        #[serde(with = "crate::pending_encoding::audience")]
        audience: Audience,
        /// The missing key.
        key: KeyId,
        /// The members that hold the key, as the history this device applied
        /// names them, when the record was made with that history at hand: a
        /// stuck slot's, and the first queued write's. `Some` and empty means
        /// no member can ever send a copy (§12.1), which only Failed says.
        /// `None` is a record made without the history (the start of a
        /// connection, an operation's failure): it names no state.
        holders: Option<Vec<KeyHolder>>,
    },
    /// Processing requires a supported schema or format.
    UpdateRequired(RequiredUpdate),
    /// No storage is connected.
    NoStorage,
    /// A classified provider failure.
    Storage {
        /// The provider.
        provider: CloudProvider,
        /// The classification, without a process-local cause.
        failure: StorageFailure,
        /// Whether the provider answered the failed request, even with an
        /// error. `Network` alone cannot say: a server's error response is
        /// an answer, a lookup or connection failure is not (E5).
        answered: bool,
    },
    /// Complete immutable bytes failed a check.
    Refused(Refusal),
    /// Writes consumed after a permanent lower-slot cut-off.
    WritesRefused {
        /// The removal or replacement slot.
        #[serde(with = "coven_database::slot_json")]
        cutoff: Slot,
        /// The event that excluded the original author.
        cause: coven_database::WriteCutoff,
    },
    /// A provider has accepted work that is still waiting.
    ProviderPending {
        /// The intended change.
        action: ProviderAccessAction,
        /// The provider account.
        account: String,
    },
    /// Another member or invite still needs this account.
    AccountInUse {
        /// The shared account.
        account: String,
    },
    /// Revocation retained grants requiring owner action.
    AccessRemains {
        /// The account whose access remains.
        account: String,
        /// The retained grants.
        shares: Vec<RetainedAccess>,
    },
    /// The uploading device cannot supply this file.
    FileUnavailable(FileMissingReason),
    /// A user's original a move uploads is missing, and the move waits for
    /// it, or changed, and the move failed (§17.3).
    Original {
        /// The original's recorded path.
        #[serde(with = "crate::pending_encoding::path")]
        path: std::path::PathBuf,
        /// Missing or changed.
        failure: FileSourceFailure,
    },
    /// A local failure prevented progress.
    Failed(LocalFailure),
    /// The action was refused; an enclosing cleanup may still be owed (§11.3).
    Rejected(crate::OperationRejection),
    /// Both the initiating failure and the failure preventing undo (§11.3).
    Cleanup {
        /// The failure that made cleanup necessary.
        operation: Box<PendingReason>,
        /// The current condition preventing cleanup.
        cleanup: Box<PendingReason>,
    },
    /// Work awaits the app’s next action, such as approval or resuming a transfer.
    Paused,
    /// This installation is not in the store (§13.2, §14.1).
    Removed(coven_database::Removal),
    /// Sending needs this stable member's current credentials, which this
    /// installation does not hold (§14.1).
    MemberKeysReplaced {
        /// The member whose held keys were replaced.
        member: MemberId,
    },
}

/// What can permit another attempt; no attempt count determines it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retry {
    /// Retry when the prerequisite or retry deadline changes.
    Automatic,
    /// Retry only under software that may decide otherwise: a refusal under
    /// another build of coven, a required update once it is installed.
    AfterUpdate,
    /// A person must act.
    AppAction,
    /// A permanent cut-off or removal never retries.
    Never,
}
impl PendingRecord {
    /// The record of `subject`'s first unmet condition, with no cause.
    pub fn new(subject: PendingSubject, reason: PendingReason) -> Self {
        Self { subject, reason, cause: None }
    }

    /// Retry according to the cause and the owner of the unfinished work.
    pub fn retry(&self) -> Retry {
        match (&self.subject, self.reason.retry_cause()) {
            (
                PendingSubject::Upload { .. },
                PendingReason::FileUnavailable(FileMissingReason::Source(_)),
            ) => Retry::Automatic,
            // Only a pin's failed download is recorded, and its call
            // returns the failure: the app's next pin or read tries again,
            // and nothing else does.
            (PendingSubject::Download { .. }, reason) => match reason.clears() {
                Some(Clears::Never) => Retry::Never,
                Some(Clears::ByItself | Clears::ByPerson) => Retry::AppAction,
                None => match reason.retry() {
                    Retry::Automatic => Retry::AppAction,
                    retry => retry,
                },
            },
            (subject, reason)
                if matches!(
                    subject,
                    PendingSubject::Upload { .. } | PendingSubject::Keep { .. }
                ) || matches!(subject, PendingSubject::Operation { kind, .. } if kind.coven_owned()) =>
            {
                match reason.clears() {
                    Some(Clears::ByItself) => Retry::Automatic,
                    Some(Clears::ByPerson) => Retry::AppAction,
                    Some(Clears::Never) => Retry::Never,
                    None => reason.retry(),
                }
            }
            // An operation the app started returned this failure to the
            // app's call, and the app calls again. Nobody calls again for
            // work coven drives itself, such as the pass: a local failure
            // that clears by itself retries under the shared backoff.
            (subject, reason @ PendingReason::Failed(_))
                if !matches!(subject, PendingSubject::Operation { .. })
                    && reason.clears() == Some(Clears::ByItself) =>
            {
                Retry::Automatic
            }
            (_, reason) => reason.retry(),
        }
    }
}

/// How a failure clears (E5). Every failure is classified here, once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Clears {
    /// It can clear on its own: retry under the shared backoff.
    ByItself,
    /// Only a person's action clears it, such as signing in again.
    ByPerson,
    /// Nothing clears it: the provider or this installation refuses outright.
    Never,
}

/// A failure that received no usable provider response: the device is
/// offline (E5), and only a change in its network can end the wait early
/// (§6.7). A server's error response is an answer, so it is not offline.
pub(crate) fn offline(failure: StorageFailure, answered: bool) -> bool {
    failure == StorageFailure::Network && !answered
}

/// Storage failures that subjects other than files and coven's own
/// operations retry automatically (E5).
pub(crate) fn automatic_storage(failure: StorageFailure) -> bool {
    matches!(
        failure,
        StorageFailure::Network
            | StorageFailure::Corrupted
            | StorageFailure::Conflict
            | StorageFailure::RateLimited
            | StorageFailure::NotFound
            | StorageFailure::SessionExpired
    )
}

/// How a provider failure clears. Every variant is matched by name, so a
/// new one is classified here.
#[deny(clippy::wildcard_enum_match_arm)]
pub(crate) fn storage_clears(failure: StorageFailure) -> Clears {
    use StorageFailure::*;
    match failure {
        Network | Corrupted | Conflict | RateLimited | NotFound | SessionExpired
        | CatchUpRequired | QuotaExceeded | File | Bug => Clears::ByItself,
        Authentication
        | PermissionDenied
        | AccessNotRegistered { .. }
        | AccessNotGranted { .. }
        | ContainerNotFound
        | RegionMismatch
        | ClockSkew
        | InvalidConfiguration
        | NotStoreOwner
        | AccountIdUnavailable
        | SharingRefused(_)
        | ProviderUnavailable
        | MemberKeysMissing => Clears::ByPerson,
        DropboxAppFolderNotShareable
        | AlreadyExists
        | StoreExists
        | OwnLocationRequired
        | Refused
        | Protocol
        | InvalidPath
        | InvalidRange
        | InviteeIsStoreOwner
        | InvitationMismatch
        | SessionMismatch
        | InvalidPart
        | SingleRequestTooLarge { .. }
        | CopyTooLarge { .. }
        | Encoding => Clears::Never,
    }
}

impl PendingReason {
    /// Cleanup retries its own unmet condition, retaining the initiating cause.
    pub(crate) fn retry_cause(&self) -> &Self {
        match self {
            Self::Cleanup { cleanup, .. } => cleanup.retry_cause(),
            reason => reason,
        }
    }

    /// How the failure this reason reports clears; `None` for a wait or a
    /// verdict, which are not failures.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub(crate) fn clears(&self) -> Option<Clears> {
        match self.retry_cause() {
            Self::Failed(
                LocalFailure::Database
                | LocalFailure::DiskFull
                | LocalFailure::DiskDenied
                | LocalFailure::DiskOther
                | LocalFailure::KeyCustody
                | LocalFailure::Shutdown
                | LocalFailure::Bug,
            ) => Some(Clears::ByItself),
            Self::Failed(LocalFailure::Crypto | LocalFailure::Operation) => Some(Clears::Never),
            Self::Storage { failure, .. } => Some(storage_clears(*failure)),
            Self::Missing { .. }
            | Self::Waits(_)
            | Self::KeyUnavailable { .. }
            | Self::UpdateRequired(_)
            | Self::NoStorage
            | Self::Refused(_)
            | Self::WritesRefused { .. }
            | Self::ProviderPending { .. }
            | Self::AccountInUse { .. }
            | Self::AccessRemains { .. }
            | Self::FileUnavailable(_)
            | Self::Original { .. }
            | Self::Rejected(_)
            | Self::Cleanup { .. }
            | Self::Paused
            | Self::Removed(_)
            | Self::MemberKeysReplaced { .. } => None,
        }
    }

    /// Whether this reason waits for a condition to change, as opposed to
    /// reporting a failure or a verdict. A wait is ordinary progress, and
    /// is logged at debug where a failure is logged at warn.
    pub(crate) fn waits(&self) -> bool {
        match self {
            Self::Missing { .. }
            | Self::Waits(_)
            | Self::KeyUnavailable { .. }
            | Self::NoStorage
            | Self::ProviderPending { .. }
            | Self::AccountInUse { .. }
            | Self::Paused => true,
            Self::Original { failure, .. } => *failure == FileSourceFailure::Missing,
            Self::UpdateRequired(_)
            | Self::Storage { .. }
            | Self::Refused(_)
            | Self::WritesRefused { .. }
            | Self::AccessRemains { .. }
            | Self::FileUnavailable(_)
            | Self::Failed(_)
            | Self::Rejected(_)
            | Self::Removed(_)
            | Self::MemberKeysReplaced { .. } => false,
            Self::Cleanup { operation, cleanup } => operation.waits() && cleanup.waits(),
        }
    }

    /// Whether this reason records an attempt that received no usable
    /// provider response, which the connection reports as `Offline` (E5).
    pub(crate) fn offline(&self) -> bool {
        matches!(self, Self::Storage { failure, answered, .. } if offline(*failure, *answered))
    }

    #[deny(clippy::wildcard_enum_match_arm)]
    fn retry(&self) -> Retry {
        match self {
            Self::Missing { .. }
            | Self::Waits(_)
            | Self::KeyUnavailable { .. }
            | Self::ProviderPending { .. }
            | Self::AccountInUse { .. } => Retry::Automatic,
            // A missing original's move retries as its upload does, until
            // the original is back; a changed one failed the move, which
            // stays recorded until the app cancels it (§17.3).
            Self::Original { failure: FileSourceFailure::Missing, .. } => Retry::Automatic,
            Self::Original {
                failure: FileSourceFailure::Changed | FileSourceFailure::Integrity,
                ..
            } => Retry::AppAction,
            Self::Storage { failure, .. } if automatic_storage(*failure) => Retry::Automatic,
            Self::Refused(_) | Self::UpdateRequired(_) => Retry::AfterUpdate,
            Self::Rejected(_)
            | Self::WritesRefused { .. }
            | Self::Removed(_)
            | Self::MemberKeysReplaced { .. } => Retry::Never,
            Self::NoStorage
            | Self::Storage { .. }
            | Self::AccessRemains { .. }
            | Self::FileUnavailable(_)
            | Self::Failed(_)
            | Self::Cleanup { .. }
            | Self::Paused => Retry::AppAction,
        }
    }
}

/// A record as coven's `coven_uploads` and `coven_pending` views hold it
/// (api.md E4), without its cause: `coven_pending.cause` holds that.
impl coven_database::types::FromSql for PendingRecord {
    fn column_result(
        value: coven_database::types::ValueRef<'_>,
    ) -> coven_database::types::FromSqlResult<Self> {
        serde_json::from_slice(value.as_blob()?)
            .map_err(|error| coven_database::types::FromSqlError::Other(Box::new(error)))
    }
}

impl PendingValue for PendingRecord {
    fn provider_access(id: OperationId, account: String, grant: bool) -> Self {
        Self::new(
            PendingSubject::Operation { id, kind: OperationKind::RevokeAccess },
            PendingReason::ProviderPending {
                action: if grant {
                    ProviderAccessAction::Grant
                } else {
                    ProviderAccessAction::Revoke
                },
                account,
            },
        )
    }
    fn reload(audience: Audience) -> Self {
        Self::new(
            PendingSubject::Audience(audience.clone()),
            PendingReason::Waits(Prerequisite::Reload(audience)),
        )
    }
    fn writes_refused(slot: Slot, cutoff: Slot, cause: coven_database::WriteCutoff) -> Self {
        Self::new(PendingSubject::Slot(slot), PendingReason::WritesRefused { cutoff, cause })
    }
    type Subject = PendingSubject;
    fn subject(&self) -> &PendingSubject {
        &self.subject
    }
    fn operation(&self) -> Option<OperationId> {
        match self.subject {
            PendingSubject::Operation { id, .. } => Some(id),
            _ => None,
        }
    }
    fn move_of(&self) -> Option<coven_database::MoveId> {
        match self.subject {
            PendingSubject::Move { id } => Some(id),
            _ => None,
        }
    }
    fn pin_of(&self) -> Option<FileId> {
        match self.subject {
            PendingSubject::Keep { file } => Some(file),
            _ => None,
        }
    }
    fn object(&self) -> Option<(coven_database::FileWork, DeviceId, ObjectId)> {
        match self.subject {
            PendingSubject::Upload { device, object } => {
                Some((coven_database::FileWork::Upload, device, object))
            }
            PendingSubject::Download { device, object } => {
                Some((coven_database::FileWork::Download, device, object))
            }
            _ => None,
        }
    }
    fn slot(&self) -> Option<Slot> {
        match self.subject {
            PendingSubject::Slot(slot) => Some(slot),
            _ => None,
        }
    }
    fn ends_when_covered(&self) -> bool {
        !matches!(
            self.reason.retry_cause(),
            PendingReason::Refused(_) | PendingReason::WritesRefused { .. }
        )
    }
    /// Drawn at random each time this crate compiles. Every refusal is made
    /// here, from checks in this crate and in the crates it depends on, so a
    /// change to any of that code, or to a dependency, compiles this crate
    /// again and draws another. A version number would not do: development
    /// never moves it, so a refusal a since-fixed bug made would never be
    /// rechecked.
    // crates/coven-sync/build.rs fails the build when CONST_RANDOM_SEED fixes this draw.
    const BUILD: u128 = const_random::const_random!(u128);
    /// A refusal is a verdict on bytes that never change, judged against
    /// what never changes either: only another build can judge it
    /// differently, and each other build checks it once (§19.1).
    fn can_retry(&self, recorded_build: u128, schema_version: u32) -> bool {
        match self.reason.retry_cause() {
            PendingReason::Refused(_) => recorded_build != Self::BUILD,
            PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version }) => {
                schema_version >= *version
            }
            PendingReason::UpdateRequired(RequiredUpdate::CovenFormat { version }) => {
                coven_format::FORMAT_VERSION >= *version
            }
            _ => self.retry() == Retry::Automatic,
        }
    }
    fn cause(&self) -> Option<&str> {
        self.cause.as_deref()
    }
    fn with_cause(self, cause: Option<String>) -> Self {
        Self { cause, ..self }
    }
}

#[cfg(test)]
#[path = "pending_tests.rs"]
mod tests;
