//! Current unmet conditions shared by calls, persistence and the application query.

use super::pending_encoding as encoding;
use crate::{OperationId, OperationKind, Refusal};
use coven_database::PendingValue;
use coven_format::{admin::SnapshotId, path::ObjectPath};
use coven_foundation::id_source::{DeviceId, FileId, InviteId, KeyId, MemberId};
use coven_merge::{Audience, Slot, WriteId};
use coven_storage::{CloudProvider, RetainedAccess, StorageFailure};
use serde::{Deserialize, Serialize};

pub use coven_format::RequiredUpdate;

/// The attaching device cannot supply the original file bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileSourceFailure {
    /// The original file is absent.
    Missing,
    /// The original changed after attachment.
    Changed,
    /// The original fails its retained content checks.
    Integrity,
}

/// One local first unmet condition, identified by its subject (§19.1, E5).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingRecord {
    /// The work that cannot advance.
    pub subject: PendingSubject,
    /// The first condition in its processing order.
    pub reason: PendingReason,
    /// The error behind `reason`, while this process holds it: the provider,
    /// disk or database failure, with every cause below it. Only the reason
    /// is persisted, so a record read after a restart, or one whose last
    /// failure went back to an app call, has none. It names the failure and
    /// is no part of the record's identity: equality ignores it.
    #[serde(skip)]
    pub cause: Option<std::sync::Arc<dyn std::error::Error + Send + Sync>>,
}

impl PartialEq for PendingRecord {
    fn eq(&self, other: &Self) -> bool {
        self.subject == other.subject && self.reason == other.reason
    }
}
impl Eq for PendingRecord {}

/// The subject and its reason, then the error behind it while this process
/// holds it, such as `Operation { .. }: Storage { .. }: S3: Network: …`.
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
    Slot(#[serde(with = "encoding::slot")] Slot),
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
    /// A file and its uploader.
    File {
        /// The uploading device.
        device: DeviceId,
        /// The file's identity.
        file: FileId,
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
    /// A joining request.
    JoinRequest(InviteId),
    /// The local storage connection.
    Connection,
}

/// A fact required before processing can continue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Prerequisite {
    /// A completed read and apply through the current sequence end.
    SequenceCatchUp,
    /// A storage object must arrive.
    Object(ObjectPath),
    /// The author's device must be registered.
    DeviceRegistration(DeviceId),
    /// A migrated schema must be published.
    SchemaRaise {
        /// The schema's audience.
        #[serde(with = "crate::pending_encoding::audience")]
        audience: Audience,
        /// The migrated version.
        version: u32,
    },
    /// This audience must reload.
    Reload(#[serde(with = "crate::pending_encoding::audience")] Audience),
    /// A snapshot needed to load history below the floor.
    Snapshot(#[serde(with = "encoding::snapshot_id")] SnapshotId),
}

/// A local invariant or resource failure; immediate calls retain the native cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalFailure {
    /// Database state or persistence failed.
    Database,
    /// Local file access failed.
    Disk,
    /// Key custody failed.
    KeyCustody,
    /// Cryptographic work failed.
    Crypto,
    /// Operation state failed.
    Operation,
}

/// The provider's intended sharing change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderAccessAction {
    /// Share the location.
    Grant,
    /// Take sharing back.
    Revoke,
}

/// Why a file can't be read (§17.4).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileMissingReason {
    /// Storage confirms nothing at the path a row names: the provider lost
    /// it. Nothing brings it back on its own; the app can attach it again.
    Lost,
    /// This device's pending attachment, whose source failed; its upload
    /// retries (§17.7).
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
        #[serde(with = "encoding::slot")]
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
    /// The owner's device must perform this work.
    PendingOwner {
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
    /// This installation was removed.
    Removed,
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
    /// Retry only after the applicable software update.
    AfterUpdate,
    /// A person must act.
    AppAction,
    /// A permanent cut-off or removal never retries.
    Never,
}
impl PendingRecord {
    /// The record of `subject`'s first unmet condition, with no cause held.
    pub fn new(subject: PendingSubject, reason: PendingReason) -> Self {
        Self {
            subject,
            reason,
            cause: None,
        }
    }

    /// Retry according to the cause and the owner of the unfinished work.
    pub fn retry(&self) -> Retry {
        match (&self.subject, self.reason.retry_cause()) {
            (
                PendingSubject::File { .. },
                PendingReason::FileUnavailable(FileMissingReason::Source(_)),
            ) => Retry::Automatic,
            (subject, reason)
                if matches!(subject, PendingSubject::File { .. })
                    || matches!(subject, PendingSubject::Operation { kind, .. } if kind.coven_owned()) =>
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
        Network | Conflict | RateLimited | NotFound | SessionExpired | CatchUpRequired
        | QuotaExceeded | File => Clears::ByItself,
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
        | ProviderUnavailable
        | MemberKeysMissing => Clears::ByPerson,
        DropboxAppFolderNotShareable
        | AlreadyExists
        | StoreExists
        | OwnBucketRequired { .. }
        | Refused
        | Protocol
        | InvalidPath
        | InvalidRange
        | InviteeIsStoreOwner
        | InvitationMismatch
        | SessionMismatch
        | SessionIdentityUnknown
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
                LocalFailure::Database | LocalFailure::Disk | LocalFailure::KeyCustody,
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
            | Self::PendingOwner { .. }
            | Self::AccountInUse { .. }
            | Self::AccessRemains { .. }
            | Self::FileUnavailable(_)
            | Self::Rejected(_)
            | Self::Cleanup { .. }
            | Self::Paused
            | Self::Removed
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
            | Self::PendingOwner { .. }
            | Self::AccountInUse { .. }
            | Self::Paused => true,
            Self::UpdateRequired(_)
            | Self::Storage { .. }
            | Self::Refused(_)
            | Self::WritesRefused { .. }
            | Self::AccessRemains { .. }
            | Self::FileUnavailable(_)
            | Self::Failed(_)
            | Self::Rejected(_)
            | Self::Removed
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
            | Self::PendingOwner { .. }
            | Self::AccountInUse { .. } => Retry::Automatic,
            Self::Storage { failure, .. } if automatic_storage(*failure) => Retry::Automatic,
            Self::Refused(_) | Self::UpdateRequired(_) => Retry::AfterUpdate,
            Self::Rejected(_)
            | Self::WritesRefused { .. }
            | Self::Removed
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
/// (api.md E4), without the cause only this process holds.
impl coven_database::types::FromSql for PendingRecord {
    fn column_result(
        value: coven_database::types::ValueRef<'_>,
    ) -> coven_database::types::FromSqlResult<Self> {
        serde_json::from_slice(value.as_blob()?)
            .map_err(|error| coven_database::types::FromSqlError::Other(Box::new(error)))
    }
}

impl PendingValue for PendingRecord {
    fn provider_access(id: OperationId, account: String, grant: bool, owner: bool) -> Self {
        Self::new(
            PendingSubject::Operation {
                id,
                kind: OperationKind::RevokeAccess,
            },
            if owner {
                PendingReason::ProviderPending {
                    action: if grant {
                        ProviderAccessAction::Grant
                    } else {
                        ProviderAccessAction::Revoke
                    },
                    account,
                }
            } else {
                PendingReason::PendingOwner { account }
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
        Self::new(
            PendingSubject::Slot(slot),
            PendingReason::WritesRefused { cutoff, cause },
        )
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
    fn file(&self) -> Option<(DeviceId, FileId)> {
        match self.subject {
            PendingSubject::File { device, file } => Some((device, file)),
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
    fn can_retry(&self, recorded_version: &str, schema_version: u32) -> bool {
        match self.reason.retry_cause() {
            PendingReason::Refused(_) => recorded_version != env!("CARGO_PKG_VERSION"),
            PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version }) => {
                schema_version >= *version
            }
            PendingReason::UpdateRequired(RequiredUpdate::CovenFormat { version }) => {
                coven_format::FORMAT_VERSION >= *version
            }
            _ => self.retry() == Retry::Automatic,
        }
    }
}

#[cfg(test)]
#[path = "pending_tests.rs"]
mod tests;
