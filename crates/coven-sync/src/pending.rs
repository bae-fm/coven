//! Current unmet conditions shared by calls, persistence and the application query.

use super::pending_encoding as encoding;
use crate::{OperationId, OperationKind, Refusal};
use coven_database::PendingValue;
use coven_format::{admin::SnapshotId, path::ObjectPath};
use coven_foundation::id_source::{DeviceId, FileId, InviteId, KeyId, MemberId};
use coven_merge::{Audience, Slot, WriteId};
use coven_storage::{CloudProvider, RetainedAccess, StorageFailure};
use serde::{Deserialize, Serialize};

/// The software version required to decode or apply an input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequiredUpdate {
    /// The application's schema.
    AppSchema {
        /// Required application schema version.
        version: u32,
    },
    /// Coven's stored-object format.
    CovenFormat {
        /// Required object format version.
        version: u16,
    },
}

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRecord {
    /// The work that cannot advance.
    pub subject: PendingSubject,
    /// The first condition in its processing order.
    pub reason: PendingReason,
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
        /// The recipient's exact sealing credential (D8).
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

/// Why a file's uploader cannot supply its bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileMissingReason {
    /// The uploading device was removed.
    DeviceRemoved,
    /// Another installation replaced the uploader.
    DeviceReplaced,
    /// Its local source is unavailable.
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
    /// This S3 key awaits deletion with admin access or explicit confirmation (§14.3).
    DeleteAccessKey(#[serde(with = "encoding::AccessKey")] crate::AccessKeyToDelete),
    /// The uploading device cannot supply this file.
    FileUnavailable(FileMissingReason),
    /// A local failure prevented progress.
    Failed(LocalFailure),
    /// Work awaits the app’s next action, such as approval or resuming a transfer.
    Paused,
    /// Provider work needs the storage account's admin access again, which
    /// coven never keeps beyond one operation (§20.10).
    AdminAccessRequired,
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
    /// Retry according to the cause and the owner of the unfinished work.
    pub fn retry(&self) -> Retry {
        match (&self.subject, &self.reason) {
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

/// How a provider failure clears.
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
        _ => Clears::Never,
    }
}

impl PendingReason {
    /// How the failure this reason reports clears; `None` for a wait or a
    /// verdict, which are not failures.
    pub(crate) fn clears(&self) -> Option<Clears> {
        match self {
            Self::Failed(
                LocalFailure::Database | LocalFailure::Disk | LocalFailure::KeyCustody,
            ) => Some(Clears::ByItself),
            Self::Failed(LocalFailure::Crypto | LocalFailure::Operation) => Some(Clears::Never),
            Self::Storage { failure, .. } => Some(storage_clears(*failure)),
            _ => None,
        }
    }

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
            Self::WritesRefused { .. } | Self::Removed | Self::MemberKeysReplaced { .. } => {
                Retry::Never
            }
            _ => Retry::AppAction,
        }
    }
}

impl PendingValue for PendingRecord {
    fn provider_access(id: OperationId, account: String, grant: bool, owner: bool) -> Self {
        Self {
            subject: PendingSubject::Operation {
                id,
                kind: OperationKind::RevokeAccess,
            },
            reason: if owner {
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
        }
    }
    fn reload(audience: Audience) -> Self {
        Self {
            subject: PendingSubject::Audience(audience.clone()),
            reason: PendingReason::Waits(Prerequisite::Reload(audience)),
        }
    }
    fn access_key(id: OperationId, key: crate::AccessKeyToDelete) -> Self {
        Self {
            subject: PendingSubject::Operation {
                id,
                kind: crate::OperationKind::DeleteAccessKey,
            },
            reason: PendingReason::DeleteAccessKey(key),
        }
    }
    fn writes_refused(slot: Slot, cutoff: Slot, cause: coven_database::WriteCutoff) -> Self {
        Self {
            subject: PendingSubject::Slot(slot),
            reason: PendingReason::WritesRefused { cutoff, cause },
        }
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
    fn slot(&self) -> Option<Slot> {
        match self.subject {
            PendingSubject::Slot(slot) => Some(slot),
            _ => None,
        }
    }
    fn ends_when_covered(&self) -> bool {
        !matches!(
            self.reason,
            PendingReason::Refused(_) | PendingReason::WritesRefused { .. }
        )
    }
    fn can_retry(&self, recorded_version: &str, schema_version: u32) -> bool {
        match &self.reason {
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
