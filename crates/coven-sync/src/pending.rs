//! Current unmet conditions shared by calls, persistence and the application query.

use super::pending_encoding as encoding;
use crate::{OperationId, OperationKind, Refusal};
use coven_database::{DropReason, PendingValue};
use coven_format::{path::ObjectPath, store_log::SnapshotId, value::EntryId};
use coven_foundation::id_source::{DeviceId, FileId, InviteId, KeyId, MemberId};
use coven_merge::{Audience, WriteId};
use coven_storage::{CloudProvider, RetainedAccess, StorageFailure};
use serde::{Deserialize, Serialize};

pub use coven_format::pending::{FileSourceFailure, RequiredUpdate};

/// One first unmet condition, identified by its subject and reporting device.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRecord {
    /// The work that cannot advance.
    pub subject: PendingSubject,
    /// The first condition in its processing order.
    pub reason: PendingReason,
    /// This device for a local observation, or the authenticated peer.
    pub reported_by: DeviceId,
}

/// Stable identities of work, independent of attempts and wall-clock time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingSubject {
    /// A numbered write.
    Write(#[serde(with = "encoding::write")] WriteId),
    /// A numbered membership entry.
    Entry(#[serde(with = "encoding::entry")] EntryId),
    /// A sealed key addressed to a member.
    KeyCopy {
        /// The key's audience.
        #[serde(with = "crate::snapshot_data::audience")]
        audience: Audience,
        /// The key identity.
        key: KeyId,
        /// The recipient.
        member: MemberId,
        /// The recipient's exact sealing credential (D8).
        #[serde(with = "encoding::sealing")]
        sealing: coven_crypto::SealingKey,
    },
    /// A key-copy object whose clear prefix does not decode. It names no
    /// audience, key or recipient, so its path identifies it (§19.1).
    KeyCopyPath {
        /// The copy's storage path.
        path: ObjectPath,
    },
    /// A numbered snapshot.
    Snapshot(#[serde(with = "crate::snapshot_data::snapshot_id")] SnapshotId),
    /// A device's replaceable positions.
    Positions(DeviceId),
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
    Audience(#[serde(with = "crate::snapshot_data::audience")] Audience),
    /// Comparable state that disagrees with a peer.
    Agreement {
        /// The compared audience.
        #[serde(with = "crate::snapshot_data::audience")]
        audience: Audience,
        /// The compared device.
        peer: DeviceId,
    },
    /// A joining request.
    JoinRequest(InviteId),
    /// The local storage connection.
    Connection,
}

/// A fact required before processing can continue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Prerequisite {
    /// Membership and key-copy discovery must finish within the sending window.
    CatchUp,
    /// A storage object must arrive.
    Object(ObjectPath),
    /// The author's device must be registered.
    DeviceRegistration(DeviceId),
    /// A migrated schema must be published.
    SchemaPublication {
        /// The schema's audience.
        #[serde(with = "crate::snapshot_data::audience")]
        audience: Audience,
        /// The migrated version.
        version: u32,
    },
    /// This audience must reload.
    Reload(#[serde(with = "crate::snapshot_data::audience")] Audience),
    /// A reader must post positions.
    Positions(DeviceId),
    /// Physical cleanup must retain an entry's reversal inputs.
    EntryFinality(#[serde(with = "encoding::entry")] EntryId),
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
    Source(#[serde(with = "encoding::SourceFailure")] FileSourceFailure),
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
        #[serde(with = "crate::snapshot_data::audience")]
        audience: Audience,
        /// The missing key.
        key: KeyId,
    },
    /// Processing requires a supported schema or format.
    UpdateRequired(#[serde(with = "encoding::Update")] RequiredUpdate),
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
    Refused(#[serde(with = "encoding::refusal")] Refusal),
    /// Replaceable positions failed a check.
    InvalidPositions(#[serde(with = "encoding::refusal")] Refusal),
    /// Replay rejected this entry or write.
    Dropped(#[serde(with = "encoding::Drop")] DropReason),
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
    /// This S3 key awaits deletion with admin access or explicit confirmation (§13).
    DeleteAccessKey(#[serde(with = "encoding::AccessKey")] crate::AccessKeyToDelete),
    /// The uploading device cannot supply this file.
    FileUnavailable(FileMissingReason),
    /// Comparable fingerprints differ.
    Disagrees,
    /// A local failure prevented progress.
    Failed(LocalFailure),
    /// Work awaits the app’s next action, such as approval or resuming a transfer.
    Paused,
    /// Provider work needs the storage account's admin access again, which
    /// coven never keeps beyond one operation (§4).
    AdminAccessRequired,
    /// This installation was removed.
    Removed,
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
    /// Recompute on replay, without retrying the subject.
    Never,
}
impl PendingRecord {
    /// Derive retry policy from the reason and its subject. A dropped entry or
    /// write is recomputed by replay; an operation whose attempt replay dropped runs again
    /// on an explicit retry, validating against the current replay (§18).
    pub fn retry(&self) -> Retry {
        match (&self.subject, &self.reason) {
            (PendingSubject::Operation { .. }, PendingReason::Dropped(_)) => Retry::AppAction,
            (_, reason) => reason.retry(),
        }
    }
}

/// Storage failures that transient work retries automatically (E5).
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

impl PendingReason {
    fn retry(&self) -> Retry {
        match self {
            Self::Missing { .. }
            | Self::Waits(_)
            | Self::KeyUnavailable { .. }
            | Self::ProviderPending { .. }
            | Self::PendingOwner { .. }
            | Self::AccountInUse { .. }
            | Self::InvalidPositions(_) => Retry::Automatic,
            Self::Storage { failure, .. } if automatic_storage(*failure) => Retry::Automatic,
            Self::Refused(_) | Self::UpdateRequired(_) => Retry::AfterUpdate,
            Self::Dropped(_) | Self::Removed => Retry::Never,
            _ => Retry::AppAction,
        }
    }
}

impl PendingValue for PendingRecord {
    fn provider_access(
        id: OperationId,
        reported_by: DeviceId,
        account: String,
        grant: bool,
        owner: bool,
    ) -> Self {
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
            reported_by,
        }
    }
    fn reload(audience: Audience, reported_by: DeviceId) -> Self {
        Self {
            subject: PendingSubject::Audience(audience.clone()),
            reason: PendingReason::Waits(Prerequisite::Reload(audience)),
            reported_by,
        }
    }
    fn access_key(id: OperationId, reported_by: DeviceId, key: crate::AccessKeyToDelete) -> Self {
        Self {
            subject: PendingSubject::Operation {
                id,
                kind: crate::OperationKind::DeleteAccessKey,
            },
            reason: PendingReason::DeleteAccessKey(key),
            reported_by,
        }
    }
    fn dropped_entry(entry: EntryId, reason: DropReason) -> Self {
        Self {
            subject: PendingSubject::Entry(entry),
            reason: PendingReason::Dropped(reason),
            reported_by: entry.device,
        }
    }
    fn dropped_write(write: coven_merge::WriteId, reason: DropReason, reporter: DeviceId) -> Self {
        Self {
            subject: PendingSubject::Write(write),
            reason: PendingReason::Dropped(reason),
            reported_by: reporter,
        }
    }
    type Subject = PendingSubject;
    fn subject(&self) -> &PendingSubject {
        &self.subject
    }
    fn reported_by(&self) -> DeviceId {
        self.reported_by
    }
    fn operation(&self) -> Option<OperationId> {
        match self.subject {
            PendingSubject::Operation { id, .. } => Some(id),
            _ => None,
        }
    }
    fn log_object(&self) -> Option<coven_database::LogObject> {
        match self.subject {
            PendingSubject::Write(id) => Some(coven_database::LogObject::Write(id)),
            PendingSubject::Entry(id) => Some(coven_database::LogObject::Entry(id)),
            _ => None,
        }
    }
    fn ends_when_covered(&self) -> bool {
        !matches!(
            self.reason,
            PendingReason::Refused(_) | PendingReason::Dropped(_)
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

impl PendingSubject {
    fn tag(&self) -> u8 {
        match self {
            Self::Write(_) => 0,
            Self::Entry(_) => 1,
            Self::KeyCopy { .. } => 2,
            Self::KeyCopyPath { .. } => 3,
            Self::Snapshot(_) => 4,
            Self::Positions(_) => 5,
            Self::File { .. } => 6,
            Self::Operation { .. } => 7,
            Self::Retention { .. } => 8,
            Self::Audience(_) => 9,
            Self::Agreement { .. } => 10,
            Self::JoinRequest(_) => 11,
            Self::Connection => 12,
        }
    }
}
impl Ord for PendingSubject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Write(a), Self::Write(b)) => a.cmp(b),
            (Self::Entry(a), Self::Entry(b)) => a.cmp(b),
            (
                Self::KeyCopy {
                    audience: a,
                    key: ak,
                    member: am,
                    sealing: asl,
                },
                Self::KeyCopy {
                    audience: b,
                    key: bk,
                    member: bm,
                    sealing: bsl,
                },
            ) => (a, ak, am, asl).cmp(&(b, bk, bm, bsl)),
            (Self::KeyCopyPath { path: a }, Self::KeyCopyPath { path: b }) => a.cmp(b),
            (Self::Snapshot(a), Self::Snapshot(b)) => {
                (&a.audience, a.device, a.number).cmp(&(&b.audience, b.device, b.number))
            }
            (Self::Positions(a), Self::Positions(b)) => a.cmp(b),
            (
                Self::File {
                    device: a,
                    file: af,
                },
                Self::File {
                    device: b,
                    file: bf,
                },
            ) => (a, af).cmp(&(b, bf)),
            (Self::Operation { id: a, kind: ak }, Self::Operation { id: b, kind: bk }) => {
                (a, ak).cmp(&(b, bk))
            }
            (Self::Retention { path: a }, Self::Retention { path: b }) => a.cmp(b),
            (Self::Audience(a), Self::Audience(b)) => a.cmp(b),
            (
                Self::Agreement {
                    audience: a,
                    peer: ap,
                },
                Self::Agreement {
                    audience: b,
                    peer: bp,
                },
            ) => (a, ap).cmp(&(b, bp)),
            (Self::JoinRequest(a), Self::JoinRequest(b)) => a.cmp(b),
            _ => self.tag().cmp(&other.tag()),
        }
    }
}
impl PartialOrd for PendingSubject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
#[path = "pending_tests.rs"]
mod tests;
