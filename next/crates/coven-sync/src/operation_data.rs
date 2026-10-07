//! Each operation's retained intent, including the captured file reference for a download.

use crate::snapshot_data::{RaisedVersion, SnapshotJob, SnapshotTask, SnapshotTrigger};
use crate::{InviteAccess, OperationKind, SyncError};
use coven_database::{NewOperation, OperationRecord, OperationUpdate};
use coven_format::{
    store_log::{MemberRole, StoreLogEntry},
    MemberAccess, Object,
};
use coven_foundation::id_source::{CircleId, DeviceId, InviteId};
use coven_storage::{MemberRemoval, StorageInvitation};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Intent {
    Reset {
        #[serde(with = "crate::snapshot_data::snapshot_id")]
        snapshot: coven_format::store_log::SnapshotId,
    },
    RemoveMember {
        member: String,
    },
    CreateCircle {
        circle: CircleId,
        name: String,
    },
    AddCircleMember {
        circle: CircleId,
        member: String,
    },
    RemoveCircleMember {
        circle: CircleId,
        member: String,
    },
    DeleteCircle {
        circle: CircleId,
        rows: CircleRows,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum CircleRows {
    Delete,
    Deleted { write: Option<(DeviceId, u64)> },
}

impl Intent {
    pub(crate) fn kind(&self) -> OperationKind {
        match self {
            Self::Reset { .. } => OperationKind::Reset,
            Self::RemoveMember { .. } => OperationKind::RemoveMember,
            Self::CreateCircle { .. } => OperationKind::CreateCircle,
            Self::AddCircleMember { .. } => OperationKind::AddCircleMember,
            Self::RemoveCircleMember { .. } => OperationKind::RemoveCircleMember,
            Self::DeleteCircle { .. } => OperationKind::DeleteCircle,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct EntryWork {
    pub(crate) intent: Intent,
    // Present once preparation fixes the author view, replacement key ids and bytes.
    pub(crate) entry: Option<Vec<u8>>,
    pub(crate) removal: Option<MemberRemoval>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct InviteWork {
    pub(crate) id: InviteId,
    pub(crate) role: MemberRole,
    pub(crate) access: InviteAccess,
    pub(crate) secret: coven_crypto::SecretText,
    pub(crate) expires_at: SystemTime,
    pub(crate) state: InviteState,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum InviteState {
    Grant,
    Waiting {
        invitation: StorageInvitation,
        request: Option<Vec<u8>>,
    },
    Approving {
        request: Vec<u8>,
        entry: Option<Vec<u8>>,
    },
    Revoking,
    Settled,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Data {
    PublishSchema {
        version: u32,
    },
    KeepFile(KeepFileWork),
    Snapshots(crate::snapshot_data::SnapshotTask),
    Entry(EntryWork),
    Invite(InviteWork),
    Revoke {
        member: String,
        result: Option<MemberRemoval>,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) struct KeepFileWork {
    // Encoded FileRef contains its file key, like the private operation record.
    pub(crate) reference: Vec<u8>,
    // A changed row ends this intent permanently, including explicit retries.
    pub(crate) obsolete: bool,
}

impl Data {
    pub(crate) fn read(record: &OperationRecord) -> Result<Self, SyncError> {
        let data: Self = serde_json::from_slice(&record.data)?;
        if data.kind().name() != record.kind {
            return Err(coven_database::DbError::DamagedDatabase.into());
        }
        Ok(data)
    }
    pub(crate) fn kind(&self) -> OperationKind {
        match self {
            Self::PublishSchema { .. } => OperationKind::MigrateSchema,
            Self::KeepFile(_) => OperationKind::ChangeFileLocation,
            Self::Entry(work) => work.intent.kind(),
            Self::Snapshots(task) => match task.job {
                SnapshotJob::Write {
                    trigger: SnapshotTrigger::Raise { version, .. },
                    ..
                } => match version {
                    RaisedVersion::Schema(_) => OperationKind::RaiseSchema,
                    RaisedVersion::Format(_) => OperationKind::RaiseFormat,
                },
                crate::snapshot_data::SnapshotJob::Write {
                    trigger: crate::snapshot_data::SnapshotTrigger::Reset,
                    ..
                } => OperationKind::Reset,
                crate::snapshot_data::SnapshotJob::Write { .. } => OperationKind::WriteSnapshot,
                crate::snapshot_data::SnapshotJob::Reload { .. } => OperationKind::ReloadSnapshots,
                crate::snapshot_data::SnapshotJob::Retain => OperationKind::Retention,
            },
            Self::Invite(_) => OperationKind::Invite,
            Self::Revoke { .. } => OperationKind::RevokeAccess,
        }
    }
    pub(crate) fn new_operation(&self, started_by: &str) -> Result<NewOperation, SyncError> {
        Ok(NewOperation {
            kind: self.kind().name().into(),
            data: serde_json::to_vec(self)?,
            started_by: started_by.into(),
        })
    }
    pub(crate) fn update(
        &self,
        record: &OperationRecord,
        step: u32,
    ) -> Result<OperationUpdate, SyncError> {
        Ok(OperationUpdate {
            id: record.id,
            previous: record.last_step,
            last_step: step,
            data: serde_json::to_vec(self)?,
        })
    }
    /// Circle deletion commits its row write and reset publishes its snapshot
    /// before the shared entry steps.
    pub(crate) fn entry_step_number(&self, step: u32) -> u32 {
        if self.raises_version() {
            return step + 2;
        }
        step + match self {
            Self::Entry(EntryWork {
                intent: Intent::DeleteCircle { .. },
                ..
            }) => 1,
            Self::Entry(EntryWork {
                intent: Intent::Reset { .. },
                ..
            }) => 3,
            _ => 0,
        }
    }
    pub(crate) fn completing_reset(&self, record: &OperationRecord) -> bool {
        matches!(
            self,
            Self::Entry(EntryWork {
                intent: Intent::Reset { .. },
                ..
            })
        ) && record.last_step >= self.entry_step_number(4)
    }

    pub(crate) fn entry(&self) -> Result<Option<StoreLogEntry>, SyncError> {
        let bytes = match self {
            Self::Snapshots(SnapshotTask {
                job:
                    SnapshotJob::Write {
                        trigger: SnapshotTrigger::Raise { entry, .. },
                        ..
                    },
                ..
            }) => entry.as_ref(),
            Self::Entry(work) => work.entry.as_ref(),
            Self::Invite(InviteWork {
                state: InviteState::Approving { entry, .. },
                ..
            }) => entry.as_ref(),
            _ => None,
        };
        bytes
            .map(|bytes| match Object::decode(bytes)? {
                Object::StoreLog(entry) => Ok(entry),
                _ => Err(coven_database::DbError::DamagedDatabase.into()),
            })
            .transpose()
    }
    pub(crate) fn set_entry(&mut self, value: Option<StoreLogEntry>) -> Result<(), SyncError> {
        let value = value
            .map(|entry| Object::StoreLog(entry).encode())
            .transpose()?;
        match self {
            Self::Snapshots(SnapshotTask {
                job:
                    SnapshotJob::Write {
                        trigger: SnapshotTrigger::Raise { entry, .. },
                        ..
                    },
                ..
            }) => *entry = value,
            Self::Entry(work) => work.entry = value,
            Self::Invite(InviteWork {
                state: InviteState::Approving { entry, .. },
                ..
            }) => *entry = value,
            _ => return Err(coven_database::DbError::DamagedDatabase.into()),
        }
        Ok(())
    }
    pub(crate) fn writes_entry(&self) -> bool {
        self.raises_version()
            || matches!(
                self,
                Self::Snapshots(SnapshotTask {
                    job: SnapshotJob::Write {
                        trigger: SnapshotTrigger::Reset,
                        ..
                    },
                    ..
                })
            )
            || matches!(
                self,
                Self::Entry(_)
                    | Self::Invite(InviteWork {
                        state: InviteState::Approving { .. },
                        ..
                    })
            )
    }

    pub(crate) fn raises_version(&self) -> bool {
        matches!(
            self,
            Self::Snapshots(SnapshotTask {
                job: SnapshotJob::Write {
                    trigger: SnapshotTrigger::Raise { .. },
                    ..
                },
                ..
            })
        )
    }
}

impl OperationKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::MigrateSchema => "migrate-schema",
            Self::Reset => "reset",
            Self::ChangeFileLocation => "change-file-location",
            Self::WriteSnapshot => "write-snapshot",
            Self::RaiseSchema => "raise-schema",
            Self::RaiseFormat => "raise-format",
            Self::ReloadSnapshots => "reload-snapshots",
            Self::Retention => "retention",
            Self::RemoveMember => "remove-member",
            Self::CreateCircle => "create-circle",
            Self::AddCircleMember => "add-circle-member",
            Self::RemoveCircleMember => "remove-circle-member",
            Self::DeleteCircle => "delete-circle",
            Self::Invite => "invite",
            Self::RevokeAccess => "revoke-access",
        }
    }
}

impl InviteAccess {
    pub(crate) fn member_access(&self) -> MemberAccess {
        match self {
            Self::ProviderAccount { email } => {
                MemberAccess::ProviderAccount(email.trim().to_ascii_lowercase())
            }
            Self::S3AccessKey { access_key_id, .. } => MemberAccess::S3AccessKey {
                access_key_id: access_key_id.clone(),
            },
        }
    }
}
