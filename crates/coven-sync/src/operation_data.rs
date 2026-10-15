//! Each operation's retained intent and completed steps.

use crate::snapshot_data::{SnapshotJob, SnapshotTask, SnapshotTrigger};
use crate::{OperationKind, SyncError};
use coven_database::{NewOperation, OperationRecord, OperationUpdate};
use coven_format::{
    store_log::{MemberRole, StoreChange, StoreLogEntry},
    MemberAccess, Object,
};
use coven_foundation::id_source::{CircleId, InviteId, MemberId};
use coven_storage::StorageInvitation;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Intent {
    RotateKey {
        #[serde(with = "crate::snapshot_data::audience")]
        audience: coven_merge::Audience,
    },
    Reset {
        #[serde(with = "crate::snapshot_data::snapshot_id")]
        snapshot: coven_format::store_log::SnapshotId,
    },
    CreateCircle {
        circle: CircleId,
        name: String,
    },
    AddCircleMember {
        circle: CircleId,
        member: MemberId,
    },
    DeleteCircle {
        circle: CircleId,
    },
}

impl Intent {
    pub(crate) fn kind(&self) -> OperationKind {
        match self {
            Self::Reset { .. } => OperationKind::Reset,
            Self::RotateKey { .. } => OperationKind::RotateKey,
            Self::CreateCircle { .. } => OperationKind::CreateCircle,
            Self::AddCircleMember { .. } => OperationKind::AddCircleMember,
            Self::DeleteCircle { .. } => OperationKind::DeleteCircle,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct EntryWork {
    pub(crate) intent: Intent,
    // Present once preparation fixes the author view and entry bytes.
    pub(crate) entry: Option<Vec<u8>>,
}

/// A key coven makes with admin access (§4, §18). Each mint call draws the
/// scope's names afresh before it reaches the provider; the minted key's
/// secret joins the journal with them, as an invite's does.
#[derive(Serialize, Deserialize)]
pub(crate) struct MintWork {
    pub(crate) scope: coven_storage::MintScope,
    pub(crate) minted: Option<coven_storage::S3MemberKey>,
    /// Whether the key passed its member-side check.
    pub(crate) checked: bool,
}

impl MintWork {
    pub(crate) fn new(scope: coven_storage::MintScope) -> Self {
        Self {
            scope,
            minted: None,
            checked: false,
        }
    }
}

/// An admin desktop replacing a member's S3 key (§13): make the key with admin
/// access, deliver it sealed to the member, record it, then delete the old one.
#[derive(Serialize, Deserialize)]
pub(crate) struct ReplaceKeyWork {
    pub(crate) member: MemberId,
    /// The attempt naming the delivery.
    pub(crate) attempt: uuid::Uuid,
    pub(crate) mint: MintWork,
    /// The sealed delivery's fixed bytes, retained for every retry.
    pub(crate) delivery: Option<Vec<u8>>,
    pub(crate) entry: Option<Vec<u8>>,
}

/// How an invited person reaches storage, as the invite's journal keeps it.
#[derive(Serialize, Deserialize)]
pub(crate) enum InviteGrant {
    /// The provider shares the store with this account.
    Account { email: String },
    /// An S3 key and who holds it, made in the console or by coven.
    S3Key(coven_storage::S3MemberKey),
    /// Coven is making the key with admin access.
    Minting(MintWork),
}

impl InviteGrant {
    /// The access the addition records; a key still being made has none yet.
    pub(crate) fn member_access(&self) -> Result<MemberAccess, SyncError> {
        match self {
            Self::Account { email } => Ok(MemberAccess::ProviderAccount(email.clone())),
            Self::S3Key(key) => Ok(MemberAccess::S3AccessKey {
                access_key_id: key.credentials.access_key_id.clone(),
                principal: key.principal.clone(),
            }),
            Self::Minting(_) => Err(SyncError::InvitationChanged),
        }
    }
}

/// A checked join request and the stored object it came from. While the
/// listing shows that same object, the request needs no further read (SP step 5).
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ReceivedRequest {
    pub(crate) bytes: Vec<u8>,
    pub(crate) object_id: String,
    pub(crate) revision: String,
}

impl ReceivedRequest {
    pub(crate) fn is(&self, object: &coven_storage::StoredObject) -> bool {
        self.object_id == object.object_id && self.revision == object.revision
    }
}

/// What approving the invite's request binds to its member id (§12.2).
#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) enum InviteBinding {
    /// Add member: a fresh id with this role and the invite's access.
    Add { role: MemberRole },
    /// Replace member keys: an existing active id keeps its role, circles,
    /// devices and recorded access; only its pair changes.
    Replace,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct InviteWork {
    pub(crate) id: InviteId,
    /// The member id the invite fixes and the request must repeat.
    pub(crate) member: MemberId,
    pub(crate) binding: InviteBinding,
    pub(crate) access: InviteGrant,
    pub(crate) secret: coven_crypto::SecretText,
    pub(crate) expires_at: SystemTime,
    pub(crate) state: InviteState,
}

impl InviteWork {
    /// The binding approval publishes for the checked request: its exact
    /// pair, on the id the invite fixed (§12.2).
    pub(crate) fn binding_change(
        &self,
        request: &coven_format::objects::JoinRequest,
    ) -> Result<StoreChange, SyncError> {
        Ok(match self.binding {
            InviteBinding::Add { role } => StoreChange::AddMember {
                member: self.member,
                keys: request.keys,
                role,
                access: self.access.member_access()?,
            },
            InviteBinding::Replace => StoreChange::ReplaceMemberKeys {
                member: self.member,
                keys: request.keys,
            },
        })
    }

    /// A kept replacement records the invite's new S3 key for its member in
    /// a separate access entry; an account invite shares only the member's
    /// recorded account, so it has nothing to record (§12.2, §13).
    pub(crate) fn records_access(&self) -> bool {
        matches!(self.binding, InviteBinding::Replace)
            && matches!(self.access, InviteGrant::S3Key(_))
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) enum InviteState {
    Grant,
    Waiting {
        invitation: StorageInvitation,
        request: Option<ReceivedRequest>,
    },
    Approving {
        request: Vec<u8>,
        entry: Option<Vec<u8>>,
    },
    /// After a kept replacement: the invite's new S3 key is recorded for the
    /// member by its own access entry, separate from the binding (§12.2).
    Recording {
        entry: Option<Vec<u8>>,
    },
    Revoking,
    Settled,
}

/// A replacement of one member's signing and sealing pair (§9, §18).
#[derive(Serialize, Deserialize)]
pub(crate) struct MemberKeysWork {
    /// The member whose pair is replaced.
    pub(crate) member: MemberId,
    /// This member's own replacement: it pauses app writes, keeps its
    /// candidate in identity custody, and installs it once the binding is
    /// kept. Otherwise an admin publishes a pair prepared elsewhere.
    pub(crate) own: bool,
    /// The pair the entry binds: the prepared one, or the candidate once
    /// identity custody keeps it.
    #[serde(with = "crate::pending_encoding::member_keys")]
    pub(crate) keys: Option<coven_crypto::MemberPublicKeys>,
    pub(crate) entry: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Data {
    PublishSchema {
        version: u32,
    },
    Snapshots(crate::snapshot_data::SnapshotTask),
    Entry(EntryWork),
    Invite(InviteWork),
    /// One account's intended provider access; the database owns its request state.
    ProviderAccess {
        account: String,
    },
    /// One S3 key's deletion obligation; its pending notice lives on this row.
    DeleteAccessKey(#[serde(with = "crate::pending_encoding::AccessKey")] crate::AccessKeyToDelete),
    ReplaceAccessKey(ReplaceKeyWork),
    ReplaceMemberKeys(MemberKeysWork),
}

impl Data {
    /// Stable journal name shared by app-requested and automatic reloads.
    pub(crate) const RELOAD_KIND: &str = "reload-snapshots";

    pub(crate) fn read(record: &OperationRecord) -> Result<Self, SyncError> {
        let data: Self = serde_json::from_slice(&record.data)?;
        if data.journal_kind() != record.kind {
            return Err(coven_database::DbError::DamagedDatabase.into());
        }
        Ok(data)
    }
    /// The purpose of every operation, including automatic maintenance.
    pub(crate) fn kind(&self) -> OperationKind {
        match self {
            Self::PublishSchema { .. } => OperationKind::SchemaChange,
            Self::Entry(work) => work.intent.kind(),
            Self::Snapshots(task) => match task.job {
                SnapshotJob::Write {
                    trigger: SnapshotTrigger::Raise { .. },
                    ..
                } => OperationKind::SchemaChange,
                SnapshotJob::Write {
                    trigger: SnapshotTrigger::Reset,
                    ..
                } => OperationKind::Reset,
                SnapshotJob::Write { .. } => OperationKind::WriteSnapshot,
                SnapshotJob::Reload { .. } => OperationKind::ReloadFromSnapshot,
            },
            Self::Invite(_) => OperationKind::Invite,
            Self::ProviderAccess { .. } => OperationKind::RevokeAccess,
            Self::DeleteAccessKey(_) => OperationKind::DeleteAccessKey,
            Self::ReplaceAccessKey(_) => OperationKind::ReplaceAccessKey,
            Self::ReplaceMemberKeys(_) => OperationKind::ReplaceMemberKeys,
        }
    }

    /// Stable names used only by the local journal, including maintenance.
    pub(crate) fn journal_kind(&self) -> &'static str {
        match self {
            Self::PublishSchema { .. } => "migrate-schema",
            Self::Entry(work) => match work.intent {
                Intent::Reset { .. } => "reset",
                Intent::RotateKey { .. } => "rotate-key",
                Intent::CreateCircle { .. } => "create-circle",
                Intent::AddCircleMember { .. } => "add-circle-member",
                Intent::DeleteCircle { .. } => "delete-circle",
            },
            Self::Snapshots(task) => match task.job {
                SnapshotJob::Write {
                    trigger: SnapshotTrigger::Raise { .. },
                    ..
                } => "raise-schema",
                SnapshotJob::Write {
                    trigger: SnapshotTrigger::Reset,
                    ..
                } => "reset",
                SnapshotJob::Write { .. } => "write-snapshot",
                SnapshotJob::Reload { .. } => Self::RELOAD_KIND,
            },
            Self::Invite(_) => "invite",
            Self::ProviderAccess { .. } => "provider-access",
            Self::DeleteAccessKey(_) => "delete-access-key",
            Self::ReplaceAccessKey(_) => "replace-access-key",
            Self::ReplaceMemberKeys(_) => "replace-member-keys",
        }
    }
    pub(crate) fn new_operation(&self, started_by: &str) -> Result<NewOperation, SyncError> {
        Ok(NewOperation {
            kind: self.journal_kind().into(),
            data: serde_json::to_vec(self)?,
            started_by: started_by.into(),
            // Self-replacement refuses app writes until its binding and
            // custody are installed (§10, §18).
            pauses_writes: matches!(self, Self::ReplaceMemberKeys(work) if work.own),
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
    /// Reset and schema publication prepare snapshots before the shared entry steps.
    pub(crate) fn entry_step_number(&self, step: u32) -> u32 {
        if self.raises_version() {
            return step + 2;
        }
        step + match self {
            Self::Entry(EntryWork {
                intent: Intent::Reset { .. },
                ..
            }) => 3,
            // Minting and delivery precede the entry.
            Self::ReplaceAccessKey(_) => 2,
            // Confirming the queue and keeping the candidate precede it.
            Self::ReplaceMemberKeys(work) if work.own => 1,
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
                state: InviteState::Approving { entry, .. } | InviteState::Recording { entry },
                ..
            }) => entry.as_ref(),
            Self::ReplaceAccessKey(work) => work.entry.as_ref(),
            Self::ReplaceMemberKeys(work) => work.entry.as_ref(),
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
                state: InviteState::Approving { entry, .. } | InviteState::Recording { entry },
                ..
            }) => *entry = value,
            Self::ReplaceAccessKey(work) => work.entry = value,
            Self::ReplaceMemberKeys(work) => work.entry = value,
            _ => {
                return Err(coven_database::DbError::Invariant {
                    what: "only entry-publishing work fixes an entry",
                }
                .into())
            }
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
                    | Self::ReplaceAccessKey(_)
                    | Self::ReplaceMemberKeys(_)
                    | Self::Invite(InviteWork {
                        state: InviteState::Approving { .. } | InviteState::Recording { .. },
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

    pub(crate) fn is_reload(&self) -> bool {
        matches!(
            self,
            Self::Snapshots(SnapshotTask {
                job: SnapshotJob::Reload { .. },
                ..
            })
        )
    }
}
