use super::*;

#[derive(Debug, Clone)]
pub struct DurableDeviceRegistration {
    pub device_id: coven_protocol::store_commit::StoreDeviceId,
    pub registration_hash: ObjectHash,
    pub registration_bytes: Vec<u8>,
    pub prepared: PreparedExactObject,
    pub initial_ack_ref: StoreAckRef,
    pub initial_ack: ExactProtocolObject<StoreAck>,
    pub state: LocalDeviceRegistrationState,
}

/// The exact Owner-recovery commit and announcement staged before either is
/// published. A retry reopens these same stored bytes instead of resealing the
/// semantic objects into different exact identities.
pub struct OwnerRecoveryPublication {
    pub commit: ExactProtocolObject<coven_protocol::store_commit::VerifiedStoreBatchCommit>,
    pub head: ExactProtocolObject<coven_protocol::store_commit::StoreDeviceHead>,
    pub history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LocalDeviceRegistrationState {
    Prepared,
    Created,
    Activated {
        authority: coven_protocol::store_commit::StoreDeviceRegistrationActivation,
    },
}

pub type PreparedLocalDeviceRegistrationRow =
    (String, String, Vec<u8>, String, String, Vec<u8>, String);
pub type LocalDeviceRegistrationJournalRow = (
    String,
    String,
    Vec<u8>,
    String,
    String,
    Vec<u8>,
    String,
    String,
);

impl DurableDeviceRegistration {
    pub fn is_activated(&self) -> bool {
        matches!(self.state, LocalDeviceRegistrationState::Activated { .. })
    }
}

#[derive(Debug, Clone)]
pub struct DurableMembershipMutation {
    pub intent_hash: ObjectHash,
    pub plan_bytes: Vec<u8>,
    pub progress_bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
pub enum MembershipMutationActivation {
    WithoutRotation,
    Rotation { generation: u64 },
}

pub struct DurableSnapshotPublication {
    pub reference: StoreSnapshotRef,
    pub meta: ExactProtocolObject<SnapshotMeta>,
    pub image: PreparedProtocolObject<Vec<u8>>,
    pub blobs: Vec<PreparedSnapshotBlob>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedSnapshotBlob {
    pub bindings: Vec<RowBlobLocatorBinding>,
    pub authority: coven_protocol::audience_package::PackageAudience,
    pub remote: RemoteObjectRecord,
    pub spool_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct PublishedStoreSnapshot {
    pub reference: StoreSnapshotRef,
    pub successor_slot: coven_protocol::objects::ObjectSlot,
    pub meta: SnapshotMeta,
}

pub struct DurableCircleSnapshotPublication {
    pub reference: coven_protocol::store_commit::CircleSnapshotRef,
    pub meta: ExactProtocolObject<coven_protocol::store_commit::CircleSnapshotMeta>,
    pub image: PreparedProtocolObject<Vec<u8>>,
    pub blobs: Vec<PreparedSnapshotBlob>,
}

#[derive(Debug, Clone)]
pub struct PublishedCircleSnapshot {
    pub reference: coven_protocol::store_commit::CircleSnapshotRef,
    pub successor_slot: coven_protocol::objects::ObjectSlot,
    pub cut: coven_protocol::store_commit::CommitFrontier,
}
