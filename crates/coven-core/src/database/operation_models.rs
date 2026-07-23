use super::*;

#[derive(Debug, Clone)]
pub(crate) struct DurableDeviceRegistration {
    pub device_id: crate::sync::store_commit::StoreDeviceId,
    pub registration_hash: ObjectHash,
    pub registration_bytes: Vec<u8>,
    pub prepared: PreparedExactObject,
    pub initial_ack_ref: StoreAckRef,
    pub initial_ack: ExactProtocolObject<StoreAck>,
    pub state: LocalDeviceRegistrationState,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum LocalDeviceRegistrationState {
    Prepared,
    Created,
    Activated {
        authority: crate::sync::store_commit::StoreDeviceRegistrationActivation,
    },
}

pub(super) type PreparedLocalDeviceRegistrationRow =
    (String, String, Vec<u8>, String, String, Vec<u8>, String);
pub(crate) type LocalDeviceRegistrationJournalRow = (
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
    pub(crate) fn is_activated(&self) -> bool {
        matches!(self.state, LocalDeviceRegistrationState::Activated { .. })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DurableMembershipMutation {
    pub intent_hash: ObjectHash,
    pub plan_bytes: Vec<u8>,
    pub progress_bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) enum MembershipMutationActivation {
    WithoutRotation,
    Rotation { generation: u64 },
}

#[derive(Debug, Clone)]
pub(crate) struct DurableSnapshotPublication {
    pub reference: StoreSnapshotRef,
    pub meta: ExactProtocolObject<SnapshotMeta>,
    pub image: ExactProtocolObject<Vec<u8>>,
    pub blobs: Vec<PreparedSnapshotBlob>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedSnapshotBlob {
    pub bindings: Vec<RowBlobLocatorBinding>,
    pub authority: crate::sync::audience_package::PackageAudience,
    pub remote: RemoteObjectRecord,
    pub spool_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub(crate) struct PublishedStoreSnapshot {
    pub reference: StoreSnapshotRef,
    pub successor_slot: crate::storage::cloud::ObjectSlot,
    pub meta: SnapshotMeta,
}
