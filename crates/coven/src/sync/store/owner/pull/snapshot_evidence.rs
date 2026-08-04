use super::*;

pub(crate) struct VerifiedActivatedStoreAck {
    pub(crate) reference: super::store_commit::StoreAckRef,
    pub(crate) value: super::store_commit::StoreAck,
    pub(crate) chain: BTreeMap<
        u64,
        (
            super::store_commit::StoreAckRef,
            super::store_commit::StoreAck,
        ),
    >,
    pub(crate) activating_commit: StoreBatchCommitRef,
    pub(crate) activating_commit_value: StoreBatchCommit,
}

pub(crate) struct VerifiedSnapshotState {
    pub(crate) device_state: ResolvedStoreDeviceState,
    pub(crate) active_registrations: BTreeMap<
        super::store_commit::StoreDeviceId,
        super::store_commit::ReferencedStoreDeviceRegistration,
    >,
}
