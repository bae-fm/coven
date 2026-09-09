use super::*;

pub(crate) struct VerifiedSnapshotState {
    pub(crate) device_state: ResolvedStoreDeviceState,
    pub(crate) active_registrations: BTreeMap<
        super::store_commit::StoreDeviceId,
        super::store_commit::ReferencedStoreDeviceRegistration,
    >,
}
