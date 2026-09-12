use coven_protocol::store_commit::{
    StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProposal, StoreDeviceRegistration,
};

pub(crate) struct DeviceExclusionHistory<'operation, 'storage> {
    history: &'operation mut super::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> DeviceExclusionHistory<'operation, 'storage> {
    pub(crate) fn new(history: &'operation mut super::MergeHistoryVerifier<'storage>) -> Self {
        Self { history }
    }

    pub(super) async fn load_outcome(
        &mut self,
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: &StoreDeviceExclusionProposal,
        target: &StoreDeviceRegistration,
    ) -> Result<
        coven_protocol::store_commit::VerifiedDeviceExclusionOutcome,
        super::StoreDeviceExclusionError,
    > {
        self.history
            .load_device_exclusion_outcome(reference, proposal, target)
            .await
            .map_err(super::StoreDeviceExclusionError::from)
    }
}
