use coven_protocol::store_commit::{
    StoreAck, StoreAckRef, StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProposalRef,
    StoreDeviceRegistration,
};

pub(crate) struct DeviceExclusionHistory<'operation, 'storage> {
    history: &'operation mut super::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> DeviceExclusionHistory<'operation, 'storage> {
    pub(crate) fn new(history: &'operation mut super::MergeHistoryVerifier<'storage>) -> Self {
        Self { history }
    }

    pub(super) async fn load_proposal(
        &mut self,
        reference: &StoreDeviceExclusionProposalRef,
    ) -> Result<
        coven_protocol::store_commit::VerifiedDeviceExclusionProposal,
        super::StoreDeviceExclusionError,
    > {
        self.history
            .load_device_exclusion_proposal(reference)
            .await
            .map_err(super::StoreDeviceExclusionError::from)
    }

    pub(super) async fn load_outcome(
        &mut self,
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: &coven_protocol::store_commit::VerifiedDeviceExclusionProposal,
    ) -> Result<
        coven_protocol::store_commit::VerifiedDeviceExclusionOutcome,
        super::StoreDeviceExclusionError,
    > {
        self.history
            .load_device_exclusion_outcome(reference, proposal)
            .await
            .map_err(super::StoreDeviceExclusionError::from)
    }

    pub(super) async fn load_acknowledgement(
        &mut self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<coven_protocol::objects::VerifiedObject<StoreAck>, super::StoreDeviceExclusionError>
    {
        self.history
            .load_store_ack(reference, registration)
            .await
            .map_err(super::StoreDeviceExclusionError::from)
    }
}
