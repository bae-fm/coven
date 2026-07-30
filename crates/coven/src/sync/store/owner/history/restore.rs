use std::collections::BTreeMap;

use crate::protocol::store_commit::{
    OwnerRecoveryNode, OwnerRecoveryNodeRef, SnapshotMeta, StoreAck, StoreAckRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreSnapshotRef,
};

pub(crate) struct RestoreHistory<'operation, 'storage> {
    history: &'operation super::super::verified_history::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> RestoreHistory<'operation, 'storage> {
    pub(crate) fn new(
        history: &'operation super::super::verified_history::MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self { history }
    }

    pub(crate) async fn load_owner_recovery_node(
        &self,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<crate::storage::VerifiedObject<OwnerRecoveryNode>, crate::storage::StoreObjectError>
    {
        self.history.load_owner_recovery_node(reference).await
    }

    pub(crate) async fn load_store_ack(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<crate::storage::VerifiedObject<StoreAck>, crate::storage::StoreObjectError> {
        self.history.load_store_ack(reference, registration).await
    }

    pub(crate) async fn load_acknowledgement_proof_chain(
        &self,
        latest_ref: StoreAckRef,
        latest: StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<
        BTreeMap<u64, (StoreAckRef, StoreAck)>,
        super::super::verified_history::registration::RegistrationLoadError,
    > {
        self.history
            .load_acknowledgement_proof_chain(latest_ref, latest, registration)
            .await
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &StoreSnapshotRef,
    ) -> Result<(StoreSnapshotRef, SnapshotMeta), crate::storage::StoreObjectError> {
        self.history
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }
}
