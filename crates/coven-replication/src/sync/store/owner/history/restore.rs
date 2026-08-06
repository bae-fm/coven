use std::collections::BTreeMap;

use coven_protocol::store_commit::{
    OwnerRecoveryNode, OwnerRecoveryNodeRef, SnapshotMeta, StoreAck, StoreAckRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreSnapshotRef,
};

pub(crate) struct RestoreHistory<'operation, 'storage> {
    history: &'operation super::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> RestoreHistory<'operation, 'storage> {
    pub(crate) fn new(history: &'operation super::MergeHistoryVerifier<'storage>) -> Self {
        Self { history }
    }

    pub(crate) async fn load_owner_recovery_node(
        &self,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<OwnerRecoveryNode>,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history.load_owner_recovery_node(reference).await
    }

    pub(crate) async fn load_store_ack(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<StoreAck>,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history.load_store_ack(reference, registration).await
    }

    pub(crate) async fn load_acknowledgement_proof_chain(
        &self,
        latest_ref: StoreAckRef,
        latest: StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<BTreeMap<u64, (StoreAckRef, StoreAck)>, super::RegistrationLoadError> {
        self.history
            .load_acknowledgement_proof_chain(latest_ref, latest, registration)
            .await
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &StoreSnapshotRef,
    ) -> Result<(StoreSnapshotRef, SnapshotMeta), coven_protocol::objects::StoreObjectError> {
        self.history
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }
}
