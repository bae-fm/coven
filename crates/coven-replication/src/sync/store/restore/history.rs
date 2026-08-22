use std::collections::BTreeMap;

use coven_protocol::store_commit::{
    OwnerRecoveryNode, OwnerRecoveryNodeRef, StoreAck, StoreAckRef, StoreDeviceRegistration,
    StoreDeviceRegistrationRef,
};

use crate::sync::store::commit_verification::merge_history::registration::RegistrationLoadError;
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;

pub(crate) struct RestoreHistory<'operation, 'storage> {
    history: &'operation MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> RestoreHistory<'operation, 'storage> {
    pub(crate) fn new(history: &'operation MergeHistoryVerifier<'storage>) -> Self {
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
    ) -> Result<StoreAck, coven_protocol::objects::StoreObjectError> {
        self.history.load_store_ack(reference, registration).await
    }

    pub(crate) async fn load_acknowledgement_proof_chain(
        &self,
        latest_ref: StoreAckRef,
        latest: StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<BTreeMap<u64, (StoreAckRef, StoreAck)>, RegistrationLoadError> {
        self.history
            .load_acknowledgement_proof_chain(latest_ref, latest, registration)
            .await
    }

    /// Every snapshot `registration` has published, in generation order, as
    /// the provider holds them now.
    pub(crate) async fn load_store_snapshot_stream(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<
        Vec<coven_database::PublishedStoreSnapshot>,
        crate::sync::store::snapshots::SnapshotError,
    > {
        self.history
            .load_store_snapshot_stream(registration_ref, registration)
            .await
    }
}
