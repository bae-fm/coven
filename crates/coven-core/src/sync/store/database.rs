mod acknowledgements;
pub(crate) mod candidate_lifecycle;
pub(crate) mod candidate_records;
mod circle_controls;
mod circle_operations;
mod device_continuation;
mod device_exclusion;
mod host_write_capture;
mod materialization;
pub(crate) mod materialization_models;
mod materialized_commit_index;
mod membership_mutations;
mod owner_promotion;
mod pending_publication;
mod preparation;
mod prepared_remote_objects;
mod publication;
pub(crate) mod publication_state;
mod reclaim;
mod retained_merge_replay;
mod store_device_state;
mod write_lifecycle;

use crate::database::{
    begin_remote_candidate_nonactivation_on, finish_outbound_store_ack_on,
    load_activated_registration_on, load_outbound_store_ack_on, load_protocol_inert_object_on,
    load_remote_object_on, persist_exact_remote_object_on, replace_prepared_merge_head_remote_on,
    required_store_root_authority_on, CandidateCleanupObject, Database, DbError,
    OutboundStoreAckActivation,
};
use crate::sync::remote_object::{
    remote_object_id, CandidateNonactivationProof, VerifiedCandidateNonactivation,
};
use crate::sync::storage::PreparedExactObject;
use crate::sync::store::operations::PreparedStoreOperationCommit;
use crate::sync::store_commit::{StoreAckRef, StoreDeviceHead, StoreDeviceHeadRef};

#[derive(Clone)]
pub struct StoreDatabase {
    database: Database,
}

pub(in crate::sync::store) struct StoreDatabaseTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
}

impl<'transaction, 'connection> StoreDatabaseTransaction<'transaction, 'connection> {
    pub(in crate::sync::store) fn new(
        transaction: &'transaction rusqlite::Transaction<'connection>,
    ) -> Self {
        Self { transaction }
    }
}

impl StoreDatabase {
    #[doc(hidden)]
    pub fn from_database(database: Database) -> Self {
        Self { database }
    }

    pub(crate) fn new(database: &Database) -> Self {
        Self::from_database(database.clone())
    }

    #[doc(hidden)]
    pub fn sqlite(&self) -> &Database {
        &self.database
    }

    #[cfg(test)]
    pub(crate) async fn required_store_root_hash(
        &self,
    ) -> Result<crate::sync::store_commit::ObjectHash, DbError> {
        self.database
            .call(|connection| Ok(required_store_root_authority_on(connection)?.store_root_hash))
            .await
    }
}

#[cfg(test)]
pub(in crate::sync) fn record_verified_circle_activations_for_test(
    connection: &rusqlite::Connection,
    commit: &crate::sync::store_commit::StoreBatchCommit,
    commit_ref: &crate::sync::store_commit::StoreBatchCommitRef,
    activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
) -> Result<(), DbError> {
    let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
    StoreDatabaseTransaction::new(&transaction).record_verified_circle_activations(
        commit,
        commit_ref,
        activations,
    )?;
    transaction.commit().map_err(DbError::from)
}

#[cfg(test)]
pub(in crate::sync) async fn store_package_is_retained_for_replay_for_test(
    database: &Database,
    package: crate::sync::store_commit::StorePackageRef,
    activation: crate::sync::store_commit::StoreBatchCommitRef,
) -> Result<bool, DbError> {
    StoreDatabase::new(database)
        .store_package_is_retained_for_replay(package, activation)
        .await
}
