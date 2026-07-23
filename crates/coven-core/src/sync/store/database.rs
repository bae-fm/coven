mod acknowledgements;
mod candidate_lifecycle;
pub(super) mod candidate_records;
mod circle_controls;
mod circle_operations;
mod device_continuation;
mod device_exclusion;
mod device_join_challenges;
mod device_registration_journal;
mod host_write_capture;
mod materialization;
pub(super) mod materialization_models;
mod materialized_commit_index;
mod membership_mutations;
mod owner_promotion;
mod pending_publication;
mod preparation;
mod prepared_remote_objects;
mod publication;
pub(super) mod publication_state;
mod reclaim;
mod retained_merge_replay;
mod snapshot_publication;
mod store_acknowledgements;
mod store_authority;
mod store_creation_attempts;
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

#[derive(Clone, Default)]
pub(crate) struct StoreDatabaseRuntime {
    /// Serializes complete membership-chain loads that share this database, so a
    /// load cannot return an older chain after another load commits a newer floor.
    membership_load: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Serializes construction and execution of the one local membership mutation
    /// whose exact signed bytes are held in `outbound_membership_mutation`.
    membership_mutation: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Serializes publication and rollback of the one durable founder graph.
    store_creation: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Serializes the exact local device-exclusion object and its Store-stream
    /// activation candidate across every database-handle clone.
    device_exclusion: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// Serializes staging and publication of the one exact snapshot generation
    /// held in `outbound_store_snapshot`.
    snapshot_publication: std::sync::Arc<tokio::sync::Mutex<()>>,
}

#[derive(Clone)]
pub struct StoreDatabase {
    database: Database,
    runtime: StoreDatabaseRuntime,
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
        let runtime = database.store_runtime();
        Self { database, runtime }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn new(database: &Database) -> Self {
        Self::from_database(database.clone())
    }

    #[doc(hidden)]
    pub fn sqlite(&self) -> &Database {
        &self.database
    }

    pub(in crate::sync::store) async fn lock_membership_load(
        &self,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.membership_load.clone().lock_owned().await
    }

    pub(in crate::sync::store) async fn lock_membership_mutation(
        &self,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.membership_mutation.clone().lock_owned().await
    }

    pub(in crate::sync::store) async fn lock_store_creation(
        &self,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.store_creation.clone().lock_owned().await
    }

    pub(in crate::sync::store) async fn lock_device_exclusion(
        &self,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.device_exclusion.clone().lock_owned().await
    }

    pub(in crate::sync::store) async fn lock_snapshot_publication(
        &self,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.snapshot_publication.clone().lock_owned().await
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
