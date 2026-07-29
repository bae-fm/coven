mod acknowledgements;
mod candidate_lifecycle;
pub(super) mod candidate_records;
mod circle_acknowledgements;
mod circle_controls;
mod circle_operation_discard;
mod circle_operations;
mod circle_snapshot_publication;
mod device_continuation;
mod device_exclusion;
mod device_join;
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
pub(super) mod reclaim;
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
use materialization_models::OwnedVerifiedMergeMaterialization;

pub(crate) use retained_merge_replay::RetainedMergeMaterializationCache;

#[derive(Clone)]
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
    /// Retained Merge inputs verified when this database opened, extended only
    /// by fully opening newly retained inputs on the owned connection.
    retained_merge_materializations:
        std::sync::Arc<std::sync::Mutex<RetainedMergeMaterializationCache>>,
    /// Serializes this device's authorship of its own Store stream: reading the
    /// position a commit extends, and publishing the head that takes it.
    ///
    /// The device owns that stream, so two of its own writers contending for one
    /// position is an implementation accident with no meaning in the protocol —
    /// not a conflict any peer could observe. Held across the pair, it cannot
    /// happen.
    own_stream_authorship: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl StoreDatabaseRuntime {
    pub(crate) fn new() -> Self {
        Self {
            membership_load: Default::default(),
            membership_mutation: Default::default(),
            store_creation: Default::default(),
            device_exclusion: Default::default(),
            snapshot_publication: Default::default(),
            retained_merge_materializations: std::sync::Arc::new(std::sync::Mutex::new(
                RetainedMergeMaterializationCache::default(),
            )),
            own_stream_authorship: Default::default(),
        }
    }
}

/// This device's exclusive turn to author its own next Store commit, held from
/// reading the position through publishing the head that takes it.
pub(super) type OwnStreamAuthorship = tokio::sync::OwnedMutexGuard<()>;

#[derive(Clone)]
pub struct StoreDatabase {
    database: Database,
    runtime: StoreDatabaseRuntime,
}

pub(super) struct StoreDatabaseTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
}

impl<'transaction, 'connection> StoreDatabaseTransaction<'transaction, 'connection> {
    pub(super) fn new(transaction: &'transaction rusqlite::Transaction<'connection>) -> Self {
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

    pub(crate) fn device_join_journal(&self) -> device_join::StoreJoinJournal<'_> {
        device_join::StoreJoinJournal::new(&self.database)
    }

    pub(super) async fn lock_membership_load(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.membership_load.clone().lock_owned().await
    }

    pub(super) async fn lock_membership_mutation(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.membership_mutation.clone().lock_owned().await
    }

    pub(super) async fn lock_store_creation(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.store_creation.clone().lock_owned().await
    }

    pub(super) async fn lock_device_exclusion(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.device_exclusion.clone().lock_owned().await
    }

    /// Wait for this device's turn to author its own next Store commit.
    ///
    /// Every path that reads the local position to compose a commit, and every
    /// path that publishes a device head, takes this and holds it across the
    /// pair. Never taken twice in one call chain: a composer holds it until its
    /// candidate is either activated or durably persisted, and a publisher of an
    /// already-persisted candidate takes it for that publication alone.
    pub(super) async fn author_own_stream(&self) -> OwnStreamAuthorship {
        self.runtime
            .own_stream_authorship
            .clone()
            .lock_owned()
            .await
    }

    pub(super) async fn lock_snapshot_publication(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.runtime.snapshot_publication.clone().lock_owned().await
    }

    pub(super) fn retained_merge_materialization_cache(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<RetainedMergeMaterializationCache>> {
        self.runtime.retained_merge_materializations.clone()
    }

    pub(super) async fn begin_store_creation_attempt(
        &self,
        initialized: crate::sync::store::protocol_root::StoreCreationAttempt,
    ) -> Result<crate::sync::store::protocol_root::StoreCreationAttempt, DbError> {
        let value = serde_json::to_string(&initialized).map_err(|error| {
            DbError::Message(format!("serialize Store creation attempt: {error}"))
        })?;
        self.sqlite()
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO NOTHING",
                    (
                        crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                        &value,
                    ),
                )
                .map_err(DbError::from)?;
                let actual = crate::database::required_protocol_state_on(
                    &tx,
                    crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                )?;
                tx.commit().map_err(DbError::from)?;
                serde_json::from_str(&actual).map_err(|error| {
                    DbError::Message(format!("parse Store creation attempt: {error}"))
                })
            })
            .await
    }

    pub(super) async fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<crate::sync::store::protocol_root::StoreCreationAttempt>, DbError> {
        self.sqlite()
            .call(move |conn| {
                crate::database::get_protocol_state_on(
                    conn,
                    crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                )?
                .map(|value| {
                    serde_json::from_str(&value).map_err(|error| {
                        DbError::Message(format!("parse Store creation attempt: {error}"))
                    })
                })
                .transpose()
            })
            .await
    }

    pub(super) async fn advance_store_creation_attempt(
        &self,
        previous: crate::sync::store::protocol_root::StoreCreationAttempt,
        next: crate::sync::store::protocol_root::StoreCreationAttempt,
    ) -> Result<(), DbError> {
        let previous = serde_json::to_string(&previous).map_err(|error| {
            DbError::Message(format!("serialize Store creation predecessor: {error}"))
        })?;
        let next = serde_json::to_string(&next).map_err(|error| {
            DbError::Message(format!("serialize Store creation successor: {error}"))
        })?;
        self.sqlite()
            .call(move |conn| {
                let changed = conn
                    .execute(
                        "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                        (
                            &next,
                            crate::sync::store::protocol_root::STORE_CREATION_ATTEMPT_STATE_KEY,
                            &previous,
                        ),
                    )
                    .map_err(DbError::from)?;
                if changed != 1 {
                    return Err(DbError::Message(
                        "Store creation attempt advance lost its exact predecessor".to_string(),
                    ));
                }
                Ok(())
            })
            .await
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
pub(crate) fn record_verified_circle_activations_for_test(
    connection: &rusqlite::Connection,
    commit: &crate::sync::store_commit::VerifiedStoreBatchCommit,
    activations: &[crate::sync::store::circle_controls::VerifiedCircleReference],
) -> Result<(), DbError> {
    let transaction = connection.unchecked_transaction().map_err(DbError::from)?;
    StoreDatabaseTransaction::new(&transaction)
        .record_verified_circle_activations(commit, activations)?;
    transaction.commit().map_err(DbError::from)
}

#[cfg(test)]
pub(crate) async fn store_package_is_retained_for_replay_for_test(
    database: &Database,
    package: crate::sync::store_commit::StorePackageRef,
    activation: crate::sync::store_commit::StoreBatchCommitRef,
) -> Result<bool, DbError> {
    let database = StoreDatabase::new(database);
    let root = database
        .local_store_root_ref()
        .await?
        .ok_or_else(|| DbError::Message("test Store root is not installed".to_string()))?;
    database
        .store_package_is_retained_for_replay(root, package, activation)
        .await
}
