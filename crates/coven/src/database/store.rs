mod acknowledgements;
mod blob_bindings;
mod blob_outbox;
mod blob_transitions;
mod candidate_lifecycle;
pub(super) mod candidate_records;
mod circle_acknowledgements;
mod circle_controls;
mod circle_operation_discard;
mod circle_operations;
use circle_operations::{circle_blob_opening_protection_on, circle_publication_context_on};
mod circle_snapshot_publication;
mod device_continuation;
mod device_exclusion;
mod device_join;
mod device_join_challenges;
mod device_registration_journal;
mod host_sql;
mod host_sql_transaction;
mod host_write_capture;
mod host_write_operation;
mod local_blob_cleanup;
pub(crate) mod local_blob_cleanup_intents;
mod materialization;
pub(super) mod materialization_models;
mod materialized_commit_index;
use materialized_commit_index::record_activated_store_device_registrations_on;
mod membership_mutations;
mod merge_materialization_transaction;
mod owner_promotion;
mod pending_publication;
mod preparation;
mod prepared_remote_objects;
mod provider_probe;
mod publication;
pub(super) mod publication_state;
mod pull_replay;
pub(super) mod reclaim;
mod retained_merge_replay;
mod retained_replay;
mod snapshot_image;
mod snapshot_publication;
mod store_acknowledgements;
mod store_authority;
mod store_device_state;
mod stream_activation_records;
#[cfg(test)]
mod test_support;
mod write_lifecycle;

use crate::database::{
    begin_remote_candidate_nonactivation_on, finish_outbound_store_ack_on,
    load_activated_registration_on, load_outbound_store_ack_on, load_protocol_inert_object_on,
    load_remote_object_on, persist_exact_remote_object_on, replace_prepared_merge_head_remote_on,
    required_store_root_authority_on, Database, DbError, OutboundStoreAckActivation,
};
use crate::protocol::objects::PreparedExactObject;
use crate::protocol::prepared_commit::PreparedStoreOperationCommit;
use crate::protocol::remote_object::{
    remote_object_id, CandidateNonactivationProof, VerifiedCandidateNonactivation,
};
#[cfg(test)]
use crate::protocol::store_commit::StoreBatchCommitRef;
use crate::protocol::store_commit::{StoreAckRef, StoreDeviceHead, StoreDeviceHeadRef};

const CACHE_BUDGET_STATE_KEY_PREFIX: &str = "cache_budget:";

fn cache_budget_state_key(namespace: &str) -> String {
    format!("{CACHE_BUDGET_STATE_KEY_PREFIX}{namespace}")
}

pub use blob_outbox::{MakeRemoteProgress, QueuedDelete, QueuedUpload};
pub(crate) use blob_outbox::{OutboxEntry, OutboxOperation, OutboxUploadState};
pub(crate) use blob_transitions::MaterializedLocalBlob;
pub(crate) use blob_transitions::PostUpload;
#[cfg(test)]
pub(crate) use candidate_records::select_author_exclusion_activation_locator;
pub(crate) use candidate_records::CandidateCleanupObject;
pub(crate) use device_join::DeviceJoinJournalStore;
pub use host_sql::{SqlContext, SqlReadContext};
pub(crate) use host_write_capture::{
    audience_moves_by_row, AudienceBlobMoveStaging, HostWriteBlobTransaction,
    StagedAudienceBlobRollback,
};
pub(crate) use host_write_operation::StoreRowWrites;
pub use host_write_operation::WriteBatch;
pub(crate) use host_write_operation::{HostWriteError, HostWriteOperation};
pub(crate) use local_blob_cleanup::LocalBlobCleanup;
pub(crate) use materialization_models::{
    activated_merge_membership_remote_objects, DeviceJoinBootstrapActivation,
    DeviceJoinBootstrapCommit, DeviceJoinBootstrapPlan, MembershipAuthorityBytes,
    OwnedVerifiedMergeMaterialization, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage, RetainedAudiencePackage, RetainedMergeMaterializationKey,
    RetainedPackageApplication, VerifiedMergeMaterialization, VerifiedMergeMembershipObjects,
    VerifiedStoreSnapshotStability,
};
#[cfg(test)]
pub(crate) use merge_materialization_transaction::{resolve_and_apply_changeset, ApplyResult};
pub(crate) use merge_materialization_transaction::{
    IncomingTimestampPolicy, MergeMaterializationTransaction, TableSchema, ValidatedChangeset,
    WinningRow,
};
pub(crate) use publication_state::{MergeCandidateAbandonmentPreparation, StoreWritePreparation};
pub(crate) use pull_replay::{
    install_circle_bootstrap_image_on, install_circle_bootstrap_remote_objects_on,
};
pub(crate) use reclaim::journal::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, ReclaimCommitActivation,
    ReclaimedStorePackage, StoreReclaimCandidateLoss, StoreReclaimJournalError,
};
pub(crate) use retained_merge_replay::RetainedMergeMaterializationCache;
pub(crate) use retained_replay::{
    copy_table_with_conflicts, projection_table_names, RetainedReplayAuthority,
    RetainedReplayBaseline, RetainedReplayGenesisAuthority, RetainedReplaySnapshotAuthority,
    GENERATION_ZERO,
};
pub(crate) use snapshot_image::{
    verify_circle_bootstrap_image, CreatedSnapshot, SnapshotBlobAudience, SnapshotDatabaseImage,
    SnapshotImageError, SnapshotImageOperationError,
};
use store_device_state::{
    apply_store_device_exclusion_freezes_on, load_declared_store_device_state_on,
};
#[cfg(test)]
pub(crate) use test_support::AuthorExclusionLocatorTamper;
pub(crate) use write_lifecycle::BlockedWriteDiscard;

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
    /// Serializes the full durable-intent to filesystem-deletion to
    /// intent-removal operation across every clone of this database.
    local_blob_cleanup: std::sync::Arc<tokio::sync::Mutex<()>>,
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
            local_blob_cleanup: Default::default(),
        }
    }
}

pub(crate) struct MembershipLoadPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(crate) struct MembershipMutationPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(crate) struct StoreCreationPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(crate) struct DeviceExclusionPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// This device's exclusive turn to author its own next Store commit, held from
/// reading the position through publishing the head that takes it.
pub(crate) struct OwnStreamAuthorship {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(crate) struct SnapshotPublicationPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

#[derive(Clone)]
struct StoreDatabaseConnection(crate::database::DatabaseConnection);

impl StoreDatabaseConnection {
    fn new(connection: crate::database::DatabaseConnection) -> Self {
        Self(connection)
    }

    async fn call<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<R, DbError> + Send + 'static,
        R: Send + 'static,
    {
        self.0.call(operation).await
    }
}

#[derive(Clone)]
pub(crate) struct StoreDatabase {
    connection: StoreDatabaseConnection,
    runtime: StoreDatabaseRuntime,
    hlc: std::sync::Arc<crate::protocol::hlc::Hlc>,
    synced_tables: std::sync::Arc<Vec<crate::SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: crate::protocol::store_commit::ObjectHash,
    gates: std::sync::Arc<crate::database::Gates>,
    blob_decls: std::sync::Arc<crate::database::BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
    transfer_limits: crate::protocol::blob::TransferLimits,
    ids: crate::id_provider::IdRef,
    write_statuses: std::sync::Arc<
        std::sync::Mutex<
            std::collections::HashMap<
                crate::WriteId,
                tokio::sync::watch::Sender<crate::WriteStatus>,
            >,
        >,
    >,
    #[cfg(test)]
    test_access: crate::database::StoreDatabaseTestAccess,
}

impl StoreDatabase {
    #[doc(hidden)]
    pub(crate) fn from_database(database: Database) -> Self {
        let Database { connection, state } = database;
        Self {
            connection: StoreDatabaseConnection::new(connection),
            runtime: state.store_runtime,
            hlc: state.hlc,
            synced_tables: state.synced_tables,
            schema_version: state.schema_version,
            sync_routing_hash: state.sync_routing_hash,
            gates: state.gates,
            blob_decls: state.blob_decls,
            blob_tombstone_grace: state.blob_tombstone_grace,
            transfer_limits: state.transfer_limits,
            ids: state.ids,
            write_statuses: state.write_statuses,
            #[cfg(test)]
            test_access: crate::database::StoreDatabaseTestAccess {
                pause_points: state.test_pause_points,
                merge_materialization_failure: state.merge_materialization_failure,
            },
        }
    }

    pub(crate) async fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.connection
            .call(move |connection| {
                let authorization = host_sql_transaction::HostSqlAuthorization::begin(connection)?;
                Ok(authorization.run(|| read(SqlReadContext::new(connection))))
            })
            .await
    }

    pub(crate) fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub(crate) fn sync_routing_hash(&self) -> crate::protocol::store_commit::ObjectHash {
        self.sync_routing_hash
    }

    pub(crate) fn synced_tables(&self) -> &[crate::SyncedTable] {
        &self.synced_tables
    }

    pub(crate) fn transfer_limits(&self) -> crate::protocol::blob::TransferLimits {
        self.transfer_limits
    }

    pub(crate) fn blob_tombstone_grace(&self) -> chrono::Duration {
        self.blob_tombstone_grace
    }

    fn gates(&self) -> std::sync::Arc<crate::database::Gates> {
        self.gates.clone()
    }

    pub(crate) fn has_scoped_graph(&self) -> bool {
        self.gates.has_scoped_graph()
    }

    fn blob_decls(&self) -> std::sync::Arc<crate::database::BlobDecls> {
        self.blob_decls.clone()
    }

    fn hlc(&self) -> std::sync::Arc<crate::protocol::hlc::Hlc> {
        self.hlc.clone()
    }

    pub(crate) fn stamp(&self) -> String {
        self.hlc.now().to_string()
    }

    pub(crate) async fn persist_hlc_high_water(&self) -> Result<(), DbError> {
        self.set_protocol_state(
            crate::protocol::hlc::HIGHWATER_STATE_KEY,
            &self.hlc.high_water().to_string(),
        )
        .await
    }

    pub(crate) fn blob_ref_from_change(
        &self,
        change: &crate::changeset::RowChange,
    ) -> Result<Option<crate::protocol::blob::BlobRef>, crate::database::BlobDeclError> {
        self.blob_decls.ref_from_change(change)
    }

    pub(crate) fn validate_local_blob_cleanup_changes(
        &self,
        old_changes: &[crate::changeset::RowChange],
        new_changes: &[crate::changeset::RowChange],
    ) -> Result<(), crate::database::BlobDeclError> {
        crate::database::local_blob_cleanup_intents::intents_from_changes(
            self.blob_decls.as_ref(),
            old_changes,
            new_changes,
        )
        .map(|_| ())
    }

    pub(crate) fn receive_wall_ms(&self) -> u64 {
        self.hlc.wall_now_ms()
    }

    pub(crate) fn new_store_write_id(&self) -> crate::WriteId {
        crate::WriteId::from_generated(self.ids.new_id())
    }

    pub(crate) async fn get_protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.connection
            .call(move |connection| crate::database::get_protocol_state_on(connection, &key))
            .await
    }

    pub(crate) async fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let key = key.to_string();
        let value = value.to_string();
        self.connection
            .call(move |connection| {
                crate::database::set_protocol_state_on(connection, &key, &value)
            })
            .await
    }

    pub(crate) async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let key = cache_budget_state_key(namespace);
        match self.get_protocol_state(&key).await? {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|error| {
                DbError::Message(format!(
                    "cache budget for {namespace:?} in protocol_state is not a byte count: {error}"
                ))
            }),
            None => Ok(None),
        }
    }

    #[doc(hidden)]
    pub(crate) async fn set_cache_budget(
        &self,
        namespace: &str,
        max_bytes: u64,
    ) -> Result<(), DbError> {
        let key = cache_budget_state_key(namespace);
        self.set_protocol_state(&key, &max_bytes.to_string()).await
    }

    pub(crate) async fn write_status(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<crate::WriteStatus, DbError> {
        let write_id = write_id.clone();
        self.connection
            .call(move |connection| {
                let raw: String = connection
                    .query_row(
                        "SELECT status FROM store_writes WHERE write_id = ?1",
                        [write_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                serde_json::from_str(&raw)
                    .map_err(|error| DbError::Message(format!("write {write_id} status: {error}")))
            })
            .await
    }

    pub(crate) fn notify_write_status(&self, write_id: crate::WriteId, status: crate::WriteStatus) {
        let senders = self
            .write_statuses
            .lock()
            .expect("write status mutex poisoned");
        if let Some(sender) = senders.get(&write_id) {
            sender.send_replace(status);
        }
    }

    pub(crate) async fn membership_load_permit(&self) -> MembershipLoadPermit {
        MembershipLoadPermit {
            _guard: self.runtime.membership_load.clone().lock_owned().await,
        }
    }

    pub(crate) async fn membership_mutation_permit(&self) -> MembershipMutationPermit {
        MembershipMutationPermit {
            _guard: self.runtime.membership_mutation.clone().lock_owned().await,
        }
    }

    pub(crate) async fn store_creation_permit(&self) -> StoreCreationPermit {
        StoreCreationPermit {
            _guard: self.runtime.store_creation.clone().lock_owned().await,
        }
    }

    pub(crate) async fn device_exclusion_permit(&self) -> DeviceExclusionPermit {
        DeviceExclusionPermit {
            _guard: self.runtime.device_exclusion.clone().lock_owned().await,
        }
    }

    /// Wait for this device's turn to author its own next Store commit.
    ///
    /// Every path that reads the local position to compose a commit, and every
    /// path that publishes a device head, takes this and holds it across the
    /// pair. Never taken twice in one call chain: a composer holds it until its
    /// candidate is either activated or durably persisted, and a publisher of an
    /// already-persisted candidate takes it for that publication alone.
    pub(crate) async fn author_own_stream(&self) -> OwnStreamAuthorship {
        OwnStreamAuthorship {
            _guard: self
                .runtime
                .own_stream_authorship
                .clone()
                .lock_owned()
                .await,
        }
    }

    pub(crate) async fn snapshot_publication_permit(&self) -> SnapshotPublicationPermit {
        SnapshotPublicationPermit {
            _guard: self.runtime.snapshot_publication.clone().lock_owned().await,
        }
    }

    async fn with_retained_merge_materializations<F, R>(&self, operation: F) -> Result<R, DbError>
    where
        F: FnOnce(
                &rusqlite::Connection,
                &mut RetainedMergeMaterializationCache,
            ) -> Result<R, DbError>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let retained = self.runtime.retained_merge_materializations.clone();
        self.connection
            .call(move |connection| {
                let mut retained = retained.lock().map_err(|_| {
                    DbError::Message(
                        "retained Merge materialization cache lock is poisoned".to_string(),
                    )
                })?;
                operation(connection, &mut retained)
            })
            .await
    }

    pub(crate) async fn begin_store_creation_attempt(
        &self,
        initialized: crate::protocol::store_creation::StoreCreationAttempt,
    ) -> Result<crate::protocol::store_creation::StoreCreationAttempt, DbError> {
        let value = serde_json::to_string(&initialized).map_err(|error| {
            DbError::Message(format!("serialize Store creation attempt: {error}"))
        })?;
        self.connection
            .call(move |conn| {
                let tx = conn.unchecked_transaction().map_err(DbError::from)?;
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO NOTHING",
                    (
                        crate::protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
                        &value,
                    ),
                )
                .map_err(DbError::from)?;
                let actual = crate::database::required_protocol_state_on(
                    &tx,
                    crate::protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
                )?;
                tx.commit().map_err(DbError::from)?;
                serde_json::from_str(&actual).map_err(|error| {
                    DbError::Message(format!("parse Store creation attempt: {error}"))
                })
            })
            .await
    }

    pub(crate) async fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<crate::protocol::store_creation::StoreCreationAttempt>, DbError> {
        self.connection
            .call(move |conn| {
                crate::database::get_protocol_state_on(
                    conn,
                    crate::protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
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

    pub(crate) async fn advance_store_creation_attempt(
        &self,
        previous: crate::protocol::store_creation::StoreCreationAttempt,
        next: crate::protocol::store_creation::StoreCreationAttempt,
    ) -> Result<(), DbError> {
        let previous = serde_json::to_string(&previous).map_err(|error| {
            DbError::Message(format!("serialize Store creation predecessor: {error}"))
        })?;
        let next = serde_json::to_string(&next).map_err(|error| {
            DbError::Message(format!("serialize Store creation successor: {error}"))
        })?;
        self.connection
            .call(move |conn| {
                let changed = conn
                    .execute(
                        "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                        (
                            &next,
                            crate::protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
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
    pub(crate) fn new(database: &Database) -> Self {
        Self::from_database(database.clone())
    }

    #[cfg(test)]
    pub(crate) async fn set_invalid_cache_budget_for_test(
        &self,
        namespace: &str,
        value: &str,
    ) -> Result<(), DbError> {
        let key = cache_budget_state_key(namespace);
        self.set_protocol_state(&key, value).await
    }

    #[cfg(test)]
    pub(crate) fn merge_materialization_failure_injection(
        &self,
    ) -> crate::database::MergeMaterializationFailureInjection {
        self.test_access.merge_materialization_failure_injection()
    }

    #[cfg(test)]
    pub(crate) async fn reach_test_point(&self, point: crate::database::DatabaseTestPoint) {
        self.test_access.reach(point).await;
    }

    #[cfg(test)]
    pub(crate) async fn required_store_root_hash(
        &self,
    ) -> Result<crate::protocol::store_commit::ObjectHash, DbError> {
        self.connection
            .call(|connection| Ok(required_store_root_authority_on(connection)?.store_root_hash))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn scoped_snapshot_counts_for_test(&self) -> Result<(i64, i64, i64), DbError> {
        self.connection
            .call(|connection| {
                connection
                    .query_row(
                        "SELECT
                             (SELECT COUNT(*) FROM documents),
                             (SELECT COUNT(*) FROM paragraphs),
                             (SELECT COUNT(*) FROM _coven_row_routes)",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn migrated_scoped_snapshot_facts_for_test(
        &self,
    ) -> Result<(i64, i64, String), DbError> {
        self.connection
            .call(|connection| {
                connection
                    .query_row(
                        "SELECT
                             (SELECT COUNT(*) FROM documents),
                             (SELECT COUNT(*) FROM _coven_row_routes),
                             (SELECT ordinary FROM documents)",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(DbError::from)
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn generation_zero_replay_baseline_for_test(
        &self,
    ) -> Result<crate::database::RetainedReplayBaseline, DbError> {
        self.connection
            .call(Self::generation_zero_replay_baseline_on)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn replace_generation_zero_replay_authority_for_test(
        &self,
        authority_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .execute(
                        "UPDATE retained_replay_baselines SET authority_bytes = ?1
                         WHERE singleton = 1",
                        [authority_bytes],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_coverage_ref(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        self.connection
            .call(move |connection| Self::circle_bootstrap_coverage_ref_on(connection, circle_id))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_replay_inputs(
        &self,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            crate::protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        self.connection
            .call(Self::circle_bootstrap_replay_inputs_on)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_control_activation_count_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.connection
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                        [circle_id.to_string()],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)
            })
            .await
    }
}

impl crate::id_provider::IdProvider for StoreDatabase {
    fn new_id(&self) -> String {
        self.ids.new_id()
    }
}
