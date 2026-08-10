mod acknowledgements;
mod activated_registration_records;
mod blob_bindings;
mod blob_outbox;
mod blob_transitions;
mod candidate_lifecycle;
pub mod candidate_records;
mod circle_acknowledgements;
mod circle_authority;
mod circle_controls;
mod circle_operation_discard;
mod circle_operations;
use circle_authority::circle_blob_opening_protection_on;
use circle_operations::circle_publication_context_on;
mod circle_snapshot_publication;
mod device_continuation;
mod device_exclusion;
mod device_join;
mod device_join_challenges;
pub mod device_join_journal;
mod device_registration_journal;
mod host_sql;
mod host_sql_transaction;
mod host_write_capture;
mod host_write_operation;
mod local_blob_cleanup;
pub mod local_blob_cleanup_intents;
mod materialization;
pub mod materialization_models;
mod materialized_commit_index;
use activated_registration_records::record_activated_store_device_registrations_on;
mod membership_mutations;
mod membership_rotation;
mod merge_materialization_transaction;
mod owner_promotion;
pub mod payload_spool;
mod pending_publication;
mod preparation;
mod prepared_remote_objects;
pub(crate) use prepared_remote_objects::persist_prepared_audience_objects_on;
mod provider_probe;
mod publication;
pub mod publication_state;
mod pull_replay;
pub mod reclaim;
mod replay_projection;
use replay_projection::ReplayProjection;
mod retained_merge_replay;
mod retained_replay;
mod snapshot_image;
mod snapshot_publication;
mod store_acknowledgements;
mod store_authority;
mod store_device_state;
mod store_records;
pub(crate) use store_records::{StoreRecordTransaction, StoreRecords};
mod store_session;
mod stream_activation_records;
mod verified_store_authority;
pub(crate) use verified_store_authority::VerifiedStoreAuthority;
#[cfg(any(test, feature = "test-utils"))]
mod test_support;
mod write_lifecycle;

use crate::{
    begin_remote_candidate_nonactivation_on, finish_outbound_store_ack_on,
    load_protocol_inert_object_on, load_remote_object_on, persist_exact_remote_object_on,
    replace_prepared_merge_head_remote_on, Database, DbError, OutboundStoreAckActivation,
};
use coven_protocol::objects::PreparedExactObject;
use coven_protocol::prepared_commit::PreparedStoreOperationCommit;
use coven_protocol::remote_object::{
    remote_object_id, CandidateNonactivationProof, VerifiedCandidateNonactivation,
};
#[cfg(any(test, feature = "test-utils"))]
use coven_protocol::store_commit::StoreBatchCommitRef;
use coven_protocol::store_commit::{StoreAckRef, StoreDeviceHead};

const CACHE_BUDGET_STATE_KEY_PREFIX: &str = "cache_budget:";

fn cache_budget_state_key(namespace: &str) -> String {
    format!("{CACHE_BUDGET_STATE_KEY_PREFIX}{namespace}")
}

pub use blob_outbox::{MakeRemoteProgress, QueuedDelete, QueuedUpload};
pub use blob_outbox::{OutboxEntry, OutboxOperation, OutboxUploadState};
pub use blob_transitions::MaterializedLocalBlob;
pub use blob_transitions::PostUpload;
#[cfg(any(test, feature = "test-utils"))]
pub use candidate_records::select_author_exclusion_activation_locator;
pub use candidate_records::CandidateCleanupObject;
pub use circle_controls::PreparedCircleObjects;
pub use device_join::DeviceJoinJournalStore;
pub use host_sql::{SqlContext, SqlReadContext};
pub use host_write_capture::{
    audience_moves_by_row, AudienceBlobMoveStaging, HostWriteBlobTransaction,
    StagedAudienceBlobRollback,
};
pub use host_write_operation::StoreRowWrites;
pub use host_write_operation::{BlobFileFailure, BlobFileFailures, WriteBatch};
pub use host_write_operation::{HostWriteError, HostWriteOperation};
pub(crate) use local_blob_cleanup::record_obsolete_copy_intents_on;
pub use local_blob_cleanup::LocalBlobCleanup;
pub use materialization_models::{
    activated_merge_membership_remote_objects, DeviceJoinBootstrapActivation,
    DeviceJoinBootstrapCommit, DeviceJoinBootstrapPlan, MembershipAuthorityBytes,
    OwnedVerifiedMergeMaterialization, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage, RetainedAudiencePackage, RetainedMergeHistoryCheckpoint,
    RetainedMergeMaterializationKey, RetainedPackageApplication, VerifiedMergeMaterialization,
    VerifiedMergeMembershipObjects, VerifiedStoreSnapshotStability,
};
#[cfg(any(test, feature = "test-utils"))]
pub use merge_materialization_transaction::{resolve_and_apply_changeset, ApplyResult};
pub use merge_materialization_transaction::{
    IncomingTimestampPolicy, MergeMaterializationTransaction, TableSchema, ValidatedChangeset,
    WinningRow,
};
pub use publication_state::{MergeCandidateAbandonmentPreparation, StoreWritePreparation};
pub use pull_replay::{
    install_circle_bootstrap_connection_on, install_circle_bootstrap_image_on,
    install_circle_bootstrap_remote_objects_on,
};
pub use reclaim::journal::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, ReclaimCommitActivation,
    ReclaimedStorePackage, StoreReclaimCandidateLoss, StoreReclaimJournalError,
};
pub use retained_replay::{
    copy_table_with_conflicts, projection_table_names, RetainedReplayAuthority,
    RetainedReplayBaseline, RetainedReplayGenesisAuthority, RetainedReplaySnapshotAuthority,
    GENERATION_ZERO,
};
use snapshot_image::snapshot_image_db_error;
pub use snapshot_image::{
    verify_circle_bootstrap_connection, verify_circle_bootstrap_image, CreatedSnapshot,
    SnapshotBlobAudience, SnapshotDatabaseImage, SnapshotImageError, SnapshotImageOperationError,
};
use store_device_state::apply_store_device_exclusion_freezes_on;
#[cfg(any(test, feature = "test-utils"))]
pub use test_support::AuthorExclusionLocatorTamper;
pub use write_lifecycle::BlockedWriteDiscard;

#[derive(Clone)]
pub struct StoreDatabaseRuntime {
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
    pub fn new() -> Self {
        Self {
            membership_load: Default::default(),
            membership_mutation: Default::default(),
            store_creation: Default::default(),
            device_exclusion: Default::default(),
            snapshot_publication: Default::default(),
            own_stream_authorship: Default::default(),
            local_blob_cleanup: Default::default(),
        }
    }
}

pub struct MembershipLoadPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub struct MembershipMutationPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub struct StoreCreationPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub struct DeviceExclusionPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// This device's exclusive turn to author its own next Store commit, held from
/// reading the position through publishing the head that takes it.
pub struct OwnStreamAuthorship {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub struct SnapshotPublicationPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// One connection-thread Store operation.
///
/// The Store implementation can combine its row-and-payload capability with
/// authority verified by the same connection. Code outside this module cannot
/// obtain either retained dependency.
pub(crate) struct StoreSession<'session> {
    conn: &'session rusqlite::Connection,
    store_dir: &'session coven_foundation::store_dir::StoreDir,
    verified_store_authority: &'session mut VerifiedStoreAuthority,
    gates: &'session crate::Gates,
    synced_tables: &'session [coven_protocol::synced_schema::SyncedTable],
    schema_version: u32,
    sync_routing_hash: coven_protocol::store_commit::ObjectHash,
    hlc: &'session std::sync::Arc<coven_protocol::hlc::Hlc>,
    blob_decls: &'session crate::BlobDecls,
}

#[derive(Clone)]
pub struct StoreDatabase {
    store_dir: coven_foundation::store_dir::StoreDir,
    connection: crate::DatabaseConnection,
    runtime: StoreDatabaseRuntime,
    hlc: std::sync::Arc<coven_protocol::hlc::Hlc>,
    synced_tables: std::sync::Arc<Vec<coven_protocol::synced_schema::SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: coven_protocol::store_commit::ObjectHash,
    gates: std::sync::Arc<crate::Gates>,
    blob_decls: std::sync::Arc<crate::BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
    transfer_limits: coven_protocol::blob::TransferLimits,
    ids: coven_foundation::id_provider::IdRef,
    write_statuses: std::sync::Arc<
        std::sync::Mutex<
            std::collections::HashMap<
                coven_protocol::write::WriteId,
                tokio::sync::watch::Sender<coven_protocol::write::WriteStatus>,
            >,
        >,
    >,
    #[cfg(any(test, feature = "test-utils"))]
    test_access: crate::StoreDatabaseTestAccess,
}

impl StoreSession<'_> {
    fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E>,
    {
        let authorization = host_sql_transaction::HostSqlAuthorization::begin(self.conn)?;
        Ok(authorization.run(|| read(SqlReadContext::new(self.conn))))
    }

    fn protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        crate::get_protocol_state_on(self.conn, key)
    }

    fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        crate::set_protocol_state_on(self.conn, key, value)
    }

    fn write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::write::WriteStatus, DbError> {
        let raw: String = self
            .conn
            .query_row(
                "SELECT status FROM store_writes WHERE write_id = ?1",
                [write_id.as_str()],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        serde_json::from_str(&raw)
            .map_err(|error| DbError::context(format!("write {write_id} status"), error))
    }

    fn begin_store_creation_attempt(
        &self,
        value: &str,
    ) -> Result<coven_protocol::store_creation::StoreCreationAttempt, DbError> {
        let tx = self.conn.unchecked_transaction().map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO NOTHING",
            (
                coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
                value,
            ),
        )
        .map_err(DbError::from)?;
        let actual = crate::required_protocol_state_on(
            &tx,
            coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
        )?;
        tx.commit().map_err(DbError::from)?;
        serde_json::from_str(&actual)
            .map_err(|error| DbError::context("parse Store creation attempt", error))
    }

    fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<coven_protocol::store_creation::StoreCreationAttempt>, DbError> {
        self.protocol_state(coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY)?
            .map(|value| {
                serde_json::from_str(&value)
                    .map_err(|error| DbError::context("parse Store creation attempt", error))
            })
            .transpose()
    }

    fn advance_store_creation_attempt(&self, previous: &str, next: &str) -> Result<(), DbError> {
        let changed = self
            .conn
            .execute(
                "UPDATE protocol_state SET value = ?1 WHERE key = ?2 AND value = ?3",
                (
                    next,
                    coven_protocol::store_creation::STORE_CREATION_ATTEMPT_STATE_KEY,
                    previous,
                ),
            )
            .map_err(DbError::from)?;
        if changed != 1 {
            return Err(DbError::Message(
                "Store creation attempt advance lost its exact predecessor".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn scoped_snapshot_counts(&self) -> Result<(i64, i64, i64), DbError> {
        self.conn
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM documents),
                     (SELECT COUNT(*) FROM paragraphs),
                     (SELECT COUNT(*) FROM _coven_row_routes)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn migrated_scoped_snapshot_facts(&self) -> Result<(i64, i64, String), DbError> {
        self.conn
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM documents),
                     (SELECT COUNT(*) FROM _coven_row_routes),
                     (SELECT ordinary FROM documents)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn circle_bootstrap_coverage_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        StoreDatabase::circle_bootstrap_coverage_ref_on(self.conn, circle_id)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn circle_control_activation_count(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM circle_control_activations WHERE circle_id = ?1",
                [circle_id.to_string()],
                |row| row.get(0),
            )
            .map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn generation_zero_replay_baseline(&self) -> Result<crate::RetainedReplayBaseline, DbError> {
        StoreDatabase::generation_zero_replay_baseline_on(crate::store::StoreRecords::new(
            self.conn,
            self.store_dir,
        ))
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn replace_generation_zero_replay_authority(
        &self,
        authority_bytes: &[u8],
    ) -> Result<(), DbError> {
        let authority_hash = payload_spool::write_payload_blocking(self.store_dir, authority_bytes)
            .map_err(|error| {
                DbError::Message(format!("install retained replay authority: {error}"))
            })?;
        let transaction = self.conn.unchecked_transaction().map_err(DbError::from)?;
        transaction
            .execute(
                "UPDATE retained_replay_baselines SET authority_hash = ?1
                 WHERE singleton = 1",
                [authority_hash.to_string()],
            )
            .map_err(DbError::from)?;
        let image_hash: String = transaction
            .query_row(
                "SELECT image_hash FROM retained_replay_baselines WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        payload_spool::set_payload_owner_claims_on(
            &transaction,
            payload_spool::RETAINED_REPLAY_BASELINE_OWNER_KEY,
            &std::collections::BTreeSet::from([image_hash.parse()?, authority_hash]),
        )?;
        transaction.commit().map_err(DbError::from)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn circle_bootstrap_replay_inputs(
        &self,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        StoreDatabase::circle_bootstrap_replay_inputs_on(crate::store::StoreRecords::new(
            self.conn,
            self.store_dir,
        ))
    }
}

impl StoreDatabase {
    #[doc(hidden)]
    pub fn from_database(database: Database) -> Self {
        let Database { connection, state } = database;
        Self {
            connection,
            store_dir: state.store_dir,
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
            #[cfg(any(test, feature = "test-utils"))]
            test_access: crate::StoreDatabaseTestAccess {
                pause_points: state.test_pause_points,
                merge_materialization_failure: state.merge_materialization_failure,
            },
        }
    }

    pub async fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.connection
            .call_store(move |session| session.read(read))
            .await
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn sync_routing_hash(&self) -> coven_protocol::store_commit::ObjectHash {
        self.sync_routing_hash
    }

    pub fn synced_tables(&self) -> &[coven_protocol::synced_schema::SyncedTable] {
        &self.synced_tables
    }

    pub fn transfer_limits(&self) -> coven_protocol::blob::TransferLimits {
        self.transfer_limits
    }

    pub fn blob_tombstone_grace(&self) -> chrono::Duration {
        self.blob_tombstone_grace
    }

    pub fn has_scoped_graph(&self) -> bool {
        self.gates.has_scoped_graph()
    }

    pub fn stamp(&self) -> String {
        self.hlc.now().to_string()
    }

    pub async fn persist_hlc_high_water(&self) -> Result<(), DbError> {
        self.set_protocol_state(
            coven_protocol::hlc::HIGHWATER_STATE_KEY,
            &self.hlc.high_water().to_string(),
        )
        .await
    }

    pub fn blob_ref_from_change(
        &self,
        change: &coven_foundation::changeset::RowChange,
    ) -> Result<Option<coven_protocol::blob::BlobRef>, crate::BlobDeclError> {
        self.blob_decls.ref_from_change(change)
    }

    pub fn validate_local_blob_cleanup_changes(
        &self,
        old_changes: &[coven_foundation::changeset::RowChange],
        new_changes: &[coven_foundation::changeset::RowChange],
    ) -> Result<(), crate::BlobDeclError> {
        crate::local_blob_cleanup_intents::intents_from_changes(
            self.blob_decls.as_ref(),
            old_changes,
            new_changes,
        )
        .map(|_| ())
    }

    pub fn receive_wall_ms(&self) -> u64 {
        self.hlc.wall_now_ms()
    }

    pub fn new_store_write_id(&self) -> coven_protocol::write::WriteId {
        coven_protocol::write::WriteId::from_generated(self.ids.new_id())
    }

    pub async fn get_protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        let key = key.to_string();
        self.connection
            .call_store(move |session| session.protocol_state(&key))
            .await
    }

    pub async fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        let key = key.to_string();
        let value = value.to_string();
        self.connection
            .call_store(move |session| session.set_protocol_state(&key, &value))
            .await
    }

    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError> {
        let key = cache_budget_state_key(namespace);
        match self.get_protocol_state(&key).await? {
            Some(raw) => raw.parse::<u64>().map(Some).map_err(|error| {
                DbError::context(
                    format!("cache budget for {namespace:?} in protocol_state is not a byte count"),
                    error,
                )
            }),
            None => Ok(None),
        }
    }

    #[doc(hidden)]
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError> {
        let key = cache_budget_state_key(namespace);
        self.set_protocol_state(&key, &max_bytes.to_string()).await
    }

    pub async fn write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::write::WriteStatus, DbError> {
        let write_id = write_id.clone();
        self.connection
            .call_store(move |session| session.write_status(&write_id))
            .await
    }

    pub fn notify_write_status(
        &self,
        write_id: coven_protocol::write::WriteId,
        status: coven_protocol::write::WriteStatus,
    ) {
        let senders = self
            .write_statuses
            .lock()
            .expect("write status mutex poisoned");
        if let Some(sender) = senders.get(&write_id) {
            sender.send_replace(status);
        }
    }

    pub async fn membership_load_permit(&self) -> MembershipLoadPermit {
        MembershipLoadPermit {
            _guard: self.runtime.membership_load.clone().lock_owned().await,
        }
    }

    pub async fn membership_mutation_permit(&self) -> MembershipMutationPermit {
        MembershipMutationPermit {
            _guard: self.runtime.membership_mutation.clone().lock_owned().await,
        }
    }

    pub async fn store_creation_permit(&self) -> StoreCreationPermit {
        StoreCreationPermit {
            _guard: self.runtime.store_creation.clone().lock_owned().await,
        }
    }

    pub async fn device_exclusion_permit(&self) -> DeviceExclusionPermit {
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
    pub async fn author_own_stream(&self) -> OwnStreamAuthorship {
        OwnStreamAuthorship {
            _guard: self
                .runtime
                .own_stream_authorship
                .clone()
                .lock_owned()
                .await,
        }
    }

    pub async fn snapshot_publication_permit(&self) -> SnapshotPublicationPermit {
        SnapshotPublicationPermit {
            _guard: self.runtime.snapshot_publication.clone().lock_owned().await,
        }
    }

    pub async fn begin_store_creation_attempt(
        &self,
        initialized: coven_protocol::store_creation::StoreCreationAttempt,
    ) -> Result<coven_protocol::store_creation::StoreCreationAttempt, DbError> {
        let value = serde_json::to_string(&initialized)
            .map_err(|error| DbError::context("serialize Store creation attempt", error))?;
        self.connection
            .call_store(move |session| session.begin_store_creation_attempt(&value))
            .await
    }

    pub async fn load_store_creation_attempt(
        &self,
    ) -> Result<Option<coven_protocol::store_creation::StoreCreationAttempt>, DbError> {
        self.connection
            .call_store(|session| session.load_store_creation_attempt())
            .await
    }

    pub async fn advance_store_creation_attempt(
        &self,
        previous: coven_protocol::store_creation::StoreCreationAttempt,
        next: coven_protocol::store_creation::StoreCreationAttempt,
    ) -> Result<(), DbError> {
        let previous = serde_json::to_string(&previous)
            .map_err(|error| DbError::context("serialize Store creation predecessor", error))?;
        let next = serde_json::to_string(&next)
            .map_err(|error| DbError::context("serialize Store creation successor", error))?;
        self.connection
            .call_store(move |session| session.advance_store_creation_attempt(&previous, &next))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn new(database: &Database) -> Self {
        Self::from_database(database.clone())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn set_invalid_cache_budget_for_test(
        &self,
        namespace: &str,
        value: &str,
    ) -> Result<(), DbError> {
        let key = cache_budget_state_key(namespace);
        self.set_protocol_state(&key, value).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn merge_materialization_failure_injection(
        &self,
    ) -> crate::MergeMaterializationFailureInjection {
        self.test_access.merge_materialization_failure_injection()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn reach_test_point(&self, point: crate::DatabaseTestPoint) {
        self.test_access.reach(point).await;
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn required_store_root_hash(
        &self,
    ) -> Result<coven_protocol::store_commit::ObjectHash, DbError> {
        self.connection
            .call_store(|session| Ok(session.required_root_authority()?.store_root_hash))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn scoped_snapshot_counts_for_test(&self) -> Result<(i64, i64, i64), DbError> {
        self.connection
            .call_store(|session| session.scoped_snapshot_counts())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn migrated_scoped_snapshot_facts_for_test(
        &self,
    ) -> Result<(i64, i64, String), DbError> {
        self.connection
            .call_store(|session| session.migrated_scoped_snapshot_facts())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn generation_zero_replay_baseline_for_test(
        &self,
    ) -> Result<crate::RetainedReplayBaseline, DbError> {
        self.connection
            .call_store(|session| session.generation_zero_replay_baseline())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn replace_generation_zero_replay_authority_for_test(
        &self,
        authority_bytes: Vec<u8>,
    ) -> Result<(), DbError> {
        self.connection
            .call_store(move |session| {
                session.replace_generation_zero_replay_authority(&authority_bytes)
            })
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn circle_bootstrap_coverage_ref(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, DbError> {
        self.connection
            .call_store(move |session| session.circle_bootstrap_coverage_ref(circle_id))
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn circle_bootstrap_replay_inputs(
        &self,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        DbError,
    > {
        self.connection
            .call_store(|session| session.circle_bootstrap_replay_inputs())
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn circle_control_activation_count_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.connection
            .call_store(move |session| session.circle_control_activation_count(circle_id))
            .await
    }
}

impl coven_foundation::id_provider::IdProvider for StoreDatabase {
    fn new_id(&self) -> String {
        self.ids.new_id()
    }
}
