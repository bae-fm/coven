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
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use circle_operations::circle_current_state_on;
use circle_operations::circle_publication_context_on;
mod circle_snapshot_publication;
mod device_continuation;
mod device_exclusion;
mod device_join;
pub(crate) use device_join::{
    advance_device_join_on, begin_device_join_on, begin_device_join_replacement_terminal_on,
    complete_device_join_from_pending_on, device_join_records_on,
};
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use device_join::{
    forget_device_join_on, forget_provider_administrator_device_joins_on,
};
mod device_join_challenges;
pub mod device_join_journal;
mod device_registration_journal;
mod host_sql;
mod host_sql_transaction;
mod host_write_capture;
mod host_write_operation;
pub(crate) use host_write_operation::{NewBlob, StagedBlobBatch};
mod local_blob_cleanup;
pub(crate) use local_blob_cleanup::{
    complete_local_blob_cleanup_on, local_blob_cleanup_intents_on,
};
pub mod local_blob_cleanup_intents;
mod materialization;
pub mod materialization_models;
mod materialized_commit_index;
use activated_registration_records::record_activated_store_device_registrations_on;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use materialized_commit_index::materialized_frontier_on;
mod membership_mutations;
mod membership_rotation;
mod merge_materialization_transaction;
mod owner_promotion;
mod owner_recovery_publication;
pub mod payload_spool;
mod pending_publication;
mod preparation;
mod prepared_remote_objects;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use prepared_remote_objects::persist_prepared_audience_objects_on;
mod provider_probe;
mod publication;
pub mod publication_state;
mod pull_replay;
pub mod reclaim;
mod replay_projection;
use replay_projection::ReplayProjection;
mod retained_merge_replay;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use retained_merge_replay::circle_bootstrap_coverage_ref_on;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use retained_merge_replay::remove_retained_replay_ownership_from_snapshot_on;
mod retained_replay;
mod snapshot_image;
mod snapshot_publication;
mod store_acknowledgements;
mod store_authority;
mod store_database;
mod store_device_state;
mod store_records;
pub use store_database::StoreDatabase;
mod store_session;
mod stream_activation_records;
mod verified_store_authority;
pub(crate) use verified_store_authority::VerifiedStoreAuthority;
#[cfg(any(test, feature = "test-utils"))]
mod test_support;
mod write_lifecycle;

/// One Store's row connection and matching payload directory.
///
/// A record whose bytes live in the spool is half a row and half a file, so
/// record operations carry both halves as one scoped value. Operations that
/// touch rows alone continue to take the connection in their private SQL leaf.
#[derive(Clone, Copy)]
pub(crate) struct StoreRecords<'store> {
    conn: &'store rusqlite::Connection,
    store_dir: &'store coven_foundation::store_dir::StoreDir,
}

/// One Store transaction and its matching row-and-payload capability.
///
/// Payloads land before the row naming them commits, while ownership claims
/// land in this transaction. Keeping both borrows together prevents a record
/// mutation from using another Store's payload directory.
#[derive(Clone, Copy)]
pub(crate) struct StoreTransaction<'store, 'connection> {
    transaction: &'store rusqlite::Transaction<'connection>,
    records: StoreRecords<'store>,
}

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
use coven_protocol::store_commit::{StoreAckRef, StoreBatchCommitRef, StoreDeviceHead};

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
#[cfg(any(test, feature = "test-utils"))]
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
pub(crate) use merge_materialization_transaction::MergeMaterializationTransaction;
#[cfg(any(test, feature = "test-utils"))]
pub use merge_materialization_transaction::{resolve_and_apply_changeset, ApplyResult};
pub use merge_materialization_transaction::{
    IncomingTimestampPolicy, TableSchema, ValidatedChangeset, WinningRow,
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

pub(crate) fn install_verified_snapshot_bootstrap_on(
    transaction: &rusqlite::Transaction<'_>,
    store_dir: &coven_foundation::store_dir::StoreDir,
    install: &crate::VerifiedSnapshotBootstrapInstall,
    schema_version: u32,
    routing_hash: coven_protocol::store_commit::ObjectHash,
    synced_tables: &[coven_protocol::synced_schema::SyncedTable],
) -> Result<(), DbError> {
    StoreTransaction::new(transaction, store_dir).install_verified_snapshot_bootstrap(
        install,
        schema_version,
        routing_hash,
        synced_tables,
    )
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn circle_bootstrap_replay_inputs_for_test(
    connection: &rusqlite::Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
) -> Result<
    Vec<(
        StoreBatchCommitRef,
        coven_protocol::circle_activation::VerifiedCircleImage,
    )>,
    DbError,
> {
    StoreDatabase::circle_bootstrap_replay_inputs_on(StoreRecords::new(connection, store_dir))
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn retained_merge_replay_inputs_for_test(
    connection: &rusqlite::Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    root: &coven_protocol::store_commit::StoreRootRef,
) -> Result<Vec<OwnedVerifiedMergeMaterialization>, DbError> {
    let mut authority = VerifiedStoreAuthority::default();
    StoreDatabase::load_retained_merge_replay_inputs_on(
        StoreRecords::new(connection, store_dir),
        root,
        &mut authority,
    )
}

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
            own_stream_authorship: Default::default(),
            local_blob_cleanup: Default::default(),
        }
    }

    pub(crate) async fn membership_load_permit(&self) -> MembershipLoadPermit {
        MembershipLoadPermit {
            _guard: self.membership_load.clone().lock_owned().await,
        }
    }

    pub(crate) async fn membership_mutation_permit(&self) -> MembershipMutationPermit {
        MembershipMutationPermit {
            _guard: self.membership_mutation.clone().lock_owned().await,
        }
    }

    pub(crate) async fn store_creation_permit(&self) -> StoreCreationPermit {
        StoreCreationPermit {
            _guard: self.store_creation.clone().lock_owned().await,
        }
    }

    pub(crate) async fn device_exclusion_permit(&self) -> DeviceExclusionPermit {
        DeviceExclusionPermit {
            _guard: self.device_exclusion.clone().lock_owned().await,
        }
    }

    pub(crate) async fn author_own_stream(&self) -> OwnStreamAuthorship {
        OwnStreamAuthorship {
            _guard: self.own_stream_authorship.clone().lock_owned().await,
        }
    }

    pub(crate) async fn snapshot_publication_permit(&self) -> SnapshotPublicationPermit {
        SnapshotPublicationPermit {
            _guard: self.snapshot_publication.clone().lock_owned().await,
        }
    }

    pub(crate) async fn local_blob_cleanup_permit(&self) -> LocalBlobCleanupPermit {
        LocalBlobCleanupPermit {
            _guard: self.local_blob_cleanup.clone().lock_owned().await,
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

pub struct LocalBlobCleanupPermit {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// One connection-thread Store operation.
///
/// The Store implementation can combine its row-and-payload capability with
/// authority verified by the same connection. Code outside this module cannot
/// obtain either retained dependency.
pub(crate) struct StoreSession<'session> {
    records: StoreRecords<'session>,
    verified_store_authority: &'session mut VerifiedStoreAuthority,
    gates: &'session crate::Gates,
    synced_tables: &'session [coven_protocol::synced_schema::SyncedTable],
    schema_version: u32,
    sync_routing_hash: coven_protocol::store_commit::ObjectHash,
    hlc: &'session std::sync::Arc<coven_protocol::hlc::Hlc>,
    blob_decls: &'session crate::BlobDecls,
    #[cfg(any(test, feature = "test-utils"))]
    merge_materialization_failure:
        &'session std::sync::Mutex<Option<crate::MergeMaterializationFailurePoint>>,
}

impl StoreSession<'_> {
    fn read<F, R, E>(&self, read: F) -> Result<Result<R, E>, DbError>
    where
        F: for<'connection> FnOnce(SqlReadContext<'connection>) -> Result<R, E>,
    {
        let authorization = host_sql_transaction::HostSqlAuthorization::begin(self.records.conn)?;
        Ok(authorization.run(|| read(SqlReadContext::new(self.records.conn))))
    }

    fn protocol_state(&self, key: &str) -> Result<Option<String>, DbError> {
        crate::get_protocol_state_on(self.records.conn, key)
    }

    fn set_protocol_state(&self, key: &str, value: &str) -> Result<(), DbError> {
        crate::set_protocol_state_on(self.records.conn, key, value)
    }

    fn write_status(
        &self,
        write_id: &coven_protocol::write::WriteId,
    ) -> Result<coven_protocol::write::WriteStatus, DbError> {
        let raw: String = self
            .records
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
        let tx = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
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
            .records
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
        self.records
            .conn
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
        self.records
            .conn
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
        retained_merge_replay::circle_bootstrap_coverage_ref_on(self.records.conn, circle_id)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn circle_control_activation_count(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, DbError> {
        self.records
            .conn
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
            self.records.conn,
            self.records.store_dir,
        ))
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn replace_generation_zero_replay_authority(
        &self,
        authority_bytes: &[u8],
    ) -> Result<(), DbError> {
        let authority_hash =
            payload_spool::write_payload_blocking(self.records.store_dir, authority_bytes)
                .map_err(|error| {
                    DbError::Message(format!("install retained replay authority: {error}"))
                })?;
        let transaction = self
            .records
            .conn
            .unchecked_transaction()
            .map_err(DbError::from)?;
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
            self.records.conn,
            self.records.store_dir,
        ))
    }
}
