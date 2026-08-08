//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` together with the sync bookkeeping
//! beside it. Every database access — the host's app SQL, coven's bookkeeping,
//! changeset capture and apply — runs against that one connection, so access is
//! serialized.
//!
//! Hosts open coven with `Coven::builder` and run app SQL through
//! `CovenHandle::sql` or `CovenHandle::write`.

pub use crate::blob_records::{load_activated_registration_on, remote_audience_to_db};
pub use crate::circle_snapshot_records::{
    load_outbound_circle_snapshot_on, load_published_circle_snapshot_on,
};
pub use crate::cloud_outbox_records::{
    outbox_identity, row_to_outbox_entry, CloudOutboxRecords, OutboxIdentity,
};
use crate::connection_io::open_connection;
use crate::connection_io::open_connection_read_only;
use crate::connection_io::scan_max_updated_at;
use crate::connection_io::seed_from;
pub use crate::local_state::{
    delete_protocol_state_on, get_protocol_state_on, required_protocol_state_on,
    set_protocol_state_on,
};
use crate::local_store_identity::pin_host_device_id_on;
use crate::local_store_identity::validate_host_device_id_on;
pub use crate::remote_object_records::begin_remote_candidate_nonactivation_on;
pub use crate::remote_object_records::begin_remote_candidate_nonactivation_with_verified_head_on;
pub use crate::remote_object_records::candidate_graph_exact_objects;
pub use crate::remote_object_records::finish_remote_candidate_nonactivation_on;
pub use crate::remote_object_records::index_retained_replay_owner_on;
pub use crate::remote_object_records::load_protocol_inert_object_on;
pub use crate::remote_object_records::mark_remote_object_uploaded_on;
pub use crate::remote_object_records::mark_reusable_retained_authority_uploaded_on;
pub use crate::remote_object_records::persist_exact_remote_object_on;
pub use crate::remote_object_records::persist_prepared_remote_object_on;
pub use crate::remote_object_records::record_reclaimed_store_package_on;
pub use crate::remote_object_records::replace_prepared_merge_head_remote_on;
pub use crate::remote_object_records::update_remote_object_on;
pub use crate::remote_object_records::{load_remote_object_on, reopen_remote_object_on};
pub use crate::remote_object_records::{
    validate_prepared_blob_on, validate_prepared_package_on, validate_remote_object_on,
};
use crate::snapshot_objects::validate_snapshot_object_owners_on;
pub use crate::snapshot_objects::{
    install_snapshot_blob_plan_on, install_snapshot_blob_plans_on, persist_snapshot_image_on,
    snapshot_generation_as_i64, validate_snapshot_author, validate_snapshot_blob_plans_on,
    validate_snapshot_image, verify_snapshot_blob_spools,
};
pub use crate::snapshot_records::{
    load_outbound_store_snapshot_on, load_published_store_snapshot_on,
};
pub use crate::store_ack_records::{
    finish_outbound_store_ack_on, load_published_store_ack_on, store_snapshot_first_slot,
    verify_next_local_store_ack_on,
};
pub use crate::store_authority_records::install_store_founder_state_on;
pub use crate::store_reclaim_records::{
    insert_store_reclaim_operation_on, load_store_reclaim_operation_on,
    parse_store_reclaim_operation, store_reclaim_journal_error, update_store_reclaim_operation_on,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coven_keys::encryption::EncryptionService;
use coven_protocol::audience_package::{AudiencePackage, RowBlobLocatorBinding};
use coven_protocol::blob::locator::{BlobLocator, RemoteAudience, StoredBlobRef};
use coven_protocol::blob::{BlobRef, RowBlobAuthority, RowBlobRef};
use coven_protocol::circle::Audience;
use coven_protocol::hlc::{Hlc, Timestamp, HIGHWATER_STATE_KEY, MAX_FUTURE_SKEW_MS};
use coven_protocol::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
};
use coven_protocol::objects::{ExactObjectRef, PreparedExactObject};
use coven_protocol::remote_object::{
    remote_object_id, CandidateExclusiveObjectDomain, RemoteObjectRecord, RetainedReplayOwner,
    SharedLiveSetObjectDomain,
};
use coven_protocol::store_commit::{
    ack_slot_prefix, CommitFrontier, ObjectHash, ResolvedStoreDeviceState, SnapshotImageRef,
    SnapshotMeta, StoreAck, StoreAckRef, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolRoot,
    StoreSnapshotRef,
};
use coven_protocol::synced_schema::SyncedTable;
use coven_protocol::write::{WriteId, WriteStatus};
use rusqlite::{Connection, OptionalExtension};

pub use rusqlite;

mod blob_bindings;
mod blob_declarations;
mod blob_records;
mod changeset;
mod changeset_identity;
mod circle_operation_records;
mod cloud_outbox_records;
mod connection_io;
mod coven_schema;
mod database_connection;
pub use connection_io::{attach_session, capture_changeset, open_database_image};
#[cfg(test)]
pub(crate) use coven_schema::all_table_names;
pub use coven_schema::{
    apply_coven_routing_schema, apply_coven_schema, expected_coven_schema_manifest,
    is_reserved_table_name, live_coven_schema_manifest, user_table_names, CovenSchemaManifest,
};
mod circle_snapshot_records;
mod database_open;
mod database_runtime;
mod external_blob_records;
mod gate;
mod local_state;
mod local_store_identity;
mod make_remote;
mod migration;
mod operation_models;
mod prepared_audience_objects;
mod remote_object_records;
mod routing_contract;
mod schema_contract;
mod schema_introspection;
mod snapshot_objects;
mod snapshot_records;
pub mod store;
mod store_ack_records;
mod store_authority_records;
mod store_coordinates;
mod store_reclaim_records;
#[cfg(any(test, feature = "test-utils"))]
mod test_sql;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_support;
#[cfg(any(test, feature = "test-utils"))]
mod test_transaction;
#[cfg(any(test, feature = "test-utils"))]
pub use coven_schema::DatabaseTestTable;
#[cfg(any(test, feature = "test-utils"))]
pub use test_sql::DatabaseTestSql;
#[cfg(any(test, feature = "test-utils"))]
pub use test_support::synthetic_store;
#[cfg(any(test, feature = "test-utils"))]
pub use test_support::{
    DatabaseImageTest, OutboxAttempt, RetainedRegistrationTamper, ScopedRoutingStateForTest,
};
#[cfg(any(test, feature = "test-utils"))]
pub use test_transaction::DatabaseTestTransaction;
mod write_lifecycle;
mod write_models;

#[cfg(any(test, feature = "test-utils"))]
pub use blob_declarations::{from_tables_call_count, reset_from_tables_call_count};
pub use blob_declarations::{BlobDeclError, BlobDecls, PublicationBlob};
pub use blob_records::{load_prepared_audience_objects_on, previous_row_blob_for_write_on};
pub use changeset::{value_ref_to_string, walk as walk_changeset, walk_old as walk_old_changeset};
pub use changeset_identity::ChangesetIdentityError;
pub(crate) use circle_operation_records::{
    circle_operation_ids_in_phase_on, circle_operation_phase_json,
};
pub use circle_operation_records::{
    circle_operation_uploaded_steps_on, load_circle_operation_on, parse_circle_operation_row,
    PreparedCircleOperationRow,
};
pub use coven_protocol::objects::{ExactProtocolObject, PreparedProtocolObject};
pub use database_connection::DatabaseConnection;
use database_open::CovenMetadataOpen;
pub use external_blob_records::ExternalBlob;
use external_blob_records::ExternalBlobRecords;
pub use gate::query_truth;
pub use gate::{
    active_circle_control, align_inbound_scoped_root_audiences, audience_moves,
    capture_routing_changes, filter_inbound_circle_changeset, filter_inbound_store_rows,
    is_routing_table, live_row_audience, normalize_inbound_store_changeset, partition_outbound,
    prune_ineligible_scoped_rows, prune_private_routes_without_rows, retain_snapshot_audience_rows,
    store_audience_transitions, validate_scoped_foreign_key_audiences,
    validate_snapshot_routing_state, AudienceMove, AudiencePartition, CirclePartitionControl,
    GateError, Gates, RoutingChanges, StoreAudienceTransitions,
};
#[cfg(any(test, feature = "test-utils"))]
pub use gate::{
    from_tables_call_count as gate_from_tables_call_count,
    reset_from_tables_call_count as reset_gate_from_tables_call_count,
};
pub use local_store_identity::local_merge_stream_id_on;
pub use local_store_identity::{local_activated_registration_ref_on, local_store_authority_on};
pub use migration::{ensure_schema_supported, run_migrations_in_transaction, supported_version};
pub use migration::{Migration, MigrationContext, MigrationError, MigrationStep};
pub use operation_models::{
    DurableCircleSnapshotPublication, DurableDeviceRegistration, DurableMembershipMutation,
    DurableSnapshotPublication, LocalDeviceRegistrationJournalRow, LocalDeviceRegistrationState,
    MembershipMutationActivation, PreparedLocalDeviceRegistrationRow, PreparedSnapshotBlob,
    PublishedCircleSnapshot, PublishedStoreSnapshot,
};
pub use prepared_audience_objects::{
    validate_prepared_audience_blob_graph, BlobActivation, MakeRemoteIntentState,
    PreparedAudienceBlob, PreparedAudienceObjects, PreparedAudiencePackage, PreparedRemoteObject,
    StoredBlobReferenceState,
};
pub use routing_contract::SyncRoutingContract;
use schema_contract::validate_host_synced_tables;
pub use schema_contract::DurablePreparedProtocolObject;
pub use schema_contract::{StoreBatchCompletion, StoreBatchLocalCleanup};
pub use schema_introspection::{
    create_table_sql, foreign_key_edges, quote_ident, rewrite_create_into_schema, table_columns,
    CreateTableSchemaError, ForeignKeyEdge, ForeignKeySchemaError,
};
pub use store::device_join_journal;
pub use store::device_join_journal::DeviceJoinJournalError;
pub use store::payload_spool;
pub use store::{
    activated_merge_membership_remote_objects, DeviceJoinBootstrapActivation,
    DeviceJoinBootstrapCommit, DeviceJoinBootstrapPlan, MembershipAuthorityBytes,
    PreparedMergeMaterialization, PreparedMergeMaterializationPackage,
    VerifiedStoreSnapshotStability,
};
pub use store::{
    audience_moves_by_row, local_blob_cleanup_intents, AudienceBlobMoveStaging, PostUpload,
    StagedAudienceBlobRollback,
};
pub use store::{
    copy_table_with_conflicts, install_circle_bootstrap_image_on,
    install_circle_bootstrap_remote_objects_on, projection_table_names,
    verify_circle_bootstrap_image, BlockedWriteDiscard, CandidateCleanupObject, CreatedSnapshot,
    DeviceJoinJournalStore, DurableStoreReclaimObject, DurableStoreReclaimOperation,
    HostWriteBlobTransaction, HostWriteError, HostWriteOperation, IncomingTimestampPolicy,
    LocalBlobCleanup, MaterializedLocalBlob, MergeCandidateAbandonmentPreparation, OutboxEntry,
    OutboxOperation, OutboxUploadState, OwnStreamAuthorship, OwnedVerifiedMergeMaterialization,
    PreparedCircleObjects, ReclaimCommitActivation, ReclaimedStorePackage, RetainedAudiencePackage,
    RetainedMergeMaterializationKey, RetainedPackageApplication, RetainedReplayAuthority,
    RetainedReplayBaseline, RetainedReplayGenesisAuthority, RetainedReplaySnapshotAuthority,
    SnapshotBlobAudience, SnapshotDatabaseImage, SnapshotImageError, SnapshotImageOperationError,
    SnapshotPublicationPermit, StoreDatabase, StoreDatabaseRuntime, StoreReclaimJournalError,
    StoreRowWrites, StoreWritePreparation, TableSchema, ValidatedChangeset,
    VerifiedMergeMaterialization, VerifiedMergeMembershipObjects, WinningRow, GENERATION_ZERO,
};
#[cfg(any(test, feature = "test-utils"))]
pub use store::{resolve_and_apply_changeset, ApplyResult, MergeMaterializationTransaction};
#[cfg(any(test, feature = "test-utils"))]
pub use store::{select_author_exclusion_activation_locator, AuthorExclusionLocatorTamper};
pub use store::{BlobFileFailure, BlobFileFailures, SqlContext, SqlReadContext, WriteBatch};
pub use store::{MakeRemoteProgress, QueuedDelete, QueuedUpload};
pub use store_authority_records::{
    ensure_founder_replay_baseline_on, founder_graph_identity,
    install_generation_zero_replay_baseline_on, install_snapshot_replay_baseline_on,
    install_store_root_authority_on, load_local_store_founder_graph_on,
    load_store_root_authority_on, DurableFounderMembershipJournal,
};
pub use store_authority_records::{
    load_generation_zero_replay_baseline_on, required_store_root_authority_on,
};
pub use store_authority_records::{
    DurableFounderGraph, DurableFounderMembership, FounderMembershipRefs,
};
pub use write_models::{
    AuthorExclusionActivationLocator, BlockedMergeCandidate, CompletePreparedStoreWriteOutcome,
    InitialStoreMembershipAuthority, MergeAbandonmentState, MergeReplayWriteOverlay,
    OutboundStoreAck, OutboundStoreAckActivation, PreparedMergeAbandonmentCandidates,
    PreparedStoreWrite, PreparedStoreWriteCommit, PreparedStoreWritePartitions, PublishedStoreAck,
    StoreWriteBase, StoreWriteBlobFact, StoreWriteBlobFacts, StoreWriteBlobMoveDestination,
    StoreWriteRemoteBlob, StoreWriteRouting, TerminalCandidateAuthority,
    TerminalCandidateCleanupVerification,
};

pub const LOCAL_DEVICE_ID_STATE_KEY: &str = "local_device_id";
const HOST_DEVICE_ID_STATE_KEY: &str = "host_device_id";
pub const SYNC_ROUTING_CONTRACT_STATE_KEY: &str = "sync_routing_contract";
pub const SYNC_ROUTING_HASH_STATE_KEY: &str = "sync_routing_hash";
pub const COVEN_SCHEMA_MANIFEST_STATE_KEY: &str = "coven_schema_manifest";
pub const COVEN_INITIALIZED_STATE_KEY: &str = "coven_initialized";
pub const COVEN_INITIALIZED_STATE_VALUE: &str = "1";
pub const STORE_DEVICE_GENESIS_STATE_KEY: &str = "store_device_genesis_state";
const GATE_BASELINE_SCHEMA: &str = "coven_gate_empty";
const COVEN_CLEANUP_GUARD_PREFIX: &str = "coven_cleanup_guard_";

fn is_coven_cleanup_guard_name(name: &str) -> bool {
    name.get(..COVEN_CLEANUP_GUARD_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(COVEN_CLEANUP_GUARD_PREFIX))
}

thread_local! {
    /// How many Coven-owned write operations are on the current call stack.
    ///
    /// The host-SQL authorizer denies statements that access Coven's reserved
    /// tables, but Coven's own entry points are documented to run inside the
    /// host's write closure (`register_external_blob`, `enqueue_blob_delete`,
    /// `clear_external_blob` all bind to the row version the same write
    /// produced). Those operations announce themselves through this depth so
    /// the authorizer can tell "Coven writing its own bookkeeping" apart from
    /// "host SQL reaching into it" — the statement text is identical; the
    /// caller is not. Thread-local is sound because a write closure and every
    /// statement it executes run synchronously on one thread.
    static COVEN_SQL_AUTHORITY_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Run `f` with Coven's own SQL authority, so the host-SQL authorizer permits
/// the reserved-table writes it performs. Panic-safe: the depth restores when
/// the guard drops.
pub fn with_coven_sql_authority<R>(f: impl FnOnce() -> R) -> R {
    struct DepthGuard;
    impl Drop for DepthGuard {
        fn drop(&mut self) {
            COVEN_SQL_AUTHORITY_DEPTH.with(|depth| depth.set(depth.get() - 1));
        }
    }
    COVEN_SQL_AUTHORITY_DEPTH.with(|depth| depth.set(depth.get() + 1));
    let _guard = DepthGuard;
    f()
}

pub fn authorize_host_sql(
    context: rusqlite::hooks::AuthContext<'_>,
) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};

    if COVEN_SQL_AUTHORITY_DEPTH.with(|depth| depth.get()) > 0 {
        return Authorization::Allow;
    }

    let runs_from_coven_cleanup_guard = context.accessor.is_some_and(is_coven_cleanup_guard_name);
    let mut accesses_coven_table = match context.action {
        AuthAction::Delete { table_name }
        | AuthAction::Insert { table_name }
        | AuthAction::CreateTable { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::CreateVtable { table_name, .. }
        | AuthAction::DropVtable { table_name, .. } => is_reserved_table_name(table_name),
        AuthAction::Update { table_name, .. }
        | AuthAction::Read { table_name, .. }
        | AuthAction::CreateIndex { table_name, .. }
        | AuthAction::DropIndex { table_name, .. }
        | AuthAction::CreateTrigger { table_name, .. }
        | AuthAction::DropTrigger { table_name, .. }
        | AuthAction::AlterTable { table_name, .. } => is_reserved_table_name(table_name),
        _ => false,
    };
    if runs_from_coven_cleanup_guard {
        accesses_coven_table = false;
    }
    let changes_coven_cleanup_guard = match context.action {
        AuthAction::CreateTempTrigger { trigger_name, .. }
        | AuthAction::CreateTrigger { trigger_name, .. }
        | AuthAction::DropTempTrigger { trigger_name, .. }
        | AuthAction::DropTrigger { trigger_name, .. } => is_coven_cleanup_guard_name(trigger_name),
        _ => false,
    };
    if accesses_coven_table
        || changes_coven_cleanup_guard
        || context
            .database_name
            .is_some_and(|name| name.eq_ignore_ascii_case(GATE_BASELINE_SCHEMA))
        || matches!(
            context.action,
            AuthAction::Detach { database_name }
                if database_name.eq_ignore_ascii_case(GATE_BASELINE_SCHEMA)
        )
        || matches!(
            context.action,
            AuthAction::Pragma { pragma_name, .. }
                if pragma_name.eq_ignore_ascii_case("database_list")
        )
    {
        Authorization::Deny
    } else {
        Authorization::Allow
    }
}

/// A staged audience-move blob file that could not be rolled back, and why.
/// Names the file so a host learns which staged bytes are left on disk.
#[derive(Debug)]
pub struct StagedBlobRollbackFailure {
    pub path: PathBuf,
    pub reason: String,
}

impl std::fmt::Display for StagedBlobRollbackFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.path.display(), self.reason)
    }
}

/// Every staged file that could not be rolled back, in the order attempted.
#[derive(Debug)]
pub struct StagedBlobRollbackFailures(pub Vec<StagedBlobRollbackFailure>);

impl std::fmt::Display for StagedBlobRollbackFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, failure) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{failure}")?;
        }
        Ok(())
    }
}

/// An error from the owned database.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Message(String),
    #[error("{0}")]
    Sqlite(#[from] rusqlite::Error),
    /// A JSON column's bytes did not read back as the value they encode, or a
    /// value would not encode. Every synced-protocol column is stored as JSON,
    /// so this is the shape of every column-level decode failure.
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    /// Durable bytes failed the Store protocol's own validation — a hash that
    /// does not match, a signature that does not verify, an object in the wrong
    /// slot. The database read them back intact; the protocol refused them.
    #[error("{0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("{0}")]
    RemoteObject(#[from] coven_protocol::remote_object::RemoteObjectRecordError),
    #[error("{0}")]
    AudiencePackage(#[from] coven_protocol::audience_package::AudiencePackageError),
    #[error("{0}")]
    ObjectHash(#[from] coven_foundation::object_hash::InvalidObjectHash),
    #[error("{0}")]
    BlobLocator(#[from] coven_protocol::blob::locator::BlobLocatorError),
    #[error("{0}")]
    CircleId(#[from] coven_protocol::circle::CircleIdError),
    #[error("{0}")]
    RowRoutingKey(#[from] coven_protocol::circle::RowRoutingKeyError),
    #[error("{0}")]
    Gate(#[from] crate::gate::GateError),
    #[error("{0}")]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error("unsafe blob path: {0}")]
    BlobPath(#[from] coven_foundation::store_dir::PathTokenError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// A stored integer column did not fit the type the schema says it holds.
    #[error("stored value is out of range: {0}")]
    IntRange(#[from] std::num::TryFromIntError),
    #[error("stored value is not an integer: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("stored value is not UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("{0}")]
    BlobDecl(#[from] crate::BlobDeclError),
    #[error("{0}")]
    ChangesetIdentity(#[from] crate::ChangesetIdentityError),
    #[error("{0}")]
    Encryption(#[from] coven_keys::encryption::EncryptionError),
    #[error("{0}")]
    SnapshotImage(#[from] crate::store::SnapshotImageError),
    #[error("{0}")]
    FromSql(#[from] rusqlite::types::FromSqlError),
    #[error("{0}")]
    BlobOpeningAuthority(#[from] coven_protocol::blob::BlobOpeningAuthorityError),
    #[error("{0}")]
    CommitNewFile(#[from] coven_foundation::local_file::CommitNewFileError),
    /// Staging a write's audience-move blobs failed. The implementation of
    /// `AudienceBlobMoveStaging` is injected from above, so its failure is
    /// carried as an opaque source rather than named here.
    #[error("audience blob staging: {0}")]
    AudienceBlobStaging(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Staging a write's audience-move blobs failed AND rolling the staged
    /// files back failed, so those files are left on disk. Carries both
    /// failures rather than reporting one and describing the other.
    #[error("{operation}; audience blob rollback failed: {rollback}")]
    AudienceBlobRollbackFailed {
        operation: Box<DbError>,
        rollback: StagedBlobRollbackFailures,
    },
    #[error("staged audience blob rollback failed: {0}")]
    StagedBlobRollback(StagedBlobRollbackFailures),
    /// An audience move needs its blob materialized locally and it is not —
    /// the row's bytes are absent, stale, or refuse their declared identity.
    /// Names the row so the caller can act on it.
    #[error(
        "blob move requires materialization for {table}/{row_id}/{column} at {row_stamp}: {reason}"
    )]
    BlobMoveRequiresMaterialization {
        table: String,
        row_id: String,
        column: String,
        row_stamp: String,
        reason: Box<DbError>,
    },
    /// A queued outbox entry's `last_attempt_at` is not an RFC 3339 timestamp,
    /// so whether the entry is still inside its retry backoff cannot be
    /// decided. Names the entry so the caller can act on that row.
    #[error("outbox entry {entry_id} has unparseable last_attempt_at {value:?}: {source}")]
    UnparseableOutboxAttemptTime {
        entry_id: i64,
        value: String,
        source: chrono::ParseError,
    },
    /// A [`DbError`] with the operation that produced it named in front of it.
    /// Carries the cause as a [`DbError`] so callers keep matching on it after
    /// it crosses the layer that added the description.
    #[error("{context}: {source}")]
    Context {
        context: String,
        source: Box<DbError>,
    },
    #[error("database error: Store protocol root hash is absent")]
    StoreRootHashMissing,
    /// The local device was excluded from a Circle epoch close and has not yet
    /// reset its projection from the successor bootstrap, so it cannot publish
    /// into the Circle. Stays matchable at the publication boundary rather than
    /// flattening into a message.
    #[error(
        "device excluded from circle {circle_id} close {close_id} must reset before publishing"
    )]
    ExcludedDeviceMustReset {
        circle_id: coven_protocol::circle::CircleId,
        close_id: coven_protocol::circle::CircleEpochCloseId,
    },
}

impl DbError {
    /// Name the operation `source` failed in without flattening it: the cause
    /// stays a [`DbError`] the caller can still match on.
    pub fn context(context: impl Into<String>, source: impl Into<DbError>) -> DbError {
        DbError::Context {
            context: context.into(),
            source: Box::new(source.into()),
        }
    }
}

/// Run `sql`, map every row through `mapper`, and collect the results.
///
/// It returns the SQLite failure as it happened, so every caller's `?` converts
/// it into whatever error that caller already returns — one helper, no error
/// vocabulary of its own.
pub fn query_mapped_rows<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    mut mapper: F,
) -> Result<Vec<T>, rusqlite::Error>
where
    P: rusqlite::Params,
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    let mut statement = conn.prepare(sql)?;
    let rows = statement.query_map(params, |row| mapper(row))?;
    let mut mapped = Vec::new();
    for row in rows {
        mapped.push(row?);
    }
    Ok(mapped)
}

/// Why opening the database failed. Splits a migration-ladder failure from every
/// other open-time database error so the [`MigrationError`] a host acts on —
/// [`MigrationError::SchemaTooNew`], whose remedy is "update the app" — stays
/// matchable at the open boundary instead of being flattened into a
/// [`DbError`] string.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error(transparent)]
    Migration(#[from] MigrationError),
    #[error(transparent)]
    Db(#[from] DbError),
}

#[derive(Clone)]
struct DatabaseState {
    /// The directory holding this database, and with it the payload spool the
    /// rows name. Bound at open because a row and the file it names are one
    /// store's state, so nothing downstream gets to pick a different directory.
    store_dir: coven_foundation::store_dir::StoreDir,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    /// The host's blob-tombstone convergence window, read by the tombstone GC to
    /// age each tombstone's `deleted_at`. Host config carried here alongside
    /// `synced_tables` so the sync layer reads it from the one owner rather than
    /// threading a separately-passed copy that could diverge.
    blob_tombstone_grace: chrono::Duration,
    /// How many blob transfers each transfer loop runs at once, read by the upload
    /// drain and the pin loop (both hold `&Database`). Open-time host config carried
    /// here for the same single-owner reason as `blob_tombstone_grace`.
    transfer_limits: coven_protocol::blob::TransferLimits,
    store_runtime: crate::StoreDatabaseRuntime,
    ids: coven_foundation::id_provider::IdRef,
    write_statuses:
        Arc<std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>>,
    #[cfg(any(test, feature = "test-utils"))]
    test_pause_points: Arc<TestPausePoints<DatabaseTestPoint>>,
    #[cfg(any(test, feature = "test-utils"))]
    merge_materialization_failure: Arc<std::sync::Mutex<Option<MergeMaterializationFailurePoint>>>,
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone)]
pub struct StoreDatabaseTestAccess {
    pause_points: Arc<TestPausePoints<DatabaseTestPoint>>,
    merge_materialization_failure: Arc<std::sync::Mutex<Option<MergeMaterializationFailurePoint>>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl StoreDatabaseTestAccess {
    pub fn arm(
        &self,
        point: DatabaseTestPoint,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        self.pause_points.arm(point)
    }

    pub async fn reach(&self, point: DatabaseTestPoint) {
        self.pause_points.reach(point).await;
    }

    pub fn merge_materialization_failure_injection(&self) -> MergeMaterializationFailureInjection {
        MergeMaterializationFailureInjection {
            armed: self.merge_materialization_failure.clone(),
        }
    }
}

/// Test-only checkpoints reached by database operations whose ordering matters.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseTestPoint {
    LocalBlobCleanupRequested,
    LocalBlobCleanupAcquired,
    LocalBlobCleanupBeforeFilesystem {
        namespace: String,
        blob_id: String,
    },
    LocalBlobCleanupFinished,
    PullAfterRemoteCommit {
        device_id: String,
        seq: u64,
    },
    StoreWriteCommitUploaded {
        write_id: WriteId,
    },
    StoreWriteHeadReadBack {
        write_id: WriteId,
    },
    StoreDeviceExclusionCandidateStaged,
    /// The owner's device-join acceptance has read the position its attempt
    /// will be bound to and holds the turn to author it, but has not yet
    /// published the head that takes it.
    DeviceJoinAttemptPositionHeld,
}

#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMaterializationFailurePoint {
    SummaryMaterialization,
    RetractionDeletion,
    ProjectionReplacement,
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone)]
pub struct MergeMaterializationFailureInjection {
    armed: Arc<std::sync::Mutex<Option<MergeMaterializationFailurePoint>>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl MergeMaterializationFailureInjection {
    pub fn reach(&self, point: MergeMaterializationFailurePoint) -> Result<bool, DbError> {
        let mut armed = self.armed.lock().map_err(|_| {
            DbError::Message("Merge materialization failure lock poisoned".to_string())
        })?;
        if armed.as_ref() != Some(&point) {
            return Ok(false);
        }
        armed.take();
        Ok(true)
    }
}

#[cfg(any(test, feature = "test-utils"))]
struct ArmedTestPause<K> {
    point: K,
    reached: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}

#[cfg(any(test, feature = "test-utils"))]
struct TestPauseState<K> {
    armed: Option<ArmedTestPause<K>>,
    observers: Vec<tokio::sync::mpsc::UnboundedSender<K>>,
}

#[cfg(any(test, feature = "test-utils"))]
struct TestPausePoints<K> {
    state: std::sync::Mutex<TestPauseState<K>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl<K> Default for TestPausePoints<K> {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(TestPauseState {
                armed: None,
                observers: Vec::new(),
            }),
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<K: Clone + PartialEq> TestPausePoints<K> {
    fn arm(&self, point: K) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let prior = self
            .state
            .lock()
            .expect("database test pause mutex poisoned")
            .armed
            .replace(ArmedTestPause {
                point,
                reached: reached.clone(),
                resume: resume.clone(),
            });
        assert!(prior.is_none(), "database test pause already armed");
        (reached, resume)
    }

    fn observe(&self) -> tokio::sync::mpsc::UnboundedReceiver<K> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.state
            .lock()
            .expect("database test pause mutex poisoned")
            .observers
            .push(sender);
        receiver
    }

    async fn reach(&self, point: K) {
        let pause = {
            let mut state = self
                .state
                .lock()
                .expect("database test pause mutex poisoned");
            state
                .observers
                .retain(|observer| observer.send(point.clone()).is_ok());
            if state
                .armed
                .as_ref()
                .is_some_and(|pause| pause.point == point)
            {
                state.armed.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
    }
}

/// The owned SQLite connection and the sync bookkeeping resolved beside it at
/// open. One connection thread owns this for the connection's whole life; every
/// database access runs against it, so access is serialized. Changeset capture is
/// per-transaction: the host-write transaction retains one attached session for
/// its full span and drains it into the existing write records, so no capture
/// state lives on the core between calls.
///
/// `DatabaseCore` holds only `Send` fields (a `rusqlite::Connection`, which is
/// `Send`, plus `Arc`s and a `u32`), so it is `Send` by construction — the
/// connection thread receives it by value across a single `thread::spawn` with no
/// manual `unsafe impl`.
struct DatabaseCore {
    store_dir: coven_foundation::store_dir::StoreDir,
    conn: Connection,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
    transfer_limits: coven_protocol::blob::TransferLimits,
}

/// One Circle's staged restore outcome, decided by selection against the
/// restoring identity's re-resolved access. `Install` carries a verified image to
/// project and record coverage for; `ClearCoverage` names a Circle the identity
/// cannot decrypt, whose preserved coverage row must be deleted so replay never
/// reconstructs an image the identity has no access to.
pub enum StagedCircleDecision {
    Install {
        activation_commit: StoreBatchCommitRef,
        image: coven_protocol::circle_activation::VerifiedCircleImage,
    },
    ClearCoverage(coven_protocol::circle::CircleId),
}

pub struct VerifiedSnapshotBootstrapInstall {
    snapshot: PublishedStoreSnapshot,
    store_root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
    founder: coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
    stability: RetainedReplaySnapshotAuthority,
    membership: InitialStoreMembershipAuthority,
    routing_key: Option<coven_protocol::circle::RowRoutingKey>,
    circle_decisions: Vec<StagedCircleDecision>,
    /// Fail the Circle-decision step of the install transaction, after the Store
    /// image has been installed within it — a test's stand-in for a crash between
    /// the Store and Circle installs, exercising the single-transaction rollback.
    #[cfg(any(test, feature = "test-utils"))]
    fail_circle_install: bool,
}

impl VerifiedSnapshotBootstrapInstall {
    pub fn new(
        snapshot: PublishedStoreSnapshot,
        store_root: coven_protocol::objects::VerifiedObject<StoreProtocolRoot>,
        founder: coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
        stability: crate::VerifiedStoreSnapshotStability,
        membership: InitialStoreMembershipAuthority,
        routing_encryption: Option<&EncryptionService>,
        circle_decisions: Vec<StagedCircleDecision>,
    ) -> Result<Self, DbError> {
        if store_root.value.to_bytes() != store_root.bytes
            || store_root.value.object_hash() != store_root.semantic_hash
        {
            return Err(DbError::Message(
                "bootstrap Store root differs from its verified object".to_string(),
            ));
        }
        let root = coven_protocol::store_commit::StoreRootRef {
            store_root_id: store_root.value.descriptor.store_root_id(),
            store_root_hash: store_root.semantic_hash,
            object: store_root.object.clone(),
        };
        let founder_reference =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        if founder.semantic_hash != founder_reference.registration_hash {
            return Err(DbError::Message(
                "bootstrap founder semantic hash differs from its exact registration".to_string(),
            ));
        }
        let stability = stability.into_authority();
        stability.validate()?;
        if stability.store_root != root
            || stability.founder_registration != founder_reference
            || stability.snapshot != snapshot.reference
            || stability.metadata != snapshot.meta
            || snapshot.meta.successor.next_slot != snapshot.successor_slot
        {
            return Err(DbError::Message(
                "bootstrap snapshot differs from its verified stability authority".to_string(),
            ));
        }
        let routing_key = routing_encryption
            .map(|encryption| {
                coven_protocol::circle::derive_row_routing_key(encryption, root.store_root_hash)
                    .map_err(|error| DbError::context("derive bootstrap row-routing key", error))
            })
            .transpose()?;
        Ok(Self {
            snapshot,
            store_root,
            founder,
            stability,
            membership,
            routing_key,
            circle_decisions,
            #[cfg(any(test, feature = "test-utils"))]
            fail_circle_install: false,
        })
    }

    /// Attach the Circle install/clear decisions selected against a throwaway
    /// query copy opened through this same authority. Kept separate from `new` so
    /// one verified install can first query (with no decisions) and then install
    /// for real (with them), without re-verifying the Store authority.
    pub fn with_circle_decisions(mut self, circle_decisions: Vec<StagedCircleDecision>) -> Self {
        self.circle_decisions = circle_decisions;
        self
    }

    fn install_on(
        &self,
        records: crate::payload_spool::StoreRecords<'_>,
        schema_version: u32,
        routing_hash: ObjectHash,
        synced_tables: &[SyncedTable],
    ) -> Result<(), DbError> {
        let conn = records.conn();
        let root = coven_protocol::store_commit::StoreRootRef {
            store_root_id: self.store_root.value.descriptor.store_root_id(),
            store_root_hash: self.store_root.semantic_hash,
            object: self.store_root.object.clone(),
        };
        let founder_reference = StoreDeviceRegistrationRef::from_registration(
            &self.founder.value,
            self.founder.object.clone(),
        );
        let genesis = ResolvedStoreDeviceState::founder(
            &root,
            founder_reference.clone(),
            &self.store_root.value.descriptor.founder_pubkey,
            self.store_root.value.descriptor.founder_grant.clone(),
            &self.store_root.value.descriptor.founder_recovery,
        )
        .map_err(|error| DbError::Message(error.to_string()))?;
        validate_snapshot_object_owners_on(conn, &root, &self.snapshot.meta)?;
        install_store_root_authority_on(conn, &root, &self.store_root.bytes)?;
        install_store_founder_state_on(
            conn,
            &root,
            &founder_reference,
            &self.founder.value,
            &self.founder.bytes,
            &genesis,
        )?;
        crate::set_protocol_state_on(
            conn,
            coven_protocol::membership::OWNER_PUBKEY_STATE_KEY,
            &self.store_root.value.descriptor.founder_pubkey,
        )?;
        self.membership.install_on(conn)?;
        conn.execute("DELETE FROM snapshot_coverage", [])
            .map_err(DbError::from)?;
        for (stream_id, reference) in self.snapshot.meta.coverage.clone().into_refs() {
            let encoded = serde_json::to_string(&reference)
                .map_err(|error| DbError::context("serialize snapshot exact commit ref", error))?;
            conn.execute(
                "INSERT INTO snapshot_coverage
                 (device_id, seq, commit_ref, snapshot_hash) VALUES (?1, ?2, ?3, ?4)",
                (
                    &stream_id,
                    Database::sequence_to_sqlite(&stream_id, reference.coord.sequence())?,
                    encoded,
                    self.snapshot.reference.snapshot_hash.to_string(),
                ),
            )
            .map_err(DbError::from)?;
        }
        install_snapshot_replay_baseline_on(
            records,
            schema_version,
            routing_hash,
            self.stability.clone(),
        )?;
        self.install_circle_decisions_on(records, &root, synced_tables)
    }

    /// Apply the staged Circle decisions inside the Store install's single
    /// transaction: each `Install` projects the verified image and records its
    /// coverage row (accepting a strictly newer cut, refusing a regression); each
    /// `ClearCoverage` deletes the preserved row for a Circle the restoring
    /// identity cannot decrypt. The whole set commits or rolls back with the Store
    /// image — a partially installed union is never exposed.
    fn install_circle_decisions_on(
        &self,
        records: crate::payload_spool::StoreRecords<'_>,
        root: &coven_protocol::store_commit::StoreRootRef,
        synced_tables: &[SyncedTable],
    ) -> Result<(), DbError> {
        use crate::StoreDatabase;
        let conn = records.conn();
        #[cfg(any(test, feature = "test-utils"))]
        if self.fail_circle_install {
            return Err(DbError::Message(
                "injected Circle install failure after Store install".to_string(),
            ));
        }
        for decision in &self.circle_decisions {
            match decision {
                StagedCircleDecision::Install {
                    activation_commit,
                    image,
                } => {
                    let activation = StoreDatabase::verified_circle_activation_on(
                        records,
                        root,
                        image.circle_id(),
                        image.control(),
                    )?
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "restored Circle {} image names a control absent from the installed \
                             control indexes",
                            image.circle_id()
                        ))
                    })?;
                    crate::install_circle_bootstrap_image_on(
                        conn,
                        synced_tables,
                        activation_commit,
                        image,
                    )?;
                    StoreDatabase::record_one_circle_bootstrap_coverage_on(
                        records,
                        root,
                        activation_commit,
                        image,
                        &activation.control,
                    )?;
                }
                StagedCircleDecision::ClearCoverage(circle_id) => {
                    StoreDatabase::clear_circle_bootstrap_coverage_on(conn, *circle_id)?;
                }
            }
        }
        Ok(())
    }

    /// Arm the Circle-install failure injection: the install transaction rolls
    /// back after the Store image is installed but before any Circle decision
    /// commits, standing in for a crash between the two installs.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_circle_install_for_test(mut self) -> Self {
        self.fail_circle_install = true;
        self
    }
}

/// A handle to the owned database. Cloneable; every clone sends work to the one
/// connection thread over the same channel, so access serializes as the channel's
/// FIFO.
#[derive(Clone)]
pub struct Database {
    connection: DatabaseConnection,
    state: DatabaseState,
}

#[cfg(test)]
mod tests;
