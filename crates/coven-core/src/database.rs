//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` together with the sync bookkeeping
//! beside it. Every database access — the host's app SQL, coven's bookkeeping,
//! changeset capture and apply — runs against that one connection, so access is
//! serialized.
//!
//! Hosts open coven with [`crate::Coven::builder`] and run app SQL through
//! [`crate::CovenHandle::sql`] or [`crate::CovenHandle::write`].

pub(crate) use crate::database::blob_records::{
    load_activated_registration_on, remote_audience_to_db,
};
pub(crate) use crate::database::cloud_outbox_records::consume_created_upload_handoff_on;
use crate::database::connection_io::open_connection;
use crate::database::connection_io::open_connection_read_only;
use crate::database::connection_io::scan_max_updated_at;
use crate::database::connection_io::seed_from;
use crate::database::local_store_identity::pin_host_device_id_on;
use crate::database::local_store_identity::validate_host_device_id_on;
pub(crate) use crate::database::remote_object_records::begin_remote_candidate_nonactivation_on;
pub(crate) use crate::database::remote_object_records::begin_remote_candidate_nonactivation_with_verified_head_on;
pub(crate) use crate::database::remote_object_records::candidate_graph_exact_objects;
pub(crate) use crate::database::remote_object_records::finish_remote_candidate_nonactivation_on;
pub(crate) use crate::database::remote_object_records::index_retained_replay_owner_on;
pub(crate) use crate::database::remote_object_records::load_protocol_inert_object_on;
pub(crate) use crate::database::remote_object_records::load_remote_object_on;
pub(crate) use crate::database::remote_object_records::mark_remote_object_uploaded_on;
pub(crate) use crate::database::remote_object_records::mark_reusable_retained_authority_uploaded_on;
pub(crate) use crate::database::remote_object_records::merge_prepared_remote_object;
pub(crate) use crate::database::remote_object_records::persist_exact_remote_object_on;
pub(crate) use crate::database::remote_object_records::record_reclaimed_store_package_on;
pub(crate) use crate::database::remote_object_records::replace_prepared_merge_head_remote_on;
pub(crate) use crate::database::remote_object_records::update_remote_object_on;
pub(crate) use crate::database::remote_object_records::{
    validate_prepared_blob_on, validate_prepared_package_on, validate_remote_object_on,
    RemoteStoredRepresentationRef,
};
use crate::database::snapshot_objects::validate_snapshot_object_owners_on;
pub(crate) use crate::database::snapshot_objects::{
    install_snapshot_blob_plan_on, validate_snapshot_blob_plan_on,
};
pub(crate) use crate::database::snapshot_records::{
    load_outbound_store_snapshot_on, load_published_store_snapshot_on,
};
pub(crate) use crate::database::store_ack_records::{
    finish_outbound_store_ack_on, load_outbound_store_ack_on, load_published_store_ack_on,
    record_activated_store_ack_on, store_snapshot_first_slot, verify_next_local_store_ack_on,
};
pub(crate) use crate::database::store_authority_records::install_store_founder_state_on;
pub(crate) use crate::database::store_reclaim_records::{
    insert_store_reclaim_operation_on, load_store_reclaim_operation_on,
    parse_store_reclaim_operation, record_store_reclaim_activation_on, store_reclaim_journal_error,
    update_store_reclaim_operation_on,
};
pub(crate) use crate::database::stream_activation_records::load_registered_stream_activation_on;
pub(crate) use crate::database::stream_activation_records::record_verified_stream_activations_on;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tracing::error;

use crate::blob::decl::BlobDecls;
use crate::blob::locator::{BlobLocator, RemoteAudience, StoredBlobRef};
use crate::blob::{BlobRef, RowBlobAuthority, RowBlobRef};
use crate::db::{
    apply_coven_schema, expected_coven_schema_manifest, is_reserved_table_name,
    live_coven_schema_manifest, CovenSchemaManifest, ExternalBlob, OutboxEntry, OutboxOperation,
    OutboxUploadState,
};
use crate::encryption::EncryptionService;
use crate::migration::{run_migrations_in_transaction, Migration, MigrationError};
use crate::sync::audience_package::{AudiencePackage, RowBlobLocatorBinding};
use crate::sync::circle::Audience;
use crate::sync::gate::{self, Gates};
use crate::sync::hlc::{Hlc, Timestamp, UpdatedAtStamper, HIGHWATER_STATE_KEY, MAX_FUTURE_SKEW_MS};
use crate::sync::membership::{AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef};
use crate::sync::remote_object::{
    remote_object_id, CandidateExclusiveObjectDomain, RemoteObjectRecord, RetainedReplayOwner,
    SharedLiveSetObjectDomain,
};
use crate::sync::routing_contract::SyncRoutingContract;
use crate::sync::session::{quote_ident, SyncedTable};
use crate::sync::storage::{ExactObjectRef, PreparedExactObject};
use crate::sync::store::circle_controls::activation::VerifiedStreamActivations;
use crate::sync::store::retained_replay::{
    RetainedReplayAuthority, RetainedReplayBaseline, RetainedReplayGenesisAuthority,
    RetainedReplaySnapshotAuthority,
};
use crate::sync::store::{
    DurableStoreReclaimOperation, ReclaimCommitActivation, ReclaimedStorePackage,
    StoreReclaimJournalError,
};
use crate::sync::store_commit::{
    ack_slot_prefix, CommitFrontier, ObjectHash, ResolvedStoreDeviceState, SnapshotImageRef,
    SnapshotMeta, StoreAck, StoreAckRef, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreProtocolRoot,
    StoreSnapshotRef, StreamActivationId,
};
use crate::write::{PendingWrite, WriteId, WriteStatus};

mod blob_bindings;
mod blob_records;
mod circle_operation_records;
mod cloud_outbox;
mod cloud_outbox_records;
mod connection_io;
pub(crate) use connection_io::{attach_session, capture_changeset};
mod database_open;
mod database_runtime;
mod local_state;
mod local_store_identity;
mod make_remote;
mod operation_models;
mod prepared_audience_objects;
mod provider_probes;
mod remote_object_records;
mod schema_contract;
mod snapshot_objects;
mod snapshot_records;
mod store_ack_records;
mod store_authority_records;
mod store_coordinates;
mod store_reclaim_records;
mod stream_activation_records;
mod write_lifecycle;
mod write_models;

pub(crate) use crate::sync::store::database::candidate_records::CandidateCleanupObject;
pub(crate) use crate::sync::store::database::materialization_models::{
    OwnedVerifiedMergeMaterialization, RetainedMergeMaterializationKey, RetainedPackageApplication,
    VerifiedMergeMaterialization, VerifiedMergeMembershipObjects,
};
pub(crate) use blob_records::{load_prepared_audience_objects_on, previous_row_blob_for_write_on};
pub(crate) use circle_operation_records::{
    load_circle_operation_on, parse_circle_operation_row, PreparedCircleOperationRow,
};
use database_open::{run_connection_thread, ConnectionThread, CovenMetadataOpen, DbJob};
pub(crate) use local_store_identity::local_merge_stream_id_on;
pub(crate) use local_store_identity::{
    local_activated_registration_ref_on, local_store_authority_on,
};
pub(crate) use operation_models::{
    DurableDeviceRegistration, DurableMembershipMutation, DurableSnapshotPublication,
    LocalDeviceRegistrationJournalRow, LocalDeviceRegistrationState, MembershipMutationActivation,
    PreparedLocalDeviceRegistrationRow, PreparedSnapshotBlob, PublishedStoreSnapshot,
};
#[cfg(feature = "invariant-tests")]
pub use prepared_audience_objects::exercise_exact_outbound_blob_graph;
pub(crate) use prepared_audience_objects::{
    validate_prepared_audience_blob_graph, BlobActivation, MakeRemoteIntentState,
    PreparedAudienceBlob, PreparedAudienceObjects, PreparedAudiencePackage, PreparedRemoteObject,
    StoredBlobReferenceState,
};
use schema_contract::validate_host_synced_tables;
pub(crate) use schema_contract::DurablePreparedProtocolObject;
pub(crate) use schema_contract::{StoreBatchCompletion, StoreBatchLocalCleanup};
pub(crate) use store_authority_records::{
    ensure_founder_replay_baseline_on, founder_graph_identity,
    install_generation_zero_replay_baseline_on, install_snapshot_replay_baseline_on,
    install_store_root_authority_on, load_local_store_founder_graph_on,
    load_store_root_authority_on, validate_founder_graph, DurableFounderMembershipJournal,
};
pub(crate) use store_authority_records::{
    load_generation_zero_replay_baseline_on, required_store_root_authority_on,
};
pub(crate) use store_authority_records::{
    DurableFounderGraph, DurableFounderMembership, FounderMembershipRefs,
};
pub(crate) use write_models::{
    AuthorExclusionActivationLocator, BlockedMergeCandidate, CompletePreparedStoreWriteOutcome,
    ExactProtocolObject, InitialStoreMembershipAuthority, MergeAbandonmentState,
    MergeReplayWriteOverlay, OutboundStoreAck, OutboundStoreAckActivation,
    PreparedMergeAbandonmentCandidates, PreparedProtocolObject, PreparedStoreWrite,
    PreparedStoreWriteCommit, PreparedStoreWritePartitions, PublishedStoreAck, StoreWriteBase,
    StoreWriteBlobFact, StoreWriteBlobFacts, StoreWriteRemoteBlob, StoreWriteRouting,
    TerminalCandidateAuthority, TerminalCandidateCleanupVerification,
};

pub const LOCAL_DEVICE_ID_STATE_KEY: &str = "local_device_id";
const HOST_DEVICE_ID_STATE_KEY: &str = "host_device_id";
pub(crate) const SYNC_ROUTING_CONTRACT_STATE_KEY: &str = "sync_routing_contract";
pub const SYNC_ROUTING_HASH_STATE_KEY: &str = "sync_routing_hash";
pub(crate) const COVEN_SCHEMA_MANIFEST_STATE_KEY: &str = "coven_schema_manifest";
pub(crate) const COVEN_INITIALIZED_STATE_KEY: &str = "coven_initialized";
pub(crate) const COVEN_INITIALIZED_STATE_VALUE: &str = "1";
pub(crate) const STORE_DEVICE_GENESIS_STATE_KEY: &str = "store_device_genesis_state";
const GATE_BASELINE_SCHEMA: &str = "coven_gate_empty";

pub(crate) fn authorize_host_sql(
    context: rusqlite::hooks::AuthContext<'_>,
) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};

    if context
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

/// An error from the owned database.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Message(String),
    #[error("database error: Store protocol root hash is absent")]
    StoreRootHashMissing,
}

impl DbError {
    pub(crate) fn into_message(self) -> String {
        match self {
            Self::Message(message) => message,
            Self::StoreRootHashMissing => "Store protocol root hash is absent".to_string(),
        }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::Message(e.to_string())
    }
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
    transfer_limits: crate::blob::TransferLimits,
    store_runtime: crate::sync::store::database::StoreDatabaseRuntime,
    /// Serializes the full durable-intent to filesystem-deletion to intent-removal
    /// operation across every clone of this database.
    local_blob_cleanup: Arc<tokio::sync::Mutex<()>>,
    ids: crate::id_provider::IdRef,
    write_statuses:
        Arc<std::sync::Mutex<HashMap<WriteId, tokio::sync::watch::Sender<WriteStatus>>>>,
    #[cfg(any(test, feature = "test-utils"))]
    test_pause_points: Arc<TestPausePoints<DatabaseTestPoint>>,
    #[cfg(any(test, feature = "test-utils"))]
    merge_materialization_failure: Arc<std::sync::Mutex<Option<MergeMaterializationFailurePoint>>>,
}

/// Test-only checkpoints reached by database operations whose ordering matters.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseTestPoint {
    LocalBlobCleanupRequested,
    LocalBlobCleanupAcquired,
    LocalBlobCleanupBeforeFilesystem { namespace: String, blob_id: String },
    LocalBlobCleanupFinished,
    PullAfterRemoteCommit { device_id: String, seq: u64 },
    StoreWriteCommitUploaded { write_id: WriteId },
    StoreWriteHeadReadBack { write_id: WriteId },
    StoreDeviceExclusionCandidateStaged,
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
pub(crate) struct MergeMaterializationFailureInjection {
    armed: Arc<std::sync::Mutex<Option<MergeMaterializationFailurePoint>>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl MergeMaterializationFailureInjection {
    pub(crate) fn reach(&self, point: MergeMaterializationFailurePoint) -> Result<bool, DbError> {
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
/// per-transaction — [`Database::run_store_write_transaction_on`] attaches a
/// session for the span of one host write and drains it into the existing write records —
/// so no capture state lives on the core between calls.
///
/// `DatabaseCore` holds only `Send` fields (a `rusqlite::Connection`, which is
/// `Send`, plus `Arc`s and a `u32`), so it is `Send` by construction — the
/// connection thread receives it by value across a single `thread::spawn` with no
/// manual `unsafe impl`.
struct DatabaseCore {
    conn: Connection,
    hlc: Arc<Hlc>,
    synced_tables: Arc<Vec<SyncedTable>>,
    schema_version: u32,
    sync_routing_hash: ObjectHash,
    gates: Arc<Gates>,
    blob_decls: Arc<BlobDecls>,
    blob_tombstone_grace: chrono::Duration,
    transfer_limits: crate::blob::TransferLimits,
}

pub(crate) struct VerifiedSnapshotBootstrapInstall {
    snapshot: PublishedStoreSnapshot,
    store_root: crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
    founder: crate::sync::store_objects::VerifiedObject<StoreDeviceRegistration>,
    stability: RetainedReplaySnapshotAuthority,
}

impl VerifiedSnapshotBootstrapInstall {
    pub(crate) fn new(
        snapshot: PublishedStoreSnapshot,
        store_root: crate::sync::store_objects::VerifiedObject<StoreProtocolRoot>,
        founder: crate::sync::store_objects::VerifiedObject<StoreDeviceRegistration>,
        stability: crate::sync::store::VerifiedStoreSnapshotStability,
    ) -> Result<Self, DbError> {
        if store_root.value.to_bytes() != store_root.bytes
            || store_root.value.object_hash() != store_root.semantic_hash
        {
            return Err(DbError::Message(
                "bootstrap Store root differs from its verified object".to_string(),
            ));
        }
        let root = crate::sync::store_commit::StoreRootRef {
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
        Ok(Self {
            snapshot,
            store_root,
            founder,
            stability,
        })
    }

    fn install_on(
        &self,
        conn: &Connection,
        schema_version: u32,
        routing_hash: ObjectHash,
    ) -> Result<(), DbError> {
        let root = crate::sync::store_commit::StoreRootRef {
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
        conn.execute("DELETE FROM snapshot_coverage", [])
            .map_err(DbError::from)?;
        for (stream_id, reference) in self.snapshot.meta.coverage.clone().into_refs() {
            let encoded = serde_json::to_string(&reference).map_err(|error| {
                DbError::Message(format!("serialize snapshot exact commit ref: {error}"))
            })?;
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
            conn,
            schema_version,
            routing_hash,
            self.stability.clone(),
        )
    }
}

/// A handle to the owned database. Cloneable; every clone sends work to the one
/// connection thread over the same channel, so access serializes as the channel's
/// FIFO.
#[derive(Clone)]
pub struct Database {
    thread: Arc<ConnectionThread>,
    state: DatabaseState,
}

#[cfg(test)]
#[path = "database/tests.rs"]
mod tests;
