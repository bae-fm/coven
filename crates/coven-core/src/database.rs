//! The owned SQLite connection.
//!
//! coven owns one `rusqlite::Connection` together with the sync bookkeeping
//! beside it. Every database access — the host's app SQL, coven's bookkeeping,
//! changeset capture and apply — runs against that one connection, so access is
//! serialized.
//!
//! Hosts open coven with [`crate::Coven::builder`] and run app SQL through
//! [`crate::CovenHandle::sql`] or [`crate::CovenHandle::write`].

pub(crate) use crate::database::blob_records::load_activated_registration_on;
use crate::database::connection_io::open_connection;
use crate::database::connection_io::open_connection_read_only;
use crate::database::connection_io::scan_max_updated_at;
use crate::database::connection_io::seed_from;
use crate::database::local_store_identity::local_activated_registration_ref_on;
use crate::database::local_store_identity::pin_host_device_id_on;
use crate::database::local_store_identity::validate_host_device_id_on;
pub(crate) use crate::database::remote_object_records::begin_remote_candidate_nonactivation_on;
use crate::database::remote_object_records::begin_remote_candidate_nonactivation_with_verified_head_on;
pub(crate) use crate::database::remote_object_records::candidate_graph_exact_objects;
pub(crate) use crate::database::remote_object_records::load_protocol_inert_object_on;
pub(crate) use crate::database::remote_object_records::load_remote_object_on;
pub(crate) use crate::database::remote_object_records::persist_exact_remote_object_on;
pub(crate) use crate::database::remote_object_records::replace_prepared_merge_head_remote_on;
pub(crate) use crate::database::remote_object_records::update_remote_object_on;
use crate::database::snapshot_objects::validate_snapshot_object_owners_on;
pub(crate) use crate::database::store_device_state::store_serial_predecessor_on;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension};
use tracing::error;
use tracing::warn;

use crate::blob::decl::BlobDecls;
use crate::blob::locator::{BlobLocator, RemoteAudience, StoredBlobRef};
use crate::blob::{BlobRef, Provenance, RowBlobAuthority, RowBlobRef};
use crate::db::{
    apply_coven_schema, expected_coven_schema_manifest, is_reserved_table_name,
    live_coven_schema_manifest, CovenSchemaManifest, ExternalBlob, OutboxEntry, OutboxOperation,
    OutboxUploadState,
};
use crate::encryption::EncryptionService;
use crate::migration::{run_migrations_in_transaction, Migration, MigrationError};
use crate::sync::audience_package::{AudiencePackage, RowBlobLocatorBinding};
use crate::sync::circle::Audience;
use crate::sync::circle_activation::{VerifiedCircleActivations, VerifiedStreamActivations};
use crate::sync::gate::{self, Gates};
use crate::sync::hlc::{Hlc, Timestamp, UpdatedAtStamper, HIGHWATER_STATE_KEY, MAX_FUTURE_SKEW_MS};
use crate::sync::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef, SerialAuthorizationState,
    SerialMembershipState, StoreMembershipConflictResolutionRef,
};
use crate::sync::provider::ProviderAdminState;
use crate::sync::remote_object::{
    remote_object_id, CandidateExclusiveObjectDomain, RemoteObjectRecord, RetainedReplayOwner,
    SharedLiveSetObjectDomain,
};
use crate::sync::retained_replay::{
    RetainedReplayAuthority, RetainedReplayBaseline, RetainedReplayGenesisAuthority,
    RetainedReplaySnapshotAuthority, GENERATION_ZERO,
};
use crate::sync::routing_contract::SyncRoutingContract;
use crate::sync::session::{quote_ident, SyncedTable};
use crate::sync::storage::{ExactObjectRef, PreparedExactObject, VersionedObject};
use crate::sync::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, snapshot_image_semantic_prefix, snapshot_slot_prefix,
    CirclePackageRef, CommitFrontier, ObjectHash, ResolvedStoreDeviceState,
    RetainedStoreDeviceOperations, RetainedStoreDeviceRegistrationActivations, SnapshotImageRef,
    SnapshotMeta, StoreAck, StoreAckRef, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceExclusionProposalId, StoreDeviceHead, StoreDeviceProposalAck,
    StoreDeviceProposalState, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreDeviceStateRef, StoreHistoryCut, StorePackageRef, StoreProtocolRoot, StoreSerialHead,
    StoreSerialHeadState, StoreSerialPredecessor, StoreSnapshotRef, StreamActivationId,
    VerifiedStoreDeviceOperations, SERIAL_STREAM_ID,
};
use crate::sync::store_device_exclusion::{
    DurableStoreDeviceExclusionOperation, StoreDeviceExclusionCompletion,
    StoreDeviceExclusionJournalError,
};
use crate::sync::store_reclaim_journal::{
    DurableStoreReclaimOperation, ReclaimCommitActivation, ReclaimedStorePackage,
    StoreReclaimJournalError,
};
use crate::write::{
    AffectedRow, PendingBranch, PendingBranchId, PendingWrite, PublishedPosition, WriteId,
    WriteReceipt, WriteResolution, WriteStatus,
};
use crate::WritePolicy;

mod blob_bindings;
mod blob_records;
mod circle_operation_records;
mod circle_operations;
mod cloud_outbox;
mod cloud_outbox_records;
mod commit_materialization;
mod connection_io;
mod database_open;
mod database_runtime;
mod device_continuation;
mod device_join_challenges;
mod device_registration_journal;
mod host_write_capture;
mod local_state;
mod local_store_identity;
mod make_remote;
mod materialized_commit_index;
mod membership_mutations;
mod merge_candidate_lifecycle;
mod merge_candidate_records;
mod operation_models;
mod owner_promotions;
mod prepared_audience_objects;
mod prepared_remote_objects;
mod provider_probes;
mod remote_object_records;
mod retained_merge_replay;
mod schema_contract;
mod serial_authorization;
mod snapshot_objects;
mod snapshot_publication;
mod snapshot_records;
mod store_ack_records;
mod store_acknowledgements;
mod store_authority;
mod store_authority_records;
mod store_coordinates;
mod store_creation_attempts;
mod store_device_exclusion_records;
mod store_device_exclusions;
mod store_device_state;
mod store_reclaim_records;
mod store_reclamation;
mod store_write_preparation;
mod store_write_publication;
mod stream_activation_records;
mod write_lifecycle;
mod write_models;
mod write_publication_records;

pub(crate) use blob_records::previous_row_blob_for_write_on;
use circle_operation_records::*;
use database_open::{run_connection_thread, ConnectionThread, CovenMetadataOpen, DbJob};
pub(crate) use merge_candidate_records::parse_prepared_serial_candidate;
use merge_candidate_records::*;
pub(crate) use operation_models::{
    DurableDeviceRegistration, DurableMembershipMutation, DurableSnapshotPublication,
    LocalDeviceRegistrationState, MembershipMutationActivation, PreparedSnapshotBlob,
    PublishedStoreSnapshot, TerminalMembershipMutation,
};
use operation_models::{LocalDeviceRegistrationJournalRow, PreparedLocalDeviceRegistrationRow};
#[cfg(feature = "invariant-tests")]
pub use prepared_audience_objects::exercise_exact_outbound_blob_graph;
use prepared_audience_objects::{
    validate_prepared_audience_blob_bindings, validate_prepared_audience_blob_graph,
};
pub(crate) use prepared_audience_objects::{
    BlobActivation, MakeRemoteIntentState, PreparedAudienceBlob, PreparedAudienceObjects,
    PreparedAudiencePackage, PreparedRemoteObject, StoredBlobReferenceState,
};
use schema_contract::{validate_host_synced_tables, DurablePreparedProtocolObject};
pub(crate) use schema_contract::{StoreBatchCompletion, StoreBatchLocalCleanup};
pub(crate) use store_ack_records::{finish_outbound_store_ack_on, load_outbound_store_ack_on};
pub(crate) use store_authority_records::required_store_root_authority_on;
use store_authority_records::{
    consume_store_creation_probes_on, ensure_founder_replay_baseline_on, founder_graph_identity,
    install_generation_zero_replay_baseline_on, install_snapshot_replay_baseline_on,
    install_store_founder_state_on, install_store_root_authority_on,
    load_generation_zero_replay_baseline_on, load_local_store_founder_graph_on,
    load_store_root_authority_on, validate_founder_graph, DurableFounderMembershipJournal,
};
pub(crate) use store_authority_records::{
    DurableFounderGraph, DurableFounderMembership, FounderMembershipRefs,
};
pub use write_models::SerialBranchDiscardState;
pub(crate) use write_models::{
    AuthorExclusionActivationLocator, BlockedMergeCandidate, CanonicalProtocolObject,
    CompletePreparedStoreWriteOutcome, ExactProtocolObject, InitialStoreMembershipAuthority,
    MergeAbandonmentState, MergeReplayWriteOverlay, OutboundStoreAck, OutboundStoreAckActivation,
    PreparedMergeAbandonmentCandidates, PreparedProtocolObject, PreparedSerialCandidateAbandonment,
    PreparedSerialStoreBranch, PreparedSerialStoreWriteCommit, PreparedStoreWrite,
    PreparedStoreWriteCommit, PreparedStoreWritePartitions, PublishedStoreAck,
    SerialStoreBranchPreparationWork, StoreWriteBase, StoreWriteBlobFact, StoreWriteBlobFacts,
    StoreWriteRemoteBlob, StoreWriteRouting, TerminalCandidateAuthority,
    TerminalCandidateCleanupVerification, UnresolvedSerialBranch,
};
pub(crate) use write_publication_records::{
    parse_prepared_serial_write_state, PreparedWriteMaterialization,
};
pub(crate) use write_publication_records::{
    CandidateCleanupObject, LocalRetirementMaterialization, MergeCandidateAbandonmentPreparation,
    OwnedVerifiedMergeMaterialization, RetainedPackageApplication,
    SerialCandidateAbandonmentPreparation, SerialStoreWritePreparation,
    SerialStoreWritePreparationEntry, StoreWritePreparation, VerifiedMergeMaterialization,
    VerifiedMergeMembershipObjects,
};
use write_publication_records::{
    DurableSerialCandidateAbandonment, MaterializedCommitRetention, MergeAbandonmentOutcome,
    MergeRetractionCleanupInput, PreparedMergeCandidate, PreparedSerialCandidate,
    PreparedStoreWriteState, RetainedAudiencePackage, RetainedCommitActivationInput,
    RetainedMergeMaterializationInput, RetainedMergeMaterializationKey,
};

pub const LOCAL_DEVICE_ID_STATE_KEY: &str = "local_device_id";
const HOST_DEVICE_ID_STATE_KEY: &str = "host_device_id";
pub const WRITE_POLICY_STATE_KEY: &str = "write_policy";
pub(crate) const SYNC_ROUTING_CONTRACT_STATE_KEY: &str = "sync_routing_contract";
pub const SYNC_ROUTING_HASH_STATE_KEY: &str = "sync_routing_hash";
pub(crate) const COVEN_SCHEMA_MANIFEST_STATE_KEY: &str = "coven_schema_manifest";
pub(crate) const COVEN_INITIALIZED_STATE_KEY: &str = "coven_initialized";
pub(crate) const COVEN_INITIALIZED_STATE_VALUE: &str = "1";
pub const SERIAL_MEMBERSHIP_STATE_KEY: &str = "serial_membership_state";
pub const SERIAL_KEY_GENERATION_STATE_KEY: &str = "serial_key_generation";
pub const SERIAL_PROVIDER_ADMIN_STATE_KEY: &str = "serial_provider_admin_state";
pub const SERIAL_WRAPPED_KEYS_STATE_KEY: &str = "serial_wrapped_keys";
pub(crate) const SERIAL_CANDIDATE_ABANDONMENT_STATE_KEY: &str = "serial_candidate_abandonment";
pub(crate) const STORE_DEVICE_GENESIS_STATE_KEY: &str = "store_device_genesis_state";
const GATE_BASELINE_SCHEMA: &str = "coven_gate_empty";

fn authorize_host_sql(context: rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization {
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

    fn missing_store_root_hash() -> Self {
        Self::StoreRootHashMissing
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
    write_policy: WritePolicy,
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
    /// Serializes complete membership-chain loads that share this database, so a
    /// load cannot return an older chain after another load commits a newer floor.
    membership_load: Arc<tokio::sync::Mutex<()>>,
    /// Serializes construction and execution of the one local membership mutation
    /// whose exact signed bytes are held in `outbound_membership_mutation`.
    membership_mutation: Arc<tokio::sync::Mutex<()>>,
    /// Serializes publication and rollback of the one durable founder graph.
    store_creation: Arc<tokio::sync::Mutex<()>>,
    /// Serializes the exact local device-exclusion object and its Store-stream
    /// activation candidate across every database-handle clone.
    store_device_exclusion: Arc<tokio::sync::Mutex<()>>,
    /// Serializes staging and publication of the one exact snapshot generation
    /// held in `outbound_store_snapshot`.
    snapshot_publication: Arc<tokio::sync::Mutex<()>>,
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
    SerialStoreHeadActivated,
    SerialStoreMaterialized,
    SerialRemovalBeforeAdoption,
    SerialMembershipTerminalized,
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
    write_policy: WritePolicy,
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
        stability: crate::sync::store_pull::VerifiedStoreSnapshotStability,
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
            || snapshot.meta.coverage.policy() != store_root.value.descriptor.write_policy
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
        write_policy: WritePolicy,
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
            write_policy,
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
