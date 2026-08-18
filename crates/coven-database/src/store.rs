mod activated_registration_records;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use circle_operations::circle_current_state_on;
use circle_operations::circle_publication_context_on;
pub(crate) use store_session::payload_store;
#[cfg(any(test, feature = "test-utils"))]
use store_session::test_support;
use store_session::{
    blob_outbox, blob_transitions, circle_authority, circle_controls, circle_operations,
    host_write_capture, host_write_operation, local_blob_cleanup, materialized_commit_index,
    merge_materialization_transaction, pull_replay, replay_projection, retained_merge_replay,
    retained_replay, snapshot_image, stream_activation_records, verified_store_authority,
    write_lifecycle,
};
pub use store_session::{candidate_records, payload_store::PayloadStoreError, reclaim};
mod device_join;
pub(crate) use device_join::{
    advance_device_join_on, begin_device_join_on, begin_device_join_replacement_terminal_on,
    complete_device_join_from_pending_on, device_join_records_on,
};
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use device_join::{
    forget_device_join_on, forget_provider_administrator_device_joins_on,
};
pub mod device_join_journal;
mod host_sql;
mod host_sql_transaction;
pub(crate) use host_write_operation::{NewBlob, StagedBlobBatch};
pub(crate) use local_blob_cleanup::{
    complete_local_blob_cleanup_on, local_blob_cleanup_intents_on,
};
pub mod local_blob_cleanup_intents;
pub mod materialization_models;
use activated_registration_records::record_activated_store_device_registrations_on;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use store_session::prepared_remote_objects::persist_prepared_audience_objects_on;
pub mod publication_state;
use replay_projection::ReplayProjection;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use retained_merge_replay::remove_retained_replay_ownership_from_snapshot_on;
mod store_database;
mod store_device_state;
pub use store_database::StoreDatabase;
mod store_session;
pub(crate) use store_session::StoreSession;
pub(crate) use verified_store_authority::VerifiedStoreAuthority;

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

pub use blob_outbox::{
    CloudOutboxSnapshot, MakeRemoteProgress, QueuedDelete, QueuedMakeRemote, QueuedUpload,
    QueuedUploadPhase,
};
pub use blob_outbox::{OutboxEntry, OutboxOperation, OutboxUploadState};
pub use blob_transitions::{BlobTransitionRoot, MaterializedLocalBlob, PostUpload};
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
pub use local_blob_cleanup::LocalBlobCleanup;
pub use materialization_models::{
    activated_merge_membership_remote_objects, DeviceJoinBootstrapActivation,
    DeviceJoinBootstrapCommit, DeviceJoinBootstrapPlan, MembershipAuthorityBytes,
    OwnedVerifiedMergeMaterialization, PreparedMergeMaterialization,
    PreparedMergeMaterializationPackage, RetainedAudiencePackage, RetainedMergeHistoryCheckpoint,
    RetainedMergeMaterializationKey, RetainedPackageApplication, VerifiedMergeMaterialization,
    VerifiedMergeMembershipObjects, VerifiedStoreSnapshotStability,
};
#[cfg(test)]
pub(crate) use merge_materialization_transaction::test_install_winning_blob_bindings;
#[cfg(test)]
pub(crate) use merge_materialization_transaction::test_retire_circle_bootstrap_coverage;
pub(crate) use merge_materialization_transaction::MergeMaterializationTransaction;
#[cfg(any(test, feature = "test-utils"))]
pub use merge_materialization_transaction::{resolve_and_apply_changeset, ApplyResult};
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use merge_materialization_transaction::{
    test_apply_changeset, test_record_verified_circle_activations,
};
pub use merge_materialization_transaction::{
    IncomingTimestampPolicy, TableSchema, ValidatedChangeset, WinningRow,
};
pub use publication_state::{MergeCandidateAbandonmentPreparation, StoreWritePreparation};
pub(crate) use pull_replay::{
    install_circle_bootstrap_connection_on, install_circle_bootstrap_image_on,
    install_circle_bootstrap_remote_objects_on,
};
pub use reclaim::journal::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, ReclaimCommitActivation,
    ReclaimedStorePackage, StoreReclaimCandidateLoss, StoreReclaimJournalError,
};
pub(crate) use retained_replay::copy_table_with_conflicts;
pub use retained_replay::{
    projection_table_names, RetainedReplayAuthority, RetainedReplayBaseline,
    RetainedReplayGenesisAuthority, GENERATION_ZERO,
};
pub(crate) use snapshot_image::verify_circle_bootstrap_connection;
pub use snapshot_image::{
    CreatedSnapshot, SnapshotBlobAudience, SnapshotBlobFact, SnapshotDatabaseImage,
    SnapshotImageError, SnapshotImageOperationError,
};
use store_device_state::apply_store_device_exclusion_freezes_on;
pub use store_session::circle_acknowledgements::CircleAckPublicationInput;
#[cfg(any(test, feature = "test-utils"))]
pub use test_support::AuthorExclusionLocatorTamper;
pub use write_lifecycle::BlockedWriteDiscard;

pub(crate) use store_session::install_verified_snapshot_bootstrap_on;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use store_session::{
    circle_bootstrap_replay_inputs_for_test, retained_merge_replay_inputs_for_test,
};

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
