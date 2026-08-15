//! Sync cycle orchestration.
//!
//! Runs a single sync cycle (gate + push local changes, pull remote changes,
//! manage snapshots) and initializes sync infrastructure. All connection access
//! goes through the owned [`Database`](coven_database::Database). Local changes are published from the
//! durable pending-changeset journal, which each host write appends to inside its
//! own journaled transaction — so a host write landing mid-cycle is captured for
//! the next outgoing changeset, while the pull's apply is a plain connection write
//! that is never journaled and so never echoes applied rows.

use tracing::{debug, info, warn};

use crate::blob::DrainOutcome;
use coven_foundation::changeset::RowChange;
use coven_foundation::store_dir::StoreDir;
use coven_protocol::blob::BlobTransitionObserver;

use super::status::DeviceActivity;
use super::store::HeldStorePosition;
use super::store::{AuthorizedWriterOperation, Store};
use coven_protocol::objects::RotationPending;
use coven_storage::{
    BlobPathScheme, CloudSyncCipherStateAccess, CloudSyncConnection, CloudSyncObjectStorage,
    CloudSyncRotationStateAccess,
};

/// Result of a single sync cycle.
#[derive(Debug)]
pub struct SyncCycleResult {
    /// Number of remote changesets that were applied.
    pub changesets_applied: u64,
    /// Changesets whose present cloud object failed validation or apply. The
    /// position is held at the bad seq for that device. Carries per-changeset
    /// detail (device, seq, reason) so a host can say which changesets are
    /// stalled, not only how many.
    pub held_positions: Vec<HeldStorePosition>,
    /// Per-device activity of the other devices seen in the sync storage —
    /// device id, its member's author key, latest seq, and RFC 3339 last-sync
    /// time — so a host can render which devices synced and when.
    pub device_activity: Vec<DeviceActivity>,
    /// RFC 3339 timestamp of when this cycle completed.
    pub sync_time: String,
    /// Blobs needed before apply failed to download; their changesets and positions
    /// remain pending.
    pub asset_downloads_failed: bool,
    /// Post-commit local blob cleanup still has durable filesystem work pending.
    /// Its corresponding rows and positions are already durable.
    pub local_blob_cleanup_pending: bool,
    /// Row changes from applied changesets, for the host to map to domain events.
    pub row_changes: Vec<RowChange>,
    /// The outbox drain broke this cycle to publish a just-completed make_remote
    /// (coven flipped a root's gate the moment its last blob landed), so the loop
    /// should run the next cycle promptly to drain + publish the rest instead of
    /// waiting the idle interval.
    pub resume_drain_promptly: bool,
    /// Set when an exact local rotation operation or a committed peer rotation
    /// still blocks sealing. While set, this cycle sealed no changeset, blob,
    /// tombstone, or snapshot. The state identifies whether the blocker is a
    /// candidate, a local committed removal, a peer commit, or both.
    pub rotation_pending: Option<RotationPending>,
}

#[derive(Debug)]
pub struct SyncCycleFailure {
    kind: SyncCycleFailureKind,
    operation: &'static str,
    cause: Box<SyncCycleCause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncCycleFailureKind {
    Offline,
    Failed,
}

impl SyncCycleFailure {
    pub(crate) fn operation<E>(operation: &'static str, error: E) -> Self
    where
        E: Into<SyncCycleCause>,
    {
        let cause = error.into();
        let kind = if super::error::error_chain_contains_transport(&cause) {
            SyncCycleFailureKind::Offline
        } else {
            SyncCycleFailureKind::Failed
        };
        Self {
            kind,
            operation,
            cause: Box::new(cause),
        }
    }

    pub(crate) fn is_offline(&self) -> bool {
        self.kind == SyncCycleFailureKind::Offline
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.to_string().contains(pattern)
    }
}

impl std::fmt::Display for SyncCycleFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.operation, self.cause)
    }
}

impl std::error::Error for SyncCycleFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SyncCycleCause {
    #[error("{0}")]
    Database(#[from] coven_database::DbError),
    #[error("{0}")]
    Store(#[from] super::store::StoreError),
    #[error("{0}")]
    Registration(#[from] super::store::StoreRegistrationError),
    #[error("{0}")]
    Initialization(#[from] super::store::StoreInitializationError),
    #[error("{0}")]
    Circle(#[from] super::store::CircleOperationError),
    #[error("{0}")]
    DeviceExclusion(#[from] super::store::StoreDeviceExclusionError),
    #[error("{0}")]
    Reclaim(#[from] super::store::StoreReclaimError),
    #[error("{0}")]
    TombstoneDrain(#[from] crate::blob::delete::TombstoneDrainError),
    #[error("{0}")]
    TombstoneGc(#[from] super::store::commit_publication::operation::TombstoneGcError),
    #[error("{0}")]
    UploadFailures(#[from] crate::blob::UploadFailures),
    #[error("{0}")]
    WriterAuthorization(
        #[from] super::store::commit_publication::operation::StoreWriterAuthorizationError,
    ),
    #[error("{0}")]
    Acknowledgement(#[from] super::store::acknowledgements::StoreAckError),
    #[error("{0}")]
    Membership(#[from] super::store::MembershipOpsError),
    #[error("{0}")]
    Pull(#[from] super::store::StorePullError),
    #[error("{0}")]
    AuthorizationRefresh(
        #[from] super::store::commit_publication::operation::AuthorizationRefreshError,
    ),
    #[error("{0}")]
    PublishedBlobDrop(#[from] super::store::blob::PublishedBlobDropError),
    #[error("{0}")]
    StoreProtocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("{0}")]
    RowRoutingKey(#[from] coven_protocol::circle::RowRoutingKeyError),
    #[error("{0}")]
    Snapshot(#[from] super::store::snapshots::SnapshotError),
}

#[cfg(test)]
mod sync_cycle_failure_tests {
    use super::*;

    #[test]
    fn registration_transport_source_is_offline() {
        let error = crate::sync::store::StoreRegistrationError::Object(
            coven_protocol::objects::StoreObjectError::Storage(
                coven_protocol::objects::StorageError::Storage("provider unavailable".to_string()),
            ),
        );

        let object = std::error::Error::source(&error).expect("object source");
        assert!(object
            .downcast_ref::<coven_protocol::objects::StoreObjectError>()
            .is_some());
        let storage = object.source().expect("storage source");
        assert!(storage
            .downcast_ref::<coven_protocol::objects::StorageError>()
            .is_some());

        assert!(SyncCycleFailure::operation("register", error).is_offline());
    }

    #[test]
    fn registration_configuration_source_is_failed() {
        let error = crate::sync::store::StoreRegistrationError::Object(
            coven_protocol::objects::StoreObjectError::Storage(
                coven_protocol::objects::StorageError::Configuration("missing bucket".to_string()),
            ),
        );

        assert!(!SyncCycleFailure::operation("register", error).is_offline());
    }
}

struct PreparedCycle {
    sync_time: String,
    resume_drain_promptly: bool,
    rotation_pending: Option<RotationPending>,
}

struct CompletedPullCycle {
    store_pull: super::store::StorePullResult,
    local_blob_cleanup_pending: bool,
    sync_time: String,
    resume_drain_promptly: bool,
    rotation_pending: Option<RotationPending>,
}

struct AuthorizedSyncCycle<'cycle, 'store> {
    device_id: &'cycle str,
    clock: &'cycle dyn coven_foundation::clock::Clock,
    cipher: &'cycle dyn CloudSyncCipherStateAccess,
    pending_rotation: &'cycle dyn CloudSyncRotationStateAccess,
    master_keys: Option<&'cycle dyn coven_keys::keys::MasterKeyCustody>,
    routing_encryption: Option<&'cycle coven_keys::encryption::EncryptionService>,
    local_blob_access: &'cycle super::store::blob::LocalStoreBlobAccess,
    observer: Option<&'cycle dyn BlobTransitionObserver>,
    authorization: AuthorizedWriterOperation<'store>,
}

impl AuthorizedSyncCycle<'_, '_> {
    async fn run(mut self) -> Result<SyncCycleResult, SyncCycleFailure> {
        self.authorization
            .resume_operations(self.routing_encryption)
            .await?;
        let prepared = Box::pin(self.prepare_before_pull()).await?;
        let store_pull = self.authorization.pull(self.routing_encryption).await?;
        let completed = Box::pin(self.complete_after_pull(prepared, store_pull)).await?;
        if completed.rotation_pending.is_none() {
            self.authorization
                .circles()
                .publish_circle_epoch_close_responses()
                .await
                .map_err(|error| {
                    SyncCycleFailure::operation("publish Circle epoch-close responses", error)
                })?;
            if let Some(routing_encryption) = self.routing_encryption {
                self.authorization
                    .circles()
                    .finalize_ready_circle_epoch_closes(&completed.sync_time, routing_encryption)
                    .await
                    .map_err(|error| {
                        SyncCycleFailure::operation("finalize Circle epoch closes", error)
                    })?;
            }
            Box::pin(
                self.authorization
                    .acknowledgements()
                    .stage_and_publish(&completed.sync_time),
            )
            .await?;
            Box::pin(self.reclaim_packages()).await?;
        }
        Ok(SyncCycleResult {
            changesets_applied: completed.store_pull.changesets_applied,
            held_positions: completed.store_pull.held_positions,
            device_activity: super::status::other_device_activity(
                &completed.store_pull.visible_heads,
                self.device_id,
            ),
            sync_time: completed.sync_time,
            asset_downloads_failed: completed.store_pull.asset_downloads_failed,
            local_blob_cleanup_pending: completed.local_blob_cleanup_pending,
            row_changes: completed.store_pull.row_changes,
            resume_drain_promptly: completed.resume_drain_promptly,
            rotation_pending: completed.rotation_pending,
        })
    }

    async fn prepare_before_pull(&mut self) -> Result<PreparedCycle, SyncCycleFailure> {
        // Refresh authorization/decryption state BEFORE anything this cycle pushes,
        // judges, or decrypts. Membership and the rotatable store key are
        // per-cycle preconditions, not init-time bootstraps:
        // re-read them now so a removed member's writes are rejected and a rotated key
        // is adopted on a running device without a restart. Runs before the blob drain
        // so the drain (and every push/pull below) uses the current key. A failure here
        // aborts the cycle and retries next time — a refresh that can't complete must
        // not also corrupt state. Adoption itself failing is not this kind of failure —
        // see `rotation_pending` below.
        self.authorization
            .refresh_authorization_state(self.cipher, self.pending_rotation, self.master_keys)
            .await?;

        // Whether this device has adopted everything the store has committed. Read
        // once, right after the refresh that is the one place this cycle could adopt
        // a rotation, and used below to skip every write that would otherwise seal
        // new data under a generation the store has already superseded: the blob
        // upload drain, Store write preparation, the tombstone
        // write drain, both changeset-push paths, and the snapshot. Pull, local writes,
        // and delete-only tombstone GC are unaffected — the gate
        // is on sealing for the cloud, not on using the store. An unadoptable
        // rotation is marked pending by the refresh and pauses exactly this set; it
        // never aborts the cycle.
        let rotation_pending = self
            .pending_rotation
            .check(self.cipher.current_generation())
            .err();
        if let Some(pending) = &rotation_pending {
            warn!(
                rotation_state = ?pending.state,
                live_generation = pending.live_generation,
                "sync paused: store-key rotation work is incomplete; sealing nothing new for the cloud"
            );
        }

        if rotation_pending.is_none() {
            let drained = self
                .authorization
                .drain_tombstones(self.clock)
                .await
                .map_err(|error| {
                    SyncCycleFailure::operation("drain queued blob tombstones", error)
                })?;
            if drained > 0 {
                info!(count = drained, "Drained blob tombstones");
            }
        }
        let reclaimed = self
            .authorization
            .gc_tombstones(self.clock)
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("garbage-collect blob tombstones", error)
            })?;
        if reclaimed > 0 {
            info!(count = reclaimed, "Reclaimed tombstoned blobs");
        }

        let local_seq = self
            .authorization
            .latest_local_store_position()
            .await
            .map_err(|error| SyncCycleFailure::operation("read local Store position", error))?
            .map_or(0, |reference| reference.coord.sequence());
        self.local_blob_access
            .drain_published_blob_drop_intents(local_seq)
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("drain published blob drop intents", error)
            })?;

        // One wall-clock reading for this whole cycle. Store acknowledgements and
        // the status built at the end record the same instant. Store write commits
        // carry a separate HLC stamp (`timestamp` below) for causal ordering.
        let sync_time = self.clock.now().to_rfc3339();

        let mut resume_drain_promptly = false;
        if rotation_pending.is_none() {
            let outcome = self
                .authorization
                .drain_uploads(self.clock, self.routing_encryption, self.observer)
                .await
                .map_err(|error| SyncCycleFailure::operation("drain queued blob uploads", error))?;
            match outcome {
                DrainOutcome::Drained {
                    uploaded,
                    yielded_for_publish,
                    failures,
                } => {
                    if failures.has_transport_failure() {
                        return Err(SyncCycleFailure::operation("upload queued blobs", failures));
                    }
                    resume_drain_promptly = yielded_for_publish;
                    if uploaded > 0 {
                        info!(count = uploaded, "Drained blob uploads");
                    }
                }
                DrainOutcome::QueueEmpty => {}
                DrainOutcome::AllInBackoff => {
                    debug!("Every queued blob upload is inside its retry backoff");
                }
                DrainOutcome::Paused => {
                    debug!("Blob uploads are paused by the host; nothing was admitted");
                }
            }
        }

        if rotation_pending.is_none() {
            let published = self.authorization.publish_prepared_store_writes().await?;
            if published > 0 {
                info!(published, "Published queued Store writes");
            }
        }

        Ok(PreparedCycle {
            sync_time,
            resume_drain_promptly,
            rotation_pending,
        })
    }

    async fn complete_after_pull(
        &mut self,
        prepared: PreparedCycle,
        store_pull: super::store::StorePullResult,
    ) -> Result<CompletedPullCycle, SyncCycleFailure> {
        let PreparedCycle {
            sync_time,
            resume_drain_promptly,
            rotation_pending,
        } = prepared;
        if rotation_pending.is_none() {
            // Pull installs the membership state that decides whether this active
            // member may write. One capability then retains that decision through
            // preparation and publication of every pending Store write.
            let published = self.authorization.publish_pending_store_writes().await?;
            if published > 0 {
                info!(published, "Published Store writes");
            }
        }

        let local_seq = self
            .authorization
            .latest_local_store_position()
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("read local Store position after publish", error)
            })?
            .map_or(0, |position| position.coord.sequence());
        self.local_blob_access
            .drain_published_blob_drop_intents(local_seq)
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("drain published blob drop intents", error)
            })?;
        let local_blob_cleanup_pending = self
            .authorization
            .drain_local_blob_cleanup()
            .await
            .map_err(|error| {
                SyncCycleFailure::operation(
                    "drain local blob cleanup after Store publication",
                    error,
                )
            })?
            || store_pull.local_blob_cleanup_pending;

        // Flush the clock's high-water mark so a restart re-seeds past it. Store pull
        // advances the clock in the row-and-materialized-position commit closure, so
        // `high_water` reflects remote commits and host stamps minted this cycle. A
        // persist error aborts the cycle rather than risking a backward jump.
        self.authorization
            .persist_hlc_high_water()
            .await
            .map_err(|error| SyncCycleFailure::operation("persist HLC high-water mark", error))?;

        self.authorization
            .snapshots()
            .publish_due_snapshots(
                &sync_time,
                self.routing_encryption,
                rotation_pending.is_some(),
            )
            .await?;

        Ok(CompletedPullCycle {
            store_pull,
            local_blob_cleanup_pending,
            sync_time,
            resume_drain_promptly,
            rotation_pending,
        })
    }

    async fn reclaim_packages(&mut self) -> Result<(), SyncCycleFailure> {
        match self.authorization.reclaim_packages().await {
            Ok(result) if result.packages_deleted > 0 => info!(
                packages = result.packages_deleted,
                copies = result.physical_copies_deleted,
                "Reclaimed snapshot-covered Store packages"
            ),
            Ok(_) => {}
            Err(
                error @ (super::store::StoreReclaimError::NoSnapshot
                | super::store::StoreReclaimError::MissingAcknowledgement { .. }),
            ) => info!(%error, "Store package reclamation is awaiting coverage"),
            Err(error) => return Err(SyncCycleFailure::operation("reclaim Store packages", error)),
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InitSyncError {
    #[error("no synced tables configured; pass a non-empty synced-table set before sync starts")]
    NoSyncedTables,
    #[error("cloud cipher and blob path scheme describe different storage modes")]
    IncoherentStorageRepresentation,
    #[error("Store row routing initialization failed: {0}")]
    RowRouting(coven_database::DbError),
    #[error("Store initialization failed: {0}")]
    Initialization(#[from] crate::sync::store::StoreInitializationError),
    #[error("restoring the persisted pending rotation failed: {0}")]
    PendingRotationRestore(#[source] coven_database::DbError),
    #[error("prepared sync identity differs from its storage identity")]
    StorageIdentityMismatch,
    #[error("unlock requires an existing Store root")]
    ExistingStoreRequired,
}

/// Establish the storage representation and signed owner anchor over an
/// already-built [`CloudSyncConnection`], returning the only runnable sync session.
#[derive(Debug, Clone)]
pub enum StoreInitialization {
    CreateStore,
    OpenStore {
        expected_store_root: coven_protocol::store_commit::StoreRootRef,
    },
}

/// One connected Store representation used by an entire sync cycle.
///
/// Transport, at-rest protection, and pending key rotation come from one object
/// so callers cannot assemble a cycle from unrelated storage sessions.
pub(crate) trait CloudSyncCycleConnection:
    CloudSyncObjectStorage + CloudSyncCipherStateAccess + CloudSyncRotationStateAccess
{
}

impl CloudSyncCycleConnection for CloudSyncConnection {}

/// A sync session whose local and cloud representation has been validated
/// before Store creation or opening can perform protocol work.
pub struct PreparedSyncComponents {
    database: coven_database::StoreDatabase,
    store_dir: StoreDir,
    local_blob_access: super::store::blob::LocalStoreBlobAccess,
    storage: std::sync::Arc<CloudSyncConnection>,
    identity: coven_keys::keys::UserKeypair,
    initialization: StoreInitialization,
    store_id: String,
    routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    master_keys: std::sync::Arc<dyn coven_keys::keys::MasterKeyCustody>,
}

impl PreparedSyncComponents {
    pub async fn prepare(
        database: coven_database::StoreDatabase,
        store_dir: StoreDir,
        storage: impl Into<std::sync::Arc<CloudSyncConnection>>,
        identity: coven_keys::keys::UserKeypair,
        initialization: StoreInitialization,
        routing_encryption: Option<coven_keys::encryption::EncryptionService>,
        master_keys: std::sync::Arc<dyn coven_keys::keys::MasterKeyCustody>,
    ) -> Result<Self, InitSyncError> {
        #[cfg(any(test, feature = "test-utils"))]
        database.assert_owns_payload_directory_for_test(&store_dir);
        let storage = storage.into();
        if !storage.uses_identity(&identity) {
            return Err(InitSyncError::StorageIdentityMismatch);
        }
        // Integration guard. The host declared its synced tables on the builder; an
        // empty set means a synced store would attach nothing, every changeset would
        // come out empty, and sync would silently become snapshot-only. Refuse loudly
        // instead of pretending to sync.
        if !database.has_synced_tables() {
            return Err(InitSyncError::NoSyncedTables);
        }
        database
            .validate_store_write_routing(routing_encryption.as_ref())
            .map_err(InitSyncError::RowRouting)?;

        let cipher_is_plaintext = storage.is_plaintext();
        let representation_is_coherent = matches!(
            (cipher_is_plaintext, storage.blob_path_scheme()),
            (true, BlobPathScheme::Plain) | (false, BlobPathScheme::Hashed)
        );
        if !representation_is_coherent {
            return Err(InitSyncError::IncoherentStorageRepresentation);
        }

        // Restore the durable marker before Store creation or opening performs
        // protocol work, so malformed local rotation state cannot accompany new
        // remote state from a failed initialization.
        if !cipher_is_plaintext {
            let gate = database
                .load_rotation_gate()
                .await
                .map_err(InitSyncError::PendingRotationRestore)?;
            storage.install_durable_gate(gate);
        }

        let store_id = storage.store_id().to_string();
        let local_blob_access = super::store::blob::LocalStoreBlobAccess::new(
            database.clone(),
            store_dir.clone(),
            super::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone()),
        );
        Ok(Self {
            database,
            store_dir,
            local_blob_access,
            storage,
            identity,
            initialization,
            store_id,
            routing_encryption,
            master_keys,
        })
    }

    pub async fn initialize(
        self,
        observer: Option<std::sync::Arc<dyn BlobTransitionObserver>>,
    ) -> Result<SyncComponents, InitSyncError> {
        let storage: std::sync::Arc<dyn CloudSyncCycleConnection> = self.storage;
        let store_storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage> =
            storage.clone();
        let initialized = match self.initialization {
            StoreInitialization::CreateStore => {
                Store::create(
                    self.database.clone(),
                    store_storage.clone(),
                    self.store_dir.clone(),
                    &self.database.stamp(),
                    &self.identity,
                )
                .await
            }
            StoreInitialization::OpenStore {
                expected_store_root,
            } => {
                Store::open(
                    self.database.clone(),
                    store_storage.clone(),
                    self.store_dir.clone(),
                    &expected_store_root,
                    &self.identity,
                )
                .await
            }
        }
        .map_err(InitSyncError::Initialization)?;

        let (store, device_id) = initialized.into_parts();
        let blob_transitions = crate::blob::transition::ConnectedBlobTransitions::new(
            crate::blob::transition::LocalBlobTransitions::new(
                self.database.clone(),
                self.store_dir.clone(),
            ),
            std::sync::Arc::new(super::store::blob::RemoteStoreBlobAccess::new(
                self.local_blob_access.clone(),
                super::store::blob::CurrentRemoteBlobSource::current(
                    self.database.clone(),
                    store_storage,
                ),
            )),
            self.routing_encryption.clone(),
            observer,
        );
        info!("Sync initialized (device: {})", device_id);
        Ok(SyncComponents {
            store: std::sync::Arc::new(store),
            database: self.database,
            local_blob_access: self.local_blob_access,
            storage,
            store_id: self.store_id,
            device_id,
            routing_encryption: self.routing_encryption,
            master_keys: self.master_keys,
            blob_transitions,
        })
    }

    pub async fn verify_open_store_key(&self) -> Result<(), InitSyncError> {
        let StoreInitialization::OpenStore {
            expected_store_root,
        } = &self.initialization
        else {
            return Err(InitSyncError::ExistingStoreRequired);
        };
        super::store::protocol_root::verify_store_key_confirmation(
            &self.database,
            self.storage.as_ref(),
            expected_store_root,
        )
        .await
        .map_err(crate::sync::store::StoreInitializationError::from)
        .map_err(InitSyncError::Initialization)
    }
}

/// Components needed to run sync cycles.
///
/// Owns the exact database, storage, register clock, device identity, at-rest
/// cipher, pending-rotation marker, and signing identity that initialization
/// checked. Callers cannot replace any of them before running a cycle.
pub struct SyncComponents {
    store: std::sync::Arc<Store>,
    database: coven_database::StoreDatabase,
    local_blob_access: super::store::blob::LocalStoreBlobAccess,
    storage: std::sync::Arc<dyn CloudSyncCycleConnection>,
    /// The store this sync loop is for. Binds the snapshot meta/pointer it
    /// publishes so a member of two stores can't replay one's catalog as the
    /// other's.
    store_id: String,
    device_id: String,
    routing_encryption: Option<coven_keys::encryption::EncryptionService>,
    master_keys: std::sync::Arc<dyn coven_keys::keys::MasterKeyCustody>,
    blob_transitions: crate::blob::transition::ConnectedBlobTransitions,
}

impl SyncComponents {
    pub(crate) async fn probe_storage(&self) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage.probe_provider().await
    }

    pub(crate) async fn pending_blocked_writes(
        &self,
    ) -> Result<Vec<coven_protocol::write::PendingWrite>, coven_database::DbError> {
        Ok(self
            .database
            .pending_writes()
            .await?
            .into_iter()
            .filter(|write| matches!(write.status, coven_protocol::write::WriteStatus::Blocked(_)))
            .collect())
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<Vec<coven_protocol::write::WriteId>, super::store::StoreError> {
        self.store.discard_blocked_write(write_id).await
    }

    pub(crate) async fn members(
        &self,
    ) -> Result<Vec<coven_protocol::membership::MemberInfo>, super::store::MembershipOpsError> {
        self.store.members().await
    }

    pub(crate) async fn membership_conflict(
        &self,
    ) -> Result<Option<coven_protocol::MembershipConflictInfo>, super::store::MembershipOpsError>
    {
        self.store.membership_conflict().await
    }

    pub(crate) async fn restore_membership(
        &self,
    ) -> Result<super::store::authorization::StoreRestoreMembership, super::store::MembershipOpsError>
    {
        self.store.restore_membership().await
    }

    pub(crate) fn host_write_blob_staging(
        &self,
        runtime: tokio::runtime::Handle,
    ) -> super::store::HostWriteBlobStaging {
        self.store.host_write_blob_staging(runtime)
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        device_id: coven_protocol::StoreDeviceId,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        super::store::StoreDeviceExclusionError,
    > {
        self.store
            .propose_device_exclusion_for_device(device_id)
            .await
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), super::store::StoreDeviceExclusionError> {
        self.store.cancel_device_exclusion_proposal(proposal).await
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), super::store::StoreDeviceExclusionError> {
        self.store
            .finalize_device_exclusion_proposal(proposal)
            .await
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: coven_protocol::StoreDeviceId,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionRequest,
        super::store::OwnerPromotionError,
    > {
        self.store.begin_owner_promotion_for_device(device_id).await
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: coven_protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionAcceptance,
        super::store::OwnerPromotionError,
    > {
        self.store.accept_owner_promotion(request).await
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), super::store::OwnerPromotionError> {
        let encryption = self
            .routing_encryption
            .as_ref()
            .ok_or(super::store::OwnerPromotionError::EncryptionRequired)?;
        self.store
            .finalize_owner_promotion(encryption, acceptance)
            .await
            .map(|_| ())
    }

    pub(crate) async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::sync::DeviceJoinOfferBundle, super::store::DeviceJoinTransportError> {
        self.store.begin_device_join_bundle(member_pubkey).await
    }

    pub(crate) async fn drive_device_join(
        &self,
        bundle: &crate::sync::DeviceJoinOfferBundle,
        policy: crate::sync::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::sync::DeviceProviderAccessAdministrator>,
        timing: crate::sync::DeviceJoinTransportTiming,
    ) -> Result<crate::sync::DeviceJoinDriveOutcome, super::store::DeviceJoinTransportError> {
        self.store
            .device_join_transport()
            .drive(bundle, policy, access_administrator, timing)
            .await
    }

    pub(crate) async fn cancel_device_join_transport(
        &self,
        bundle: &crate::sync::DeviceJoinOfferBundle,
        timing: crate::sync::DeviceJoinTransportTiming,
    ) -> Result<crate::sync::DeviceJoinCleanupActivation, super::store::DeviceJoinTransportError>
    {
        self.store
            .device_join_transport()
            .cancel(bundle, timing)
            .await
    }

    pub(crate) async fn abandon_device_join_transport(
        &self,
        bundle: &crate::sync::DeviceJoinOfferBundle,
    ) -> Result<crate::sync::DeviceJoinAbandonment, super::store::DeviceJoinTransportError> {
        self.store.device_join_transport().abandon(bundle).await
    }

    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::sync::DeviceJoinOffer, crate::sync::DeviceJoinError> {
        self.store.begin_device_join(member_pubkey).await
    }

    pub(crate) async fn abandon_device_join(
        &self,
        offer: crate::sync::DeviceJoinOffer,
    ) -> Result<crate::sync::DeviceJoinAbandonment, crate::sync::DeviceJoinError> {
        self.store.abandon_device_join(offer).await
    }

    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: crate::sync::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::sync::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::sync::DeviceProviderAdmissionApproval, crate::sync::DeviceJoinError> {
        self.store
            .authorize_device_provider_access(request, access_administrator)
            .await
    }

    pub(crate) async fn accept_device_registration(
        &self,
        request: crate::sync::DeviceRegistrationRequest,
    ) -> Result<crate::sync::ProvisionalDeviceBootstrap, crate::sync::DeviceJoinError> {
        self.store.accept_device_registration_request(request).await
    }

    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::sync::ProvisionalDeviceBootstrap,
    ) -> Result<crate::sync::ProviderReadyDeviceBootstrap, crate::sync::DeviceJoinError> {
        self.store
            .publish_device_provider_challenge(bootstrap)
            .await
    }

    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: crate::sync::DeviceJoinReadiness,
    ) -> Result<crate::sync::DeviceProviderAdmissionCompletion, crate::sync::DeviceJoinError> {
        self.store
            .complete_device_provider_admission(readiness)
            .await
    }

    pub(crate) async fn finalize_device_join(
        &self,
        completion: crate::sync::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::sync::DeviceJoinActivation, crate::sync::DeviceJoinError> {
        self.store.finalize_device_join(completion).await
    }

    pub(crate) async fn cancel_device_join(
        &self,
        attempt: coven_protocol::DeviceJoinAttemptRef,
    ) -> Result<crate::sync::DeviceJoinCancellation, crate::sync::DeviceJoinError> {
        self.store.cancel_device_join(attempt).await
    }

    pub(crate) async fn close_device_provider_admission(
        &self,
        cancellation: crate::sync::DeviceJoinCancellation,
    ) -> Result<crate::sync::ProviderAdminJoinTerminal, crate::sync::DeviceJoinError> {
        self.store
            .close_device_provider_admission(cancellation)
            .await
    }

    pub(crate) async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::sync::DeviceJoinCancellation,
        executor: &dyn crate::sync::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::sync::ProviderAdminJoinTerminal, crate::sync::DeviceJoinError> {
        self.store
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await
    }

    pub(crate) async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::sync::DeviceJoinCancellation,
        executor: &dyn crate::sync::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::sync::JoinerJoinTerminal, crate::sync::DeviceJoinError> {
        self.store
            .revoke_joining_device_writes(cancellation, executor)
            .await
    }

    pub(crate) async fn activate_device_join_cleanup(
        &self,
        receipt: crate::sync::DeviceJoinCleanupReceipt,
    ) -> Result<crate::sync::DeviceJoinCleanupActivation, crate::sync::DeviceJoinError> {
        self.store.activate_device_join_cleanup(receipt).await
    }

    pub(crate) async fn complete_owner_device_join_cleanup(
        &self,
        activation: crate::sync::DeviceJoinCleanupActivation,
    ) -> Result<crate::sync::DeviceJoinCleanupActivation, crate::sync::DeviceJoinError> {
        self.store
            .complete_owner_device_join_cleanup(activation)
            .await
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.store.blob_path_scheme()
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        !self.storage.is_plaintext()
    }

    pub(crate) async fn drain_uploads(
        &self,
        clock: &dyn coven_foundation::clock::Clock,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<crate::blob::DrainOutcome, crate::sync::store::StoreError> {
        self.store
            .authorize_writer()
            .await
            .map_err(crate::sync::store::StoreError::from)?
            .drain_uploads(clock, self.routing_encryption.as_ref(), observer)
            .await
            .map_err(crate::sync::store::StoreError::from)
    }

    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.blob_transitions
            .make_remote(root_table, root_id, pin)
            .await
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.blob_transitions
            .cancel_make_remote(root_table, root_id)
            .await
    }

    pub(crate) async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &std::collections::HashMap<String, std::path::PathBuf>,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), crate::blob::transition::MakeLocalError> {
        self.blob_transitions
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    pub(crate) async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        store_name: &str,
    ) -> Result<crate::sync::store::MemberInvitation, super::store::MembershipOpsError> {
        let encryption = self
            .routing_encryption
            .as_ref()
            .ok_or(super::store::MembershipOpsError::NotEncryptedHome)?;
        self.store
            .invite_member(
                public_key_hex,
                invitee_email,
                role,
                encryption,
                &self.store_id,
                store_name,
            )
            .await
    }

    pub(crate) async fn remove_member(
        &self,
        public_key_hex: &str,
    ) -> Result<String, super::store::MembershipOpsError> {
        let encryption = self
            .routing_encryption
            .as_ref()
            .ok_or(super::store::MembershipOpsError::NotEncryptedHome)?;
        self.store
            .remove_member(
                public_key_hex,
                encryption,
                self.master_keys.as_ref(),
                self.storage.as_ref(),
                self.storage.as_ref(),
            )
            .await
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &coven_protocol::membership::MembershipConflictChoice,
    ) -> Result<(), super::store::MembershipOpsError> {
        self.store
            .resolve_membership_conflict(choice, &self.database.stamp())
            .await?;
        Ok(())
    }

    pub(crate) async fn create_circle(
        &self,
        name: &str,
    ) -> Result<coven_protocol::circle::CircleId, super::store::CircleOperationError> {
        self.store
            .circles()
            .create_circle(&self.database.stamp(), name)
            .await
    }

    pub(crate) async fn rename_circle(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        name: &str,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .rename_circle(&self.database.stamp(), circle_id, name)
            .await
    }

    pub(crate) async fn resolve_circle_control(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        chosen: coven_protocol::circle::CircleControlCoord,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .resolve_circle_control(circle_id, chosen)
            .await
    }

    pub(crate) async fn delete_circle(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store.circles().delete_circle(circle_id).await
    }

    pub(crate) async fn add_circle_member(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        member_pubkey: String,
        role: coven_protocol::circle::CircleRole,
    ) -> Result<(), super::store::CircleOperationError> {
        use super::store::CircleOperationError;
        // A member addition captures a bootstrap over the scoped routing graph, so
        // an unscoped (browsable) Store cannot author one — the same refusal
        // `Store::add_circle_member` raises, surfaced here before the setup work.
        let routing_encryption = self
            .routing_encryption
            .as_ref()
            .ok_or(CircleOperationError::BrowsableStorage)?;
        let mut authorization = self
            .store
            .authorize_writer()
            .await
            .map_err(CircleOperationError::from)?;
        authorization
            .publish_pending_store_writes()
            .await
            .map_err(CircleOperationError::from)?;
        let bootstrap = authorization
            .circles()
            .snapshots()
            .capture_circle_snapshot_cut(routing_encryption, circle_id)
            .await?;
        let routing_key = coven_protocol::circle::derive_row_routing_key(
            routing_encryption,
            self.store.store_root().store_root_hash,
        )
        .map_err(CircleOperationError::from)?;
        authorization
            .circles()
            .add_circle_member(circle_id, member_pubkey, role, bootstrap, &routing_key)
            .await
    }

    pub(crate) async fn remove_circle_member(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        member_pubkey: String,
    ) -> Result<coven_protocol::circle::CircleOperationId, super::store::CircleOperationError> {
        self.store
            .circles()
            .remove_circle_member(circle_id, member_pubkey)
            .await
    }

    pub(crate) async fn cancel_circle_epoch_close(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<coven_protocol::circle::CircleOperationId, super::store::CircleOperationError> {
        self.store
            .circles()
            .cancel_circle_epoch_close(circle_id)
            .await
    }

    pub(crate) async fn exclude_circle_close_device(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        excluded_device_id: coven_protocol::store_commit::StoreDeviceId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .exclude_circle_close_device(circle_id, excluded_device_id)
            .await
    }

    pub(crate) async fn retry_circle_operation(
        &self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .retry_circle_operation(operation_id, self.routing_encryption.as_ref())
            .await
    }

    pub(crate) async fn discard_circle_operation(
        &self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .discard_circle_operation(operation_id)
            .await
    }

    pub(crate) async fn circle_close_status(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<coven_protocol::circle::CircleCloseStatus, super::store::CircleOperationError> {
        self.store.circles().circle_close_status(circle_id).await
    }

    pub async fn run_cycle(
        &self,
        clock: &dyn coven_foundation::clock::Clock,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<SyncCycleResult, SyncCycleFailure> {
        let authorization =
            self.store.authorize_writer().await.map_err(|error| {
                SyncCycleFailure::operation("authorize local Store writer", error)
            })?;
        AuthorizedSyncCycle {
            device_id: &self.device_id,
            clock,
            cipher: self.storage.as_ref(),
            pending_rotation: self.storage.as_ref(),
            master_keys: Some(self.master_keys.as_ref()),
            routing_encryption: self.routing_encryption.as_ref(),
            local_blob_access: &self.local_blob_access,
            observer,
            authorization,
        }
        .run()
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn from_retained_test_device<S>(
        store: std::sync::Arc<Store>,
        database: coven_database::StoreDatabase,
        store_dir: StoreDir,
        storage: std::sync::Arc<S>,
        store_id: String,
        device_id: String,
        master_keys: std::sync::Arc<dyn coven_keys::keys::MasterKeyCustody>,
    ) -> Self
    where
        S: CloudSyncCycleConnection + 'static,
    {
        database.assert_owns_payload_directory_for_test(&store_dir);
        let storage: std::sync::Arc<dyn CloudSyncCycleConnection> = storage;
        let store_storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage> =
            storage.clone();
        let local_blob_access = super::store::blob::LocalStoreBlobAccess::new(
            database.clone(),
            store_dir.clone(),
            super::store::blob::StoreBlobCache::new(database.clone(), store_dir.clone()),
        );
        let blob_transitions = crate::blob::transition::ConnectedBlobTransitions::new(
            crate::blob::transition::LocalBlobTransitions::new(database.clone(), store_dir),
            std::sync::Arc::new(super::store::blob::RemoteStoreBlobAccess::new(
                local_blob_access.clone(),
                super::store::blob::CurrentRemoteBlobSource::current(
                    database.clone(),
                    store_storage,
                ),
            )),
            None,
            None,
        );
        Self {
            store,
            database,
            local_blob_access,
            store_id,
            storage,
            device_id,
            routing_encryption: None,
            master_keys,
            blob_transitions,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn list_storage_objects_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, coven_protocol::objects::StorageError> {
        self.storage.list_provider_keys_for_test(prefix).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn uses_storage_for_test(
        &self,
        expected: &std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage>,
    ) -> bool {
        let actual: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage> =
            self.storage.clone();
        std::sync::Arc::ptr_eq(&actual, expected)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn uses_store_dir_for_test(&self, expected: &StoreDir) -> bool {
        self.local_blob_access.uses_store_dir_for_test(expected)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn encryption_generation_for_test(&self) -> Option<u64> {
        self.storage.current_generation()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_sealed_blob_for_test(
        &self,
        stored: &[u8],
        aad_context: &[u8],
    ) -> Result<
        (coven_keys::encryption::KeyFingerprint, Vec<u8>),
        coven_keys::encryption::EncryptionError,
    > {
        self.storage.open_sealed_blob_for_test(stored, aad_context)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn adopt_key_rotation(
        &self,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> Result<String, coven_keys::keys::KeyError> {
        CloudSyncCipherStateAccess::adopt_key_rotation(
            self.storage.as_ref(),
            &encryption,
            self.master_keys.as_ref(),
        )
        .map(|adopted| adopted.fingerprint().to_string())
    }
}
