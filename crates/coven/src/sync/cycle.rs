//! Sync cycle orchestration.
//!
//! Runs a single sync cycle (gate + push local changes, pull remote changes,
//! manage snapshots) and initializes sync infrastructure. All connection access
//! goes through the owned [`Database`]. Local changes are published from the
//! durable pending-changeset journal, which each host write appends to inside its
//! own journaled transaction — so a host write landing mid-cycle is captured for
//! the next outgoing changeset, while the pull's apply is a plain connection write
//! that is never journaled and so never echoes applied rows.

use tracing::{debug, info, warn};

use crate::database::DbError;
use crate::protocol::blob::BlobTransitionObserver;
use crate::protocol::blob::DrainOutcome;
use coven_foundation::changeset::RowChange;
use coven_foundation::store_dir::StoreDir;

use super::status::DeviceActivity;
use super::store::HeldStorePosition;
use super::store::{AuthorizedWriterOperation, Store};
use crate::protocol::objects::RotationPending;
use crate::storage::{
    BlobPathScheme, CloudCipherAccess, CloudRotationAccess, CloudSyncStorage, SyncStorage,
};

/// Result of a single sync cycle.
#[derive(Debug)]
pub(crate) struct SyncCycleResult {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SyncCycleFailure {
    Offline(String),
    Failed(String),
}

impl SyncCycleFailure {
    pub(crate) fn operation<E>(operation: &str, error: E) -> Self
    where
        E: std::error::Error + 'static,
    {
        let offline = error_chain_contains_transport(&error);
        let message = format!("{operation}: {error}");
        if offline {
            Self::Offline(message)
        } else {
            Self::Failed(message)
        }
    }

    pub(crate) fn is_offline(&self) -> bool {
        matches!(self, Self::Offline(_))
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.to_string().contains(pattern)
    }
}

impl From<String> for SyncCycleFailure {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

impl std::fmt::Display for SyncCycleFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Offline(message) | Self::Failed(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for SyncCycleFailure {}

fn error_chain_contains_transport(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<crate::protocol::objects::StorageError>()
            .is_some_and(crate::protocol::objects::StorageError::is_transport)
            || source
                .downcast_ref::<crate::storage::cloud::CloudHomeError>()
                .is_some_and(|error| {
                    matches!(
                        error,
                        crate::storage::cloud::CloudHomeError::Transport(_)
                            | crate::storage::cloud::CloudHomeError::Io(_)
                    )
                })
        {
            return true;
        }
        current = source.source();
    }
    false
}

#[cfg(test)]
mod sync_cycle_failure_tests {
    use super::*;

    #[test]
    fn registration_transport_source_is_offline() {
        let error = crate::sync::store::StoreRegistrationError::Object(
            crate::protocol::objects::StoreObjectError::Storage(
                crate::protocol::objects::StorageError::Storage("provider unavailable".to_string()),
            ),
        );

        let object = std::error::Error::source(&error).expect("object source");
        assert!(object
            .downcast_ref::<crate::protocol::objects::StoreObjectError>()
            .is_some());
        let storage = object.source().expect("storage source");
        assert!(storage
            .downcast_ref::<crate::protocol::objects::StorageError>()
            .is_some());

        assert!(SyncCycleFailure::operation("register", error).is_offline());
    }

    #[test]
    fn registration_configuration_source_is_failed() {
        let error = crate::sync::store::StoreRegistrationError::Object(
            crate::protocol::objects::StoreObjectError::Storage(
                crate::protocol::objects::StorageError::Configuration("missing bucket".to_string()),
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
    cipher: &'cycle dyn CloudCipherAccess,
    pending_rotation: &'cycle dyn CloudRotationAccess,
    master_keys: Option<&'cycle dyn crate::keys::MasterKeyCustody>,
    routing_encryption: Option<&'cycle crate::encryption::EncryptionService>,
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
                    .stage_and_publish_ack(&completed.sync_time),
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
        let rotation_pending = self.pending_rotation.check(&self.cipher.snapshot()).err();
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
                .drain_tombstones(self.cipher, self.pending_rotation, self.clock)
                .await
                .map_err(|error| format!("drain queued blob tombstones: {error}"))?;
            if drained > 0 {
                info!(count = drained, "Drained blob tombstones");
            }
        }
        let reclaimed = self
            .authorization
            .gc_tombstones(self.cipher, self.clock)
            .await
            .map_err(|error| format!("garbage-collect blob tombstones: {error}"))?;
        if reclaimed > 0 {
            info!(count = reclaimed, "Reclaimed tombstoned blobs");
        }

        let local_seq = self
            .authorization
            .latest_local_store_position()
            .await
            .map_err(|error| format!("read local Store position: {error}"))?
            .map_or(0, |reference| reference.coord.sequence());
        self.local_blob_access
            .drain_published_blob_drop_intents(local_seq)
            .await?;

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
            .map_err(|error| format!("read local Store position after publish: {error}"))?
            .map_or(0, |position| position.coord.sequence());
        self.local_blob_access
            .drain_published_blob_drop_intents(local_seq)
            .await?;
        let local_blob_cleanup_pending = self
            .authorization
            .drain_local_blob_cleanup()
            .await
            .map_err(|error| {
                format!("drain local blob cleanup after Store publication: {error}")
            })?
            || store_pull.local_blob_cleanup_pending;

        // Flush the clock's high-water mark so a restart re-seeds past it. Store pull
        // advances the clock in the row-and-materialized-position commit closure, so
        // `high_water` reflects remote commits and host stamps minted this cycle. A
        // persist error aborts the cycle rather than risking a backward jump.
        self.authorization
            .persist_hlc_high_water()
            .await
            .map_err(|e| format!("Failed to persist HLC high-water mark: {e}"))?;

        self.authorization
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
    RowRouting(crate::database::DbError),
    #[error("Store protocol root failed: {0}")]
    StoreProtocolRoot(String),
    #[error("membership chain bootstrap/anchor failed: {0}")]
    MembershipAnchor(String),
    #[error("restoring the persisted pending rotation failed: {0}")]
    PendingRotationRestore(String),
    #[error("prepared sync identity differs from its storage identity")]
    StorageIdentityMismatch,
}

/// Establish the storage representation and signed owner anchor over an
/// already-built [`CloudSyncStorage`], returning the only runnable sync session.
#[derive(Debug, Clone)]
pub(crate) enum StoreInitialization {
    CreateStore,
    OpenStore {
        expected_store_root: crate::protocol::store_commit::StoreRootRef,
    },
}

/// One connected Store representation used by an entire sync cycle.
///
/// Transport, at-rest protection, and pending key rotation come from one object
/// so callers cannot assemble a cycle from unrelated storage sessions.
pub(crate) trait SyncCycleStorage:
    SyncStorage + CloudCipherAccess + CloudRotationAccess
{
}

impl SyncCycleStorage for CloudSyncStorage {}

/// A sync session whose local and cloud representation has been validated
/// before Store creation or opening can perform protocol work.
pub(crate) struct PreparedSyncComponents {
    database: crate::database::StoreDatabase,
    store_dir: StoreDir,
    local_blob_access: super::store::blob::LocalStoreBlobAccess,
    storage: std::sync::Arc<CloudSyncStorage>,
    identity: crate::keys::UserKeypair,
    initialization: StoreInitialization,
    store_id: String,
    routing_encryption: Option<crate::encryption::EncryptionService>,
}

impl PreparedSyncComponents {
    pub(crate) async fn prepare(
        database: crate::database::StoreDatabase,
        store_dir: StoreDir,
        local_blob_access: super::store::blob::LocalStoreBlobAccess,
        storage: impl Into<std::sync::Arc<CloudSyncStorage>>,
        identity: crate::keys::UserKeypair,
        initialization: StoreInitialization,
        routing_encryption: Option<crate::encryption::EncryptionService>,
    ) -> Result<Self, InitSyncError> {
        let storage = storage.into();
        if !storage.uses_identity(&identity) {
            return Err(InitSyncError::StorageIdentityMismatch);
        }
        // Integration guard. The host declared its synced tables on the builder; an
        // empty set means a synced store would attach nothing, every changeset would
        // come out empty, and sync would silently become snapshot-only. Refuse loudly
        // instead of pretending to sync.
        if database.synced_tables().is_empty() {
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
                .map_err(|e| InitSyncError::PendingRotationRestore(e.to_string()))?;
            storage.install_durable_gate(gate);
        }

        let store_id = storage.store_id().to_string();
        Ok(Self {
            database,
            store_dir,
            local_blob_access,
            storage,
            identity,
            initialization,
            store_id,
            routing_encryption,
        })
    }

    pub(crate) async fn initialize(self) -> Result<SyncComponents, InitSyncError> {
        let storage: std::sync::Arc<dyn SyncCycleStorage> = self.storage;
        let store_storage: std::sync::Arc<dyn crate::storage::SyncStorage> = storage.clone();
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
        .map_err(|error| match error {
            crate::sync::store::StoreInitializationError::ProtocolRoot(error) => {
                InitSyncError::StoreProtocolRoot(error)
            }
            crate::sync::store::StoreInitializationError::MembershipAnchor(error) => {
                InitSyncError::MembershipAnchor(error)
            }
        })?;

        info!("Sync initialized (device: {})", initialized.device_id);
        Ok(SyncComponents {
            store: std::sync::Arc::new(initialized.store),
            database: self.database,
            local_blob_access: self.local_blob_access,
            storage,
            store_id: self.store_id,
            device_id: initialized.device_id,
            routing_encryption: self.routing_encryption,
        })
    }
}

/// Components needed to run sync cycles.
///
/// Owns the exact database, storage, register clock, device identity, at-rest
/// cipher, pending-rotation marker, and signing identity that initialization
/// checked. Callers cannot replace any of them before running a cycle.
pub(crate) struct SyncComponents {
    store: std::sync::Arc<Store>,
    database: crate::database::StoreDatabase,
    local_blob_access: super::store::blob::LocalStoreBlobAccess,
    storage: std::sync::Arc<dyn SyncCycleStorage>,
    /// The store this sync loop is for. Binds the snapshot meta/pointer it
    /// publishes so a member of two stores can't replay one's catalog as the
    /// other's.
    store_id: String,
    device_id: String,
    routing_encryption: Option<crate::encryption::EncryptionService>,
}

impl SyncComponents {
    pub(crate) async fn probe_storage(&self) -> Result<(), crate::protocol::objects::StorageError> {
        self.storage.probe_provider().await
    }

    pub(crate) async fn pending_blocked_writes(
        &self,
    ) -> Result<Vec<crate::PendingWrite>, crate::database::DbError> {
        Ok(self
            .database
            .pending_writes()
            .await?
            .into_iter()
            .filter(|write| matches!(write.status, crate::WriteStatus::Blocked(_)))
            .collect())
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, super::store::StoreError> {
        self.store.discard_blocked_write(write_id).await
    }

    pub(crate) async fn members(
        &self,
    ) -> Result<Vec<crate::protocol::membership::MemberInfo>, super::store::MembershipOpsError>
    {
        self.store.members().await
    }

    pub(crate) async fn membership_conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, super::store::MembershipOpsError> {
        self.store.membership_conflict().await
    }

    pub(crate) async fn restore_membership(
        &self,
    ) -> Result<super::store::owner::StoreRestoreMembership, super::store::MembershipOpsError> {
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
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::StoreDeviceExclusionProposalRef, String> {
        self.store
            .propose_device_exclusion_for_device(device_id)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), String> {
        self.store
            .cancel_device_exclusion_proposal(proposal)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), String> {
        self.store
            .finalize_device_exclusion_proposal(proposal)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionRequest, String> {
        self.store
            .begin_owner_promotion_for_device(device_id)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionAcceptance, String> {
        self.store
            .accept_owner_promotion(request)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), String> {
        let encryption = self
            .current_encryption()
            .ok_or_else(|| "owner promotion requires an encrypted cloud home".to_string())?;
        self.store
            .finalize_owner_promotion(&encryption, acceptance)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOfferBundle, super::store::DeviceJoinTransportError> {
        self.store.begin_device_join_bundle(member_pubkey).await
    }

    pub(crate) async fn drive_device_join(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, super::store::DeviceJoinTransportError> {
        self.store
            .device_join_transport()
            .drive(bundle, policy, access_administrator, timing)
            .await
    }

    pub(crate) async fn cancel_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinCleanupActivation, super::store::DeviceJoinTransportError> {
        self.store
            .device_join_transport()
            .cancel(bundle, timing)
            .await
    }

    pub(crate) async fn abandon_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
    ) -> Result<crate::DeviceJoinAbandonment, super::store::DeviceJoinTransportError> {
        self.store.device_join_transport().abandon(bundle).await
    }

    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, crate::DeviceJoinError> {
        self.store.begin_device_join(member_pubkey).await
    }

    pub(crate) async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, crate::DeviceJoinError> {
        self.store.abandon_device_join(offer).await
    }

    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, crate::DeviceJoinError> {
        self.store
            .authorize_device_provider_access(request, access_administrator)
            .await
    }

    pub(crate) async fn accept_device_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, crate::DeviceJoinError> {
        self.store.accept_device_registration_request(request).await
    }

    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, crate::DeviceJoinError> {
        self.store
            .publish_device_provider_challenge(bootstrap)
            .await
    }

    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, crate::DeviceJoinError> {
        self.store
            .complete_device_provider_admission(readiness)
            .await
    }

    pub(crate) async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, crate::DeviceJoinError> {
        self.store.finalize_device_join(completion).await
    }

    pub(crate) async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, crate::DeviceJoinError> {
        self.store.cancel_device_join(attempt).await
    }

    pub(crate) async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.store
            .close_device_provider_admission(cancellation)
            .await
    }

    pub(crate) async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.store
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await
    }

    pub(crate) async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, crate::DeviceJoinError> {
        self.store
            .revoke_joining_device_writes(cancellation, executor)
            .await
    }

    pub(crate) async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::DeviceJoinError> {
        self.store.activate_device_join_cleanup(receipt).await
    }

    pub(crate) async fn complete_owner_device_join_cleanup(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::DeviceJoinError> {
        self.store
            .complete_owner_device_join_cleanup(activation)
            .await
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.store.blob_path_scheme()
    }

    fn current_encryption(&self) -> Option<crate::encryption::EncryptionService> {
        match self.storage.snapshot() {
            crate::storage::CloudCipher::Encrypted(encryption) => Some(encryption),
            crate::storage::CloudCipher::Plaintext => None,
        }
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        self.current_encryption().is_some()
    }

    pub(crate) async fn drain_uploads(
        &self,
        clock: &dyn coven_foundation::clock::Clock,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<crate::protocol::blob::DrainOutcome, DbError> {
        self.store
            .authorize_writer()
            .await
            .map_err(|error| DbError::Message(error.to_string()))?
            .drain_uploads(clock, self.routing_encryption.as_ref(), observer)
            .await
    }

    pub(crate) async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, super::store::MembershipOpsError> {
        let encryption = self
            .current_encryption()
            .ok_or(super::store::MembershipOpsError::NotEncryptedHome)?;
        self.store
            .invite_member(
                public_key_hex,
                invitee_email,
                role,
                &encryption,
                &self.store_id,
                store_name,
            )
            .await
    }

    pub(crate) async fn remove_member(
        &self,
        public_key_hex: &str,
        master_keys: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<String, super::store::MembershipOpsError> {
        let encryption = self
            .current_encryption()
            .ok_or(super::store::MembershipOpsError::NotEncryptedHome)?;
        self.store
            .remove_member(
                public_key_hex,
                &encryption,
                master_keys,
                self.storage.as_ref(),
                self.storage.as_ref(),
            )
            .await
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &crate::protocol::membership::MembershipConflictChoice,
    ) -> Result<(), super::store::MembershipOpsError> {
        self.store
            .resolve_membership_conflict(choice, &self.database.stamp())
            .await?;
        Ok(())
    }

    pub(crate) async fn create_circle(
        &self,
        name: &str,
    ) -> Result<crate::protocol::circle::CircleId, super::store::CircleOperationError> {
        self.store
            .circles()
            .create_circle(&self.database.stamp(), name)
            .await
    }

    pub(crate) async fn rename_circle(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        name: &str,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .rename_circle(&self.database.stamp(), circle_id, name)
            .await
    }

    pub(crate) async fn resolve_circle_control(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        chosen: crate::protocol::circle::CircleControlCoord,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .resolve_circle_control(circle_id, chosen)
            .await
    }

    pub(crate) async fn delete_circle(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store.circles().delete_circle(circle_id).await
    }

    pub(crate) async fn add_circle_member(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        member_pubkey: String,
        role: crate::protocol::circle::CircleRole,
    ) -> Result<(), super::store::CircleOperationError> {
        use super::store::CircleOperationError;
        // A member addition captures a bootstrap over the scoped routing graph, so
        // an unscoped (browsable) Store cannot author one — the same refusal
        // `Store::add_circle_member` raises, surfaced here before the setup work.
        let routing_encryption = self
            .current_encryption()
            .ok_or(CircleOperationError::BrowsableStorage)?;
        let mut authorization = self
            .store
            .authorize_writer()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        authorization
            .publish_pending_store_writes()
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let bootstrap = authorization
            .circles()
            .snapshots()
            .capture_circle_snapshot_cut(&routing_encryption, circle_id)
            .await?;
        let routing_key = crate::protocol::circle::derive_row_routing_key(
            &routing_encryption,
            self.store.store_root().store_root_hash,
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        authorization
            .circles()
            .add_circle_member(circle_id, member_pubkey, role, bootstrap, &routing_key)
            .await
    }

    pub(crate) async fn remove_circle_member(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        member_pubkey: String,
    ) -> Result<crate::protocol::circle::CircleOperationId, super::store::CircleOperationError>
    {
        self.store
            .circles()
            .remove_circle_member(circle_id, member_pubkey)
            .await
    }

    pub(crate) async fn cancel_circle_epoch_close(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, super::store::CircleOperationError>
    {
        self.store
            .circles()
            .cancel_circle_epoch_close(circle_id)
            .await
    }

    pub(crate) async fn exclude_circle_close_device(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        excluded_device_id: crate::protocol::store_commit::StoreDeviceId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .exclude_circle_close_device(circle_id, excluded_device_id)
            .await
    }

    pub(crate) async fn retry_circle_operation(
        &self,
        operation_id: &crate::protocol::circle::CircleOperationId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .retry_circle_operation(operation_id, self.routing_encryption.as_ref())
            .await
    }

    pub(crate) async fn discard_circle_operation(
        &self,
        operation_id: &crate::protocol::circle::CircleOperationId,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .discard_circle_operation(operation_id)
            .await
    }

    pub(crate) async fn circle_close_status(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<crate::protocol::circle::CircleCloseStatus, super::store::CircleOperationError>
    {
        self.store.circles().circle_close_status(circle_id).await
    }

    pub(crate) async fn run_cycle(
        &self,
        clock: &dyn coven_foundation::clock::Clock,
        master_keys: Option<&dyn crate::keys::MasterKeyCustody>,
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
            master_keys,
            routing_encryption: self.routing_encryption.as_ref(),
            local_blob_access: &self.local_blob_access,
            observer,
            authorization,
        }
        .run()
        .await
    }

    #[cfg(test)]
    pub(crate) fn from_retained_test_device<S>(
        store: std::sync::Arc<Store>,
        database: crate::database::StoreDatabase,
        local_blob_access: super::store::blob::LocalStoreBlobAccess,
        storage: std::sync::Arc<S>,
        store_id: String,
        device_id: String,
    ) -> Self
    where
        S: SyncCycleStorage + 'static,
    {
        let storage: std::sync::Arc<dyn SyncCycleStorage> = storage;
        Self {
            store,
            database,
            local_blob_access,
            store_id,
            storage,
            device_id,
            routing_encryption: None,
        }
    }

    #[cfg(test)]
    pub(crate) async fn list_storage_objects_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::protocol::objects::StorageError> {
        self.storage.list_provider_objects(prefix).await
    }

    #[cfg(test)]
    pub(crate) fn uses_storage_for_test(
        &self,
        expected: &std::sync::Arc<dyn crate::storage::SyncStorage>,
    ) -> bool {
        let actual: std::sync::Arc<dyn crate::storage::SyncStorage> = self.storage.clone();
        std::sync::Arc::ptr_eq(&actual, expected)
    }

    #[cfg(test)]
    pub(crate) fn encryption_generation_for_test(&self) -> Option<u64> {
        self.current_encryption()
            .map(|encryption| encryption.current_generation())
    }

    #[cfg(test)]
    pub(crate) fn open_sealed_blob_for_test(
        &self,
        stored: &[u8],
        aad_context: &[u8],
    ) -> Result<(crate::encryption::KeyFingerprint, Vec<u8>), String> {
        let encryption = self
            .current_encryption()
            .ok_or_else(|| "session is not encrypted".to_string())?;
        let (fingerprint, header, chunks) =
            crate::storage::split_sealed_blob(stored).map_err(|error| error.to_string())?;
        let plaintext = encryption
            .blob_opener(
                header,
                &crate::encryption::NoncePolicy::DerivedFromContext {
                    context: aad_context.to_vec(),
                },
                aad_context,
            )
            .map_err(|error| error.to_string())?
            .open_chunks(0..header.chunk_count(), chunks)
            .map_err(|error| error.to_string())?;
        Ok((fingerprint, plaintext))
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation(
        &self,
        encryption: crate::encryption::EncryptionService,
        master_keys: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<String, crate::keys::KeyError> {
        CloudCipherAccess::adopt_key_rotation(self.storage.as_ref(), &encryption, master_keys)
    }
}
