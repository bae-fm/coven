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

use crate::blob::upload::DrainOutcome;
use crate::blob::BlobTransitionObserver;
use crate::changeset::RowChange;
use crate::database::DbError;
#[cfg(test)]
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::hlc::Hlc;
use super::status::DeviceActivity;
use super::store::HeldStorePosition;
use super::store::{AuthorizedWriterOperation, Store};
use crate::storage::{
    BlobPathScheme, CloudCipherAccess, CloudCipherState, CloudSyncStorage, PendingRotation,
    RotationPending,
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
            .downcast_ref::<crate::storage::StorageError>()
            .is_some_and(crate::storage::StorageError::is_transport)
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
        let error = super::super::store::StoreRegistrationError::Object(
            crate::storage::StoreObjectError::Storage(crate::storage::StorageError::Storage(
                "provider unavailable".to_string(),
            )),
        );

        let object = std::error::Error::source(&error).expect("object source");
        assert!(object
            .downcast_ref::<crate::storage::StoreObjectError>()
            .is_some());
        let storage = object.source().expect("storage source");
        assert!(storage
            .downcast_ref::<crate::storage::StorageError>()
            .is_some());

        assert!(SyncCycleFailure::operation("register", error).is_offline());
    }

    #[test]
    fn registration_configuration_source_is_failed() {
        let error = super::super::store::StoreRegistrationError::Object(
            crate::storage::StoreObjectError::Storage(crate::storage::StorageError::Configuration(
                "missing bucket".to_string(),
            )),
        );

        assert!(!SyncCycleFailure::operation("register", error).is_offline());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeferredLocalBlobDisposition {
    Drop,
    Cache,
    Pin,
}

impl DeferredLocalBlobDisposition {
    pub(crate) fn as_db(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Cache => "cache",
            Self::Pin => "pin",
        }
    }

    pub(crate) fn from_db(raw: &str) -> Result<Self, String> {
        match raw {
            "drop" => Ok(Self::Drop),
            "cache" => Ok(Self::Cache),
            "pin" => Ok(Self::Pin),
            other => Err(format!(
                "unknown disposition in published blob drop intent: {other}"
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeferredLocalBlobDrop {
    pub namespace: String,
    pub id: String,
    pub size: u64,
    pub plaintext_hash: crate::protocol::store_commit::ObjectHash,
    pub locator_hash: crate::protocol::store_commit::ObjectHash,
    pub disposition: DeferredLocalBlobDisposition,
}

/// Run a single sync cycle: drain pending local changes + gate + push, pull,
/// bookkeeping, snapshot.
///
/// All connection access goes through `db`. Local writes are published from the
/// durable pending-changeset journal; the pull's apply is a plain connection
/// write that is never journaled, so applied rows are never republished as this
/// device's own changes.
/// Loads/persists all cycle state (local_seq, positions, staging, snapshots) through
/// `db`'s bookkeeping API rather than keeping mutable state across calls.
#[cfg(test)]
pub(crate) async fn run_single_sync_cycle(
    storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    db: &crate::database::Database,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    user_keypair: &UserKeypair,
    security: Option<&crate::store_security::StoreSecurity>,
    store_dir: &StoreDir,
    cloud_home: Option<&dyn CloudHome>,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<SyncCycleResult, SyncCycleFailure> {
    let store_database = crate::database::StoreDatabase::new(db);
    let store = Store::load(store_database.clone(), storage, user_keypair.clone())
        .await
        .map_err(|error| SyncCycleFailure::operation("load local Store", error))?;
    let authorization = store
        .authorize_writer()
        .await
        .map_err(|error| SyncCycleFailure::operation("authorize local Store writer", error))?;
    let blob_cache =
        super::store::blob::StoreBlobCache::new(store_database.clone(), store_dir.clone());
    let local_blob_access = super::store::blob::LocalStoreBlobAccess::new(
        store_database,
        store_dir.clone(),
        blob_cache,
    );
    AuthorizedSyncCycle {
        device_id,
        hlc,
        clock,
        cipher,
        pending_rotation,
        security,
        routing_encryption: None,
        store_dir,
        local_blob_access: &local_blob_access,
        cloud_home,
        observer,
        authorization,
    }
    .run()
    .await
}

struct PreparedCycle {
    sync_time: String,
    resume_drain_promptly: bool,
    rotation_pending: Option<RotationPending>,
}

enum CycleBeforePull {
    Continue(PreparedCycle),
    Complete(SyncCycleResult),
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
    hlc: &'cycle Hlc,
    clock: &'cycle dyn crate::clock::Clock,
    cipher: &'cycle dyn CloudCipherAccess,
    pending_rotation: &'cycle PendingRotation,
    security: Option<&'cycle crate::store_security::StoreSecurity>,
    routing_encryption: Option<&'cycle crate::encryption::EncryptionService>,
    store_dir: &'cycle StoreDir,
    local_blob_access: &'cycle super::store::blob::LocalStoreBlobAccess,
    cloud_home: Option<&'cycle dyn CloudHome>,
    observer: Option<&'cycle dyn BlobTransitionObserver>,
    authorization: AuthorizedWriterOperation<'store>,
}

impl AuthorizedSyncCycle<'_, '_> {
    async fn run(mut self) -> Result<SyncCycleResult, SyncCycleFailure> {
        self.authorization
            .resume_operations(self.routing_encryption)
            .await?;
        let prepared = match Box::pin(self.prepare_before_pull()).await? {
            CycleBeforePull::Continue(prepared) => prepared,
            CycleBeforePull::Complete(result) => return Ok(result),
        };
        let store_pull = self
            .authorization
            .pull(self.store_dir, self.routing_encryption)
            .await?;
        let completed = Box::pin(self.complete_after_pull(prepared, store_pull)).await?;
        if completed.rotation_pending.is_none() {
            self.authorization
                .circles()
                .close()
                .publish_circle_epoch_close_responses()
                .await
                .map_err(|error| {
                    SyncCycleFailure::operation("publish Circle epoch-close responses", error)
                })?;
            if let Some(routing_encryption) = self.routing_encryption {
                self.authorization
                    .circles()
                    .close()
                    .finalize_ready_circle_epoch_closes(
                        &completed.sync_time,
                        self.store_dir,
                        routing_encryption,
                    )
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

    async fn prepare_before_pull(&mut self) -> Result<CycleBeforePull, SyncCycleFailure> {
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
            .refresh_authorization_state(self.cipher, self.pending_rotation, self.security)
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

        if let Some(home) = self.cloud_home {
            if rotation_pending.is_none() {
                let drained = self
                    .authorization
                    .drain_tombstones(home, self.cipher, self.pending_rotation, self.clock)
                    .await
                    .map_err(|error| format!("drain queued blob tombstones: {error}"))?;
                if drained > 0 {
                    info!(count = drained, "Drained blob tombstones");
                }
            }
            let reclaimed = self
                .authorization
                .gc_tombstones(home, self.cipher, self.clock)
                .await
                .map_err(|error| format!("garbage-collect blob tombstones: {error}"))?;
            if reclaimed > 0 {
                info!(count = reclaimed, "Reclaimed tombstoned blobs");
            }
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
                .drain_uploads(
                    self.store_dir,
                    self.clock,
                    self.hlc,
                    self.routing_encryption,
                    self.observer,
                )
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

        if self.authorization.should_stop_before_pull().await? {
            return Ok(CycleBeforePull::Complete(SyncCycleResult {
                changesets_applied: 0,
                held_positions: Vec::new(),
                device_activity: Vec::new(),
                sync_time,
                asset_downloads_failed: false,
                local_blob_cleanup_pending: false,
                row_changes: Vec::new(),
                resume_drain_promptly: false,
                rotation_pending,
            }));
        }

        Ok(CycleBeforePull::Continue(PreparedCycle {
            sync_time,
            resume_drain_promptly,
            rotation_pending,
        }))
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
            let published = self
                .authorization
                .publish_pending_store_writes(self.store_dir)
                .await?;
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
            .drain_local_blob_cleanup(self.store_dir)
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
            .persist_hlc_high_water(self.hlc)
            .await
            .map_err(|e| format!("Failed to persist HLC high-water mark: {e}"))?;

        self.authorization
            .publish_due_snapshots(
                self.store_dir,
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
    RowRouting(String),
    #[error("Store protocol root failed: {0}")]
    StoreProtocolRoot(String),
    #[error("membership chain bootstrap/anchor failed: {0}")]
    MembershipAnchor(String),
    #[error("restoring the persisted pending rotation failed: {0}")]
    PendingRotationRestore(String),
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

pub(crate) async fn init_sync_over_storage(
    store_database: &crate::database::StoreDatabase,
    local_blob_access: super::store::blob::LocalStoreBlobAccess,
    storage: impl Into<std::sync::Arc<CloudSyncStorage>>,
    initialization: StoreInitialization,
    routing_encryption: Option<crate::encryption::EncryptionService>,
) -> Result<SyncComponents, InitSyncError> {
    let db = store_database;
    let storage = storage.into();
    // Integration guard. The host declared its synced tables on the builder; an
    // empty set means a synced store would attach nothing, every changeset would
    // come out empty, and sync would silently become snapshot-only. Refuse loudly
    // instead of pretending to sync.
    if db.synced_tables().is_empty() {
        return Err(InitSyncError::NoSyncedTables);
    }
    crate::database::StoreDatabase::validate_store_write_routing(
        db.gates().as_ref(),
        routing_encryption.as_ref(),
    )
    .map_err(|error| InitSyncError::RowRouting(error.into_message()))?;

    let cipher = storage.cipher_state().clone();
    let cipher_is_plaintext = cipher.is_plaintext();
    let representation_is_coherent = matches!(
        (cipher_is_plaintext, storage.blob_path_scheme()),
        (true, BlobPathScheme::Plain) | (false, BlobPathScheme::Hashed)
    );
    if !representation_is_coherent {
        return Err(InitSyncError::IncoherentStorageRepresentation);
    }

    let hlc = db.hlc();
    let user_keypair = storage.user_keypair().clone();
    let store_id = storage.store_id().to_string();
    let store_storage: std::sync::Arc<dyn crate::storage::SyncStorage> = storage.clone();
    let initialized = match initialization {
        StoreInitialization::CreateStore => {
            Store::create(
                store_database.clone(),
                store_storage.clone(),
                &hlc.now().to_string(),
                &user_keypair,
            )
            .await
        }
        StoreInitialization::OpenStore {
            expected_store_root,
        } => {
            Store::open(
                store_database.clone(),
                store_storage.clone(),
                &expected_store_root,
                &user_keypair,
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

    // Restore any durably-recorded pending rotation into this connection's marker
    // before the first cycle seals anything, so a restart that interrupted an
    // unadopted rotation resumes paused rather than sealing under the superseded
    // generation.
    if !cipher_is_plaintext {
        storage
            .shared_pending_rotation()
            .restore_from(db)
            .await
            .map_err(|e| InitSyncError::PendingRotationRestore(e.to_string()))?;
    }

    let pending_rotation = storage.shared_pending_rotation();
    info!("Sync initialized (device: {})", initialized.device_id);

    Ok(SyncComponents {
        store: std::sync::Arc::new(initialized.store),
        database: store_database.clone(),
        local_blob_access,
        storage: store_storage,
        hlc,
        store_id,
        device_id: initialized.device_id,
        cipher,
        pending_rotation,
        routing_encryption,
    })
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
    storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
    hlc: std::sync::Arc<Hlc>,
    /// The store this sync loop is for. Binds the snapshot meta/pointer it
    /// publishes so a member of two stores can't replay one's catalog as the
    /// other's.
    store_id: String,
    device_id: String,
    cipher: std::sync::Arc<CloudCipherState>,
    pending_rotation: std::sync::Arc<PendingRotation>,
    routing_encryption: Option<crate::encryption::EncryptionService>,
}

impl SyncComponents {
    pub(crate) fn store(&self) -> std::sync::Arc<Store> {
        self.store.clone()
    }

    pub(crate) async fn probe_storage(&self) -> Result<(), crate::storage::StorageError> {
        self.storage
            .cloud_home()
            .probe()
            .await
            .map_err(crate::storage::StorageError::from)
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

    #[cfg(test)]
    pub(crate) async fn list_storage_objects_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, crate::storage::StorageError> {
        self.storage
            .cloud_home()
            .list(prefix)
            .await
            .map_err(crate::storage::StorageError::from)
    }

    #[cfg(test)]
    pub(crate) fn uses_storage_for_test(
        &self,
        expected: &std::sync::Arc<dyn crate::storage::SyncStorage>,
    ) -> bool {
        std::sync::Arc::ptr_eq(&self.storage, expected)
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.store.blob_path_scheme()
    }

    pub(crate) fn current_encryption(&self) -> Option<crate::encryption::EncryptionService> {
        self.cipher.encryption()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.store.self_uploader()
    }

    pub(crate) async fn drain_uploads(
        &self,
        clock: &dyn crate::clock::Clock,
        store_dir: &StoreDir,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, DbError> {
        self.store
            .authorize_writer()
            .await
            .map_err(|error| DbError::Message(error.to_string()))?
            .drain_uploads(
                store_dir,
                clock,
                &self.hlc,
                self.routing_encryption.as_ref(),
                observer,
            )
            .await
    }

    pub(crate) async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        store_name: &str,
    ) -> Result<crate::joining::InviteCode, super::store::MembershipOpsError> {
        let encryption = self
            .current_encryption()
            .ok_or(super::store::MembershipOpsError::NotEncryptedHome)?;
        self.store
            .invite_member(
                &self.hlc,
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
        security: &crate::store_security::StoreSecurity,
    ) -> Result<String, super::store::MembershipOpsError> {
        let encryption = self
            .current_encryption()
            .ok_or(super::store::MembershipOpsError::NotEncryptedHome)?;
        self.store
            .remove_member(
                &self.hlc,
                public_key_hex,
                &encryption,
                security,
                &self.cipher,
                &self.pending_rotation,
            )
            .await
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &crate::protocol::membership::MembershipConflictChoice,
    ) -> Result<(), super::store::MembershipOpsError> {
        self.store
            .resolve_membership_conflict(choice, &self.hlc.now().to_string())
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation(
        &self,
        encryption: crate::encryption::EncryptionService,
        security: &crate::store_security::StoreSecurity,
    ) -> Result<String, crate::keys::KeyError> {
        security.adopt_key_rotation(&self.cipher, &encryption)
    }

    pub(crate) async fn create_circle(
        &self,
        name: &str,
    ) -> Result<crate::protocol::circle::CircleId, super::store::CircleOperationError> {
        self.store
            .circles()
            .create_circle(&self.hlc.now().to_string(), name)
            .await
    }

    pub(crate) async fn rename_circle(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        name: &str,
    ) -> Result<(), super::store::CircleOperationError> {
        self.store
            .circles()
            .rename_circle(&self.hlc.now().to_string(), circle_id, name)
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
        store_dir: &StoreDir,
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
            .publish_pending_store_writes(store_dir)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let bootstrap = authorization
            .circles()
            .snapshots()
            .capture_circle_snapshot_cut(
                store_dir.as_ref().to_path_buf(),
                &routing_encryption,
                circle_id,
            )
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
        clock: &dyn crate::clock::Clock,
        security: Option<&crate::store_security::StoreSecurity>,
        store_dir: &StoreDir,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<SyncCycleResult, SyncCycleFailure> {
        let authorization =
            self.store.authorize_writer().await.map_err(|error| {
                SyncCycleFailure::operation("authorize local Store writer", error)
            })?;
        AuthorizedSyncCycle {
            device_id: &self.device_id,
            hlc: &self.hlc,
            clock,
            cipher: &self.cipher,
            pending_rotation: &self.pending_rotation,
            security,
            routing_encryption: self.routing_encryption.as_ref(),
            store_dir,
            local_blob_access: &self.local_blob_access,
            cloud_home: Some(self.storage.cloud_home()),
            observer,
            authorization,
        }
        .run()
        .await
    }
}
