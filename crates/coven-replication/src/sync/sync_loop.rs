//! Sync loop handle: runs the background sync loop on a prepared OS thread.
//!
//! Owns the sync infrastructure (storage client, HLC, the owned [`Database`](coven_database::Database)
//! handle, etc.) and runs sync cycles on a timer or manual trigger. Setup
//! prepares that thread and its current-thread Tokio runtime before Store
//! publication, so installing a connected Store does not construct a runtime
//! or depend on a host-provided one.
//! Publishes the current [`SyncLoopStatus`] through a watch channel the
//! host handle owns — so a subscription survives a loop
//! restart, and the loop only ever sends.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc::error::TrySendError;
use tracing::debug;

use coven_foundation::clock::ClockRef;
use coven_foundation::config::Config;
#[cfg(any(test, feature = "test-utils"))]
use coven_foundation::store_dir::StoreDir;
use coven_foundation::store_dir::StoreOpenGuard;
use coven_protocol::blob::BlobTransitionObserver;

use super::cycle::SyncComponents;
use super::loop_policy::SyncLoopSuccess;
use coven_storage::BlobPathScheme;

mod thread;
pub use thread::PreparedSyncLoopRuntime;
#[cfg(test)]
use thread::{current_success_status, storage_check_failure_status};

/// Why preparing the background sync loop failed.
#[derive(Debug, thiserror::Error)]
pub enum SyncLoopError {
    /// The dedicated sync-loop OS thread could not be spawned.
    #[error("failed to spawn sync loop thread: {0}")]
    ThreadSpawn(std::io::Error),
    /// The dedicated sync-loop thread could not construct its Tokio runtime.
    #[error("failed to create sync loop runtime: {0}")]
    Runtime(Arc<std::io::Error>),
    /// The sync-loop thread panicked; `stop` observed it on join.
    #[error("sync loop thread panicked")]
    ThreadPanicked,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SyncLoopFailure {
    #[error("check sync storage: {0}")]
    Storage(Arc<coven_protocol::objects::StorageError>),
    #[error("sync cycle: {0}")]
    Cycle(Arc<crate::sync::cycle::SyncCycleFailure>),
    #[error("read pending writes after sync: {0}")]
    PendingWrites(Arc<coven_database::DbError>),
    #[error("sync loop panicked")]
    Panicked,
}

/// Creates a ready sync-loop thread and runtime before Store publication.
pub trait SyncLoopRuntimeFactory: Send + Sync {
    /// Prepare the runtime without attaching an initialized Store session.
    fn prepare(&self) -> Result<PreparedSyncLoopRuntime, SyncLoopError>;
}

/// The production sync-loop runtime factory.
pub struct SystemSyncLoopRuntimeFactory;

impl SyncLoopRuntimeFactory for SystemSyncLoopRuntimeFactory {
    fn prepare(&self) -> Result<PreparedSyncLoopRuntime, SyncLoopError> {
        PreparedSyncLoopRuntime::prepare()
    }
}

/// A sync-loop status the host renders. The loop reports provider reachability,
/// publication, and one terminal status. [`Blocked`](Self::Blocked) is a
/// successful storage cycle with unresolved durable writes;
/// [`Synchronized`](Self::Synchronized) has none, while
/// [`Failed`](Self::Failed) means the cycle itself failed. The in-progress marker
/// is the variant itself, so there is no separate "syncing" flag.
///
/// A whole-cycle failure is `Failed`; an otherwise-successful cycle carries its
/// [`SyncLoopSuccess`] in `Synchronized` or `Blocked`. Warnings ride in
/// [`SyncLoopSuccess::alerts`].
///
/// A subscription immediately exposes the current value. Intermediate values may
/// be coalesced when the producer changes state faster than a receiver observes
/// it. A `Synchronized` value's [`SyncLoopSuccess::row_changes`] therefore remains a
/// refresh hint, not a complete change stream.
#[derive(Debug, Clone)]
pub enum SyncLoopStatus {
    /// No provider operation has succeeded for the current connection.
    Offline,
    /// The loop is checking whether storage is reachable.
    CheckingStorage,
    /// Storage is reachable and the cycle may publish local state.
    Publishing,
    /// The cycle completed. Warnings, if any, ride in the success's `alerts`;
    /// the observed device activity and applied row changes are on it too.
    Synchronized(SyncLoopSuccess),
    /// The cycle reached storage, but one or more writes cannot publish until
    /// their named prerequisite is supplied or repaired.
    Blocked {
        success: SyncLoopSuccess,
        writes: Vec<coven_protocol::write::PendingWrite>,
    },
    /// The cycle failed as a whole — no outcome to report, only the fault.
    Failed { error: SyncLoopFailure },
}

/// Manages the background sync loop and provides access to sync components.
pub struct SyncLoopHandle {
    inner: Arc<SyncLoopHandleInner>,
    trigger_tx: tokio::sync::mpsc::Sender<()>,
    command_tx: tokio::sync::mpsc::Sender<SyncCommand>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    eager_cache_cancel_tx: tokio::sync::watch::Sender<bool>,
    activate_tx: tokio::sync::watch::Sender<bool>,
    /// The current status value, owned by the [`CovenHandle`] and cloned into each
    /// loop it starts, so a subscription survives a loop restart (a reconnect
    /// builds a fresh loop but keeps this same sender). The loop only sends here.
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    thread_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
}

struct SyncLoopHandleInner {
    components: SyncComponents,
    clock: ClockRef,
    config: Config,
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// The store-directory lock, held so it releases only when the loop's
    /// thread exits. The running thread keeps a clone of this `SyncLoopHandleInner`
    /// alive across its whole cycle, so the last handle dropping never releases
    /// `.coven-lock` while a mid-cycle pull or upload is still writing — a second
    /// `open()` of the same store stays refused until this writer is gone.
    _open_guard: Arc<StoreOpenGuard>,
}

type CircleReply<T> =
    tokio::sync::oneshot::Sender<Result<T, crate::sync::store::CircleOperationError>>;

enum SyncCommand {
    CreateCircle {
        name: String,
        reply: CircleReply<coven_protocol::CircleId>,
    },
    RenameCircle {
        circle_id: coven_protocol::CircleId,
        name: String,
        reply: CircleReply<()>,
    },
    AddCircleMember {
        circle_id: coven_protocol::CircleId,
        member_pubkey: String,
        role: coven_protocol::CircleRole,
        reply: CircleReply<()>,
    },
    RemoveCircleMember {
        circle_id: coven_protocol::CircleId,
        member_pubkey: String,
        reply: CircleReply<coven_protocol::CircleOperationId>,
    },
    ResolveCircleControl {
        circle_id: coven_protocol::CircleId,
        chosen: coven_protocol::CircleControlCoord,
        reply: CircleReply<()>,
    },
    CancelCircleEpochClose {
        circle_id: coven_protocol::CircleId,
        reply: CircleReply<coven_protocol::CircleOperationId>,
    },
    ExcludeCircleCloseDevice {
        circle_id: coven_protocol::CircleId,
        excluded_device_id: coven_protocol::StoreDeviceId,
        reply: CircleReply<()>,
    },
    DeleteCircle {
        circle_id: coven_protocol::CircleId,
        reply: CircleReply<()>,
    },
    RetryCircleOperation {
        operation_id: coven_protocol::CircleOperationId,
        reply: CircleReply<()>,
    },
    DiscardCircleOperation {
        operation_id: coven_protocol::CircleOperationId,
        reply: CircleReply<()>,
    },
}

impl SyncLoopHandle {
    pub fn new(
        components: SyncComponents,
        clock: ClockRef,
        config: Config,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
        eager_cache_status_tx: tokio::sync::watch::Sender<super::store::EagerCacheFillStatus>,
        runtime: Option<PreparedSyncLoopRuntime>,
    ) -> Self {
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(1);
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(16);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let (eager_cache_cancel_tx, eager_cache_cancel_rx) = tokio::sync::watch::channel(false);
        let (activate_tx, activate_rx) = tokio::sync::watch::channel(false);
        let inner = Arc::new(SyncLoopHandleInner {
            components,
            clock,
            config,
            observer,
            _open_guard: open_guard,
        });
        let running = Arc::new(AtomicBool::new(runtime.is_some()));
        let thread_handle = runtime.map(|runtime| {
            runtime.install(thread::SyncLoopThread::new(
                Arc::clone(&inner),
                trigger_rx,
                command_rx,
                stop_rx,
                eager_cache_cancel_rx,
                activate_rx,
                status_tx.clone(),
                eager_cache_status_tx,
                Arc::clone(&running),
            ))
        });
        Self {
            inner,
            trigger_tx,
            command_tx,
            stop_tx,
            eager_cache_cancel_tx,
            activate_tx,
            status_tx,
            thread_handle: std::sync::Mutex::new(thread_handle),
            running,
        }
    }

    /// Release a prepared loop to begin its normal startup delay and cycles.
    pub fn activate(&self) {
        self.activate_tx.send_replace(true);
    }

    /// The provider-operation counter of the home this loop works through, so
    /// a run driven from outside the loop — an owner-side device-join step —
    /// can report each stage's count beside its wall time.
    pub fn provider_requests(
        &self,
    ) -> Option<Arc<dyn coven_foundation::stage_timing::ProviderRequests>> {
        self.inner.components.provider_requests()
    }

    /// Whether the background sync thread is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Request loop shutdown and join the sync thread.
    pub fn stop(&self) {
        let handle = {
            let mut guard = self.thread_handle.lock().unwrap();
            if guard.is_none() && !self.running.load(Ordering::Acquire) {
                return;
            }
            if self.stop_tx.send(true).is_err() {
                debug!("sync loop stop requested after stop receiver closed");
            }
            self.eager_cache_cancel_tx.send_replace(true);
            self.trigger();
            guard.take()
        };

        if let Some(handle) = handle {
            if handle.join().is_err() {
                self.running.store(false, Ordering::Release);
                let failure = SyncLoopFailure::Panicked;
                self.status_tx
                    .send_replace(SyncLoopStatus::Failed { error: failure });
            }
        }
        self.running.store(false, Ordering::Release);
    }

    /// Signal the sync loop to run a cycle immediately.
    ///
    /// `Full` means a trigger is already pending — our request collapses into the
    /// existing one, which is exactly what the capacity-1 channel is for.
    /// `Closed` means the loop is gone, so the trigger is moot.
    pub fn trigger(&self) {
        match self.trigger_tx.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => {}
            Err(TrySendError::Closed(())) => {
                debug!("Sync trigger channel closed, loop is not running");
            }
        }
    }

    /// Stop the post-open CacheEager fill without stopping cloud sync.
    pub fn cancel_eager_cache_fill(&self) {
        self.eager_cache_cancel_tx.send_replace(true);
    }

    pub async fn discard_blocked_write(
        &self,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<Vec<coven_protocol::write::WriteId>, crate::sync::store::StoreError> {
        self.inner.components.discard_blocked_write(write_id).await
    }

    pub async fn members(
        &self,
    ) -> Result<Vec<coven_protocol::membership::MemberInfo>, super::store::MembershipOpsError> {
        self.inner.components.members().await
    }

    pub async fn membership_conflict(
        &self,
    ) -> Result<Option<coven_protocol::MembershipConflictInfo>, super::store::MembershipOpsError>
    {
        self.inner.components.membership_conflict().await
    }

    pub async fn restore_membership(
        &self,
    ) -> Result<super::store::authorization::StoreRestoreMembership, super::store::MembershipOpsError>
    {
        self.inner.components.restore_membership().await
    }

    pub fn host_write_blob_staging(
        &self,
        runtime: tokio::runtime::Handle,
    ) -> crate::sync::store::HostWriteBlobStaging {
        self.inner.components.host_write_blob_staging(runtime)
    }

    pub async fn propose_device_exclusion(
        &self,
        device_id: coven_protocol::StoreDeviceId,
    ) -> Result<
        coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        crate::sync::store::StoreDeviceExclusionError,
    > {
        self.inner
            .components
            .propose_device_exclusion(device_id)
            .await
    }

    pub async fn cancel_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), crate::sync::store::StoreDeviceExclusionError> {
        self.inner
            .components
            .cancel_device_exclusion(proposal)
            .await
    }

    pub async fn finalize_device_exclusion(
        &self,
        proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), crate::sync::store::StoreDeviceExclusionError> {
        self.inner
            .components
            .finalize_device_exclusion(proposal)
            .await
    }

    pub async fn begin_owner_promotion(
        &self,
        device_id: coven_protocol::StoreDeviceId,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionRequest,
        crate::sync::store::OwnerPromotionError,
    > {
        self.inner.components.begin_owner_promotion(device_id).await
    }

    pub async fn accept_owner_promotion(
        &self,
        request: coven_protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<
        coven_protocol::store_commit::OwnerPromotionAcceptance,
        crate::sync::store::OwnerPromotionError,
    > {
        self.inner.components.accept_owner_promotion(request).await
    }

    pub async fn finalize_owner_promotion(
        &self,
        acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), crate::sync::store::OwnerPromotionError> {
        self.inner
            .components
            .finalize_owner_promotion(acceptance)
            .await
    }

    pub async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::sync::DeviceJoinOfferBundle, crate::sync::store::DeviceJoinTransportError>
    {
        self.inner
            .components
            .begin_device_join_bundle(member_pubkey)
            .await
    }

    pub async fn drive_device_join(
        &self,
        bundle: &crate::sync::DeviceJoinOfferBundle,
        policy: crate::sync::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::sync::DeviceProviderAccessAdministrator>,
        on_progress: &(dyn Fn(crate::sync::AdmittingDeviceJoinProgress) + Send + Sync),
        timing: crate::sync::DeviceJoinTransportTiming,
    ) -> Result<crate::sync::DeviceJoinDriveOutcome, crate::sync::store::DeviceJoinTransportError>
    {
        self.inner
            .components
            .drive_device_join(bundle, policy, access_administrator, on_progress, timing)
            .await
    }

    pub async fn abandon_device_join_transport(
        &self,
        bundle: &crate::sync::DeviceJoinOfferBundle,
    ) -> Result<crate::sync::DeviceJoinAbandonment, crate::sync::store::DeviceJoinTransportError>
    {
        self.inner
            .components
            .abandon_device_join_transport(bundle)
            .await
    }

    pub async fn abort_device_join_transport(
        &self,
        bundle: &crate::sync::DeviceJoinOfferBundle,
    ) -> Result<(), crate::sync::store::DeviceJoinTransportError> {
        self.inner
            .components
            .abort_device_join_transport(bundle)
            .await
    }

    pub async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::sync::DeviceJoinOffer, crate::sync::DeviceJoinError> {
        self.inner.components.begin_device_join(member_pubkey).await
    }

    pub async fn abandon_device_join(
        &self,
        offer: crate::sync::DeviceJoinOffer,
    ) -> Result<crate::sync::DeviceJoinAbandonment, crate::sync::DeviceJoinError> {
        self.inner.components.abandon_device_join(offer).await
    }

    pub async fn authorize_device_provider_access(
        &self,
        request: crate::sync::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::sync::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::sync::DeviceProviderAdmissionApproval, crate::sync::DeviceJoinError> {
        self.inner
            .components
            .authorize_device_provider_access(request, access_administrator)
            .await
    }

    pub async fn accept_device_registration(
        &self,
        request: crate::sync::DeviceRegistrationRequest,
    ) -> Result<crate::sync::ProvisionalDeviceBootstrap, crate::sync::DeviceJoinError> {
        self.inner
            .components
            .accept_device_registration(request)
            .await
    }

    pub async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::sync::ProvisionalDeviceBootstrap,
    ) -> Result<crate::sync::ProviderReadyDeviceBootstrap, crate::sync::DeviceJoinError> {
        self.inner
            .components
            .publish_device_provider_challenge(bootstrap)
            .await
    }

    pub async fn complete_device_provider_admission(
        &self,
        readiness: crate::sync::DeviceJoinReadiness,
    ) -> Result<crate::sync::DeviceProviderAdmissionCompletion, crate::sync::DeviceJoinError> {
        self.inner
            .components
            .complete_device_provider_admission(readiness)
            .await
    }

    pub async fn finalize_device_join(
        &self,
        completion: crate::sync::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::sync::DeviceJoinActivation, crate::sync::DeviceJoinError> {
        self.inner.components.finalize_device_join(completion).await
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn blob_path_scheme(&self) -> BlobPathScheme {
        self.inner.components.blob_path_scheme()
    }

    pub fn is_encrypted(&self) -> bool {
        self.inner.components.is_encrypted()
    }

    pub async fn admit_member(
        &self,
        public_key_hex: &str,
        member_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        store_name: &str,
    ) -> Result<crate::sync::store::MemberAdmission, super::store::MembershipOpsError> {
        self.inner
            .components
            .admit_member(public_key_hex, member_email, role, store_name)
            .await
    }

    pub async fn remove_member(
        &self,
        public_key_hex: &str,
    ) -> Result<String, super::store::MembershipOpsError> {
        self.inner.components.remove_member(public_key_hex).await
    }

    pub async fn resolve_membership_conflict(
        &self,
        choice: &coven_protocol::MembershipConflictChoice,
    ) -> Result<(), super::store::MembershipOpsError> {
        self.inner
            .components
            .resolve_membership_conflict(choice)
            .await
    }

    pub async fn drain_uploads(
        &self,
    ) -> Result<crate::blob::DrainOutcome, super::store::StoreError> {
        self.inner
            .components
            .drain_uploads(self.inner.clock.as_ref(), self.inner.observer.as_deref())
            .await
    }

    pub async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        root_label: &str,
        pin: bool,
        refs: Vec<coven_protocol::blob::RowBlobRef>,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.inner
            .components
            .make_remote(root_table, root_id, root_label, pin, refs)
            .await
    }

    pub async fn make_remote_batch(
        &self,
        root_table: &str,
        roots: Vec<crate::blob::MakeRemoteRoot>,
        pin: bool,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.inner
            .components
            .make_remote_batch(root_table, roots, pin)
            .await
    }

    pub async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.inner
            .components
            .cancel_make_remote(root_table, root_id)
            .await
    }

    pub async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &std::collections::HashMap<String, std::path::PathBuf>,
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), crate::blob::transition::MakeLocalError> {
        self.inner
            .components
            .make_local(root_table, root_id, dest, cancel)
            .await
    }

    /// Send a Circle write command to the loop thread and await its reply. Circle
    /// writes run on the loop thread so they never interleave with a sync cycle.
    async fn send_circle_command<T>(
        &self,
        command: impl FnOnce(CircleReply<T>) -> SyncCommand,
    ) -> Result<T, crate::sync::store::CircleOperationError> {
        let (reply, response) = tokio::sync::oneshot::channel();
        self.command_tx
            .send(command(reply))
            .await
            .map_err(|_| crate::sync::store::CircleOperationError::CommandChannelClosed)?;
        response
            .await
            .map_err(|_| crate::sync::store::CircleOperationError::ReplyChannelClosed)?
    }

    pub async fn create_circle(
        &self,
        name: &str,
    ) -> Result<coven_protocol::CircleId, crate::sync::store::CircleOperationError> {
        let name = name.to_string();
        self.send_circle_command(|reply| SyncCommand::CreateCircle { name, reply })
            .await
    }

    pub async fn rename_circle(
        &self,
        circle_id: coven_protocol::CircleId,
        name: &str,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        let name = name.to_string();
        self.send_circle_command(|reply| SyncCommand::RenameCircle {
            circle_id,
            name,
            reply,
        })
        .await
    }

    pub async fn add_circle_member(
        &self,
        circle_id: coven_protocol::CircleId,
        member_pubkey: String,
        role: coven_protocol::CircleRole,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::AddCircleMember {
            circle_id,
            member_pubkey,
            role,
            reply,
        })
        .await
    }

    pub async fn remove_circle_member(
        &self,
        circle_id: coven_protocol::CircleId,
        member_pubkey: String,
    ) -> Result<coven_protocol::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::RemoveCircleMember {
            circle_id,
            member_pubkey,
            reply,
        })
        .await
    }

    pub async fn resolve_circle_control(
        &self,
        circle_id: coven_protocol::CircleId,
        chosen: coven_protocol::CircleControlCoord,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::ResolveCircleControl {
            circle_id,
            chosen,
            reply,
        })
        .await
    }

    pub async fn cancel_circle_epoch_close(
        &self,
        circle_id: coven_protocol::CircleId,
    ) -> Result<coven_protocol::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::CancelCircleEpochClose { circle_id, reply })
            .await
    }

    pub async fn exclude_circle_close_device(
        &self,
        circle_id: coven_protocol::CircleId,
        excluded_device_id: coven_protocol::StoreDeviceId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::ExcludeCircleCloseDevice {
            circle_id,
            excluded_device_id,
            reply,
        })
        .await
    }

    pub async fn delete_circle(
        &self,
        circle_id: coven_protocol::CircleId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::DeleteCircle { circle_id, reply })
            .await
    }

    pub async fn retry_circle_operation(
        &self,
        operation_id: coven_protocol::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::RetryCircleOperation {
            operation_id,
            reply,
        })
        .await
    }

    pub async fn discard_circle_operation(
        &self,
        operation_id: coven_protocol::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::DiscardCircleOperation {
            operation_id,
            reply,
        })
        .await
    }

    /// Inspect a Circle's in-flight epoch close. A read, so it runs directly on the
    /// components rather than serializing behind the write-command channel.
    pub async fn circle_close_status(
        &self,
        circle_id: coven_protocol::CircleId,
    ) -> Result<coven_protocol::CircleCloseStatus, crate::sync::store::CircleOperationError> {
        self.inner.components.circle_close_status(circle_id).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn uses_storage_for_test(
        &self,
        expected: &Arc<dyn coven_storage::CloudSyncObjectStorage>,
    ) -> bool {
        self.inner.components.uses_storage_for_test(expected)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn uses_store_dir_for_test(&self, expected: &StoreDir) -> bool {
        self.inner.components.uses_store_dir_for_test(expected)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn encryption_generation_for_test(&self) -> Option<u64> {
        self.inner.components.encryption_generation_for_test()
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
        self.inner
            .components
            .open_sealed_blob_for_test(stored, aad_context)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn adopt_key_rotation_for_test(
        &self,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> Result<String, coven_keys::keys::KeyError> {
        self.inner.components.adopt_key_rotation(encryption)
    }
}

impl SyncLoopHandleInner {
    async fn execute_command(&self, command: SyncCommand) {
        match command {
            SyncCommand::CreateCircle { name, reply } => {
                reply_circle_command(reply, self.components.create_circle(&name).await);
            }
            SyncCommand::RenameCircle {
                circle_id,
                name,
                reply,
            } => {
                reply_circle_command(reply, self.components.rename_circle(circle_id, &name).await);
            }
            SyncCommand::AddCircleMember {
                circle_id,
                member_pubkey,
                role,
                reply,
            } => {
                reply_circle_command(
                    reply,
                    self.components
                        .add_circle_member(circle_id, member_pubkey, role)
                        .await,
                );
            }
            SyncCommand::RemoveCircleMember {
                circle_id,
                member_pubkey,
                reply,
            } => {
                reply_circle_command(
                    reply,
                    self.components
                        .remove_circle_member(circle_id, member_pubkey)
                        .await,
                );
            }
            SyncCommand::ResolveCircleControl {
                circle_id,
                chosen,
                reply,
            } => {
                reply_circle_command(
                    reply,
                    self.components
                        .resolve_circle_control(circle_id, chosen)
                        .await,
                );
            }
            SyncCommand::CancelCircleEpochClose { circle_id, reply } => {
                reply_circle_command(
                    reply,
                    self.components.cancel_circle_epoch_close(circle_id).await,
                );
            }
            SyncCommand::ExcludeCircleCloseDevice {
                circle_id,
                excluded_device_id,
                reply,
            } => {
                reply_circle_command(
                    reply,
                    self.components
                        .exclude_circle_close_device(circle_id, excluded_device_id)
                        .await,
                );
            }
            SyncCommand::DeleteCircle { circle_id, reply } => {
                reply_circle_command(reply, self.components.delete_circle(circle_id).await);
            }
            SyncCommand::RetryCircleOperation {
                operation_id,
                reply,
            } => {
                reply_circle_command(
                    reply,
                    self.components.retry_circle_operation(&operation_id).await,
                );
            }
            SyncCommand::DiscardCircleOperation {
                operation_id,
                reply,
            } => {
                reply_circle_command(
                    reply,
                    self.components
                        .discard_circle_operation(&operation_id)
                        .await,
                );
            }
        }
    }

    async fn run_single_cycle(
        &self,
    ) -> Result<super::cycle::SyncCycleResult, super::cycle::SyncCycleFailure> {
        self.components
            .run_cycle(self.clock.as_ref(), self.observer.as_deref())
            .await
    }
}

fn reply_circle_command<T>(
    reply: CircleReply<T>,
    result: Result<T, crate::sync::store::CircleOperationError>,
) {
    if reply.send(result).is_err() {
        debug!("Circle command caller dropped its reply receiver");
    }
}

#[cfg(test)]
#[path = "sync_loop_tests.rs"]
mod tests;
