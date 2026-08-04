//! Sync loop handle: runs the background sync loop on a dedicated OS thread.
//!
//! Owns the sync infrastructure (storage client, HLC, the owned [`Database`]
//! handle, etc.) and runs sync cycles on a timer or manual trigger. Its
//! dedicated OS thread owns the current-thread Tokio runtime, so starting the
//! loop does not depend on a host-provided async runtime.
//! Publishes the current [`SyncLoopStatus`] through a watch channel the
//! [`CovenHandle`](crate::CovenHandle) owns — so a subscription survives a loop
//! restart, and the loop only ever sends.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, error, info};

use crate::blob::BlobTransitionObserver;
use crate::clock::ClockRef;
use crate::config::Config;
use crate::coven::StoreOpenGuard;
#[cfg(test)]
use crate::store_dir::StoreDir;
use crate::store_security::StoreSecurity;

use super::cycle::SyncComponents;
use super::loop_policy::{self, LoopWait, SyncLoopReport, SyncLoopSuccess};
use crate::storage::BlobPathScheme;

/// Why starting or stopping the background sync loop failed.
#[derive(Debug, thiserror::Error)]
pub enum SyncLoopError {
    /// `start` was called on a handle whose channels a prior `start` already
    /// took: a stopped loop's handle is not restartable.
    #[error("sync loop cannot be restarted after its channels were taken")]
    NotRestartable,
    /// The dedicated sync-loop OS thread could not be spawned.
    #[error("failed to spawn sync loop thread: {0}")]
    ThreadSpawn(std::io::Error),
    /// The sync-loop thread panicked; `stop` observed it on join.
    #[error("sync loop thread panicked")]
    ThreadPanicked,
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
        writes: Vec<crate::PendingWrite>,
    },
    /// The cycle failed as a whole — no outcome to report, only the fault.
    Failed { error: String },
}

/// Manages the background sync loop and provides access to sync components.
pub(crate) struct SyncLoopHandle {
    inner: Arc<SyncLoopHandleInner>,
    trigger_tx: tokio::sync::mpsc::Sender<()>,
    trigger_rx: std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>,
    command_tx: tokio::sync::mpsc::Sender<SyncCommand>,
    command_rx: std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<SyncCommand>>>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    stop_rx: std::sync::Mutex<Option<tokio::sync::watch::Receiver<bool>>>,
    /// The current status value, owned by the [`CovenHandle`] and cloned into each
    /// loop it starts, so a subscription survives a loop restart (a reconnect
    /// builds a fresh loop but keeps this same sender). The loop only sends here.
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    thread_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
}

struct SyncLoopHandleInner {
    components: SyncComponents,
    blob_transitions: crate::blob::transition::ConnectedBlobTransitions,
    security: StoreSecurity,
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
        reply: CircleReply<crate::CircleId>,
    },
    RenameCircle {
        circle_id: crate::CircleId,
        name: String,
        reply: CircleReply<()>,
    },
    AddCircleMember {
        circle_id: crate::CircleId,
        member_pubkey: String,
        role: crate::CircleRole,
        reply: CircleReply<()>,
    },
    RemoveCircleMember {
        circle_id: crate::CircleId,
        member_pubkey: String,
        reply: CircleReply<crate::CircleOperationId>,
    },
    ResolveCircleControl {
        circle_id: crate::CircleId,
        chosen: crate::CircleControlCoord,
        reply: CircleReply<()>,
    },
    CancelCircleEpochClose {
        circle_id: crate::CircleId,
        reply: CircleReply<crate::CircleOperationId>,
    },
    ExcludeCircleCloseDevice {
        circle_id: crate::CircleId,
        excluded_device_id: crate::StoreDeviceId,
        reply: CircleReply<()>,
    },
    DeleteCircle {
        circle_id: crate::CircleId,
        reply: CircleReply<()>,
    },
    RetryCircleOperation {
        operation_id: crate::CircleOperationId,
        reply: CircleReply<()>,
    },
    DiscardCircleOperation {
        operation_id: crate::CircleOperationId,
        reply: CircleReply<()>,
    },
}

impl SyncLoopHandle {
    pub(crate) fn new(
        components: SyncComponents,
        blob_transitions: crate::blob::transition::ConnectedBlobTransitions,
        security: StoreSecurity,
        clock: ClockRef,
        config: Config,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    ) -> Self {
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(1);
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(16);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        Self {
            inner: Arc::new(SyncLoopHandleInner {
                components,
                blob_transitions,
                security,
                clock,
                config,
                observer,
                _open_guard: open_guard,
            }),
            trigger_tx,
            trigger_rx: std::sync::Mutex::new(Some(trigger_rx)),
            command_tx,
            command_rx: std::sync::Mutex::new(Some(command_rx)),
            stop_tx,
            stop_rx: std::sync::Mutex::new(Some(stop_rx)),
            status_tx,
            thread_handle: std::sync::Mutex::new(None),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the background sync loop on a dedicated OS thread. No-op if already
    /// running.
    ///
    /// The thread runs a current-thread Tokio runtime that `block_on`s the loop.
    /// S3 work runs on the runtime retained by the S3 provider, so this thread
    /// needs no provider-specific stack configuration.
    pub(crate) fn start(&self) -> Result<(), SyncLoopError> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let mut trigger_rx = match self.trigger_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => {
                self.running.store(false, Ordering::Release);
                return Err(SyncLoopError::NotRestartable);
            }
        };
        let mut stop_rx = match self.stop_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => {
                self.running.store(false, Ordering::Release);
                return Err(SyncLoopError::NotRestartable);
            }
        };
        let mut command_rx = match self.command_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => {
                self.running.store(false, Ordering::Release);
                return Err(SyncLoopError::NotRestartable);
            }
        };

        let inner = Arc::clone(&self.inner);
        let status_tx = self.status_tx.clone();
        let running = Arc::clone(&self.running);

        let handle = std::thread::Builder::new()
            .name("coven-sync-loop".to_string())
            .spawn(move || {
                let _running_guard = RunningGuard {
                    running: Arc::clone(&running),
                };
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let error = format!("failed to create sync loop runtime: {e}");
                        error!("{error}");
                        status_tx.send_replace(SyncLoopStatus::Failed { error });
                        return;
                    }
                };

                rt.block_on(async move {
                    // Short delay to avoid racing with app startup.
                    let startup_delay = tokio::time::sleep(Duration::from_secs(3));
                    tokio::pin!(startup_delay);
                    loop {
                        tokio::select! {
                            _ = &mut startup_delay => break,
                            changed = stop_rx.changed() => {
                                if changed.is_err() || *stop_rx.borrow() {
                                    info!("Sync loop stopped before first cycle");
                                    return;
                                }
                            }
                            command = command_rx.recv() => {
                                let Some(command) = command else {
                                    info!("Sync command channel closed before first cycle");
                                    return;
                                };
                                inner.execute_command(command).await;
                            }
                        }
                    }

                    let mut consecutive_failures: u32 = 0;
                    while running.load(Ordering::Acquire) && !*stop_rx.borrow() {
                        status_tx.send_replace(SyncLoopStatus::CheckingStorage);
                        let reachable = inner.components.probe_storage().await;
                        let (decision, status) = match reachable {
                            Err(error) => {
                                let status = storage_check_failure_status(&error);
                                let decision = loop_policy::after_failure(
                                    format!("check sync storage: {error}"),
                                    consecutive_failures,
                                    300,
                                );
                                (decision, status)
                            }
                            Ok(_) => {
                                status_tx.send_replace(SyncLoopStatus::Publishing);
                                let (decision, cycle_went_offline) = match inner.run_single_cycle().await {
                                    Ok(result) => (loop_policy::after_success(result), false),
                                    Err(error) => {
                                        let offline = error.is_offline();
                                        (
                                            loop_policy::after_failure(
                                                error.to_string(),
                                                consecutive_failures,
                                                300,
                                            ),
                                            offline,
                                        )
                                    }
                                };
                                let status = match &decision.report {
                                    SyncLoopReport::Success(success) => {
                                        let projected = match inner
                                            .components
                                            .pending_blocked_writes()
                                            .await
                                        {
                                            Ok(writes) => current_success_status(writes, success.clone()),
                                            Err(error) => Err(format!("read pending writes after sync: {error}")),
                                        };
                                        match projected {
                                            Ok(status) => status,
                                            Err(error) => SyncLoopStatus::Failed { error },
                                        }
                                    }
                                    SyncLoopReport::Failure(_) if cycle_went_offline => {
                                        SyncLoopStatus::Offline
                                    }
                                    SyncLoopReport::Failure(error) => SyncLoopStatus::Failed {
                                        error: error.clone(),
                                    },
                                };
                                (decision, status)
                            }
                        };
                        consecutive_failures = decision.consecutive_failures;
                        status_tx.send_replace(status);

                        let wait = match decision.wait {
                            LoopWait::Immediate => Duration::ZERO,
                            LoopWait::Idle => Duration::from_secs(super::backoff::backoff_secs(0, 300)),
                            LoopWait::BackoffSecs(secs) => Duration::from_secs(secs),
                        };
                        if matches!(decision.wait, LoopWait::BackoffSecs(_)) {
                            debug!(
                                "Backing off {wait:?} after {consecutive_failures} consecutive failure(s)",
                            );
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => {}
                            changed = stop_rx.changed() => {
                                if changed.is_err() || *stop_rx.borrow() {
                                    info!("Sync loop stop requested");
                                    break;
                                }
                            }
                            msg = trigger_rx.recv() => {
                                if msg.is_none() {
                                    info!("Sync trigger channel closed, stopping sync loop");
                                    break;
                                }
                            }
                            command = command_rx.recv() => {
                                let Some(command) = command else {
                                    info!("Sync command channel closed, stopping sync loop");
                                    break;
                                };
                                inner.execute_command(command).await;
                            }
                        }
                    }
                });
            })
            .map_err(|e| {
                self.running.store(false, Ordering::Release);
                SyncLoopError::ThreadSpawn(e)
            })?;

        *self.thread_handle.lock().unwrap() = Some(handle);
        Ok(())
    }

    /// Whether the background sync thread is running.
    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Request loop shutdown and join the sync thread.
    pub(crate) fn stop(&self) -> Result<(), SyncLoopError> {
        let handle = {
            let mut guard = self.thread_handle.lock().unwrap();
            if guard.is_none() && !self.running.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.stop_tx.send(true).is_err() {
                debug!("sync loop stop requested after stop receiver closed");
            }
            self.trigger();
            guard.take()
        };

        if let Some(handle) = handle {
            if handle.join().is_err() {
                self.running.store(false, Ordering::Release);
                return Err(SyncLoopError::ThreadPanicked);
            }
        }
        self.running.store(false, Ordering::Release);
        Ok(())
    }

    /// Signal the sync loop to run a cycle immediately.
    ///
    /// `Full` means a trigger is already pending — our request collapses into the
    /// existing one, which is exactly what the capacity-1 channel is for.
    /// `Closed` means the loop is gone, so the trigger is moot.
    pub(crate) fn trigger(&self) {
        match self.trigger_tx.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => {}
            Err(TrySendError::Closed(())) => {
                debug!("Sync trigger channel closed, loop is not running");
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn uses_storage_for_test(
        &self,
        expected: &Arc<dyn crate::storage::SyncStorage>,
    ) -> bool {
        self.inner.components.uses_storage_for_test(expected)
    }

    pub(crate) async fn discard_blocked_write(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Vec<crate::WriteId>, crate::sync::store::StoreError> {
        self.inner.components.discard_blocked_write(write_id).await
    }

    pub(crate) async fn members(
        &self,
    ) -> Result<Vec<crate::protocol::membership::MemberInfo>, super::store::MembershipOpsError>
    {
        self.inner.components.members().await
    }

    pub(crate) async fn membership_conflict(
        &self,
    ) -> Result<Option<crate::MembershipConflictInfo>, super::store::MembershipOpsError> {
        self.inner.components.membership_conflict().await
    }

    pub(crate) async fn restore_membership(
        &self,
    ) -> Result<super::store::owner::StoreRestoreMembership, super::store::MembershipOpsError> {
        self.inner.components.restore_membership().await
    }

    pub(crate) fn host_write_blob_staging(
        &self,
        runtime: tokio::runtime::Handle,
    ) -> crate::sync::store::HostWriteBlobStaging {
        self.inner.components.host_write_blob_staging(runtime)
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::StoreDeviceExclusionProposalRef, String> {
        self.inner
            .components
            .propose_device_exclusion(device_id)
            .await
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), String> {
        self.inner
            .components
            .cancel_device_exclusion(proposal)
            .await
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        proposal: &crate::protocol::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<(), String> {
        self.inner
            .components
            .finalize_device_exclusion(proposal)
            .await
    }

    pub(crate) async fn begin_owner_promotion(
        &self,
        device_id: crate::StoreDeviceId,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionRequest, String> {
        self.inner.components.begin_owner_promotion(device_id).await
    }

    pub(crate) async fn accept_owner_promotion(
        &self,
        request: crate::protocol::store_commit::OwnerPromotionRequest,
    ) -> Result<crate::protocol::store_commit::OwnerPromotionAcceptance, String> {
        self.inner.components.accept_owner_promotion(request).await
    }

    pub(crate) async fn finalize_owner_promotion(
        &self,
        acceptance: crate::protocol::store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), String> {
        self.inner
            .components
            .finalize_owner_promotion(acceptance)
            .await
    }

    pub(crate) async fn begin_device_join_bundle(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOfferBundle, crate::sync::store::DeviceJoinTransportError> {
        self.inner
            .components
            .begin_device_join_bundle(member_pubkey)
            .await
    }

    pub(crate) async fn drive_device_join(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinDriveOutcome, crate::sync::store::DeviceJoinTransportError> {
        self.inner
            .components
            .drive_device_join(bundle, policy, access_administrator, timing)
            .await
    }

    pub(crate) async fn cancel_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
        timing: crate::DeviceJoinTransportTiming,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::sync::store::DeviceJoinTransportError>
    {
        self.inner
            .components
            .cancel_device_join_transport(bundle, timing)
            .await
    }

    pub(crate) async fn abandon_device_join_transport(
        &self,
        bundle: &crate::DeviceJoinOfferBundle,
    ) -> Result<crate::DeviceJoinAbandonment, crate::sync::store::DeviceJoinTransportError> {
        self.inner
            .components
            .abandon_device_join_transport(bundle)
            .await
    }

    pub(crate) async fn begin_device_join(
        &self,
        member_pubkey: &str,
    ) -> Result<crate::DeviceJoinOffer, crate::DeviceJoinError> {
        self.inner.components.begin_device_join(member_pubkey).await
    }

    pub(crate) async fn abandon_device_join(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceJoinAbandonment, crate::DeviceJoinError> {
        self.inner.components.abandon_device_join(offer).await
    }

    pub(crate) async fn authorize_device_provider_access(
        &self,
        request: crate::DeviceProviderAccessRequest,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
    ) -> Result<crate::DeviceProviderAdmissionApproval, crate::DeviceJoinError> {
        self.inner
            .components
            .authorize_device_provider_access(request, access_administrator)
            .await
    }

    pub(crate) async fn accept_device_registration(
        &self,
        request: crate::DeviceRegistrationRequest,
    ) -> Result<crate::ProvisionalDeviceBootstrap, crate::DeviceJoinError> {
        self.inner
            .components
            .accept_device_registration(request)
            .await
    }

    pub(crate) async fn publish_device_provider_challenge(
        &self,
        bootstrap: crate::ProvisionalDeviceBootstrap,
    ) -> Result<crate::ProviderReadyDeviceBootstrap, crate::DeviceJoinError> {
        self.inner
            .components
            .publish_device_provider_challenge(bootstrap)
            .await
    }

    pub(crate) async fn complete_device_provider_admission(
        &self,
        readiness: crate::DeviceJoinReadiness,
    ) -> Result<crate::DeviceProviderAdmissionCompletion, crate::DeviceJoinError> {
        self.inner
            .components
            .complete_device_provider_admission(readiness)
            .await
    }

    pub(crate) async fn finalize_device_join(
        &self,
        completion: crate::DeviceProviderAdmissionCompletion,
    ) -> Result<crate::DeviceJoinActivation, crate::DeviceJoinError> {
        self.inner.components.finalize_device_join(completion).await
    }

    pub(crate) async fn cancel_device_join(
        &self,
        attempt: crate::DeviceJoinAttemptRef,
    ) -> Result<crate::DeviceJoinCancellation, crate::DeviceJoinError> {
        self.inner.components.cancel_device_join(attempt).await
    }

    pub(crate) async fn close_device_provider_admission(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.inner
            .components
            .close_device_provider_admission(cancellation)
            .await
    }

    pub(crate) async fn revoke_device_provider_admission_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::ProviderAdminJoinTerminal, crate::DeviceJoinError> {
        self.inner
            .components
            .revoke_device_provider_admission_writes(cancellation, executor)
            .await
    }

    pub(crate) async fn revoke_joining_device_writes(
        &self,
        cancellation: crate::DeviceJoinCancellation,
        executor: &dyn crate::DeviceJoinWriteRevocationExecutor,
    ) -> Result<crate::JoinerJoinTerminal, crate::DeviceJoinError> {
        self.inner
            .components
            .revoke_joining_device_writes(cancellation, executor)
            .await
    }

    pub(crate) async fn activate_device_join_cleanup(
        &self,
        receipt: crate::DeviceJoinCleanupReceipt,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::DeviceJoinError> {
        self.inner
            .components
            .activate_device_join_cleanup(receipt)
            .await
    }

    pub(crate) async fn complete_owner_device_join_cleanup(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<crate::DeviceJoinCleanupActivation, crate::DeviceJoinError> {
        self.inner
            .components
            .complete_owner_device_join_cleanup(activation)
            .await
    }

    #[cfg(test)]
    pub(crate) fn uses_store_dir_for_test(&self, expected: &StoreDir) -> bool {
        &self.inner.config.store_dir == expected
    }

    pub(crate) fn config(&self) -> &Config {
        &self.inner.config
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.inner.components.blob_path_scheme()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.inner.components.self_uploader()
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        self.inner.components.is_encrypted()
    }

    #[cfg(test)]
    pub(crate) fn encryption_generation_for_test(&self) -> Option<u64> {
        self.inner.components.encryption_generation_for_test()
    }

    #[cfg(test)]
    pub(crate) fn open_sealed_blob_for_test(
        &self,
        stored: &[u8],
        aad_context: &[u8],
    ) -> Result<(crate::encryption::KeyFingerprint, Vec<u8>), String> {
        self.inner
            .components
            .open_sealed_blob_for_test(stored, aad_context)
    }

    pub(crate) async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: crate::protocol::membership::MemberRole,
        store_name: &str,
    ) -> Result<crate::joining::InviteCode, super::store::MembershipOpsError> {
        self.inner
            .components
            .invite_member(public_key_hex, invitee_email, role, store_name)
            .await
    }

    pub(crate) async fn remove_member(
        &self,
        public_key_hex: &str,
    ) -> Result<String, super::store::MembershipOpsError> {
        self.inner
            .components
            .remove_member(public_key_hex, &self.inner.security)
            .await
    }

    pub(crate) async fn resolve_membership_conflict(
        &self,
        choice: &crate::MembershipConflictChoice,
    ) -> Result<(), super::store::MembershipOpsError> {
        self.inner
            .components
            .resolve_membership_conflict(choice)
            .await
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation_for_test(
        &self,
        encryption: crate::encryption::EncryptionService,
    ) -> Result<String, crate::keys::KeyError> {
        self.inner
            .components
            .adopt_key_rotation(encryption, &self.inner.security)
    }

    pub(crate) async fn drain_uploads(
        &self,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        self.inner
            .components
            .drain_uploads(self.inner.clock.as_ref(), self.inner.observer.as_deref())
            .await
    }

    pub(crate) async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.inner
            .blob_transitions
            .make_remote(root_table, root_id, pin)
            .await
    }

    pub(crate) async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), crate::blob::transition::MakeRemoteError> {
        self.inner
            .blob_transitions
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
        self.inner
            .blob_transitions
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

    pub(crate) async fn create_circle(
        &self,
        name: &str,
    ) -> Result<crate::CircleId, crate::sync::store::CircleOperationError> {
        let name = name.to_string();
        self.send_circle_command(|reply| SyncCommand::CreateCircle { name, reply })
            .await
    }

    pub(crate) async fn rename_circle(
        &self,
        circle_id: crate::CircleId,
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

    pub(crate) async fn add_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
        role: crate::CircleRole,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::AddCircleMember {
            circle_id,
            member_pubkey,
            role,
            reply,
        })
        .await
    }

    pub(crate) async fn remove_circle_member(
        &self,
        circle_id: crate::CircleId,
        member_pubkey: String,
    ) -> Result<crate::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::RemoveCircleMember {
            circle_id,
            member_pubkey,
            reply,
        })
        .await
    }

    pub(crate) async fn resolve_circle_control(
        &self,
        circle_id: crate::CircleId,
        chosen: crate::CircleControlCoord,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::ResolveCircleControl {
            circle_id,
            chosen,
            reply,
        })
        .await
    }

    pub(crate) async fn cancel_circle_epoch_close(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleOperationId, crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::CancelCircleEpochClose { circle_id, reply })
            .await
    }

    pub(crate) async fn exclude_circle_close_device(
        &self,
        circle_id: crate::CircleId,
        excluded_device_id: crate::StoreDeviceId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::ExcludeCircleCloseDevice {
            circle_id,
            excluded_device_id,
            reply,
        })
        .await
    }

    pub(crate) async fn delete_circle(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::DeleteCircle { circle_id, reply })
            .await
    }

    pub(crate) async fn retry_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::RetryCircleOperation {
            operation_id,
            reply,
        })
        .await
    }

    pub(crate) async fn discard_circle_operation(
        &self,
        operation_id: crate::CircleOperationId,
    ) -> Result<(), crate::sync::store::CircleOperationError> {
        self.send_circle_command(|reply| SyncCommand::DiscardCircleOperation {
            operation_id,
            reply,
        })
        .await
    }

    /// Inspect a Circle's in-flight epoch close. A read, so it runs directly on the
    /// components rather than serializing behind the write-command channel.
    pub(crate) async fn circle_close_status(
        &self,
        circle_id: crate::CircleId,
    ) -> Result<crate::CircleCloseStatus, crate::sync::store::CircleOperationError> {
        self.inner.components.circle_close_status(circle_id).await
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
            .run_cycle(
                self.clock.as_ref(),
                Some(&self.security),
                self.observer.as_deref(),
            )
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

fn storage_check_failure_status(error: &crate::storage::StorageError) -> SyncLoopStatus {
    if error.is_transport() {
        SyncLoopStatus::Offline
    } else {
        SyncLoopStatus::Failed {
            error: format!("check sync storage: {error}"),
        }
    }
}

fn current_success_status(
    writes: Vec<crate::PendingWrite>,
    success: SyncLoopSuccess,
) -> Result<SyncLoopStatus, String> {
    if !writes.is_empty() {
        return Ok(SyncLoopStatus::Blocked { success, writes });
    }
    Ok(SyncLoopStatus::Synchronized(success))
}

struct RunningGuard {
    running: Arc<AtomicBool>,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn success() -> SyncLoopSuccess {
        SyncLoopSuccess {
            last_sync_time: "2026-07-14T00:00:00Z".to_string(),
            device_count: 1,
            device_activity: Vec::new(),
            data_changed: false,
            row_changes: None,
            alerts: crate::SyncLoopAlerts {
                rotation_pending: None,
                held_positions: Vec::new(),
                asset_downloads_failed: false,
                local_blob_cleanup_pending: false,
            },
        }
    }

    #[test]
    fn storage_configuration_failure_is_terminal() {
        let status = storage_check_failure_status(&crate::storage::StorageError::Configuration(
            "missing bucket".to_string(),
        ));

        assert!(matches!(status, SyncLoopStatus::Failed { .. }));
    }

    fn database() -> crate::database::StoreDatabase {
        let database = crate::database::Database::open(
            std::path::Path::new(":memory:"),
            Vec::new(),
            chrono::Duration::days(30),
            crate::blob::TransferLimits::one_at_a_time(),
            "status-test".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &[],
        )
        .expect("open status test database");
        crate::database::StoreDatabase::new(&database)
    }

    #[tokio::test]
    async fn successful_cycle_projects_durable_blocked_state() {
        let database = database();
        let write_id = crate::WriteId::from_generated("blocked-write".to_string());
        database
            .insert_write_status_for_test(
                write_id.clone(),
                crate::WriteStatus::Blocked(crate::WriteBlock::MissingBlob {
                    namespace: "audio".to_string(),
                    id: "missing".to_string(),
                }),
            )
            .await
            .expect("insert durable write status");
        let writes = database
            .pending_writes()
            .await
            .expect("load blocked writes");
        let blocked = current_success_status(writes, success()).expect("project blocked state");
        assert!(matches!(
            blocked,
            SyncLoopStatus::Blocked { writes, .. }
                if writes.len() == 1 && writes[0].write_id == write_id
        ));
        database
            .delete_write_for_test(write_id)
            .await
            .expect("remove blocked projection fixture");

        assert!(matches!(
            current_success_status(
                database
                    .pending_writes()
                    .await
                    .expect("load synchronized writes"),
                success(),
            )
            .expect("project synchronized state"),
            SyncLoopStatus::Synchronized(_)
        ));
    }
}
