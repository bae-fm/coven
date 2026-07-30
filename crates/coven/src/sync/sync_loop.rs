//! Sync loop handle: runs the background sync loop on a dedicated OS thread.
//!
//! Owns the sync infrastructure (storage client, HLC, the owned [`Database`]
//! handle, etc.) and runs sync cycles on a timer or manual trigger. The
//! database access goes through the [`Database`] handle, so the loop holds
//! nothing `!Send` — but it still runs on its own OS thread (a current-thread
//! tokio runtime that `block_on`s the loop) for the *stack*: aws-sdk-s3's
//! endpoint/auth resolution recurses deeply enough to overflow the default
//! secondary-thread stack in debug builds. The thread is given a main-thread-
//! sized stack so S3 sync doesn't `SIGBUS` in `resolve_endpoint`.
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
    inner: Arc<SyncLoopInner>,
    clock: ClockRef,
    config: Config,
    store_dir: StoreDir,
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

struct SyncLoopInner {
    components: SyncComponents,
    blob_transitions: crate::blob::transition::ConnectedBlobTransitions,
    security: StoreSecurity,
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// The store-directory lock, held so it releases only when the loop's
    /// thread exits. The running thread keeps a clone of this `SyncLoopInner`
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
        let store_dir = config.store_dir.clone();
        let blob_transitions =
            components.connected_blob_transitions(store_dir.clone(), observer.clone());

        Self {
            inner: Arc::new(SyncLoopInner {
                components,
                blob_transitions,
                security,
                observer,
                _open_guard: open_guard,
            }),
            clock,
            config,
            store_dir,
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
    /// The thread runs a current-thread tokio runtime that `block_on`s the loop.
    /// A dedicated thread (rather than `tokio::spawn` on the host runtime) is for
    /// the *stack*: aws-sdk-s3's endpoint/auth resolution recurses deeply enough
    /// to overflow the default secondary-thread stack in debug builds (SIGBUS in
    /// `resolve_endpoint`), so the thread is given a main-thread-sized stack.
    /// Everything the loop holds is `Send`; database access goes through async
    /// calls on the [`Database`] handle, so nothing here is bound to this thread
    /// except by choice of stack size.
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
        let clock = self.clock.clone();
        let store_dir = self.store_dir.clone();
        let running = Arc::clone(&self.running);

        let handle = std::thread::Builder::new()
            .name("coven-sync-loop".to_string())
            // aws-sdk-s3's endpoint/auth resolution recurses deeply enough to blow
            // the ~2 MiB default secondary-thread stack in debug builds (SIGBUS in
            // resolve_endpoint). Give this thread a main-thread-sized stack so S3
            // sync doesn't overflow it.
            .stack_size(8 * 1024 * 1024)
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
                                execute_command(&inner, &store_dir, command).await;
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
                                let (decision, cycle_went_offline) = match run_single_cycle(&inner, clock.as_ref(), &store_dir).await {
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
                                execute_command(&inner, &store_dir, command).await;
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

    // -- Accessors for membership operations --

    pub(crate) fn store(&self) -> Arc<crate::sync::store::Store> {
        self.inner.components.store()
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

    #[cfg(test)]
    pub(crate) fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.inner.components.blob_path_scheme()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.inner.components.self_uploader()
    }

    pub(crate) fn current_encryption(&self) -> Option<crate::encryption::EncryptionService> {
        self.inner.components.current_encryption()
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
            .drain_uploads(
                self.clock.as_ref(),
                &self.store_dir,
                self.inner.observer.as_deref(),
            )
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

async fn execute_command(inner: &SyncLoopInner, store_dir: &StoreDir, command: SyncCommand) {
    match command {
        SyncCommand::CreateCircle { name, reply } => {
            reply_circle_command(reply, inner.components.create_circle(&name).await);
        }
        SyncCommand::RenameCircle {
            circle_id,
            name,
            reply,
        } => {
            reply_circle_command(
                reply,
                inner.components.rename_circle(circle_id, &name).await,
            );
        }
        SyncCommand::AddCircleMember {
            circle_id,
            member_pubkey,
            role,
            reply,
        } => {
            reply_circle_command(
                reply,
                inner
                    .components
                    .add_circle_member(store_dir, circle_id, member_pubkey, role)
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
                inner
                    .components
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
                inner
                    .components
                    .resolve_circle_control(circle_id, chosen)
                    .await,
            );
        }
        SyncCommand::CancelCircleEpochClose { circle_id, reply } => {
            reply_circle_command(
                reply,
                inner.components.cancel_circle_epoch_close(circle_id).await,
            );
        }
        SyncCommand::ExcludeCircleCloseDevice {
            circle_id,
            excluded_device_id,
            reply,
        } => {
            reply_circle_command(
                reply,
                inner
                    .components
                    .exclude_circle_close_device(circle_id, excluded_device_id)
                    .await,
            );
        }
        SyncCommand::DeleteCircle { circle_id, reply } => {
            reply_circle_command(reply, inner.components.delete_circle(circle_id).await);
        }
        SyncCommand::RetryCircleOperation {
            operation_id,
            reply,
        } => {
            reply_circle_command(
                reply,
                inner.components.retry_circle_operation(&operation_id).await,
            );
        }
        SyncCommand::DiscardCircleOperation {
            operation_id,
            reply,
        } => {
            reply_circle_command(
                reply,
                inner
                    .components
                    .discard_circle_operation(&operation_id)
                    .await,
            );
        }
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

/// Run a single sync cycle.
async fn run_single_cycle(
    inner: &SyncLoopInner,
    clock: &dyn crate::clock::Clock,
    store_dir: &StoreDir,
) -> Result<super::cycle::SyncCycleResult, super::cycle::SyncCycleFailure> {
    inner
        .components
        .run_cycle(
            clock,
            Some(&inner.security),
            store_dir,
            inner.observer.as_deref(),
        )
        .await
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

    fn database() -> crate::database::Database {
        crate::database::Database::open(
            std::path::Path::new(":memory:"),
            Vec::new(),
            chrono::Duration::days(30),
            crate::blob::TransferLimits::one_at_a_time(),
            "status-test".to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &[],
        )
        .expect("open status test database")
        .0
    }

    async fn insert_write_status(
        db: &crate::database::Database,
        write_id: &'static str,
        status: crate::WriteStatus,
    ) {
        let base = serde_json::json!({ "dependencies": {} }).to_string();
        let status = serde_json::to_string(&status).expect("serialize durable write status");
        db.call(move |conn| {
            conn.execute(
                r#"INSERT INTO store_writes
                 (write_id, status, affected_rows, changeset, inverse_changeset, base, blob_facts)
                 VALUES (?1, ?2, '[]', X'', X'', ?3, '{"blobs":[]}')"#,
                (write_id, status, base),
            )
            .map(|_| ())
            .map_err(crate::DbError::from)
        })
        .await
        .expect("insert durable write status");
    }

    #[tokio::test]
    async fn successful_cycle_projects_durable_blocked_state() {
        let db = database();
        let store_database = crate::database::StoreDatabase::new(&db);
        insert_write_status(
            &db,
            "blocked-write",
            crate::WriteStatus::Blocked(crate::WriteBlock::MissingBlob {
                namespace: "audio".to_string(),
                id: "missing".to_string(),
            }),
        )
        .await;
        let writes = store_database
            .pending_writes()
            .await
            .expect("load blocked writes");
        let blocked = current_success_status(writes, success()).expect("project blocked state");
        assert!(matches!(
            blocked,
            SyncLoopStatus::Blocked { writes, .. }
                if writes.len() == 1 && writes[0].write_id.as_str() == "blocked-write"
        ));
        db.call(|conn| {
            conn.execute(
                "DELETE FROM store_writes WHERE write_id = 'blocked-write'",
                [],
            )
            .map(|_| ())
            .map_err(crate::DbError::from)
        })
        .await
        .expect("remove blocked projection fixture");

        assert!(matches!(
            current_success_status(
                store_database
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
