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
//! Emits [`SyncLoopStatus`] events through a broadcast channel the
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
use crate::keys::MasterKeyCustody;
use crate::store_dir::StoreDir;

use super::cloud_storage::{BlobPathScheme, CloudSyncStorage};
use super::cycle::SyncComponents;
use super::hlc::Hlc;
use super::loop_policy::{self, LoopWait, SyncLoopReport, SyncLoopSuccess};

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

/// A sync-loop status the host renders. The loop emits [`Started`](Self::Started)
/// when a cycle begins and one terminal status — [`Succeeded`](Self::Succeeded)
/// or [`Failed`](Self::Failed) — when it ends. The in-progress marker is the
/// variant itself, so there is no separate "syncing" flag and no outcome fields
/// to leave unset on a start.
///
/// A completed cycle is [`Succeeded`] or [`Failed`], never both: a whole-cycle
/// failure is `Failed`; an otherwise-successful cycle that surfaced warnings
/// (skipped-schema, unauthorized, held changesets, …) is `Succeeded`, and the
/// warnings ride in its [`SyncLoopSuccess::alerts`]. So a host tells "failed" from
/// "succeeded with warnings" by which variant it got, not by sniffing a field.
///
/// Delivery is a bounded broadcast (capacity 16). A subscriber that falls more
/// than 16 statuses behind observes a lag error and misses the intervening
/// statuses — including a `Succeeded`'s [`SyncLoopSuccess::row_changes`], which is
/// a refresh *hint* (see its own doc), not a complete change stream.
#[derive(Debug, Clone)]
pub enum SyncLoopStatus {
    /// A cycle has begun; the paired terminal status carries its outcome.
    Started,
    /// The cycle completed. Warnings, if any, ride in the success's `alerts`;
    /// the observed device activity and applied row changes are on it too.
    Succeeded(SyncLoopSuccess),
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
    stop_tx: tokio::sync::watch::Sender<bool>,
    stop_rx: std::sync::Mutex<Option<tokio::sync::watch::Receiver<bool>>>,
    /// The status broadcast, owned by the [`CovenHandle`] and cloned into each
    /// loop it starts, so a subscription survives a loop restart (a reconnect
    /// builds a fresh loop but keeps this same sender). The loop only sends here.
    status_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,
    thread_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
}

struct SyncLoopInner {
    components: SyncComponents,
    custody: Arc<dyn MasterKeyCustody>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// The store-directory lock, held so it releases only when the loop's
    /// thread exits. The running thread keeps a clone of this `SyncLoopInner`
    /// alive across its whole cycle, so the last handle dropping never releases
    /// `.coven-lock` while a mid-cycle pull or upload is still writing — a second
    /// `open()` of the same store stays refused until this writer is gone.
    _open_guard: Arc<StoreOpenGuard>,
}

impl SyncLoopHandle {
    pub(crate) fn new(
        components: SyncComponents,
        custody: Arc<dyn MasterKeyCustody>,
        clock: ClockRef,
        config: Config,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        status_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,
    ) -> Self {
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(1);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let store_dir = config.store_dir.clone();

        Self {
            inner: Arc::new(SyncLoopInner {
                components,
                custody,
                observer,
                _open_guard: open_guard,
            }),
            clock,
            config,
            store_dir,
            trigger_tx,
            trigger_rx: std::sync::Mutex::new(Some(trigger_rx)),
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
                        if status_tx.send(SyncLoopStatus::Failed { error }).is_err() {
                            debug!("sync loop runtime failure had no status subscribers");
                        }
                        return;
                    }
                };

                rt.block_on(async move {
                    // Short delay to avoid racing with app startup.
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(3)) => {}
                        changed = stop_rx.changed() => {
                            if changed.is_err() || *stop_rx.borrow() {
                                info!("Sync loop stopped before first cycle");
                                return;
                            }
                        }
                    }

                    let mut consecutive_failures: u32 = 0;
                    while running.load(Ordering::Acquire) && !*stop_rx.borrow() {
                        // A cycle is starting — mark it in progress before the work,
                        // so a host can show that a sync is running.
                        if status_tx.send(SyncLoopStatus::Started).is_err() {
                            debug!("sync loop cycle-start status had no subscribers");
                        }

                        let decision = match run_single_cycle(&inner, clock.as_ref(), &store_dir).await {
                            Ok(result) => loop_policy::after_success(result),
                            Err(error) => loop_policy::after_failure(error, consecutive_failures, 300),
                        };
                        consecutive_failures = decision.consecutive_failures;

                        let status = match decision.report {
                            SyncLoopReport::Success(success) => SyncLoopStatus::Succeeded(success),
                            SyncLoopReport::Failure(error) => SyncLoopStatus::Failed { error },
                        };
                        if status_tx.send(status).is_err() {
                            debug!("sync loop cycle-complete status had no subscribers");
                        }

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

    pub(crate) fn storage(&self) -> &Arc<CloudSyncStorage> {
        self.inner.components.storage()
    }

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

    pub(crate) fn hlc(&self) -> &Arc<Hlc> {
        self.inner.components.hlc()
    }

    pub(crate) fn current_encryption(&self) -> Option<crate::encryption::EncryptionService> {
        self.inner.components.current_encryption()
    }

    pub(crate) async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: super::membership::MemberRole,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, super::membership_ops::MembershipOpsError> {
        self.inner
            .components
            .invite_member(public_key_hex, invitee_email, role, store_name)
            .await
    }

    pub(crate) async fn remove_member(
        &self,
        public_key_hex: &str,
    ) -> Result<String, super::membership_ops::MembershipOpsError> {
        self.inner
            .components
            .remove_member(public_key_hex, self.inner.custody.as_ref())
            .await
    }

    pub(crate) async fn persist_pending_rotation(&self) -> Result<(), crate::database::DbError> {
        self.inner.components.persist_pending_rotation().await
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation_for_test(
        &self,
        encryption: crate::encryption::EncryptionService,
    ) -> Result<String, crate::keys::KeyError> {
        self.inner
            .components
            .adopt_key_rotation(encryption, self.inner.custody.as_ref())
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
) -> Result<super::cycle::SyncCycleResult, String> {
    inner
        .components
        .run_cycle(
            clock,
            Some(inner.custody.as_ref()),
            store_dir,
            inner.observer.as_deref(),
        )
        .await
}
