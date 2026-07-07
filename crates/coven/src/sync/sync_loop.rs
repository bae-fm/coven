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
//! Emits [`SyncLoopStatus`] events through a broadcast channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, error, info};

use crate::blob::BlobTransitionObserver;
use crate::changeset::RowChange;
use crate::clock::ClockRef;
use crate::coven::LibraryOpenGuard;
use crate::database::Database;
use crate::keys::{KeyService, UserKeypair};
use crate::library_dir::LibraryDir;

use super::cloud_storage::{CloudCipher, CloudSyncStorage};
use super::cycle::SyncComponents;
use super::hlc::Hlc;
use super::loop_policy::{self, LoopWait, SyncLoopReport};
use super::storage::SyncStorage;

/// Status emitted by the sync loop after each cycle.
#[derive(Debug, Clone)]
pub struct SyncLoopStatus {
    pub configured: bool,
    pub syncing: bool,
    pub last_sync_time: Option<String>,
    pub error: Option<String>,
    pub device_count: u32,
    pub data_changed: bool,
    /// Row changes from applied changesets, for the host to map to domain
    /// events. Present when `data_changed` is true.
    pub row_changes: Option<Vec<RowChange>>,
}

/// Manages the background sync loop and provides access to sync components.
pub struct SyncLoopHandle {
    inner: Arc<SyncLoopInner>,
    clock: ClockRef,
    library_dir: LibraryDir,
    trigger_tx: tokio::sync::mpsc::Sender<()>,
    trigger_rx: std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    stop_rx: std::sync::Mutex<Option<tokio::sync::watch::Receiver<bool>>>,
    event_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,
    thread_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
}

struct SyncLoopInner {
    storage: Arc<CloudSyncStorage>,
    hlc: Arc<Hlc>,
    library_id: String,
    device_id: String,
    cipher: Arc<std::sync::RwLock<CloudCipher>>,
    db: Database,
    user_keypair: UserKeypair,
    key_service: KeyService,
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// The library-directory lock, held so it releases only when the loop's
    /// thread exits. The running thread keeps a clone of this `SyncLoopInner`
    /// alive across its whole cycle, so the last handle dropping never releases
    /// `.coven-lock` while a mid-cycle pull or upload is still writing — a second
    /// `open()` of the same library stays refused until this writer is gone.
    _open_guard: Arc<LibraryOpenGuard>,
}

impl SyncLoopHandle {
    pub(crate) fn new(
        components: SyncComponents,
        db: Database,
        key_service: KeyService,
        clock: ClockRef,
        library_dir: LibraryDir,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<LibraryOpenGuard>,
    ) -> Self {
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(1);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let (event_tx, _) = tokio::sync::broadcast::channel(16);

        Self {
            inner: Arc::new(SyncLoopInner {
                storage: components.storage,
                hlc: components.hlc,
                library_id: components.library_id,
                device_id: components.device_id,
                cipher: components.cipher,
                db,
                user_keypair: components.user_keypair,
                key_service,
                observer,
                _open_guard: open_guard,
            }),
            clock,
            library_dir,
            trigger_tx,
            trigger_rx: std::sync::Mutex::new(Some(trigger_rx)),
            stop_tx,
            stop_rx: std::sync::Mutex::new(Some(stop_rx)),
            event_tx,
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
    pub fn start(&self) -> Result<(), String> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let mut trigger_rx = match self.trigger_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => {
                self.running.store(false, Ordering::Release);
                return Err(
                    "sync loop cannot be restarted after its receiver was taken".to_string()
                );
            }
        };
        let mut stop_rx = match self.stop_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => {
                self.running.store(false, Ordering::Release);
                return Err(
                    "sync loop cannot be restarted after its stop receiver was taken".to_string(),
                );
            }
        };

        let inner = Arc::clone(&self.inner);
        let event_tx = self.event_tx.clone();
        let clock = self.clock.clone();
        let library_dir = self.library_dir.clone();
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
                        let status = SyncLoopStatus {
                            configured: true,
                            syncing: false,
                            last_sync_time: None,
                            error: Some(error),
                            device_count: 0,
                            data_changed: false,
                            row_changes: None,
                        };
                        if event_tx.send(status).is_err() {
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
                        let decision = match run_single_cycle(&inner, clock.as_ref(), &library_dir).await {
                            Ok(result) => loop_policy::after_success(result),
                            Err(error) => loop_policy::after_failure(error, consecutive_failures, 300),
                        };
                        consecutive_failures = decision.consecutive_failures;

                        match decision.report {
                            SyncLoopReport::Success(success) => {
                                let status = SyncLoopStatus {
                                    configured: true,
                                    syncing: false,
                                    last_sync_time: Some(success.last_sync_time),
                                    error: success.alerts.primary_message(),
                                    device_count: success.device_count,
                                    data_changed: success.data_changed,
                                    row_changes: success.row_changes,
                                };
                                if event_tx.send(status).is_err() {
                                    debug!("sync loop success status had no subscribers");
                                }
                            }
                            SyncLoopReport::Failure(error) => {
                                let status = SyncLoopStatus {
                                    configured: true,
                                    syncing: false,
                                    last_sync_time: None,
                                    error: Some(error),
                                    device_count: 0,
                                    data_changed: false,
                                    row_changes: None,
                                };
                                if event_tx.send(status).is_err() {
                                    debug!("sync loop failure status had no subscribers");
                                }
                            }
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
                format!("failed to spawn sync loop thread: {e}")
            })?;

        *self.thread_handle.lock().unwrap() = Some(handle);
        Ok(())
    }

    /// Whether the background sync thread is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Request loop shutdown and join the sync thread.
    pub fn stop(&self) -> Result<(), String> {
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
                return Err("sync loop thread panicked".to_string());
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
    pub fn trigger(&self) {
        match self.trigger_tx.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => {}
            Err(TrySendError::Closed(())) => {
                debug!("Sync trigger channel closed, loop is not running");
            }
        }
    }

    /// Subscribe to sync loop status events.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SyncLoopStatus> {
        self.event_tx.subscribe()
    }

    // -- Accessors for membership operations --

    pub fn storage(&self) -> &Arc<CloudSyncStorage> {
        &self.inner.storage
    }

    pub fn user_keypair(&self) -> &UserKeypair {
        &self.inner.user_keypair
    }

    pub fn hlc(&self) -> &Arc<Hlc> {
        &self.inner.hlc
    }

    pub fn cipher(&self) -> &Arc<std::sync::RwLock<CloudCipher>> {
        &self.inner.cipher
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
    library_dir: &LibraryDir,
) -> Result<super::cycle::SyncCycleResult, String> {
    let storage: &dyn SyncStorage = &*inner.storage;
    let cloud_home = inner.storage.cloud_home();

    super::cycle::run_single_sync_cycle(
        storage,
        &inner.library_id,
        &inner.device_id,
        &inner.hlc,
        clock,
        &inner.db,
        &inner.cipher,
        &inner.user_keypair,
        Some(&inner.key_service),
        library_dir,
        Some(cloud_home),
        inner.observer.as_deref(),
    )
    .await
}
