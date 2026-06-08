//! Sync loop handle: runs the background sync loop on a dedicated OS thread.
//!
//! Owns the sync infrastructure (storage client, HLC, the owned [`Database`]
//! handle, etc.) and runs sync cycles on a timer or manual trigger. The
//! connection itself lives on the [`Database`] actor thread, so the loop holds
//! nothing `!Send` — but it still runs on its own OS thread (a current-thread
//! tokio runtime that `block_on`s the loop) for the *stack*: aws-sdk-s3's
//! endpoint/auth resolution recurses deeply enough to overflow the default
//! secondary-thread stack in debug builds. The thread is given a main-thread-
//! sized stack so S3 sync doesn't `SIGBUS` in `resolve_endpoint`.
//! Emits [`SyncLoopStatus`] events through a broadcast channel.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, info, warn};

use crate::blob::{BlobPlan, BlobUploadObserver};
use crate::changeset::RowChange;
use crate::clock::ClockRef;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;

use super::cycle::SyncComponents;
use super::encrypted_storage::EncryptedSyncStorage;
use super::hlc::Hlc;
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
    event_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,
    thread_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct SyncLoopInner {
    storage: Arc<EncryptedSyncStorage>,
    hlc: Arc<Hlc>,
    device_id: String,
    encryption: Arc<std::sync::RwLock<EncryptionService>>,
    db: Database,
    user_keypair: UserKeypair,
    blob_plan: Arc<dyn BlobPlan>,
    observer: Option<Arc<dyn BlobUploadObserver>>,
}

impl SyncLoopHandle {
    pub fn new(
        components: SyncComponents,
        db: Database,
        clock: ClockRef,
        library_dir: LibraryDir,
        blob_plan: Arc<dyn BlobPlan>,
        observer: Option<Arc<dyn BlobUploadObserver>>,
    ) -> Self {
        let (trigger_tx, trigger_rx) = tokio::sync::mpsc::channel(1);
        let (event_tx, _) = tokio::sync::broadcast::channel(16);

        Self {
            inner: Arc::new(SyncLoopInner {
                storage: components.storage,
                hlc: components.hlc,
                device_id: components.device_id,
                encryption: components.encryption,
                db,
                user_keypair: components.user_keypair,
                blob_plan,
                observer,
            }),
            clock,
            library_dir,
            trigger_tx,
            trigger_rx: std::sync::Mutex::new(Some(trigger_rx)),
            event_tx,
            thread_handle: std::sync::Mutex::new(None),
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
    /// Everything the loop holds is `Send` — the connection lives on the
    /// [`Database`] actor thread, reached only through async calls — so nothing
    /// here is bound to this thread except by choice of stack size.
    pub fn start(&self) {
        {
            let guard = self.thread_handle.lock().unwrap();
            if guard.is_some() {
                return;
            }
        }

        let mut trigger_rx = match self.trigger_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => {
                warn!("Sync trigger receiver already taken");
                return;
            }
        };

        let inner = Arc::clone(&self.inner);
        let event_tx = self.event_tx.clone();
        let clock = self.clock.clone();
        let library_dir = self.library_dir.clone();

        let handle = std::thread::Builder::new()
            .name("coven-sync-loop".to_string())
            // aws-sdk-s3's endpoint/auth resolution recurses deeply enough to blow
            // the ~2 MiB default secondary-thread stack in debug builds (SIGBUS in
            // resolve_endpoint). Give this thread a main-thread-sized stack so S3
            // sync doesn't overflow it.
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        warn!("Failed to create sync loop runtime: {e}");
                        return;
                    }
                };

                rt.block_on(async move {
                    // Short delay to avoid racing with app startup.
                    tokio::time::sleep(Duration::from_secs(3)).await;

                    let mut consecutive_failures: u32 = 0;
                    loop {
                        match run_single_cycle(&inner, clock.as_ref(), &library_dir).await {
                            Ok(result) => {
                                consecutive_failures = 0;
                                // Schema-skip takes priority — newer-version changesets
                                // are permanently inapplicable until the user updates the
                                // app, while asset download failures retry naturally next
                                // cycle.
                                let error = if result.skipped_schema > 0 {
                                    Some(format!(
                                        "{} changes from a newer app version were skipped. Update the app to apply them.",
                                        result.skipped_schema,
                                    ))
                                } else if result.asset_downloads_failed {
                                    Some("Some files failed to download, will retry".to_string())
                                } else {
                                    None
                                };
                                let data_changed = result.changesets_applied > 0;
                                let row_changes = if data_changed && !result.row_changes.is_empty() {
                                    Some(result.row_changes)
                                } else {
                                    None
                                };
                                let status = SyncLoopStatus {
                                    configured: true,
                                    syncing: false,
                                    last_sync_time: Some(result.sync_time),
                                    error,
                                    device_count: (result.other_device_count + 1) as u32,
                                    data_changed,
                                    row_changes,
                                };
                                let _ = event_tx.send(status);
                            }
                            Err(e) => {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                let status = SyncLoopStatus {
                                    configured: true,
                                    syncing: false,
                                    last_sync_time: None,
                                    error: Some(e),
                                    device_count: 0,
                                    data_changed: false,
                                    row_changes: None,
                                };
                                let _ = event_tx.send(status);
                            }
                        }

                        let wait = Duration::from_secs(super::backoff::backoff_secs(
                            consecutive_failures,
                            300,
                        ));
                        if consecutive_failures > 0 {
                            debug!(
                                "Backing off {wait:?} after {consecutive_failures} consecutive failure(s)",
                            );
                        }
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => {}
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
            .expect("Failed to spawn sync loop thread");

        *self.thread_handle.lock().unwrap() = Some(handle);
    }

    /// Whether the background sync thread is running.
    pub fn is_running(&self) -> bool {
        self.thread_handle.lock().unwrap().is_some()
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

    pub fn storage(&self) -> &Arc<EncryptedSyncStorage> {
        &self.inner.storage
    }

    pub fn user_keypair(&self) -> &UserKeypair {
        &self.inner.user_keypair
    }

    pub fn hlc(&self) -> &Arc<Hlc> {
        &self.inner.hlc
    }

    pub fn encryption(&self) -> &Arc<std::sync::RwLock<EncryptionService>> {
        &self.inner.encryption
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
        &inner.device_id,
        &inner.hlc,
        clock,
        &inner.db,
        &inner.encryption,
        &inner.user_keypair,
        library_dir,
        Some(cloud_home),
        inner.blob_plan.as_ref(),
        inner.observer.as_deref(),
    )
    .await
}
