//! Sync loop handle: manages the background sync loop thread.
//!
//! Owns the sync infrastructure (storage client, HLC, session, etc.) and
//! spawns a dedicated OS thread that runs sync cycles on a timer or manual
//! trigger. Emits `SyncLoopStatus` events through a broadcast channel.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tracing::{debug, info, warn};

use crate::blob::{BlobPlan, BlobUploadObserver};
use crate::changeset::RowChange;
use crate::clock::ClockRef;
use crate::db::SyncBookkeeping;
use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::library_dir::LibraryDir;

use super::cycle::{SyncComponents, SyncCycleOutcome};
use super::encrypted_storage::EncryptedSyncStorage;
use super::hlc::Hlc;
use super::session::SyncSession;
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
    db: Arc<dyn SyncBookkeeping>,
    clock: ClockRef,
    library_dir: LibraryDir,
    trigger_tx: tokio::sync::mpsc::Sender<()>,
    trigger_rx: std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>,
    event_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,
    loop_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct SyncLoopInner {
    storage: Arc<EncryptedSyncStorage>,
    hlc: Arc<Hlc>,
    device_id: String,
    encryption: Arc<std::sync::RwLock<EncryptionService>>,
    raw_db: *mut libsqlite3_sys::sqlite3,
    session: tokio::sync::Mutex<Option<SyncSession>>,
    user_keypair: UserKeypair,
    blob_plan: Arc<dyn BlobPlan>,
    observer: Option<Arc<dyn BlobUploadObserver>>,
}

// SAFETY: The raw sqlite3 pointer is only used for session extension operations
// which are serialized through the sync loop. The pointer itself is stable
// (heap-allocated write connection held by the host).
unsafe impl Send for SyncLoopInner {}
unsafe impl Sync for SyncLoopInner {}

impl SyncLoopHandle {
    pub fn new(
        components: SyncComponents,
        db: Arc<dyn SyncBookkeeping>,
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
                raw_db: components.raw_db,
                session: tokio::sync::Mutex::new(Some(components.session)),
                user_keypair: components.user_keypair,
                blob_plan,
                observer,
            }),
            db,
            clock,
            library_dir,
            trigger_tx,
            trigger_rx: std::sync::Mutex::new(Some(trigger_rx)),
            event_tx,
            loop_handle: std::sync::Mutex::new(None),
        }
    }

    /// Start the background sync loop. No-op if already running.
    ///
    /// Spawns a dedicated OS thread with its own tokio runtime because
    /// the sync session holds a raw sqlite3 pointer (not Send across
    /// tokio task boundaries).
    pub fn start(&self) {
        {
            let guard = self.loop_handle.lock().unwrap();
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
        let db = Arc::clone(&self.db);
        let clock = self.clock.clone();
        let library_dir = self.library_dir.clone();

        let handle = std::thread::Builder::new()
            .name("coven-sync-loop".to_string())
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

                rt.block_on(async {
                    // Short delay to avoid racing with app startup
                    tokio::time::sleep(Duration::from_secs(3)).await;

                    loop {
                        match run_single_cycle(&inner, db.as_ref(), clock.as_ref(), &library_dir)
                            .await
                        {
                            Ok(result) => {
                                let error = if result.asset_downloads_failed {
                                    Some("Some files failed to download, will retry".to_string())
                                } else {
                                    None
                                };
                                let data_changed = result.changesets_applied > 0;
                                let row_changes = if data_changed && !result.row_changes.is_empty()
                                {
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

                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(30)) => {}
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

        *self.loop_handle.lock().unwrap() = Some(handle);
    }

    /// Whether the background sync thread is running.
    pub fn is_running(&self) -> bool {
        self.loop_handle.lock().unwrap().is_some()
    }

    /// Signal the sync loop to run a cycle immediately.
    ///
    /// `Full` means a trigger is already pending — our request collapses
    /// into the existing one, which is exactly what the capacity-1 channel
    /// is for. `Closed` means the loop is gone, so the trigger is moot.
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

/// Run a single sync cycle, managing session lifecycle.
async fn run_single_cycle(
    inner: &SyncLoopInner,
    db: &dyn SyncBookkeeping,
    clock: &dyn crate::clock::Clock,
    library_dir: &LibraryDir,
) -> Result<super::cycle::SyncCycleResult, String> {
    let storage: &dyn SyncStorage = &*inner.storage;

    let session = match inner.session.lock().await.take() {
        Some(s) => s,
        None => {
            warn!("Sync session was None, creating a new one");
            unsafe { SyncSession::start(inner.raw_db) }
                .map_err(|e| format!("Failed to create replacement sync session: {e}"))?
        }
    };

    let cloud_home = inner.storage.cloud_home();

    let outcome = unsafe {
        super::cycle::run_single_sync_cycle(
            storage,
            &inner.device_id,
            &inner.hlc,
            clock,
            inner.raw_db,
            session,
            &inner.encryption,
            &inner.user_keypair,
            db,
            library_dir,
            Some(cloud_home),
            inner.blob_plan.as_ref(),
            inner.observer.as_deref(),
        )
        .await
    };

    match outcome {
        SyncCycleOutcome::Ok(result, new_session) => {
            *inner.session.lock().await = Some(new_session);
            Ok(result)
        }
        SyncCycleOutcome::ErrWithSession(e, new_session) => {
            *inner.session.lock().await = Some(new_session);
            Err(e)
        }
        SyncCycleOutcome::ErrNoSession(e) => Err(e),
    }
}
