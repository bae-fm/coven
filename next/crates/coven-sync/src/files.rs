//! File transfers and cached reads, composed beside the database at the root.

#[path = "file_cache.rs"]
mod file_cache;
#[path = "file_error.rs"]
mod file_error;
#[path = "file_keep.rs"]
mod file_keep;
#[path = "file_read.rs"]
mod file_read;
#[path = "file_upload.rs"]
pub(crate) mod file_upload;
use crate::SyncError;
use coven_database::{DbError, FileDatabase};
use coven_foundation::{clock::ClockRef, files::StoreDir, id_source::IdSourceRef};
use coven_storage::Storage;
pub use file_cache::{EagerCacheFillStatus, PinProgress, RowsPinnedLiveQuery};
pub use file_error::{FileReadError, RecordedUploadFailure, UploadFailure, UploadFailures};
pub use file_read::{FileRangeStream, FileStream};
pub use file_upload::{DrainOutcome, QueuedUpload, UploadPhase, UploadQueue, UploadsLiveQuery};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{watch, Notify};

/// Owns storage, database file operations and the device's file area (§21.2).
/// Construction starts commit-driven uploads and eager caching; no sync loop or
/// timer is created. Call `close` before closing the database.
#[derive(Clone)]
pub struct Files {
    inner: Arc<FilesInner>,
    worker: Arc<FileWorker>,
}
struct FilesInner {
    database: FileDatabase,
    directory: StoreDir,
    storage: std::sync::RwLock<Option<Arc<dyn Storage>>>,
    clock: ClockRef,
    ids: IdSourceRef,
    state: Mutex<UploadState>,
    changed: watch::Sender<u64>,
    wake: Arc<Notify>,
    drain: tokio::sync::Mutex<()>,
    cache: tokio::sync::Mutex<()>,
    pins: tokio::sync::Mutex<()>,
    closed: AtomicBool,
    eager: watch::Sender<crate::EagerCacheFillStatus>,
    cancel_eager: watch::Sender<u64>,
}
struct UploadState {
    paused: bool,
    active: BTreeMap<i64, UploadPhase>,
    failures: BTreeMap<i64, Arc<UploadFailure>>,
}
struct FileWorker {
    stop: watch::Sender<bool>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for FileWorker {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
impl Files {
    /// Compose the file owner. Storage may be absent for device-local use.
    pub fn new(
        database: FileDatabase,
        directory: StoreDir,
        storage: Option<Arc<dyn Storage>>,
        clock: ClockRef,
        ids: IdSourceRef,
    ) -> Self {
        let mut changes = database.changes();
        let (changed, _) = watch::channel(0);
        let (eager, _) = watch::channel(crate::EagerCacheFillStatus::Idle);
        let (cancel_eager, _) = watch::channel(0);
        let (stop, mut stopping) = watch::channel(false);
        let inner = Arc::new(FilesInner {
            database,
            directory,
            storage: std::sync::RwLock::new(storage),
            clock,
            ids,
            state: Mutex::new(UploadState {
                paused: false,
                active: BTreeMap::new(),
                failures: BTreeMap::new(),
            }),
            changed,
            wake: Arc::new(Notify::new()),
            drain: tokio::sync::Mutex::new(()),
            cache: tokio::sync::Mutex::new(()),
            pins: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
            eager,
            cancel_eager,
        });
        let weak = Arc::downgrade(&inner);
        let wake = inner.wake.clone();
        let task = tokio::spawn(async move {
            let mut seen = Vec::new();
            loop {
                tokio::select! {
                    biased;
                    _=stopping.changed()=>break,
                    result=changes.next()=>if let Err(error)=result {
                        if !matches!(error,DbError::StoreClosed) {tracing::error!(?error,"file commit observation failed");}
                        break;
                    },
                    _=wake.notified()=>{},
                }
                let Some(owner) = weak.upgrade() else { break };
                let work = async {
                    if let Err(error) = owner.drain_uploads(false).await {
                        tracing::error!(?error, "upload queue failed");
                    }
                    owner.fill_eager(&mut seen).await;
                };
                tokio::select! { biased; _=stopping.changed()=>break, _=work=>{} }
                owner.notify();
            }
        });
        Self {
            inner,
            worker: Arc::new(FileWorker {
                stop,
                task: tokio::sync::Mutex::new(Some(task)),
            }),
        }
    }
    /// Stop background work and wait for in-flight database and transfer work.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.worker.stop.send_replace(true);
        if let Some(task) = self.worker.task.lock().await.take() {
            join(task).await;
        }
        let _drain = self.inner.drain.lock().await;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.lock().await;
        self.inner.notify();
    }
    /// Retry all waiting files now; no timer or implicit retry is installed.
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        self.inner.check_open()?;
        if self
            .inner
            .storage
            .read()
            .expect("storage lock poisoned")
            .is_none()
        {
            return Err(SyncError::NoStorage);
        }
        Ok(self.inner.drain_uploads(true).await?)
    }
    /// Pause between confirmed parts, preserving the current provider session.
    pub fn set_uploads_paused(&self, paused: bool) {
        self.inner
            .state
            .lock()
            .expect("upload state poisoned")
            .paused = paused;
        self.inner.notify();
        if !paused {
            self.inner.wake.notify_one();
        }
    }

    pub(crate) fn set_storage(&self, storage: Option<Arc<dyn Storage>>) {
        *self.inner.storage.write().expect("storage lock poisoned") = storage;
        self.inner.wake.notify_one();
    }
}
impl FilesInner {
    pub(crate) fn notify(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }
    pub(crate) fn check_open(&self) -> Result<(), DbError> {
        if self.closed.load(Ordering::Acquire) {
            Err(DbError::StoreClosed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn paused(&self) -> bool {
        self.state.lock().expect("upload state poisoned").paused
    }
    pub(crate) fn phase(&self, id: i64, phase: UploadPhase) {
        self.state
            .lock()
            .expect("upload state poisoned")
            .active
            .insert(id, phase);
        self.notify();
    }
    pub(crate) async fn trim(&self, namespace: &str) {
        if let Err(error) = self.database.trim_cache(namespace).await {
            // Space reclamation is ancillary to a successful read (§16.4),
            // but its failure must remain visible in the structured log.
            tracing::error!(?error, namespace, "cache eviction failed");
        }
    }
}
pub(crate) async fn join<T>(task: tokio::task::JoinHandle<T>) -> T {
    match task.await {
        Ok(value) => value,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("file task cancelled: {error}"),
    }
}

#[cfg(test)]
#[path = "files_tests.rs"]
mod tests;
