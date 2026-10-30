//! File transfers and cached reads, composed beside the database at the root.

#[path = "file_cache.rs"]
mod file_cache;
#[path = "file_cache_retry.rs"]
mod file_cache_retry;
#[path = "file_error.rs"]
mod file_error;
#[path = "file_failure.rs"]
mod file_failure;
#[path = "file_read.rs"]
mod file_read;
#[path = "file_status.rs"]
mod file_status;
#[path = "file_upload.rs"]
mod file_upload;
#[path = "file_upload_failure.rs"]
mod file_upload_failure;
#[path = "file_upload_pool.rs"]
mod file_upload_pool;
#[path = "file_upload_queue.rs"]
mod file_upload_queue;
#[path = "file_upload_retry.rs"]
mod file_upload_retry;
use crate::SyncError;
pub use coven_database::PinProgress;
use coven_database::{DbError, FileDatabase};
use coven_foundation::{files::StoreDir, id_source::IdSourceRef};
use coven_storage::Storage;
pub use file_error::FileReadError;
pub use file_read::{FileRangeStream, FileStream};
pub use file_status::FileStatus;
pub use file_upload_pool::DrainOutcome;
pub use file_upload_queue::{QueuedUpload, UploadPhase, UploadProgress, UploadQueue};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{watch, Notify};

use coven_storage::TransferBudget;
pub use coven_storage::TransferLimits;

/// Owns storage, database file operations and the device's file area (Appendix F.3).
/// Construction starts one commit-driven worker, for uploads; no sync loop or
/// timer is created. Call `close` before closing the database.
#[derive(Clone)]
pub struct Files {
    inner: Arc<FilesInner>,
    worker: Arc<FileWorker>,
}
/// One lock per stored object's cache entries, by namespace and object.
type FileReadLocks = BTreeMap<
    (String, coven_foundation::id_source::FileId),
    std::sync::Weak<tokio::sync::Mutex<()>>,
>;
struct FilesInner {
    database: FileDatabase,
    directory: StoreDir,
    storage: std::sync::RwLock<Option<Arc<dyn Storage>>>,
    send: Arc<crate::SyncState>,
    waits: Arc<crate::RetryWaits>,
    /// Failures observed on this connection that require a new connection.
    /// Reopening creates a new connection and permits one attempt again.
    connection_waits: Mutex<BTreeSet<coven_database::RetrySubject>>,
    ids: IdSourceRef,
    /// The persisted pause setting as of the last upload run or app change;
    /// parts check it between sends.
    paused: AtomicBool,
    /// Held by an app's pause change, and by an upload run from its read of
    /// the persisted setting until it has acted on it, so neither the copy
    /// above nor a retry's answer comes from a setting already replaced.
    pause_change: tokio::sync::Mutex<()>,
    /// Wakes the upload worker, or its run in progress.
    upload_wake: Arc<Notify>,
    /// One upload run at a time.
    drain: tokio::sync::Mutex<()>,
    /// The files being sent now, which coven's views show (§4, E7).
    in_flight: coven_database::InFlight,
    /// The files being downloaded to keep whole now, which
    /// `coven_downloading` shows (§4, E8).
    downloading: coven_database::InFlight,
    /// App retries waiting for an upload run to serve them.
    app_retries: Mutex<Vec<file_upload_pool::AppRetry>>,
    /// Connection changes waiting for the uploads in flight to end; while
    /// any waits, no upload starts.
    changing: AtomicUsize,
    cache: tokio::sync::RwLock<()>,
    reads: Mutex<FileReadLocks>,
    transfers: Arc<TransferBudget>,
    pins: tokio::sync::Mutex<()>,
    closed: AtomicBool,
    /// Runs where an upload run that found uploads blocked, with none in
    /// flight, is about to act on that, so a test can change the setting
    /// between the run's check and what it does.
    #[cfg(test)]
    blocked_end_gate: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
struct FileWorker {
    stop: watch::Sender<bool>,
    /// The upload worker.
    tasks: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl Drop for FileWorker {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
impl Files {
    /// Compose the file owner. Storage may be absent while uploads wait.
    pub fn new(
        database: FileDatabase,
        directory: StoreDir,
        storage: Option<Arc<dyn Storage>>,
        send: Arc<crate::SyncState>,
        waits: Arc<crate::RetryWaits>,
        ids: IdSourceRef,
        transfers: Arc<TransferBudget>,
    ) -> Self {
        let uploads_changes = database.queued_work_changes();
        let in_flight = database.uploads_in_flight();
        let downloading = database.downloads_in_flight();
        let (stop, _) = watch::channel(false);
        let inner = Arc::new(FilesInner {
            database,
            directory,
            storage: std::sync::RwLock::new(storage),
            send,
            waits,
            connection_waits: Mutex::new(BTreeSet::new()),
            ids,
            paused: AtomicBool::new(false),
            pause_change: tokio::sync::Mutex::new(()),
            upload_wake: Arc::new(Notify::new()),
            drain: tokio::sync::Mutex::new(()),
            in_flight,
            downloading,
            app_retries: Mutex::new(Vec::new()),
            changing: AtomicUsize::new(0),
            cache: tokio::sync::RwLock::new(()),
            reads: Mutex::new(BTreeMap::new()),
            transfers,
            pins: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
            #[cfg(test)]
            blocked_end_gate: Mutex::new(None),
        });
        let tasks = vec![tokio::spawn(upload_worker(
            Arc::downgrade(&inner),
            stop.subscribe(),
            uploads_changes,
        ))];
        Self {
            inner,
            worker: Arc::new(FileWorker {
                stop,
                tasks: tokio::sync::Mutex::new(tasks),
            }),
        }
    }
    /// The error behind each failure this store's work recorded in this
    /// session, by record, and again whenever one a record would show
    /// appears, changes or ends (E5).
    pub fn subscribe_pending_causes(&self) -> tokio::sync::watch::Receiver<crate::PendingCauses> {
        self.inner.waits.causes().subscribe()
    }
    /// The limits shared by this handle's transfer consumers.
    pub fn transfer_limits(&self) -> TransferLimits {
        self.inner.transfers.limits()
    }
    /// Change upload and download admission. The next upload to start sees
    /// the new upload limit; uploads in flight keep running. Active response
    /// bodies retain their leases until they finish or are dropped.
    pub fn set_transfer_limits(&self, limits: TransferLimits) {
        self.inner.transfers.set_limits(limits);
        self.inner.upload_wake.notify_one();
    }
    /// Stop background work and wait for in-flight database and transfer work.
    /// An app retry still waiting fails as the store closes.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.worker.stop.send_replace(true);
        for task in std::mem::take(&mut *self.worker.tasks.lock().await) {
            join(task).await;
        }
        let _drain = self.inner.drain.lock().await;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        self.inner
            .app_retries
            .lock()
            .expect("app retries poisoned")
            .clear();
    }
    /// Optionally retry recoverable uploads early: the upload worker attempts
    /// every upload that can retry, whatever its wait, and this returns once
    /// each attempt has ended. Provider cooldowns and failures that never
    /// clear still hold (§17.8).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        let answer = self.inner.ask_retry()?;
        // The worker stopping with the store drops the retry unanswered.
        answer
            .await
            .unwrap_or_else(|_| Err(DbError::StoreClosed.into()))
    }
    /// Persist the pause setting; a running upload stops between confirmed
    /// parts, preserving its provider session (E7).
    pub async fn set_uploads_paused(&self, paused: bool) -> Result<(), DbError> {
        self.inner.check_open()?;
        {
            let _change = self.inner.pause_change.lock().await;
            self.inner.database.set_uploads_paused(paused).await?;
            self.inner.paused.store(paused, Ordering::Release);
        }
        if !paused {
            self.inner.upload_wake.notify_one();
        }
        Ok(())
    }

    /// Finish transfers before verifying and committing a connection change.
    /// No upload starts meanwhile. A failed setup leaves the current
    /// connection installed.
    pub async fn set_storage(
        &self,
        storage: Option<Arc<dyn Storage>>,
        ready: impl std::future::Future<Output = Result<(), SyncError>>,
    ) -> Result<(), SyncError> {
        let _changing = ConnectionChange::start(&self.inner.changing, &self.inner.upload_wake);
        let _drain = self.inner.drain.lock().await;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        self.inner.check_open()?;
        ready.await?;
        *self.inner.storage.write().expect("storage lock poisoned") = storage;
        self.inner
            .connection_waits
            .lock()
            .expect("file connection waits poisoned")
            .clear();
        Ok(())
    }
}

/// A connection change waiting for, then holding, the upload runs. While it
/// lasts no upload starts, so the run in progress ends once its attempts do;
/// as it ends, also when its caller is cancelled, the upload worker runs again.
struct ConnectionChange<'a> {
    changing: &'a AtomicUsize,
    wake: &'a Notify,
}
impl<'a> ConnectionChange<'a> {
    fn start(changing: &'a AtomicUsize, wake: &'a Notify) -> Self {
        changing.fetch_add(1, Ordering::AcqRel);
        wake.notify_one();
        Self { changing, wake }
    }
}
impl Drop for ConnectionChange<'_> {
    fn drop(&mut self) {
        self.changing.fetch_sub(1, Ordering::AcqRel);
        self.wake.notify_one();
    }
}

/// The upload worker: runs uploads whenever queued work, a changed
/// prerequisite or a due retry may let one start (§17.8). A run keeps the
/// upload limit's number in flight until none can start.
async fn upload_worker(
    owner: std::sync::Weak<FilesInner>,
    mut stopping: watch::Receiver<bool>,
    mut changes: coven_database::DatabaseChanges,
) {
    let Some(first) = owner.upgrade() else { return };
    let wake = first.upload_wake.clone();
    let mut sending = first.send.subscribe();
    let mut reports = first.waits.network_reports();
    drop(first);
    // The earliest due upload retry, armed after each run (§6.7).
    let mut retry: crate::retry_waits::Wake = Box::pin(std::future::pending());
    loop {
        tokio::select! {
            biased;
            _ = stopping.changed() => break,
            result = changes.next() => if let Err(error) = result {
                if !matches!(error, DbError::StoreClosed) {
                    tracing::error!(?error, "file commit observation failed");
                }
                break;
            },
            () = wake.notified() => {},
            result = sending.changed() => if result.is_err() { break; },
            result = reports.changed() => if result.is_err() { break; },
            () = &mut retry => {},
        }
        let Some(owner) = owner.upgrade() else { break };
        let run = async {
            match owner.drain_uploads().await {
                Ok(_) => (),
                Err(SyncError::NoStorage) => {
                    tracing::debug!("file uploads waiting for storage");
                }
                Err(SyncError::Storage(error))
                    if error.failure() == coven_storage::StorageFailure::CatchUpRequired =>
                {
                    tracing::debug!("file uploads waiting for a completed catch-up");
                }
                Err(SyncError::Database(DbError::StoreClosed)) => (),
                Err(error) => tracing::error!(?error, "upload queue failed"),
            }
        };
        tokio::select! { biased; _ = stopping.changed() => break, () = run => {} }
        retry = owner
            .next_upload_retry()
            .unwrap_or_else(|| Box::pin(std::future::pending()));
    }
}

impl FilesInner {
    fn connection_due(&self, subject: coven_database::RetrySubject) -> bool {
        !self
            .connection_waits
            .lock()
            .expect("file connection waits poisoned")
            .contains(&subject)
    }

    fn wait_for_connection(&self, subject: coven_database::RetrySubject) {
        self.connection_waits
            .lock()
            .expect("file connection waits poisoned")
            .insert(subject);
    }

    fn forget_connection_wait(&self, subject: coven_database::RetrySubject) {
        self.connection_waits
            .lock()
            .expect("file connection waits poisoned")
            .remove(&subject);
    }

    /// Hold the lock on one stored object's cache entries while reading
    /// them, fetching and caching chunks. Files over the same bytes share it.
    pub(crate) async fn lock_file(
        &self,
        file: &coven_database::FileRef,
        location: &coven_database::FileLocation,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut reads = self.reads.lock().expect("file read locks poisoned");
            reads.retain(|_, lock| lock.strong_count() != 0);
            let key = (file.namespace().to_owned(), location.object());
            match reads.get(&key).and_then(std::sync::Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    reads.insert(key, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }

    pub(crate) fn check_open(&self) -> Result<(), DbError> {
        if self.closed.load(Ordering::Acquire) {
            Err(DbError::StoreClosed)
        } else {
            Ok(())
        }
    }
    pub(crate) fn paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }
    pub(crate) async fn trim(&self, namespace: &str) {
        if let Err(error) = self.database.trim_cache(namespace).await {
            // Space reclamation is ancillary to a successful read (§17.7),
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
