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
pub(crate) mod file_upload;
#[path = "file_upload_failure.rs"]
mod file_upload_failure;
#[path = "file_upload_retry.rs"]
mod file_upload_retry;
use crate::SyncError;
use coven_database::{DbError, FileDatabase};
pub use coven_database::{EagerCacheFillStatus, PinProgress};
use coven_foundation::{files::StoreDir, id_source::IdSourceRef};
use coven_storage::Storage;
pub use file_error::FileReadError;
pub use file_read::{FileRangeStream, FileStream};
pub use file_status::FileStatus;
pub use file_upload::{
    upload_queue_from_state, DrainOutcome, QueuedUpload, UploadPhase, UploadQueue,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{watch, Notify};

use coven_storage::TransferBudget;
pub use coven_storage::TransferLimits;

/// Owns storage, database file operations and the device's file area (Appendix F.3).
/// Construction starts commit-driven uploads and eager caching; no sync loop or
/// timer is created. Call `close` before closing the database.
#[derive(Clone)]
pub struct Files {
    inner: Arc<FilesInner>,
    worker: Arc<FileWorker>,
}
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
    /// The persisted pause setting as of the last drain or app change; parts
    /// check it between sends.
    paused: AtomicBool,
    wake: Arc<Notify>,
    drain: tokio::sync::Mutex<()>,
    cache: tokio::sync::RwLock<()>,
    reads: Mutex<FileReadLocks>,
    transfers: Arc<TransferBudget>,
    pins: tokio::sync::Mutex<()>,
    closed: AtomicBool,
    /// One eager fill at a time, and none while the connection changes.
    eager_fill: tokio::sync::Mutex<()>,
    cancel_eager: watch::Sender<u64>,
    /// A sequence pass needs the same download capacity as this worker.
    sequence_active: watch::Sender<bool>,
}
pub(crate) struct SequenceDownloads<'a> {
    active: &'a watch::Sender<bool>,
    wake: &'a Notify,
}
impl Drop for SequenceDownloads<'_> {
    fn drop(&mut self) {
        self.active.send_replace(false);
        self.wake.notify_one();
    }
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
        let mut changes = database.queued_work_changes();
        let mut sending = send.subscribe();
        let mut reports = waits.network_reports();
        let (cancel_eager, _) = watch::channel(0);
        let (sequence_active, _) = watch::channel(false);
        let (stop, mut stopping) = watch::channel(false);
        let inner = Arc::new(FilesInner {
            database,
            directory,
            storage: std::sync::RwLock::new(storage),
            send,
            waits,
            connection_waits: Mutex::new(BTreeSet::new()),
            ids,
            paused: AtomicBool::new(false),
            wake: Arc::new(Notify::new()),
            drain: tokio::sync::Mutex::new(()),
            cache: tokio::sync::RwLock::new(()),
            reads: Mutex::new(BTreeMap::new()),
            transfers,
            pins: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
            eager_fill: tokio::sync::Mutex::new(()),
            cancel_eager,
            sequence_active,
        });
        let weak = Arc::downgrade(&inner);
        let wake = inner.wake.clone();
        let task = tokio::spawn(async move {
            // The earliest due upload or eager download retry, armed after
            // each drain and fill (§6.7).
            let mut retry: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(std::future::pending());
            loop {
                tokio::select! {
                    biased;
                    _=stopping.changed()=>break,
                    result=changes.next()=>if let Err(error)=result {
                        if !matches!(error,DbError::StoreClosed) {tracing::error!(?error,"file commit observation failed");}
                        break;
                    },
                    _=wake.notified()=>{},
                    result=sending.changed()=>if result.is_err() { break; },
                    result=reports.changed()=>if result.is_err() { break; },
                    _=&mut retry=>{},
                }
                let Some(owner) = weak.upgrade() else { break };
                let work = async {
                    // A network report ends upload and download waits for
                    // the network; reports during this work are acted on
                    // after it, ending the waits it fails into (§6.7).
                    if let Err(error) = owner
                        .waits
                        .end_offline_waits(crate::NetworkOwner::Files)
                        .await
                    {
                        // The waits keep their backoff and the report stays,
                        // so the worker's next plan ends them.
                        tracing::error!(?error, "ending file waits for the network failed");
                    }
                    match owner.drain_uploads(false).await {
                        Ok(_) => (),
                        Err(SyncError::NoStorage) => {
                            tracing::debug!("file uploads waiting for storage");
                        }
                        Err(SyncError::Storage(error))
                            if error.failure()
                                == coven_storage::StorageFailure::CatchUpRequired =>
                        {
                            tracing::debug!("file uploads waiting for a completed catch-up");
                        }
                        Err(error) => tracing::error!(?error, "upload queue failed"),
                    }
                    if let Err(error) = owner.fill_eager().await {
                        tracing::error!(?error, "eager cache fill failed");
                    }
                };
                tokio::select! { biased; _=stopping.changed()=>break, _=work=>{} }
                retry = owner
                    .next_retry()
                    .unwrap_or_else(|| Box::pin(std::future::pending()));
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
    /// The error behind each failure this store's work recorded in this
    /// session, which `PendingCauses::attach` gives the records it explains.
    pub fn pending_causes(&self) -> crate::PendingCauses {
        self.inner.waits.causes().clone()
    }
    /// The limits shared by this handle's transfer consumers.
    pub fn transfer_limits(&self) -> TransferLimits {
        self.inner.transfers.limits()
    }
    /// Change future upload batches and download admission. Active response
    /// bodies retain their leases until they finish or are dropped.
    pub fn set_transfer_limits(&self, limits: TransferLimits) {
        self.inner.transfers.set_limits(limits);
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
        let _cache = self.inner.cache.write().await;
    }
    /// Optionally retry recoverable uploads early. Provider cooldowns and
    /// failures that never clear still hold (§17.7).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        self.inner.drain_uploads(true).await
    }
    /// Persist the pause setting; a running upload stops between confirmed
    /// parts, preserving its provider session (E7).
    pub async fn set_uploads_paused(&self, paused: bool) -> Result<(), DbError> {
        self.inner.check_open()?;
        self.inner.database.set_uploads_paused(paused).await?;
        self.inner.paused.store(paused, Ordering::Release);
        if !paused {
            self.inner.wake.notify_one();
        }
        Ok(())
    }

    /// A serialized sequence pass interrupts background downloads, releasing
    /// their shared capacity. Dropping the guard resumes retained requests,
    /// including when the pass fails or its caller cancels (§6.6, §17.6).
    pub(crate) fn sequence_downloads(&self) -> SequenceDownloads<'_> {
        self.inner.sequence_active.send_replace(true);
        SequenceDownloads {
            active: &self.inner.sequence_active,
            wake: &self.inner.wake,
        }
    }

    /// Finish transfers before verifying and committing a connection change.
    /// A failed setup leaves the current connection installed.
    pub async fn set_storage(
        &self,
        storage: Option<Arc<dyn Storage>>,
        ready: impl std::future::Future<Output = Result<(), SyncError>>,
    ) -> Result<(), SyncError> {
        let _eager = self.inner.eager_fill.lock().await;
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
        self.inner.wake.notify_one();
        Ok(())
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

    pub(crate) async fn lock_file(
        &self,
        file: &coven_database::FileRef,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut reads = self.reads.lock().expect("file read locks poisoned");
            reads.retain(|_, lock| lock.strong_count() != 0);
            let key = (file.namespace().to_owned(), file.id());
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
            // Space reclamation is ancillary to a successful read (§17.6),
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
