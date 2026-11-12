//! File transfers and cached reads, composed beside the database at the root.

#[path = "file_cache.rs"]
mod file_cache;
#[path = "file_cache_retry.rs"]
mod file_cache_retry;
#[path = "file_device_only.rs"]
mod file_device_only;
#[path = "file_error.rs"]
mod file_error;
#[path = "file_failure.rs"]
mod file_failure;
#[path = "file_keep.rs"]
mod file_keep;
#[path = "file_read.rs"]
mod file_read;
#[path = "file_status.rs"]
mod file_status;
#[path = "file_upload.rs"]
mod file_upload;
#[path = "file_upload_abandoned.rs"]
mod file_upload_abandoned;
#[path = "file_upload_connections.rs"]
mod file_upload_connections;
#[path = "file_upload_failure.rs"]
mod file_upload_failure;
#[path = "file_upload_pool.rs"]
mod file_upload_pool;
#[path = "file_upload_queue.rs"]
mod file_upload_queue;
#[path = "file_upload_retry.rs"]
mod file_upload_retry;
#[path = "file_upload_settings.rs"]
mod file_upload_settings;
use crate::SyncError;
pub use coven_database::PinProgress;
use coven_database::{DbError, FileDatabase, Mode, ReadOnly, Writer};
use coven_foundation::locks::{lock_unpoisoned, write_unpoisoned};
use coven_foundation::{files::StoreDir, id_source::IdSourceRef};
use coven_storage::Storage;
pub use file_device_only::{DeviceOnlyError, DeviceOnlyStep};
pub use file_error::FileReadError;
pub use file_read::{FileRangeStream, FileStream, ReadProgress};
pub use file_status::FileStatus;
pub(crate) use file_upload_connections::SpareConnection;
pub use file_upload_pool::DrainOutcome;
pub use file_upload_queue::{QueuedUpload, UploadPhase, UploadProgress, UploadQueue};
pub use file_upload_settings::UploadSettings;
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

/// What a handle's mode adds to its file owner: the writer's upload
/// settings, which the upload runs and the app's setting changes use, and
/// nothing for a read-only handle, which runs no upload. Sealed with `Mode`.
pub trait FilesMode: Mode {
    /// The mode's upload state, beside what every file read uses.
    type Uploads: Send + Sync + 'static;
}
impl FilesMode for Writer {
    type Uploads = UploadSettings;
}
impl FilesMode for ReadOnly {
    type Uploads = ();
}

/// Owns storage, database file operations and the device's file area
/// (Appendix F.3), for the store's writer or a read-only handle
/// (`Files<ReadOnly>`), which only reads files and keeps its cache. The
/// writer's starts one commit-driven worker, for uploads and pins; no sync
/// loop or timer is created. Call `close` before closing the database.
pub struct Files<M: FilesMode = Writer> {
    inner: Arc<FilesInner<M>>,
    worker: Arc<FileWorker>,
}
// Written out: `derive(Clone)` would ask the mode to be `Clone`.
impl<M: FilesMode> Clone for Files<M> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), worker: self.worker.clone() }
    }
}
/// One lock per stored object's cache entries, by namespace and object.
type FileReadLocks = BTreeMap<
    (String, coven_foundation::id_source::ObjectId),
    std::sync::Weak<tokio::sync::Mutex<()>>,
>;
struct FilesInner<M: FilesMode = Writer> {
    database: FileDatabase<M>,
    directory: StoreDir,
    storage: std::sync::RwLock<Option<Arc<dyn Storage>>>,
    send: Arc<crate::SequenceState>,
    waits: Arc<crate::RetryWaits>,
    /// Failures observed on this connection that require a new connection.
    /// Reopening creates a new connection and permits one attempt again.
    connection_waits: Mutex<BTreeSet<coven_database::RetrySubject>>,
    ids: IdSourceRef,
    /// Paces the progress a long call reports.
    clock: coven_foundation::clock::ClockRef,
    /// The mode's upload state: the writer's copies of the upload settings,
    /// and nothing for a read-only handle.
    uploads: M::Uploads,
    /// The persisted setting to move files only on unmetered networks, as
    /// of the last upload run or app change; parts and pins' downloads
    /// check it, with the network, between requests (§17.9).
    unmetered_only: AtomicBool,
    /// The device's network, as the store's watch says it is (§6.7).
    network: coven_foundation::network_watch::NetworkView,
    /// Wakes a pin's download waiting for the network as the setting
    /// changes or the store closes.
    hold_wake: Notify,
    /// How many pins' downloads wait for the network now, for a test.
    #[cfg(test)]
    pub(crate) held_downloads: tokio::sync::watch::Sender<usize>,
    /// How many reads have found another read of the same stored bytes
    /// holding their lock, for a test.
    #[cfg(test)]
    pub(crate) shared_reads: tokio::sync::watch::Sender<usize>,
    /// Wakes the upload worker, or its run in progress.
    upload_wake: Arc<Notify>,
    /// Each time the upload worker has taken everything that can wake it
    /// and waits.
    upload_parks: crate::Parks,
    /// One upload run at a time.
    drain: tokio::sync::Mutex<()>,
    /// App retries waiting for an upload run to serve them.
    app_retries: Mutex<Vec<file_upload_pool::AppRetry>>,
    /// Why the upload worker ended, other than told to stop, set under
    /// `app_retries`' lock: no run serves a retry once it is set, so each
    /// fails with it (E15).
    worker_ended: std::sync::OnceLock<file_upload_pool::WorkerEnd>,
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
    /// Holds a device-only call at the place a test names: where a copy's
    /// bytes were just appended, or where a copy is renamed to its path and
    /// not yet recorded placed. It says it got there, then waits to be
    /// resumed.
    #[cfg(test)]
    device_only_gate: Mutex<Option<file_device_only::DeviceOnlyGate>>,
}
struct FileWorker {
    stop: watch::Sender<bool>,
    /// The writer's upload worker, until the owner closes; a read-only
    /// handle runs none.
    task: tokio::sync::Mutex<Option<coven_foundation::task::OwnedTask>>,
}
impl Drop for FileWorker {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
impl Files<ReadOnly> {
    /// Compose a read-only handle's file owner, which reads files and keeps
    /// its cache; it uploads, pins and records nothing, and runs no worker.
    pub fn read_only(
        database: FileDatabase<ReadOnly>,
        directory: StoreDir,
        storage: Option<Arc<dyn Storage>>,
        send: Arc<crate::SequenceState>,
        waits: Arc<crate::RetryWaits>,
        ids: IdSourceRef,
        transfers: Arc<TransferBudget>,
        clock: coven_foundation::clock::ClockRef,
        network: coven_foundation::network_watch::NetworkView,
    ) -> Self {
        let inner = Arc::new(FilesInner::new(
            database,
            directory,
            storage,
            send,
            waits,
            ids,
            transfers,
            clock,
            network,
            (),
        ));
        let worker =
            FileWorker { stop: watch::channel(false).0, task: tokio::sync::Mutex::new(None) };
        Self { inner, worker: Arc::new(worker) }
    }
}
impl<M: FilesMode> FilesInner<M> {
    fn new(
        database: FileDatabase<M>,
        directory: StoreDir,
        storage: Option<Arc<dyn Storage>>,
        send: Arc<crate::SequenceState>,
        waits: Arc<crate::RetryWaits>,
        ids: IdSourceRef,
        transfers: Arc<TransferBudget>,
        clock: coven_foundation::clock::ClockRef,
        network: coven_foundation::network_watch::NetworkView,
        uploads: M::Uploads,
    ) -> Self {
        Self {
            database,
            directory,
            storage: std::sync::RwLock::new(storage),
            send,
            waits,
            connection_waits: Mutex::new(BTreeSet::new()),
            ids,
            clock,
            uploads,
            unmetered_only: AtomicBool::new(false),
            network,
            hold_wake: Notify::new(),
            #[cfg(test)]
            held_downloads: tokio::sync::watch::channel(0).0,
            #[cfg(test)]
            shared_reads: tokio::sync::watch::channel(0).0,
            upload_wake: Arc::new(Notify::new()),
            upload_parks: crate::Parks::new(),
            drain: tokio::sync::Mutex::new(()),
            app_retries: Mutex::new(Vec::new()),
            worker_ended: std::sync::OnceLock::new(),
            changing: AtomicUsize::new(0),
            cache: tokio::sync::RwLock::new(()),
            reads: Mutex::new(BTreeMap::new()),
            transfers,
            pins: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
            #[cfg(test)]
            blocked_end_gate: Mutex::new(None),
            #[cfg(test)]
            device_only_gate: Mutex::new(None),
        }
    }
}
impl<M: FilesMode> Files<M> {
    /// Stop background work, wait for in-flight database and transfer work,
    /// then release the storage client, while clones of this owner live on.
    /// An app retry still waiting fails as the store closes, as does a read
    /// of a file stream still open.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        // A pin's download waiting for the network fails as the store closes.
        self.inner.hold_wake.notify_waiters();
        self.worker.stop.send_replace(true);
        if let Some(task) = self.worker.task.lock().await.take() {
            task.ended().await;
        }
        let _drain = self.inner.drain.lock().await;
        let _pins = self.inner.pins.lock().await;
        let _cache = self.inner.cache.write().await;
        lock_unpoisoned(&self.inner.app_retries).clear();
        write_unpoisoned(&self.inner.storage).take();
    }
}
impl Files {
    /// Each time the upload worker has taken every change, wake and due
    /// retry that was ready, and waits for the next.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn uploads_parked_from_now(
        &self,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.inner.upload_parks.parked_from_now()
    }

    /// Compose the writer's file owner and start its upload worker.
    /// Storage may be absent while uploads wait.
    pub fn new(
        database: FileDatabase,
        directory: StoreDir,
        storage: Option<Arc<dyn Storage>>,
        send: Arc<crate::SequenceState>,
        waits: Arc<crate::RetryWaits>,
        ids: IdSourceRef,
        transfers: Arc<TransferBudget>,
        clock: coven_foundation::clock::ClockRef,
        network: coven_foundation::network_watch::NetworkView,
    ) -> Self {
        let uploads_changes = database.queued_work_changes();
        let (stop, _) = watch::channel(false);
        let inner = Arc::new(FilesInner::new(
            database,
            directory,
            storage,
            send,
            waits,
            ids,
            transfers,
            clock,
            network,
            UploadSettings::new(),
        ));
        // A panic in the worker goes to the store's bug sink, halts sync
        // until the store reopens, as the sync loop's own panic does, and
        // fails the app retries it would have served (E15).
        let owner = Arc::downgrade(&inner);
        let task = coven_foundation::task::spawn_owned(
            "the upload worker",
            inner.database.bugs().clone(),
            upload_worker(Arc::downgrade(&inner), stop.subscribe(), uploads_changes),
            |bug| async move {
                if let Some(owner) = owner.upgrade() {
                    // A failed write has failed the handle, which reports
                    // it, or its bug goes to the sink.
                    if let Err(error) = owner.waits.sync_halted(bug.clone()).await {
                        let error = crate::SyncError::from(error);
                        if !coven_foundation::task::report_carried(
                            owner.database.bugs().as_ref(),
                            &error,
                        ) {
                            tracing::debug!(
                                "halting sync after the upload worker's panic failed: {error}"
                            );
                        }
                    }
                    owner.worker_ended(file_upload_pool::WorkerEnd::Panicked(bug));
                }
            },
        );
        let worker = FileWorker { stop, task: tokio::sync::Mutex::new(Some(task)) };
        Self { inner, worker: Arc::new(worker) }
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
    /// Optionally retry recoverable uploads early: the upload worker attempts
    /// every upload that can retry, whatever its wait, and this returns once
    /// each attempt has ended. Provider cooldowns and failures that never
    /// clear still hold (§17.9), and while uploads are paused only bound ones
    /// are tried; while sync is stopped none is (§17.4).
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError> {
        // The worker stopping with the store drops the retry unanswered; a
        // worker that ended otherwise answers it with why.
        self.inner.ask_retry()?.answered().await
    }
    /// Persist the pause setting, as a step of `sql`'s write; a running
    /// trailing upload stops between confirmed parts, preserving its
    /// provider session (E7). Bound uploads run on (§17.4). Once the write
    /// commits, and before it returns, the copy parts check takes the new
    /// setting, after any upload run that read the old one has acted on it:
    /// so no retry asked after the write returns is answered from the
    /// setting it replaced.
    pub fn set_uploads_paused(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        paused: bool,
    ) -> Result<(), DbError> {
        self.inner.check_open()?;
        sql.set_uploads_paused(paused)?;
        let inner = self.inner.clone();
        sql.after_commit(move || {
            // The write's effects run on its blocking thread, where waiting
            // for the run is allowed.
            let change = inner.hold_change_blocking();
            inner.set_paused(paused);
            drop(change);
            if !paused {
                inner.upload_wake.notify_one();
            }
        });
        Ok(())
    }
    /// Persist the setting to move files only on unmetered networks, as a
    /// step of `sql`'s write (§17.9): while the network is metered, a running
    /// trailing upload stops between confirmed parts, as a pause stops it,
    /// and pins' downloads wait before their next request to storage. Bound
    /// uploads run on. Once the write commits, and before it returns, the
    /// uploads and downloads take the new setting, as they take a pause.
    pub fn set_unmetered_only(
        &self,
        sql: &coven_database::SqlContext<'_, '_>,
        unmetered_only: bool,
    ) -> Result<(), DbError> {
        self.inner.check_open()?;
        sql.set_unmetered_only(unmetered_only)?;
        let inner = self.inner.clone();
        sql.after_commit(move || {
            let change = inner.hold_change_blocking();
            inner.unmetered_only.store(unmetered_only, Ordering::Release);
            drop(change);
            if !unmetered_only {
                inner.upload_wake.notify_one();
                inner.hold_wake.notify_waiters();
            }
        });
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
        *write_unpoisoned(&self.inner.storage) = storage;
        lock_unpoisoned(&self.inner.connection_waits).clear();
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
/// prerequisite or a due retry may let one start, while sync is started
/// (§17.4, §17.9). A run keeps the
/// upload limit's number in flight until none can start. After each run it
/// makes the pins coven keeps that may be made (§17.8).
async fn upload_worker(
    owner: std::sync::Weak<FilesInner>,
    stopping: watch::Receiver<bool>,
    changes: coven_database::DatabaseChanges,
) {
    let ended = serve_uploads(owner.clone(), stopping, changes).await;
    // No run serves an app retry once the worker has ended, so each waiting
    // one, and each asked after, hears why.
    if let (Some(ended), Some(owner)) = (ended, owner.upgrade()) {
        owner.worker_ended(file_upload_pool::WorkerEnd::of(ended));
    }
}

/// The upload worker's runs, until it is told to stop or its owner is gone,
/// which end it with `None`, or until what it watches fails, which ends it
/// with that failure.
async fn serve_uploads(
    owner: std::sync::Weak<FilesInner>,
    mut stopping: watch::Receiver<bool>,
    mut changes: coven_database::DatabaseChanges,
) -> Option<SyncError> {
    let first = owner.upgrade()?;
    let parks = first.upload_parks.clone();
    let _running = parks.running();
    let wake = first.upload_wake.clone();
    // Pins coven keeps resume as the store opens (§17.8).
    wake.notify_one();
    let mut sending = first.send.watch();
    let mut syncing = first.waits.syncing();
    let mut reports = first.waits.network_reports();
    // The network turning unmetered lets held uploads and pins go (§17.9).
    let mut network = first.network.clone();
    drop(first);
    // The earliest due upload retry, armed after each run (§6.7).
    let mut retry: crate::retry_waits::Wake = Box::pin(std::future::pending());
    loop {
        tokio::select! {
            biased;
            never = parks.polling() => match never {},
            _ = stopping.changed() => return None,
            result = changes.next() => if let Err(error) = result {
                if !matches!(error, DbError::StoreClosed) {
                    tracing::error!(?error, "file commit observation failed");
                }
                return Some(error.into());
            },
            () = wake.notified() => {},
            // The store's sequence state, retry waits and network go only
            // with the store.
            result = sending.changed() => if result.is_err() { return Some(DbError::StoreClosed.into()) },
            // Starting sync lets uploads run (§17.4).
            result = syncing.changed() => if result.is_err() { return Some(DbError::StoreClosed.into()) },
            result = reports.changed() => if result.is_err() { return Some(DbError::StoreClosed.into()) },
            () = network.changed() => {},
            () = &mut retry => {},
            never = parks.parked() => match never {},
        }
        let owner = owner.upgrade()?;
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
                Err(error) => show_worker_failure(&owner, error).await?,
            }
            // The switches a run made can ask for pins; a paused queue or no
            // storage holds no pin made from coven's copy.
            match owner.keep_pinned().await {
                Ok(()) | Err(SyncError::Database(DbError::StoreClosed)) => Ok(()),
                Err(error) => show_worker_failure(&owner, error).await,
            }
        };
        let ran =
            tokio::select! { biased; _ = stopping.changed() => return None, ran = run => ran };
        if let Err(error) = ran {
            return Some(error);
        }
        // A paused queue still runs its bound uploads, whose retries the
        // worker must wake for (§17.4).
        retry = match owner.database.bound_uploads().await {
            Ok(bound) => {
                owner.next_upload_retry(&bound).unwrap_or_else(|| Box::pin(std::future::pending()))
            }
            Err(DbError::StoreClosed) => return Some(DbError::StoreClosed.into()),
            // Commits, network reports and wakes still run the worker; no
            // retry wait wakes it until a later run arms one.
            Err(error) => {
                if let Err(error) = show_worker_failure(&owner, error.into()).await {
                    return Some(error);
                }
                Box::pin(std::future::pending())
            }
        };
    }
}

/// A failure of the upload worker's own work, which no caller hears: a bug
/// goes to the store's bug sink, and any other failure is written as how
/// the last pass ended, so the sync state says the queue is failing (§19.1)
/// rather than only the log. A failed write of that has failed the store's
/// handle, and ends the worker.
async fn show_worker_failure(owner: &FilesInner, error: SyncError) -> Result<(), SyncError> {
    if coven_foundation::task::report_carried(owner.database.bugs().as_ref(), &error) {
        return Ok(());
    }
    owner.waits.show_worker_failure(error).await?;
    Ok(())
}

impl<M: FilesMode> FilesInner<M> {
    fn connection_due(&self, subject: coven_database::RetrySubject) -> bool {
        !lock_unpoisoned(&self.connection_waits).contains(&subject)
    }

    fn wait_for_connection(&self, subject: coven_database::RetrySubject) {
        lock_unpoisoned(&self.connection_waits).insert(subject);
    }

    fn forget_connection_wait(&self, subject: coven_database::RetrySubject) {
        lock_unpoisoned(&self.connection_waits).remove(&subject);
    }

    /// Keep only the connection waits `keep` accepts.
    fn retain_connection_waits(&self, keep: impl Fn(&coven_database::RetrySubject) -> bool) {
        lock_unpoisoned(&self.connection_waits).retain(|subject| keep(subject));
    }

    /// Hold the lock on one stored object's cache entries while reading
    /// them, fetching and caching chunks. Files over the same bytes share it.
    pub(crate) async fn lock_file(
        &self,
        file: &coven_database::FileRef,
        location: &coven_database::FileLocation,
    ) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut reads = lock_unpoisoned(&self.reads);
            reads.retain(|_, lock| lock.strong_count() != 0);
            let key = (file.namespace().to_owned(), location.object());
            match reads.get(&key).and_then(std::sync::Weak::upgrade) {
                Some(lock) => {
                    #[cfg(test)]
                    self.shared_reads.send_modify(|shared| *shared += 1);
                    lock
                }
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
    /// Whether the setting to move files only on unmetered networks holds a
    /// transfer now: it is on, the network is metered, and it holds this
    /// transfer, which for an upload depends on whether it is bound.
    pub(crate) fn metered_holds(&self, bound: bool) -> bool {
        self.unmetered_only.load(Ordering::Acquire)
            && self.network.current().metered
            && held_on_metered(bound)
    }
    /// Whether a pin's download waits for the network now (§17.8).
    pub(crate) fn downloads_held(&self) -> bool {
        self.metered_holds(false)
    }
    /// Wait until no metered network holds a pin's download: the network
    /// turns unmetered, or the setting goes off. Fails once the store
    /// closes.
    pub(crate) async fn until_downloads_unheld(&self) -> Result<(), DbError> {
        let mut network = self.network.clone();
        loop {
            let woken = self.hold_wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            self.check_open()?;
            network.read();
            if !self.downloads_held() {
                return Ok(());
            }
            #[cfg(test)]
            let _held = HeldDownload::new(&self.held_downloads);
            tokio::select! {
                () = woken => {}
                () = network.changed() => {}
            }
        }
    }
    pub(crate) async fn trim(&self, namespace: &str) {
        if let Err(error) = self.database.trim_cache(namespace).await {
            // Space reclamation is ancillary to a successful read (§17.8),
            // but its failure must remain visible in the structured log.
            tracing::error!(?error, namespace, "cache eviction failed");
        }
    }
}
impl FilesInner {
    /// Take the persisted setting to move files only on unmetered networks
    /// as this owner's copy, as an upload run does for both settings: a pin
    /// prepared as the store opens holds as the setting says, before any
    /// upload run has read it.
    pub(crate) async fn read_unmetered_only(&self) -> Result<(), DbError> {
        let _change = self.lock_hold_change().await;
        let unmetered_only = self.database.unmetered_only().await?;
        self.unmetered_only.store(unmetered_only, Ordering::Release);
        Ok(())
    }
}
/// A pin's download counted as waiting for the network while it waits.
#[cfg(test)]
struct HeldDownload<'a>(&'a tokio::sync::watch::Sender<usize>);

#[cfg(test)]
impl<'a> HeldDownload<'a> {
    fn new(held: &'a tokio::sync::watch::Sender<usize>) -> Self {
        held.send_modify(|held| *held += 1);
        Self(held)
    }
}

#[cfg(test)]
impl Drop for HeldDownload<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|held| *held -= 1);
    }
}

/// What holds trailing uploads (§17.4, §17.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrailingHold {
    /// The app paused uploads.
    Paused,
    /// A self-replacement pauses this device's writes, so a trailing
    /// upload's completion would have to wait for it (§12.3). A bound
    /// upload's completion goes through: the replacement waits for it.
    MemberKeysChanging,
    /// The network is metered and the app moves files only on unmetered
    /// networks.
    Metered,
}

/// Whether the setting to move files only on unmetered networks holds an
/// upload on a metered network: the trailing ones, as the pause holds them,
/// since every later write waits for a bound one (§17.4). The Lean model's
/// `Upload.heldOnMetered`; the views' `coven_uploads.metered` says the same.
pub(crate) fn held_on_metered(bound: bool) -> bool {
    !bound
}

#[cfg(test)]
#[path = "files_tests.rs"]
mod tests;
